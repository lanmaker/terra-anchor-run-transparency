import csv
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd
import requests
from dotenv import load_dotenv

from src.config import (
    ANCHOR_MARKET_CONTRACT,
    AUST_CONTRACT,
    RAW_DIR,
    WINDOW_END,
    WINDOW_START,
)

DEFAULT_FCD_URL = "https://terra-classic-fcd.publicnode.com"
DEFAULT_TIMEOUT = 30
DEFAULT_POLL_SECONDS = 0.2
DEFAULT_LIMIT = 100
DEFAULT_MAX_PAGES = 20000
DEFAULT_MAX_SEEK_PAGES = 5000
DEFAULT_RETRIES = 5
DEFAULT_BACKOFF = 1.5
DEFAULT_RAW_PATH = "data/interim/actions_raw.csv"
DEFAULT_CHECKPOINT_DIR = "data/interim"

SENDER_KEYS = {"sender", "from", "owner", "redeemer"}
AMOUNT_KEYS = {"amount", "deposit_amount", "redeem_amount", "returned_amount"}


def _parse_dt(value: str) -> datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value).astimezone(timezone.utc)


def _parse_amount(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    text = str(value)
    if not text:
        return None
    if "uusd" in text:
        for part in text.split(","):
            part = part.strip()
            if part.endswith("uusd"):
                try:
                    return float(part.replace("uusd", "")) / 1e6
                except ValueError:
                    return None
        return None
    if text.isdigit():
        return float(text) / 1e6
    try:
        return float(text)
    except ValueError:
        return None


class FCDClient:
    def __init__(self) -> None:
        load_dotenv()
        self.base_url = os.environ.get("TERRA_FCD_URL", DEFAULT_FCD_URL).rstrip("/")
        self.timeout = int(os.environ.get("TERRA_REQUEST_TIMEOUT", DEFAULT_TIMEOUT))
        self.poll_seconds = float(os.environ.get("TERRA_POLL_SECONDS", DEFAULT_POLL_SECONDS))
        self.limit = int(os.environ.get("TERRA_FCD_LIMIT", DEFAULT_LIMIT))
        self.max_pages = int(os.environ.get("TERRA_FCD_MAX_PAGES", DEFAULT_MAX_PAGES))
        self.max_seek_pages = int(os.environ.get("TERRA_FCD_MAX_SEEK_PAGES", DEFAULT_MAX_SEEK_PAGES))
        self.retries = int(os.environ.get("TERRA_FCD_RETRIES", DEFAULT_RETRIES))
        self.backoff = float(os.environ.get("TERRA_FCD_BACKOFF", DEFAULT_BACKOFF))
        self.session = requests.Session()

    def _request(self, params: dict) -> dict:
        url = f"{self.base_url}/v1/txs"
        attempt = 0
        while True:
            try:
                resp = self.session.get(url, params=params, timeout=self.timeout)
                resp.raise_for_status()
                return resp.json()
            except Exception:
                attempt += 1
                if attempt >= self.retries:
                    raise
                time.sleep(self.backoff * attempt)

    def fetch_page(self, account: str, offset: Optional[int] = None) -> dict:
        params = {"account": account, "limit": self.limit}
        if offset is not None:
            params["offset"] = offset
        return self._request(params)

    def latest_id(self, account: str) -> Optional[int]:
        data = self.fetch_page(account)
        txs = data.get("txs", [])
        if not txs:
            return None
        return int(txs[0].get("id"))


@dataclass
class ActionWriter:
    path: Path
    header_written: bool = False

    def write(self, rows: list[dict]) -> None:
        if not rows:
            return
        fieldnames = ["hour", "wallet", "action", "amount", "txhash"]
        mode = "a"
        with self.path.open(mode, newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            if not self.header_written:
                writer.writeheader()
                self.header_written = True
            for row in rows:
                writer.writerow(row)


def _read_checkpoint(path: Path) -> Optional[dict]:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError:
        return None


def _write_checkpoint(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2))


def _extract_sender(tx: dict) -> Optional[str]:
    messages = tx.get("tx", {}).get("value", {}).get("msg", [])
    for msg in messages:
        sender = msg.get("value", {}).get("sender") or msg.get("value", {}).get("from_address")
        if sender:
            return sender
    return None


def _iter_events(tx: dict) -> Iterable[dict]:
    logs = tx.get("logs")
    if isinstance(logs, list) and logs:
        for log in logs:
            for event in log.get("events", []):
                yield event
        return

    raw_log = tx.get("raw_log")
    if not raw_log or raw_log == "[]":
        return
    try:
        parsed = json.loads(raw_log)
    except json.JSONDecodeError:
        return
    for log in parsed:
        for event in log.get("events", []):
            yield event


def _event_segments(event: dict) -> list[dict]:
    attrs = event.get("attributes", [])
    segments = []
    current = None
    current_contract = None

    for attr in attrs:
        key = attr.get("key")
        val = attr.get("value")
        if key == "contract_address":
            current_contract = val
            continue
        if key == "action":
            if current:
                segments.append(current)
            current = {
                "action": val,
                "contract_address": current_contract,
                "amount": None,
                "sender": None,
            }
            continue
        if current is None:
            continue
        if key in SENDER_KEYS and current["sender"] is None:
            current["sender"] = val
        if key in AMOUNT_KEYS and current["amount"] is None:
            current["amount"] = val

    if current:
        segments.append(current)
    return segments


def _extract_actions(tx: dict) -> list[dict]:
    sender = _extract_sender(tx)
    txhash = tx.get("txhash")
    rows = []
    for event in _iter_events(tx):
        if event.get("type") not in {"wasm", "execute_contract"}:
            continue
        segments = _event_segments(event)
        if not segments:
            continue
        for seg in segments:
            action = seg.get("action")
            if action not in {"deposit_stable", "redeem_stable"}:
                continue
            amount = _parse_amount(seg.get("amount"))
            if amount is None:
                continue
            rows.append(
                {
                    "action": action,
                    "wallet": seg.get("sender") or sender,
                    "amount": amount,
                    "txhash": txhash,
                }
            )
    return rows


def _collect_actions(
    client: FCDClient,
    account: str,
    start: datetime,
    end: datetime,
    label: str,
    writer: ActionWriter,
    checkpoint_path: Path,
) -> list[dict]:
    offset = None
    pages = 0
    seeking = True
    collected = []

    manual_offset = os.environ.get("TERRA_FCD_START_OFFSET")
    if manual_offset:
        try:
            offset = int(manual_offset)
            seeking = False
            print(f"{label}: using start offset {offset}", flush=True)
        except ValueError:
            offset = None

    checkpoint = _read_checkpoint(checkpoint_path)
    if seeking and checkpoint:
        if checkpoint.get("window_start") == start.isoformat() and checkpoint.get("window_end") == end.isoformat():
            offset = checkpoint.get("offset")
            seeking = False if offset else True
            print(f"{label}: resuming from checkpoint offset {offset}", flush=True)

    if seeking:
        latest_id = client.latest_id(account)
        if latest_id is None:
            return collected
        low, high = 0, latest_id
        best = None
        for _ in range(client.max_seek_pages):
            mid = (low + high) // 2
            data = client.fetch_page(account, mid)
            txs = data.get("txs", [])
            if not txs:
                high = mid - 1
                continue
            ts = _parse_dt(txs[0]["timestamp"])
            if ts > end:
                high = mid - 1
            else:
                best = mid
                low = mid + 1
            time.sleep(client.poll_seconds)
        if best is not None:
            offset = best
            seeking = False
            print(f"{label}: binary seek offset {offset}", flush=True)

    while pages < client.max_pages:
        data = client.fetch_page(account, offset)
        txs = data.get("txs", [])
        if not txs:
            break

        oldest_ts = _parse_dt(txs[-1]["timestamp"])
        newest_ts = _parse_dt(txs[0]["timestamp"])

        if seeking:
            if oldest_ts > end:
                offset = data.get("next")
                pages += 1
                if pages % 50 == 0:
                    print(f"{label}: seeking page {pages}, oldest {oldest_ts.isoformat()}", flush=True)
                time.sleep(client.poll_seconds)
                continue
            seeking = False

        rows_to_write: list[dict] = []
        stop = False
        for tx in txs:
            ts = _parse_dt(tx["timestamp"])
            if ts < start:
                stop = True
                break
            if ts > end:
                continue
            hour = ts.replace(minute=0, second=0, microsecond=0)
            actions = _extract_actions(tx)
            for action in actions:
                if not action.get("wallet"):
                    continue
                rows_to_write.append(
                    {
                        "hour": hour.isoformat(),
                        "wallet": action["wallet"],
                        "action": action["action"],
                        "amount": action["amount"],
                        "txhash": action.get("txhash"),
                    }
                )

        if rows_to_write:
            writer.write(rows_to_write)

        offset = data.get("next")
        checkpoint_payload = {
            "offset": offset,
            "pages": pages,
            "label": label,
            "account": account,
            "window_start": start.isoformat(),
            "window_end": end.isoformat(),
            "oldest_ts": oldest_ts.isoformat(),
            "newest_ts": newest_ts.isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        _write_checkpoint(checkpoint_path, checkpoint_payload)
        pages += 1
        if pages % 50 == 0:
            print(f"{label}: processed {pages} pages", flush=True)
        time.sleep(client.poll_seconds)

        if stop:
            break

    return collected


def _build_hourly(df: pd.DataFrame, action: str, col_name: str) -> pd.DataFrame:
    subset = df[df["action"] == action]
    if subset.empty:
        return pd.DataFrame(columns=["hour", "wallet", col_name])
    out = (
        subset.groupby(["hour", "wallet"], as_index=False)["amount"].sum().rename(columns={"amount": col_name})
    )
    return out


def _aggregate_raw(path: Path, start: datetime, end: datetime) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not path.exists():
        return (
            pd.DataFrame(columns=["hour", "wallet", "ust_inflow"]),
            pd.DataFrame(columns=["hour", "wallet", "ust_outflow"]),
        )

    deposits = {}
    redeems = {}
    for chunk in pd.read_csv(path, chunksize=250000):
        chunk["hour"] = pd.to_datetime(chunk["hour"], utc=True)
        chunk = chunk[(chunk["hour"] >= start) & (chunk["hour"] <= end)]
        if chunk.empty:
            continue
        for action, target in [("deposit_stable", deposits), ("redeem_stable", redeems)]:
            subset = chunk[chunk["action"] == action]
            if subset.empty:
                continue
            grouped = subset.groupby(["hour", "wallet"])["amount"].sum()
            for (hour, wallet), amount in grouped.items():
                key = (hour, wallet)
                target[key] = target.get(key, 0.0) + amount

    dep_rows = [{"hour": k[0], "wallet": k[1], "ust_inflow": v} for k, v in deposits.items()]
    red_rows = [{"hour": k[0], "wallet": k[1], "ust_outflow": v} for k, v in redeems.items()]
    return pd.DataFrame(dep_rows), pd.DataFrame(red_rows)


def main() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    interim_dir = Path(os.environ.get("TERRA_FCD_CHECKPOINT_DIR", DEFAULT_CHECKPOINT_DIR))
    interim_dir.mkdir(parents=True, exist_ok=True)
    raw_path = Path(os.environ.get("TERRA_FCD_RAW_PATH", DEFAULT_RAW_PATH))
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    only_aggregate = os.environ.get("TERRA_FCD_ONLY_AGGREGATE", "0") == "1"

    start = _parse_dt(WINDOW_START + "+00:00")
    end = _parse_dt(WINDOW_END + "+00:00")

    include_aust = os.environ.get("TERRA_INCLUDE_AUST", "0") == "1"

    client = FCDClient()

    writer = ActionWriter(raw_path)
    if raw_path.exists() and raw_path.stat().st_size > 0:
        writer.header_written = True

    if not only_aggregate:
        print("Fetching Anchor market transactions...", flush=True)
        _collect_actions(
            client,
            ANCHOR_MARKET_CONTRACT,
            start,
            end,
            "market",
            writer,
            interim_dir / "fcd_checkpoint_market.json",
        )

        if include_aust:
            print("Fetching aUST transactions...", flush=True)
            _collect_actions(
                client,
                AUST_CONTRACT,
                start,
                end,
                "aust",
                writer,
                interim_dir / "fcd_checkpoint_aust.json",
            )

    deposits, redeems = _aggregate_raw(raw_path, start, end)
    if deposits.empty and redeems.empty:
        print("No actions collected. Check FCD availability or widen the window.", flush=True)
        return

    deposits.to_csv(RAW_DIR / "anchor_deposits_hourly.csv", index=False)
    redeems.to_csv(RAW_DIR / "anchor_redeems_hourly.csv", index=False)

    print("Saved:", flush=True)
    print(" -", RAW_DIR / "anchor_deposits_hourly.csv", flush=True)
    print(" -", RAW_DIR / "anchor_redeems_hourly.csv", flush=True)


if __name__ == "__main__":
    main()
