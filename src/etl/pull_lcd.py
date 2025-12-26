import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd
import requests
from dotenv import load_dotenv

from src.config import (
    ANCHOR_MARKET_CONTRACT,
    HISTORY_START,
    RAW_DIR,
    WINDOW_END,
    WINDOW_START,
)

DEFAULT_LCD_URLS = [
    "https://terra-classic-lcd.publicnode.com",
]

DEFAULT_RPC_URLS = [
    "https://terra-classic-rpc.publicnode.com",
]

DEFAULT_TIMEOUT = 30
DEFAULT_POLL_SECONDS = 0.2
DEFAULT_PAGE_LIMIT = 100
DEFAULT_MAX_PAGES = 2000


def _parse_dt(value: str) -> datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value).astimezone(timezone.utc)


def _parse_uusd(value: Optional[str]) -> Optional[float]:
    if not value:
        return None
    if isinstance(value, (int, float)):
        return float(value) / 1e6
    text = str(value)
    if "uusd" in text:
        for part in text.split(","):
            part = part.strip()
            if part.endswith("uusd"):
                return float(part.replace("uusd", "")) / 1e6
        return None
    if text.isdigit():
        return float(text) / 1e6
    return None


class LCDClient:
    def __init__(self) -> None:
        load_dotenv()
        env_url = os.environ.get("TERRA_LCD_URL")
        urls = []
        if env_url:
            urls.append(env_url)
        urls.extend(DEFAULT_LCD_URLS)
        self.base_urls = [u.rstrip("/") for u in urls]

        env_rpc = os.environ.get("TERRA_RPC_URL")
        rpc_urls = []
        if env_rpc:
            rpc_urls.append(env_rpc)
        rpc_urls.extend(DEFAULT_RPC_URLS)
        self.rpc_urls = [u.rstrip("/") for u in rpc_urls]

        self.timeout = int(os.environ.get("TERRA_REQUEST_TIMEOUT", DEFAULT_TIMEOUT))
        self.poll_seconds = float(os.environ.get("TERRA_POLL_SECONDS", DEFAULT_POLL_SECONDS))
        self.page_limit = int(os.environ.get("TERRA_PAGE_LIMIT", DEFAULT_PAGE_LIMIT))
        self.max_pages = int(os.environ.get("TERRA_MAX_PAGES", DEFAULT_MAX_PAGES))
        self.session = requests.Session()

    def _request(self, path: str, params: Optional[list[tuple[str, str]]] = None) -> dict:
        last_error = None
        for base in self.base_urls:
            url = f"{base}{path}"
            try:
                resp = self.session.get(url, params=params, timeout=self.timeout)
                resp.raise_for_status()
                return resp.json()
            except Exception as exc:  # pragma: no cover - fallback between endpoints
                last_error = exc
                continue
        raise RuntimeError(f"LCD request failed: {last_error}")

    def _request_rpc(self, path: str, params: Optional[dict] = None) -> dict:
        last_error = None
        for base in self.rpc_urls:
            url = f"{base}{path}"
            try:
                resp = self.session.get(url, params=params, timeout=self.timeout)
                resp.raise_for_status()
                return resp.json()
            except Exception as exc:  # pragma: no cover - fallback between endpoints
                last_error = exc
                continue
        raise RuntimeError(f"RPC request failed: {last_error}")

    def latest_height(self) -> int:
        try:
            data = self._request("/cosmos/base/tendermint/v1beta1/blocks/latest")
            return int(data["block"]["header"]["height"])
        except Exception:
            data = self._request_rpc("/status")
            return int(data["result"]["sync_info"]["latest_block_height"])

    def block_time(self, height: int) -> datetime:
        try:
            data = self._request(f"/cosmos/base/tendermint/v1beta1/blocks/{height}")
            return _parse_dt(data["block"]["header"]["time"])
        except Exception:
            data = self._request_rpc("/block", params={"height": height})
            return _parse_dt(data["result"]["block"]["header"]["time"])

    def find_height_at_or_before(self, target: datetime) -> int:
        low = 1
        high = self.latest_height()
        best = 1
        while low <= high:
            mid = (low + high) // 2
            mid_time = self.block_time(mid)
            if mid_time <= target:
                best = mid
                low = mid + 1
            else:
                high = mid - 1
            time.sleep(self.poll_seconds)
        return best

    def find_height_at_or_after(self, target: datetime) -> int:
        low = 1
        high = self.latest_height()
        best = high
        while low <= high:
            mid = (low + high) // 2
            mid_time = self.block_time(mid)
            if mid_time >= target:
                best = mid
                high = mid - 1
            else:
                low = mid + 1
            time.sleep(self.poll_seconds)
        return best

    def search_txs(
        self,
        events: Iterable[str],
        reverse: bool,
        start_time: datetime,
        end_time: datetime,
        height_filter: bool,
    ) -> list[dict]:
        rows: list[dict] = []
        next_key: Optional[str] = None
        pages = 0
        use_height = height_filter
        height_events = list(events)

        if height_filter:
            try:
                start_height = self.find_height_at_or_after(start_time)
                end_height = self.find_height_at_or_before(end_time)
                height_events.extend(
                    [
                        f"tx.height>={start_height}",
                        f"tx.height<={end_height}",
                    ]
                )
            except Exception as exc:
                print(f"Height lookup failed ({exc}); falling back to time filter only.")
                use_height = False

        while True:
            params: list[tuple[str, str]] = [("pagination.limit", str(self.page_limit))]
            params.append(("pagination.reverse", "true" if reverse else "false"))
            if next_key:
                params.append(("pagination.key", next_key))
            for ev in height_events if use_height else events:
                params.append(("events", ev))

            try:
                data = self._request("/cosmos/tx/v1beta1/txs", params=params)
            except RuntimeError as exc:
                if use_height and "tx.height" in str(exc):
                    use_height = False
                    next_key = None
                    continue
                raise

            txs = data.get("tx_responses", [])
            if not txs:
                break

            for tx in txs:
                ts = _parse_dt(tx.get("timestamp", "1970-01-01T00:00:00Z"))
                if ts < start_time or ts > end_time:
                    if reverse and ts < start_time:
                        return rows
                    continue
                rows.append(tx)

            next_key = data.get("pagination", {}).get("next_key")
            pages += 1
            if not next_key or pages >= self.max_pages:
                break

            time.sleep(self.poll_seconds)

        return rows


def _extract_sender(tx: dict) -> Optional[str]:
    messages = tx.get("tx", {}).get("body", {}).get("messages", [])
    for msg in messages:
        sender = msg.get("sender") or msg.get("from_address")
        if sender:
            return sender
    return None


def _extract_amount(tx: dict, action: str) -> Optional[float]:
    for log in tx.get("logs", []):
        for event in log.get("events", []):
            if event.get("type") != "wasm":
                continue
            attrs = {a.get("key"): a.get("value") for a in event.get("attributes", [])}
            if attrs.get("action") != action:
                continue
            for key in ("amount", "deposit_amount", "redeem_amount"):
                val = _parse_uusd(attrs.get(key))
                if val is not None:
                    return val

    # Fallback: use coins on execute message if present
    messages = tx.get("tx", {}).get("body", {}).get("messages", [])
    for msg in messages:
        coins = msg.get("coins") or msg.get("funds") or []
        for coin in coins:
            if coin.get("denom") == "uusd":
                return float(coin.get("amount", 0)) / 1e6
    return None


def _build_rows(txs: list[dict], action: str) -> pd.DataFrame:
    records = []
    for tx in txs:
        ts = _parse_dt(tx.get("timestamp", "1970-01-01T00:00:00Z"))
        hour = ts.replace(minute=0, second=0, microsecond=0)
        sender = _extract_sender(tx)
        amount = _extract_amount(tx, action)
        if sender is None or amount is None:
            continue
        records.append({"hour": hour, "wallet": sender, "amount": amount})
    if not records:
        return pd.DataFrame(columns=["hour", "wallet", "amount"])
    df = pd.DataFrame(records)
    return df


def main() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    start = _parse_dt(WINDOW_START + "+00:00")
    end = _parse_dt(WINDOW_END + "+00:00")
    history_start = _parse_dt(HISTORY_START + "+00:00")

    client = LCDClient()

    events_base = [
        f"wasm._contract_address='{ANCHOR_MARKET_CONTRACT}'",
    ]

    print("Fetching deposits...")
    deposit_txs = client.search_txs(
        events=events_base + ["wasm.action='deposit_stable'"],
        reverse=False,
        start_time=start,
        end_time=end,
        height_filter=True,
    )
    deposits = _build_rows(deposit_txs, "deposit_stable")
    deposits = deposits.groupby(["hour", "wallet"], as_index=False)["amount"].sum()
    deposits = deposits.rename(columns={"amount": "ust_inflow"})
    deposits.to_csv(RAW_DIR / "anchor_deposits_hourly.csv", index=False)
    print("Saved anchor_deposits_hourly.csv")

    print("Fetching redeems...")
    redeem_txs = client.search_txs(
        events=events_base + ["wasm.action='redeem_stable'"],
        reverse=False,
        start_time=start,
        end_time=end,
        height_filter=True,
    )
    redeems = _build_rows(redeem_txs, "redeem_stable")
    redeems = redeems.groupby(["hour", "wallet"], as_index=False)["amount"].sum()
    redeems = redeems.rename(columns={"amount": "ust_outflow"})
    redeems.to_csv(RAW_DIR / "anchor_redeems_hourly.csv", index=False)
    print("Saved anchor_redeems_hourly.csv")

    # Anchor-based wallet activity proxy (pre-run window)
    if not deposits.empty or not redeems.empty:
        combined = pd.concat([deposits, redeems], ignore_index=True)
        combined = combined[combined["hour"] < start]
        combined = combined[combined["hour"] >= history_start]
        if not combined.empty:
            combined["date"] = combined["hour"].dt.date
            activity = (
                combined.groupby("wallet")
                .agg(tx_count=("amount", "count"), active_days=("date", "nunique"))
                .reset_index()
            )
            activity.to_csv(RAW_DIR / "wallet_activity.csv", index=False)
            print("Saved wallet_activity.csv")


if __name__ == "__main__":
    main()
