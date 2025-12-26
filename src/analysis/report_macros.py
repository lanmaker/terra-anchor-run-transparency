from datetime import datetime
from pathlib import Path

import pandas as pd

from src.config import (
    EARLY_EXIT_THRESHOLD,
    EVENT_WINDOW,
    LAG_MAX,
    PROCESSED_DIR,
    RUN_END,
    RUN_START,
    WHALE_EVENT_Q,
    WHALE_TOP_PCT,
    WINDOW_END,
    WINDOW_START,
)


def _format_date(dt_str: str) -> str:
    return datetime.fromisoformat(dt_str).strftime("%Y-%m-%d")


def _format_month(dt_str: str) -> str:
    return datetime.fromisoformat(dt_str).strftime("%B %Y")


def _format_pct(value: float) -> str:
    pct = value * 100
    if abs(pct - round(pct)) < 1e-6:
        return f"{pct:.0f}\\%"
    return f"{pct:.1f}\\%"


def _format_int(value: int) -> str:
    return f"{int(value)}"


def main() -> None:
    macros: dict[str, str] = {}

    macros["sampleStart"] = _format_date(WINDOW_START)
    macros["sampleEnd"] = _format_date(WINDOW_END)
    macros["runStart"] = _format_date(RUN_START)
    macros["runEnd"] = _format_date(RUN_END)
    macros["runMonth"] = _format_month(RUN_START)
    macros["sampleWindow"] = f"{macros['sampleStart']}--{macros['sampleEnd']}"
    macros["runWindow"] = f"{macros['runStart']}--{macros['runEnd']}"
    macros["earlyExitPct"] = _format_pct(EARLY_EXIT_THRESHOLD)
    macros["whalePct"] = _format_pct(WHALE_TOP_PCT)
    macros["whaleEventPct"] = _format_pct(1 - WHALE_EVENT_Q)
    macros["lagMax"] = _format_int(LAG_MAX)
    macros["eventWindow"] = _format_int(EVENT_WINDOW)

    wallet_static_path = PROCESSED_DIR / "wallet_static.parquet"
    if wallet_static_path.exists():
        wallet_static = pd.read_parquet(wallet_static_path)
        if "pre_run_balance" in wallet_static.columns:
            wallet_static = wallet_static[wallet_static["pre_run_balance"] > 0]
        macros["nWallets"] = _format_int(len(wallet_static))
        if "is_whale" in wallet_static.columns:
            macros["nWhales"] = _format_int(wallet_static["is_whale"].sum())

    flows_path = PROCESSED_DIR / "flows_hourly.csv"
    if flows_path.exists():
        flows = pd.read_csv(flows_path, parse_dates=["hour"])
        macros["nHours"] = _format_int(len(flows))
        if not flows.empty and "whale_outflow" in flows.columns:
            threshold = flows["whale_outflow"].quantile(WHALE_EVENT_Q)
            macros["nWhaleEvents"] = _format_int((flows["whale_outflow"] >= threshold).sum())

    wallet_hour_path = PROCESSED_DIR / "wallet_hour.parquet"
    if wallet_hour_path.exists() and wallet_static_path.exists():
        wallet_hour = pd.read_parquet(wallet_hour_path)
        wallet_static = pd.read_parquet(wallet_static_path)
        wallet_static = wallet_static[wallet_static["pre_run_balance"] > 0]
        if not wallet_static.empty:
            post = wallet_hour.copy()
            post["hour"] = pd.to_datetime(post["hour"], utc=True)
            run_start = pd.to_datetime(RUN_START, utc=True)
            post = post[post["hour"] >= run_start]
            post = post.merge(
                wallet_static[["wallet", "pre_run_balance"]],
                on="wallet",
                how="inner",
            )
            post = post.sort_values(["wallet", "hour"])
            post["cum_outflow"] = post.groupby("wallet")["net_outflow"].cumsum()
            post["threshold"] = post["pre_run_balance"] * EARLY_EXIT_THRESHOLD
            exiters = post.loc[post["cum_outflow"] >= post["threshold"], "wallet"].nunique()
            macros["nExiters"] = _format_int(exiters)

    defaults = {
        "nWallets": "NA",
        "nWhales": "NA",
        "nHours": "NA",
        "nWhaleEvents": "NA",
        "nExiters": "NA",
    }
    for key, value in defaults.items():
        macros.setdefault(key, value)

    out_dir = Path("report/tables")
    out_dir.mkdir(parents=True, exist_ok=True)
    macro_path = out_dir / "report_macros.tex"
    lines = ["% Auto-generated file. Do not edit directly."]
    for key in sorted(macros):
        lines.append(f"\\newcommand{{\\{key}}}{{{macros[key]}}}")
    macro_path.write_text("\n".join(lines) + "\n")
    print("Wrote:", macro_path)


if __name__ == "__main__":
    main()
