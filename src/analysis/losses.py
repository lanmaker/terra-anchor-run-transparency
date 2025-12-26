"""H3: early exit advantage and loss distribution."""
from pathlib import Path

import numpy as np
import pandas as pd

from src.analysis.latex_utils import write_threeparttable
from src.config import EARLY_EXIT_THRESHOLD, PROCESSED_DIR, RAW_DIR, RUN_END, RUN_START


def main() -> None:
    price_path = RAW_DIR / "ust_prices.csv"
    if not price_path.exists():
        print("Missing price series at", price_path)
        return

    wallet_hour = pd.read_parquet(PROCESSED_DIR / "wallet_hour.parquet")
    wallet_static = pd.read_parquet(PROCESSED_DIR / "wallet_static.parquet")
    prices = pd.read_csv(price_path, parse_dates=["hour"])

    run_start = pd.to_datetime(RUN_START, utc=True)
    run_end = pd.to_datetime(RUN_END, utc=True)

    prices = prices.sort_values("hour")
    wallet_static = wallet_static[wallet_static["pre_run_balance"] > 0].copy()
    if wallet_static.empty:
        print("No wallets with positive pre-run balance.")
        return

    post = wallet_hour[(wallet_hour["hour"] >= run_start) & (wallet_hour["hour"] <= run_end)].copy()
    post = post.merge(prices, on="hour", how="left")
    post = post.merge(wallet_static[["wallet", "pre_run_balance"]], on="wallet", how="inner")

    # Exit timing based on cumulative outflow relative to pre-run balance
    post = post.sort_values(["wallet", "hour"])
    post["cum_outflow"] = post.groupby("wallet")["net_outflow"].cumsum()
    post["threshold"] = post["pre_run_balance"] * EARLY_EXIT_THRESHOLD
    exit_time = post.loc[post["cum_outflow"] >= post["threshold"]].groupby("wallet")["hour"].min()

    # Weighted average exit price for each wallet
    outflows = post[post["ust_outflow"] > 0].copy()
    outflows["weighted_price"] = outflows["ust_outflow"] * outflows["price"]
    agg = (
        outflows.groupby("wallet")
        .agg(
            total_outflow=("ust_outflow", "sum"),
            weighted_price=("weighted_price", "sum"),
        )
        .reset_index()
    )
    agg["avg_price"] = agg["weighted_price"] / agg["total_outflow"]

    df = wallet_static.merge(agg[["wallet", "avg_price"]], on="wallet", how="left")
    df["exit_time"] = df["wallet"].map(exit_time)
    df["exit_time"] = df["exit_time"].fillna(run_end)
    df["exit_rank"] = df["exit_time"].rank(method="first")

    # Define early vs late using quartiles among exiters
    exiters = df[df["wallet"].isin(exit_time.index)]
    if exiters.empty:
        print("No exiters found in run window.")
        return
    q1 = exiters["exit_time"].quantile(0.25)
    q3 = exiters["exit_time"].quantile(0.75)

    def _group(row: pd.Series) -> str:
        if row["exit_time"] <= q1:
            return "Early"
        if row["exit_time"] >= q3:
            return "Late"
        return "Middle"

    df["exit_group"] = df.apply(_group, axis=1)
    df = df[df["avg_price"].notna()].copy()
    df["loss_rate"] = (1 - df["avg_price"]).clip(lower=0, upper=1)

    # Summary table
    summary = df.groupby("exit_group")["loss_rate"].agg(
        count="count",
        mean="mean",
        median="median",
        std="std",
    )
    summary = summary.reset_index().rename(
        columns={
            "exit_group": "Exit group",
            "count": "N",
            "mean": "Mean loss",
            "median": "Median loss",
            "std": "Std. dev.",
        }
    )
    out_dir = Path("report/tables")
    out_dir.mkdir(parents=True, exist_ok=True)
    notes = (
        "Loss rate defined as the complement of the outflow-weighted exit price. "
        "Exit timing uses the \\earlyExitPct cumulative outflow threshold."
    )
    write_threeparttable(
        summary,
        out_dir / "loss_summary.tex",
        notes=notes,
        column_format="lrrrr",
        float_format="%.4f",
    )

    # Distribution plot
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(7, 4))
    data = [df[df["exit_group"] == g]["loss_rate"] for g in ["Early", "Middle", "Late"]]
    ax.boxplot(data, labels=["Early", "Middle", "Late"], showfliers=False)
    ax.set_title("Loss rate by exit timing")
    ax.set_ylabel("Loss rate (1 - price)")
    fig.tight_layout()

    fig_path = Path("report/figures/fig6_loss_distribution.pdf")
    fig_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(fig_path)

    print("Wrote:", out_dir / "loss_summary.tex")
    print("Saved:", fig_path)


if __name__ == "__main__":
    main()
