import pandas as pd

from src.config import (
    RAW_DIR,
    PROCESSED_DIR,
    RUN_START,
    WINDOW_START,
    WINDOW_END,
    WHALE_TOP_PCT,
)


def _load_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    if "hour" in df.columns:
        df["hour"] = pd.to_datetime(df["hour"], utc=True)
    return df


def main() -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    deposits_path = RAW_DIR / "anchor_deposits_hourly.csv"
    redeems_path = RAW_DIR / "anchor_redeems_hourly.csv"

    deposits = _load_csv(deposits_path)
    redeems = _load_csv(redeems_path)

    # Normalize columns
    deposits = deposits.rename(columns={"ust_inflow": "ust_inflow"})
    redeems = redeems.rename(columns={"ust_outflow": "ust_outflow"})

    wallet_hour = deposits.merge(
        redeems, on=["hour", "wallet"], how="outer"
    ).fillna(0)

    # If UST outflow is missing but aUST is present, use aUST as proxy.
    if "ust_outflow" in wallet_hour.columns and "aust_sent" in wallet_hour.columns:
        wallet_hour["ust_outflow"] = wallet_hour["ust_outflow"].where(
            wallet_hour["ust_outflow"] > 0, wallet_hour["aust_sent"]
        )

    wallet_hour["net_outflow"] = wallet_hour["ust_outflow"] - wallet_hour["ust_inflow"]
    wallet_hour["net_inflow"] = wallet_hour["ust_inflow"] - wallet_hour["ust_outflow"]

    # Pre-run balance proxy: cumulative net inflow before RUN_START
    run_start = pd.to_datetime(RUN_START, utc=True)
    pre_run = wallet_hour[wallet_hour["hour"] < run_start].copy()
    pre_bal = (
        pre_run.groupby("wallet")["net_inflow"].sum().rename("pre_run_balance")
    )
    pre_bal = pre_bal.clip(lower=0)

    wallet_static = pre_bal.reset_index()
    if not wallet_static.empty:
        wallet_static["size_quantile"] = pd.qcut(
            wallet_static["pre_run_balance"].rank(method="first"),
            10,
            labels=False,
        )

    # Optional maturity features (if provided)
    activity_path = RAW_DIR / "wallet_activity.csv"
    if activity_path.exists():
        activity = _load_csv(activity_path)
        wallet_static = wallet_static.merge(activity, on="wallet", how="left")

    # Whale classification by pre-run balance
    if not wallet_static.empty:
        positive_bal = wallet_static[wallet_static["pre_run_balance"] > 0]
        if not positive_bal.empty:
            cutoff = positive_bal["pre_run_balance"].quantile(1 - WHALE_TOP_PCT)
            wallet_static["is_whale"] = wallet_static["pre_run_balance"] >= cutoff
        else:
            wallet_static["is_whale"] = False
    else:
        wallet_static["is_whale"] = False

    # Hourly aggregates
    flows = (
        wallet_hour.groupby("hour")
        .agg(
            ust_inflow=("ust_inflow", "sum"),
            ust_outflow=("ust_outflow", "sum"),
        )
        .reset_index()
    )
    flows["net_outflow"] = flows["ust_outflow"] - flows["ust_inflow"]

    # Whale vs small outflow
    wallet_hour = wallet_hour.merge(
        wallet_static[["wallet", "is_whale"]], on="wallet", how="left"
    )
    wallet_hour["is_whale"] = wallet_hour["is_whale"].fillna(False)
    whale_outflow = (
        wallet_hour[wallet_hour["is_whale"]]
        .groupby("hour")["ust_outflow"]
        .sum()
        .rename("whale_outflow")
    )
    small_outflow = (
        wallet_hour[~wallet_hour["is_whale"]]
        .groupby("hour")["ust_outflow"]
        .sum()
        .rename("small_outflow")
    )
    flows = flows.merge(whale_outflow, on="hour", how="left").merge(
        small_outflow, on="hour", how="left"
    )
    flows = flows.fillna(0)

    # Concentration (HHI) using outflow shares within hour
    def _hhi(x: pd.Series) -> float:
        total = x.sum()
        if total == 0:
            return 0.0
        share = x / total
        return float((share * share).sum())

    hhi = wallet_hour.groupby("hour")["ust_outflow"].apply(_hhi).rename("hhi")
    flows = flows.merge(hhi, on="hour", how="left")
    flows["top_share"] = flows["whale_outflow"] / flows["ust_outflow"].replace(0, pd.NA)

    # Persist
    wallet_hour.to_parquet(PROCESSED_DIR / "wallet_hour.parquet", index=False)
    wallet_static.to_parquet(PROCESSED_DIR / "wallet_static.parquet", index=False)
    flows.to_csv(PROCESSED_DIR / "flows_hourly.csv", index=False)

    print("Wrote:")
    print(" -", PROCESSED_DIR / "wallet_hour.parquet")
    print(" -", PROCESSED_DIR / "wallet_static.parquet")
    print(" -", PROCESSED_DIR / "flows_hourly.csv")


if __name__ == "__main__":
    main()
