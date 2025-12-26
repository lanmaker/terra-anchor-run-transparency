"""H1: early exit and wallet characteristics (hazard model)."""
from pathlib import Path

import numpy as np
import pandas as pd
from lifelines import CoxPHFitter, KaplanMeierFitter

from src.analysis.latex_utils import write_threeparttable
from src.config import EARLY_EXIT_THRESHOLD, PROCESSED_DIR, RUN_START


def main() -> None:
    wallet_hour = pd.read_parquet(PROCESSED_DIR / "wallet_hour.parquet")
    wallet_static = pd.read_parquet(PROCESSED_DIR / "wallet_static.parquet")

    run_start = pd.to_datetime(RUN_START, utc=True)
    post = wallet_hour[wallet_hour["hour"] >= run_start].copy()

    static = wallet_static.copy()
    static = static[static["pre_run_balance"] > 0].copy()
    if static.empty or post.empty:
        print("No data available for hazard model.")
        return

    for col in ["tx_count", "active_days"]:
        if col not in static.columns:
            static[col] = 0
        static[col] = static[col].fillna(0)

    static["log_balance"] = np.log1p(static["pre_run_balance"])
    static["log_tx"] = np.log1p(static["tx_count"])
    static["log_active"] = np.log1p(static["active_days"])

    post = post.merge(
        static[["wallet", "pre_run_balance"]],
        on="wallet",
        how="inner",
    )
    post = post.sort_values(["wallet", "hour"])
    post["cum_outflow"] = post.groupby("wallet")["net_outflow"].cumsum()
    post["threshold"] = post["pre_run_balance"] * EARLY_EXIT_THRESHOLD
    post["exit_flag"] = post["cum_outflow"] >= post["threshold"]

    exit_time = post.loc[post["exit_flag"]].groupby("wallet")["hour"].min()
    max_hour = post["hour"].max()

    df = static[["wallet", "log_balance", "log_tx", "log_active"]].copy()
    df["exit_time"] = df["wallet"].map(exit_time)
    df["event"] = df["exit_time"].notna().astype(int)
    df["exit_time"] = df["exit_time"].fillna(max_hour)
    df["duration"] = (df["exit_time"] - run_start).dt.total_seconds() / 3600.0
    df = df[df["duration"] >= 0].copy()

    covars = ["log_balance", "log_tx", "log_active"]
    var = df[covars].var()
    covars = [c for c in covars if var[c] > 1e-10]
    if not covars:
        print("No covariates with variance for hazard model.")
        return

    cph = CoxPHFitter(penalizer=0.1)
    cph.fit(
        df[["duration", "event"] + covars],
        duration_col="duration",
        event_col="event",
    )

    out_dir = Path("report/tables")
    out_dir.mkdir(parents=True, exist_ok=True)
    summary = cph.summary.loc[covars, ["coef", "exp(coef)", "se(coef)", "p"]].copy()
    summary = summary.reset_index().rename(
        columns={
            "covariate": "Variable",
            "coef": "Coef.",
            "exp(coef)": "Hazard Ratio",
            "se(coef)": "Std. Err.",
            "p": "P-value",
        }
    )
    label_map = {
        "log_balance": "Log pre-run balance",
        "log_tx": "Log tx count",
        "log_active": "Log active days",
    }
    summary["Variable"] = summary["Variable"].map(label_map).fillna(summary["Variable"])
    notes = (
        "Cox proportional hazards model. Exit defined as cumulative net outflow exceeding "
        "\\earlyExitPct of pre-run balance."
    )
    write_threeparttable(
        summary,
        out_dir / "hazard_cox.tex",
        notes=notes,
        column_format="lrrrr",
        float_format="%.4f",
    )

    # Kaplan-Meier survival by size quartiles
    static["size_group"] = pd.qcut(static["pre_run_balance"], 4, labels=["Q1", "Q2", "Q3", "Q4"])
    df = df.merge(static[["wallet", "size_group"]], on="wallet", how="left")

    kmf = KaplanMeierFitter()
    fig_path = Path("report/figures/fig4_survival_size.pdf")
    fig_path.parent.mkdir(parents=True, exist_ok=True)

    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(8, 4))
    for label in ["Q1", "Q4"]:
        sub = df[df["size_group"] == label]
        if sub.empty:
            continue
        kmf.fit(sub["duration"], event_observed=sub["event"], label=f"Size {label}")
        kmf.plot_survival_function(ax=ax)

    ax.set_title("Survival (time-to-exit) by size quartile")
    ax.set_xlabel("Hours since run start")
    ax.set_ylabel("Survival probability")
    fig.tight_layout()
    fig.savefig(fig_path)

    print("Wrote:", out_dir / "hazard_cox.tex")
    print("Saved:", fig_path)


if __name__ == "__main__":
    main()
