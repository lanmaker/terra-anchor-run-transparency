"""H2: information diffusion (whale outflow -> small outflow)."""
from pathlib import Path

import numpy as np
import pandas as pd
import statsmodels.api as sm

from src.analysis.latex_utils import write_threeparttable
from src.config import EVENT_WINDOW, LAG_MAX, PROCESSED_DIR, WHALE_EVENT_Q


def main() -> None:
    flows = pd.read_csv(PROCESSED_DIR / "flows_hourly.csv", parse_dates=["hour"])
    flows = flows.sort_values("hour")

    if flows.empty:
        print("No flows data available.")
        return

    # Event study: whale outflow in top 1% of hours
    threshold = flows["whale_outflow"].quantile(WHALE_EVENT_Q)
    events = flows.loc[flows["whale_outflow"] >= threshold, "hour"].tolist()

    window = range(-EVENT_WINDOW, EVENT_WINDOW + 1)
    idx = flows.set_index("hour")
    records = []
    for event_time in events:
        for k in window:
            t = event_time + pd.Timedelta(hours=k)
            if t not in idx.index:
                continue
            records.append({"k": k, "small_outflow": idx.at[t, "small_outflow"]})

    event_df = pd.DataFrame(records)
    if not event_df.empty:
        stats = (
            event_df.groupby("k")["small_outflow"]
            .agg(["mean", "std", "count"])
            .reset_index()
        )
        stats["se"] = stats["std"] / np.sqrt(stats["count"])

        import matplotlib.pyplot as plt

        fig, ax = plt.subplots(figsize=(8, 4))
        ax.plot(stats["k"], stats["mean"], color="#1f6f8b")
        ax.fill_between(
            stats["k"],
            stats["mean"] - 1.96 * stats["se"],
            stats["mean"] + 1.96 * stats["se"],
            color="#1f6f8b",
            alpha=0.2,
        )
        ax.axvline(0, color="black", linestyle="--", linewidth=1)
        ax.set_title("Event study: small outflow around whale events")
        ax.set_xlabel("Hours relative to whale event")
        ax.set_ylabel("Small outflow (UST)")
        fig.tight_layout()

        fig_path = Path("report/figures/fig5_event_study.pdf")
        fig_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(fig_path)
        print("Saved:", fig_path)

    # Lag regression: small_outflow on whale_outflow lags
    df = flows.copy()
    for lag in range(1, LAG_MAX + 1):
        df[f"whale_lag{lag}"] = df["whale_outflow"].shift(lag)
    df = df.dropna()

    if df.empty:
        print("Not enough data for lag regression.")
        return

    X = df[[f"whale_lag{lag}" for lag in range(1, LAG_MAX + 1)]]
    X = sm.add_constant(X)
    model = sm.OLS(df["small_outflow"], X).fit(
        cov_type="HAC", cov_kwds={"maxlags": LAG_MAX}
    )

    out_dir = Path("report/tables")
    out_dir.mkdir(parents=True, exist_ok=True)
    summary = model.summary2().tables[1].copy()
    summary = summary.rename(
        columns={
            "Coef.": "Coef.",
            "Std.Err.": "Std. Err.",
            "P>|z|": "P-value",
            "[0.025": "CI 2.5\\%",
            "0.975]": "CI 97.5\\%",
        }
    )
    summary.index.name = "Variable"
    summary = summary.reset_index()

    def _label(var: str) -> str:
        if var == "const":
            return "Constant"
        if var.startswith("whale_lag"):
            lag = var.replace("whale_lag", "")
            if lag.isdigit():
                return f"Whale outflow (t-{lag})"
        return var

    summary["Variable"] = summary["Variable"].apply(_label)
    notes = (
        "HAC standard errors with maxlags = \\lagMax. Whale events defined as the "
        "top \\whaleEventPct of hours by whale outflow."
    )
    write_threeparttable(
        summary,
        out_dir / "lag_regression.tex",
        notes=notes,
        column_format="lrrrrrr",
        float_format="%.4f",
    )
    print("Wrote:", out_dir / "lag_regression.tex")


if __name__ == "__main__":
    main()
