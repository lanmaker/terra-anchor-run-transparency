import pandas as pd
import matplotlib.pyplot as plt

from src.config import PROCESSED_DIR


def main() -> None:
    flows_path = PROCESSED_DIR / "flows_hourly.csv"
    flows = pd.read_csv(flows_path, parse_dates=["hour"])

    # Figure 1: Net outflow
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(flows["hour"], flows["net_outflow"], color="black", linewidth=1)
    ax.set_title("Anchor net outflow (hourly)")
    ax.set_xlabel("Hour")
    ax.set_ylabel("UST")
    fig.tight_layout()
    fig.savefig("report/figures/fig1_net_outflow.pdf")

    # Figure 2: Whale vs rest
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(flows["hour"], flows["whale_outflow"], label="Whale outflow", color="#1f77b4")
    ax.plot(flows["hour"], flows["small_outflow"], label="Small outflow", color="#ff7f0e")
    ax.set_title("Whale vs small outflow (hourly)")
    ax.set_xlabel("Hour")
    ax.set_ylabel("UST")
    ax.legend(frameon=False)
    fig.tight_layout()
    fig.savefig("report/figures/fig2_whale_vs_rest.pdf")

    # Figure 3: Concentration
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(flows["hour"], flows["hhi"], color="#2ca02c", linewidth=1)
    ax.set_title("Outflow concentration (HHI)")
    ax.set_xlabel("Hour")
    ax.set_ylabel("HHI")
    fig.tight_layout()
    fig.savefig("report/figures/fig3_concentration.pdf")

    print("Saved figures to report/figures")


if __name__ == "__main__":
    main()
