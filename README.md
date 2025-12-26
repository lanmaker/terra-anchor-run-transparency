# Terra/Anchor Run: On-Chain Transparency and Early Exit Advantage

Research-first replication study of the May 2022 Terra/Anchor run. The project asks:

- H1: Are early exiters larger and more sophisticated wallets?
- H2: Does large-wallet outflow trigger faster small-wallet outflow (transparent run dynamics)?
- H3: Do early exiters suffer systematically smaller losses?

## Data source
Primary source: Terra Classic FCD public indexer. The analysis pulls Anchor deposit and redeem events and aggregates them to hourly panels.

Main analysis window (UTC):
- 2022-04-20 00:00 to 2022-05-13 23:59

Optional feature window for wallet history:
- 2022-03-01 00:00 to 2022-05-13 23:59

## Project layout
- `docs/` research plan + data dictionary
- `src/sql/` Flipside SQL templates
- `src/etl/` build analysis-ready panels
- `src/analysis/` descriptive plots + models
- `report/` LaTeX memo
- `frontend/` static site (Plotly)

## Quick start
1) Configure FCD endpoint (optional)
- Copy `.env.example` to `.env` and set `TERRA_FCD_URL` if you want a custom endpoint

2) Fetch data (fully automated via FCD)
```
make fetch
```
Outputs:
- `data/raw/anchor_deposits_hourly.csv`
- `data/raw/anchor_redeems_hourly.csv`
- `data/raw/wallet_activity.csv`

3) Fetch UST price series
```
make prices
```
Outputs:
- `data/raw/ust_prices.csv`
Notes:
- Defaults to Binance `USTUSDT` hourly closes. If the Binance API is blocked, it falls back to the public Binance data archive.
- Override with `PRICE_SOURCES`, `BINANCE_SYMBOL`, or `BINANCE_DATA_BASE` in `.env`.

4) Build panels
```
python -m src.etl.build_panel
```
Outputs:
- `data/processed/flows_hourly.csv`
- `data/processed/wallet_hour.parquet`
- `data/processed/wallet_static.parquet`

5) Create descriptive figures
```
python -m src.analysis.descriptive
```
Outputs:
- `report/figures/fig1_net_outflow.pdf`
- `report/figures/fig2_whale_vs_rest.pdf`
- `report/figures/fig3_concentration.pdf`

6) H1/H2/H3 analysis
```
make analysis
```
Outputs:
- `report/figures/fig4_survival_size.pdf`
- `report/figures/fig5_event_study.pdf`
- `report/figures/fig6_loss_distribution.pdf`
- `report/tables/hazard_cox.tex`
- `report/tables/lag_regression.tex`
- `report/tables/loss_summary.tex`
- `report/tables/report_macros.tex`

7) Static frontend (local)
```
cp data/processed/flows_hourly.csv frontend/assets/flows_hourly.csv
```
```
python -m http.server 8000 -d frontend
```
Then open http://localhost:8000

## One-command pipeline
```
make all
```

## Performance tuning
- If requests time out, set `TERRA_REQUEST_TIMEOUT=120` and/or `TERRA_FCD_LIMIT=10` in `.env`.
- To speed up seeking, set `TERRA_FCD_START_OFFSET` (e.g., `300000000`) to jump near 2022 data.
- The FCD fetcher writes `data/interim/actions_raw.csv` and checkpoints under `data/interim/`. Re-run `make fetch` to resume. Delete those files to restart from scratch.

## Notes
- The FCD-based extractor may need minor field adjustments after sampling FCD responses.
- Data directories are git-ignored; commit only lightweight derived outputs if needed.
