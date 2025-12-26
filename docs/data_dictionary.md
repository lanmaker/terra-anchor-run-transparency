# Data dictionary (draft)

This file documents the fields used after confirming schemas from FCD responses.

## Raw extracts (FCD)

### anchor_deposits_hourly.csv
- hour: UTC hour bucket
- wallet: depositor address
- ust_inflow: UST amount deposited (in units of UST)

### anchor_redeems_hourly.csv
- hour: UTC hour bucket
- wallet: redeemer address
- ust_outflow: UST amount redeemed (in units of UST)

### wallet_activity.csv
- wallet
- tx_count: anchor-related tx count (pre-run window)
- active_days: unique active days (pre-run window)

## Processed datasets

### flows_hourly.csv
- hour
- ust_inflow
- ust_outflow
- net_outflow = ust_outflow - ust_inflow
- whale_outflow
- small_outflow
- top_share (Top 1% share)
- hhi (Herfindahl index of outflow shares)

### wallet_hour.parquet
- wallet
- hour
- ust_inflow
- ust_outflow
- net_outflow
- pre_run_balance
- is_exit (exit indicator)

### wallet_static.parquet
- wallet
- pre_run_balance
- size_quantile
- tx_count
- active_days
