# Pre-analysis plan (draft)

## Research questions
H1: Are early exiters larger and more sophisticated wallets?
H2: Does large-wallet outflow lead small-wallet outflow (transparent run dynamics)?
H3: Do early exiters experience smaller losses?

## Sample
- Chain: Terra Classic
- Protocol: Anchor (market + aUST)
- Main window (UTC): 2022-04-20 00:00 to 2022-05-13 23:59
- Optional history window for wallet maturity features: 2022-03-01 00:00 to 2022-05-13 23:59
- Frequency: hourly

## Definitions
- Deposit: Anchor market `deposit_stable`
- Redeem/withdraw: aUST `send` with embedded `redeem_stable`
- Early exit: first hour in which cumulative net outflow >= 50% of pre-run balance
- Whale: top 1% wallets by pre-run balance (robustness: top 0.1%, top 5%)

## Identification strategy
- H1: survival/hazard model for time-to-exit using wallet size and anchor-activity proxies
- H2: time-series regression and event study of small-wallet outflow on lagged whale outflow, controlling for peg deviation
- H3: compare loss distributions by exit timing and wallet type

## Planned robustness
- Alternative exit thresholds: 30% / 80%
- Alternative whale definitions
- Exclude exchange/bridge/contract addresses
- Alternative frequency (15-min) if data permits
- Placebo events outside the run window
