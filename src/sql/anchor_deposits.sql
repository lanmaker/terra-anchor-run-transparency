-- Anchor market deposits (wallet-hour)
-- TODO: verify table name and JSON paths in Flipside via sampling.

WITH base AS (
  SELECT
    DATE_TRUNC('hour', block_timestamp) AS hour,
    msg_sender AS wallet,
    TRY_TO_NUMBER(event_attributes:deposit_amount::string) / 1e6 AS ust_inflow
  FROM terra.msgs
  WHERE tx_status = 'SUCCEEDED'
    AND block_timestamp >= '2022-04-20' AND block_timestamp < '2022-05-14'
    AND msg_value:contract::string = 'terra1sepfj7s0aeg5967uxnfk4thzlerrsktkpelm5s'
    AND msg_value:execute_msg:deposit_stable IS NOT NULL
)
SELECT
  hour,
  wallet,
  SUM(ust_inflow) AS ust_inflow
FROM base
GROUP BY 1, 2
ORDER BY 1, 2;
