-- Anchor redeem (wallet-hour) via aUST send + redeem_stable
-- TODO: verify table name and JSON paths in Flipside via sampling.

WITH base AS (
  SELECT
    DATE_TRUNC('hour', block_timestamp) AS hour,
    msg_sender AS wallet,
    TRY_TO_NUMBER(msg_value:execute_msg:send:amount::string) / 1e6 AS aust_sent,
    TRY_TO_NUMBER(event_attributes:redeem_amount::string) / 1e6 AS ust_outflow
  FROM terra.msgs
  WHERE tx_status = 'SUCCEEDED'
    AND block_timestamp >= '2022-04-20' AND block_timestamp < '2022-05-14'
    AND msg_value:contract::string = 'terra1hzh9vpxhsk8253se0vv5jj6etdvxu3nv8z07zu'
    AND msg_value:execute_msg:send:msg:redeem_stable IS NOT NULL
)
SELECT
  hour,
  wallet,
  SUM(ust_outflow) AS ust_outflow,
  SUM(aust_sent) AS aust_sent
FROM base
GROUP BY 1, 2
ORDER BY 1, 2;
