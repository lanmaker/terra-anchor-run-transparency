-- Wallet activity prior to run window (for maturity proxies)
-- TODO: verify table name and JSON paths in Flipside via sampling.

SELECT
  msg_sender AS wallet,
  COUNT(*) AS tx_count,
  COUNT(DISTINCT msg_value:contract::string) AS contract_count,
  COUNT(DISTINCT DATE_TRUNC('day', block_timestamp)) AS active_days
FROM terra.msgs
WHERE tx_status = 'SUCCEEDED'
  AND block_timestamp >= '2022-03-01' AND block_timestamp < '2022-04-20'
GROUP BY 1
ORDER BY 2 DESC;
