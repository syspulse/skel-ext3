WITH token_contract_addresses (address) AS (
    VALUES (TRY_CAST('{{token_contract}}' AS VARCHAR))
)

, target_date AS (
    SELECT TRY_CAST('{{valuation_date}}' AS TIMESTAMP) AS valuation_date
)

, balance AS (
    SELECT
        b.day
        , tc.address
        , token_balance_owner
        , token_balance AS qty
    FROM solana_utils.daily_balances b
    INNER JOIN token_contract_addresses tc ON b.token_mint_address = tc.address
    WHERE b.day <= (SELECT valuation_date FROM target_date)
)

, date_series AS (
    SELECT DISTINCT timestamp AS date
    FROM utils.days
    WHERE timestamp >= TRY_CAST((SELECT MIN(day) FROM balance) AS DATE)
    AND timestamp <= (SELECT valuation_date FROM target_date)
)

, all_symbols AS (
    SELECT DISTINCT
    token_balance_owner
  FROM balance
)

, daily_snapshot AS (
    SELECT  ds.date AS block_date
            , alls.token_balance_owner
            , (COALESCE(b.qty, LAST_VALUE(b.qty) IGNORE NULLS OVER (PARTITION BY alls.token_balance_owner ORDER BY ds.date ASC)) ) AS balance
    FROM date_series ds
    CROSS JOIN all_symbols alls
    LEFT JOIN balance b ON ds.date = b.day AND alls.token_balance_owner = b.token_balance_owner
)

SELECT
        block_date
        , token_balance_owner AS address
        , CONCAT('<a href="https://solscan.io/account/', token_balance_owner, '" target="_blank">', token_balance_owner, '</a>') AS solscan
        , CAST(balance AS DOUBLE) AS balance
        , balance / (SUM(balance) OVER ()) AS perc
        , CASE 
        WHEN balance / (SUM(balance) OVER ()) >= 0.005 THEN token_balance_owner
        ELSE 'Others' 
    END AS tag
FROM daily_snapshot
WHERE block_date = (SELECT valuation_date FROM target_date)
AND balance > 0
ORDER BY block_date DESC, balance DESC

