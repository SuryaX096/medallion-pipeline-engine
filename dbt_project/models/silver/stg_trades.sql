{{ config(
    materialized='table',
    engine='ReplacingMergeTree(updated_at)',
    order_by=['organisation_id', 'processing_date', 'trade_id']
) }}

SELECT
    assumeNotNull(organisation_id) AS organisation_id,
    assumeNotNull(CAST(toString(processing_date) AS Date)) AS processing_date,
    assumeNotNull(trade_id) AS trade_id,
    portfolio_id,
    security_id,
    side,
    quantity,
    price,
    trade_value,
    commission,
    parseDateTimeBestEffort(trade_timestamp) AS trade_timestamp,
    assumeNotNull(parseDateTimeBestEffort(updated_at)) AS updated_at
FROM (
    SELECT *,
           row_number() OVER (
               PARTITION BY organisation_id, processing_date, trade_id 
               ORDER BY parseDateTimeBestEffort(updated_at) DESC
           ) as rn
    FROM {{ source('bronze', 'bronze_trades') }}
)
WHERE rn = 1
