{{ config(
    materialized='table',
    engine='ReplacingMergeTree(updated_at)',
    order_by=['organisation_id', 'processing_date', 'holding_id']
) }}

SELECT
    assumeNotNull(organisation_id) AS organisation_id,
    assumeNotNull(CAST(toString(processing_date) AS Date)) AS processing_date,
    assumeNotNull(holding_id) AS holding_id,
    portfolio_id,
    security_id,
    quantity,
    market_value,
    cost_basis,
    toDate(as_of_date) AS as_of_date,
    assumeNotNull(parseDateTimeBestEffort(updated_at)) AS updated_at
FROM (
    SELECT *,
           row_number() OVER (
               PARTITION BY organisation_id, processing_date, holding_id 
               ORDER BY parseDateTimeBestEffort(updated_at) DESC
           ) as rn
    FROM {{ source('bronze', 'bronze_holdings') }}
)
WHERE rn = 1
