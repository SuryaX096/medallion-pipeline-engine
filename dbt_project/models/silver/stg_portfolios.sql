{{ config(
    materialized='table',
    engine='ReplacingMergeTree(updated_at)',
    order_by=['organisation_id', 'processing_date', 'portfolio_id']
) }}

SELECT
    assumeNotNull(organisation_id) AS organisation_id,
    assumeNotNull(CAST(toString(processing_date) AS Date)) AS processing_date,
    assumeNotNull(portfolio_id) AS portfolio_id,
    account_id,
    portfolio_name,
    strategy,
    risk_profile,
    assumeNotNull(parseDateTimeBestEffort(updated_at)) AS updated_at
FROM (
    SELECT *,
           row_number() OVER (
               PARTITION BY organisation_id, processing_date, portfolio_id 
               ORDER BY parseDateTimeBestEffort(updated_at) DESC
           ) as rn
    FROM {{ source('bronze', 'bronze_portfolios') }}
)
WHERE rn = 1
