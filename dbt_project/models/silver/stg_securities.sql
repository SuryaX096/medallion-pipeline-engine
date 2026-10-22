{{ config(
    materialized='table',
    engine='ReplacingMergeTree(updated_at)',
    order_by=['organisation_id', 'processing_date', 'security_id']
) }}

SELECT
    assumeNotNull(organisation_id) AS organisation_id,
    assumeNotNull(CAST(toString(processing_date) AS Date)) AS processing_date,
    assumeNotNull(security_id) AS security_id,
    ticker,
    security_name,
    asset_class,
    currency,
    assumeNotNull(parseDateTimeBestEffort(updated_at)) AS updated_at
FROM (
    SELECT *,
           row_number() OVER (
               PARTITION BY organisation_id, processing_date, security_id 
               ORDER BY parseDateTimeBestEffort(updated_at) DESC
           ) as rn
    FROM {{ source('bronze', 'bronze_securities') }}
)
WHERE rn = 1
