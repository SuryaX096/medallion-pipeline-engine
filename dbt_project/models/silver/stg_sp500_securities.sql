{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by=['symbol']
) }}

SELECT
    symbol,
    security_name,
    gics_sector,
    gics_sub_industry,
    headquarters,
    date_added,
    cik,
    founded,
    ingested_at
FROM {{ source('bronze', 'bronze_sp500_constituents') }}
