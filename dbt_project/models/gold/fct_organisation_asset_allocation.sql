{{ config(
    materialized='table',
    schema='gold',
    engine='SummingMergeTree((total_allocation_value, asset_count))',
    order_by=['organisation_id', 'processing_date', 'asset_class']
) }}

SELECT
    e.organisation_id AS organisation_id,
    e.processing_date AS processing_date,
    e.asset_class AS asset_class,
    round(sum(e.market_value), 2) AS total_allocation_value,
    count() AS asset_count
FROM {{ ref('int_portfolio_holdings_enriched') }} e
GROUP BY
    e.organisation_id,
    e.processing_date,
    e.asset_class
