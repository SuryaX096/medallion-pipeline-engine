{{ config(
    materialized='table',
    schema='gold',
    engine='SummingMergeTree((total_market_value, total_cost_basis, position_count))',
    order_by=['organisation_id', 'processing_date']
) }}

SELECT
    h.organisation_id AS organisation_id,
    h.processing_date AS processing_date,
    round(sum(h.market_value), 2) AS total_market_value,
    round(sum(h.cost_basis), 2) AS total_cost_basis,
    count() AS position_count
FROM {{ ref('stg_holdings') }} h
GROUP BY
    h.organisation_id,
    h.processing_date
