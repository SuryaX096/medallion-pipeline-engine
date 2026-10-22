{{ config(
    materialized='table',
    schema='gold',
    engine='SummingMergeTree((total_gross_volume, total_commissions, total_quantity, trade_count))',
    order_by=['organisation_id', 'processing_date', 'side']
) }}

SELECT
    t.organisation_id AS organisation_id,
    t.processing_date AS processing_date,
    assumeNotNull(t.side) AS side,
    round(sum(t.trade_value), 2) AS total_gross_volume,
    round(sum(t.commission), 2) AS total_commissions,
    round(sum(t.quantity), 4) AS total_quantity,
    count() AS trade_count
FROM {{ ref('stg_trades') }} t
GROUP BY
    t.organisation_id,
    t.processing_date,
    t.side
