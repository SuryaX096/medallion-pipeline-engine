{{ config(
    materialized='table',
    engine='ReplacingMergeTree(updated_at)',
    order_by=['organisation_id', 'processing_date', 'account_id']
) }}

SELECT
    assumeNotNull(organisation_id) AS organisation_id,
    assumeNotNull(CAST(toString(processing_date) AS Date)) AS processing_date,
    assumeNotNull(account_id) AS account_id,
    account_name,
    status,
    currency,
    advisor_id,
    assumeNotNull(parseDateTimeBestEffort(opened_at)) AS opened_at,
    assumeNotNull(parseDateTimeBestEffort(updated_at)) AS updated_at
FROM (
    SELECT *,
           row_number() OVER (
               PARTITION BY organisation_id, processing_date, account_id 
               ORDER BY parseDateTimeBestEffort(updated_at) DESC
           ) as rn
    FROM {{ source('bronze', 'bronze_accounts') }}
)
WHERE rn = 1
