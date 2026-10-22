{{ config(
    materialized='view'
) }}

SELECT
    h.organisation_id AS organisation_id,
    h.processing_date AS processing_date,
    h.holding_id AS holding_id,
    h.portfolio_id AS portfolio_id,
    p.portfolio_name AS portfolio_name,
    p.strategy AS strategy,
    p.risk_profile AS risk_profile,
    p.account_id AS account_id,
    a.account_name AS account_name,
    a.status AS account_status,
    h.security_id AS security_id,
    assumeNotNull(ifNull(s.ticker, 'UNKNOWN')) AS ticker,
    assumeNotNull(ifNull(s.security_name, 'UNKNOWN')) AS security_name,
    assumeNotNull(ifNull(s.asset_class, 'UNKNOWN')) AS asset_class,
    h.quantity AS quantity,
    h.market_value AS market_value,
    h.cost_basis AS cost_basis,
    h.as_of_date AS as_of_date,
    h.updated_at AS updated_at
FROM {{ ref('stg_holdings') }} h
INNER JOIN {{ ref('stg_portfolios') }} p 
    ON h.portfolio_id = p.portfolio_id 
   AND h.organisation_id = p.organisation_id
INNER JOIN {{ ref('stg_accounts') }} a 
    ON p.account_id = a.account_id 
   AND p.organisation_id = a.organisation_id
LEFT JOIN {{ ref('stg_securities') }} s 
    ON h.security_id = s.security_id 
   AND h.organisation_id = s.organisation_id
