-- ============================================================================
-- SILVER LAYER: ENRICHED RELATIONAL JOIN VIEW
-- View: silver.silver_portfolio_holdings_enriched
-- 3NF Normalization: Joins Holdings -> Portfolios -> Accounts -> Securities
-- Tenant Isolation: Strictly enforces `organisation_id` in EVERY join predicate
-- ============================================================================

CREATE OR REPLACE VIEW silver.silver_portfolio_holdings_enriched AS
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
    s.ticker AS ticker,
    s.security_name AS security_name,
    s.asset_class AS asset_class,
    h.quantity AS quantity,
    h.market_value AS market_value,
    h.cost_basis AS cost_basis,
    h.as_of_date AS as_of_date,
    h.updated_at AS updated_at
FROM silver.silver_holdings h
INNER JOIN silver.silver_portfolios p 
    ON h.portfolio_id = p.portfolio_id 
   AND h.organisation_id = p.organisation_id
INNER JOIN silver.silver_accounts a 
    ON p.account_id = a.account_id 
   AND p.organisation_id = a.organisation_id
LEFT JOIN silver.silver_securities s 
    ON h.security_id = s.security_id 
   AND h.organisation_id = s.organisation_id;
