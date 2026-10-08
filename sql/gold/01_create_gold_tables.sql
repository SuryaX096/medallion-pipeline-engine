-- ============================================================================
-- GOLD LAYER DDL & MULTI-TENANT AGGREGATION METRICS
-- Engine: SummingMergeTree
-- Primary Keys: Must include `organisation_id` as first grouping component
-- Guard: Strict organisation_id grouping preventing cross-tenant leakage
-- ============================================================================

CREATE DATABASE IF NOT EXISTS gold;

-- 1. Gold Daily Organisation AUM
CREATE TABLE IF NOT EXISTS gold.gold_daily_organisation_aum (
    organisation_id LowCardinality(String),
    processing_date Date,
    total_market_value Float64,
    total_cost_basis Float64,
    position_count UInt64
) ENGINE = SummingMergeTree((total_market_value, total_cost_basis, position_count))
PARTITION BY toYYYYMM(processing_date)
ORDER BY (organisation_id, processing_date);

-- Daily Rollup Materialization Query
INSERT INTO gold.gold_daily_organisation_aum
SELECT
    h.organisation_id AS organisation_id,
    h.processing_date AS processing_date,
    round(sum(h.market_value), 2) AS total_market_value,
    round(sum(h.cost_basis), 2) AS total_cost_basis,
    count() AS position_count
FROM silver.silver_holdings h
GROUP BY
    h.organisation_id,
    h.processing_date;


-- 2. Gold Organisation Asset Allocation
CREATE TABLE IF NOT EXISTS gold.gold_organisation_asset_allocation (
    organisation_id LowCardinality(String),
    processing_date Date,
    asset_class LowCardinality(String),
    total_allocation_value Float64,
    asset_count UInt64
) ENGINE = SummingMergeTree((total_allocation_value, asset_count))
PARTITION BY toYYYYMM(processing_date)
ORDER BY (organisation_id, processing_date, asset_class);

-- Asset Allocation Materialization Query
INSERT INTO gold.gold_organisation_asset_allocation
SELECT
    e.organisation_id AS organisation_id,
    e.processing_date AS processing_date,
    e.asset_class AS asset_class,
    round(sum(e.market_value), 2) AS total_allocation_value,
    count() AS asset_count
FROM silver.silver_portfolio_holdings_enriched e
GROUP BY
    e.organisation_id,
    e.processing_date,
    e.asset_class;


-- 3. Gold Organisation Trading Volume
CREATE TABLE IF NOT EXISTS gold.gold_organisation_trading_volume (
    organisation_id LowCardinality(String),
    processing_date Date,
    side LowCardinality(String),
    total_gross_volume Float64,
    total_commissions Float64,
    total_quantity Float64,
    trade_count UInt64
) ENGINE = SummingMergeTree((total_gross_volume, total_commissions, total_quantity, trade_count))
PARTITION BY toYYYYMM(processing_date)
ORDER BY (organisation_id, processing_date, side);

-- Trading Volume Materialization Query
INSERT INTO gold.gold_organisation_trading_volume
SELECT
    t.organisation_id AS organisation_id,
    t.processing_date AS processing_date,
    t.side AS side,
    round(sum(t.trade_value), 2) AS total_gross_volume,
    round(sum(t.commission), 2) AS total_commissions,
    round(sum(t.quantity), 4) AS total_quantity,
    count() AS trade_count
FROM silver.silver_trades t
GROUP BY
    t.organisation_id,
    t.processing_date,
    t.side;
