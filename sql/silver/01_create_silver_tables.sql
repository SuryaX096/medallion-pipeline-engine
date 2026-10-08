-- ============================================================================
-- SILVER LAYER DDL & DEDUPLICATION / NORMALIZATION PIPELINES
-- Engine: ReplacingMergeTree(updated_at)
-- Primary Keys: (organisation_id, processing_date, <business_key>)
-- Multi-Tenancy: Strict organisation_id composite keys
-- ============================================================================

CREATE DATABASE IF NOT EXISTS silver;

-- 1. Silver Accounts
CREATE TABLE IF NOT EXISTS silver.silver_accounts (
    organisation_id LowCardinality(String),
    processing_date Date,
    account_id String,
    account_name String,
    status LowCardinality(String),
    currency LowCardinality(String),
    advisor_id String,
    opened_at DateTime,
    updated_at DateTime
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(processing_date)
ORDER BY (organisation_id, processing_date, account_id);

-- 2. Silver Contacts
CREATE TABLE IF NOT EXISTS silver.silver_contacts (
    organisation_id LowCardinality(String),
    processing_date Date,
    contact_id String,
    account_id String,
    first_name String,
    last_name String,
    full_name String,
    email String,
    phone String,
    updated_at DateTime
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(processing_date)
ORDER BY (organisation_id, processing_date, contact_id);

-- 3. Silver Portfolios
CREATE TABLE IF NOT EXISTS silver.silver_portfolios (
    organisation_id LowCardinality(String),
    processing_date Date,
    portfolio_id String,
    account_id String,
    portfolio_name String,
    strategy LowCardinality(String),
    risk_profile LowCardinality(String),
    updated_at DateTime
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(processing_date)
ORDER BY (organisation_id, processing_date, portfolio_id);

-- 4. Silver Securities
CREATE TABLE IF NOT EXISTS silver.silver_securities (
    organisation_id LowCardinality(String),
    processing_date Date,
    security_id String,
    ticker LowCardinality(String),
    security_name String,
    asset_class LowCardinality(String),
    currency LowCardinality(String),
    updated_at DateTime
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(processing_date)
ORDER BY (organisation_id, processing_date, security_id);

-- 5. Silver Holdings
CREATE TABLE IF NOT EXISTS silver.silver_holdings (
    organisation_id LowCardinality(String),
    processing_date Date,
    holding_id String,
    portfolio_id String,
    security_id String,
    quantity Float64,
    market_value Float64,
    cost_basis Float64,
    as_of_date Date,
    updated_at DateTime
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(processing_date)
ORDER BY (organisation_id, processing_date, holding_id);

-- 6. Silver Trades
CREATE TABLE IF NOT EXISTS silver.silver_trades (
    organisation_id LowCardinality(String),
    processing_date Date,
    trade_id String,
    portfolio_id String,
    security_id String,
    side LowCardinality(String),
    quantity Float64,
    price Float64,
    trade_value Float64,
    commission Float64,
    executed_at DateTime,
    updated_at DateTime
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(processing_date)
ORDER BY (organisation_id, processing_date, trade_id);

-- 7. Silver Transactions
CREATE TABLE IF NOT EXISTS silver.silver_transactions (
    organisation_id LowCardinality(String),
    processing_date Date,
    transaction_id String,
    account_id String,
    transaction_type LowCardinality(String),
    amount Float64,
    currency LowCardinality(String),
    description String,
    transaction_date Date,
    updated_at DateTime
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(processing_date)
ORDER BY (organisation_id, processing_date, transaction_id);

-- 8. Silver Advisors
CREATE TABLE IF NOT EXISTS silver.silver_advisors (
    organisation_id LowCardinality(String),
    processing_date Date,
    advisor_id String,
    first_name String,
    last_name String,
    advisor_full_name String,
    email String,
    branch LowCardinality(String),
    aum_target Float64,
    updated_at DateTime
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(processing_date)
ORDER BY (organisation_id, processing_date, advisor_id);

-- 9. Silver Fee Schedules
CREATE TABLE IF NOT EXISTS silver.silver_fee_schedules (
    organisation_id LowCardinality(String),
    processing_date Date,
    fee_schedule_id String,
    account_id String,
    fee_type LowCardinality(String),
    rate_bps Float64,
    billing_frequency LowCardinality(String),
    effective_date Date,
    updated_at DateTime
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(processing_date)
ORDER BY (organisation_id, processing_date, fee_schedule_id);

-- 10. Silver Statements
CREATE TABLE IF NOT EXISTS silver.silver_statements (
    organisation_id LowCardinality(String),
    processing_date Date,
    statement_id String,
    account_id String,
    period_start Date,
    period_end Date,
    opening_balance Float64,
    closing_balance Float64,
    currency LowCardinality(String),
    updated_at DateTime
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(processing_date)
ORDER BY (organisation_id, processing_date, statement_id);
