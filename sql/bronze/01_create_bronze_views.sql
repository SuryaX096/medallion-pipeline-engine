-- ============================================================================
-- BRONZE LAYER DDL & PARTITIONED VIEWS
-- Engine: file(..., ORC) with virtual partition extraction from _path
-- Tables: 10 Financial Tables
-- ============================================================================

CREATE DATABASE IF NOT EXISTS bronze;

-- 1. Bronze Accounts
CREATE OR REPLACE VIEW bronze.bronze_accounts AS
SELECT
    extract(_path, 'organisation_id=([^/]+)') AS organisation_id,
    toDate(extract(_path, 'processing_date=([^/]+)')) AS processing_date,
    account_id,
    account_name,
    status,
    currency,
    advisor_id,
    opened_at,
    updated_at,
    _path AS _source_file
FROM file('raw_lake/accounts/*/*/*.orc', ORC);

-- 2. Bronze Contacts
CREATE OR REPLACE VIEW bronze.bronze_contacts AS
SELECT
    extract(_path, 'organisation_id=([^/]+)') AS organisation_id,
    toDate(extract(_path, 'processing_date=([^/]+)')) AS processing_date,
    contact_id,
    account_id,
    first_name,
    last_name,
    email,
    phone,
    updated_at,
    _path AS _source_file
FROM file('raw_lake/contacts/*/*/*.orc', ORC);

-- 3. Bronze Portfolios
CREATE OR REPLACE VIEW bronze.bronze_portfolios AS
SELECT
    extract(_path, 'organisation_id=([^/]+)') AS organisation_id,
    toDate(extract(_path, 'processing_date=([^/]+)')) AS processing_date,
    portfolio_id,
    account_id,
    portfolio_name,
    strategy,
    risk_profile,
    updated_at,
    _path AS _source_file
FROM file('raw_lake/portfolios/*/*/*.orc', ORC);

-- 4. Bronze Securities
CREATE OR REPLACE VIEW bronze.bronze_securities AS
SELECT
    extract(_path, 'organisation_id=([^/]+)') AS organisation_id,
    toDate(extract(_path, 'processing_date=([^/]+)')) AS processing_date,
    security_id,
    ticker,
    security_name,
    asset_class,
    currency,
    updated_at,
    _path AS _source_file
FROM file('raw_lake/securities/*/*/*.orc', ORC);

-- 5. Bronze Holdings
CREATE OR REPLACE VIEW bronze.bronze_holdings AS
SELECT
    extract(_path, 'organisation_id=([^/]+)') AS organisation_id,
    toDate(extract(_path, 'processing_date=([^/]+)')) AS processing_date,
    holding_id,
    portfolio_id,
    security_id,
    quantity,
    market_value,
    cost_basis,
    as_of_date,
    updated_at,
    _path AS _source_file
FROM file('raw_lake/holdings/*/*/*.orc', ORC);

-- 6. Bronze Trades
CREATE OR REPLACE VIEW bronze.bronze_trades AS
SELECT
    extract(_path, 'organisation_id=([^/]+)') AS organisation_id,
    toDate(extract(_path, 'processing_date=([^/]+)')) AS processing_date,
    trade_id,
    portfolio_id,
    security_id,
    side,
    quantity,
    price,
    trade_value,
    commission,
    executed_at,
    updated_at,
    _path AS _source_file
FROM file('raw_lake/trades/*/*/*.orc', ORC);

-- 7. Bronze Transactions
CREATE OR REPLACE VIEW bronze.bronze_transactions AS
SELECT
    extract(_path, 'organisation_id=([^/]+)') AS organisation_id,
    toDate(extract(_path, 'processing_date=([^/]+)')) AS processing_date,
    transaction_id,
    account_id,
    transaction_type,
    amount,
    currency,
    description,
    transaction_date,
    updated_at,
    _path AS _source_file
FROM file('raw_lake/transactions/*/*/*.orc', ORC);

-- 8. Bronze Advisors
CREATE OR REPLACE VIEW bronze.bronze_advisors AS
SELECT
    extract(_path, 'organisation_id=([^/]+)') AS organisation_id,
    toDate(extract(_path, 'processing_date=([^/]+)')) AS processing_date,
    advisor_id,
    first_name,
    last_name,
    email,
    branch,
    aum_target,
    updated_at,
    _path AS _source_file
FROM file('raw_lake/advisors/*/*/*.orc', ORC);

-- 9. Bronze Fee Schedules
CREATE OR REPLACE VIEW bronze.bronze_fee_schedules AS
SELECT
    extract(_path, 'organisation_id=([^/]+)') AS organisation_id,
    toDate(extract(_path, 'processing_date=([^/]+)')) AS processing_date,
    fee_schedule_id,
    account_id,
    fee_type,
    rate_bps,
    billing_frequency,
    effective_date,
    updated_at,
    _path AS _source_file
FROM file('raw_lake/fee_schedules/*/*/*.orc', ORC);

-- 10. Bronze Statements
CREATE OR REPLACE VIEW bronze.bronze_statements AS
SELECT
    extract(_path, 'organisation_id=([^/]+)') AS organisation_id,
    toDate(extract(_path, 'processing_date=([^/]+)')) AS processing_date,
    statement_id,
    account_id,
    period_start,
    period_end,
    opening_balance,
    closing_balance,
    currency,
    updated_at,
    _path AS _source_file
FROM file('raw_lake/statements/*/*/*.orc', ORC);
