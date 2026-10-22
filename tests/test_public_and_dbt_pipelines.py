import pytest
import clickhouse_connect

@pytest.fixture(scope="module")
def ch_client():
    client = clickhouse_connect.get_client(host="localhost", port=8123, username="admin", password="admin123")
    return client

def test_databases_exist(ch_client):
    databases = [row[0] for row in ch_client.query("SHOW DATABASES").result_rows]
    for db in ["bronze", "silver", "gold", "default"]:
        assert db in databases, f"Database {db} missing from ClickHouse"

def test_default_tables_populated(ch_client):
    default_tables = [row[0] for row in ch_client.query("SHOW TABLES FROM default").result_rows]
    assert len(default_tables) > 0, "default database must have tables"
    assert "gold_daily_organisation_aum" in default_tables

def test_public_batch_sp500_ingested(ch_client):
    count = ch_client.query("SELECT count() FROM bronze.bronze_sp500_constituents").result_rows[0][0]
    assert count >= 500, f"Expected at least 500 S&P 500 constituents, got {count}"
    
    # Verify dbt silver stage
    dbt_count = ch_client.query("SELECT count() FROM silver.stg_sp500_securities").result_rows[0][0]
    assert dbt_count >= 500

def test_realtime_crypto_stream_ingested(ch_client):
    count = ch_client.query("SELECT count() FROM bronze.bronze_crypto_stream").result_rows[0][0]
    assert count > 0, "bronze.bronze_crypto_stream must contain live streaming records"
    
    symbols = [row[0] for row in ch_client.query("SELECT DISTINCT symbol FROM bronze.bronze_crypto_stream").result_rows]
    assert "BTCUSDT" in symbols

def test_dbt_models_in_silver_and_gold(ch_client):
    silver_tables = [row[0] for row in ch_client.query("SHOW TABLES FROM silver").result_rows]
    assert "stg_accounts" in silver_tables
    assert "stg_holdings" in silver_tables
    assert "int_portfolio_holdings_enriched" in silver_tables
    
    gold_tables = [row[0] for row in ch_client.query("SHOW TABLES FROM gold").result_rows]
    assert "fct_daily_organisation_aum" in gold_tables
    assert "fct_organisation_asset_allocation" in gold_tables
    assert "fct_organisation_trading_volume" in gold_tables
