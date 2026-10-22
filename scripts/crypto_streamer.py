import time
import json
import logging
import urllib.request
import clickhouse_connect

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("CryptoStreamer")

TICKER_URL = 'https://api.binance.com/api/v3/ticker/price?symbols=["BTCUSDT","ETHUSDT","BNBUSDT","SOLUSDT","ADAUSDT"]'

def fetch_stream_ticks():
    req = urllib.request.Request(TICKER_URL, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode("utf-8"))

def ingest_crypto_stream(host="localhost", port=8123, user="admin", password="admin123", iterations=1, interval_sec=5):
    client = clickhouse_connect.get_client(host=host, port=port, username=user, password=password)
    
    # Ensure bronze table exists
    client.command("""
    CREATE TABLE IF NOT EXISTS bronze.bronze_crypto_stream (
        symbol LowCardinality(String),
        price Float64,
        ingested_at DateTime DEFAULT now(),
        source_feed LowCardinality(String) DEFAULT 'binance_realtime'
    ) ENGINE = MergeTree()
    ORDER BY (symbol, ingested_at)
    """)
    
    logger.info(f"Starting Crypto Stream Ingest to ClickHouse bronze.bronze_crypto_stream (iterations={iterations})...")
    for i in range(iterations):
        ticks = fetch_stream_ticks()
        rows = [[t["symbol"], float(t["price"])] for t in ticks]
        client.insert(
            "bronze.bronze_crypto_stream",
            rows,
            column_names=["symbol", "price"]
        )
        total_count = client.query("SELECT count() FROM bronze.bronze_crypto_stream").result_rows[0][0]
        logger.info(f"Iteration {i+1}/{iterations}: Ingested {len(rows)} live ticks (Total streaming rows: {total_count})")
        for t in ticks:
            logger.info(f"   -> {t['symbol']}: ${float(t['price']):,.2f}")
        if i < iterations - 1:
            time.sleep(interval_sec)

if __name__ == "__main__":
    import os, sys
    h = os.environ.get("CLICKHOUSE_HOST", "localhost")
    p = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
    u = os.environ.get("CLICKHOUSE_USER", "admin")
    pw = os.environ.get("CLICKHOUSE_PASSWORD", "admin123")
    iters = int(sys.argv[1]) if len(sys.argv) > 1 else 3
    ingest_crypto_stream(h, p, u, pw, iterations=iters, interval_sec=2)
