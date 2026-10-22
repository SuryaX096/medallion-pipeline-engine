import csv
import io
import urllib.request
import logging
from datetime import datetime
import clickhouse_connect

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("SP500Ingest")

URL = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv"

def ingest_sp500(host="localhost", port=8123, user="admin", password="admin123"):
    logger.info("Fetching S&P 500 constituents from public batch source...")
    req = urllib.request.Request(URL, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        content = resp.read().decode("utf-8")

    reader = csv.DictReader(io.StringIO(content))
    rows = []
    for r in reader:
        symbol = r.get("Symbol", "").strip()
        name = r.get("Security", "").strip()
        sector = r.get("GICS Sector", "").strip()
        sub_ind = r.get("GICS Sub-Industry", "").strip()
        hq = r.get("Headquarters Location", "").strip()
        raw_date = r.get("Date added", "").strip()
        date_added = None
        if raw_date:
            try:
                date_added = datetime.strptime(raw_date, "%Y-%m-%d").date()
            except Exception:
                date_added = None
        cik_str = r.get("CIK", "").strip()
        cik = int(cik_str) if cik_str and cik_str.isdigit() else None
        founded = r.get("Founded", "").strip()
        rows.append([symbol, name, sector, sub_ind, hq, date_added, cik, founded])

    logger.info(f"Parsed {len(rows)} constituent records. Ingesting into ClickHouse bronze...")
    client = clickhouse_connect.get_client(host=host, port=port, username=user, password=password)
    client.command("DROP TABLE IF EXISTS bronze.bronze_sp500_constituents")
    client.command("""
    CREATE TABLE bronze.bronze_sp500_constituents (
        symbol LowCardinality(String),
        security_name String,
        gics_sector LowCardinality(String),
        gics_sub_industry String,
        headquarters String,
        date_added Nullable(Date32),
        cik Nullable(UInt32),
        founded String,
        ingested_at DateTime DEFAULT now()
    ) ENGINE = MergeTree()
    ORDER BY (symbol)
    """)
    client.insert(
        "bronze.bronze_sp500_constituents",
        rows,
        column_names=["symbol", "security_name", "gics_sector", "gics_sub_industry", "headquarters", "date_added", "cik", "founded"]
    )
    count = client.query("SELECT count() FROM bronze.bronze_sp500_constituents").result_rows[0][0]
    logger.info(f"Ingested {count} records into bronze.bronze_sp500_constituents successfully!")
    return count

if __name__ == "__main__":
    import os
    h = os.environ.get("CLICKHOUSE_HOST", "localhost")
    p = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
    u = os.environ.get("CLICKHOUSE_USER", "admin")
    pw = os.environ.get("CLICKHOUSE_PASSWORD", "admin123")
    ingest_sp500(h, p, u, pw)
