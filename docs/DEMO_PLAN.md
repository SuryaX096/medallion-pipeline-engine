# DATA-1: Medallion Pipeline Engine — Reviewer Demonstration Plan (5–8 Minutes)

This document provides a minute-by-minute demonstration script and commands to present the completed **DATA-1 Medallion Pipeline Engine** project to reviewers.

---

## ⏱️ Video / Live Demo Schedule Overview

| Minute | Segment | Focus | Screen / Tool |
|---|---|---|---|
| **0:00 - 1:00** | **Architecture & Requirements Overview** | Medallion (Bronze $\rightarrow$ Silver $\rightarrow$ Gold), Multi-Tenancy (`organisation_id`), 2 Public Sources | Presentation / Architecture Diagram |
| **1:00 - 2:30** | **Real-Time Pipeline (Apache NiFi)** | Real-time crypto stream from Binance API into ClickHouse Bronze | Apache NiFi UI (`https://localhost:8443/nifi`) & ClickHouse SQL |
| **2:30 - 4:00** | **Batch Pipeline Orchestration (Airflow + dbt)** | Orchestrated batch pipeline (Public S&P 500 + 210 synthetic ORC files $\rightarrow$ dbt Silver/Gold) | Airflow Web UI (`http://localhost:8080`) |
| **4:00 - 5:30** | **ClickHouse Analytical Database Verification** | Direct SQL inspection: `SHOW DATABASES;`, `SHOW TABLES FROM default;`, table counts, Medallion layers | ClickHouse Client / Terminal |
| **5:30 - 6:45** | **Multi-Tenant Collision Isolation & Data Integrity** | Proving `ACC-COLLISION-999` survives independently for both `org_alpha` and `org_beta` | ClickHouse SQL Query & Guard Test |
| **6:45 - 8:00** | **Automated Quality Testing & Conclusion** | Running dbt tests (14/14 PASS) and pytest suite (12/12 PASS) | Terminal (`dbt test`, `pytest`) |

---

## 🎬 Minute-by-Minute Step-by-Step Script

### Minute 0:00 – 1:00: Architecture Overview
- **Talking Points:**
  - *"Welcome to the demonstration of DATA-1: Medallion Pipeline Engine."*
  - *"The architecture implements a multi-tenant, configuration-driven lakehouse using ClickHouse, Apache NiFi, Apache Airflow, and dbt."*
  - *"We ingest two public data sources: a real-time crypto ticker stream via Apache NiFi and a batch reference dataset (S&P 500 constituents) via Airflow, alongside 10 enterprise financial tables partitioned across 3 tenants and 7 processing dates."*

---

### Minute 1:00 – 2:30: Real-Time Pipeline (Apache NiFi)
1. **Open Apache NiFi UI:**
   - **URL:** [https://localhost:8443/nifi](https://localhost:8443/nifi)
   - **Username:** `admin`
   - **Password:** `Admin12345678!`
2. **Show the Process Group:**
   - Navigate into `Crypto_Streaming_Pipeline`.
   - Highlight the data flow:
     - `Fetch_Binance_Crypto_Ticks` (InvokeHTTP polling `https://api.binance.com/api/v3/ticker/price...`)
     - `Split_Crypto_Ticks` (SplitJson `$.*`)
     - `Ingest_To_ClickHouse_Bronze` (InvokeHTTP streaming into `bronze.bronze_crypto_stream`)
3. **Verify Live Stream in ClickHouse:**
   Run in PowerShell or terminal:
   ```bash
   python -c "
   import clickhouse_connect
   client = clickhouse_connect.get_client(host='localhost', port=8123, username='admin', password='admin123')
   res = client.query('SELECT symbol, price, ingested_at, source_feed FROM bronze.bronze_crypto_stream ORDER BY ingested_at DESC LIMIT 5')
   for row in res.result_rows:
       print(row)
   "
   ```
   - *Expected Result:* Real-time live prices for BTC, ETH, SOL, BNB, ADA with fresh timestamps.

---

### Minute 2:30 – 4:00: Batch Pipeline Orchestration (Airflow + dbt)
1. **Open Airflow Web UI:**
   - **URL:** [http://localhost:8080](http://localhost:8080)
   - **Username:** `admin`
   - **Password:** `admin123`
2. **Inspect DAG `medallion_pipeline_engine`:**
   - Switch to **Graph View**. Show the 8 orchestrated tasks:
     1. `generate_synthetic_orc_lake` (Synthetic generator: 210 ORC files across 10 tables)
     2. `ingest_public_batch_sp500` (Batch ingestion of S&P 500 constituents)
     3. `bronze_partitioned_ingestion` (Bronze views with partition pruning)
     4. `silver_dedup_and_normalize` (Deduplication via `ReplacingMergeTree`)
     5. `dbt_transform_silver_and_gold` (Executes `dbt run` against ClickHouse)
     6. `dbt_test_data_quality` (Executes `dbt test` schema and uniqueness rules)
     7. `gold_multitenant_aggregation` (SummingMergeTree aggregations)
     8. `validate_tenant_collision_isolation` (Multi-tenant isolation barrier test)
3. **Show Recent Successful Run:**
   - Demonstrate that the DAG has completed with status `success` across all 8 nodes.

---

### Minute 3:30 – 4:30: dbt Model Execution
Run inside the Airflow environment or container:
```bash
docker exec medallion-airflow dbt run --project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_project
```
- *Output:* 10 models executed (Silver staging tables, enriched views, Gold aggregations).
- *Result:* `Done. PASS=10 WARN=0 ERROR=0 SKIP=0 TOTAL=10`.

---

### Minute 4:00 – 5:30: ClickHouse Analytical Database Verification
The reviewer previously noted missing databases and 0 rows in `default`. Prove both are resolved:

1. **Verify Databases:**
   ```sql
   SHOW DATABASES;
   ```
   - *Output:* `bronze`, `silver`, `gold`, `default`, `system`.

2. **Verify `default` Database Tables (Alias Layer):**
   ```sql
   SHOW TABLES FROM default;
   ```
   - *Output:* Contains `bronze_accounts`, `bronze_holdings`, `gold_daily_organisation_aum`, `gold_organisation_asset_allocation`, `gold_organisation_trading_volume`, `silver_accounts`, `silver_holdings`, `silver_trades`.

3. **Verify Table Row Counts in Bronze, Silver, Gold:**
   ```bash
   python -c "
   import clickhouse_connect
   client = clickhouse_connect.get_client(host='localhost', port=8123, username='admin', password='admin123')
   for db in ['bronze', 'silver', 'gold']:
       print(f'=== {db.upper()} TABLES ===')
       for t in [r[0] for r in client.query(f'SHOW TABLES FROM {db}').result_rows]:
           count = client.query(f'SELECT count() FROM {db}.{t}').result_rows[0][0]
           print(f'  {t}: {count} rows')
   "
   ```

---

### Minute 5:30 – 6:45: Multi-Tenant Collision Isolation
Demonstrate that deliberate collisions (`account_id = ACC-COLLISION-999`) across two tenants remain strictly isolated:

```sql
SELECT
    organisation_id,
    account_id,
    account_name,
    status,
    currency
FROM silver.silver_accounts
WHERE account_id = 'ACC-COLLISION-999';
```
- **Observed Result:**
  - Row 1: `org_alpha` | `ACC-COLLISION-999` | `Collision Account Owned By ORG_ALPHA`
  - Row 2: `org_beta`  | `ACC-COLLISION-999` | `Collision Account Owned By ORG_BETA`
- Explain why: The primary and sorting keys always compose `(organisation_id, processing_date, account_id)`, preventing cross-tenant leakage.

---

### Minute 6:45 – 8:00: Automated Quality Testing
Execute all automated test suites to conclude the demo:

1. **dbt Data Quality Tests:**
   ```bash
   docker exec medallion-airflow dbt test --project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_project
   ```
   - *Result:* `Done. PASS=14 WARN=0 ERROR=0 SKIP=0 TOTAL=14`.

2. **Pytest Suite:**
   ```bash
   python -m pytest tests/ -v
   ```
   - *Result:* `12 passed in 7.96s` (Bronze partitioning, Silver deduplication, 3NF join enrichment, multi-tenant collision isolation, Gold aggregation guards, public batch and stream data sources).

---

## 📌 Summary Checklist for Reviewer Requirements

| Reviewer Requirement | Implementation | Status |
|---|---|---|
| **Two Public Data Sources** | Real-time: Binance Crypto API (`https://api.binance.com/`)<br>Batch: S&P 500 Constituents (`https://raw.githubusercontent.com/...`) | ✅ Verified |
| **Real-time Pipeline using Apache NiFi** | `medallion-nifi` container on port 8443; Processor Group `Crypto_Streaming_Pipeline` ingesting into `bronze.bronze_crypto_stream` | ✅ Verified |
| **Batch Pipeline Orchestrated using Airflow & dbt** | `medallion_pipeline_engine` DAG executing S&P 500 ingest + Bronze views + dbt run/test + Gold rollups | ✅ Verified |
| **ClickHouse as Analytical Database** | ClickHouse 26.9 with persistent `bronze`, `silver`, `gold`, and `default` alias views | ✅ Verified |
| **Medallion Architecture (Bronze $\rightarrow$ Silver $\rightarrow$ Gold)** | Bronze (10 ORC tables + 2 public) $\rightarrow$ Silver (ReplacingMergeTree 3NF deduplication) $\rightarrow$ Gold (SummingMergeTree multi-tenant metrics) | ✅ Verified |
| **Tenant Collision Isolation** | Guaranteed preservation of `ACC-COLLISION-999` under `org_alpha` and `org_beta` without bleeding | ✅ Verified |
