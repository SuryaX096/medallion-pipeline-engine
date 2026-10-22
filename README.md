# Medallion Pipeline Engine (`DATA-1`)

A production-grade, configuration-driven Medallion Pipeline Engine (**Bronze** $\rightarrow$ **Silver** $\rightarrow$ **Gold**) implemented from scratch in **ClickHouse**, orchestrated with **Apache Airflow 2.10.5** and **dbt 1.12.6**, featuring real-time streaming ingestion with **Apache NiFi 1.28.0**, and backed by **PostgreSQL 16**.

The engine processes both enterprise multi-tenant vendor ORC files and public streaming/batch sources with strict tenant isolation (`organisation_id`), folder-based partition extraction, 3NF relational normalization, stateful deduplication, and pre-aggregated analytics.

---

## 🏛️ Architecture Overview

```
                      ┌────────────────────────────────────────┐
                      │          DATA INGESTION FEEDS          │
                      └────────────────────────────────────────┘
                                   │               │
             [Real-Time Streaming] │               │ [Public & Vendor Batch]
             Binance Crypto Ticker │               │ S&P 500 CSV Reference
                                   ▼               │ 210 Partitioned ORC Files
                      ┌────────────────────────┐   │
                      │  Apache NiFi 1.28.0    │   │
                      │  (Processor Pipeline)  │   │
                      └────────────────────────┘   │
                                   │               │
                                   ▼               ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER (ClickHouse: `bronze` database)                                          │
│  - 10 Partitioned ORC Views (Accounts, Contacts, Holdings, Trades, Portfolios, etc.)   │
│  - Real-time Stream Table: `bronze.bronze_crypto_stream` (Live crypto price ticks)     │
│  - Public Batch Table: `bronze.bronze_sp500_constituents` (500+ S&P 500 constituents)   │
│  - Dynamic partition extraction: `organisation_id` & `processing_date`                 │
└────────────────────────────────────────────────────────────────────────────────────────┘
                                           │
                                           ▼ (Airflow Orchestration + dbt ClickHouse)
┌────────────────────────────────────────────────────────────────────────────────────────┐
│  SILVER LAYER (ClickHouse: `silver` database)                                          │
│  - `ReplacingMergeTree(updated_at)` engines for stateful deduplication                 │
│  - Window function deduplication: ROW_NUMBER() per (organisation_id, date, PK) = 1     │
│  - Relational 3NF joins with strict tenant guards: `int_portfolio_holdings_enriched`   │
│  - Public Data Integration: `silver.stg_sp500_securities` enriching master securities  │
│  - Guaranteed tenant collision isolation (`ACC-COLLISION-999`)                         │
└────────────────────────────────────────────────────────────────────────────────────────┘
                                           │
                                           ▼ (dbt Models & Aggregations)
┌────────────────────────────────────────────────────────────────────────────────────────┐
│  GOLD LAYER (ClickHouse: `gold` database & `default` alias views)                      │
│  - `SummingMergeTree` additive engines for tenant rollups & background merges          │
│  - Models: `fct_daily_organisation_aum`, `fct_organisation_asset_allocation`,          │
│            `fct_organisation_trading_volume`                                           │
│  - Multi-tenant grouping enforcement with automated guard test barrier                 │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 🌐 Public Data Sources

1. **Real-Time Streaming Source:**
   - **Endpoint:** Binance Public Market Ticker API (`https://api.binance.com/api/v3/ticker/price`)
   - **Ingestion Engine:** **Apache NiFi 1.28.0** (`Crypto_Streaming_Pipeline` processor group: `InvokeHTTP` $\rightarrow$ `SplitJson` $\rightarrow$ `InvokeHTTP` POST to ClickHouse).
   - **Target Table:** `bronze.bronze_crypto_stream`
   - **Metrics:** Symbol, live price, source feed, and ingestion timestamp.

2. **Public Batch Source:**
   - **Endpoint:** S&P 500 Market Constituents Dataset (`https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv`)
   - **Ingestion Engine:** Python / Airflow batch task (`scripts/ingest_sp500_batch.py`).
   - **Target Table:** `bronze.bronze_sp500_constituents` (503 corporate reference records).
   - **Silver Transformation:** `silver.stg_sp500_securities` (built and verified via dbt).

---

## 📂 Project Structure

```
medallion-pipeline-engine/
├── airflow/
│   ├── dags/
│   │   └── medallion_dag.py             # Airflow 2.10.5 DAG (8 orchestrated tasks)
│   ├── logs/                            # Airflow execution logs
│   └── plugins/
├── config/
│   ├── bronze_tables.yaml               # 10 Bronze raw schemas & path globs
│   ├── silver_tables.yaml               # 10 Silver 3NF schemas & enriched joins
│   ├── gold_tables.yaml                 # Gold metrics & SummingMergeTree definitions
│   └── auto_generated_portfolios.yaml   # CLI auto-generated schema artifact
├── dbt_project/                         # Production dbt ClickHouse Project
│   ├── dbt_project.yml                  # dbt configuration
│   ├── profiles.yml                     # ClickHouse adapter connection profile
│   ├── macros/
│   │   └── generate_schema_name.sql     # Schema routing to silver/gold databases
│   └── models/
│       ├── sources.yml                  # Bronze source definitions
│       ├── silver/                      # Silver staging models (stg_*, int_*) & tests
│       └── gold/                        # Gold aggregate models (fct_*) & tests
├── data/
│   ├── raw_lake/                        # 210 partitioned ORC files
│   │   ├── accounts/
│   │   ├── contacts/
│   │   ├── portfolios/
│   │   ├── securities/
│   │   ├── holdings/
│   │   ├── trades/
│   │   ├── transactions/
│   │   ├── advisors/
│   │   ├── fee_schedules/
│   │   └── statements/
│   └── generated/                       # Output artifacts & export staging
├── docker/
│   └── clickhouse/
│       └── users.d/admin.xml            # ClickHouse user & access management
├── docs/
│   ├── ARCHITECTURE.md                  # Comprehensive architectural specifications
│   ├── SCHEMA_DICTIONARY.md             # Full data model dictionary for all layers
│   ├── AUDIT_REPORT.md                  # Detailed system audit & fix log
│   └── DEMO_PLAN.md                     # Minute-by-minute 5–8 min reviewer demo guide
├── engine/
│   ├── pipeline_engine.py               # Medallion Engine with ClickHouse persistence
│   └── schema_helper.py                 # Standalone CLI tool to auto-generate YAML
├── generator/
│   └── synthetic_data_generator.py      # Synthetic ORC generator (10 tables, 3 orgs, 7 dates)
├── nifi/
│   └── deploy_crypto_flow.py            # Automated NiFi streaming flow deployer
├── scripts/
│   ├── crypto_streamer.py               # Real-time crypto stream ingestion runner
│   └── ingest_sp500_batch.py            # Public S&P 500 batch ingestion runner
├── sql/
│   ├── bronze/
│   │   └── 01_create_bronze_views.sql   # Bronze partitioned view DDLs
│   ├── silver/
│   │   ├── 01_create_silver_tables.sql  # ReplacingMergeTree DDLs & dedup logic
│   │   └── 02_create_enriched_portfolio_holdings.sql # 3NF relational joins
│   └── gold/
│       └── 01_create_gold_tables.sql    # SummingMergeTree tables & aggregates
├── tests/
│   ├── test_medallion_pipeline.py       # Core Medallion & multi-tenant guard tests
│   └── test_public_and_dbt_pipelines.py # Public batch/stream & dbt model tests
├── docker-compose.yml                   # PostgreSQL 16, ClickHouse, Airflow, and NiFi stack
├── requirements.txt                     # Dependencies
└── README.md
```

---

## 🚀 Quickstart & Reproduction

### 1. Start Infrastructure Stack
Start PostgreSQL, ClickHouse, Apache Airflow, and Apache NiFi:
```bash
docker compose up -d
docker compose ps
```
Services will be active at:
- **Airflow Web UI:** [http://localhost:8080](http://localhost:8080) (Username: `admin`, Password: `admin123`)
- **Apache NiFi UI:** [https://localhost:8443/nifi](https://localhost:8443/nifi) (Username: `admin`, Password: `Admin12345678!`)
- **ClickHouse HTTP:** `http://localhost:8123` (Username: `admin`, Password: `admin123`)
- **PostgreSQL:** `localhost:5432` (Database: `medallion`)

### 2. Generate Synthetic ORC Raw Lake
Generates 210 logical ORC files (10 tables $\times$ 3 organisations $\times$ 7 dates) including the seeded collision `ACC-COLLISION-999`:
```bash
python generator/synthetic_data_generator.py
```

### 3. Ingest Public Sources
- **Public Batch (S&P 500):**
  ```bash
  python scripts/ingest_sp500_batch.py
  ```
- **Real-Time Stream (Binance Crypto Tickers):**
  ```bash
  # Deploy flow to NiFi
  python nifi/deploy_crypto_flow.py
  
  # Or run streamer script directly
  python scripts/crypto_streamer.py 5
  ```

### 4. Execute Medallion Pipeline Engine & dbt
- **Run Engine:**
  ```bash
  python engine/pipeline_engine.py
  ```
- **Run dbt Models:**
  ```bash
  docker exec medallion-airflow dbt run --project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_project
  ```
- **Run dbt Tests:**
  ```bash
  docker exec medallion-airflow dbt test --project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_project
  ```

### 5. Run the Complete Automated Test Suite
Execute the 12-test automated suite:
```bash
python -m pytest tests/ -v
```

---

## 🧪 Automated Test Results (12/12 PASS)

```
tests/test_medallion_pipeline.py::test_10_plus_tables_exist_in_bronze PASSED             [  8%]
tests/test_medallion_pipeline.py::test_bronze_partition_columns_populated PASSED         [ 16%]
tests/test_medallion_pipeline.py::test_silver_deduplication_integrity PASSED             [ 25%]
tests/test_medallion_pipeline.py::test_relational_normalization_and_enrichment PASSED     [ 33%]
tests/test_medallion_pipeline.py::test_multi_tenant_collision_isolation PASSED          [ 41%]
tests/test_medallion_pipeline.py::test_gold_aggregates_group_by_organisation PASSED       [ 50%]
tests/test_medallion_pipeline.py::test_guard_test_fails_if_organisation_id_removed PASSED [ 58%]
tests/test_public_and_dbt_pipelines.py::test_databases_exist PASSED                      [ 66%]
tests/test_public_and_dbt_pipelines.py::test_default_tables_populated PASSED             [ 75%]
tests/test_public_and_dbt_pipelines.py::test_public_batch_sp500_ingested PASSED          [ 83%]
tests/test_public_and_dbt_pipelines.py::test_realtime_crypto_stream_ingested PASSED      [ 91%]
tests/test_public_and_dbt_pipelines.py::test_dbt_models_in_silver_and_gold PASSED        [100%]

============================= 12 passed in 7.96s ==============================
```

---

## 💬 Reviewer Q&A and Verification

### Q1: Were ClickHouse databases empty in the previous demo?
**Resolution:**  
In the previous demo, queries operated directly on files without persisting physical tables. The engine now creates and materializes persistent physical tables in `bronze`, `silver`, and `gold`, as well as convenience alias views in `default`. Querying `SHOW DATABASES;` and `SHOW TABLES FROM default;` now returns all active models.

### Q2: How is Apache NiFi integrated?
**Resolution:**  
`medallion-nifi` (version 1.28.0) runs on HTTPS port 8443 with a dedicated process group (`Crypto_Streaming_Pipeline`). It polls Binance's public price API every 10 seconds, splits ticks into JSON, and streams them into `bronze.bronze_crypto_stream` in ClickHouse.

### Q3: How is dbt integrated with Airflow?
**Resolution:**  
`dbt-clickhouse` 1.10.3 and `dbt-core` 1.12.6 are installed in the Airflow environment. The DAG `medallion_pipeline_engine` features tasks `dbt_transform_silver_and_gold` (`dbt run`) and `dbt_test_data_quality` (`dbt test`), validating 10 models and 14 automated data quality rules.

### Q4: How is multi-tenant collision handled?
**Resolution:**  
Account `ACC-COLLISION-999` is seeded under both `org_alpha` and `org_beta`. Every composite primary key, partition key, and join condition strictly enforces `organisation_id`, ensuring both accounts survive independently with zero cross-tenant bleeding.

---

## 🎥 Demonstration Guide
For the step-by-step 5–8 minute live review script, refer to:
👉 **[`docs/DEMO_PLAN.md`](docs/DEMO_PLAN.md)**
