# Medallion Pipeline Engine (`DATA-1`)

A production-grade, configuration-driven Medallion Pipeline Engine (**Bronze** $\rightarrow$ **Silver** $\rightarrow$ **Gold**) implemented from scratch in **ClickHouse**, orchestrated with **Apache Airflow 2.10.5**, and backed by **PostgreSQL 16**.

The engine processes raw multi-tenant vendor ORC files with strict tenant isolation (`organisation_id`), folder-based partition extraction, 3NF relational normalization, stateful deduplication, and pre-aggregated analytics.

---

## 🏛️ Architecture Overview

```
[Raw Lake (Local ORC Files / MinIO-ready)]
  data/raw_lake/<table_name>/organisation_id=<org>/processing_date=<date>/data.orc
                                      │
                                      ▼
  ┌────────────────────────────────────────────────────────────────────────┐
  │  BRONZE LAYER (10 Financial Tables)                                    │
  │  ClickHouse File / Object-storage views over ORC callsets              │
  │  Dynamic partition extraction:                                         │
  │    - organisation_id: extract(_path, 'organisation_id=([^/]+)')        │
  │    - processing_date: toDate(extract(_path, 'processing_date=([^/]+)'))│
  └────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
  ┌────────────────────────────────────────────────────────────────────────┐
  │  SILVER LAYER (Deduplication & Relational 3NF Normalization)           │
  │  - ReplacingMergeTree(updated_at) engines                              │
  │  - Window function deduplication: ROW_NUMBER() per (org, date, PK) = 1 │
  │  - Relational parent-child enriched joins with tenant guards           │
  │  - Multi-tenant collision isolation (ACC-COLLISION-999)               │
  └────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
  ┌────────────────────────────────────────────────────────────────────────┐
  │  GOLD LAYER (Multi-Tenant Pre-Aggregated Analytics)                   │
  │  - SummingMergeTree table engines for additive metric rollups          │
  │  - Daily AUM, asset class exposures, and trading turnover metrics      │
  │  - Strictly grouped by organisation_id with automated security guards  │
  └────────────────────────────────────────────────────────────────────────┘
```

---

## 📂 Project Structure

```
medallion-data-platform/
├── airflow/
│   ├── dags/
│   │   └── medallion_dag.py             # Airflow 2.10.5 DAG orchestration
│   ├── logs/                            # Airflow execution logs
│   └── plugins/
├── config/
│   ├── bronze_tables.yaml               # 10 Bronze raw schemas & path globs
│   ├── silver_tables.yaml               # 10 Silver 3NF schemas & enriched joins
│   ├── gold_tables.yaml                 # Gold metrics & SummingMergeTree definitions
│   └── auto_generated_portfolios.yaml   # CLI auto-generated schema artifact
├── data/
│   └── raw_lake/                        # 210 partitioned ORC files
│       ├── accounts/
│       ├── contacts/
│       ├── portfolios/
│       ├── securities/
│       ├── holdings/
│       ├── trades/
│       ├── transactions/
│       ├── advisors/
│       ├── fee_schedules/
│       └── statements/
├── docker/
│   └── clickhouse/
│       └── users.d/admin.xml            # ClickHouse user & access management
├── engine/
│   ├── pipeline_engine.py               # Medallion Engine (Bronze -> Silver -> Gold)
│   └── schema_helper.py                 # Standalone CLI tool to auto-generate YAML
├── generator/
│   └── synthetic_data_generator.py      # Synthetic ORC generator (10 tables, 3 orgs, 7 dates)
├── tests/
│   └── test_medallion_pipeline.py       # 6-phase test suite validating all criteria
├── Dockerfile                           # Custom Airflow 2.10.5 image with dependencies
├── docker-compose.yml                   # PostgreSQL 16, ClickHouse, and Airflow stack
├── requirements.txt                     # Minimal project dependencies
├── .gitignore                           # Clean exclusion rules
└── README.md                            # Complete architecture & review documentation
```

---

## 🚀 Quickstart & Reproduction

### 1. Start Infrastructure Stack
Start PostgreSQL, ClickHouse, and Apache Airflow with Docker Compose:
```bash
docker compose up -d
docker compose ps
```
Services will be active at:
- **Airflow Web UI:** [http://localhost:8080](http://localhost:8080) (Username: `admin`, Password: `admin123`)
- **ClickHouse HTTP:** `http://localhost:8123` (Username: `admin`, Password: `admin123`)
- **PostgreSQL:** `localhost:5432` (Database: `medallion`)

> **Storage Notice regarding MinIO:**  
> In accordance with the project environment boundary, the storage layer uses a local raw lake structure (`data/raw_lake/`) mounted directly into ClickHouse's `user_files/raw_lake`. The paths and queries are designed with S3/MinIO compatible URI patterns so MinIO or AWS S3 can be attached without rewriting business transformations.

### 2. Generate Synthetic ORC Raw Lake
Generates 210 logical ORC files (10 tables $\times$ 3 organisations $\times$ 7 dates) including the seeded collision `ACC-COLLISION-999`:
```bash
python generator/synthetic_data_generator.py
```

### 3. Run Schema Auto-Configuration Helper
Inspect any raw ORC file to generate a draft Bronze YAML configuration:
```bash
python engine/schema_helper.py data/raw_lake/portfolios/organisation_id=org_alpha/processing_date=2026-03-01/data.orc -o config/auto_generated_portfolios.yaml
```

### 4. Execute the Medallion Pipeline Engine
Run the end-to-end transformation across Bronze, Silver, and Gold layers:
```bash
python engine/pipeline_engine.py
```

### 5. Run the 6-Phase Automated Test Suite
Execute the test suite validating all acceptance criteria:
```bash
python -m pytest tests/test_medallion_pipeline.py -v
```

---

## 🧪 6-Phase Verification Results

```
tests/test_medallion_pipeline.py::test_10_plus_tables_exist_in_bronze PASSED             [ 14%]
tests/test_medallion_pipeline.py::test_bronze_partition_columns_populated PASSED         [ 28%]
tests/test_medallion_pipeline.py::test_silver_deduplication_integrity PASSED             [ 42%]
tests/test_medallion_pipeline.py::test_relational_normalization_and_enrichment PASSED     [ 57%]
tests/test_medallion_pipeline.py::test_multi_tenant_collision_isolation PASSED          [ 71%]
tests/test_medallion_pipeline.py::test_gold_aggregates_group_by_organisation PASSED       [ 85%]
tests/test_medallion_pipeline.py::test_guard_test_fails_if_organisation_id_removed PASSED [100%]

============================== 7 passed in 7.67s ==============================
```

---

## 💬 Review Q&A Check

### Q1: Are partition columns populated across all 10+ Bronze tables?
**Answer:**  
**Yes.** All 10 Bronze tables extract `organisation_id` and `processing_date` from `_path` via regex introspection:
```sql
extract(_path, 'organisation_id=([^/]+)') AS organisation_id,
toDate(extract(_path, 'processing_date=([^/]+)')) AS processing_date
```
The automated test `test_bronze_partition_columns_populated` proves that across all 10 tables:
- `countIf(organisation_id IS NULL OR organisation_id = '') == 0`
- `countIf(processing_date = '1970-01-01') == 0`

### Q2: Explain your Silver relational normalization model.
**Answer:**  
- **3NF Entities:** Raw ingestion files are decomposed into clean relational entities:
  - `accounts`, `contacts`, `portfolios`, `securities`, `holdings`, `trades`, `transactions`, `advisors`, `fee_schedules`, `statements`.
- **Deduplication:** State history deduplication is enforced by selecting `rn = 1` from:
  ```sql
  ROW_NUMBER() OVER (
      PARTITION BY organisation_id, processing_date, <business_key>
      ORDER BY parseDateTimeBestEffort(updated_at) DESC
  ) as rn
  ```
- **Tenant-Safe Enrichment:** The denormalized enriched relation `silver_portfolio_holdings_enriched` joins `holdings` $\rightarrow$ `portfolios` $\rightarrow$ `accounts` $\rightarrow$ `securities` with mandatory tenant predicates on every `ON` clause (`h.organisation_id = p.organisation_id` and `p.organisation_id = a.organisation_id`).

### Q3: Show how your Silver table handles the seeded multi-tenant collision.
**Answer:**  
Account `ACC-COLLISION-999` is seeded under both `org_alpha` and `org_beta`.
Because the Silver layer partitions and orders by `(organisation_id, account_id)`:
```
org_alpha: ACC-COLLISION-999 -> "Collision Account Owned By ORG_ALPHA" (7 daily rows)
org_beta:  ACC-COLLISION-999 -> "Collision Account Owned By ORG_BETA"  (7 daily rows)
```
Both records survive independently with zero cross-tenant contamination or overwrites. Verified by `test_multi_tenant_collision_isolation`.

### Q4: Why was this specific ClickHouse engine chosen for the Gold table?
**Answer:**  
We chose **`SummingMergeTree`** for the Gold tables (`gold_daily_organisation_aum`, `gold_organisation_asset_allocation`, `gold_organisation_trading_volume`):
1. **Additive Business Metrics:** Financial metrics such as total market value, total cost basis, gross trade volume, and broker commissions are strictly additive across daily partitions.
2. **Background Compression:** `SummingMergeTree` automatically consolidates numerical metrics during background merges without requiring expensive full-table re-scans.
3. **Partition Pruning:** Tables use `PARTITION BY toYYYYMM(processing_date)` and `ORDER BY (organisation_id, processing_date, ...)` to guarantee strict data locality per tenant and fast point queries.
