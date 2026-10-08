# Medallion Pipeline Engine (`DATA-1`)

A configuration-driven Medallion Pipeline Engine (Bronze $\rightarrow$ Silver $\rightarrow$ Gold) implemented from scratch in **ClickHouse** and orchestrated with **Airflow**, featuring strict multi-tenant isolation (`organisation_id`), automated schema configuration discovery, and relational normalization.

---

## 🏛️ Architecture Overview

The pipeline ingests raw vendor ORC files from object storage/lake into a three-tier Medallion architecture:

```
[Raw Lake / MinIO (ORC)]
  organisation_id=<org>/processing_date=<date>/data.orc
                       │
                       ▼
  ┌────────────────────────────────────────────────────────┐
  │  BRONZE LAYER (10+ Tables)                             │
  │  ClickHouse File / S3 Views                            │
  │  Extracts organisation_id & processing_date from path  │
  └────────────────────────────────────────────────────────┘
                       │
                       ▼
  ┌────────────────────────────────────────────────────────┐
  │  SILVER LAYER (Deduplication & Relational 3NF)         │
  │  - ReplacingMergeTree(updated_at)                      │
  │  - ROW_NUMBER() window filter per (org, date, PK)      │
  │  - State history & parent-child relational joins       │
  │  - Proof against multi-tenant key collision            │
  └────────────────────────────────────────────────────────┘
                       │
                       ▼
  ┌────────────────────────────────────────────────────────┐
  │  GOLD LAYER (Multi-Tenant Aggregated Metrics)          │
  │  - ClickHouse SummingMergeTree engines                 │
  │  - Strictly grouped by organisation_id & date          │
  │  - Automated guard test suite                          │
  └────────────────────────────────────────────────────────┘
```

---

## 📂 Project Structure

```
project 1 task/
├── config/
│   ├── bronze_tables.yaml          # Bronze tables schema & file patterns
│   ├── silver_tables.yaml          # Silver deduplication rules & complex joins
│   ├── gold_tables.yaml            # Gold metrics & SummingMergeTree definitions
│   └── auto_generated_portfolios.yaml # Sample CLI auto-generated schema
├── dags/
│   └── medallion_dag.py            # Airflow DAG definition (medallion_pipeline_engine)
├── engine/
│   ├── pipeline_engine.py          # Core Medallion execution engine
│   └── schema_helper.py            # Standalone CLI tool to auto-generate Bronze YAML
├── generator/
│   └── synthetic_data_generator.py # Synthetic ORC generator (10 tables, 3 orgs, 7 dates)
├── tests/
│   └── test_medallion_pipeline.py  # 6-phase test suite validating all criteria
├── docker-compose.yml              # Local Airflow, MinIO, and ClickHouse stack
└── README.md                       # Comprehensive documentation & review answers
```

---

## 🚀 Quickstart & Execution

### 1. Generate Synthetic ORC Lake Data (10+ Tables)
Generates 10 financial tables across 3 organisations (`org_alpha`, `org_beta`, `org_gamma`) and 7 dates (`2026-03-01` to `2026-03-07`) with deliberately seeded collisions:
```bash
python generator/synthetic_data_generator.py
```

### 2. Auto-Generate Draft Bronze YAML from any ORC File
Inspect any raw ORC file to automatically extract data types and produce a valid Bronze configuration:
```bash
python engine/schema_helper.py data/raw_lake/portfolios/organisation_id=org_alpha/processing_date=2026-03-01/data.orc -o config/auto_generated_portfolios.yaml
```

### 3. Run the Medallion Pipeline Engine (Bronze $\rightarrow$ Silver $\rightarrow$ Gold)
```bash
python engine/pipeline_engine.py
```

### 4. Execute Full Verification Test Suite
```bash
python -m pytest tests/test_medallion_pipeline.py -v
```

---

## 💬 Review Q&A Check

### Q1: Are partition columns populated across all 10+ Bronze tables?
**Answer:**  
**Yes.** All 10+ Bronze tables use ClickHouse's path-introspection function `extract(_path, 'organisation_id=([^/]+)')` and `toDate(extract(_path, 'processing_date=([^/]+)'))`.  
Our automated test `test_bronze_partition_columns_populated` asserts that `countIf(organisation_id IS NULL OR organisation_id = '') == 0` and `countIf(processing_date = '1970-01-01') == 0` across all 10 tables (`accounts`, `contacts`, `portfolios`, `securities`, `holdings`, `trades`, `transactions`, `advisors`, `fee_schedules`, `statements`).

### Q2: Explain your Silver relational normalization model.
**Answer:**  
- **Third Normal Form (3NF) Separation:** Raw vendor files frequently bundle entity properties together. Silver splits and normalizes them into distinct relational entities with clean foreign key links:
  - `silver_accounts` (Account master, status, currency)
  - `silver_contacts` (Account persons with clean emails and concatenated full names)
  - `silver_portfolios` (Portfolio strategies linked via `account_id`)
  - `silver_securities` (Securities master catalog linked via `security_id`)
  - `silver_holdings` (Positions referencing `portfolio_id` and `security_id`)
- **Deduplication:** Raw files often contain duplicate updates. Silver enforces exactly 1 row per business key per organisation per date using:
  ```sql
  ROW_NUMBER() OVER (
      PARTITION BY organisation_id, processing_date, account_id 
      ORDER BY parseDateTimeBestEffort(updated_at) DESC
  ) = 1
  ```
- **State History & Enrichment:** Built `silver_portfolio_holdings_enriched` joining `holdings` $\rightarrow$ `portfolios` $\rightarrow$ `accounts` $\rightarrow$ `securities` with tenant guards on every `ON` predicate (`h.organisation_id = p.organisation_id`).

### Q3: Show how your Silver table handles the seeded multi-tenant collision.
**Answer:**  
We deliberately seeded an account with identical primary key `ACC-COLLISION-999` under both `org_alpha` and `org_beta`.
Because the Silver primary key and partition key are defined as `(organisation_id, account_id)` and deduplication windows partition by `organisation_id, processing_date, account_id`:
- `org_alpha` retains its account: `Collision Account Owned By ORG_ALPHA`
- `org_beta` retains its account: `Collision Account Owned By ORG_BETA`
- Neither tenant's data bleeds or overwrites the other (verified by `test_multi_tenant_collision_isolation`).

### Q4: Why was this specific ClickHouse engine chosen for the Gold table?
**Answer:**  
We selected **`SummingMergeTree`** for our Gold aggregate tables (`gold_daily_organisation_aum`, `gold_organisation_asset_allocation`, `gold_organisation_trading_volume`):
1. **Additive Rollups:** Financial metrics such as total market value, total cost basis, gross volume, and commission fees are strictly additive across daily partitions.
2. **Background Compression:** `SummingMergeTree` automatically merges and sums numerical fields during ClickHouse background merges without requiring full table re-scans.
3. **Multi-Tenant Partitioning:** Configured with `PARTITION BY toYYYYMM(processing_date)` and `ORDER BY (organisation_id, processing_date, ...)`, which guarantees data locality per tenant and fast multi-tenant point queries.
