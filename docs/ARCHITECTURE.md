# Medallion Pipeline Engine Architecture (DATA-1)

## Overview
The **Medallion Pipeline Engine** is an enterprise data engineering solution implementing a strict, multi-tenant Medallion Architecture (**Bronze** $\rightarrow$ **Silver** $\rightarrow$ **Gold**) built from scratch with **ClickHouse**, **Apache Airflow**, **PostgreSQL**, and **PyArrow ORC**.

```
                           RAW OBJECT LAKE
         data/raw_lake/<table_name>/organisation_id=<org>/processing_date=<date>/data.orc
                                       │
                                       ▼
                       BRONZE: PARTITIONED INGESTION
          • 10+ financial raw tables indexed via ClickHouse file engine
          • Virtual path partition extraction (organisation_id, processing_date)
          • Zero data duplication / raw schema preservation
                                       │
                                       ▼
                     SILVER: DEDUPLICATION & 3NF NORMALIZATION
          • Windowed deduplication: ROW_NUMBER() OVER (PARTITION BY org, date, pk ORDER BY updated_at DESC) = 1
          • ReplacingMergeTree(updated_at) for immutable idempotent state compaction
          • 3NF relational normalization across entity boundaries
          • Multi-tenant collision isolation (ACC-COLLISION-999) guaranteed
                                       │
                                       ▼
                       GOLD: MULTI-TENANT AGGREGATION
          • High-performance OLAP rollups using SummingMergeTree
          • Strict mandatory tenant grouping (GROUP BY organisation_id)
          • Business metrics: Daily AUM, Asset Allocation, Execution Volumes
```

---

## 1. Bronze Partitioned Ingestion Layer
- **Input:** 210 logical ORC files (10 tables $\times$ 3 organisations $\times$ 7 processing dates).
- **Partitioning Pattern:** `raw_lake/<table_name>/organisation_id=<org>/processing_date=<date>/data.orc`.
- **Partition Extraction:** In ClickHouse, virtual columns `organisation_id` and `processing_date` are parsed directly from `_path` using regular expressions:
  - `extract(_path, 'organisation_id=([^/]+)') AS organisation_id`
  - `toDate(extract(_path, 'processing_date=([^/]+)')) AS processing_date`
- **Validation:** Automated test `test_bronze_partition_columns_populated` verifies that zero records contain empty strings or default `1970-01-01` dates.

---

## 2. Silver Deduplication & Relational Normalization Layer
- **Deduplication Strategy:** In distributed multi-tenant environments, late-arriving records or repeated raw batch uploads can introduce duplicates. The Silver pipeline enforces:
  ```sql
  ROW_NUMBER() OVER (
      PARTITION BY organisation_id, processing_date, <primary_key>
      ORDER BY updated_at DESC
  ) = 1
  ```
- **ClickHouse Engine:** `ReplacingMergeTree(updated_at)` ordered by `(organisation_id, processing_date, <primary_key>)`.
- **Multi-Tenant Collision Proof:**
  - Seeded collision record: `account_id = 'ACC-COLLISION-999'` in both `org_alpha` and `org_beta`.
  - Both records survive independently with isolated balances and names without collision or leakage because all join conditions and keys are composite: `(organisation_id, account_id)`.
- **3NF Enriched View:**
  - View `silver_portfolio_holdings_enriched` joins `holdings -> portfolios -> accounts -> securities`.
  - Every join predicate contains `AND child.organisation_id = parent.organisation_id`.

---

## 3. Gold Multi-Tenant Aggregation Layer
- **ClickHouse Engine:** `SummingMergeTree`
- **Rationale for Table Engine:**
  - `SummingMergeTree` automatically pre-aggregates numeric values (such as `total_market_value`, `total_gross_volume`, `trade_count`) across rows sharing the same sorting primary key during background merges.
  - Eliminates slow full table scans for real-time dashboards and multi-tenant analytics.
- **Tenant Grouping Guard:**
  - Every aggregate is strictly grouped by `organisation_id`.
  - Automated security guard test `test_guard_test_fails_if_organisation_id_removed` parses the SQL and throws a security exception if any Gold aggregate query lacks tenant grouping.

---

## 4. Airflow Orchestration DAG
The pipeline is orchestrated via [airflow/dags/medallion_dag.py](../airflow/dags/medallion_dag.py):
1. `generate_synthetic_orc_lake`: Synthesizes 210 ORC files.
2. `bronze_partitioned_ingestion`: Validates Bronze tables and schema extraction.
3. `silver_dedup_and_normalize`: Executes deduplication and 3NF views.
4. `gold_multitenant_aggregation`: Rolls up business metrics into SummingMergeTree tables.
5. `validate_tenant_collision_isolation`: Validates collision isolation integrity.
