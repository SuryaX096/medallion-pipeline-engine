# Phase 1: System and Repository Audit Report

**Project:** DATA-1 — Medallion Pipeline Engine  
**Repository:** [https://github.com/SuryaX096/medallion-pipeline-engine](https://github.com/SuryaX096/medallion-pipeline-engine)  
**Date:** 2026-10-22  
**Auditor:** Senior Data Engineering Architect & QA Team  

---

## 1. Executive Summary
An in-depth inspection of the project repository (`C:\Users\surya\project 1 task`), the Docker runtime environment (`medallion-data-platform`), ClickHouse server (v26.9.12.8), and Apache Airflow (v2.10.5) was conducted. 

The audit explains why previous reviewer checks showed zero tables in ClickHouse despite green Airflow DAG runs:
> **Core Root Cause:** The existing engine executed dynamic queries directly against raw files using ClickHouse's transient `file(...)` table function in memory. While query logic was relationally sound, **no persistent tables (`CREATE TABLE`) or persistent inserts (`INSERT INTO`) were executed against ClickHouse disk storage**. Consequently, `SHOW DATABASES;` and `SHOW TABLES FROM default;` showed zero user tables.

Furthermore, Apache NiFi, dbt transformations, and the two required public data sources (one real-time, one batch) are being implemented to fulfill the reviewer's demonstration requirements.

---

## 2. Component-by-Component Verification

| Component | Status | Verified Findings |
|---|---|---|
| **ClickHouse Server** | 🟡 Partial | Version 26.9.12.8 is running healthy on ports `8123` (HTTP) and `9002->9000` (Native). Admin credentials (`admin:admin123`) work. However, user tables were not materialized to disk. |
| **Airflow 2.10.5** | 🟡 Partial | Webserver active on `http://localhost:8080`, scheduler running, DAG `medallion_pipeline_engine` registered. However, tasks only executed in-memory file scans rather than orchestrating real database materialization and dbt. |
| **PostgreSQL 16** | 🟢 Healthy | Running on port `5432`, serving as the Airflow metadata database. |
| **Synthetic Lake** | 🟢 Complete | 210 logical ORC files generated across 10 financial tables, 3 organisations (`org_alpha`, `org_beta`, `org_gamma`), and 7 dates (`2026-03-01` to `2026-03-07`) with multi-tenant collision `ACC-COLLISION-999`. |
| **Schema Helper CLI** | 🟢 Complete | `engine/schema_helper.py` inspects raw ORC schemas and auto-generates draft Bronze YAML configurations. |
| **dbt Models** | 🔴 Missing | No dbt project exists yet. Batch transformations must be migrated to dbt models targeting ClickHouse with data quality tests. |
| **Apache NiFi** | 🔴 Missing | Not yet running in Docker Compose. Required for real-time streaming pipeline from a public API into ClickHouse Bronze. |
| **Public Data Sources** | 🔴 Missing | Two public sources required: (1) Real-time public API stream for NiFi, (2) Public batch dataset for Airflow + dbt. |

---

## 3. Confirmed Defects & Technical Debt

1. **Lack of Physical Table Persistence:**
   - In `engine/pipeline_engine.py`, `build_bronze_layer()`, `build_silver_layer()`, and `build_gold_layer()` only ran `SELECT count() FROM (SELECT ... FROM file('raw_lake/...', ORC))`.
   - Databases `bronze`, `silver`, `gold` were never initialized on the ClickHouse server, leaving `SHOW TABLES;` empty.
2. **Dual Workspace Inconsistency:**
   - Docker Compose containers were launched under project name `medallion-data-platform` (`C:\Users\surya\internship\medallion-data-platform`) while local git tracking is in `c:\Users\surya\project 1 task`.
   - Both directories must be kept strictly mirrored and unified under the same compose service configuration.
3. **Airflow DAG Independence from Data Writes:**
   - Airflow tasks marked `SUCCESS` because the Python functions finished without exceptions, not because database rows were verified in persistent storage.

---

## 4. Implementation Roadmap

```
Step 1: Repair ClickHouse Persistence & Materialize Bronze, Silver, Gold Tables
Step 2: Implement dbt Project (dbt-clickhouse, models, tests)
Step 3: Integrate Public Batch Dataset (S&P 500 Market Constituents)
Step 4: Deploy Apache NiFi & Public Real-Time Ingestion (Live Market Rates)
Step 5: Orchestrate Unified Batch Pipeline in Airflow
Step 6: Run Comprehensive Test Suite (Pytest + dbt test + Multi-Tenant Validation)
Step 7: Update README & Prepare Truthful 5–8 Minute Demo Plan
```
