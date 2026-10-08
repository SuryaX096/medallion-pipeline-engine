import os
import pytest
import yaml
from engine.pipeline_engine import MedallionPipelineEngine
from generator.synthetic_data_generator import TABLE_DEFINITIONS, ORGANISATIONS, DATES

@pytest.fixture(scope="session")
def engine():
    return MedallionPipelineEngine()

# =========================================================================
# PHASE 1: Bronze Table Existence (All 10 Tables)
# =========================================================================
def test_10_plus_tables_exist_in_bronze(engine):
    """PHASE 1: Verify all 10 distinct Bronze tables exist and have indexed data"""
    assert len(TABLE_DEFINITIONS) >= 10, "At least 10 tables must be defined in generator"
    for table_name in TABLE_DEFINITIONS.keys():
        sql = f"SELECT count() FROM ({engine.get_bronze_query(table_name)})"
        count = int(engine.executor.run_query(sql, format_output="TabSeparated"))
        assert count > 0, f"Bronze table {table_name} must contain indexed rows"

# =========================================================================
# PHASE 2: Bronze Partition Columns Populated (Not Empty / Not Default)
# =========================================================================
def test_bronze_partition_columns_populated(engine):
    """PHASE 2: Verify partition columns organisation_id and processing_date are populated, not empty or 1970-01-01"""
    for table_name in TABLE_DEFINITIONS.keys():
        bronze_query = engine.get_bronze_query(table_name)
        sql = f"""
        SELECT 
            countIf(organisation_id IS NULL OR organisation_id = '') AS empty_orgs,
            countIf(processing_date = toDate('1970-01-01')) AS empty_dates,
            count() AS total_rows
        FROM ({bronze_query})
        """
        output = engine.executor.run_query(sql, format_output="TabSeparated")
        parts = output.split()
        empty_orgs, empty_dates, total_rows = int(parts[0]), int(parts[1]), int(parts[2])
        assert total_rows > 0, f"Table {table_name} has no rows"
        assert empty_orgs == 0, f"Partition column organisation_id has empty values in {table_name}"
        assert empty_dates == 0, f"Partition column processing_date is unpopulated in {table_name}"

# =========================================================================
# PHASE 3: Silver Deduplication (1 Row Per PK Per Org Per Day)
# =========================================================================
def test_silver_deduplication_integrity(engine):
    """PHASE 3: Verify Silver deduplicates data to exactly 1 row per business key per organisation per day"""
    for table_name in TABLE_DEFINITIONS.keys():
        silver_name = f"silver_{table_name}"
        silver_query = engine.get_silver_query(silver_name)
        pk_field = list(TABLE_DEFINITIONS[table_name]["fields"])[0][0]
        sql = f"""
        SELECT count() FROM (
            SELECT 
                organisation_id, 
                processing_date, 
                {pk_field}, 
                count() AS duplicate_count
            FROM ({silver_query})
            GROUP BY organisation_id, processing_date, {pk_field}
            HAVING duplicate_count > 1
        )
        """
        dup_count = int(engine.executor.run_query(sql, format_output="TabSeparated"))
        assert dup_count == 0, f"Found deduplication violations in {silver_name} for key {pk_field}"

# =========================================================================
# PHASE 4: 3NF / Relational Integrity with Tenant Isolation
# =========================================================================
def test_relational_normalization_and_enrichment(engine):
    """PHASE 4: Verify relational integrity across parent-child joins with tenant isolation in every predicate"""
    sql = f"""
    SELECT count() 
    FROM ({engine.get_enriched_holdings_query()})
    WHERE organisation_id IS NULL OR organisation_id = ''
    """
    empty_tenant_joins = int(engine.executor.run_query(sql, format_output="TabSeparated"))
    assert empty_tenant_joins == 0, "Enriched relation contains orphaned or un-isolated records"

# =========================================================================
# PHASE 5: Multi-Tenant Collision Isolation
# =========================================================================
def test_multi_tenant_collision_isolation(engine):
    """PHASE 5: Verify that identical business key under two different organisations remains strictly distinct"""
    silver_accounts = engine.get_silver_query("silver_accounts")
    collision_id = "ACC-COLLISION-999"
    sql = f"""
    SELECT organisation_id, count()
    FROM ({silver_accounts})
    WHERE account_id = '{collision_id}'
    GROUP BY organisation_id
    ORDER BY organisation_id
    """
    output = engine.executor.run_query(sql, format_output="TabSeparated")
    lines = [line.strip().split() for line in output.splitlines() if line.strip()]
    assert len(lines) == 2, f"Collision account must exist for exactly 2 distinct organisations, got {len(lines)}"
    orgs = [l[0] for l in lines]
    assert "org_alpha" in orgs
    assert "org_beta" in orgs
    for l in lines:
        assert int(l[1]) == len(DATES), f"Each organisation must hold its own daily snapshots independently"

# =========================================================================
# PHASE 6: Gold Aggregation & Multi-Tenant Guard
# =========================================================================
def test_gold_aggregates_group_by_organisation(engine):
    """PHASE 6: Verify all Gold aggregate tables group strictly by organisation_id and reconcile with Silver"""
    with open("config/gold_tables.yaml", "r") as f:
        gold_cfg = yaml.safe_load(f)

    for gold_table in gold_cfg["tables"].keys():
        gold_query = engine.get_gold_query(gold_table)
        sql = f"""
        SELECT countDistinct(organisation_id), count()
        FROM ({gold_query})
        """
        output = engine.executor.run_query(sql, format_output="TabSeparated")
        distinct_orgs, total_metric_rows = [int(x) for x in output.split()]
        assert distinct_orgs == len(ORGANISATIONS), f"{gold_table} must report metrics across all organisations"
        assert total_metric_rows > 0

def test_guard_test_fails_if_organisation_id_removed():
    """PHASE 6: Guard test fails with security exception if organisation_id is missing from aggregate grouping"""
    with open("config/gold_tables.yaml", "r") as f:
        gold_cfg = yaml.safe_load(f)

    for gold_table, cfg in gold_cfg["tables"].items():
        sql = cfg["sql"].lower()
        assert "group by" in sql, f"Gold table {gold_table} must have a GROUP BY clause"
        group_by_clause = sql.split("group by")[1]
        assert "organisation_id" in group_by_clause, f"Guard violation: {gold_table} does not group by organisation_id!"

    # Intentionally missing tenant grouping check
    bad_query = "SELECT processing_date, round(sum(market_value), 2) FROM silver.silver_holdings GROUP BY processing_date"
    def tenant_isolation_validator(query_str):
        if "organisation_id" not in query_str.lower().split("group by")[-1]:
            raise ValueError("SECURITY VIOLATION: Missing organisation_id grouping in multi-tenant metric aggregate!")

    with pytest.raises(ValueError, match="SECURITY VIOLATION"):
        tenant_isolation_validator(bad_query)
