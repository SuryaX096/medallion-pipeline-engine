import os
import subprocess
import yaml

class ClickHouseLocalRunner:
    """
    Executes ClickHouse queries using `clickhouse local` in WSL Ubuntu.
    This guarantees 100% reliability with zero background daemon dependency.
    """
    def __init__(self, data_root=None):
        self.data_root = data_root or os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "raw_lake"))
        self.wsl_data_root = self._to_wsl_path(self.data_root)

    def _to_wsl_path(self, win_path):
        win_path = os.path.abspath(win_path).replace("\\", "/")
        if ":" in win_path:
            drive, rest = win_path.split(":", 1)
            return f"/mnt/{drive.lower()}{rest}"
        return win_path

    def run_query(self, query, format_output="TabSeparated"):
        full_query = query.strip()
        if format_output and not full_query.upper().endswith("FORMAT " + format_output.upper()):
            full_query += f" FORMAT {format_output}"

        cmd = [
            "wsl", "-d", "Ubuntu", "-e",
            "/home/surya/bin/clickhouse", "local",
            "--query", full_query
        ]
        
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode != 0:
            raise RuntimeError(f"ClickHouse execution error:\nSTDOUT: {proc.stdout}\nSTDERR: {proc.stderr}\nQUERY: {query}")
        return proc.stdout.strip()

class MedallionPipelineEngine:
    def __init__(self, config_dir=None):
        self.config_dir = config_dir or os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "config"))
        self.runner = ClickHouseLocalRunner()
        self.bronze_cfg = self._load_yaml("bronze_tables.yaml")
        self.silver_cfg = self._load_yaml("silver_tables.yaml")
        self.gold_cfg = self._load_yaml("gold_tables.yaml")

    def _load_yaml(self, filename):
        path = os.path.join(self.config_dir, filename)
        with open(path, "r") as f:
            return yaml.safe_load(f)

    # ==========================================
    # BRONZE LAYER
    # ==========================================
    def get_bronze_query(self, table_name):
        """
        Returns ClickHouse SQL query defining the Bronze layer over raw partitioned ORC files.
        Extracts folder-based partition columns (organisation_id, processing_date).
        """
        cols = ", ".join(self.bronze_cfg["tables"][table_name]["columns"].keys())
        file_glob = f"{self.runner.wsl_data_root}/{table_name}/*/*/*.orc"
        return f"""
        SELECT
            extract(_path, 'organisation_id=([^/]+)') AS organisation_id,
            toDate(extract(_path, 'processing_date=([^/]+)')) AS processing_date,
            {cols},
            _path AS _source_file
        FROM file('{file_glob}', ORC)
        """

    def build_bronze_layer(self):
        print("[BRONZE] Validating Bronze partitioned ingestion queries for all 10+ tables...")
        results = {}
        for table_name in self.bronze_cfg["tables"].keys():
            sql = f"SELECT count() FROM ({self.get_bronze_query(table_name)})"
            count = int(self.runner.run_query(sql, format_output="TabSeparated"))
            bronze_table_name = f"bronze_{table_name}"
            results[bronze_table_name] = count
            print(f"  [OK] {bronze_table_name}: Bronze partitioned ingestion ready ({count} rows indexed)")
        return results

    # ==========================================
    # SILVER LAYER
    # ==========================================
    def get_silver_query(self, table_name):
        """
        Returns the deduplicated, relationally normalized Silver SQL query.
        Applies row_number() window filtering per (organisation_id, processing_date, PK).
        """
        bronze_sql = self.get_bronze_query(table_name.replace("silver_", ""))
        
        if table_name == "silver_accounts":
            return f"""
            SELECT
                organisation_id,
                processing_date,
                account_id,
                account_number,
                account_name,
                account_type,
                currency,
                status,
                parseDateTimeBestEffort(created_at) AS created_at,
                parseDateTimeBestEffort(updated_at) AS updated_at,
                1 AS is_current
            FROM (
                SELECT *,
                       row_number() OVER (
                           PARTITION BY organisation_id, processing_date, account_id 
                           ORDER BY parseDateTimeBestEffort(updated_at) DESC
                       ) as rn
                FROM ({bronze_sql})
            )
            WHERE rn = 1
            """
        elif table_name == "silver_contacts":
            return f"""
            SELECT
                organisation_id,
                processing_date,
                contact_id,
                account_id,
                first_name,
                last_name,
                concat(first_name, ' ', last_name) AS full_name,
                lower(email) AS email,
                phone,
                role,
                parseDateTimeBestEffort(updated_at) AS updated_at
            FROM (
                SELECT *,
                       row_number() OVER (
                           PARTITION BY organisation_id, processing_date, contact_id 
                           ORDER BY parseDateTimeBestEffort(updated_at) DESC
                       ) as rn
                FROM ({bronze_sql})
            )
            WHERE rn = 1
            """
        elif table_name == "silver_portfolios":
            return f"""
            SELECT
                organisation_id,
                processing_date,
                portfolio_id,
                account_id,
                portfolio_name,
                benchmark_code,
                risk_tolerance,
                toUInt8(is_active) AS is_active,
                parseDateTimeBestEffort(updated_at) AS updated_at
            FROM (
                SELECT *,
                       row_number() OVER (
                           PARTITION BY organisation_id, processing_date, portfolio_id 
                           ORDER BY parseDateTimeBestEffort(updated_at) DESC
                       ) as rn
                FROM ({bronze_sql})
            )
            WHERE rn = 1
            """
        elif table_name == "silver_securities":
            return f"""
            SELECT
                organisation_id,
                processing_date,
                security_id,
                symbol,
                security_name,
                asset_class,
                sector,
                exchange,
                parseDateTimeBestEffort(updated_at) AS updated_at
            FROM (
                SELECT *,
                       row_number() OVER (
                           PARTITION BY organisation_id, processing_date, security_id 
                           ORDER BY parseDateTimeBestEffort(updated_at) DESC
                       ) as rn
                FROM ({bronze_sql})
            )
            WHERE rn = 1
            """
        elif table_name == "silver_holdings":
            return f"""
            SELECT
                organisation_id,
                processing_date,
                holding_id,
                portfolio_id,
                security_id,
                quantity,
                market_value,
                cost_basis,
                unrealized_gain_loss,
                toDate(as_of_date) AS as_of_date,
                parseDateTimeBestEffort(updated_at) AS updated_at
            FROM (
                SELECT *,
                       row_number() OVER (
                           PARTITION BY organisation_id, processing_date, holding_id 
                           ORDER BY parseDateTimeBestEffort(updated_at) DESC
                       ) as rn
                FROM ({bronze_sql})
            )
            WHERE rn = 1
            """
        elif table_name == "silver_trades":
            return f"""
            SELECT
                organisation_id,
                processing_date,
                trade_id,
                portfolio_id,
                security_id,
                trade_type,
                trade_status,
                units,
                execution_price,
                gross_amount,
                commission_fee,
                parseDateTimeBestEffort(executed_at) AS executed_at,
                parseDateTimeBestEffort(updated_at) AS updated_at
            FROM (
                SELECT *,
                       row_number() OVER (
                           PARTITION BY organisation_id, processing_date, trade_id 
                           ORDER BY parseDateTimeBestEffort(updated_at) DESC
                       ) as rn
                FROM ({bronze_sql})
            )
            WHERE rn = 1
            """
        elif table_name == "silver_transactions":
            return f"""
            SELECT
                organisation_id,
                processing_date,
                transaction_id,
                account_id,
                portfolio_id,
                transaction_type,
                amount,
                currency,
                description,
                toDate(effective_date) AS effective_date,
                parseDateTimeBestEffort(updated_at) AS updated_at
            FROM (
                SELECT *,
                       row_number() OVER (
                           PARTITION BY organisation_id, processing_date, transaction_id 
                           ORDER BY parseDateTimeBestEffort(updated_at) DESC
                       ) as rn
                FROM ({bronze_sql})
            )
            WHERE rn = 1
            """
        elif table_name == "silver_advisors":
            return f"""
            SELECT
                organisation_id,
                processing_date,
                advisor_id,
                advisor_code,
                full_name,
                branch_code,
                license_status,
                parseDateTimeBestEffort(updated_at) AS updated_at
            FROM (
                SELECT *,
                       row_number() OVER (
                           PARTITION BY organisation_id, processing_date, advisor_id 
                           ORDER BY parseDateTimeBestEffort(updated_at) DESC
                       ) as rn
                FROM ({bronze_sql})
            )
            WHERE rn = 1
            """
        elif table_name == "silver_fee_schedules":
            return f"""
            SELECT
                organisation_id,
                processing_date,
                fee_schedule_id,
                account_id,
                tier_name,
                rate_bps,
                min_annual_fee,
                billing_frequency,
                toDate(effective_from) AS effective_from,
                parseDateTimeBestEffort(updated_at) AS updated_at
            FROM (
                SELECT *,
                       row_number() OVER (
                           PARTITION BY organisation_id, processing_date, fee_schedule_id 
                           ORDER BY parseDateTimeBestEffort(updated_at) DESC
                       ) as rn
                FROM ({bronze_sql})
            )
            WHERE rn = 1
            """
        elif table_name == "silver_statements":
            return f"""
            SELECT
                organisation_id,
                processing_date,
                statement_id,
                account_id,
                statement_period,
                starting_balance,
                ending_balance,
                total_fees,
                statement_url,
                parseDateTimeBestEffort(generated_at) AS generated_at
            FROM (
                SELECT *,
                       row_number() OVER (
                           PARTITION BY organisation_id, processing_date, statement_id 
                           ORDER BY parseDateTimeBestEffort(generated_at) DESC
                       ) as rn
                FROM ({bronze_sql})
            )
            WHERE rn = 1
            """
        else:
            raise ValueError(f"Unknown silver table: {table_name}")

    def build_silver_layer(self):
        print("\n[SILVER] Validating Silver deduplication and relational normalization layer...")
        results = {}
        for table_name in self.silver_cfg["tables"].keys():
            sql = f"SELECT count() FROM ({self.get_silver_query(table_name)})"
            count = int(self.runner.run_query(sql, format_output="TabSeparated"))
            results[table_name] = count
            print(f"  [OK] {table_name}: Deduplicated & normalized ({count} rows)")
        return results

    # ==========================================
    # GOLD LAYER
    # ==========================================
    def get_gold_query(self, table_name):
        if table_name == "gold_daily_organisation_aum":
            holdings_sql = self.get_silver_query("silver_holdings")
            return f"""
            SELECT
                organisation_id,
                processing_date,
                round(sum(market_value), 2) AS total_market_value,
                round(sum(cost_basis), 2) AS total_cost_basis,
                round(sum(unrealized_gain_loss), 2) AS total_unrealized_pnl,
                count() AS position_count
            FROM ({holdings_sql})
            GROUP BY organisation_id, processing_date
            """
        elif table_name == "gold_organisation_asset_allocation":
            holdings_sql = self.get_silver_query("silver_holdings")
            sec_sql = self.get_silver_query("silver_securities")
            return f"""
            SELECT
                h.organisation_id AS organisation_id,
                h.processing_date AS processing_date,
                s.asset_class AS asset_class,
                round(sum(h.market_value), 2) AS total_allocation_value,
                count() AS asset_count
            FROM ({holdings_sql}) h
            LEFT JOIN ({sec_sql}) s
              ON h.security_id = s.security_id AND h.organisation_id = s.organisation_id
            GROUP BY h.organisation_id, h.processing_date, s.asset_class
            """
        elif table_name == "gold_organisation_trading_volume":
            trades_sql = self.get_silver_query("silver_trades")
            return f"""
            SELECT
                organisation_id,
                processing_date,
                trade_type,
                round(sum(gross_amount), 2) AS total_gross_volume,
                round(sum(commission_fee), 2) AS total_commissions,
                round(sum(units), 4) AS total_units,
                count() AS trade_count
            FROM ({trades_sql})
            GROUP BY organisation_id, processing_date, trade_type
            """
        else:
            raise ValueError(f"Unknown gold table: {table_name}")

    def build_gold_layer(self):
        print("\n[GOLD] Validating Gold multi-tenant aggregation layer...")
        results = {}
        for gold_table in self.gold_cfg["tables"].keys():
            sql = f"SELECT count() FROM ({self.get_gold_query(gold_table)})"
            count = int(self.runner.run_query(sql, format_output="TabSeparated"))
            results[gold_table] = count
            print(f"  [OK] {gold_table}: Aggregated ({count} metric buckets)")
        return results

    def run_entire_pipeline(self):
        print("\n" + "="*70)
        print("  STARTING MEDALLION PIPELINE ENGINE RUN (BRONZE -> SILVER -> GOLD)")
        print("="*70)
        b_res = self.build_bronze_layer()
        s_res = self.build_silver_layer()
        g_res = self.build_gold_layer()
        print("\n" + "="*70)
        print("  MEDALLION PIPELINE EXECUTION FINISHED SUCCESSFULLY!")
        print("="*70)
        return {"bronze": b_res, "silver": s_res, "gold": g_res}

if __name__ == "__main__":
    engine = MedallionPipelineEngine()
    engine.run_entire_pipeline()
