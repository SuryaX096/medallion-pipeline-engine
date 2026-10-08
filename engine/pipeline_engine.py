import os
import subprocess
import yaml
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("MedallionEngine")

class ClickHouseExecutor:
    """
    Executes ClickHouse queries reliably.
    If ClickHouse HTTP server (localhost:8123 or $CLICKHOUSE_HOST:$CLICKHOUSE_PORT) is accessible,
    it executes over HTTP via clickhouse-connect.
    If not, it falls back seamlessly to `clickhouse local` in WSL Ubuntu.
    """
    def __init__(self, host=None, port=None, username=None, password=None, data_root=None):
        self.host = host or os.environ.get("CLICKHOUSE_HOST", "localhost")
        self.port = int(port or os.environ.get("CLICKHOUSE_PORT", "8123"))
        self.username = username or os.environ.get("CLICKHOUSE_USER", "admin")
        self.password = password or os.environ.get("CLICKHOUSE_PASSWORD", "admin123")
        self.data_root = data_root or os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "raw_lake"))
        self.wsl_data_root = self._to_wsl_path(self.data_root)
        self.client = None
        self._init_client()

    def _init_client(self):
        try:
            import clickhouse_connect
            self.client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                connect_timeout=3,
                send_receive_timeout=15
            )
            # Test query
            self.client.command("SELECT 1")
            logger.info(f"Connected to ClickHouse server at {self.host}:{self.port} as {self.username}")
        except Exception as e:
            logger.warning(f"Could not connect to ClickHouse server ({e}). Using native ClickHouse local runner.")
            self.client = None

    def _to_wsl_path(self, win_path):
        win_path = os.path.abspath(win_path).replace("\\", "/")
        if ":" in win_path:
            drive, rest = win_path.split(":", 1)
            return f"/mnt/{drive.lower()}{rest}"
        return win_path

    def run_query(self, query, format_output="TabSeparated"):
        full_query = query.strip()
        if self.client:
            try:
                res = self.client.query(full_query)
                return "\n".join(["\t".join(str(val) for val in row) for row in res.result_rows])
            except Exception as e:
                logger.warning(f"HTTP server query failed ({e}). Falling back to ClickHouse local.")

        # Fallback to clickhouse local in WSL
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

    def execute_command(self, query):
        if self.client:
            try:
                return self.client.command(query)
            except Exception as e:
                logger.warning(f"HTTP server command failed ({e}). Falling back to ClickHouse local.")

        cmd = [
            "wsl", "-d", "Ubuntu", "-e",
            "/home/surya/bin/clickhouse", "local",
            "--query", query
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode != 0:
            raise RuntimeError(f"ClickHouse execution error:\nSTDOUT: {proc.stdout}\nSTDERR: {proc.stderr}\nQUERY: {query}")
        return proc.stdout.strip()


class MedallionPipelineEngine:
    def __init__(self, config_dir=None):
        self.config_dir = config_dir or os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "config"))
        self.executor = ClickHouseExecutor()
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
        cols = ", ".join(self.bronze_cfg["tables"][table_name]["columns"].keys())
        if self.executor.client:
            # Inside ClickHouse container user_files mount
            file_glob = f"raw_lake/{table_name}/*/*/*.orc"
        else:
            # Standalone ClickHouse local runner
            file_glob = f"{self.executor.wsl_data_root}/{table_name}/*/*/*.orc"

        return f"""
        SELECT
            extract(_path, 'organisation_id=([^/]+)') AS organisation_id,
            toDate(extract(_path, 'processing_date=([^/]+)')) AS processing_date,
            {cols},
            _path AS _source_file
        FROM file('{file_glob}', ORC)
        """

    def build_bronze_layer(self):
        logger.info("[BRONZE] Building Bronze partitioned ingestion layer for all 10+ tables...")
        results = {}
        for table_name in self.bronze_cfg["tables"].keys():
            sql = f"SELECT count() FROM ({self.get_bronze_query(table_name)})"
            count = int(self.executor.run_query(sql, format_output="TabSeparated"))
            bronze_table_name = f"bronze_{table_name}"
            results[bronze_table_name] = count
            logger.info(f"  [OK] {bronze_table_name}: Ingestion verified ({count} rows indexed)")
        return results

    # ==========================================
    # SILVER LAYER
    # ==========================================
    def get_silver_query(self, table_name):
        base_name = table_name.replace("silver_", "")
        bronze_sql = self.get_bronze_query(base_name)

        if table_name == "silver_accounts":
            return f"""
            SELECT
                organisation_id,
                processing_date,
                account_id,
                account_name,
                status,
                currency,
                advisor_id,
                parseDateTimeBestEffort(opened_at) AS opened_at,
                parseDateTimeBestEffort(updated_at) AS updated_at
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
                strategy,
                risk_profile,
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
                ticker,
                security_name,
                asset_class,
                currency,
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
                side,
                quantity,
                price,
                trade_value,
                commission,
                parseDateTimeBestEffort(trade_timestamp) AS trade_timestamp,
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
                transaction_type,
                amount,
                currency,
                parseDateTimeBestEffort(transaction_timestamp) AS transaction_timestamp,
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
                advisor_name,
                email,
                region,
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
                fee_type,
                rate,
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
                toDate(statement_date) AS statement_date,
                total_value,
                currency,
                parseDateTimeBestEffort(updated_at) AS updated_at
            FROM (
                SELECT *,
                       row_number() OVER (
                           PARTITION BY organisation_id, processing_date, statement_id 
                           ORDER BY parseDateTimeBestEffort(updated_at) DESC
                       ) as rn
                FROM ({bronze_sql})
            )
            WHERE rn = 1
            """
        else:
            raise ValueError(f"Unknown silver table: {table_name}")

    def get_enriched_holdings_query(self):
        holdings_sql = self.get_silver_query("silver_holdings")
        portfolios_sql = self.get_silver_query("silver_portfolios")
        accounts_sql = self.get_silver_query("silver_accounts")
        securities_sql = self.get_silver_query("silver_securities")
        return f"""
        SELECT
            h.organisation_id AS organisation_id,
            h.processing_date AS processing_date,
            h.holding_id AS holding_id,
            h.portfolio_id AS portfolio_id,
            p.portfolio_name AS portfolio_name,
            p.strategy AS strategy,
            p.risk_profile AS risk_profile,
            p.account_id AS account_id,
            a.account_name AS account_name,
            a.status AS account_status,
            h.security_id AS security_id,
            s.ticker AS ticker,
            s.security_name AS security_name,
            s.asset_class AS asset_class,
            h.quantity AS quantity,
            h.market_value AS market_value,
            h.cost_basis AS cost_basis,
            h.as_of_date AS as_of_date,
            h.updated_at AS updated_at
        FROM ({holdings_sql}) h
        INNER JOIN ({portfolios_sql}) p 
            ON h.portfolio_id = p.portfolio_id 
           AND h.organisation_id = p.organisation_id
        INNER JOIN ({accounts_sql}) a 
            ON p.account_id = a.account_id 
           AND p.organisation_id = a.organisation_id
        LEFT JOIN ({securities_sql}) s 
            ON h.security_id = s.security_id 
           AND h.organisation_id = s.organisation_id
        """

    def build_silver_layer(self):
        logger.info("\n[SILVER] Validating Silver deduplication and relational normalization layer...")
        results = {}
        for table_name in self.silver_cfg["tables"].keys():
            sql = f"SELECT count() FROM ({self.get_silver_query(table_name)})"
            count = int(self.executor.run_query(sql, format_output="TabSeparated"))
            results[table_name] = count
            logger.info(f"  [OK] {table_name}: Deduplicated & normalized ({count} rows)")

        # Enriched join validation
        sql = f"SELECT count() FROM ({self.get_enriched_holdings_query()})"
        count = int(self.executor.run_query(sql, format_output="TabSeparated"))
        results["silver_portfolio_holdings_enriched"] = count
        logger.info(f"  [OK] silver_portfolio_holdings_enriched: Enriched relational join verified ({count} rows)")

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
                count() AS position_count
            FROM ({holdings_sql})
            GROUP BY organisation_id, processing_date
            """
        elif table_name == "gold_organisation_asset_allocation":
            enriched_sql = self.get_enriched_holdings_query()
            return f"""
            SELECT
                organisation_id,
                processing_date,
                asset_class,
                round(sum(market_value), 2) AS total_allocation_value,
                count() AS asset_count
            FROM ({enriched_sql})
            GROUP BY organisation_id, processing_date, asset_class
            """
        elif table_name == "gold_organisation_trading_volume":
            trades_sql = self.get_silver_query("silver_trades")
            return f"""
            SELECT
                organisation_id,
                processing_date,
                side,
                round(sum(trade_value), 2) AS total_gross_volume,
                round(sum(commission), 2) AS total_commissions,
                round(sum(quantity), 4) AS total_quantity,
                count() AS trade_count
            FROM ({trades_sql})
            GROUP BY organisation_id, processing_date, side
            """
        else:
            raise ValueError(f"Unknown gold table: {table_name}")

    def build_gold_layer(self):
        logger.info("\n[GOLD] Validating Gold multi-tenant aggregation layer...")
        results = {}
        for gold_table in self.gold_cfg["tables"].keys():
            sql = f"SELECT count() FROM ({self.get_gold_query(gold_table)})"
            count = int(self.executor.run_query(sql, format_output="TabSeparated"))
            results[gold_table] = count
            logger.info(f"  [OK] {gold_table}: Aggregated ({count} metric buckets)")
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
