from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

def get_project_root():
    if os.path.exists("/opt/airflow/generator") and os.path.exists("/opt/airflow/data"):
        return "/opt/airflow"
    candidate = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    if os.path.exists(os.path.join(candidate, "generator")):
        return candidate
    return os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

PROJECT_ROOT = get_project_root()
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from generator.synthetic_data_generator import generate_and_save_dataset
from engine.pipeline_engine import MedallionPipelineEngine

default_args = {
    'owner': 'data_platform',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'medallion_pipeline_engine',
    default_args=default_args,
    description='DATA-1 Medallion Pipeline Engine (Bronze -> Silver -> Gold) with ClickHouse',
    schedule_interval='@daily',
    catchup=False,
    tags=['medallion', 'clickhouse', 'multi-tenant', 'data-1'],
)

def task_generate_raw_data(**kwargs):
    raw_dir = os.path.join(PROJECT_ROOT, "data", "raw_lake")
    print(f"Generating synthetic ORC files into: {raw_dir}")
    stats = generate_and_save_dataset(raw_dir)
    print("Generation complete:", stats)
    return stats

def task_run_bronze(**kwargs):
    config_dir = os.path.join(PROJECT_ROOT, "config")
    engine = MedallionPipelineEngine(config_dir=config_dir)
    return engine.build_bronze_layer()

def task_run_silver(**kwargs):
    config_dir = os.path.join(PROJECT_ROOT, "config")
    engine = MedallionPipelineEngine(config_dir=config_dir)
    return engine.build_silver_layer()

def task_run_gold(**kwargs):
    config_dir = os.path.join(PROJECT_ROOT, "config")
    engine = MedallionPipelineEngine(config_dir=config_dir)
    return engine.build_gold_layer()

def task_validate_medallion(**kwargs):
    config_dir = os.path.join(PROJECT_ROOT, "config")
    engine = MedallionPipelineEngine(config_dir=config_dir)
    collision_id = "ACC-COLLISION-999"
    sql = f"""
    SELECT organisation_id, count()
    FROM ({engine.get_silver_query('silver_accounts')})
    WHERE account_id = '{collision_id}'
    GROUP BY organisation_id
    """
    res = engine.executor.run_query(sql, format_output="TabSeparated")
    lines = [line.strip().split() for line in res.splitlines() if line.strip()]
    if len(lines) != 2:
        raise ValueError(f"Multi-tenant collision validation failed: {lines}")
    print("Multi-tenant collision guard passed! Results:", lines)
    return True

gen_task = PythonOperator(
    task_id='generate_synthetic_orc_lake',
    python_callable=task_generate_raw_data,
    dag=dag,
)

bronze_task = PythonOperator(
    task_id='bronze_partitioned_ingestion',
    python_callable=task_run_bronze,
    dag=dag,
)

silver_task = PythonOperator(
    task_id='silver_dedup_and_normalize',
    python_callable=task_run_silver,
    dag=dag,
)

gold_task = PythonOperator(
    task_id='gold_multitenant_aggregation',
    python_callable=task_run_gold,
    dag=dag,
)

validation_task = PythonOperator(
    task_id='validate_tenant_collision_isolation',
    python_callable=task_validate_medallion,
    dag=dag,
)

gen_task >> bronze_task >> silver_task >> gold_task >> validation_task
