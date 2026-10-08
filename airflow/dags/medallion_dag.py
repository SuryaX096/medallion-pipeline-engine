from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

ENGINE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ENGINE_PATH not in sys.path:
    sys.path.insert(0, ENGINE_PATH)

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
    raw_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "data", "raw_lake"))
    print(f"Generating synthetic ORC files into: {raw_dir}")
    stats = generate_and_save_dataset(raw_dir)
    print("Generation complete:", stats)
    return stats

def task_run_bronze(**kwargs):
    engine = MedallionPipelineEngine()
    return engine.build_bronze_layer()

def task_run_silver(**kwargs):
    engine = MedallionPipelineEngine()
    return engine.build_silver_layer()

def task_run_gold(**kwargs):
    engine = MedallionPipelineEngine()
    return engine.build_gold_layer()

def task_validate_medallion(**kwargs):
    engine = MedallionPipelineEngine()
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
