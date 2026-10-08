from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

# Ensure engine path is importable
ENGINE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ENGINE_PATH not in sys.path:
    sys.path.insert(0, ENGINE_PATH)

from engine.pipeline_engine import MedallionPipelineEngine
from generator.synthetic_data_generator import generate_and_save_dataset

default_args = {
    'owner': 'data_platform',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'medallion_pipeline_engine',
    default_args=default_args,
    description='Configuration-driven Medallion Pipeline (Bronze -> Silver -> Gold) with ClickHouse',
    schedule_interval='@daily',
    catchup=False,
    tags=['medallion', 'clickhouse', 'minio', 'multi-tenant'],
)

def run_synthetic_data_generation():
    raw_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "raw_lake"))
    print(f"Generating synthetic ORC files into {raw_dir}...")
    stats = generate_and_save_dataset(raw_dir)
    print("Generation complete:", stats)
    return stats

def run_bronze_ingestion():
    engine = MedallionPipelineEngine()
    return engine.build_bronze_layer()

def run_silver_transformation():
    engine = MedallionPipelineEngine()
    return engine.build_silver_layer()

def run_gold_aggregation():
    engine = MedallionPipelineEngine()
    return engine.build_gold_layer()

task_generate_orc = PythonOperator(
    task_id='generate_synthetic_orc_lake',
    python_callable=run_synthetic_data_generation,
    dag=dag,
)

task_bronze = PythonOperator(
    task_id='bronze_partitioned_ingestion',
    python_callable=run_bronze_ingestion,
    dag=dag,
)

task_silver = PythonOperator(
    task_id='silver_dedup_and_normalize',
    python_callable=run_silver_transformation,
    dag=dag,
)

task_gold = PythonOperator(
    task_id='gold_multitenant_aggregation',
    python_callable=run_gold_aggregation,
    dag=dag,
)

# Pipeline DAG Dependencies
task_generate_orc >> task_bronze >> task_silver >> task_gold
