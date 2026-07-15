from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='olist_e2e_dwh_pipeline',
    default_args=default_args,
    description='ETL pipeline for Olist E-Commerce Analytics DWH',
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['olist', 'dwh', 'postgres'],
) as dag:

    init_schema = SQLExecuteQueryOperator(
        task_id='init_dwh_schema',
        conn_id='postgres_dwh',
        sql='sql/01_init_schema.sql'
    )

    load_raw_data = BashOperator(
        task_id='extract_and_load_raw',
        bash_command='python3 /opt/airflow/scripts/load_raw.py'
    )

    transform_staging = SQLExecuteQueryOperator(
        task_id='run_staging_transformations',
        conn_id='postgres_dwh',
        sql='sql/02_staging.sql'
    )

    build_marts = SQLExecuteQueryOperator(
        task_id='build_analytical_marts',
        conn_id='postgres_dwh',
        sql='sql/03_marts.sql'
    )

    # Логика выполнения (Pipeline dependencies)
    init_schema >> load_raw_data >> transform_staging >> build_marts