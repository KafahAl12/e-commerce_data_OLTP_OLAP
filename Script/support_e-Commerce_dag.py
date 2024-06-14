from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Default arg for DAG
default_args_management = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Make DAG
dag_management = DAG(
    'etl_management_tasks',
    default_args=default_args_management,
    description='DAG untuk mengelola pekerjaan ETL',
    schedule_interval=None,  # Dag running with automatic trigger
    start_date=datetime(2024, 1, 1),
    catchup=False
)

# Cleaning parquet files
clean_temp_files = BashOperator(
    task_id='clean_temp_files',
    bash_command='rm -f /home/kafah/parquet_files/*.parquet',
    dag=dag_management,
)

# Task to check job status
def check_job_status():
    print("Semua pekerjaan berjalan dengan baik.")

check_status = PythonOperator(
    task_id='check_status',
    python_callable=check_job_status,
    dag=dag_management,
)

clean_temp_files >> check_status
