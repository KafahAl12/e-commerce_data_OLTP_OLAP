from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta

# Get Connection postgres from Airflow Connection
oltp_conn = BaseHook.get_connection('oltp_postgres')
olap_conn = BaseHook.get_connection('olap_postgres')

jdbc_url_oltp = f"jdbc:postgresql://{oltp_conn.host}:{oltp_conn.port}/{oltp_conn.schema}"
jdbc_url_olap = f"jdbc:postgresql://{olap_conn.host}:{olap_conn.port}/{olap_conn.schema}"
db_user = oltp_conn.login
db_password = oltp_conn.password

# Default arg for DAGs
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Make DAG
dag = DAG(
    'e_Commerce_OLTP_OLAP',
    default_args=default_args,
    description='ETL DAG for transforming OLTP data to OLAP data',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False
)

# Make command template for bash task
bash_command_template = """
export JDBC_URL_OLTP={jdbc_url_oltp} && \
export JDBC_URL_OLAP={jdbc_url_olap} && \
export DB_USER={db_user} && \
export DB_PASSWORD={db_password} && \
spark-submit \
    --jars /home/kafah/postgresql-42.2.26.jar \
    --conf spark.driver.memory=1g \
    --conf spark.executor.memory=1g \
    /home/kafah/PySpark_script/{script}.py
"""

# First task bash for read data OLTP database
t1 = BashOperator(
    task_id='read_oltp_data',
    bash_command=bash_command_template.format(jdbc_url_oltp=jdbc_url_oltp, jdbc_url_olap=jdbc_url_olap, db_user=db_user, db_password=db_password, script='read_oltp_data'),
    dag=dag,
)

# Second task bash for transform data
t2 = BashOperator(
    task_id='transform_data',
    bash_command=bash_command_template.format(jdbc_url_oltp=jdbc_url_oltp, jdbc_url_olap=jdbc_url_olap, db_user=db_user, db_password=db_password, script='transform'),
    dag=dag,
)

# Third task bash for write data to OLAP database
t3 = BashOperator(
    task_id='write_olap_data',
    bash_command=bash_command_template.format(jdbc_url_oltp=jdbc_url_oltp, jdbc_url_olap=jdbc_url_olap, db_user=db_user, db_password=db_password, script='write_olap_data'),
    dag=dag,
)

# Trigger the management DAG after this DAG succeeds
trigger_management_dag = TriggerDagRunOperator(
    task_id='trigger_etl_management_tasks',
    trigger_dag_id='etl_management_tasks',
    dag=dag,
)

# Define task dependencies
t1 >> t2 >> t3 >> trigger_management_dag
