# etl_dag.py
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl_dag',
    default_args=default_args,
    description='A simple DAG for ETL',
    schedule_interval=timedelta(days=1),
)

etl_task = BashOperator(
    task_id='run_etl',
    bash_command='python /path/to/your/etl_script.py',
    dag=dag,
)

dag
