from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import json
from airflow.operators.bash_operator import BashOperator

# Define your extract, transform, and load functions

def extract_data():
    url = 'http://validate.jsontest.com/?json=%7B%22key%22:%22value%22%7D'
    response = requests.get(url)
    if response.status_code == 200:
        return json.dumps(response.json())  # Serialize the JSON response into a string
    else:
        raise Exception('Failed to fetch data from URL')


import logging

def transform_data(data):
    data_dict = json.loads(data)  # Deserialize the JSON string into a dictionary
    data_dict['new_field'] = 'example_value'
    return json.dumps(data_dict)  # Serialize it back into a string to pass to the next task


def load_data(data):
    with open('/home/sourav/airflow/transformed_data.json', 'w') as file:
        file.write(data)


# Define default arguments for your DAG

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 12, 16),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define your DAG

dag = DAG(
    'etl_json_dag',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
)

# Define tasks

extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform_data,
    provide_context=True,
    op_kwargs={'data': '{{ task_instance.xcom_pull(task_ids="extract") }}'},
    dag=dag,
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load_data,
    provide_context=True,
    op_kwargs={'data': '{{ task_instance.xcom_pull(task_ids="transform") }}'},
    dag=dag,
)

# DVC and Git integration tasks

dvc_add = BashOperator(
    task_id='dvc_add',
    bash_command='cd /home/sourav/airflow && dvc add transformed_data.json',
    dag=dag,
)

git_commit = BashOperator(
    task_id='git_commit',
    bash_command='cd /home/sourav/airflow && git add . && git commit -m "Update data file"',
    dag=dag,
)

dvc_push = BashOperator(
    task_id='dvc_push',
    bash_command='cd /home/sourav/airflow && dvc push',
    dag=dag,
)

git_push = BashOperator(
    task_id='git_push',
    bash_command='cd /home/sourav/airflow && git push origin new',
    dag=dag,
)
ssh-keygen

# Set task dependencies

extract_task >> transform_task >> load_task >> dvc_add >> git_commit >> dvc_push >> git_push

# Set task dependencies

extract_task >> transform_task >> load_task

