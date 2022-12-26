from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from etl import start_etl


default_args = {
    'owner': 'kelompok2',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


with DAG(
    dag_id='final_project_dag',
    default_args=default_args,
    description='This is our first dag that we write',
    start_date=datetime(2022, 12, 16, 5),
    schedule_interval='@daily'
) as dag:
    task1 = BashOperator(
        task_id='etl_test',
        bash_command="scripts/etl.sh"
    )

    task2 = PythonOperator(
        task_id='extract_test',
        python_callable=start_etl,
    )

    task1 >> task2
