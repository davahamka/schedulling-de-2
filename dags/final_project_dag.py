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

    # task2 = BashOperator(
    #     task_id='second_task',
    #     bash_command="echo hey, I am task2 and will be running after task1!"
    # )

    # task3 = BashOperator(
    #     task_id='thrid_task',
    #     bash_command="echo hey, I am task3 and will be running after task1 at the same time as task2!"
    # )

    # Task dependency method 1
    # task1.set_downstream(task2)
    # task1.set_downstream(task3)

    # Task dependency method 2
    # task1 >> task2
    # task1 >> task3

    # Task dependency method 3
    task1 >> task2
