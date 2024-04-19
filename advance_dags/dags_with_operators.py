from airflow import DAG
from datetime import datetime,timedelta
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

default_args={
    'owner':'bibek',
    'retries':1
}

def task_a():
    print("Task A executed")

def task_b():
    print("Task B executed")

def task_c():
    print("Task C executed")

def task_d():
    print("Task D executed")

def task_e():
    print("Task E executed")

with DAG(
    dag_id='dag_with_operators',
    description='Python operators in DAGs',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='* * * * *',
    tags=['Python','Operators','Basic DAG Workflow']
) as dag:

    taskAA=PythonOperator(
        task_id='taskA',
        python_callable=task_a
    )

    taskBB=PythonOperator(
        task_id='taskB',
        python_callable=task_b
    )

    taskCC=PythonOperator(
        task_id='taskC',
        python_callable=task_c
    )

    taskDD=PythonOperator(
        task_id='taskD',
        python_callable=task_d
    )

    taskEE=PythonOperator(
        task_id='taskE',
        python_callable=task_e
    )


# set the task dependency
taskAA >> [ taskBB , taskCC , taskDD ] >> taskEE

