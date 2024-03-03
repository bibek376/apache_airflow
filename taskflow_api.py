from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.decorators import dag,task



# Define default arguments
default_args = {
    'owner': 'bibek',
    'start_date': datetime(2024, 2, 28),
    'retries': 1,
}

# Instantiate your DAG old style
#dag = DAG('my_first_dag', default_args=default_args, schedule_interval=None)

@dag(dag_id='dag_code_in_different_style',
            default_args=default_args,
            tags=['dependencies','python','taskflow_api']
            )


def dag_with_taskflow_api():

    @task
    def task1():
        print("Executing Task 1")

    @task
    def task2():
        print("Executing Task 2")

    @task
    def task3():
        print("Executing Task 3")

    @task
    def task4():
        print("Executing Task 4")

    @task
    def task5():
        print("Executing Task 5")

    # Set task dependencies
    task1() >> [ task2(),task3(),task4()] >> task5()

dag_with_taskflow_api()














