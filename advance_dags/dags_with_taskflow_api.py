from airflow import DAG
from datetime import datetime,timedelta
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.decorators import dag,task

default_args={
    'owner':'bibek',
    'retries':1
}

@dag(dag_id='dag_with_taskflow',
    description='DAG with taskflow',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='* * * * *',
    tags=['Python','Operators','taskflow API'])

def main_function():

    @task
    def test_func1():
        pass

    @task
    def test_func2():
        pass

    @task
    def test_func3():
        pass

    @task
    def test_func4():
        pass

    @task 
    def test_func5():
        pass

    @task
    def test_func6():
        pass

    test_func1() >> [ test_func2(), test_func3(), test_func4() ] >> test_func5() >> test_func6() 
main_function()















