from airflow import DAG
from datetime import datetime
# from airflow.operators.python_operator import PythonOperator
from airflow.decorators import task,dag


# Define default arguments
default_args = {
    'owner': 'bibek',
    'start_date': datetime(2024, 2, 28),
    'retries': 1,
}

# Instantiate your DAG
@dag('dag_with_taskflow', default_args=default_args, 
    tags=['python','dependencies','taskflow_api'], schedule_interval=None)

def dag_with_taskflow_api():

    # Define tasks
    @task
    def func1():
        print("Executing Task 1")

    @task
    def func2():
        print("Executing Task 2")

    @task
    def func3():
        print("Executing Task 3")

    @task
    def func4():
        print("Executing Task 4")

    @task
    def func5():
        print("Executing Task 5")

    @task
    def func6():
        print("Executing Task 6")


    # Set task dependencies
    func1() >> func2() >> [func3(), func4(), func5()] >> func6() 


dag_with_taskflow_api()



