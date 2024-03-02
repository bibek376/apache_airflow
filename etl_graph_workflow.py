from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.operators.python_operator import PythonOperator



# Define default arguments
default_args = {
    'owner': 'bibek',
    'start_date': datetime(2024, 2, 28),
    'retries': 1,
}

# Instantiate your DAG
dag = DAG('my_first_dag', default_args=default_args, 
          tags=['python','dependencies'], schedule_interval=None)

# Define tasks
def task1():
    print("Executing Task 1")

def task2():
    print("Executing Task 2")

def task3():
    print("Executing Task 3")

def task4():
    print("Executing Task 4")

def task5():
    print("Executing Task 5")

def task6():
    print("Executing Task 6")

def task7():
    print("Executing Task 7")

def task8():
    print("Executing Task 8")

task_1 = PythonOperator(
    task_id='task_1',
    python_callable=task1,
    dag=dag,
)

task_2 = PythonOperator(
    task_id='task_2',
    python_callable=task2,
    dag=dag,
)

task_3 = PythonOperator(
    task_id='task_3',
    python_callable=task3,
    dag=dag,
)

task_4 = PythonOperator(
    task_id='task_4',
    python_callable=task4,
    dag=dag,
)

task_5 = PythonOperator(
    task_id='task_5',
    python_callable=task5,
    dag=dag,
)

task_6 = PythonOperator(
    task_id='task_6',
    python_callable=task6,
    dag=dag,
)

task_7 = PythonOperator(
    task_id='task_7',
    python_callable=task7,
    dag=dag,
)

task_8 = PythonOperator(
    task_id='final_task',
    python_callable=task8,
    dag=dag,
)



# Set task dependencies
#task_1 >> task_2 >> task_3

# task_1 >> task_2 >> [task_3, task_4, task_5] >> task_6

task_1 >> task_3
task_2 >> [ task_4, task_5 ]
task_3 >> task_6
[ task_4, task_5 ] >> task_7
[ task_6 , task_7 ] >> task_8


