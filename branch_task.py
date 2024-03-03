from airflow import DAG
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator
from datetime import datetime

# Define the Python function to decide which branch to take
def decide_branch(**kwargs):
    current_date = datetime.now()
    if current_date.day % 2 == 0:
        return 'even_branch'
    else:
        return 'odd_branch'

# Define the tasks for even branch
def even_branch_task(**kwargs):
    print("Running tasks for even branch")

# Define the tasks for odd branch
def odd_branch_task(**kwargs):
    print("Running tasks for odd branch")

# Define the default arguments
default_args = {
    'owner': 'bibek'
}

# Instantiate the DAG
with DAG('branching_example', default_args=default_args, schedule_interval=None) as dag:
    
    # Define the BranchPythonOperator to decide the branch
    decide_branch_task = BranchPythonOperator(
        task_id='decide_branch_task',
        python_callable=decide_branch,
        provide_context=True,
    )
    
    # Define the tasks for the even branch
    even_branch = PythonOperator(
        task_id='even_branch_task',
        python_callable=even_branch_task,
        provide_context=True,
    )
    
    # Define the tasks for the odd branch
    odd_branch = PythonOperator(
        task_id='odd_branch_task',
        python_callable=odd_branch_task,
        provide_context=True,
    )

    # Define the task dependencies
    decide_branch_task >> [even_branch, odd_branch]
