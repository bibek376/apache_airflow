from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

# Define the DAG
with DAG(dag_id="task_group", start_date=datetime(2023, 1, 1), schedule_interval=None) as dag:

    # A dummy task to start the DAG
    start = BashOperator(task_id="start", bash_command="echo start")

    # First process_data TaskGroup
    with TaskGroup(
        group_id="process_data_1",
        ui_color="green",
        ui_fgcolor="black",
        tooltip="This task group performs data processing steps on a CSV file",
    ) as process_data_1:
        # A task to validate the data
        validate_data = BashOperator(task_id="validate_data", bash_command="echo validate_data")
        # A task to read the data from a CSV file
        read_data = BashOperator(task_id="read_data", bash_command="echo read_data")
        # A task to transform the data
        transform_data = BashOperator(task_id="transform_data", bash_command="echo transform_data")
        # Set the dependencies within the TaskGroup
        validate_data >> [ read_data , transform_data ]

    # Second process_data TaskGroup
    with TaskGroup(
        group_id="process_data_2",
        ui_color="blue",
        ui_fgcolor="white",
        tooltip="This task group performs additional data processing steps",
    ) as process_data_2:
        # A task to clean the data
        clean_data = BashOperator(task_id="clean_data", bash_command="echo clean_data")
        # A task to export the data
        export_data = BashOperator(task_id="export_data", bash_command="echo export_data")
        # Set the dependencies within the TaskGroup
        clean_data >> export_data

    # A dummy task to end the DAG
    end = BashOperator(task_id="end", bash_command="echo end")

    # Define the dependencies
    start >> process_data_1 >> process_data_2 >> end








