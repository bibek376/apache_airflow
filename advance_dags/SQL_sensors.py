from airflow import DAG
from datetime import datetime,timedelta
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator 
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.decorators import dag,task
import json
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.sensors.sql_sensor import SqlSensor


def drop_tables_on_failure(context):
    """
    Callback function to drop tables on task failure.
    """
    task_instance = context.get('task_instance')
    task_id = task_instance.task_id
    dag_id = task_instance.dag_id
    print(f"Task '{task_id}' in DAG '{dag_id}' failed. Dropping tables...")
    
    # Connect to the database and execute drop table statements
    mssql_hook = MsSqlHook(mssql_conn_id='FDTestDatabase_mssql')
    drop_statements = [
        "DROP TABLE IF EXISTS t_emp_details;",
        "DROP TABLE IF EXISTS t_highest_earning_emp_details;"
    ]
    for statement in drop_statements:
        mssql_hook.run(sql=statement)


default_args={
    'owner':'bibek',
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
    'on_failure_callback': drop_tables_on_failure
}


with DAG(dag_id='sql_sensor',
    description='SQL Sensor To filterout Highest Earning Employee',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    tags=['SqlSensor','mssql','mssqlhook','mssqloperator'],
    template_searchpath='/opt/airflow/raw_query/'
) as dag:
    create_employee_table=MsSqlOperator(
        task_id='create_employee_table',
        mssql_conn_id='FDTestDatabase_mssql',
        sql='emp_details.sql'
    )
    create_highest_earning_employee_table=MsSqlOperator(
        task_id='highest_earning_employee',
        mssql_conn_id='FDTestDatabase_mssql',
        sql=f"""create table t_highest_earning_emp_details(id int,
                name varchar(100),
                dept_name varchar(40),
                salary float);
                """
    )
    wait_for_highest_earning_employee=SqlSensor(
        task_id='wait_for_highest_earning_employee',
        conn_id='FDTestDatabase_mssql',
        sql=f"""IF EXISTS (
                SELECT 1
                FROM t_emp_details
                WHERE salary > 500000
            )
                SELECT 1 AS Result
            ELSE
                SELECT 0 AS Result;
            """,
        poke_interval=5,
        timeout=10
    )

    insert_data_into_highest_earning_employee_table = MsSqlOperator(
        task_id = 'insert_data_into_highest_earning_employee_table',
        mssql_conn_id='FDTestDatabase_mssql',
        sql=f"""INSERT INTO t_highest_earning_emp_details (id, name, 
        dept_name, salary)
        SELECT id, name, dept_name, salary
        FROM t_emp_details
        WHERE salary > 500000;
        """
    )


    [ create_employee_table , create_highest_earning_employee_table ] >> wait_for_highest_earning_employee >> \
     insert_data_into_highest_earning_employee_table

