from airflow import DAG
from datetime import datetime,timedelta
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator 
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.decorators import dag,task
import json
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args={
    'owner':'bibek',
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

def join_and_save_to_csv(engineering,finance):
    # Read SQL query from file
    pg_hook=PostgresHook(postgres_conn_id='datasources_crawl_db_psql')
    sql_query= f'''select e.employee_id,e.employee_name,d.department_name
                  from employee e
                  inner join department d
                  on e.department_id=d.department_id
                  where d.department_name in ('{engineering}','{finance}');'''
    
    result=pg_hook.get_records(sql_query)

    df = pd.DataFrame(result, columns = ['employee_id', 'employee_name', 'department_name'])

    df.to_csv('/opt/airflow/output/department_wise_employee.csv',index=False)

def drop_tables():
    pg_hook=PostgresHook(postgres_conn_id='datasources_crawl_db_psql')
    drop_dept='DROP TABLE IF EXISTS department ;'
    drop_emp='DROP TABLE IF EXISTS employee ;'
    pg_hook.run(drop_dept)
    pg_hook.run(drop_emp)


with DAG(dag_id='reporting_data',
    description='Generate Report By Joining Two tables',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    tags=['PostgreSQL','Python','Operators'],
    template_searchpath='/opt/airflow/raw_query/'
) as dag:
    create_dept_table=PostgresOperator(
        task_id='department_table_creation_task',
        postgres_conn_id='datasources_crawl_db_psql',
        sql='dept_table.sql'
    )
    create_emp_table=PostgresOperator(
        task_id='employee_table_creation_task',
        postgres_conn_id='datasources_crawl_db_psql',
        sql='emp_table.sql'
    )
    insert_dept_table=PostgresOperator(
        task_id='department_table_data_insert_task',
        postgres_conn_id='datasources_crawl_db_psql',
        sql='insert_dept_data.sql'
    )
    insert_employee_table=PostgresOperator(
        task_id='employee_table_data_insert_task',
        postgres_conn_id='datasources_crawl_db_psql',
        sql='insert_emp_data.sql'
    )
    join_and_save_to_csv_hook=PythonOperator(
        task_id='join_and_save_to_csv_task',
        python_callable=join_and_save_to_csv,
        op_kwargs={'engineering':'Engineering','finance':'Finance'}
    )

create_dept_table >> insert_dept_table >> create_emp_table >> insert_employee_table >> join_and_save_to_csv_hook



