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


default_args={
    'owner':'bibek',
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

def populate_categories_table():
    # Read SQL query from file
    mssql_hook=MsSqlHook(mssql_conn_id='FDTestDatabase_mssql')
    
    sql_query= f'''insert
                        into
                        FDTestDatabase.Apple.Categories(qualifiedId,
                        name,
                        description)
                    select
                        qualifiedId,
                        name,
                        description
                    from
                        Flightdeck.Apple.[new-taxonomy-20240201];'''
    mssql_hook.run(sql=sql_query)

with DAG(dag_id='apple_data_er_design',
    description='Table creation on apple schema',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    tags=['mssql','mssqlhook','mssqloperator'],
    template_searchpath='/opt/airflow/raw_query/'
) as dag:
    create_all_table=MsSqlOperator(
        task_id='create_all_table',
        mssql_conn_id='FDTestDatabase_mssql',
        sql='app_table_creation.sql'
    )
    insert_data_in_country_table=MsSqlOperator(
        task_id='insert_data_in_country_table',
        mssql_conn_id='FDTestDatabase_mssql',
        sql='app_insert_country_data.sql'
    )
    insert_data_in_categories_table=PythonOperator(
        task_id='insert_data_in_categories_task',
        python_callable=populate_categories_table
    )

create_all_table >> insert_data_in_country_table >> insert_data_in_categories_table


