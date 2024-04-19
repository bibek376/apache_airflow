from airflow import DAG
from datetime import datetime,timedelta
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator 
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.decorators import dag,task
import json
import pandas as pd

default_args={
    'owner':'bibek',
    'retries': '1'
}

with DAG(dag_id='create_psql_table',
    description='Create PostgreSQL Table',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    tags=['PostgreSQL','Python','Operators'],
    template_searchpath='/opt/airflow/raw_query/'
) as dag:
    create_psql_table=PostgresOperator(
        task_id='postgresql_table_creation_task',
        postgres_conn_id='datasources_crawl_db_psql',
        sql='create_test_table.sql'
    )


