from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.mysql_hook import MySqlHook
from datetime import datetime

def query_postgresql(**kwargs):
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    result = postgres_hook.get_records("SELECT * FROM my_table")
    print("PostgreSQL query result:", result)

def query_mysql(**kwargs):
    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    result = mysql_hook.get_records("SELECT * FROM my_table")
    print("MySQL query result:", result)

default_args = {
    'owner': 'airflow'
}

with DAG('database_example', default_args=default_args, schedule_interval=None) as dag:
    task_query_postgresql = PythonOperator(
        task_id='query_postgresql',
        python_callable=query_postgresql,
        provide_context=True,
    )

    task_query_mysql = PythonOperator(
        task_id='query_mysql',
        python_callable=query_mysql,
        provide_context=True,
    )

    task_query_postgresql >> task_query_mysql
