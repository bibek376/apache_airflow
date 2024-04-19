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



def delete_all_table():
    # Read SQL query from file
    mssql_hook=MsSqlHook(mssql_conn_id='FDTestDatabase_mssql')
    
    drop_businessdetailscategory_query= f'''drop table FDTestDatabase.Apple.BusinessDetailsCategories ;'''
    drop_responsehistory_query= f'''drop table FDTestDatabase.Apple.ResponseHistory ;'''
    drop_displaynames_query= f'''drop table FDTestDatabase.Apple.DisplayNames ;'''
    drop_businessdetails_query= f'''drop table FDTestDatabase.Apple.BusinessDetails ;'''
    drop_company_query= f'''drop table FDTestDatabase.Apple.Company ;'''
    drop_countrycode_query= f'''drop table FDTestDatabase.Apple.CountryCode ;'''
    drop_urls_query= f'''drop table FDTestDatabase.Apple.Urls'''
    drop_categories_query= f'''drop table FDTestDatabase.Apple.Categories ;'''

    mssql_hook.run(sql=drop_businessdetailscategory_query)
    mssql_hook.run(sql=drop_responsehistory_query)
    mssql_hook.run(sql=drop_displaynames_query)
    mssql_hook.run(sql=drop_businessdetails_query)
    mssql_hook.run(sql=drop_company_query)
    mssql_hook.run(sql=drop_countrycode_query)
    mssql_hook.run(sql=drop_urls_query)
    mssql_hook.run(sql=drop_categories_query)

    

with DAG(dag_id='apple_data_delete_design',
    description='Table deletion on apple schema',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    tags=['mssql','mssqlhook','mssqloperator'],
    template_searchpath='/opt/airflow/raw_query/'
) as dag:
    delete_all_table=PythonOperator(
        task_id='app_delete_all_table_task',
        python_callable=delete_all_table
    )

delete_all_table 

