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



default_args = {
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
    "depends_on_past": False
}  

@dag(dag_id='semantic_analysis_test',
    description='Semantic Analysis of Client',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['semantic_analysis','Python','decorators']
) 


def main_function():

    #Return ClusterID
    @task
    def return_cluster_id():
        try:
            mssql_hook=MsSqlHook(mssql_conn_id='internaltoolset_mssql')

            sql_stmt=f"""
            SELECT
                            DISTINCT Keyword,
                            SA.SemanticRawData.SemanticKeywordId
                        FROM
                            SA.SemanticRawData
                        INNER JOIN SA.SemanticKeywordLibrary ON
                            SA.SemanticKeywordLibrary.SemanticKeywordId = SA.SemanticRawData.SemanticKeywordId
                        WHERE
                            ProjectId =2265;
            """

            existing_id=mssql_hook.get_records(sql=sql_stmt)
            print("Length is :",len(existing_id))
            print(f"------------------{existing_id}--------------------------")
            if existing_id[0][0] :
                pass 
            #     return existing_id[0][0]
            # else:
            #     insert_stmt=f"""INSERT INTO SA.SemanticCluster (ClusterName) VALUES ('{cluster_name}')"""
            #     mssql_hook.run(sql=insert_stmt)

            #     #Retrive the new_id
            #     new_id=mssql_hook.get_records(sql=sql_stmt)

            #     return new_cluster_id[0][0]

        except Exception as e:
            print(f"Error: {e}")
    return_cluster_id()    
main_function()



