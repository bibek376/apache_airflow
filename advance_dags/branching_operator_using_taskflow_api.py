from airflow import DAG
from datetime import datetime,timedelta
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.decorators import dag,task
import json
import pandas as pd 
from airflow.models import Variable



default_args={
    'owner':'bibek',
    'retries': None
}


@dag(dag_id='branching_operator_using_taskflow_api',
    description='branching operator to select one task',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    tags=['Branching','Python','Operators']
) 


def branching_using_taskflow():
    def read_csv_file():
        df=pd.read_csv("/opt/airflow/datasets/car_data.csv")
        return df.to_json()

    @task.branch
    def determine_branch():
        final_variable=Variable.get("transform",default_var=None)

        if final_variable == 'filter_two_seater':
            return 'filter_two_seater_task'
        elif final_variable == 'filter_except_two_seater':
            return 'filter_except_two_seater_task'

    def filter_two_seater(ti):
        json_data=ti.xcom_pull(task_ids='read_csv_file_task')

        df=pd.read_json(json_data)

        two_seater_df=df[df['Seats']==2]

        ti.xcom_push(key='transform_result',value=two_seater_df.to_json())
        ti.xcom_push(key='filename',value='two_seaters')


    def filter_except_two_seater(ti):
        json_data=ti.xcom_pull(task_ids='read_csv_file_task')

        df=pd.read_json(json_data)

        expect_two_seater_df=df[df['Seats'] !=2 ]

        ti.xcom_push(key='transform_result',value=expect_two_seater_df.to_json())
        ti.xcom_push(key='filename',value='expect_two_seater')

    def write_csv_final_data(ti):
        json_data=ti.xcom_pull(key='transform_result')
        filename=ti.xcom_pull(key='filename')

        df=pd.read_json(json_data)

        df.to_csv(f'/opt/airflow/output/{filename}.csv',index=False)

    read_csv_file=PythonOperator(
        task_id='read_csv_file_task',
        python_callable=read_csv_file
    )
    filter_two_seater=PythonOperator(
        task_id='filter_two_seater_task',
        python_callable=filter_two_seater
    )
    filter_except_two_seater=PythonOperator(
        task_id='filter_except_two_seater_task',
        python_callable=filter_except_two_seater
    )
    write_csv_final_data=PythonOperator(
        task_id='write_csv_final_data_task',
        python_callable=write_csv_final_data,
        trigger_rule='none_failed'
    )

    read_csv_file >> determine_branch() >> [ filter_two_seater, filter_except_two_seater ] >> write_csv_final_data


branching_using_taskflow()


