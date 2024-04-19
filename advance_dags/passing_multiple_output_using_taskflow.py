from airflow import DAG
from datetime import datetime,timedelta
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.decorators import dag,task
import json
import pandas as pd

default_args={
    'owner':'bibek',
    'retries': '1'
}

@dag(dag_id='compute_sum_and_avg_using_taskflow_mul_op',
    description='multiple op using taskflow API example',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    tags=['TaskflowAPI','Python','Operators']
) 

def passing_data_with_taskflow_api():
    
    @task
    def get_order_price_data():
        order_price_data={
            'o1':235.02,
            'o2':35.01,
            'o3':723.1,
            'o4':65.82,
            'o5':76.34
        }
        return order_price_data

    @task(multiple_outputs=True)
    def compute_sum_and_avg(order_price_data: dict):
        sum=0
        count=0
        for order in order_price_data:
            sum +=order_price_data[order]
            count +=1
        average= sum / count    
        return {'sum':sum,'avg':average}

    @task
    def display_result(total:float , average:float):

        print(f"Total sum is {total}")
        print(f"average price is {average}")

    order_data=get_order_price_data()
    result_dict=compute_sum_and_avg(order_data)
    display_result(result_dict['sum'],result_dict['avg'])

passing_data_with_taskflow_api()
