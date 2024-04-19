from airflow import DAG
from datetime import datetime,timedelta
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.decorators import dag,task
import json

default_args={
    'owner':'bibek',
    'retries': '1'
}

@dag(dag_id='compute_sum_and_avg_using_taskflow',
    description='Xcom and Operators example',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    tags=['Xcom','Python','Operators']
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

    @task
    def compute_sum(order_price_data: dict ):

        sum=0
        for order in order_price_data:
            sum +=order_price_data[order]

        return sum

    @task
    def compute_avg(order_price_data: dict):

        total=0
        count=0

        for order in order_price_data:
            total +=order_price_data[order]
            count +=1
        
        average= total / count

        return average

    @task
    def display_result(total: float, average :float ):

        print(f"Total sum is {total}")
        print(f"average price is {average}")

    order_data=get_order_price_data()
    sum_data=compute_sum(order_data)
    avg_data=compute_avg(order_data)
    display_result(sum_data,avg_data)

passing_data_with_taskflow_api()







