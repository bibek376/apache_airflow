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

def get_order_price_data(**kwargs):
    ti=kwargs["ti"]

    order_price_data={
        'o1':235.02,
        'o2':35.01,
        'o3':723.1,
        'o4':65.82,
        'o5':76.34
    }

    order_price_data_string=json.dumps(order_price_data)

    ti.xcom_push('order_price_data',order_price_data_string)


def compute_sum(**kwargs):
    ti=kwargs["ti"]

    order_price_data_string=ti.xcom_pull(
        task_ids='get_order_prices',key='order_price_data'
    )

    print(order_price_data_string)

    order_price_data=json.loads(order_price_data_string)
    sum=0

    for order in order_price_data:
        sum +=order_price_data[order]

    ti.xcom_push('total_price',sum)

def compute_avg(**kwargs):
    ti=kwargs["ti"]

    order_price_data_string=ti.xcom_pull(
        task_ids='get_order_prices',key='order_price_data'
    )

    print(order_price_data_string)

    order_price_data=json.loads(order_price_data_string)
    total=0
    count=0

    for order in order_price_data:
        total +=order_price_data[order]
        count +=1
    
    average= total / count

    ti.xcom_push('average_price',average)

def display_result(**kwargs):
    ti=kwargs["ti"]

    total=ti.xcom_pull(
        task_ids='compute_sum',key='total_price'
    )
    average=ti.xcom_pull(
        task_ids='compute_average',key='average_price'
    )

    print(f"Total sum is {total}")
    print(f"average price is {average}")

with DAG(
    dag_id='cross_task_communication',
    description='Xcom and Operators example',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    tags=['Xcom','Python','Operators']
) as dag:
    get_order_prices=PythonOperator(
        task_id='get_order_prices',
        python_callable=get_order_price_data
    )
    compute_sum=PythonOperator(
        task_id='compute_sum',
        python_callable=compute_sum
    )
    compute_avg=PythonOperator(
        task_id='compute_avg',
        python_callable=compute_avg
    )
    display_result=PythonOperator(
        task_id='display_result',
        python_callable=display_result
    )


get_order_prices >> [ compute_sum, compute_avg ] >> display_result




























