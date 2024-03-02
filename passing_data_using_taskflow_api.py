import time 
import json
from datetime import datetime,timedelta
from airflow.utils.dates import days_ago
from airflow import DAG 
from airflow.operators.python import PythonOperator
from airflow.decorators import task,dag


default_args={
    'owner':'Bibek'
}


# Instantiate your DAG
@dag('cross_task_communication_taskflow', default_args=default_args, 
    tags=['python','dependencies','taskflow_api'], schedule_interval=None)


def passing_data_with_taskflow_api():

    @task 
    def get_order_prices():

        order_price_data={
            'o1':234,
            'o2':123,
            'o3':45,
            'o4':32,
            'o5':97,
            'o6':74
        }

        return order_price_data

    @task
    def compute_sum(order_price_data: dict):
        total=0
        for order in order_price_data:
            total +=order_price_data[order]

        return total

    @task
    def compute_avg(order_price_data: dict):
        total=0
        count=0
        for order in order_price_data:
            total +=order_price_data[order]
            count +=1
        average=total / count

        return average
    
    @task
    def display_result(total: int, average:float):

        print("Total price of goods is {total}".format(total=total))
        print("Avg price of goods is {avgs}".format(avgs=average))

    order_price_data=get_order_prices()
    total=compute_sum(order_price_data)
    average=compute_avg(order_price_data)
    display_result(total,average)

passing_data_with_taskflow_api()

