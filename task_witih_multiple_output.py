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
@dag('cross_task_communication_taskflow_with_multiple_output', default_args=default_args, 
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

    @task(multiple_outputs=True)
    def compute_sum_and_avg(order_price_data: dict):
        total=0
        count=0
        for order in order_price_data:
            total +=order_price_data[order]
            count +=1
        avg= total / count
        return {'total_price':total,'avg_price':avg}
    
    # @task
    # def display_result(price_summary_data: dict):
    #     total=price_summary_data['total_price']
    #     avg=price_summary_data['avg_price']

    #     print("Total price of goods is {total}".format(total=total))
    #     print("Avg price of goods is {avgs}".format(avgs=avg))

    # order_price_data=get_order_prices()
    # price_summary_data=compute_sum_and_avg(order_price_data)
    # display_result(price_summary_data)

    @task
    def display_result(total,avg):

        print("Total price of goods is {total}".format(total=total))
        print("Avg price of goods is {avgs}".format(avgs=avg))

    order_price_data=get_order_prices()
    price_summary_data=compute_sum_and_avg(order_price_data)
    display_result(
        price_summary_data['total_price'],
        price_summary_data['avg_price']
        )


passing_data_with_taskflow_api()



