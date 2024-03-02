from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator 
from airflow.utils.dates import days_ago
from datetime import datetime
import requests
import boto3
from io import StringIO
import json
import pandas as pd




def fetch_and_save_country_details_to_s3():
    # Define country name and API URL
    country_name = "Nepal"
    api_url = f'https://api.api-ninjas.com/v1/country?name={country_name}'
    
    # Make API request to fetch country details
    response = requests.get(api_url, headers={'X-Api-Key': 'dCc4gXhgBdcjQI7wayV/wQ==tSXKKX4AYnBG5xYk'})
    
    # Check if the request was successful
    if response.status_code == requests.codes.ok:
        data = response.json()  # Convert response to JSON
        
        # Connect to S3
        s3 = boto3.client(
            's3',
            region_name='ap-south-1',
            aws_access_key_id="AKIAW3MEFQRJXZRJ7XOQ",
            aws_secret_access_key="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"
        )
        
        # Convert JSON data to bytes
        json_data = json.dumps(data).encode('utf-8')
        
        # Upload JSON data to S3 bucket directly
        s3.put_object(Bucket='weather-api-country-data', Key='country_details.json', Body=json_data)
        print("JSON data saved to 'country_details.json' in S3 bucket")
    else:
        print("Error:", response.status_code, response.text)




fetch_and_save_country_details_to_s3()



