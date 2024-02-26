from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import pandas as pd
from datetime import datetime, timedelta
import json

def get_data(**kwargs):
    url = 'https://raw.githubusercontent.com/airscholar/ApacheFlink-SalesAnalytics/main/output/new-output.csv'

    response = requests.get(url)

    if response.status_code == 200:
        df = pd.read_csv(url, header=None, names=['Category', 'Price', 'Quantity'])

        json_data = df.to_json(orient='records')

        kwargs['ti'].xcom_push(key='data', value=json_data)
    else:
        raise Exception(f'Failed to parse data, HTTP status code: {response.status_code}')
    
def show_data(**kwargs):
    output = kwargs['ti'].xcom_pull(key='data', task_ids='fetch_data')
    print(output)
    if output:
        output = json.loads(output)
    else:
        raise ValueError('No data')

default_args = {
    'owner': 'Alekk',
    'start_date': datetime(2024, 2, 25),
    'catchup': False
}

with DAG(
    dag_id='fetch_and_preview',
    default_args=default_args,
    tags=['study', 'kubernetes'],
    schedule=timedelta(days=1)
) as dag:
    
    fetch_data = PythonOperator(
        task_id='fetch_data',
        python_callable=get_data
    )

    preview_data = PythonOperator(
        task_id='preview_data',
        python_callable=show_data
    )

    fetch_data >> preview_data