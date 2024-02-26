from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
import requests

from datetime import datetime, timedelta

default_args = {
  'owner' : 'Alekk',
  'depends_on_past': False,
  'start_date': datetime(2023, 9, 10),
  'end_date': None,
  'email' : 'muchau04@gmail.com',
  'email_on_failure': False,
  'email_on_retry': False,
  'retries': 1,
  'retry_delay': timedelta(seconds=2)
}

with DAG('http_sensor_dag',
         schedule=None,
         start_date=datetime(2023, 9, 10),
         catchup=False,
         default_args=default_args,
         default_view='graph',
         tags=['study', 'airflow-first-view'],
        ) as dag:

  def query_api():
    response = requests.get("https://api.publicapis.org/entries")
    print(response.text)

  check_api = HttpSensor(task_id="check_api", 
                         http_conn_id="apis_connection",
                         endpoint="entriesdfsa",
                         poke_interval=5,
                         timeout=20)
  
  query_api = PythonOperator(task_id="query_api", python_callable=query_api)

  check_api >> query_api