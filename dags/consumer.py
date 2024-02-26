from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

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

dataset = Dataset('/opt/airflow/data/Churn_new.csv')

with DAG('consumer_dag',
         schedule=[dataset],
         start_date=datetime(2023, 9, 10),
         catchup=False,
         default_args=default_args,
         default_view='graph',
         tags=['study', 'airflow-first-view'],
        ) as dag:
  
  def load_and_save_file():
    dataset = pd.read_csv('/opt/airflow/data/Churn_new.csv', sep=';')
    dataset.to_csv("/opt/airflow/data/Churn_new_test.csv", sep=';', index=False)
  
  load_and_save = PythonOperator(task_id="load_and_save", python_callable=load_and_save_file, provide_context=True)
  
  load_and_save