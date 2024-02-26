import json
with open('/home/alekk/Documents/airflow-dags-study-kubernetes/utilities') as tags:
  tags = json.load(tags)

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

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

with DAG('postgres_dag',
         schedule=None,
         start_date=datetime(2023, 9, 10),
         catchup=False,
         default_args=default_args,
         default_view='graph',
         tags=['study', 'airflow-first-view'],
        ) as dag:
  
  create_table = PostgresOperator(task_id="create_table",
                                  postgres_conn_id='postgres',
                                  sql='create table if not exists teste(id int);')
  
  insert_data = PostgresOperator(task_id="insert_data",
                                postgres_conn_id='postgres',
                                sql='insert into teste values(1);')
  
  get_data = PostgresOperator(task_id="get_data",
                              postgres_conn_id='postgres',
                              sql='select * from teste;')
  
  print_result = PythonOperator(task_id="print_result", python_callable=lambda **kwargs: print(kwargs['ti'].xcom_pull(task_ids='get_data')), provide_context=True)

  create_table >> insert_data >> get_data >> print_result