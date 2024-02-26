import json
with open('/home/alekk/Documents/airflow-dags-study-kubernetes/utilities') as tags:
  tags = json.load(tags)

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

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

with DAG('postgres_hooks_dag',
         schedule=None,
         start_date=datetime(2023, 9, 10),
         catchup=False,
         default_args=default_args,
         default_view='graph',
         tags=[tags['study'], tags['airflow-first-view']],
        ) as dag:
  
  def create_table():
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    pg_hook.run('create table if not exists teste2(id int);', autocommit=True)

  create_table = PythonOperator(task_id="create_table", python_callable=create_table)

  def insert_table():
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    pg_hook.run(f'insert into teste2 (id) select max(id) + 1 from teste2;', autocommit=True)

  insert_table = PythonOperator(task_id="insert_table", python_callable=insert_table)

  def get_data(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    records = pg_hook.get_records('select * from teste2;')
    print(records)
    kwargs['ti'].xcom_push(key='select_all', value=records)

  get_data = PythonOperator(task_id="get_data", python_callable=get_data, provide_context=True)

  print_data = PythonOperator(task_id="print_data", python_callable=lambda **kwargs: print(kwargs['ti'].xcom_pull(key='select_all', task_ids='get_data')), provide_context=True)

  create_table >> insert_table >> get_data >> print_data
  