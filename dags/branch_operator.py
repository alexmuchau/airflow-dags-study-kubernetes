from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import random

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

with DAG('branch_dag',
         schedule_interval='@daily',
         start_date=datetime(2023, 9, 10),
         catchup=False,
         default_args=default_args,
         default_view='graph',
         tags=['branchs', 'python'],
        ) as dag:
  
  def random_num():
    return random.randint(1, 100)
  
  def is_even(**context):
    value = context['task_instance'].xcom_pull(task_ids='generate_num')
    if value % 2 == 0:
      return 'even_number'
    else:
      return 'odd_numbe'
    
  def print_even_number(**context):
    value = context['task_instance'].xcom_pull(task_ids='generate_num')
    print(f'valor par: {value}')
  
  def print_odd_number(**context):
    value = context['task_instance'].xcom_pull(task_ids='generate_num')
    print(f'valor impar: {value}')
  
  generate_num = PythonOperator(task_id="generate_num", python_callable=random_num)
  is_even = BranchPythonOperator(task_id="is_even", python_callable=is_even, provide_context=True)
  even_number = PythonOperator(task_id="even_number", python_callable=print_even_number)
  odd_number = PythonOperator(task_id="odd_number", python_callable=print_odd_number)

  generate_num >> is_even >> [even_number, odd_number]
  