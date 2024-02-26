from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
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

with DAG('xcom_dag',
         schedule_interval='@hourly',
         start_date=datetime(2023, 9, 10),
         catchup=False,
         default_args=default_args,
         default_view='graph',
         tags=['study', 'airflow-first-view'],
        ) as dag:

  # ------> TASK1
  def task_push_xcom(**kwargs):
    kwargs['ti'].xcom_push(key='value', value=5)

  task1 = PythonOperator(task_id="task1", python_callable=task_push_xcom)

  # ------> TASK2
  def task_pull_xcom(**kwargs):
    value = kwargs['ti'].xcom_pull(key='value', task_ids='task1')
    print(f'valor: {value}')

  task2 = PythonOperator(task_id="task2", python_callable=task_pull_xcom)

  task1 >> task2 