from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
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

with DAG('variable_dag',
         schedule_interval='@daily',
         start_date=datetime(2023, 9, 10),
         catchup=False,
         default_args=default_args,
         default_view='graph',
         tags=['variable'],
        ) as dag:

  # ------> TASK1
  def print_variable(**context):
    var = Variable.get('firstvar')
    print(f'variable: {var}')

  task1 = PythonOperator(task_id="task1", python_callable=print_variable)

  task1