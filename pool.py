from airflow import DAG
from airflow.operators.bash import BashOperator
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

with DAG('pool_dag',
         schedule_interval='@daily',
         start_date=datetime(2023, 9, 10),
         catchup=False,
         default_args=default_args,
         default_view='graph',
         tags=['pool'],
        ) as dag:
  
  task1 = BashOperator(task_id="task1", bash_command='sleep 5', pool="firstpool")
  task2 = BashOperator(task_id="task2", bash_command='sleep 5', pool="firstpool", priority_weight=5)
  task3 = BashOperator(task_id="task3", bash_command='sleep 5', pool="firstpool")
  task4 = BashOperator(task_id="task4", bash_command='sleep 5', pool="firstpool", priority_weight=10)