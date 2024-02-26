import json
with open('/home/alekk/Documents/airflow-dags-study-kubernetes/utilities') as tags:
  tags = json.load(tags)

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
  'retry_delay': timedelta(minutes=5)
}

with DAG('default_args_dag',
         schedule_interval='@hourly',
         start_date=datetime(2023, 9, 10),
         catchup=False,
         default_args=default_args,
         default_view='graph',
         tags=[tags['study'], tags['airflow-first-view']],
        ) as dag:

  task1 = BashOperator(task_id="task1", bash_command="sleep 5")
  task2 = BashOperator(task_id="task2", bash_command="sleep 5")
  task3 = BashOperator(task_id="task3", bash_command="sleep 5")

  task1 >> task2 >> task3