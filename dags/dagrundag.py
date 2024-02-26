import json
with open('/home/alekk/Documents/airflow-dags-study-kubernetes/utilities') as tags:
  tags = json.load(tags)

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
  'dagrundag',
  schedule_interval=None,
  start_date=datetime(2023, 9, 10), 
  catchup=False,
  tags=['study', 'airflow-first-view']
) as dag:
  
  task1 = BashOperator(task_id="task1", bash_command="sleep 5")
  task2 = TriggerDagRunOperator(task_id="task2", trigger_dag_id="taskgroup")
  
  task1 >> task2