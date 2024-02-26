from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
  'first_dag',
  schedule_interval=None,
  start_date=datetime(2023, 9, 10),
  catchup=False,
  tags=['study', 'airflow-first-view']
) as dag:
  task1 = BashOperator(task_id="task1", bash_command="sleep 5")
  task2 = BashOperator(task_id="task2", bash_command="sleep 5")
  task3 = BashOperator(task_id="task3", bash_command="sleep 5")

  task1 >> task2 >> task3