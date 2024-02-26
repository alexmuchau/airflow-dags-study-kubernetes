from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG('complexitydag', schedule_interval=None, start_date=datetime(2023, 9, 10), catchup=False) as dag:
  
  task1 = BashOperator(task_id="task1", bash_command="sleep 5")
  task2 = BashOperator(task_id="task2", bash_command="sleep 5")
  task3 = BashOperator(task_id="task3", bash_command="sleep 5")
  task4 = BashOperator(task_id="task4", bash_command="sleep 5")
  task5 = BashOperator(task_id="task5", bash_command="sleep 5")
  task6 = BashOperator(task_id="task6", bash_command="sleep 5")
  task7 = BashOperator(task_id="task7", bash_command="sleep 5")
  task8 = BashOperator(task_id="task8", bash_command="sleep 5")
  task9 = BashOperator(task_id="task9", bash_command="sleep 5", trigger_rule='one_failed')
  
  task1 >> task2
  task3 >> task4
  [task2, task4] >> task5 >> task6
  task6 >> [task7, task8, task9]