from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from datetime import datetime
from airflow.utils.task_group import TaskGroup 
from datetime import datetime, timedelta

default_args = {
  'owner' : 'Alekk',
  'depends_on_past': False,
  'start_date': datetime(2023, 9, 10),
  'end_date': None,
  'email' : ['muchau04@gmail.com'],
  'email_on_failure': True,
  'email_on_retry': False,
  'retries': 1,
  'retry_delay': timedelta(seconds=2)
}

with DAG('emaildag', schedule_interval=None, start_date=datetime(2023, 9, 10), catchup=False, tags=['emaildag'], default_view='graph', default_args=default_args) as dag:
  
  task1 = BashOperator(task_id="task1", bash_command="sleep 2")
  task2 = BashOperator(task_id="task2", bash_command="sleep 2")
  task3 = BashOperator(task_id="task3", bash_command="sleep 2")
  task4 = BashOperator(task_id="task4", bash_command="exit 1")

  task_group = TaskGroup("task_group")

  task5 = BashOperator(task_id="task5", bash_command="sleep 5", task_group=task_group, trigger_rule='all_success')
  task6 = BashOperator(task_id="task6", bash_command="sleep 5", task_group=task_group, trigger_rule='all_success')
  
  send_email = EmailOperator(
    task_id='send_email',
    to='alekk.dev@gmail.com',
    subject='Airflow Alert',
    html_content='<h3>Email Test</h3>',
    task_group=task_group,
    trigger_rule='one_failed'
  )
   
  task1 >> task3
  task2 >> task3 >> task4
  task4 >> task_group