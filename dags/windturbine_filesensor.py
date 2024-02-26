from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models.variable import Variable
from airflow.utils.task_group import TaskGroup

import json
from datetime import datetime, timedelta

default_args = {
  'owner' : 'Alekk',
  'depends_on_past': False,
  'start_date': datetime(2023, 9, 10),
  'end_date': None,
  'email' : 'alekk.dev@gmail.com',
  'email_on_failure': True,
  'email_on_retry': False,
  'retries': 1,
  'retry_delay': timedelta(seconds=10)
}

with DAG('windturbine_filesensor',
         schedule_interval=None,
         start_date=datetime(2023, 9, 10),
         catchup=False,
         default_args=default_args,
         default_view='graph',
         tags=['python', 'postgres', 'email', 'filesystem', 'sensor', 'windturbine'],
        ) as dag:
  
  email_temp = TaskGroup("EMAIL_TEMP")
  database_insertion = TaskGroup("DATABASE_INSERTION")
  
  file_sensor = FileSensor(task_id="file_sensor", filepath=Variable.get("path_file"), fs_conn_id="fs_default", poke_interval=10)

  def get_data(**kwargs):
    with open(Variable.get("path_file")) as f:
      data = json.load(f)
      kwargs['ti'].xcom_push(key='data', value= data)

  get_data = PythonOperator(task_id="get_data", python_callable=get_data, provide_context=True)

  def check_temp(**context):
    temperature = float(context['ti'].xcom_pull(key='data', task_ids='get_data')['temperature'])
    if temperature >= 24:
      return 'EMAIL_TEMP.send_critical_email'
    else:
      return 'EMAIL_TEMP.send_success_email'
    
  branch_temp = BranchPythonOperator(task_id="branch_temp", python_callable=check_temp, provide_context=True, task_group=email_temp,)

  send_critical_email = EmailOperator(task_id="send_critical_email", to='alekk.dev@gmail.com', subject='Airflow Alert',
                                     html_content='<h3>Temperatura Critica! {{ ti.xcom_pull(key="data", task_ids="get_data")["temperature"] }}ºC </h3>', task_group=email_temp)
  
  send_success_email = EmailOperator(task_id="send_success_email", to='alekk.dev@gmail.com', subject='Airflow Alert',
                                     html_content='<h3>Temperatura Normal! {{ ti.xcom_pull(key="data", task_ids="get_data")["temperature"] }}ºC </h3>', task_group=email_temp)
  
  create_table = PostgresOperator(task_id="create_table", postgres_conn_id='postgres',
                                    sql='''create table if not exists
                                    windturbine (idtemp varchar, powerfactor varchar, hydraulicpressure varchar, temperature varchar, timestamp varchar)
                                    ''',
                                    task_group=database_insertion
                                  )
  
  insert_data = PostgresOperator(task_id='insert_data', postgres_conn_id='postgres',
                                  parameters=(
                                    '{{ ti.xcom_pull(key="data", task_ids="get_data")["idtemp"] }}',
                                    '{{ ti.xcom_pull(key="data", task_ids="get_data")["powerfactor"] }}',
                                    '{{ ti.xcom_pull(key="data", task_ids="get_data")["hydraulicpressure"] }}',
                                    '{{ ti.xcom_pull(key="data", task_ids="get_data")["temperature"] }}',
                                    '{{ ti.xcom_pull(key="data", task_ids="get_data")["timestamp"] }}'
                                  ),
                                  sql='''insert into windturbine (idtemp, powerfactor, hydraulicpressure, temperature, timestamp)
                                  values (%s, %s, %s, %s, %s)
                                  ''',
                                  task_group=database_insertion
                                )
  
  with email_temp:
    branch_temp >> [send_success_email, send_critical_email]

  with database_insertion:
    create_table >> insert_data

  file_sensor >> get_data >> [email_temp, database_insertion]
  
  