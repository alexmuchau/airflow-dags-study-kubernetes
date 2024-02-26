from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.variable import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

import json
import uuid
from random import uniform
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

with DAG('windturbine_generator',
         schedule_interval='*/4 * * * *',
         start_date=datetime(2023, 9, 10),
         catchup=False,
         default_args=default_args,
         default_view='graph',
         tags=['study', 'airflow-first-view'],
        ) as dag:
  
  def generate_windturbine_data():
    id = uuid.uuid4()
    
    registro = {'idtemp' : str(id), 'powerfactor' : str(uniform(0.7,1)),'hydraulicpressure' : str(uniform(70,80)) ,
                'temperature' : str(uniform(20,25)) ,'timestamp' : str(datetime.now()) }

    with open(Variable.get("path_file"), 'w') as fp:
      json.dump(registro, fp)

  generate_windturbine_data = PythonOperator(task_id="generate_windturbine_data", python_callable=generate_windturbine_data)
  trigger_windturbine_filesensor = TriggerDagRunOperator(task_id="trigger_windturbine_filesensor", trigger_dag_id="windturbine_filesensor")

  generate_windturbine_data >> trigger_windturbine_filesensor