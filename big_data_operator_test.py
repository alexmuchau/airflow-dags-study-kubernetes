from airflow import DAG
from datetime import datetime
from big_data_operator import BigDataOperator

with DAG('big_data_operator_test', schedule_interval=None, start_date=datetime(2023, 9, 10), catchup=False) as dag:
  
  big_data_operator = BigDataOperator(task_id="big_data_operator", path_to_csv_file='/opt/airflow/data/Churn.csv', path_to_save_file='/opt/airflow/data/Churn.json', file_type='json')

  big_data_operator