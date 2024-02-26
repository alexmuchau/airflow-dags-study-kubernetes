from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

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

with DAG('churn_dag',
         schedule_interval='@daily',
         start_date=datetime(2023, 9, 10),
         catchup=False,
         default_args=default_args,
         default_view='graph',
         tags=['pandas', 'python'],
        ) as dag:
  
  def load_clean_data():
    df = pd.read_csv('/opt/airflow/data/Churn.csv', sep=';')
    df.columns = ["Id", "Score", "Estado", "Genero", "Idade", "Patrimonio", "Saldo",
                  "Produtos", "TemCartCredito", "Ativo", "Salario", "Saiu"]
    
    df["Salario"].fillna(df["Salario"].median(), inplace=True)
    df["Genero"].fillna("Masculino", inplace=True)
    df.loc[(df["Idade"]) < 0 | (df["Idade"] > 120), "Idade"] = df["Idade"].median()

    df.drop_duplicates(subset="Id", keep="first", inplace=True)

    df.to_csv("/opt/airflow/data/Churn_Clean.csv", sep=';', index=False)
  
  clean_and_load_churn = PythonOperator(task_id="clean_and_load_churn", python_callable=load_clean_data)
  
  clean_and_load_churn