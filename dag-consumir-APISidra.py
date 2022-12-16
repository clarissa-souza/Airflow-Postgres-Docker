from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from io import StringIO
import json
import urllib3
import os
import pandas as pd

default_args = {
        'owner': 'Clarissa Souza',
        'depends_on_past': False,
        'email': ['clarissasouza950@gmail.com'],
        'email_on_failure': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=1)
      }

def lerAPISidra():
    os.makedirs ('dadosOriginais',exist_ok = True)
    http=urllib3.PoolManager()
    urlPortalAPI='http://api.sidra.ibge.gov.br/values/t/1618/n1/all/v/all/p/all/c49/all/c48/all?formato=json'
    response = http.request('GET',urlPortalAPI)
    dataResponse=response.data.decode('utf-8')
    df = pd.read_json(dataResponse)
    df.to_csv('dadosOriginais/dadosAPISidra.csv',index=False)

with DAG ('consumir_API_Sidra',
      default_args=default_args,
      start_date = datetime(2022,11,30),
      schedule_interval='0 0 * * *',
      catchup=False ) as dag:

    inicio = EmptyOperator(
            task_id='inicio'
        )
    lerAPISidra = PythonOperator(
            task_id='lerAPISidra',
            python_callable=lerAPISidra
        )
    fim = EmptyOperator(
            task_id='fim'
        )
    
    inicio >> lerAPISidra >> fim