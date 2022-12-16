from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import pandas as pd
import json
import os

default_args = {
        'owner': 'Clarissa Souza',
        'depends_on_past': False,
        'email': ['clarissasouza950@gmail.com'],
        'email_on_failure': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=1)
      }

def trataDados():
    df = pd.read_csv('dadosOriginais/dadosAPISidra.csv')
    df.drop(['NC','NN','MC','D1C','D1N','D2C','D3N','D4C','D5C'], axis=1, inplace=True)
    df.rename(columns={'MN':'unit','V':'value','D2N':'types','D3C':'year_month','D4N':'year_harvest','D5N':'product'}, inplace=True)
    df = df.drop(0)
    # Excluir as linhas repetidas 
    filtro1 = df['product']!='1 Cereais, leguminosas e oleaginosas'
    filtro2 = df['product']!='Total'
    df = df.loc[filtro1 & filtro2]
    # retirando os números da coluna product
    df['product'] = df['product'].str.replace('.','1')
    df['product'] = df['product'].str.replace(r'\d+ \b', '', regex=True)
    # Retirar todos os acentos 
    df.replace('ç','c', regex=True, inplace=True)
    df.replace(['ã','á'],'a', regex=True, inplace=True)
    df.replace('é','e', regex=True, inplace=True)
    df.replace('Á','A', regex=True, inplace=True)
    df.replace('ú','u', regex=True, inplace=True)
    # Ajustar conforme solicitado pelo cliente
    df['year_harvest'] = df['year_harvest'].str.replace('Safra','')
    df['year_month']= pd.to_datetime(df['year_month'], format='%Y%m') 
    df['year_harvest'] = df['year_harvest'].astype(int)
    df['value'] = df['value'].str.replace('-','0')
    df['value'] = df['value'].astype(int)
    df['created_at']=pd.Timestamp.today() 
    # Grava arquivo tratado
    os.makedirs ('dadosTratados',exist_ok = True)
    df.to_csv('dadosTratados/dadosTratadosAPISidra.csv',index=True,index_label='id_Sidra')

with DAG ('trata_dados_API_Sidra',
      default_args=default_args,
      start_date = datetime(2022,11,30),
      schedule_interval='0 0 * * *',
      catchup=False ) as dag:

    inicio = EmptyOperator(
            task_id='inicio'
        )
    trataDados = PythonOperator(
            task_id='trataDados',
            python_callable=trataDados
        )
    fim = EmptyOperator(
            task_id='fim'
        )
    
    inicio >> trataDados >> fim