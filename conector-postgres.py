from datetime import datetime, timedelta
from airflow import DAG 
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import pandas as  pd

default_args = {
        'owner': 'Clarissa Souza',
        'depends_on_past': False,
        'email': ['clarissasouza950@gmail.com'],
        'email_on_failure': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=1)
      }

with DAG ('conector-postgres-v03',
      default_args=default_args,
      start_date = datetime(2022,11,30),
      schedule_interval='0 0 * * *',
      catchup=False ) as dag:

    inicio = EmptyOperator(
            task_id = 'inicio'
        )
    createTable = PostgresOperator(
            task_id = 'createTable',
            postgres_conn_id = 'postgres_localhost',
            sql = """
                create table if not exists dag_runs (
                    id_sidra int not null,
                    unit varchar,
                    value int,
                    types varchar,
                    year_month date,
                    year_harvest int,
                    product varchar,
                    created_at timestamp,
                    primary key (id_Sidra)
                )           
            """
        )
    deleteData = PostgresOperator(
            task_id = 'deleteData',
            postgres_conn_id = 'postgres_localhost',
            sql = """
                truncate table dag_runs           
            """
        )
    insertTable = BashOperator(
            task_id = 'insertTable',
            bash_command = (
                'psql postgresql://airflow:airflow@host.docker.internal:5432/teste -c " '
                'COPY dag_runs (id_sidra, unit, value, types, year_month, year_harvest, product, created_at) '
                "FROM '/tmp/dadosTratadosAPISidra.csv' "
                "DELIMITER ',' "
                'CSV HEADER"'
            )
        )
    fim = EmptyOperator(
            task_id = 'fim'
        )

    inicio >> createTable >> deleteData >> insertTable  >> fim