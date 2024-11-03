import pandas as pd
from io import BytesIO
from datetime import datetime
import boto3
import json
import pandas as pd
from sqlalchemy import create_engine
import re  # Biblioteca para expressões regulares
from datetime import datetime, timedelta
import numpy as np

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, MetaData, Table, inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import ProgrammingError

from typing import Optional
from funcoes import extrair_dados_silver_v3, haversine, salvar_dataframe_incremental_no_minio, salvar_dataframe_no_postgres

# Configurações do MinIO
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "pN2nJpDS8zkBM79eIKrh" #"6MCHYeyIPu8gka6gvIns"
MINIO_SECRET_KEY = "AYoyusCiw9CGodBvpOe3VL5Qlote2SUVSiZnSfxu" #"QaMn37c0t8QsAWy1NLpnXKIfDoY6CMxb9ZYoHQw3"
SILVER_BUCKET = "silver"
GOLD_BUCKET = "gold"

# Conexão ao MinIO
s3_client = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
)

# Definindo os argumentos padrão para as tarefas do DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'execution_timeout': timedelta(minutes=30)
}

def inserir_dados_gold():

    # Regex para extrair data e hora do nome do arquivo
    pattern = r'(\d{4}-\d{2}-\d{2})_(\d{6})\.csv'

    prefix = 'previsao/'
    file = "view_previsao.parquet"

    # Chamar a função
    df_previsao = extrair_dados_silver_v3(s3_client,MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, SILVER_BUCKET, GOLD_BUCKET, file, prefix, pattern)

    if 'horario_previsto_chegada_veiculo_na_parada' in df_previsao.columns:
        # Converter as colunas de string para o formato datetime
        df_previsao['horario_previsto_chegada_veiculo_na_parada_1'] = pd.to_datetime(df_previsao['horario_previsto_chegada_veiculo_na_parada'], format='%H:%M')
        df_previsao['hora_referencia_infos_1'] = pd.to_datetime(df_previsao['hora_referencia_infos'], format='%H:%M')

        # Calcular a diferença de tempo
        df_previsao['tempo_espera'] = (df_previsao['horario_previsto_chegada_veiculo_na_parada_1'] - df_previsao['hora_referencia_infos_1']).dt.total_seconds() / 60

        df_previsao = df_previsao.drop(columns=["horario_previsto_chegada_veiculo_na_parada_1", "hora_referencia_infos_1"])

        # Converter graus para radianos
        df_previsao['latitude_parada_rad'] = np.radians(df_previsao['latitude_parada'])
        df_previsao['longitude_parada_rad'] = np.radians(df_previsao['longitude_parada'])
        df_previsao['latitude_veiculo_rad'] = np.radians(df_previsao['latitude_veiculo'])
        df_previsao['longitude_veiculo_rad'] = np.radians(df_previsao['longitude_veiculo'])

        # Aplicar a fórmula
        df_previsao['distancia_km'] = df_previsao.apply(haversine, axis=1)

        df_previsao.drop(columns=['latitude_parada_rad', 'longitude_parada_rad', 'latitude_veiculo_rad', 'longitude_veiculo_rad'], inplace=True)

        view_previsao = df_previsao.groupby(['veiculo_acessivel_pessoas_deficiencia', 'data', 'hora', 'hora_cheia', 'dia_semana', 'codigo_parada'])[['tempo_espera', 'distancia_km']].mean().reset_index()

        # Exemplo de chamada da função
        salvar_dataframe_incremental_no_minio(
            s3_client= s3_client,
            df_novos=view_previsao,  # DataFrame que você quer salvar
            bucket_name=GOLD_BUCKET,  # Nome do bucket Gold
            gold_file_key=file,  # Caminho do arquivo no Gold
            minio_endpoint=MINIO_ENDPOINT,  # Ajuste para o seu endpoint MinIO
            access_key=MINIO_ACCESS_KEY,  # Insira sua chave de acesso
            secret_key=MINIO_SECRET_KEY,  # Insira sua chave secreta
        )

        salvar_dataframe_no_postgres(
            df=view_previsao,
            tabela_nome='view_previsao',
            schema_nome='gold',
            usuario='airflow',
            senha='airflow',
            host='postgres',
            porta=5432,
            database='postgres',
            method="append"
        )
    else:
        print("Coluna 'horario_previsto_chegada_veiculo_na_parada' ausente. Ignorando a transformação.")


    # Criando o DAG
with DAG(
    'dag_trusted_to_refined_previsao',
    default_args=default_args,
    description='Uma DAG de exemplo para ingestão de dados',
    catchup=False,
    schedule_interval= '*/30 * * * *' #'@hourly',
    
) as dag:

 
    # Definindo a tarefa
    ingest_task = PythonOperator(
        task_id='inserir_gold_previsao',
        python_callable=inserir_dados_gold,
        dag=dag,
    )

    # Definindo a ordem de execução das tarefas
    ingest_task