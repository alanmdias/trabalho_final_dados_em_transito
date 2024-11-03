import requests
import pandas as pd
import json
import io

import time
from datetime import datetime

import boto3
from botocore.exceptions import NoCredentialsError

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from typing import Optional
from funcoes import gerar_path, get_dados, get_dados_v2, salvar_json, renomear_chaves

# Definindo variáveis para salvar dados no raw do minio
minio_url = "http://minio:9000" # QUANDO FOR RODAR NO AIRFLOW
#minio_url = "http://localhost:9050" # QUANDO ESTIVER LOCAL
access_key = "pN2nJpDS8zkBM79eIKrh" 
secret_key = "AYoyusCiw9CGodBvpOe3VL5Qlote2SUVSiZnSfxu" 

# Definindo os argumentos padrão para as tarefas do DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

    

s3_client = boto3.client(
    's3',
    endpoint_url = minio_url,
    aws_access_key_id = access_key,
    aws_secret_access_key = secret_key
)
def inserir_dados_silver():
# tratando json com as informacoes de bairro

    BUCKET_NAME = "bronze"
    JSON_FILE_KEY = "aux_data/bairros.geojson"

    def carregar_json_minio():
        # Conexão ao MinIO
        # Obter o objeto do bucket
        obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=JSON_FILE_KEY)
        
        # Carregar o JSON
        data = json.loads(obj['Body'].read().decode('utf-8'))

        return data

    # Carregar os dados do JSON do MinIO
    data = carregar_json_minio()

    # Lista para armazenar os dados desagrupados
    data_list = []

    # Verificar se o JSON é do tipo esperado
    if data.get("type") == "FeatureCollection":
        for feature in data.get("features", []):
            # Extrair propriedades
            properties = feature.get("properties", {})
            nome_distrito = properties.get("NOME_DIST")
            sigla_distrito = properties.get("SIGLA_DIST")
            codigo_distrito = properties.get("COD_DIST")
            
            # Extrair e desagrupar as coordenadas de geometria
            geometry = feature.get("geometry", {})
            if geometry.get("type") == "MultiPolygon":
                for polygon in geometry.get("coordinates", []):
                    for coord in polygon[0]:  # Cada coord é um par [longitude, latitude]
                        longitude, latitude = coord
                        data_list.append({
                            "nome_distrito": nome_distrito,
                            "sigla_distrito": sigla_distrito,
                            "codigo_distrito": codigo_distrito,
                            "latitude": latitude,
                            "longitude": longitude
                        })

    # Criar o DataFrame
    df = pd.DataFrame(data_list)

    df.drop_duplicates(subset=["latitude", "longitude"], inplace=True)

    n = 2

    df['key_loc'] = (abs(df['latitude'].round(n)).astype(str).str.replace('.', '') + 
                    abs(df['longitude'].round(n)).astype(str)).str.replace('.', '')

    df.drop_duplicates(subset=["key_loc"], inplace= True)

    # Transformar colunas em string
    df = df.astype(str)

    # Salvando dados no bucket
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    s3_client.put_object(Bucket="silver", Key='localizacao.csv', Body=csv_buffer.getvalue(), ContentType='text/csv')

    # Tratamento json empresas

    BUCKET_NAME = "bronze"
    JSON_FILE_KEY = "empresa/empresa.json"

    # Carregar os dados do JSON do MinIO
    empresa = carregar_json_minio()

    # Lista para armazenar os dados desagrupados
    data_list = []

    # Extrair dados do JSON
    for item in empresa['e']:
        a_value = item['a']  # Obtendo o valor de 'a' no nível principal
        for sub_item in item['e']:
            data_list.append({
                "a": a_value,
                "c": sub_item['c'],
                "n": sub_item['n']
            })

    # Criar o DataFrame
    df = pd.DataFrame(data_list)

    # Renomear colunas
    df.rename(columns={
        "a": "operation_code",  # Renomeando a coluna 'a'
        "c": "empresa_code",        # Renomeando a coluna 'c'
        "n": "empresa_nome"           # Renomeando a coluna 'n'
    }, inplace=True)

    # Transformar colunas em string
    df = df.astype(str)

    # Salvando dados no bucket
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    s3_client.put_object(Bucket="silver", Key='empresas.csv', Body=csv_buffer.getvalue(), ContentType='text/csv')

# Criando o DAG
with DAG(
    'dag_raw_to_trusted_dimns',
    default_args=default_args,
    description='Uma DAG de exemplo para ingestão de dados',
    catchup=False,
    schedule_interval= '@weekly' #'@hourly',
    
) as dag:

 
    # Definindo a tarefa
    ingest_task = PythonOperator(
        task_id='inserir_silver',
        python_callable=inserir_dados_silver,
        dag=dag,
    )

    # Definindo a ordem de execução das tarefas
    ingest_task