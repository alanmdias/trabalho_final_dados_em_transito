#with open('imports.py', 'r') as file:
#    imports_code = file.read()

#with open('utils.py', 'r') as file:
#    utils_code = file.read()

#with open('funcoes.py', 'r') as file:
#    funcoes_code = file.read()

#exec(imports_code)
#exec(utils_code)
#exec(funcoes_code)

import requests
import time
from datetime import datetime
import pandas as pd
import json
from typing import Optional

import boto3
from botocore.exceptions import NoCredentialsError

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from funcoes import gerar_path, get_dados, salvar_json, renomear_chaves

# Toekn de autenticação
token = "c7014553669e9dc53bf808f07a85f3fa0ac0eea3db93451487735b128b8e175e"

# URL para autenticação
base_url = "http://api.olhovivo.sptrans.com.br/v2.1"
auth_parametro = "Login/Autenticar"


# Definindo os argumentos padrão para as tarefas do DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}


def inserir_dados_posicao():

    # Criando uma sessao
    session = requests.Session()

    # Fazendo várias requisições com a mesma sessão
    dados_corredor = get_dados(session, base_url, categoria="Corredor", token=token)
    dados_empresa = get_dados(session, base_url, categoria="Empresa", token=token)
    dados_posicao = get_dados(session, base_url, categoria="Posicao", token=token)

    # Finalizando a sessão quando não for mais necessária
    session.close()

    # Gerar variável para salvar no nome do arquivo
    agora = datetime.now().strftime("%Y-%m-%d_%H%M%S")


    minio_url = "http://minio:9000" #"http://localhost:9050"
    access_key = "VsbEm45pTDOcC6dVo5Vk"
    secret_key = "4heTCFf5gT2M4xpJYElhbk8h7fo6TVAn7f4yl4gH"
    bucket_name = "raw"
    object_name = f'/posicao/{agora}.json'


    s3_client = boto3.client(
        's3',
        endpoint_url = minio_url,
        aws_access_key_id = access_key,
        aws_secret_access_key = secret_key
    )

    dados_posicao_s3 = json.dumps(dados_posicao)
    s3_client.put_object(Bucket=bucket_name, Key=object_name, Body=dados_posicao_s3 )

    print("Arquivo salvo com sucesso")

# Criando o DAG
dag = DAG(
    'teste_inserir_dados_posicao',
    default_args=default_args,
    description='Uma DAG de exemplo para ingestão de dados',
    schedule_interval= '*/10 * * * *' #'@hourly',
)

# Definindo a tarefa
ingest_task = PythonOperator(
    task_id='ingest_data',
    python_callable=inserir_dados_posicao,
    dag=dag,
)

# Definindo a ordem de execução das tarefas
ingest_task