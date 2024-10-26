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

### VARIÁVEIS DO UTILS ###
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
    'retries': 0,
}


def inserir_dados_raw():
    # Criando uma sessao
    session = requests.Session()

    # Realizando autenticação antes de iniciar requisições
    path_auth = gerar_path(base_url, auth=auth_parametro, token=token)
    auth_response = session.post(path_auth)

    # Fazendo várias requisições com a mesma sessão
    dados_corredor = get_dados_v2(session, base_url, categoria="Corredor", token=token)
    dados_empresa = get_dados_v2(session, base_url, categoria="Empresa", token=token)
    dados_posicao = get_dados_v2(session, base_url, categoria="Posicao", token=token)
    print("Dados de posicao extraídos")

    # Extrair a lista de todas as linhas da posicao
    lista_linhas = list(set([i['cl'] for i in dados_posicao['l']]))

    ################## EXTRAIR DADOS DE PREVISAO DE CHEGADA DE CADA LINHA RETORNADA NO JSON DE POSICAO    ################## 
    dados_previsao = []
    for i in lista_linhas[:]: # FILTRANDO APENAS AS PRIMEIRAS 100
        previsao_linha = get_dados_v2(session, base_url, categoria="Previsao", metodo="Linha", sufix="codigoLinha", parametro=str(i), token=token)
        
        # Adiciona o código da linha 'i' ao JSON de previsão retornado
        if previsao_linha:  # Verifica se a resposta não é nula
            previsao_linha['codigo_linha'] = i  # Adiciona o código da linha ao JSON

            # Adiciona a previsão à lista
            dados_previsao.append(previsao_linha)

    ################## EXTRAIR DADOS DE PARADA DE CADA LINHA RETORNADA NO JSON DE POSICAO    ################## 
    dados_parada = []
    for i in lista_linhas[:]: # FILTRANDO APENAS AS PRIMEIRAS 100
        parada_linha = get_dados_v2(session, base_url, categoria="Parada", metodo="BuscarParadasPorLinha", sufix="codigoLinha", parametro=str(i), token=token)
        
        # Adiciona o código da linha em cada parada retornada
        if parada_linha:  # Verifica se há dados de paradas
            for parada in parada_linha:
                parada['codigo_linha'] = i # Adiciona o código da linha ao JSON
                
            dados_parada.append(parada_linha)
    print("Dados de paradas e previsao extraídos")
    # Finalizando a sessão quando não for mais necessária
    session.close()

    # Gerar variável para salvar no nome do arquivo
    agora = datetime.now().strftime("%Y-%m-%d_%H%M%S")

    # Definindo variáveis para salvar dados no raw do minio
    minio_url = "http://minio:9000" # QUANDO FOR RODAR NO AIRFLOW
    #minio_url = "http://localhost:9050" # QUANDO ESTIVER LOCAL
    access_key = "VsbEm45pTDOcC6dVo5Vk"
    secret_key = "4heTCFf5gT2M4xpJYElhbk8h7fo6TVAn7f4yl4gH"
    

    s3_client = boto3.client(
        's3',
        endpoint_url = minio_url,
        aws_access_key_id = access_key,
        aws_secret_access_key = secret_key
    )

    # Definindo o bucket de destino
    bucket_name = "bronze"

    # Caminhos de cada dado bruto 
    path_posicao = f'/posicao/{agora}.json'
    path_corredor = f'/corredor/corredor.json' #{agora}.json'
    path_empresa = f'/empresa/empresa.json' #{agora}.json'
    path_previsao = f'/previsao/{agora}.json'
    path_parada = f'/parada/{agora}.json'

    # Convertendo o dicionário extraído da API em uma string 
    dados_posicao_s3 = json.dumps(dados_posicao)
    dados_corredor_s3 = json.dumps(dados_corredor)
    dados_empresa_s3 = json.dumps(dados_empresa)
    dados_previsao_s3 = json.dumps(dados_previsao)
    dados_parada_s3 = json.dumps(dados_parada)

    # Salvando dados no bucket
    s3_client.put_object(Bucket=bucket_name, Key=path_posicao, Body=dados_posicao_s3)
    s3_client.put_object(Bucket=bucket_name, Key=path_corredor, Body=dados_corredor_s3)
    s3_client.put_object(Bucket=bucket_name, Key=path_empresa, Body=dados_empresa_s3)
    s3_client.put_object(Bucket=bucket_name, Key=path_previsao, Body=dados_previsao_s3)
    s3_client.put_object(Bucket=bucket_name, Key=path_parada, Body=dados_parada_s3)

    print("Arquivos raw salvos no bucket com sucesso")

#def inserir_dados_trusted():
    mapa_chaves_posicao = {
        'hr': 'hora_referencia_infos',
        'l': 'linhas_localizadas',
        'c': 'letreiro_completo_linha',
        'cl': 'codigo_linha',
        'sl': 'sentido_linha',
        'lt0': 'letreiro_destino_linha',
        'lt1': 'letreiro_origem_linha',
        'qv': 'quantidade_veiculos_localizados',
        'vs': 'veiculos_localizados',
        'p': 'prefixo_veiculo',
        'a': 'veiculo_acessivel_pessoas_deficiencia',
        'ta': 'horario_universal_localizacao_capturada',
        'py': 'latitude_localizada',
        'px': 'longitude_localizada'
        }

    mapa_chaves_previsao = {
        'hr': 'hora_referencia_infos',
        'ps': 'relacao_pontos_parada',
        'cp': 'codigo_parada',
        'np': 'nome_parada',
        'py': 'latitude_localizada',
        'px': 'longitude_localizada',
        'vs': 'veiculos_localizados',
        'p': 'prefixo_veiculo',
        't': 'horario_previsto_chegada_veiculo_na_parada',
        'a': 'veiculo_acessivel_pessoas_deficiencia',
        'ta': 'horario_universal_localizacao_capturada'
        }


    mapa_chaves_paradas = {
        'cp': 'codigo_parada',
        'np': 'nome_parada',
        'ed': 'endereco_localizacao_parada',
        'py': 'latitude_localizada',
        'px': 'longitude_localizada'
        }

    #### SILVER POSICAO ####
    # Aplicar função para ajustar nome dos dados
    dados_posicao_renomeados = renomear_chaves(dados_posicao, mapa_chaves_posicao)

    # Transformar o dicionário renomeado em um DataFrame
    linhas_localizadas = dados_posicao_renomeados['linhas_localizadas']

    # Extraindo informações dos veículos dentro de cada linha localizada
    veiculos_lista = []
    for linha in linhas_localizadas:
        for veiculo in linha['veiculos_localizados']:
            # Adicionando detalhes da linha junto com os veículos
            veiculo_completo = veiculo.copy()  # Copia as informações do veículo
            veiculo_completo.update({
                'hora_referencia_infos': dados_posicao_renomeados['hora_referencia_infos'],
                'letreiro_completo': linha['letreiro_completo_linha'],
                'codigo_linha': linha['codigo_linha'],
                'sentido_linha': linha['sentido_linha'],
                'letreiro_destino_linha': linha['letreiro_destino_linha'],
                'letreiro_origem_linha': linha['letreiro_origem_linha'],
                'quantidade_veiculos_localizados': linha['quantidade_veiculos_localizados']
            })
            veiculos_lista.append(veiculo_completo)

    # Criar o DataFrame com todas as informações dos veículos e suas respectivas linhas
    df_posicao = pd.DataFrame(veiculos_lista)

    # Transformar colunas em string
    df_posicao = df_posicao.astype(str)

    # Salvando dados no bucket
    csv_buffer = io.StringIO()
    df_posicao.to_csv(csv_buffer, index=False)
    s3_client.put_object(Bucket="silver", Key=f'/posicao/{agora}.csv', Body=csv_buffer.getvalue(), ContentType='text/csv')

    #### SILVER PREVISAO ####
    # Renomear as chaves do JSON usando o mapa_chaves_previsao
    dados_previsao_renomeados = renomear_chaves(dados_previsao, mapa_chaves_previsao)

    # Função para transformar o JSON renomeado em DataFrame
    def json_previsao_para_df(dados_renomeados):
        veiculos_lista = []
        for previsao in dados_renomeados:
            codigo_linha = previsao.get('codigo_linha')  # Código da linha renomeado
            hora_referencia = previsao.get('hora_referencia_infos')  # Hora de referência renomeada
            for parada in previsao.get('relacao_pontos_parada', []):  # Lista de paradas renomeada
                codigo_parada = parada.get('codigo_parada')
                nome_parada = parada.get('nome_parada')
                latitude_parada = parada.get('latitude_localizada')
                longitude_parada = parada.get('longitude_localizada')
                
                for veiculo in parada.get('veiculos_localizados', []):  # Acessa cada veículo na parada
                    veiculo_completo = veiculo.copy()
                    veiculo_completo.update({
                        'hora_referencia_infos': hora_referencia,
                        'codigo_parada': codigo_parada,
                        'nome_parada': nome_parada,
                        'latitude_parada': latitude_parada,
                        'longitude_parada': longitude_parada,
                        'codigo_linha': codigo_linha,
                        'latitude_veiculo': veiculo.get('latitude_localizada'),  # Coordenada do veículo renomeada
                        'longitude_veiculo': veiculo.get('longitude_localizada')  # Coordenada do veículo renomeada
                    })
                    veiculos_lista.append(veiculo_completo)
        
        # Criar o DataFrame com as informações extraídas
        df_previsao = pd.DataFrame(veiculos_lista)
        return df_previsao

    # Transformar os dados renomeados em um DataFrame
    df_previsao = json_previsao_para_df(dados_previsao_renomeados)

    df_previsao = df_previsao[[
        'codigo_linha',
        'hora_referencia_infos',
        'codigo_parada',
        'nome_parada',
        'latitude_parada',
        'longitude_parada',
        'prefixo_veiculo',
        'horario_previsto_chegada_veiculo_na_parada',
        'veiculo_acessivel_pessoas_deficiencia',
        'horario_universal_localizacao_capturada',
        'latitude_veiculo',
        'longitude_veiculo'
    ]]
    # Transformar colunas em string
    df_previsao = df_previsao.astype(str)

    # Salvando dados no bucket
    csv_buffer = io.StringIO()
    df_previsao.to_csv(csv_buffer, index=False)
    s3_client.put_object(Bucket="silver", Key=f'/previsao/{agora}.csv', Body=csv_buffer.getvalue(), ContentType='text/csv')


    #### SILVER PARADAS ####
    dados_paradas_renomeados = renomear_chaves(dados_parada, mapa_chaves_paradas)
    # Função para transformar o JSON renomeado em DataFrame
    def json_paradas_para_df(dados_renomeados):
        paradas_lista = []
        
        # Percorre todas as sublistas no JSON
        for sublista in dados_renomeados:
            for parada in sublista:
                paradas_lista.append(parada)
        
        # Criar o DataFrame com os dados das paradas
        df_paradas = pd.DataFrame(paradas_lista)
        return df_paradas

    # Transformar o JSON renomeado em DataFrame
    df_paradas = json_paradas_para_df(dados_paradas_renomeados)

    # Renomear colunas
    df_paradas = df_paradas.astype(str)

    # Salvando dados no bucket
    csv_buffer = io.StringIO()
    df_paradas.to_csv(csv_buffer, index=False)
    s3_client.put_object(Bucket="silver", Key=f'/paradas/{agora}.csv', Body=csv_buffer.getvalue(), ContentType='text/csv')

# Criando o DAG
with DAG(
    'oficial_bronze_prata',
    default_args=default_args,
    description='Uma DAG de exemplo para ingestão de dados',
    catchup=False,
    schedule_interval= '*/10 * * * *' #'@hourly',
    
) as dag:

# TESTE ALTERAção
 
    # Definindo a tarefa
    ingest_task = PythonOperator(
        task_id='inserir_raw',
        python_callable=inserir_dados_raw,
        dag=dag,
    )

    # Definindo a ordem de execução das tarefas
    ingest_task