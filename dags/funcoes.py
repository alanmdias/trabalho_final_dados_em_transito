from typing import Optional

import boto3
import re
from datetime import datetime, timedelta
import pandas as pd
from io import BytesIO
import io
import numpy as np

from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, MetaData, Table, inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import ProgrammingError

# Função para gerar a URL do path
def gerar_path(base, 
               categoria: Optional[str] = "",
               metodo: Optional[str] = "", 
               sufix: Optional[str] = "", 
               parametro: Optional[str] = "",
               auth: Optional[str] = "",
               token: Optional[str] = ""):
    
    if token != "":
        path = base + "/" + auth + "?token=" + token # Corrigindo como gerar URL de autenticação
    elif metodo == "":
        path = base + "/" + categoria
    elif sufix == "":
        path = base + "/" + categoria + "/" + metodo    
    else:
        path = base + "/" + categoria + "/" + metodo + "?" + sufix + "=" + parametro
    return path


# Função para obter dados, agora reutilizando a sessão
def get_dados(session, 
         base, 
         categoria,
         token: Optional[str] = "",
         metodo: Optional[str] = "", 
         sufix: Optional[str] = "",
         auth: Optional[str] = "", 
         parametro: Optional[str] = ""):

    # Corrigindo o path para autenticação
    path_auth = gerar_path(base, auth="Login/Autenticar", token=token)
    
    auth_response = session.post(path_auth)
    
    # Verifica se a autenticação foi bem-sucedida
    if auth_response.text == "true":
        print("Autenticacao realizada com sucesso!")
        
        # Gerar o path para buscar os dados
        path_get = gerar_path(base, categoria, metodo, sufix, parametro, token="")
        response_get = session.get(path_get)
        
        if response_get.status_code == 200:
            dados = response_get.json()
            return dados
        else:
            print(f"Erro na requisicao de dados: {response_get.status_code}")
            return None
    else:
        print("Falha na autenticacao:", auth_response.text)
        return None
    
# Função para salvar o arquivo json
def salvar_json(dados, path, encoding='utf-8'):
    with open(path, 'w', encoding=encoding) as f:
        json.dump(dados, f, ensure_ascii=False, indent=4)
    return "Arquivo salvo com sucesso!"

# Função para renomear as chaves do dicionário
def renomear_chaves(dado, mapa):
    if isinstance(dado, dict):
        return {mapa.get(k, k): renomear_chaves(v, mapa) for k, v in dado.items()}
    elif isinstance(dado, list):
        return [renomear_chaves(item, mapa) for item in dado]
    else:
        return dado
    

# Função para obter dados, agora reutilizando a sessão
def get_dados_v2(session, 
         base, 
         categoria,
         token: Optional[str] = "",
         metodo: Optional[str] = "", 
         sufix: Optional[str] = "",
         auth: Optional[str] = "", 
         parametro: Optional[str] = ""):

    # Corrigindo o path para autenticação
    #path_auth = gerar_path(base, auth="Login/Autenticar", token=token)
    
    #auth_response = session.post(path_auth)
    
    # Verifica se a autenticação foi bem-sucedida
    #if auth_response.text == "true":
        #print("Autenticacao realizada com sucesso!")
        
        # Gerar o path para buscar os dados
        path_get = gerar_path(base, categoria, metodo, sufix, parametro, token="")
        response_get = session.get(path_get)
        
        if response_get.status_code == 200:
            dados = response_get.json()
            return dados
        else:
            print(f"Erro na requisicao de dados: {response_get.status_code}")
            return None
    #else:
        #print("Falha na autenticacao:", auth_response.text)
        #return None

# Funcao obter dias da semana
def obter_dia_semana(data):
    dias_semana = ["Segunda", "Terça", "Quarta", "Quinta", "Sexta", "Sábado", "Domingo"]
    return dias_semana[data.weekday()]

# Funcao estrair dados silver
def extrair_dados_silver(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY,SILVER_BUCKET,prefix, pattern):
    # Conexão ao MinIO
    s3_client = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    # Listar objetos no bucket com o prefixo da pasta desejada
    response = s3_client.list_objects_v2(Bucket=SILVER_BUCKET, Prefix=prefix)
    
    # Inicializar uma lista para armazenar DataFrames
    dfs = []

    # Verificar se o bucket contém objetos
    if 'Contents' in response:
        for item in response['Contents']:
            file_key = item['Key']
            
            # Filtrar para processar apenas arquivos dentro da pasta desejada
            if file_key.startswith(prefix):
                # Extrair data e hora do nome do arquivo
                match = re.search(pattern, file_key)
                if match:
                    data_str = match.group(1)  # Pega a data no formato YYYY-MM-DD
                    hora_str = match.group(2)  # Pega a hora no formato HHMMSS

                    # Combinar data e hora e subtrair 3 horas
                    datetime_str = f"{data_str} {hora_str}"
                    datetime_obj = datetime.strptime(datetime_str, "%Y-%m-%d %H%M%S") - timedelta(hours=3)
                    
                    # Obter data e hora ajustadas
                    data_ajustada = datetime_obj.strftime("%Y-%m-%d")
                    hora_ajustada = datetime_obj.strftime("%H:%M:%S")
                    hora_cheia = datetime_obj.strftime("%H:00:00")
                    dia_semana = obter_dia_semana(datetime_obj)

                    print(f"Lendo arquivo: {file_key} | Data: {data_ajustada} | Hora: {hora_ajustada}")  # Para depuração

                    # Ler o arquivo CSV
                    obj = s3_client.get_object(Bucket=SILVER_BUCKET, Key=file_key)
                    df_silver = pd.read_csv(obj['Body'])
                    
                    # Adicionar colunas de data e hora ao DataFrame
                    df_silver['data'] = data_ajustada
                    df_silver['hora'] = hora_ajustada
                    df_silver['hora_cheia'] = hora_cheia
                    df_silver['dia_semana'] = dia_semana
                    dfs.append(df_silver)

    # Concatenar todos os DataFrames em um único DataFrame
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    else:
        print("Nenhum arquivo encontrado.")
        return pd.DataFrame()  # Retornar um DataFrame vazio se não houver arquivos

# FUNCAO CONSTRUIR DIMENSOES DE PARADAS
def dimension_paradas(df):

    df = df.drop_duplicates(subset=["codigo_parada", "nome_parada", "latitude_localizada", "longitude_localizada"]).reset_index(drop=True)
    df = df[["codigo_parada", "nome_parada", "latitude_localizada", "longitude_localizada"]]
    df["codigo_parada"] = df['codigo_parada'].round(0).astype(str)
    df = df.rename(columns={"latitude_localizada":"latitude", "longitude_localizada": "longitude"})
    n = 2
    df['key_loc'] = (abs(df['latitude'].round(n)).astype(str).str.replace('.', '') + 
                    abs(df['longitude'].round(n)).astype(str)).str.replace('.', '')
    
    return df

# FUNCAO EXTRAIR DADOS SILVER V2
def extrair_dados_silver_v2(MINIO_ENDPOINT, MINIO_ACCESS_KEY,MINIO_SECRET_KEY, SILVER_BUCKET, file):
    # Conexão ao MinIO
    s3_client = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )
    obj = s3_client.get_object(Bucket=SILVER_BUCKET, Key= file)
    df_silver = pd.read_csv(obj['Body'])
    return df_silver

# FUNCAO SALVAR DATAFRAME NO MINIO
def salvar_dataframe_no_minio(df, bucket_name, file_name, minio_endpoint, access_key, secret_key):
    """
    Transforma um DataFrame em Parquet e salva no bucket MinIO da camada gold.

    Parâmetros:
    - df (pd.DataFrame): DataFrame a ser salvo.
    - bucket_name (str): Nome do bucket no MinIO.
    - file_name (str): Nome do arquivo Parquet a ser salvo (ex: 'dados_gold.parquet').
    - minio_endpoint (str): URL de endpoint do MinIO (ex: 'http://localhost:9000').
    - access_key (str): Chave de acesso ao MinIO.
    - secret_key (str): Chave secreta para acesso ao MinIO.
    """

    # Configura o sistema de arquivos S3 com o endpoint do MinIO
    # Configura o cliente S3 usando boto3 com o endpoint do MinIO
    s3_client = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url=minio_endpoint
    )

    # Converte o DataFrame para Parquet e armazena em um buffer de memória
    buffer = BytesIO()
    df.to_parquet(buffer, engine='pyarrow')
    buffer.seek(0)  # Retorna o ponteiro para o início do buffer

    # Faz o upload do buffer para o bucket MinIO
    s3_client.upload_fileobj(buffer, bucket_name, file_name)
    
    print(f"Arquivo salvo no bucket '{bucket_name}' como '{file_name}' com sucesso.")

# FUNCAO VIEW POSICAO
def transform_posicao(df):

    df = df[["veiculo_acessivel_pessoas_deficiencia", "latitude_localizada", "longitude_localizada", "quantidade_veiculos_localizados", "data", "hora", "hora_cheia", "dia_semana"]]
    n = 2
    df = df.rename(columns={"latitude_localizada":"latitude", "longitude_localizada": "longitude"})
    df['key_loc'] = (abs(df['latitude'].round(n)).astype(str).str.replace('.', '') + 
                    abs(df['longitude'].round(n)).astype(str)).str.replace('.', '')
    df = df[["veiculo_acessivel_pessoas_deficiencia", "quantidade_veiculos_localizados", "data", "hora", "hora_cheia", "dia_semana", "key_loc"]]
    
    return df

# FUNCAO CALCULO DISTANCIA

# Fórmula de Haversine
def haversine(row):
    dlat = row['latitude_veiculo_rad'] - row['latitude_parada_rad']
    dlon = row['longitude_veiculo_rad'] - row['longitude_parada_rad']
    a = np.sin(dlat/2)**2 + np.cos(row['latitude_parada_rad']) * np.cos(row['latitude_veiculo_rad']) * np.sin(dlon/2)**2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    distancia = 6371 * c  # Raio médio da Terra em km
    return distancia

# ------ FUNCOES INCREMENTAIS -----

def extrair_dados_silver_v3(s3_client, minio_endpoint, minio_access_key, minio_secret_key, silver_bucket, gold_bucket, gold_file_key, prefix, pattern):

    # Carregar dados do arquivo já processado no bucket Gold, se existir
    try:
        obj_gold = s3_client.get_object(Bucket=gold_bucket, Key=gold_file_key)
        # Ler o arquivo Parquet usando io.BytesIO
        df_gold = pd.read_parquet(io.BytesIO(obj_gold['Body'].read()))  # Ler o arquivo Parquet

        # Ajuste de fuso horário: convertendo de Brasília (UTC-3) para UTC
        df_gold['data_hora'] = pd.to_datetime(df_gold['data'] + ' ' + df_gold['hora'])
        df_gold['data_hora'] = df_gold['data_hora'] + timedelta(hours=3)  # Ajustar para UTC
        df_gold['nome_arquivo'] = df_gold['data_hora'].dt.strftime('%Y-%m-%d_%H%M%S') + '.csv'
    except Exception as e:
        print(f"Erro ao ler arquivo do bucket Gold: {e}")
        df_gold = pd.DataFrame()  # Caso não exista arquivo, cria um DataFrame vazio

    # Listar objetos no bucket Silver com o prefixo da pasta desejada
    response_silver = s3_client.list_objects_v2(Bucket=silver_bucket, Prefix=prefix)
    
    # Inicializar uma lista para armazenar DataFrames
    dfs = []

    # Verificar se o bucket Silver contém objetos
    if 'Contents' in response_silver:
        for item in response_silver['Contents']:
            file_key = item['Key']
            
            # Filtrar para processar apenas arquivos dentro da pasta desejada
            if file_key.startswith(prefix):
                # Extrair data e hora do nome do arquivo
                match = re.search(pattern, file_key)
                if match:
                    data_str = match.group(1)  # Pega a data no formato YYYY-MM-DD
                    hora_str = match.group(2)  # Pega a hora no formato HHMMSS

                    # Combinar data e hora
                    datetime_str = f"{data_str} {hora_str}"
                    datetime_obj = datetime.strptime(datetime_str, "%Y-%m-%d %H%M%S") - timedelta(hours=3)

                    # Verificar se a data e hora ajustadas já estão no DataFrame Gold
                    if not df_gold.empty:
                        if file_key in prefix + df_gold['nome_arquivo'].values:
                            print(f"Registro já processado: {file_key}. Pulando.")
                            continue

                    # Obter data e hora ajustadas
                    data_ajustada = datetime_obj.strftime("%Y-%m-%d")
                    hora_ajustada = datetime_obj.strftime("%H:%M:%S")  # Agora inclui segundos
                    hora_cheia = datetime_obj.strftime("%H:00:00")
                    dia_semana = obter_dia_semana(datetime_obj)

                    # Ajustar a hora para o formato necessário
                    hora_formatada = datetime_obj.strftime("%H%M%S")  # Formato HHMMSS
                    nome_arquivo_formatado = f"{data_ajustada}_{hora_formatada}.csv"  # Novo nome do arquivo

                    print(f"Lendo arquivo: {file_key} | Data: {data_ajustada} | Hora: {hora_ajustada} | Nome do Arquivo Formatado: {nome_arquivo_formatado}")  # Para depuração

                    # Ler o arquivo CSV
                    obj = s3_client.get_object(Bucket=silver_bucket, Key=file_key)
                    df_silver = pd.read_csv(io.BytesIO(obj['Body'].read()))  # Usar io.BytesIO para ler o CSV
                    
                    # Adicionar colunas de data e hora ao DataFrame
                    df_silver['data'] = data_ajustada
                    df_silver['hora'] = hora_ajustada
                    df_silver['hora_cheia'] = hora_cheia
                    df_silver['dia_semana'] = dia_semana
                    df_silver['nome_arquivo'] = nome_arquivo_formatado  # Adiciona o nome do arquivo formatado
                    dfs.append(df_silver)

    # Concatenar todos os DataFrames em um único DataFrame
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    else:
        print("Nenhum arquivo novo encontrado.")
        return pd.DataFrame()  
    

def salvar_dataframe_incremental_no_minio(s3_client, df_novos, bucket_name, gold_file_key, minio_endpoint, access_key, secret_key):
    """
    Salva um DataFrame incremental no bucket MinIO da camada gold.

    Parâmetros:
    - df_novos (pd.DataFrame): DataFrame a ser salvo, contendo novos dados.
    - bucket_name (str): Nome do bucket no MinIO.
    - gold_file_key (str): Caminho do arquivo Parquet a ser salvo ou atualizado no bucket Gold.
    - minio_endpoint (str): URL de endpoint do MinIO (ex: 'http://localhost:9000').
    - access_key (str): Chave de acesso ao MinIO.
    - secret_key (str): Chave secreta para acesso ao MinIO.
    """

    # Verifica se o arquivo Parquet já existe no bucket Gold
    try:
        obj = s3_client.get_object(Bucket=bucket_name, Key=gold_file_key)
        df_existente = pd.read_parquet(io.BytesIO(obj['Body'].read()))
    except s3_client.exceptions.NoSuchKey:
        # Se o arquivo não existe, cria um novo DataFrame vazio
        df_existente = pd.DataFrame()

    # Concatena os novos dados com os dados existentes
    df_completo = pd.concat([df_existente, df_novos], ignore_index=True)

    # Salva o DataFrame combinado como Parquet no MinIO
    with io.BytesIO() as buffer:
        df_completo.to_parquet(buffer, index=False, engine='pyarrow')
        buffer.seek(0)  # Volta o ponteiro para o início do buffer

        # Faz upload do arquivo Parquet para o MinIO
        s3_client.put_object(Bucket=bucket_name, Key=gold_file_key, Body=buffer.getvalue())
    
    print(f"Arquivo salvo no bucket '{bucket_name}' como '{gold_file_key}' com sucesso.")

# Funcoes insert postgres

def criar_tabela_dim_paradas(engine, tabela_nome, schema_nome):

    metadata = MetaData(schema=schema_nome)

    # Definição da tabela (adapte os tipos conforme as colunas do seu DataFrame)
    tabela_gold = Table(
        tabela_nome,
        metadata,
        Column('codigo_parada', Integer, primary_key=True),
        Column('nome_parada', String),
        Column('latitude', Float),
        Column('longitude', Float),
        Column('key_loc', String),
        Column('nome_distrito', String),
        Column('sigla_distrito', String),
        Column('codigo_distrito', String)
        # Adicione mais colunas conforme necessário
    )

    # Cria a tabela no banco se ela ainda não existir
    metadata.create_all(engine)
    print(f"Tabela '{schema_nome}.{tabela_nome}' criada com sucesso (se não existia).")

def criar_tabela_view_paradas(engine, tabela_nome, schema_nome):

    metadata = MetaData(schema=schema_nome)

    # Definição da tabela (adapte os tipos conforme as colunas do seu DataFrame)
    tabela_gold = Table(
        tabela_nome,
        metadata,
        Column('codigo_parada', Integer),
        Column('nome_parada', String),
        Column('data', String),
        Column('hora', String),
        Column('hora_cheia', String),
        Column('dia_semana', String),
        Column('codigo_linha', Integer)
        # Adicione mais colunas conforme necessário
    )

    # Cria a tabela no banco se ela ainda não existir
    metadata.create_all(engine)
    print(f"Tabela '{schema_nome}.{tabela_nome}' criada com sucesso (se não existia).")

def criar_tabela_view_posicao(engine, tabela_nome, schema_nome):

    metadata = MetaData(schema=schema_nome)

    # Definição da tabela (adapte os tipos conforme as colunas do seu DataFrame)
    tabela_gold = Table(
        tabela_nome,
        metadata,
        Column('veiculo_acessivel_pessoas_deficiencia', String),
        Column('data', String),
        Column('hora', String),
        Column('hora_cheia', String),
        Column('dia_semana', String),
        Column('key_loc', String),
        Column('quantidade_veiculos_localizados', Integer)
        # Adicione mais colunas conforme necessário
    )

    # Cria a tabela no banco se ela ainda não existir
    metadata.create_all(engine)
    print(f"Tabela '{schema_nome}.{tabela_nome}' criada com sucesso (se não existia).")

def criar_tabela_view_previsao(engine, tabela_nome, schema_nome):

    metadata = MetaData(schema=schema_nome)

    # Definição da tabela (adapte os tipos conforme as colunas do seu DataFrame)
    tabela_gold = Table(
        tabela_nome,
        metadata,
        Column('veiculo_acessivel_pessoas_deficiencia', String),
        Column('data', String),
        Column('hora', String),
        Column('hora_cheia', String),
        Column('dia_semana', String),
        Column('codigo_parada', String),
        Column('tempo_espera', Float),
        Column('distancia_km', Float)
        # Adicione mais colunas conforme necessário
    )

    # Cria a tabela no banco se ela ainda não existir
    metadata.create_all(engine)
    print(f"Tabela '{schema_nome}.{tabela_nome}' criada com sucesso (se não existia).")


def salvar_dataframe_no_postgres(df, tabela_nome, schema_nome, usuario, senha, host, porta, database, method):
    
    # String de conexão com o PostgreSQL
    conn_str = f'postgresql://{usuario}:{senha}@{host}:{porta}/{database}'
    engine = create_engine(conn_str)
    Session = sessionmaker(bind=engine)

    # Verifica se a tabela já existe
    insp = inspect(engine)
    tabela_existe = insp.has_table(tabela_nome, schema=schema_nome)

    # Se a tabela não existe, cria a tabela
    if not tabela_existe:
        if tabela_nome == "dim_paradas":
            criar_tabela_dim_paradas(engine, tabela_nome, schema_nome)
        elif tabela_nome == "view_paradas":
            criar_tabela_view_paradas(engine, tabela_nome, schema_nome)
        elif tabela_nome == "view_posicao":
            criar_tabela_view_posicao(engine, tabela_nome, schema_nome)
        elif tabela_nome == "view_previsao":
            criar_tabela_view_previsao(engine, tabela_nome, schema_nome)

    # Insere os dados no PostgreSQL
    try:
        with engine.connect() as conn:
            df.to_sql(
                name=tabela_nome,
                con=conn,
                schema=schema_nome,
                if_exists=method,  # Apenas adiciona os dados
                index=False
            )
        print(f"Dados inseridos na tabela '{schema_nome}.{tabela_nome}' com sucesso.")
    except Exception as e:
        print(f"Erro ao inserir os dados: {e}")