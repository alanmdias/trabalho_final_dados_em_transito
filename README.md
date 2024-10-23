Instalar o python 3.10
Instalar requirements.txt
Instalar o docker 

1) Abrir um terminal e executar para iniciar o ambiente:
docker compose -f docker/docker-compose.yml up --build

2) Em outro terminal, executar o comando abaixo para acessar o webserver do Airflow:
docker compose -f docker/docker-compose.yml exec webserver bash

3) No container, criar o usuário admin:
airflow users create \
    --username admin \
    --firstname Firstname \
    --lastname Lastname \
    --role Admin \
    --email admin@example.com \
    --password admin
   
4) Criar as credenciais no minio

5) Ajustar os códigos que necessitam das credenciais do minio

6) No mesmo terminal que foi usado para acessar o webserver do Airflow, executar "exit"

7) Executar o código novamente para atualizar o Airflow:
docker compose -f docker/docker-compose.yml exec webserver bash
