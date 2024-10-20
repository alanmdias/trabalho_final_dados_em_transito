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
   
