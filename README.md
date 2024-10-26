1) Instalar o python 3.10
2) Instalar requirements.txt
3) Instalar o docker 

4) Abrir um terminal e executar para iniciar o ambiente:
```
docker compose -f docker/docker-compose.yml up --build
```

5) Em outro terminal, executar o comando abaixo para acessar o webserver do Airflow:
```
docker compose -f docker/docker-compose.yml exec webserver bash
```

7) No container, criar o usuário admin:
```
airflow users create \
    --username admin \
    --firstname Firstname \
    --lastname Lastname \
    --role Admin \
    --email admin@example.com \
    --password admin
```   
8) Criar as credenciais no minio

9) Ajustar os códigos que necessitam das credenciais do minio

10) No mesmo terminal que foi usado para acessar o webserver do Airflow, executar "exit"

11) Executar o código novamente para atualizar o Airflow:
docker compose -f docker/docker-compose.yml exec webserver bash
