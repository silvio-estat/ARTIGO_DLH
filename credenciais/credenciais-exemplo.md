# Credenciais — Ambiente Local (dev)

> Valores de desenvolvimento local, definidos em `.env` (não versionado) e
> `docker-compose.yml`. Servem apenas para o ambiente do protótipo rodando
> via Docker Compose nesta máquina — não usar como referência para produção.

## Airflow — http://localhost:8080
- **Usuário:** `admin`
- **Senha:** `admin`
- E-mail: `admin@dlh.local`
- Criado automaticamente pelo container `airflow-init` (docker-compose.yml, `airflow users create`).

## Trino — http://localhost:8090
- Sem autenticação configurada (`infra/trino/config.properties` não define `http-server.authentication.type`).
- Qualquer nome de usuário é aceito na conexão (ex.: `trino`, `admin`), sem senha.
- Catálogo disponível: `iceberg` (lakehouse).

## MinIO Console — http://localhost:9001 (API em :9000)
- **Usuário:** `minio_admin`
- **Senha:** `minio_pass_2026`
- Variáveis: `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` no `.env`.
- Bucket principal: `lakehouse` (warehouse Iceberg), bucket `landing` (dados brutos JSON).

## PostgreSQL — localhost:5432
- **Usuário:** `dlh_admin`
- **Senha:** `dlh_pass_2026`
- Bancos: `metastore_db` (Hive Metastore), `airflow_db` (Airflow), `airflow_om_db` (Airflow do perfil governance), `openmetadata_db` (perfil governance).
- Variáveis: `POSTGRES_USER` / `POSTGRES_PASSWORD` no `.env`.

## OpenMetadata — http://localhost:8585 (perfil governance)
- **Usuário/e-mail:** `admin@open-metadata.org`
- **Senha:** `admin`
- Bot de ingestão (`ingestion-bot`) precisa de um JWT em `OM_INGESTION_BOT_JWT`
  no `.env` (usado por `dag_trino_governance` e pela emissão de linhagem), e o
  Database Service `trino_lakehouse` precisa estar cadastrado no OpenMetadata
  (aponta para `trino:8090`, catálogo `iceberg`) — sem isso a ingestão de
  metadados falha mesmo com o JWT certo.
  Com o perfil governance no ar (~3 min após o boot), rode no host:
  `python scripts/setup_om_bot_token.py` — resolve os dois (busca o token via
  API do OM e cadastra o serviço, se ainda não existir) e atualiza o `.env`.
  Depois recrie os containers do Airflow (`restart` não relê o `.env`):
  `docker compose up -d airflow-webserver airflow-scheduler`.
  (Alternativa manual: Settings > Bots > ingestion-bot > Generate New Token,
  e Settings > Services > Add New Service > Trino para o serviço.)

## Spark Master UI — http://localhost:8081
## Spark Worker UI — http://localhost:8082
- Sem autenticação.

## Elasticsearch — http://localhost:9200 (perfil governance)
- Sem autenticação configurada neste ambiente.
