# Lakehouse C2 — ARQ_TEMAC

Protótipo de pesquisa (dissertação de mestrado, codinome **ARQ_TEMAC**) que implementa uma plataforma Lakehouse — MinIO (S3) + Apache Iceberg + Hive Metastore + Spark + Trino + Airflow, com OpenMetadata para governança/catálogo/linhagem — para um sistema de C2 (comando e controle) do Exército Brasileiro.

## Índice

- [Arquitetura em resumo](#arquitetura-em-resumo)
- [Pré-requisitos](#pré-requisitos)
- [Passo a passo](#passo-a-passo)
  1. [Clonar e configurar o `.env`](#1-clonar-e-configurar-o-env)
  2. [Subir a stack principal](#2-subir-a-stack-principal)
  3. [Despausar as DAGs](#3-despausar-as-dags)
  4. [Gerar dados sintéticos e rodar o pipeline](#4-gerar-dados-sintéticos-e-rodar-o-pipeline)
  5. [Perfil governance (opcional) — OpenMetadata](#5-perfil-governance-opcional--openmetadata)
  6. [Notebook de exemplo — consultas via Trino](#6-notebook-de-exemplo--consultas-via-trino)
- [Coisas que pegam desprevenido](#coisas-que-pegam-desprevenido)
- [Credenciais](#credenciais)

## Arquitetura em resumo

Os dados fluem em um único sentido por três camadas Iceberg (padrão *medallion*), todas no catálogo `lakehouse.*` (visão do Spark) / `iceberg.*` (visão do Trino), armazenadas em `s3a://lakehouse/warehouse` no MinIO:

```
MinIO landing/ (JSON bruto)
   → Bronze  (lakehouse.bronze.dados)     — dag_ingestao_bronze, spark/jobs/bronze_ingestor.py
   → Silver  (lakehouse.silver.<tipo>)    — dag_silver_transform, spark/jobs/bronze_to_silver.py
   → Gold    (lakehouse.gold.<visao>)     — dag_gold_refresh, spark/jobs/silver_to_gold.py
```

- **Bronze**: lê todo JSON que estiver em `landing/`, carimba proveniência (`id_registro`, `timestamp_chegada`, `id_lote`) e faz append numa única tabela particionada por dia de chegada + batalhão. Nunca sofre `DELETE`/`UPDATE`.
- **Silver**: uma task por tipo de dado (8 no total, rodando sequencialmente — o cluster Spark só tem 1 core), cada uma com sua própria tabela tipada, populada via `MERGE INTO` ou `INSERT ... WHERE NOT EXISTS` a partir da Bronze.
- **Gold**: zona de disponibilização analítica. A base harmonizada da Silver é organizada em visões cuja estrutura espelha os processos formais do estado-maior da brigada — a arquitetura não fixa um conjunto rígido de visões, cada processo doutrinário pode originar a sua, todas convergindo para o Cenário Operacional Comum (COC), a síntese integrada que ampara a decisão do comandante. Hoje o protótipo implementa 4: `coc` (Cenário Operacional Comum, EB70-MC-10.205), `pitcic` (integração Terreno/Met./Inimigo/Civis, EB70-MC-10.336), `ppcot` (planejamento e condução das operações terrestres, EB70-MC-10.211) e `avaliacao` (MEF/MED de monitoramento da condução, EB70-MC-10.211 Cap. V) — construídas via `JOIN` entre tabelas Silver.

**Os 8 tipos de dado** (funções de combate): `gps`, `sensor` (reconhecimento por drone), `relt_intel` (relatórios de inteligência), `paf` (pedido de apoio de fogo), `obstaculo` (obstáculos de terreno), `seg_area` (segurança de área), `pessoal` (efetivo/S1), `material` (viaturas/S4).

**Governança (perfil `governance`, opcional)**: OpenMetadata cataloga as tabelas via `dag_trino_governance` e recebe eventos de linhagem (SQL + lineage de coluna) emitidos manualmente por `airflow/dags/helpers/lineage_emitter.py` a cada execução do Silver/Gold — o listener nativo do Spark não resolve o namespace certo, então essa DAG existe para contornar isso.

## Pré-requisitos

- Docker Desktop com WSL2 (Windows) ou Docker Engine (Linux/macOS) — a stack inteira roda em containers, não há execução local do pipeline em si.
- Python >= 3.10 (para `scripts/gerar_dados.py` e o notebook de exemplo).
- ~70 GB de espaço em disco livres (imagens Docker + volumes do Iceberg/MinIO/Postgres somam bastante).

## Passo a passo

### 1. Clonar e configurar o `.env`

```bash
git clone <este-repo>
cd <pasta-do-repo>
cp .env.example .env          # Git Bash / Linux / macOS
# Copy-Item .env.example .env # PowerShell
```

O `.env.example` já vem com os mesmos valores hardcoded em `infra/trino/catalog/*.properties` — são credenciais de desenvolvimento local, não secretas (ver [Credenciais](#credenciais) abaixo). Não precisa editar nada para rodar localmente.

### 2. Subir a stack principal

```bash
docker compose up -d
docker compose ps    # confira que todos os serviços estão healthy/running
```

Isso sobe Postgres, MinIO, Hive Metastore, Spark (master + worker), Trino e Airflow. O Airflow demora ~1 min para o `airflow-init` criar o usuário admin (`admin`/`admin`). UIs: Airflow em http://localhost:8080, Trino em http://localhost:8090, MinIO console em http://localhost:9001, Spark master em http://localhost:8081.

### 3. Despausar as DAGs

**Toda DAG nasce pausada** (`AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: "True"` no `docker-compose.yml`) — mesmo tendo um `schedule` configurado, nenhuma roda sozinha até isso:

```bash
docker exec dlh_airflow_webserver airflow dags unpause dag_ingestao_bronze
docker exec dlh_airflow_webserver airflow dags unpause dag_silver_transform
docker exec dlh_airflow_webserver airflow dags unpause dag_gold_refresh
docker exec dlh_airflow_webserver airflow dags unpause dag_iceberg_maintenance
```

### 4. Gerar dados sintéticos e rodar o pipeline

```bash
bash setup_venv.sh        # Git Bash / Linux / macOS — cria ./venv, instala requirements.txt
# .\setup_venv.ps1        # PowerShell equivalente

./venv/Scripts/python.exe scripts/gerar_dados.py --lotes 15 --registros 300   # Windows
# ./venv/bin/python scripts/gerar_dados.py --lotes 15 --registros 300        # Linux/macOS
```

Isso escreve JSON em `landing/` no MinIO. A partir daí:

- `dag_ingestao_bronze` roda a cada 5 min e ingere tudo que estiver em `landing/`, removendo os arquivos processados ao final (não reingere/duplica em execuções seguintes).
- `dag_silver_transform` roda a cada 15 min (8 tasks sequenciais — o cluster Spark só tem 1 core).
- `dag_gold_refresh` roda a cada 20 min (4 tasks sequenciais).

Para não esperar os agendamentos, dispare manualmente na ordem certa (cada um só faz sentido depois que o anterior tiver dados):

```bash
docker exec dlh_airflow_webserver airflow dags trigger dag_ingestao_bronze
# espere terminar (airflow dags list-runs -d dag_ingestao_bronze), depois:
docker exec dlh_airflow_webserver airflow dags trigger dag_silver_transform
# espere (~10 min, 8 tasks sequenciais), depois:
docker exec dlh_airflow_webserver airflow dags trigger dag_gold_refresh
# ~10-15 min, 4 tasks sequenciais
```

Confira o resultado via Trino:

```bash
docker exec dlh_trino trino --server http://localhost:8090 --execute "SELECT COUNT(*) FROM iceberg.gold.pitcic"
```

### 5. Perfil governance (opcional) — OpenMetadata

```bash
docker compose --profile governance up -d
# aguarde ~3 min o OpenMetadata inicializar, depois:
python scripts/setup_om_bot_token.py
```

Esse script busca o JWT do `ingestion-bot` e cadastra o serviço `trino_lakehouse` no OpenMetadata via API, preenchendo `OM_INGESTION_BOT_JWT` no `.env` automaticamente — sem isso, `dag_trino_governance` e a emissão de linhagem falham. Depois, **recrie** (não `restart`) os containers do Airflow para o novo `.env` ser lido:

```bash
docker compose up -d airflow-webserver airflow-scheduler
```

(`restart` mantém o ambiente antigo do container — só `up -d` relê o `.env`.)

Com isso, `dag_trino_governance` (disparo manual) já roda ingestão de metadados → profiler → testes de qualidade → sample data:

```bash
docker exec dlh_airflow_webserver airflow dags unpause dag_trino_governance
docker exec dlh_airflow_webserver airflow dags trigger dag_trino_governance
```

OpenMetadata fica em http://localhost:8585 (`admin@open-metadata.org` / `admin`).

**Testes de qualidade (data quality):** nenhum test suite é criado automaticamente por nada no código. A task `data_quality` só executa test suites que já existirem no catálogo — se não existir nenhum, ela roda e não faz nada (sem erro, mas sem testar nada). Para configurar um teste, use a UI do OpenMetadata (Data Quality → Add Test) ou a API REST (`POST /api/v1/dataQuality/testSuites/basic` + `POST /api/v1/dataQuality/testCases`, apontando `entityLink` para a coluna desejada).

### 6. Notebook de exemplo — consultas via Trino

`exemplo_consulta_trino/consulta_pitcic.ipynb` mostra como consultar o Lakehouse direto do Python usando o cliente Trino, com a visão `gold.pitcic` como exemplo: consultas diretas no Trino (agregação, filtro, `JOIN` entre camadas, e até um exemplo de time-travel usando o histórico de snapshots do Iceberg), o padrão inverso de baixar tudo para um `DataFrame` pandas e analisar localmente, e alguns gráficos com `matplotlib`.

```bash
./venv/Scripts/python.exe -m pip install -r requirements.txt
./venv/Scripts/python.exe -m ipykernel install --user --name lakehouse-venv --display-name "Python (Lakehouse venv)"
./venv/Scripts/python.exe -m jupyter lab
```

Abra o notebook e selecione o kernel **Python (Lakehouse venv)**. Requer a `gold.pitcic` já populada (passo 4) — sem isso as consultas rodam mas retornam vazio.

## Coisas que pegam desprevenido

- **`docker compose restart` não relê o `.env`** — só `docker compose up -d <serviço>` recria o container com valores novos.
- **Todas as DAGs nascem pausadas** — não é só `dag_trino_governance`, é todo mundo (passo 3).
- **Spark tem só 1 core** — as DAGs multi-task encadeiam sequencialmente de propósito; rodar bronze+silver+gold do zero facilmente passa de 30-40 min.
- **`dag_ingestao_bronze` lê tudo que estiver em `landing/` a cada execução** e remove os arquivos processados ao final (`max_active_runs=1`, então não roda em paralelo consigo mesma). Isso corrige um bug real que existia até 2026-07-19: sem a remoção, execuções agendadas sucessivas reingeriam os mesmos arquivos e duplicavam a Bronze. Como defesa extra, as transformações Bronze→Silver também deduplicam a própria origem via `ROW_NUMBER()` antes do `MERGE`/`INSERT`, então mesmo que duplicatas cheguem à Bronze por algum outro motivo, não vazam para o Silver.

## Credenciais

| Serviço | Usuário | Senha |
|---|---|---|
| Airflow (http://localhost:8080) | `admin` | `admin` |
| MinIO Console (http://localhost:9001) | `minio_admin` | `minio_pass_2026` |
| PostgreSQL (`localhost:5432`) | `dlh_admin` | `dlh_pass_2026` |
| Trino (http://localhost:8090) | qualquer nome | sem senha (sem autenticação) |
| OpenMetadata (http://localhost:8585, perfil governance) | `admin@open-metadata.org` | `admin` |

Todas de desenvolvimento local, sem valor de segurança real (batem com os arquivos `infra/*/*.properties` já commitados) — não usar como referência para produção. Detalhes: `credenciais/credenciais-exemplo.md`.
