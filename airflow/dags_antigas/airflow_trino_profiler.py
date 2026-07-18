#  Copyright 2025 Collate
#  Licensed under the Collate Community License, Version 1.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  https://github.com/open-metadata/OpenMetadata/blob/main/ingestion/LICENSE
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""
DAG para executar o profiler do Trino e armazenar amostras de dados no OpenMetadata.
"""

from datetime import datetime, timedelta
import yaml
from airflow import DAG

try:
    from airflow.operators.python import PythonOperator
except ModuleNotFoundError:
    from airflow.operators.python_operator import PythonOperator

from metadata.workflow.metadata import MetadataWorkflow

default_args = {
    "owner": "user_name",
    "email": ["username@org.com"],
    "email_on_failure": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=60),
}

# Pipeline inline (mesmo conteúdo de infra/openmetadata/ingestion/trino_profiler_custom.yaml)
config = """
source:
  type: trino
  serviceName: trino
  sourceConfig:
    config:
      type: Profiler
processor:
  type: "orm-profiler"
  config: {}
sink:
  type: metadata-rest
  config: {}
workflowConfig:
  openMetadataServerConfig:
    hostPort: "http://openmetadata:8585"
    authProvider: openmetadata
"""


def metadata_ingestion_workflow(**context):
    """Execute o profiler do Trino.
    Se a DAG for disparada pelo webhook, o ``dag_run.conf`` conterá ``table_fqn``.
    Quando presente, incluímos ``entityFullyQualifiedName`` no YAML para que
    o profiler foque apenas nessa tabela; caso contrário o profiler varre
    todas as tabelas do catálogo Trino (comportamento legacy).
    """
    # Obtém o FQN da tabela a partir do contexto (dag_run.conf ou params)
    table_fqn = None
    # ``dag_run`` pode ser None quando a DAG é disparada manualmente pelo UI
    dag_run = context.get('dag_run')
    if dag_run and dag_run.conf:
        table_fqn = dag_run.conf.get('table_fqn')
    # fallback para params (não usado aqui, mas deixa o código flexível)
    if not table_fqn:
        table_fqn = context.get('params', {}).get('table_fqn')

    # Monta o YAML de configuração do profiler. Se houver ``table_fqn``
    # adicionamos o campo ``entityFullyQualifiedName`` para limitar o escopo.
    if table_fqn:
        profiler_yaml = f"""
source:
  type: trino
  serviceName: trino
  sourceConfig:
    config:
      type: Profiler
      entityFullyQualifiedName: \"{table_fqn}\"
processor:
  type: \"orm-profiler\"
  config: {{}}
sink:
  type: metadata-rest
  config: {{}}
workflowConfig:
  openMetadataServerConfig:
    hostPort: \"http://openmetadata:8585/api\"
    authProvider: openmetadata
"""
    else:
        profiler_yaml = config  # usa o config padrão (todas as tabelas)

    workflow_config = yaml.safe_load(profiler_yaml)
    workflow = MetadataWorkflow.create(workflow_config)
    workflow.execute()
    workflow.raise_from_status()
    workflow.print_status()
    workflow.stop()

with DAG(
    "trino_profiler",
    default_args=default_args,
    description="Profiling Trino tables (Iceberg) e coleta de amostras para OpenMetadata",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # dispara manualmente ou via trigger
    is_paused_upon_creation=True,
    catchup=False,
) as dag:
    ingest_task = PythonOperator(
        task_id="run_trino_profiler",
        python_callable=metadata_ingestion_workflow,
    )
