"""
Setup dos testes de qualidade exemplares — OpenMetadata (perfil governance)
Recria, de forma reprodutível e idempotente, os 5 testes de qualidade sobre
tabelas Silver citados no artigo (validade, acurácia, completude, unicidade).

Sem este script, os testes só existiam como configuração manual feita uma vez
na UI do OpenMetadata — perdida sempre que o volume do OpenMetadata é resetado
(`docker compose down -v`) ou numa clonagem nova do repositório, quebrando a
reprodutibilidade do painel de qualidade mostrado no artigo.

Uso (após `docker compose --profile governance up -d`, `scripts/setup_om_bot_token.py`
já rodado, e a `dag_trino_governance` já ter feito ao menos uma ingestão de metadados
— senão as tabelas/colunas ainda não existem no catálogo do OM):
    python scripts/setup_om_quality_tests.py

Idempotente — pode rodar de novo a qualquer momento; testes/test suites já
existentes são pulados, não duplicados.
"""
import base64
import json
import re
import sys
import urllib.error
import urllib.request
from pathlib import Path

OM_URL = "http://localhost:8585/api/v1"
TRINO_SERVICE = "trino_lakehouse.iceberg"
ENV_PATH = Path(__file__).resolve().parent.parent / ".env"

# batalhao_origem: valores de scripts/gerar_dados.py — mantenha em sincronia se o
# gerador mudar.
_CONFIABILIDADES = ["A1", "A2", "B1", "B2", "C2", "C3"]
_STATUS_PAF = ["SOLICITADO", "APROVADO", "EXECUTADO", "CANCELADO"]

TESTES = [
    {
        "tabela": "relt_intel",
        "coluna": "confiabilidade",
        "nome": "confiabilidade_valores_validos",
        "display": "Confiabilidade restrita à escala adotada (A1 a C3)",
        "definicao": "columnValuesToBeInSet",
        "parametros": [{"name": "allowedValues", "value": json.dumps(_CONFIABILIDADES)}],
    },
    {
        "tabela": "material",
        "coluna": "nivel_combustivel_pct",
        "nome": "combustivel_intervalo_valido",
        "display": "Percentual de combustível entre 0 e 100",
        "definicao": "columnValuesToBeBetween",
        "parametros": [{"name": "minValue", "value": "0"}, {"name": "maxValue", "value": "100"}],
    },
    {
        "tabela": "relt_intel",
        "coluna": "batalhao_origem",
        "nome": "batalhao_origem_completude",
        "display": "Chave de origem (usada nos cruzamentos da Gold) sem nulos",
        "definicao": "columnValuesToBeNotNull",
        "parametros": [],
    },
    {
        "tabela": "paf",
        "coluna": "status_execucao",
        "nome": "status_execucao_valores_validos",
        "display": "Estado do pedido de fogo restrito aos valores válidos do pipeline",
        "definicao": "columnValuesToBeInSet",
        "parametros": [{"name": "allowedValues", "value": json.dumps(_STATUS_PAF)}],
    },
    {
        "tabela": "relt_intel",
        "coluna": "id_relatorio",
        "nome": "id_relatorio_valores_unicos",
        "display": "Unicidade do identificador do relatório de inteligência",
        "definicao": "columnValuesToBeUnique",
        "parametros": [],
    },
]


def _post(path, payload, token=None):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(
        f"{OM_URL}{path}",
        data=json.dumps(payload).encode(),
        headers=headers,
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.load(resp)


def _get(path, token):
    req = urllib.request.Request(f"{OM_URL}{path}", headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.load(resp)


def _existe(path, token):
    try:
        _get(path, token)
        return True
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return False
        raise


def _ler_admin_email():
    if ENV_PATH.exists():
        m = re.search(r"^OM_ADMIN_EMAIL=(.+)$", ENV_PATH.read_text(encoding="utf-8"), re.MULTILINE)
        if m and m.group(1).strip():
            return m.group(1).strip()
    return "admin@open-metadata.org"


def _garantir_test_suite(tabela_fqn, token):
    """Cria a basic test suite da tabela, se ainda não existir."""
    suite_fqn = f"{tabela_fqn}.testSuite"
    if _existe(f"/dataQuality/testSuites/name/{suite_fqn}", token):
        return
    print(f"  Test suite não encontrada — criando para {tabela_fqn}...")
    _post(
        "/dataQuality/testSuites/basic",
        {
            "name": f"{tabela_fqn.split('.')[-1]}_quality_suite",
            "displayName": f"{tabela_fqn.split('.')[-1]} Quality Suite",
            "basicEntityReference": tabela_fqn,
        },
        token=token,
    )


def _garantir_teste(spec, token):
    tabela_fqn = f"{TRINO_SERVICE}.silver.{spec['tabela']}"
    _garantir_test_suite(tabela_fqn, token)

    teste_fqn = f"{tabela_fqn}.{spec['coluna']}.{spec['nome']}"
    if _existe(f"/dataQuality/testCases/name/{teste_fqn}", token):
        print(f"  já existe: {teste_fqn}")
        return

    print(f"  criando: {teste_fqn}")
    _post(
        "/dataQuality/testCases",
        {
            "name": spec["nome"],
            "displayName": spec["display"],
            "testDefinition": spec["definicao"],
            "entityLink": f"<#E::table::{tabela_fqn}::columns::{spec['coluna']}>",
            "parameterValues": spec["parametros"],
        },
        token=token,
    )


def main():
    email = _ler_admin_email()
    senha_b64 = base64.b64encode(b"admin").decode()

    print(f"Autenticando em {OM_URL} como {email} ...")
    try:
        login = _post("/users/login", {"email": email, "password": senha_b64})
    except urllib.error.URLError as e:
        print(f"\nERRO: não foi possível conectar ao OpenMetadata em {OM_URL}.")
        print("Suba o perfil governance e rode a ingestão de metadados antes:")
        print("  docker compose --profile governance up -d")
        print("  docker exec dlh_airflow_webserver airflow dags trigger dag_trino_governance")
        print(f"Detalhe: {e}")
        sys.exit(1)

    token = login["accessToken"]

    print(f"\nConfigurando {len(TESTES)} testes de qualidade exemplares...")
    for spec in TESTES:
        _garantir_teste(spec, token)

    print(
        "\nOK — para executá-los agora, rode o data_quality task da dag_trino_governance "
        "ou o snippet Python do notebook (seção de testes)."
    )


if __name__ == "__main__":
    main()
