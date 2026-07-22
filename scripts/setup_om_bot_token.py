"""
Setup do OpenMetadata (perfil governance) para a DAG `dag_trino_governance`
Faz os dois passos manuais que, se pulados, quebram a DAG num clone novo do repo:

1. Recupera o JWT do bot `ingestion-bot` (nasce com um token Unlimited no
   bootstrap do OM — só precisa buscar via API, não recriar) e grava em
   OM_INGESTION_BOT_JWT no .env.
2. Cadastra o Database Service `trino_lakehouse` no OpenMetadata (aponta para
   trino:8090, catálogo iceberg) — sem ele, `MetadataWorkflow` falha com
   "Error getting the service [trino_lakehouse] from the API" mesmo com JWT válido.

Ambos usam o login de admin (credenciais fixas de dev, ver credenciais/credenciais-exemplo.md).

Ao final, recria os containers do Airflow para que releiam o .env novo — esse é o
passo que, se esquecido, mantém o token velho no container e faz a DAG falhar com
"The given token does not match the current bot's token" (`restart` NÃO basta).

Uso (após `docker compose --profile governance up -d` e OpenMetadata de pé,
~3 min no primeiro boot):
    python scripts/setup_om_bot_token.py

Idempotente — pode rodar de novo a qualquer momento (ex.: após `docker compose down -v`,
que apaga o volume do OpenMetadata e portanto o bot token e os serviços cadastrados).
"""
import base64
import json
import re
import shutil
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path

OM_URL = "http://localhost:8585/api/v1"
ENV_PATH = Path(__file__).resolve().parent.parent / ".env"


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
    req = urllib.request.Request(
        f"{OM_URL}{path}",
        headers={"Authorization": f"Bearer {token}"},
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.load(resp)


def _garantir_servico_trino(token_admin):
    """Cadastra o Database Service 'trino_lakehouse' no OM, se ainda não existir."""
    req = urllib.request.Request(
        f"{OM_URL}/services/databaseServices/name/trino_lakehouse",
        headers={"Authorization": f"Bearer {token_admin}"},
    )
    try:
        urllib.request.urlopen(req, timeout=15)
        print("Serviço 'trino_lakehouse' já cadastrado no OpenMetadata — nada a fazer.")
        return
    except urllib.error.HTTPError as e:
        if e.code != 404:
            raise

    print("Serviço 'trino_lakehouse' não encontrado — cadastrando...")
    payload = {
        "name": "trino_lakehouse",
        "serviceType": "Trino",
        "connection": {
            "config": {
                "type": "Trino",
                "hostPort": "trino:8090",
                "username": "admin",
                "catalog": "iceberg",
                "connectionArguments": {"http_scheme": "http"},
            }
        },
    }
    _post("/services/databaseServices", payload, token=token_admin)
    print("OK — serviço 'trino_lakehouse' cadastrado no OpenMetadata.")


def _ler_admin_email():
    if ENV_PATH.exists():
        m = re.search(r"^OM_ADMIN_EMAIL=(.+)$", ENV_PATH.read_text(encoding="utf-8"), re.MULTILINE)
        if m and m.group(1).strip():
            return m.group(1).strip()
    return "admin@open-metadata.org"


def _atualizar_env(jwt):
    if not ENV_PATH.exists():
        print(f"AVISO: {ENV_PATH} não encontrado — imprimindo o valor para você colar manualmente.")
        print(f"\nOM_INGESTION_BOT_JWT={jwt}\n")
        return

    conteudo = ENV_PATH.read_text(encoding="utf-8")
    linha_nova = f"OM_INGESTION_BOT_JWT={jwt}"
    if re.search(r"^OM_INGESTION_BOT_JWT=.*$", conteudo, re.MULTILINE):
        conteudo = re.sub(r"^OM_INGESTION_BOT_JWT=.*$", linha_nova, conteudo, flags=re.MULTILINE)
    else:
        conteudo = conteudo.rstrip("\n") + f"\n{linha_nova}\n"
    ENV_PATH.write_text(conteudo, encoding="utf-8")


def main():
    email = _ler_admin_email()
    senha_b64 = base64.b64encode(b"admin").decode()

    print(f"Autenticando em {OM_URL} como {email} ...")
    try:
        login = _post("/users/login", {"email": email, "password": senha_b64})
    except urllib.error.URLError as e:
        print(f"\nERRO: não foi possível conectar ao OpenMetadata em {OM_URL}.")
        print("Suba o perfil governance e aguarde o boot (~3 min):")
        print("  docker compose --profile governance up -d")
        print(f"Detalhe: {e}")
        sys.exit(1)

    token_admin = login["accessToken"]

    bot = _get("/bots/name/ingestion-bot", token_admin)
    bot_user_id = bot["botUser"]["id"]

    token_info = _get(f"/users/token/{bot_user_id}", token_admin)
    jwt = token_info["JWTToken"]

    _atualizar_env(jwt)
    print("OK — OM_INGESTION_BOT_JWT atualizado em .env")

    _garantir_servico_trino(token_admin)

    _recriar_airflow()


def _recriar_airflow():
    """Recria os containers do Airflow para que releiam o .env novo.

    Esse é o passo que, se esquecido, mantém o token velho dentro do container e
    faz a DAG falhar com 'The given token does not match the current bot's token'
    (`restart` NÃO relê o .env — só recriar o container). Por isso o script já o
    executa; se o docker não estiver acessível, imprime o comando manual.
    """
    cmd = [
        "docker", "compose", "up", "-d", "--force-recreate",
        "airflow-webserver", "airflow-scheduler",
    ]
    manual = "  " + " ".join(cmd)

    if shutil.which("docker") is None:
        print("\nAVISO: 'docker' não encontrado no PATH. Recrie os containers do Airflow manualmente:")
        print(manual)
        return

    repo_root = ENV_PATH.parent
    print("\nRecriando os containers do Airflow para aplicar o novo .env...")
    try:
        subprocess.run(cmd, cwd=repo_root, check=True)
        print("OK — Airflow recriado com o token atual.")
    except (subprocess.CalledProcessError, OSError) as e:
        print(f"\nAVISO: não consegui recriar os containers automaticamente ({e}). Rode manualmente:")
        print(manual)


if __name__ == "__main__":
    main()
