"""
Helpers de retomada para o orquestrador do protocolo experimental.
Permite parar e reiniciar scripts/orquestrar_protocolo.sh a qualquer momento
(queda de internet, desligamento do computador, Ctrl-C) sem perder trabalho
ja concluido nem duplicar linhas nos CSVs de resultado.

Uso:
    python3 _resume_helpers.py status <tag>   # imprime: done | partial | missing
    python3 _resume_helpers.py limpar <tag>   # remove todas as linhas de <tag> dos CSVs
"""
import csv
import sys
from pathlib import Path

RESULT_DIR = Path(__file__).resolve().parent.parent / "experimentos" / "sscad2026" / "resultados"
DURACOES_CSV = RESULT_DIR / "duracoes.csv"
CONTAGENS_CSV = RESULT_DIR / "contagens.csv"

DAGS_ESPERADAS = {"dag_ingestao_bronze", "dag_silver_transform", "dag_gold_refresh"}

CSVS_POR_TAG = [
    ("duracoes.csv", "tag"),
    ("task_durations.csv", "tag"),
    ("contagens.csv", "tag"),
    ("tamanhos_arquivos.csv", "tag"),
    ("snapshots.csv", "tag"),
]


def status(tag: str) -> str:
    if not DURACOES_CSV.exists():
        return "missing"
    dags_ok = set()
    with open(DURACOES_CSV, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row["tag"] != tag:
                continue
            if row["segundos"] == "FALHOU":
                continue
            dags_ok.add(row["dag"])

    if not dags_ok:
        return "missing"
    if dags_ok != DAGS_ESPERADAS:
        return "partial"

    # As 3 DAGs terminaram — mas so conta como "done" se as metricas Trino
    # (coleta_metricas_trino.sh, que roda logo em seguida) tambem gravaram.
    # Se o processo morreu entre as duas etapas, as tabelas Iceberg daquela
    # tag ja terao sido apagadas no proximo reset — entao "metrica perdida"
    # so pode ser corrigido refazendo a repeticao inteira, nao só a coleta.
    if not CONTAGENS_CSV.exists():
        return "partial"
    with open(CONTAGENS_CSV, newline="", encoding="utf-8") as f:
        tem_metricas = any(row["tag"] == tag for row in csv.DictReader(f))
    return "done" if tem_metricas else "partial"


def limpar(tag: str) -> None:
    for nome, coluna_tag in CSVS_POR_TAG:
        p = RESULT_DIR / nome
        if not p.exists():
            continue
        with open(p, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            linhas = list(reader)
        if not linhas:
            continue
        header, corpo = linhas[0], linhas[1:]
        idx = header.index(coluna_tag)
        corpo_filtrado = [row for row in corpo if row[idx] != tag]
        with open(p, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(corpo_filtrado)


if __name__ == "__main__":
    acao, tag = sys.argv[1], sys.argv[2]
    if acao == "status":
        print(status(tag))
    elif acao == "limpar":
        limpar(tag)
    else:
        raise SystemExit(f"acao desconhecida: {acao}")
