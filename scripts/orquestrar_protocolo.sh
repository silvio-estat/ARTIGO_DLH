#!/usr/bin/env bash
# Orquestrador do Protocolo Experimental SSCAD 2026 (E1 + E2).
# Roda TODAS as repeticoes com reset (docker compose down -v / up -d) entre
# elas, coleta metricas de duracao (Airflow) e de contagem/tamanho/snapshot
# (Trino), e ao final restaura a stack completa (com governance).
#
# Nao aborta o protocolo inteiro se uma repeticao falhar: registra a falha
# no log e segue para a proxima (2 repeticoes validas ainda sao defensaveis
# para uma PoC; a ideia e nunca perder a noite inteira por causa de 1 rep).
#
# IDEMPOTENTE / RETOMAVEL: antes de cada repeticao, consulta resultados/*.csv
# (via _resume_helpers.py) para saber se aquela tag ja foi concluida (3 dags
# com sucesso) — se sim, pula. Se ficou parcial (interrompido no meio, ex.
# queda de energia/internet), limpa as linhas daquela tag e refaz do zero.
# Ou seja: pode matar este script a qualquer momento e rodar de novo
# (`./scripts/orquestrar_protocolo.sh`) que ele continua sozinho de onde parou.
#
# Uso: nohup ./scripts/orquestrar_protocolo.sh > /dev/null 2>&1 &
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"
RESULT_DIR="$REPO_ROOT/experimentos/sscad2026/resultados"
mkdir -p "$RESULT_DIR"
LOG="$RESULT_DIR/log_execucao.txt"
DAG_SILVER="airflow/dags/dag_silver_transform.py"
MARCADOR_CONCLUIDO="$RESULT_DIR/protocolo_concluido.marker"
PY="$REPO_ROOT/venv/bin/python3"

# Repeticoes por ponto (E1: por volume; E2: por config de cores). Configuravel
# por env var: REPS=15 ./scripts/orquestrar_protocolo.sh
# Referencia de tempo: ~8,4 min/rep (inclui reset da stack). 6 pontos no total,
# entao o custo aproximado e REPS * 6 * 8,4 min (10 reps ~8,5h; 15 reps ~12,5h).
# O protocolo e retomavel: se nao terminar ate de manha, rode de novo que ele
# continua de onde parou, sem perder nenhuma repeticao ja concluida.
REPS="${REPS:-12}"

if [[ -f "$MARCADOR_CONCLUIDO" ]]; then
  echo "Protocolo ja foi concluido em $(cat "$MARCADOR_CONCLUIDO")."
  echo "Para rodar de novo do zero, apague $MARCADOR_CONCLUIDO e resultados/*.csv antes."
  exit 0
fi

CORE_SERVICES="postgres minio minio-init hive-metastore spark-master spark-worker trino airflow-init airflow-webserver airflow-scheduler"

log () { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*" | tee -a "$LOG"; }

reset_stack () {
  log "--- reset: docker compose down -v"
  docker compose down -v >> "$LOG" 2>&1
  log "--- reset: docker compose up -d (core, sem governance)"
  # shellcheck disable=SC2086
  docker compose up -d $CORE_SERVICES >> "$LOG" 2>&1

  local st="000"
  for _ in $(seq 1 60); do  # ate 5 min
    st=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/health 2>/dev/null || echo "000")
    [[ "$st" == "200" ]] && break
    sleep 5
  done
  if [[ "$st" != "200" ]]; then
    log "ERRO: airflow-webserver nao ficou healthy a tempo"
    return 1
  fi
  sleep 10
  log "--- stack pronta"
  return 0
}

rodar_rep () {
  local lotes=$1 registros=$2 tag=$3 seed=$4
  local st_resumo
  st_resumo=$("$PY" "$REPO_ROOT/scripts/_resume_helpers.py" status "$tag")
  if [[ "$st_resumo" == "done" ]]; then
    log "$tag: ja concluido em tentativa anterior — pulando (retomada)"
    return 0
  fi
  if [[ "$st_resumo" == "partial" ]]; then
    log "$tag: dados parciais de tentativa anterior (interrompida) — limpando e refazendo"
    "$PY" "$REPO_ROOT/scripts/_resume_helpers.py" limpar "$tag"
  fi
  if ! reset_stack; then
    log "$tag: FALHA no reset — pulando repeticao"
    return 1
  fi
  log ">>> Rodando $tag (lotes=$lotes registros=$registros seed=$seed)"
  if ./scripts/experimento.sh "$lotes" "$registros" "$tag" "$seed" >> "$LOG" 2>&1; then
    ./scripts/coleta_metricas_trino.sh "$tag" >> "$LOG" 2>&1
    log "<<< $tag: OK"
  else
    log "<<< $tag: FALHOU (ver detalhes acima no log)"
  fi
}

set_cores_silver () {
  local n=$1
  sed -i "s/\"spark.cores.max\": \"[0-9]*\",/\"spark.cores.max\": \"$n\",/" "$DAG_SILVER"
  sed -i "s/\"spark.executor.cores\": \"[0-9]*\",/\"spark.executor.cores\": \"$n\",/" "$DAG_SILVER"
  log "dag_silver_transform.py ajustada: spark.cores.max=$n spark.executor.cores=$n"
}

log "===== INICIO DO PROTOCOLO EXPERIMENTAL SSCAD 2026 ====="

{
  echo "data_inicio_utc: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo ""
  echo "--- CPU ---"
  lscpu 2>/dev/null | grep -E "Model name|CPU\(s\):|Thread|Core\(s\)|Socket"
  echo ""
  echo "--- Memoria ---"
  free -h
  echo ""
  echo "--- Disco (repo) ---"
  df -h "$REPO_ROOT"
  echo ""
  echo "--- Docker ---"
  docker --version
  docker compose version
  echo ""
  echo "--- SO ---"
  uname -a
  echo ""
  echo "--- Imagens da stack ---"
  docker compose images 2>/dev/null
} > "$RESULT_DIR/ambiente.txt"
log "Specs de ambiente gravadas em resultados/ambiente.txt"

log "===== E1 — Escalabilidade com volume ($REPS reps/ponto) ====="
for par in "V1 2 500" "V2 20 500" "V3 200 500"; do
  read -r vol lotes registros <<< "$par"
  for rep in $(seq 1 "$REPS"); do
    rodar_rep "$lotes" "$registros" "${vol}_rep${rep}" "$rep"
  done
done
log "===== E1 concluido ====="

log "===== E2 — Speedup com paralelismo (volume fixo V3=200x500x8tipos, $REPS reps/ponto) ====="
for par in "C1 1" "C2 2" "C4 4"; do
  read -r nome cores <<< "$par"
  set_cores_silver "$cores"
  for rep in $(seq 1 "$REPS"); do
    rodar_rep 200 500 "${nome}_rep${rep}" "$rep"
  done
done
log "Restaurando dag_silver_transform.py para baseline (1 core)"
set_cores_silver 1
log "===== E2 concluido ====="

log "===== Restaurando stack completa (com governance) ====="
docker compose down -v >> "$LOG" 2>&1
docker compose up -d >> "$LOG" 2>&1
st="000"
for _ in $(seq 1 60); do
  st=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8585/api/v1/system/version 2>/dev/null || echo "000")
  [[ "$st" == "200" ]] && break
  sleep 10
done
sleep 15
if [[ "$st" == "200" ]]; then
  "$REPO_ROOT/venv/bin/python3" scripts/setup_om_bot_token.py >> "$LOG" 2>&1 \
    && docker compose up -d airflow-webserver airflow-scheduler >> "$LOG" 2>&1
else
  log "aviso: OpenMetadata nao ficou healthy a tempo — rode scripts/setup_om_bot_token.py manualmente depois"
fi
# Deixa as 3 DAGs pausadas ao final, igual ao estado original do repo
for d in dag_ingestao_bronze dag_silver_transform dag_gold_refresh; do
  docker exec dlh_airflow_webserver airflow dags pause "$d" >> "$LOG" 2>&1
done

log "===== PROTOCOLO CONCLUIDO ====="
echo "data_fim_utc: $(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$RESULT_DIR/ambiente.txt"
date -u +%Y-%m-%dT%H:%M:%SZ > "$MARCADOR_CONCLUIDO"
