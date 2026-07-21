#!/usr/bin/env bash
# Roda uma configuracao completa do protocolo experimental SSCAD 2026:
# gera dados -> dispara bronze -> silver -> gold, com run_id rastreavel
# (perf_${TAG}_${dag}), cronometra cada estagio e coleta duracao por task
# via API REST do Airflow. Grava tudo em experimentos/sscad2026/resultados/.
#
# Uso: ./scripts/experimento.sh <lotes> <registros> <tag> [seed]
set -euo pipefail

LOTES=$1; REGISTROS=$2; TAG=$3; SEED=${4:-1}

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESULT_DIR="$REPO_ROOT/experimentos/sscad2026/resultados"
mkdir -p "$RESULT_DIR"
DURACOES_CSV="$RESULT_DIR/duracoes.csv"
TASKS_CSV="$RESULT_DIR/task_durations.csv"
VOLUME_TOTAL=$((LOTES * REGISTROS * 8))  # 8 tipos gerados por padrao (--tipo todos)
[[ -f "$DURACOES_CSV" ]] || echo "tag,dag,segundos,run_id,timestamp_utc,lotes,registros,volume_total" > "$DURACOES_CSV"
[[ -f "$TASKS_CSV" ]] || echo "tag,dag,run_id,task_id,duration_s,start_date,end_date" > "$TASKS_CSV"

AF="docker exec dlh_airflow_webserver airflow"
AF_API="http://localhost:8080/api/v1"
AF_USER="admin"
AF_PASS="admin"

estado_run () {
  local dag=$1 rid=$2
  $AF dags list-runs -d "$dag" -o json 2>/dev/null \
    | python3 -c "
import json, sys
try:
    d = json.load(sys.stdin)
except Exception:
    print('')
    sys.exit(0)
runs = [r for r in d if r['run_id'] == '$rid']
print(runs[0]['state'] if runs else '')
"
}

espera_dag () {  # espera a run terminar (success|failed); timeout de seguranca em 40min
  local dag=$1 rid=$2
  local max_iter=480  # 480 * 5s = 40 min
  local i=0
  while true; do
    st=$(estado_run "$dag" "$rid")
    [[ "$st" == "success" ]] && return 0
    [[ "$st" == "failed"  ]] && return 1
    i=$((i + 1))
    if [[ $i -ge $max_iter ]]; then
      echo "TIMEOUT esperando $dag/$rid" >&2
      return 1
    fi
    sleep 5
  done
}

coleta_task_durations () {
  local dag=$1 rid=$2
  curl -s -u "$AF_USER:$AF_PASS" \
    "$AF_API/dags/$dag/dagRuns/$rid/taskInstances" \
    | python3 -c "
import sys, json
tag, dag, rid = '$TAG', '$dag', '$rid'
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(0)
for ti in d.get('task_instances', []):
    dur = ti.get('duration')
    dur = '' if dur is None else dur
    print(f\"{tag},{dag},{rid},{ti['task_id']},{dur},{ti.get('start_date') or ''},{ti.get('end_date') or ''}\")
" >> "$TASKS_CSV"
}

roda_dag () {
  local dag=$1
  local rid="perf_${TAG}_${dag}_$(date +%s)"
  local t0 t1 resultado=0
  # Neste ambiente (Airflow 2.9 + LocalExecutor), uma DAG pausada bloqueia o
  # AGENDAMENTO de qualquer task dela — inclusive tasks restantes de uma run
  # ja em andamento, nao so novas runs. Entao so podemos pausar DEPOIS que a
  # run terminar por completo (nunca no meio dela). Enquanto isso, a unica
  # protecao contra o cron da propria DAG (*/5, */15, */20 min) competir por
  # Spark durante a medicao e o max_active_runs=1 de cada DAG: uma segunda
  # DagRun so fica enfileirada, sem rodar tasks em paralelo com a nossa.
  $AF dags unpause "$dag" >/dev/null
  # Cronometragem com resolucao de milissegundos (date +%s.%N): o timer de
  # segundos inteiros anterior engolia a variancia entre repeticoes dos estagios
  # quase-planos (Silver/Gold), produzindo std=0 artificial. A subtracao de
  # floats e feita em awk (bash so faz aritmetica inteira).
  t0=$(date +%s.%N)
  $AF dags trigger "$dag" -r "$rid" >/dev/null
  espera_dag "$dag" "$rid" || resultado=1
  $AF dags pause "$dag" >/dev/null
  t1=$(date +%s.%N)
  local dur
  # LC_ALL=C forca ponto decimal: em locale pt_BR o awk usaria virgula, que
  # quebraria o CSV (virgula e o separador de campos).
  dur=$(LC_ALL=C awk "BEGIN{printf \"%.3f\", $t1 - $t0}")
  if [[ $resultado -ne 0 ]]; then
    echo "FALHOU: $dag/$rid" >&2
    echo "${TAG},${dag},FALHOU,${rid},$(date -u +%Y-%m-%dT%H:%M:%SZ),${LOTES},${REGISTROS},${VOLUME_TOTAL}" >> "$DURACOES_CSV"
    return 1
  fi
  echo "${TAG},${dag},${dur},${rid},$(date -u +%Y-%m-%dT%H:%M:%SZ),${LOTES},${REGISTROS},${VOLUME_TOTAL}" >> "$DURACOES_CSV"
  coleta_task_durations "$dag" "$rid"
  echo "  $dag OK (${dur}s)"
}

echo "== $TAG: gerando ${LOTES}x${REGISTROS} (seed=$SEED) =="
"$REPO_ROOT/venv/bin/python3" "$REPO_ROOT/scripts/gerar_dados.py" --lotes "$LOTES" --registros "$REGISTROS" --seed "$SEED"

roda_dag dag_ingestao_bronze
roda_dag dag_silver_transform
roda_dag dag_gold_refresh
echo "== $TAG concluido =="
