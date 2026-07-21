#!/usr/bin/env bash
# Resumo rapido do estado do protocolo experimental — util para reorientar
# rapidamente depois de uma interrupcao (queda de internet, desligamento).
set -uo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESULT_DIR="$REPO_ROOT/experimentos/sscad2026/resultados"
PY="$REPO_ROOT/venv/bin/python3"

echo "=== Processo orquestrador ==="
if pgrep -f "orquestrar_protocolo.sh" >/dev/null 2>&1; then
  echo "RODANDO (pid $(pgrep -f orquestrar_protocolo.sh | head -1))"
else
  echo "PARADO"
fi

echo ""
echo "=== Containers ==="
docker compose -f "$REPO_ROOT/docker-compose.yml" --project-directory "$REPO_ROOT" ps --format "table {{.Name}}\t{{.Status}}" 2>/dev/null || echo "(docker compose ps falhou — stack pode estar down)"

echo ""
echo "=== Marcador de conclusao ==="
if [[ -f "$RESULT_DIR/protocolo_concluido.marker" ]]; then
  echo "CONCLUIDO em $(cat "$RESULT_DIR/protocolo_concluido.marker")"
else
  echo "Ainda nao concluido"
fi

echo ""
echo "=== Progresso por tag (E1: V1-V3 rep1-3 | E2: C1/C2/C4 rep1-3) ==="
if [[ -f "$RESULT_DIR/duracoes.csv" && -x "$PY" ]]; then
  for prefixo in V1 V2 V3 C1 C2 C4; do
    for rep in 1 2 3; do
      tag="${prefixo}_rep${rep}"
      st=$("$PY" "$REPO_ROOT/scripts/_resume_helpers.py" status "$tag" 2>/dev/null || echo "?")
      printf "  %-10s %s\n" "$tag" "$st"
    done
  done
else
  echo "(sem resultados/duracoes.csv ainda)"
fi

echo ""
echo "=== Ultimas 15 linhas do log ==="
tail -15 "$RESULT_DIR/log_execucao.txt" 2>/dev/null || echo "(sem log ainda)"
