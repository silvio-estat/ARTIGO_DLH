#!/usr/bin/env bash
# Coleta contagens por zona, tamanho/arquivos e snapshots das tabelas Iceberg
# via Trino (Secao 2.3 do protocolo) e grava em experimentos/sscad2026/resultados/.
# Uso: ./scripts/coleta_metricas_trino.sh <tag>
set -euo pipefail

TAG=$1
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESULT_DIR="$REPO_ROOT/experimentos/sscad2026/resultados"
mkdir -p "$RESULT_DIR"

CONTAGENS_CSV="$RESULT_DIR/contagens.csv"
TAMANHOS_CSV="$RESULT_DIR/tamanhos_arquivos.csv"
SNAPSHOTS_CSV="$RESULT_DIR/snapshots.csv"
[[ -f "$CONTAGENS_CSV" ]] || echo "tag,zona,tabela,registros" > "$CONTAGENS_CSV"
[[ -f "$TAMANHOS_CSV" ]] || echo "tag,tabela,n_arquivos,tamanho_mb" > "$TAMANHOS_CSV"
[[ -f "$SNAPSHOTS_CSV" ]] || echo "tag,tabela,n_snapshots" > "$SNAPSHOTS_CSV"

TIPOS="gps sensor relt_intel paf obstaculo seg_area pessoal material"

trino_q () {
  docker exec dlh_trino trino --server http://localhost:8090 --execute "$1" --output-format CSV 2>/dev/null
}

n_bronze=$(trino_q "SELECT COUNT(*) FROM iceberg.bronze.dados" | tr -d '"')
echo "${TAG},bronze,dados,${n_bronze}" >> "$CONTAGENS_CSV"

for tipo in $TIPOS; do
  n=$(trino_q "SELECT COUNT(*) FROM iceberg.silver.${tipo}" | tr -d '"')
  echo "${TAG},silver,${tipo},${n}" >> "$CONTAGENS_CSV"

  read -r n_arq tam_mb <<< "$(trino_q "SELECT COUNT(*), ROUND(SUM(file_size_in_bytes)/1e6, 3) FROM iceberg.silver.\"${tipo}\$files\"" | tr -d '"' | tr ',' ' ')"
  echo "${TAG},silver.${tipo},${n_arq:-0},${tam_mb:-0}" >> "$TAMANHOS_CSV"

  n_snap=$(trino_q "SELECT COUNT(*) FROM iceberg.silver.\"${tipo}\$snapshots\"" | tr -d '"')
  echo "${TAG},silver.${tipo},${n_snap:-0}" >> "$SNAPSHOTS_CSV"
done

echo "Metricas Trino coletadas para tag=${TAG}"
