#!/bin/bash
# ============================================================
# Cria e configura o ambiente virtual Python do projeto
# Uso: bash setup_venv.sh   (Linux/macOS, ou Git Bash no Windows)
# ============================================================

set -e

VENV_DIR="venv"
PYTHON_MIN="3.10"

echo "=== Setup do ambiente virtual ==="

# Localiza um interpretador válido — python3 no Linux/macOS, python no Windows/Git Bash
PYTHON=""
for candidate in python3 python; do
    if command -v "$candidate" >/dev/null 2>&1 && "$candidate" -c "import sys; exit(0 if sys.version_info >= (3,10) else 1)" 2>/dev/null; then
        PYTHON="$candidate"
        break
    fi
done

if [ -z "$PYTHON" ]; then
    echo "ERRO: nenhum Python >= $PYTHON_MIN encontrado no PATH (tentei: python3, python)."
    exit 1
fi

PYTHON_VERSION=$($PYTHON --version 2>&1 | awk '{print $2}')
echo "Usando: $PYTHON ($PYTHON_VERSION)"

# Cria o venv (se não existir)
if [ ! -d "$VENV_DIR" ]; then
    echo "Criando venv em ./$VENV_DIR..."
    "$PYTHON" -m venv "$VENV_DIR"
else
    echo "venv já existe em ./$VENV_DIR — reutilizando."
fi

# Ativa o venv — layout difere entre Windows (Scripts/) e Linux/macOS (bin/)
if [ -f "$VENV_DIR/Scripts/activate" ]; then
    ACTIVATE="$VENV_DIR/Scripts/activate"
else
    ACTIVATE="$VENV_DIR/bin/activate"
fi
source "$ACTIVATE"

echo "Atualizando pip..."
python -m pip install --upgrade pip --quiet

echo "Instalando dependências de requirements.txt..."
pip install -r requirements.txt

echo ""
echo "=== Setup concluído! ==="
echo "Para ativar o venv:"
echo "  Git Bash / Linux / macOS:  source $ACTIVATE"
echo "  PowerShell:                venv\\Scripts\\Activate.ps1"
echo "Para subir a stack:  docker compose up -d"
echo "Para verificar:      docker compose ps"
