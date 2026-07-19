# ============================================================
# Cria e configura o ambiente virtual Python do projeto
# Uso: .\setup_venv.ps1
# Equivalente Windows/PowerShell do setup_venv.sh (Git Bash/Linux/macOS)
# ============================================================

$ErrorActionPreference = "Stop"

$VenvDir = "venv"
$PythonMin = [version]"3.10"

Write-Host "=== Setup do ambiente virtual ==="

$PythonCmd = Get-Command python -ErrorAction SilentlyContinue
if (-not $PythonCmd) {
    Write-Error "ERRO: 'python' nao encontrado no PATH."
    exit 1
}

$VersionString = (& python --version 2>&1) -replace "Python ", ""
$Version = [version]$VersionString

if ($Version -lt $PythonMin) {
    Write-Error "ERRO: Python >= $PythonMin e necessario (encontrado $VersionString)."
    exit 1
}
Write-Host "Python encontrado: $VersionString"

if (-not (Test-Path $VenvDir)) {
    Write-Host "Criando venv em .\$VenvDir..."
    python -m venv $VenvDir
} else {
    Write-Host "venv ja existe em .\$VenvDir - reutilizando."
}

& ".\$VenvDir\Scripts\Activate.ps1"

Write-Host "Atualizando pip..."
python -m pip install --upgrade pip --quiet

Write-Host "Instalando dependencias de requirements.txt..."
pip install -r requirements.txt
if ($LASTEXITCODE -ne 0) {
    Write-Error "ERRO: falha ao instalar requirements.txt (pip saiu com codigo $LASTEXITCODE)."
    exit 1
}

Write-Host ""
Write-Host "=== Setup concluido! ==="
Write-Host "Para ativar o venv:  $VenvDir\Scripts\Activate.ps1"
Write-Host "Para subir a stack:  docker compose up -d"
Write-Host "Para verificar:      docker compose ps"
