Write-Host "==============================================="
Write-Host "   ERP SYSTEM - ENTORNO DE DESARROLLO"
Write-Host "   Backend (FastAPI) + HTMX"
Write-Host "==============================================="
Write-Host ""

# PATH del proyecto
$root = $PSScriptRoot
$backendPath = Join-Path $root "backend"
$venvPython = Join-Path $backendPath "venv\Scripts\python.exe"

if (!(Test-Path $venvPython)) {
  Write-Host "No se encontro el venv en backend\venv."
  Write-Host "Crea una venv con Python 3.12 o 3.11 y ejecuta:"
  Write-Host "  python -m venv backend\venv"
  Write-Host "  backend\venv\Scripts\pip.exe install -r backend\requirements.txt"
  Write-Host ""
}

# ---------------------------
#   BACKEND
# ---------------------------
Write-Host "Iniciando BACKEND (FastAPI)..."

$env:DATABASE_URL = "postgresql://user:1234@localhost:5432/hollpacas"
$env:SECRET_KEY = "CHANGE_ME"
$pythonExe = if (Test-Path -LiteralPath $venvPython) { $venvPython } else { "python" }

try {
  Set-Location -Path $backendPath
  & $pythonExe -m uvicorn app.main:app --reload
} finally {
  Set-Location -Path $root
}

Write-Host ""
Write-Host "==============================================="
Write-Host " Todo esta corriendo :)"
Write-Host " Backend: http://127.0.0.1:8000"
Write-Host " UI HTMX: http://127.0.0.1:8000"
Write-Host "==============================================="
