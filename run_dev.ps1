Write-Host "==============================================="
Write-Host "   ERP SYSTEM - ENTORNO DE DESARROLLO"
Write-Host "   Backend (FastAPI) + HTMX"
Write-Host "==============================================="
Write-Host ""

# PATH del proyecto
$root = $PSScriptRoot
$backendPath = Join-Path $root "backend"
$venvCandidates = @(
  (Join-Path $backendPath "venv\Scripts\python.exe"), # Windows venv
  (Join-Path $backendPath ".venv\Scripts\python.exe"), # Windows .venv
  (Join-Path $backendPath "venv/bin/python"),          # macOS/Linux venv
  (Join-Path $backendPath ".venv/bin/python")          # macOS/Linux .venv
)
$venvPython = $venvCandidates | Where-Object { Test-Path -LiteralPath $_ } | Select-Object -First 1

if ([string]::IsNullOrWhiteSpace($venvPython)) {
  Write-Host "No se encontro un entorno virtual en backend/venv ni backend/.venv."
  Write-Host "Crea una venv y luego instala dependencias:"
  if ($IsWindows) {
    Write-Host "  python -m venv backend\venv"
    Write-Host "  backend\venv\Scripts\pip.exe install -r backend\requirements.txt"
  } else {
    Write-Host "  python3 -m venv backend/.venv"
    Write-Host "  backend/.venv/bin/pip install -r backend/requirements.txt"
  }
  Write-Host ""
}

# ---------------------------
#   BACKEND
# ---------------------------
Write-Host "Iniciando BACKEND (FastAPI)..."

$env:SECRET_KEY = "CHANGE_ME"
$pythonExe = if ($venvPython) { $venvPython } else { if ($IsWindows) { "python" } else { "python3" } }

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
