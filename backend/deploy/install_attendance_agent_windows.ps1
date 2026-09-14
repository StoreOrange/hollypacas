param(
    [string]$ApiUrl = "https://www.hollywoodpacas.ovh",
    [string]$DeviceIp = "192.168.1.132",
    [int]$DevicePort = 4370,
    [string]$DeviceCode = "ta040-central",
    [int]$CommKey = 0,
    [SecureString]$SyncToken
)

$ErrorActionPreference = "Stop"
$TaskName = "HollywoodPacas-TA040"
$InstallDir = Join-Path $env:ProgramData "HollywoodPacas\AttendanceAgent"
$SourceScript = Join-Path (Split-Path -Parent $PSScriptRoot) "scripts\sync_attendance_ta040.py"
$Python = Get-Command python -ErrorAction SilentlyContinue

$identity = [Security.Principal.WindowsIdentity]::GetCurrent()
$principal = New-Object Security.Principal.WindowsPrincipal($identity)
if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    throw "Ejecute PowerShell como Administrador para instalar el servicio del reloj."
}
if (-not $Python) {
    throw "Python 3 no esta instalado o no esta agregado al PATH. Instale Python 3 y marque 'Add Python to PATH'."
}
if (-not (Test-Path $SourceScript)) {
    throw "No se encontro el agente en $SourceScript. Ejecute este instalador desde backend\deploy del proyecto."
}
if (-not $SyncToken) {
    $SyncToken = Read-Host "Token privado ATTENDANCE_SYNC_TOKEN del servidor" -AsSecureString
}
$PlainToken = (New-Object System.Net.NetworkCredential("", $SyncToken)).Password
if ([string]::IsNullOrWhiteSpace($PlainToken)) {
    throw "El token de sincronizacion es obligatorio."
}

Write-Host "[1/6] Verificando comunicacion con el reloj..." -ForegroundColor Cyan
$ClockTest = Test-NetConnection -ComputerName $DeviceIp -Port $DevicePort -InformationLevel Quiet
if (-not $ClockTest) {
    Write-Warning "Esta PC aun no alcanza $DeviceIp`:$DevicePort. Se instalara el agente, pero el reloj seguira offline hasta corregir la red."
} else {
    Write-Host "Reloj accesible en $DeviceIp`:$DevicePort." -ForegroundColor Green
}

Write-Host "[2/6] Verificando acceso al sistema web..." -ForegroundColor Cyan
try {
    Invoke-WebRequest -Uri ($ApiUrl.TrimEnd('/') + "/") -UseBasicParsing -TimeoutSec 15 | Out-Null
    Write-Host "Sistema web accesible." -ForegroundColor Green
} catch {
    throw "No fue posible acceder a $ApiUrl desde la PC de caja: $($_.Exception.Message)"
}

Write-Host "[3/6] Preparando agente local..." -ForegroundColor Cyan
New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null
Copy-Item $SourceScript (Join-Path $InstallDir "sync_attendance_ta040.py") -Force
$VenvDir = Join-Path $InstallDir ".venv"
if (-not (Test-Path (Join-Path $VenvDir "Scripts\python.exe"))) {
    & $Python.Source -m venv $VenvDir
}
$AgentPython = Join-Path $VenvDir "Scripts\python.exe"
$AgentPythonw = Join-Path $VenvDir "Scripts\pythonw.exe"
& $AgentPython -m pip install --disable-pip-version-check --quiet --upgrade pip
& $AgentPython -m pip install --disable-pip-version-check --quiet pyzk python-dotenv

Write-Host "[4/6] Guardando configuracion segura local..." -ForegroundColor Cyan
$EnvContent = @(
    "ATTENDANCE_API_URL=$($ApiUrl.TrimEnd('/'))",
    "ATTENDANCE_DEVICE_IP=$DeviceIp",
    "ATTENDANCE_DEVICE_PORT=$DevicePort",
    "ATTENDANCE_DEVICE_CODE=$DeviceCode",
    "ATTENDANCE_DEVICE_COMM_KEY=$CommKey",
    "ATTENDANCE_SYNC_TOKEN=$PlainToken"
) -join [Environment]::NewLine
$EnvPath = Join-Path $InstallDir ".env"
[IO.File]::WriteAllText($EnvPath, $EnvContent, (New-Object Text.UTF8Encoding($false)))
& icacls $EnvPath /inheritance:r /grant:r '*S-1-5-18:(F)' '*S-1-5-32-544:(F)' | Out-Null

Write-Host "[5/6] Registrando inicio automatico..." -ForegroundColor Cyan
$AgentScript = Join-Path $InstallDir "sync_attendance_ta040.py"
$Action = New-ScheduledTaskAction -Execute $AgentPythonw -Argument ('"{0}" --watch --interval 3 --users-interval 300 --reconnect-interval 180 --watchdog-timeout 45' -f $AgentScript) -WorkingDirectory $InstallDir
$Trigger = New-ScheduledTaskTrigger -AtStartup
$Settings = New-ScheduledTaskSettingsSet -RestartCount 999 -RestartInterval (New-TimeSpan -Minutes 1) -StartWhenAvailable -ExecutionTimeLimit (New-TimeSpan -Days 3650)
$TaskPrincipal = New-ScheduledTaskPrincipal -UserId "SYSTEM" -LogonType ServiceAccount -RunLevel Highest
Register-ScheduledTask -TaskName $TaskName -Action $Action -Trigger $Trigger -Settings $Settings -Principal $TaskPrincipal -Description "Sincroniza el reloj 3nStar TA040 con Hollywood Pacas" -Force | Out-Null
Start-ScheduledTask -TaskName $TaskName

Write-Host "[6/6] Comprobando servicio..." -ForegroundColor Cyan
Start-Sleep -Seconds 4
$Task = Get-ScheduledTask -TaskName $TaskName
$Info = Get-ScheduledTaskInfo -TaskName $TaskName
Write-Host "Agente instalado correctamente." -ForegroundColor Green
Write-Host "Tarea: $TaskName | Estado: $($Task.State) | Ultimo resultado: $($Info.LastTaskResult)"
Write-Host "Ahora abra Control de marcadas y pulse 'Actualizar reloj'."
