$ErrorActionPreference = "Stop"
$TaskName = "HollywoodPacas-TA040"
$InstallDir = Join-Path $env:ProgramData "HollywoodPacas\AttendanceAgent"

$identity = [Security.Principal.WindowsIdentity]::GetCurrent()
$principal = New-Object Security.Principal.WindowsPrincipal($identity)
if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    throw "Ejecute PowerShell como Administrador."
}

if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
    Stop-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
}
if (Test-Path $InstallDir) {
    Remove-Item $InstallDir -Recurse -Force
}
Write-Host "Agente del reloj desinstalado." -ForegroundColor Green
