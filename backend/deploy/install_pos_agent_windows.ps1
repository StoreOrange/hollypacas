$ErrorActionPreference = 'Stop'
$InstallDir = Join-Path $env:LOCALAPPDATA 'HollpacasPOS'
$Config = Join-Path $PSScriptRoot 'hollpacas-pos.json'
if (-not (Test-Path $Config)) { throw 'Extrae TODO el ZIP antes de instalar.' }
$Python = Get-Command python -ErrorAction SilentlyContinue
if (-not $Python) { throw 'Instala Python 3.9 o superior, marca Add Python to PATH y vuelve a ejecutar.' }
& $Python.Source -c "import sys; assert sys.version_info >= (3,9)"
if ($LASTEXITCODE -ne 0) { throw 'Se requiere Python 3.9 o superior.' }
$Printer = Get-CimInstance Win32_Printer | Where-Object Default | Select-Object -First 1
if (-not $Printer) { throw 'Configura primero la Epson como impresora predeterminada de Windows.' }
$Sumatra = @((Join-Path $env:LOCALAPPDATA 'SumatraPDF\SumatraPDF.exe'), (Join-Path $env:ProgramFiles 'SumatraPDF\SumatraPDF.exe')) | Where-Object { Test-Path $_ } | Select-Object -First 1
if (-not $Sumatra -and ${env:ProgramFiles(x86)}) { $Candidate = Join-Path ${env:ProgramFiles(x86)} 'SumatraPDF\SumatraPDF.exe'; if (Test-Path $Candidate) { $Sumatra = $Candidate } }
if (-not $Sumatra) { throw 'Instala SumatraPDF desde https://www.sumatrapdfreader.org/download-free-pdf-viewer y vuelve a ejecutar.' }
$Settings = Get-Content $Config -Raw | ConvertFrom-Json
$TaskName = 'Hollpacas-POS-' + $Settings.station_id
$Existing = Get-ScheduledTask -TaskName 'Hollpacas-POS-*' -ErrorAction SilentlyContinue
foreach ($OldTask in $Existing) {
    Stop-ScheduledTask -TaskName $OldTask.TaskName -ErrorAction SilentlyContinue
    if ($OldTask.TaskName -ne $TaskName) { Unregister-ScheduledTask -TaskName $OldTask.TaskName -Confirm:$false }
}
New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null
$ConfigPath = Join-Path $InstallDir 'hollpacas-pos.json'
Copy-Item (Join-Path $PSScriptRoot 'hollpacas_pos_agent.py') (Join-Path $InstallDir 'hollpacas_pos_agent.py') -Force
$Settings | Add-Member -NotePropertyName sumatra_path -NotePropertyValue $Sumatra -Force
$Settings | ConvertTo-Json | Set-Content $ConfigPath -Encoding UTF8
$CurrentUser = [Security.Principal.WindowsIdentity]::GetCurrent().Name
& icacls $InstallDir /inheritance:r /grant:r "${CurrentUser}:(OI)(CI)(F)" '*S-1-5-18:(OI)(CI)(F)' | Out-Null
$Pythonw = Join-Path (Split-Path $Python.Source) 'pythonw.exe'
if (-not (Test-Path $Pythonw)) { $Pythonw = $Python.Source }
$Arguments = '"' + (Join-Path $InstallDir 'hollpacas_pos_agent.py') + '" "' + $ConfigPath + '"'
$Action = New-ScheduledTaskAction -Execute $Pythonw -Argument $Arguments -WorkingDirectory $InstallDir
$Trigger = New-ScheduledTaskTrigger -AtLogOn -User $CurrentUser
$Principal = New-ScheduledTaskPrincipal -UserId $CurrentUser -LogonType Interactive -RunLevel Limited
$TaskSettings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -ExecutionTimeLimit ([TimeSpan]::Zero) -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1) -MultipleInstances IgnoreNew
Register-ScheduledTask -TaskName $TaskName -Action $Action -Trigger $Trigger -Principal $Principal -Settings $TaskSettings -Force | Out-Null
Start-ScheduledTask -TaskName $TaskName
Write-Host "Agente instalado para $CurrentUser. Impresora predeterminada: $($Printer.Name)" -ForegroundColor Green
Write-Host 'Vuelve a Ventas y espera a que diga Agente conectado. No se imprimió ninguna factura durante la instalación.'
