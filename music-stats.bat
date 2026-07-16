@echo off
setlocal enabledelayedexpansion

REM Music Stats JAR Runner
REM This script runs the music-stats application

REM Stop permissions must match the process started by an elevated deployment.
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$principal = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent());" ^
  "if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) { Write-Host 'Requesting administrator privileges to start Music Stats...'; try { Start-Process -FilePath $env:ComSpec -ArgumentList '/c', '%~f0' -Verb RunAs -ErrorAction Stop; exit 42 } catch { Write-Error ('Administrator privileges were not granted: {0}' -f $_.Exception.Message); exit 1 } }"
if errorlevel 42 exit /b 0
if errorlevel 1 (
  echo Music Stats startup requires administrator privileges.
  exit /b 1
)

cd /d "%~dp0"

set "PROD_JAR_PATH=C:\Code\music-stats\music-stats-0.0.1-SNAPSHOT.jar"

echo.
echo ======================================
echo   Music Stats - Starting...
echo ======================================
echo.

echo Stopping running Music Stats Java processes...
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$jarProcs = @(); try { $javaProcs = Get-CimInstance Win32_Process -ErrorAction Stop | Where-Object { $_.Name -eq 'java.exe' -or $_.Name -eq 'javaw.exe' }; $jarProcs = @($javaProcs | Where-Object { $_.CommandLine -like '*music-stats-0.0.1-SNAPSHOT.jar*' }) } catch { Write-Warning ('Could not inspect Java command lines: {0}. Port 8080 checks will still run.' -f $_.Exception.Message) };" ^
  "try { $listenerPids = @(Get-NetTCPConnection -State Listen -LocalPort 8080 -ErrorAction Stop | Select-Object -ExpandProperty OwningProcess -Unique) } catch { Write-Warning ('Get-NetTCPConnection failed: {0}. Falling back to netstat.' -f $_.Exception.Message); $listenerPids = @(netstat -ano -p tcp | ForEach-Object { if ($_ -match '^\s*TCP\s+\S+:8080\s+\S+\s+LISTENING\s+(\d+)\s*$') { [int]$matches[1] } } | Sort-Object -Unique); if ($LASTEXITCODE -ne 0) { Write-Error 'Could not inspect port 8080 listeners with netstat.'; exit 1 } };" ^
  "$listenerProcs = @(); foreach ($listenerPid in $listenerPids) { try { $listenerProcs += Get-Process -Id $listenerPid -ErrorAction Stop } catch { Write-Error ('Could not inspect port 8080 owner PID {0}: {1}' -f $listenerPid, $_.Exception.Message); exit 1 } };" ^
  "$nonJavaListeners = $listenerProcs | Where-Object { $_.ProcessName -ne 'java' -and $_.ProcessName -ne 'javaw' };" ^
  "if ($nonJavaListeners) { $nonJavaListeners | ForEach-Object { Write-Error ('Port 8080 is owned by non-Java PID {0} ({1}). It was not stopped.' -f $_.Id, $_.ProcessName) }; exit 1 };" ^
  "$processIds = @($jarProcs | Select-Object -ExpandProperty ProcessId); $processIds += @($listenerProcs | Select-Object -ExpandProperty Id); $processIds = @($processIds | Sort-Object -Unique);" ^
  "if (-not $processIds) { Write-Host 'No running Music Stats Java processes or Java listeners on port 8080 found. Continuing startup.' }" ^
  "else { foreach ($processId in $processIds) { $jarProc = $jarProcs | Where-Object { $_.ProcessId -eq $processId } | Select-Object -First 1; if ($jarProc) { Write-Host ('Stopping PID {0}: {1}' -f $processId, $jarProc.CommandLine) } else { $listenerProc = $listenerProcs | Where-Object { $_.Id -eq $processId } | Select-Object -First 1; Write-Host ('Stopping Java process listening on port 8080, PID {0} ({1}).' -f $processId, $listenerProc.ProcessName) }; Stop-Process -Id $processId -Force } }"
if errorlevel 1 (
  echo Failed to stop running Music Stats processes.
  exit /b 1
)

echo.
if not exist "%PROD_JAR_PATH%" (
  echo Expected jar was not found: "%PROD_JAR_PATH%"
  exit /b 1
)

java -jar "%PROD_JAR_PATH%" --debug=false --logging.level.root=INFO --logging.level.org.springframework=INFO --logging.level.org.hibernate=INFO --spring.jpa.show-sql=false
exit /b %errorlevel%
