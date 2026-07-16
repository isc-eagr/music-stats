@echo off
setlocal enabledelayedexpansion

REM Stopping a process started by an elevated deployment requires an elevated token.
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$principal = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent());" ^
  "if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) { Write-Host 'Requesting administrator privileges for deployment...'; try { Start-Process -FilePath $env:ComSpec -ArgumentList '/c', '%~f0' -Verb RunAs -ErrorAction Stop; exit 42 } catch { Write-Error ('Administrator privileges were not granted: {0}' -f $_.Exception.Message); exit 1 } }"
if errorlevel 42 exit /b 0
if errorlevel 1 (
  echo Deployment requires administrator privileges.
  exit /b 1
)

set "APP_DIR=%~dp0"
set "JAR_PATH=%APP_DIR%target\music-stats-0.0.1-SNAPSHOT.jar"
set "DEPLOY_DIR=C:\Code\music-stats"
set "PROD_JAR_PATH=%DEPLOY_DIR%\music-stats-0.0.1-SNAPSHOT.jar"
set "RUNNER=%APP_DIR%music-stats.bat"

cd /d "%APP_DIR%"

echo.
echo ======================================
echo   Music Stats - Prod Deploy
echo ======================================
echo.

echo [1/4] Stopping running Music Stats Java processes...
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$jarProcs = @(); try { $javaProcs = Get-CimInstance Win32_Process -ErrorAction Stop | Where-Object { $_.Name -eq 'java.exe' -or $_.Name -eq 'javaw.exe' }; $jarProcs = @($javaProcs | Where-Object { $_.CommandLine -like '*music-stats-0.0.1-SNAPSHOT.jar*' }) } catch { Write-Warning ('Could not inspect Java command lines: {0}. Port 8080 checks will still run.' -f $_.Exception.Message) };" ^
  "try { $listenerPids = @(Get-NetTCPConnection -State Listen -LocalPort 8080 -ErrorAction Stop | Select-Object -ExpandProperty OwningProcess -Unique) } catch { Write-Warning ('Get-NetTCPConnection failed: {0}. Falling back to netstat.' -f $_.Exception.Message); $listenerPids = @(netstat -ano -p tcp | ForEach-Object { if ($_ -match '^\s*TCP\s+\S+:8080\s+\S+\s+LISTENING\s+(\d+)\s*$') { [int]$matches[1] } } | Sort-Object -Unique); if ($LASTEXITCODE -ne 0) { Write-Error 'Could not inspect port 8080 listeners with netstat.'; exit 1 } };" ^
  "$listenerProcs = @(); foreach ($listenerPid in $listenerPids) { try { $listenerProcs += Get-Process -Id $listenerPid -ErrorAction Stop } catch { Write-Error ('Could not inspect port 8080 owner PID {0}: {1}' -f $listenerPid, $_.Exception.Message); exit 1 } };" ^
  "$nonJavaListeners = $listenerProcs | Where-Object { $_.ProcessName -ne 'java' -and $_.ProcessName -ne 'javaw' };" ^
  "if ($nonJavaListeners) { $nonJavaListeners | ForEach-Object { Write-Error ('Port 8080 is owned by non-Java PID {0} ({1}). It was not stopped.' -f $_.Id, $_.ProcessName) }; exit 1 };" ^
  "$processIds = @($jarProcs | Select-Object -ExpandProperty ProcessId); $processIds += @($listenerProcs | Select-Object -ExpandProperty Id); $processIds = @($processIds | Sort-Object -Unique);" ^
  "if (-not $processIds) { Write-Host 'No running Music Stats Java processes or Java listeners on port 8080 found.'; exit 0 };" ^
  "foreach ($processId in $processIds) { $jarProc = $jarProcs | Where-Object { $_.ProcessId -eq $processId } | Select-Object -First 1; if ($jarProc) { Write-Host ('Stopping PID {0}: {1}' -f $processId, $jarProc.CommandLine) } else { $listenerProc = $listenerProcs | Where-Object { $_.Id -eq $processId } | Select-Object -First 1; Write-Host ('Stopping Java process listening on port 8080, PID {0} ({1}).' -f $processId, $listenerProc.ProcessName) }; Stop-Process -Id $processId -Force }"
if errorlevel 1 (
  echo Failed to stop running Music Stats processes.
  goto :fail
)

echo.
echo [2/4] Building fresh jar...
for /f %%I in ('powershell -NoProfile -Command "Get-Date -Format o"') do set "BUILD_STARTED=%%I"
call "%APP_DIR%mvnw.cmd" clean package
if errorlevel 1 (
  echo Maven build failed.
  goto :fail
)

echo.
echo [3/4] Verifying jar timestamp...
if not exist "%JAR_PATH%" (
  echo Expected jar was not found: "%JAR_PATH%"
  goto :fail
)

powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$jar = Get-Item -LiteralPath '%JAR_PATH%';" ^
  "$started = [datetimeoffset]::Parse('%BUILD_STARTED%');" ^
  "Write-Host ('Jar generated: {0}' -f $jar.LastWriteTime);" ^
  "if ($jar.LastWriteTime -lt $started.LocalDateTime) { Write-Error 'Jar timestamp is older than this deploy run.'; exit 1 }"
if errorlevel 1 (
  echo Jar timestamp verification failed.
  goto :fail
)

echo.
echo [4/4] Copying jar to deploy folder...
if not exist "%DEPLOY_DIR%" (
  mkdir "%DEPLOY_DIR%"
  if errorlevel 1 (
    echo Failed to create deploy folder: "%DEPLOY_DIR%"
    goto :fail
  )
)

copy /Y "%JAR_PATH%" "%PROD_JAR_PATH%" >nul
if errorlevel 1 (
  echo Failed to copy jar to "%PROD_JAR_PATH%".
  goto :fail
)

if not exist "%PROD_JAR_PATH%" (
  echo Copied jar was not found: "%PROD_JAR_PATH%"
  goto :fail
)

echo.
echo Build verified. Starting Music Stats in a separate window...
start "Music Stats" cmd /c ""%RUNNER%""
if errorlevel 1 (
  echo Failed to launch "%RUNNER%".
  goto :fail
)

echo.
echo Music Stats launch command was sent successfully.
echo Deploy script complete.
exit /b 0

:fail
echo.
echo ======================================
echo   Music Stats - Deploy Failed
echo ======================================
echo.
pause
exit /b 1
