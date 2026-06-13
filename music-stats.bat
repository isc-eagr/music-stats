@echo off
setlocal enabledelayedexpansion

REM Music Stats JAR Runner
REM This script runs the music-stats application

cd /d "%~dp0"

set "PROD_JAR_PATH=C:\Code\music-stats\music-stats-0.0.1-SNAPSHOT.jar"

echo.
echo ======================================
echo   Music Stats - Starting...
echo ======================================
echo.

echo Stopping running Music Stats Java processes...
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$procs = Get-CimInstance Win32_Process | Where-Object { ($_.Name -eq 'java.exe' -or $_.Name -eq 'javaw.exe') -and $_.CommandLine -like '*music-stats-0.0.1-SNAPSHOT.jar*' };" ^
  "if (-not $procs) { Write-Host 'No running Music Stats Java processes found. Continuing startup.' }" ^
  "else { foreach ($proc in $procs) { Write-Host ('Stopping PID {0}: {1}' -f $proc.ProcessId, $proc.CommandLine); Stop-Process -Id $proc.ProcessId -Force } }"
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
