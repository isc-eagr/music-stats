@echo off
setlocal enabledelayedexpansion

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
  "$procs = Get-CimInstance Win32_Process | Where-Object { ($_.Name -eq 'java.exe' -or $_.Name -eq 'javaw.exe') -and $_.CommandLine -like '*music-stats-0.0.1-SNAPSHOT.jar*' };" ^
  "if (-not $procs) { Write-Host 'No running Music Stats Java processes found.'; exit 0 };" ^
  "foreach ($proc in $procs) { Write-Host ('Stopping PID {0}: {1}' -f $proc.ProcessId, $proc.CommandLine); Stop-Process -Id $proc.ProcessId -Force }"
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
