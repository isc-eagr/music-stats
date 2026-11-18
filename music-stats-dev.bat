@echo off
setlocal

REM Music Stats DEV runner: compiles the working tree and starts it on http://localhost:8081
REM against the dev DB (C:\Music Stats DB\music-stats-dev.db), never the live one.
REM Only a Java process listening on port 8081 is stopped; the live app on 8080 is left alone.

cd /d "%~dp0"

set "DEV_DB=C:\Music Stats DB\music-stats-dev.db"

echo Stopping any Java process listening on port 8081...
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$pids = @(Get-NetTCPConnection -State Listen -LocalPort 8081 -ErrorAction SilentlyContinue | Select-Object -ExpandProperty OwningProcess -Unique);" ^
  "foreach ($p in $pids) { $proc = Get-Process -Id $p -ErrorAction SilentlyContinue; if (-not $proc) { continue }; if ($proc.ProcessName -ne 'java' -and $proc.ProcessName -ne 'javaw') { Write-Error ('Port 8081 is owned by non-Java PID {0} ({1}). It was not stopped.' -f $p, $proc.ProcessName); exit 1 }; Write-Host ('Stopping PID {0}' -f $p); Stop-Process -Id $p -Force -ErrorAction Stop }"
if errorlevel 1 exit /b 1

if not exist "%DEV_DB%" (
  echo Dev DB not found, building it first...
  call "%~dp0tools\devdb\build-dev-db.bat"
  if errorlevel 1 exit /b 1
)

call "%CD%\mvnw.cmd" spring-boot:run "-Dspring-boot.run.profiles=dev" "-Dspring-boot.run.jvmArguments=--enable-native-access=ALL-UNNAMED"
exit /b %errorlevel%
