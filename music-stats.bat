@echo off
setlocal enabledelayedexpansion

REM Music Stats JAR Runner
REM This script runs the music-stats application

cd /d "%~dp0"

echo.
echo ======================================
echo   Music Stats - Starting...
echo ======================================
echo.

java -jar target/music-stats-0.0.1-SNAPSHOT.jar --debug=false --logging.level.root=INFO --logging.level.org.springframework=INFO --logging.level.org.hibernate=INFO --spring.jpa.show-sql=false

echo.
echo ======================================
echo   Music Stats - Stopped
echo ======================================
echo.
