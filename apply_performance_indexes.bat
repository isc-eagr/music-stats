@echo off
REM Quick script to apply performance indexes to Music Stats database
REM Run this from the music-stats project directory

echo ========================================
echo Music Stats - Performance Index Installer
echo ========================================
echo.

set DB_PATH=C:\Music Stats DB\music-stats.db
set SQL_SCRIPT=db_additional_performance_indexes.sql

echo Database: %DB_PATH%
echo Script: %SQL_SCRIPT%
echo.

if not exist "%DB_PATH%" (
    echo ERROR: Database not found at %DB_PATH%
    echo Please update DB_PATH in this script if your database is elsewhere.
    pause
    exit /b 1
)

if not exist "%SQL_SCRIPT%" (
    echo ERROR: SQL script not found: %SQL_SCRIPT%
    echo Make sure you're running this from the music-stats project root.
    pause
    exit /b 1
)

echo Creating performance indexes...
echo.

sqlite3 "%DB_PATH%" < "%SQL_SCRIPT%"

if %ERRORLEVEL% EQU 0 (
    echo.
    echo ========================================
    echo SUCCESS! Indexes created successfully.
    echo ========================================
    echo.
    echo The following optimizations are now active:
    echo   - Account filter performance boost
    echo   - Chart query optimization
    echo   - Artist detail page speedup
    echo   - Override resolution improvements
    echo.
    echo Restart your application to see the performance gains.
) else (
    echo.
    echo ========================================
    echo ERROR: Failed to create indexes
    echo ========================================
    echo Check that sqlite3 is installed and accessible.
)

echo.
pause
