@echo off
setlocal

REM Rebuilds the dev database (C:\Music Stats DB\music-stats-dev.db) from a read-only slice of the live DB.
REM Stop the dev app (port 8081) first: the dev file is deleted and recreated. The live DB is never written.
REM Optional args: [live db path] [dev db path]

cd /d "%~dp0..\.."

call "%CD%\mvnw.cmd" -q dependency:build-classpath "-DincludeArtifactIds=sqlite-jdbc,slf4j-api" "-Dmdep.outputFile=target\devdb-classpath.txt"
if errorlevel 1 (
  echo Could not resolve the sqlite-jdbc classpath.
  exit /b 1
)
set /p DEVDB_CP=<target\devdb-classpath.txt

java --enable-native-access=ALL-UNNAMED -cp "%DEVDB_CP%" tools\devdb\DevDatabaseBuilder.java %*
exit /b %errorlevel%
