@echo off
setlocal ENABLEDELAYEDEXPANSION
cd /d "%~dp0"
set "PYEXE=%CD%\.venv\Scripts\python.exe"
if not exist "%PYEXE%" (
  echo Manca il virtualenv. Avvia prima RUN_WEATHER_APP.bat una volta per creare .venv
  pause
  exit /b 1
)
"%PYEXE%" weather_ingest_verbose.py
pause
endlocal
