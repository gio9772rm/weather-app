@echo off
setlocal ENABLEDELAYEDEXPANSION
cd /d "%~dp0"
set "PYEXE=%CD%\.venv\Scripts\python.exe"
if not exist "%PYEXE%" (
  echo Il virtualenv non esiste ancora. Avvia prima RUN_WEATHER_APP.bat
  pause
  exit /b 1
)
"%PYEXE%" debug_check.py
pause
endlocal
