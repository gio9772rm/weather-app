@echo off
setlocal ENABLEDELAYEDEXPANSION
cd /d "%~dp0"
set "PYEXE=%CD%\.venv\Scripts\python.exe"
if not exist "%PYEXE%" (
  echo Creo il virtualenv e installo requests...
  py -m venv ".venv"
  ".\.venv\Scripts\python" -m pip install --upgrade pip
  ".\.venv\Scripts\python" -m pip install requests
)
"%PYEXE%" ecowitt_lan_probe.py
echo.
echo Creato ecowitt_lan_probe_output.json (nella stessa cartella).
pause
endlocal