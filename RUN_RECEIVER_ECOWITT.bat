@echo off
setlocal ENABLEDELAYEDEXPANSION
cd /d "%~dp0"
set "VENV=%CD%\.venv"
set "PYEXE=%VENV%\Scripts\python.exe"
if not exist "%PYEXE%" (
  py -m venv ".venv"
  ".\.venv\Scripts\python" -m pip install --upgrade pip
  ".\.venv\Scripts\python" -m pip install flask pandas SQLAlchemy python-dotenv
) else (
  "%PYEXE%" -m pip install --upgrade pip >NUL 2>&1
  "%PYEXE%" -m pip install flask pandas SQLAlchemy python-dotenv >NUL 2>&1
)
echo Avvio ricevitore Ecowitt su http://0.0.0.0:8080/report ...
"%PYEXE%" receiver_ecowitt_customized.py
pause
endlocal