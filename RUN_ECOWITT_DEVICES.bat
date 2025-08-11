@echo off
setlocal ENABLEDELAYEDEXPANSION
cd /d "%~dp0"
set "PYEXE=%CD%\.venv\Scripts\python.exe"
if not exist "%PYEXE%" (
  echo Manca il virtualenv. Avvia prima RUN_WEATHER_APP.bat
  pause
  exit /b 1
)
"%PYEXE%" ecowitt_devices_probe.py
echo.
echo Controlla il file: ecowitt_devices.json
pause
endlocal
