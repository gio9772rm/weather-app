@echo off
setlocal ENABLEDELAYEDEXPANSION
rem === Install hourly ingest task (Ecowitt Cloud + OpenWeather) ===
cd /d "%~dp0"

set "VENV_PY=.\.venv\Scripts\python.exe"
if not exist "%VENV_PY%" (
  echo [INFO] Virtualenv non trovato: creo .venv e installo dipendenze minime...
  py -m venv ".venv"
  ".\.venv\Scripts\python" -m pip install --upgrade pip
  ".\.venv\Scripts\python" -m pip install requests pandas SQLAlchemy python-dotenv
)

rem Build full command with quotes
set "WORKDIR=%CD%"
set "CMD=""%VENV_PY%"" ""%WORKDIR%\weather_ingest_verbose.py"""
rem Remove any existing task with same name
schtasks /Query /TN "WeatherAppIngestHourly" >NUL 2>&1 && schtasks /Delete /TN "WeatherAppIngestHourly" /F >NUL 2>&1

schtasks /Create /TN "WeatherAppIngestHourly" ^
  /TR %CMD% ^
  /SC HOURLY /MO 1 ^
  /RL LIMITED /F ^
  /ST 00:00

if errorlevel 1 (
  echo [ERRORE] Creazione task fallita. Prova ad eseguire questo .bat come Amministratore.
  pause
  exit /b 1
) else (
  echo [OK] Task "WeatherAppIngestHourly" creato (ogni ora).
)
endlocal
