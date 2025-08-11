@echo off
setlocal ENABLEDELAYEDEXPANSION
rem === Install dashboard autostart on user logon ===
cd /d "%~dp0"

set "VENV_PY=.\.venv\Scripts\python.exe"
if not exist "%VENV_PY%" (
  echo [INFO] Creo .venv per lo start automatico...
  py -m venv ".venv"
  ".\.venv\Scripts\python" -m pip install --upgrade pip
  ".\.venv\Scripts\python" -m pip install streamlit
)

set "WORKDIR=%CD%"
set "CMD=""%VENV_PY%"" -m streamlit run ""%WORKDIR%\app_streamlit.py"" --server.port 8501"

schtasks /Query /TN "WeatherDashboardOnLogin" >NUL 2>&1 && schtasks /Delete /TN "WeatherDashboardOnLogin" /F >NUL 2>&1

schtasks /Create /TN "WeatherDashboardOnLogin" ^
  /TR %CMD% ^
  /SC ONLOGON ^
  /RL LIMITED /F

if errorlevel 1 (
  echo [ERRORE] Creazione task fallita. Prova ad eseguire questo .bat come Amministratore.
  pause
  exit /b 1
) else (
  echo [OK] Task "WeatherDashboardOnLogin" creato (parte all'accesso utente).
)
endlocal
