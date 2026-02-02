@echo off
setlocal ENABLEDELAYEDEXPANSION
REM === Weather App Portable Launcher (Windows) ===
REM - Crea/usa un venv locale
REM - Installa dipendenze
REM - Esegue ingest (con backfill 7d) UNA VOLTA all'avvio
REM - Crea un task schedulato ogni ora per l'aggiornamento automatico
REM - Avvia Streamlit su localhost:8501 e apre il browser

cd /d "%~dp0"
set "VENV=%CD%\.venv"
set "PYEXE=%VENV%\Scripts\python.exe"
set "WORKDIR=%CD%"
set "TASK_NAME=WeatherAppIngestHourly"

echo.
echo [1/5] Verifica ambiente Python...
if not exist "%PYEXE%" (
    echo    -> Creo l'ambiente virtuale...
    py -m venv "%VENV%"
    if errorlevel 1 (
        echo ERRORE: impossibile creare il virtualenv. Controlla l'installazione di Python.
        pause
        exit /b 1
    )
)

echo.
echo [2/5] Aggiorno pip e installo dipendenze...
call "%PYEXE%" -m pip install --upgrade pip
if exist "requirements.txt" (
    call "%PYEXE%" -m pip install -r requirements.txt
) else (
    call "%PYEXE%" -m pip install streamlit pandas numpy requests python-dotenv SQLAlchemy plotly pydeck
)

echo.
echo [3/5] Ingest iniziale (Ecowitt + OpenWeather, include backfill 7 giorni)...
call "%PYEXE%" "%WORKDIR%\weather_ingest.py"

echo.
echo [4/5] Scheduler aggiornamento automatico ogni ora...
REM Costruisco il comando con virgolette corrette
set "TASK_CMD=""%PYEXE%"" ""%WORKDIR%\weather_ingest.py"""
REM Se esiste già, lo ricreo per sicurezza
schtasks /Query /TN "%TASK_NAME%" >NUL 2>&1
if %ERRORLEVEL%==0 (
    schtasks /Delete /TN "%TASK_NAME%" /F >NUL 2>&1
)
schtasks /Create ^
  /TN "%TASK_NAME%" ^
  /TR %TASK_CMD% ^
  /SC HOURLY /MO 1 ^
  /RL LIMITED /F
if errorlevel 1 (
  echo [ERRORE] Non sono riuscito a creare il task schedulato. Prova a rieseguire questo .bat come Amministratore.
) else (
  echo [OK] Task "%TASK_NAME%" creato: aggiornera' i dati ogni ora.
)

echo.
echo [5/5] Avvio dashboard web...
call "%PYEXE%" -m streamlit run "%WORKDIR%\app_streamlit.py" --server.port 8501 --server.address localhost --server.headless false

endlocal
