@echo off
setlocal ENABLEDELAYEDEXPANSION
REM === Crea una schedulazione Windows per l'aggiornamento automatico (ogni 6 ore) ===

cd /d "%~dp0"
set "VENV=%CD%\.venv"
set "PYEXE=%VENV%\Scripts\python.exe"
if not exist "%PYEXE%" (
    echo Manca il virtualenv. Avvia prima RUN_WEATHER_APP.bat almeno una volta.
    pause
    exit /b 1
)

REM Costruisci comando con percorsi quotati
set "CMD=""%PYEXE%"" ""%CD%\weather_ingest.py"""
schtasks /Create /TN "WeatherAppIngest" /TR %CMD% /SC HOURLY /MO 6 /F
if errorlevel 1 (
    echo ERRORE: non sono riuscito a creare l'attivita' pianificata.
    echo Prova ad aprire il prompt come Amministratore.
    pause
    exit /b 1
) else (
    echo OK: Attivita' 'WeatherAppIngest' creata (ogni 6 ore).
)

echo.
echo Per rimuoverla in futuro:
echo   schtasks /Delete /TN "WeatherAppIngest" /F
pause
endlocal
