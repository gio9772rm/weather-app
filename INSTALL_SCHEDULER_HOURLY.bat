@echo off
setlocal ENABLEDELAYEDEXPANSION
REM === Crea/aggiorna schedulazione Windows per aggiornamento ogni ora ===

cd /d "%~dp0"
set "VENV=%CD%\.venv"
set "PYEXE=%VENV%\Scripts\python.exe"
if not exist "%PYEXE%" (
    echo Manca il virtualenv. Avvia prima RUN_WEATHER_APP.bat almeno una volta.
    pause
    exit /b 1
)

REM Rimuovi task vecchio se esiste
schtasks /Query /TN "WeatherAppIngest" >NUL 2>&1
if not errorlevel 1 (
  schtasks /Delete /TN "WeatherAppIngest" /F
)

set "CMD=""%PYEXE%"" ""%CD%\weather_ingest.py"""
schtasks /Create /TN "WeatherAppIngest" /TR %CMD% /SC HOURLY /MO 1 /F
if errorlevel 1 (
    echo ERRORE: non sono riuscito a creare l'attivita' pianificata (ogni ora).
    echo Prova ad aprire il prompt come Amministratore.
    pause
    exit /b 1
) else (
    echo OK: Attivita' 'WeatherAppIngest' creata (ogni ora).
)

echo.
echo Per rimuoverla in futuro:
echo   schtasks /Delete /TN "WeatherAppIngest" /F
pause
endlocal
