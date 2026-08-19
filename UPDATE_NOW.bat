@echo off
setlocal
cd /d "%~dp0"
if not exist ".venv\Scripts\python.exe" (
  echo Esegui prima SETUP_WINDOWS.bat
  pause
  exit /b 1
)
".venv\Scripts\python.exe" ingest_all.py --force-forecast
set "RESULT=%ERRORLEVEL%"
echo.
if "%RESULT%"=="0" (
  echo Aggiornamento completato.
) else (
  echo Aggiornamento non completato. Controlla .env e i messaggi sopra.
)
pause
exit /b %RESULT%
