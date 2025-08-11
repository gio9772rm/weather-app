@echo off
setlocal
set "TASK_NAME=WeatherAppIngestHourly"

echo [1/1] Rimozione task schedulato "%TASK_NAME%"...
schtasks /Query /TN "%TASK_NAME%" >NUL 2>&1
if %ERRORLEVEL%==0 (
    schtasks /Delete /TN "%TASK_NAME%" /F
    echo [OK] Task "%TASK_NAME%" rimosso con successo.
) else (
    echo [INFO] Nessun task con nome "%TASK_NAME%" trovato.
)
endlocal
pause
