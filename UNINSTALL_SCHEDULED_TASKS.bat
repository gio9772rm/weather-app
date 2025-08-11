@echo off
setlocal
schtasks /Delete /TN "WeatherAppIngestHourly" /F >NUL 2>&1
schtasks /Delete /TN "WeatherDashboardOnLogin" /F >NUL 2>&1
echo [OK] Task rimossi (se esistevano).
endlocal
