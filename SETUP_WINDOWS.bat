@echo off
setlocal
cd /d "%~dp0"

echo [1/3] Creo l'ambiente Python isolato...
if not exist ".venv\Scripts\python.exe" (
  py -3.11 -m venv .venv
  if errorlevel 1 py -m venv .venv
)
if not exist ".venv\Scripts\python.exe" (
  echo ERRORE: installa Python 3.11 o 3.12 da python.org e riprova.
  pause
  exit /b 1
)

echo [2/3] Installo le dipendenze...
".venv\Scripts\python.exe" -m pip install --upgrade pip
if errorlevel 1 goto :error
".venv\Scripts\python.exe" -m pip install -r requirements.txt
if errorlevel 1 goto :error

echo [3/3] Preparo la configurazione privata...
if not exist ".env" copy ".env.example" ".env" >NUL
echo.
echo Configurazione pronta. Apri .env con Blocco note e inserisci i tuoi valori.
echo Il file .env e' escluso da Git.
pause
exit /b 0

:error
echo ERRORE durante l'installazione. Controlla la connessione e riprova.
pause
exit /b 1
