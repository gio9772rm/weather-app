@echo off
setlocal
cd /d "%~dp0"
if not exist ".venv\Scripts\python.exe" (
  echo Eseguo prima la configurazione iniziale...
  call SETUP_WINDOWS.bat
  if errorlevel 1 exit /b 1
)
".venv\Scripts\python.exe" -m streamlit run app_streamlit.py --server.address localhost --server.port 8501
endlocal
