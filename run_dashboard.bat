@echo off
setlocal ENABLEDELAYEDEXPANSION
cd /d %~dp0
if not exist .venv (
  python -m venv .venv
)
call .venv\Scripts\activate
pip install -r requirements.txt
python weather_ingest.py
streamlit run app_streamlit.py --server.headless true --browser.gatherUsageStats false
