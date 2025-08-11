#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
python3 -m venv .venv || true
source .venv/bin/activate
pip install -r requirements.txt
python weather_ingest.py
streamlit run app_streamlit.py --server.headless true --browser.gatherUsageStats false
