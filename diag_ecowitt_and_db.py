import os
import requests
import pandas as pd
from sqlalchemy import create_engine

DB_URL = os.getenv("DATABASE_URL") or f"sqlite:///{os.getenv('SQLITE_PATH', './data/weather.db')}"

ECOWITT_API_KEY = os.getenv("ECOWITT_API_KEY")
ECOWITT_APP_KEY = os.getenv("ECOWITT_APP_KEY")
ECOWITT_MAC = os.getenv("ECOWITT_MAC")

print("Connessione DB:", DB_URL)
engine = create_engine(DB_URL)

# --- Check DB ---
def check_table(tbl):
    try:
        df = pd.read_sql(f"SELECT * FROM {tbl} LIMIT 500", engine)
    except Exception as e:
        print(f"Errore tabella {tbl}:", e)
        return
    print(f"=== {tbl} ===")
    print("Colonne:", list(df.columns))
    if not df.empty:
        desc = df.describe(include="all")
        print(desc)
    else:
        print("(vuota)")

for tbl in ["station_raw", "station_3h"]:
    check_table(tbl)

# --- Check API ---
if ECOWITT_API_KEY and ECOWITT_APP_KEY and ECOWITT_MAC:
    url = "https://api.ecowitt.net/api/v3/device/real_time"
    params = {
        "application_key": ECOWITT_APP_KEY,
        "api_key": ECOWITT_API_KEY,
        "mac": ECOWITT_MAC,
        "call_back": "all",
    }
    print("\n=== Chiamata Ecowitt real_time ===")
    r = requests.get(url, params=params, timeout=15)
    print("Status:", r.status_code)
    try:
        data = r.json()
        print("Keys:", list(data.keys()))
        if "data" in data:
            rt = data["data"]["outdoor"] if "outdoor" in data["data"] else data["data"]
            press = rt.get("pressure")
            wind = rt.get("wind")
            gust = rt.get("gust")
            print("pressure:", press)
            print("wind:", wind)
            print("gust:", gust)
    except Exception as e:
        print("Errore parsing:", e)
else:
    print("Chiavi Ecowitt non configurate (.env).")
