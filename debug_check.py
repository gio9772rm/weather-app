import os
import sys
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import pandas as pd
import requests

print("=== Meteo App • Diagnostica rapida ===")

# Load .env
load_dotenv()
SQLITE_PATH = os.getenv("SQLITE_PATH", "./data/weather.db")
OWM_API_KEY = os.getenv("OWM_API_KEY", "")
LAT = os.getenv("LAT")
LON = os.getenv("LON")
EC_APP = os.getenv("ECOWITT_APPLICATION_KEY", "")
EC_KEY = os.getenv("ECOWITT_API_KEY", "")
EC_MAC = os.getenv("ECOWITT_MAC", "")

print(f".env trovato: {'OK' if OWM_API_KEY or EC_APP or EC_KEY else 'MANCANTE/INCOMPLETO'}")
print(f"  - SQLITE_PATH: {SQLITE_PATH}")
print(f"  - OWM_API_KEY: {'OK' if len(OWM_API_KEY)>0 else 'MANCANTE'}")
print(f"  - LAT/LON: {LAT},{LON}")
print(f"  - ECOWITT APP/API/MAC: {'OK' if EC_APP and EC_KEY and EC_MAC else 'MANCANTI'}")

# Check OpenWeather connectivity
try:
    if OWM_API_KEY and LAT and LON:
        r = requests.get("https://api.openweathermap.org/data/2.5/forecast",
                         params={"lat": LAT, "lon": LON, "appid": OWM_API_KEY, "units":"metric"},
                         timeout=15)
        print("OpenWeather:", r.status_code, r.reason)
        if r.status_code != 200:
            print("  Body:", r.text[:200])
    else:
        print("OpenWeather: chiavi o coordinate mancanti, salto test.")
except Exception as e:
    print("OpenWeather: ERRORE", e)

# Check Ecowitt connectivity (real_time)
try:
    if EC_APP and EC_KEY and EC_MAC:
        r = requests.get("https://api.ecowitt.net/api/v3/device/real_time",
                         params={
                             "application_key": EC_APP,
                             "api_key": EC_KEY,
                             "mac": EC_MAC,
                             "call_back": "outdoor,indoor,solar_and_uvi,wind,pressure,rainfall"
                         },
                         timeout=15)
        print("Ecowitt:", r.status_code, r.reason)
        if r.status_code != 200:
            print("  Body:", r.text[:200])
    else:
        print("Ecowitt: credenziali mancanti, salto test.")
except Exception as e:
    print("Ecowitt: ERRORE", e)

# DB checks
try:
    eng = create_engine(f"sqlite:///{SQLITE_PATH}", future=True)
    with eng.connect() as conn:
        for table in ["station_3h","forecast_ow","meta"]:
            try:
                df = pd.read_sql_query(text(f"SELECT COUNT(*) as n FROM {table}"), conn)
                print(f"Tabella {table}: {int(df['n'].iloc[0])} righe")
            except Exception as e:
                print(f"Tabella {table}: non trovata ({e})")
except Exception as e:
    print("ERRORE accesso DB:", e)

print("=== Fine diagnostica ===")
