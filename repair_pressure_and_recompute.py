import os
import pandas as pd
from sqlalchemy import create_engine

DB_URL = os.getenv("DATABASE_URL") or f"sqlite:///{os.getenv('SQLITE_PATH', './data/weather.db')}"
engine = create_engine(DB_URL)

def fix_pressure(v):
    try:
        v = float(v)
    except:
        return None
    if 800 <= v <= 1100:
        return v
    if 8000 <= v <= 11000:
        return v/10.0
    if 80000 <= v <= 110000:
        return v/100.0
    if 50 <= v <= 200:
        return v*10.0
    return None

print("Connessione DB:", DB_URL)

df = pd.read_sql("SELECT * FROM station_raw", engine)
if df.empty:
    print("station_raw vuota")
else:
    df["press_hpa_fixed"] = df["press_hpa"].apply(fix_pressure)
    print("Range originale:", df["press_hpa"].min(), df["press_hpa"].max())
    print("Range fixato:", df["press_hpa_fixed"].min(), df["press_hpa_fixed"].max())
    df["press_hpa"] = df["press_hpa_fixed"]
    df.drop(columns=["press_hpa_fixed"], inplace=True)
    df.to_sql("station_raw", engine, if_exists="replace", index=False)
    print("station_raw aggiornato.")

    # ricostruzione station_3h semplice
    df["Time"] = pd.to_datetime(df["ts_utc"], errors="coerce")
    df = df.dropna(subset=["Time"])
    df["TimeHour"] = df["Time"].dt.floor("3h")
    agg = df.groupby("TimeHour").agg({
        "temp_c":"mean",
        "hum":"mean",
        "press_hpa":"mean",
        "wind_ms":"mean",
        "winddir":"mean",
        "rain_mm":"sum"
    }).reset_index()
    agg.rename(columns={
        "TimeHour":"Time",
        "temp_c":"Temp_C",
        "hum":"Humidity",
        "press_hpa":"Pressure_hPa",
        "wind_ms":"Wind_ms",
        "winddir":"WindDir",
        "rain_mm":"Rain_mm"
    }, inplace=True)
    agg.to_sql("station_3h", engine, if_exists="replace", index=False)
    print("station_3h ricostruita.")
