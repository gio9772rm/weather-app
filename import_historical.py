import pandas as pd
import sqlite3
from sqlalchemy import create_engine, text
import os

# === Config ===
XLSX_PATH = "storico_stazione.xlsx"  # Nome file XLSX (mettilo nella root repo)
DB_PATH = os.getenv("DATABASE_URL", "./data/weather.db")  # SQLite o Postgres

# Se DATABASE_URL contiene postgres:// lo usiamo come engine, altrimenti sqlite
if DB_PATH.startswith("postgres://") or DB_PATH.startswith("postgresql://"):
    engine = create_engine(DB_PATH)
else:
    engine = create_engine(f"sqlite:///{DB_PATH}")

print(f"Leggo {XLSX_PATH} ...")
df = pd.read_excel(XLSX_PATH)

# Adatta nomi colonne in base ai tuoi file Ecowitt
# Assumiamo che ci siano colonne tipo "Time", "Temp_C", "Humidity", "Pressure_hPa", ...
# Se i tuoi nomi sono diversi, rinominali qui:
df = df.rename(columns={
    "TempOut": "Temp_C",
    "HumidityOut": "Humidity",
    "Pressure": "Pressure_hPa",
    "Wind": "Wind_kmh",
    "WindGust": "WindGust_kmh",
    "Rain": "Rain_mm"
})

# Conversione tempo
df["Time"] = pd.to_datetime(df["Time"], utc=True, errors="coerce")

# Rimuovi righe senza timestamp
df = df.dropna(subset=["Time"])

print(f"{len(df)} righe pronte per inserimento.")

# Inserimento in blocchi
with engine.begin() as conn:
    for _, row in df.iterrows():
        conn.execute(text("""
            INSERT INTO station_3h (Time, Temp_C, Humidity, Pressure_hPa, Wind_kmh, WindGust_kmh, Rain_mm)
            VALUES (:Time, :Temp_C, :Humidity, :Pressure_hPa, :Wind_kmh, :WindGust_kmh, :Rain_mm)
            ON CONFLICT(Time) DO UPDATE SET
                Temp_C=excluded.Temp_C,
                Humidity=excluded.Humidity,
                Pressure_hPa=excluded.Pressure_hPa,
                Wind_kmh=excluded.Wind_kmh,
                WindGust_kmh=excluded.WindGust_kmh,
                Rain_mm=excluded.Rain_mm
        """), {
            "Time": row["Time"].isoformat(),
            "Temp_C": row.get("Temp_C"),
            "Humidity": row.get("Humidity"),
            "Pressure_hPa": row.get("Pressure_hPa"),
            "Wind_kmh": row.get("Wind_kmh"),
            "WindGust_kmh": row.get("WindGust_kmh"),
            "Rain_mm": row.get("Rain_mm")
        })

print("✅ Import completato.")
