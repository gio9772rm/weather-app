# backfill_once.py
from weather_ingest import ecowitt_history_3h, upsert_table, EC_APP, EC_KEY, EC_MAC
from sqlalchemy import create_engine, text
import os

SQLITE_PATH = os.getenv("SQLITE_PATH", "./data/weather.db")
engine = create_engine(f"sqlite:///{SQLITE_PATH}", future=True)

print("-> Scarico storico Ecowitt ultimi 7 giorni (30min -> 3h)...")
df = ecowitt_history_3h(EC_APP, EC_KEY, EC_MAC, days=7)
if df is None or df.empty:
    print("Nessun dato di storico ricevuto.")
else:
    n = upsert_table(df, "station_3h")
    print(f"Upserted station_3h: {n}")

# Controllo conteggi
with engine.begin() as conn:
    cnt = conn.execute(text("SELECT count(*) FROM station_3h")).fetchone()[0]
print("Righe in station_3h =", cnt)
