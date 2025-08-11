# import_historical.py
import os
from pathlib import Path
import sys
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

SQLITE_PATH = os.getenv("SQLITE_PATH", "./data/weather.db")
DATABASE_URL = os.getenv("DATABASE_URL")  # usato su Actions/Render

def make_engine():
    url = DATABASE_URL if DATABASE_URL else f"sqlite:///{SQLITE_PATH}"
    return create_engine(url, future=True)

SCHEMA = """
CREATE TABLE IF NOT EXISTS station_3h (
  Time TEXT PRIMARY KEY,
  Temp_C REAL,
  Humidity REAL,
  Pressure_hPa REAL,
  Wind_kmh REAL,
  WindGust_kmh REAL,
  Rain_mm REAL
);
CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT);
"""

NEEDED = ["Time","Temp_C","Humidity","Pressure_hPa","Wind_kmh","WindGust_kmh","Rain_mm"]

RENAME = {
    "TempOut":"Temp_C","Temperature_C":"Temp_C","Temperature":"Temp_C",
    "HumidityOut":"Humidity","Humidity_%":"Humidity",
    "Pressure":"Pressure_hPa",
    "Wind":"Wind_kmh","Wind_km_h":"Wind_kmh","Wind_kph":"Wind_kmh",
    "WindGust":"WindGust_kmh",
    "Rain":"Rain_mm","Rain_mm_3h":"Rain_mm"
}

def load_one(path: Path) -> pd.DataFrame:
    print(f"[INFO] Leggo {path.name}")
    if path.suffix.lower() == ".csv":
        df = pd.read_csv(path)
    else:
        try:
            import openpyxl  # noqa: F401
        except ImportError:
            print("[ERRORE] openpyxl non installato per XLSX.")
            raise
        df = pd.read_excel(path)

    df = df.rename(columns=RENAME)
    for c in NEEDED:
        if c not in df.columns:
            df[c] = None

    df["Time"] = pd.to_datetime(df["Time"], utc=True, errors="coerce")
    df = df.dropna(subset=["Time"])
    df["Time"] = df["Time"].dt.tz_convert("UTC").dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    for c in ["Temp_C","Humidity","Pressure_hPa","Wind_kmh","WindGust_kmh","Rain_mm"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.drop_duplicates(subset=["Time"]).sort_values("Time")
    return df[NEEDED]

def upsert(df: pd.DataFrame, eng) -> int:
    if df.empty:
        return 0
    sql = text("""
        INSERT INTO station_3h (Time, Temp_C, Humidity, Pressure_hPa, Wind_kmh, WindGust_kmh, Rain_mm)
        VALUES (:Time, :Temp_C, :Humidity, :Pressure_hPa, :Wind_kmh, :WindGust_kmh, :Rain_mm)
        ON CONFLICT (Time) DO UPDATE SET
            Temp_C=excluded.Temp_C,
            Humidity=excluded.Humidity,
            Pressure_hPa=excluded.Pressure_hPa,
            Wind_kmh=excluded.Wind_kmh,
            WindGust_kmh=excluded.WindGust_kmh,
            Rain_mm=excluded.Rain_mm
    """)
    n = 0
    with eng.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(sql, row.to_dict())
            n += 1
        conn.execute(text("INSERT OR REPLACE INTO meta (k,v) VALUES ('last_ingest', :v)"),
                     {"v": pd.Timestamp.utcnow().isoformat()+"Z"})
    return n

def main():
    eng = make_engine()
    # Assicura schema
    with eng.begin() as conn:
        for stmt in SCHEMA.split(";"):
            s = stmt.strip()
            if s:
                conn.execute(text(s))

    # Sorgenti: preferisci cartella ./historical, altrimenti singolo file ./storico_stazione.xlsx
    files = []
    hist = Path("historical")
    if hist.exists():
        files += [p for p in hist.iterdir() if p.suffix.lower() in (".csv", ".xlsx", ".xls")]
    xlsx = Path("storico_stazione.xlsx")
    if xlsx.exists():
        files.append(xlsx)

    if not files:
        print("[ERRORE] Nessun file trovato. Metti CSV/XLSX in ./historical/ oppure storico_stazione.xlsx nella root.")
        sys.exit(1)

    total = 0
    for f in sorted(files):
        try:
            df = load_one(f)
            n = upsert(df, eng)
            print(f"[OK] {f.name}: upsert {n} righe")
            total += n
        except Exception as e:
            print(f"[ERRORE] {f.name}: {e}")
            sys.exit(1)

    print(f"[DONE] Totale righe upsertate: {total}")
    sys.exit(0)

if __name__ == "__main__":
    main()
