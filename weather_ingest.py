# -*- coding: utf-8 -*-
"""
weather_ingest.py — Ingest locale da file stazione + forecast OpenWeather → DB
- Compatibile SQLite/Postgres (ON CONFLICT DO UPDATE su meta)
- Lock cross‑platform con file esclusivo (gestione lock stantii)
- Aggregazione stazione su finestra 3 ore -> tabella station_3h
- Aggiornamento forecast_ow
"""

import os, sys, glob, time
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# --------------------------- Lock cross‑platform ---------------------------
def _lock_path() -> str:
    if os.name == "nt":
        return os.path.join(os.getenv("TEMP", "."), "ingest.lock")
    return "/tmp/ingest.lock"

class FileLock:
    def __init__(self, path: Optional[str] = None, stale_seconds: int = 900):
        self.path = path or _lock_path()
        self.stale_seconds = int(stale_seconds)
        self._fd = None

    def acquire(self) -> bool:
        # lock tramite create esclusiva del file: cross‑platform
        flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
        try:
            self._fd = os.open(self.path, flags, 0o644)
            os.write(self._fd, str(int(time.time())).encode("ascii", "ignore"))
            return True
        except FileExistsError:
            # se è stantio (> stale_seconds) lo rimuovo e ritento
            try:
                ts = int(Path(self.path).read_text().strip() or "0")
            except Exception:
                ts = 0
            if ts and (time.time() - ts) > self.stale_seconds:
                try:
                    os.remove(self.path)
                except Exception:
                    pass
                # retry una volta
                try:
                    self._fd = os.open(self.path, flags, 0o644)
                    os.write(self._fd, str(int(time.time())).encode("ascii", "ignore"))
                    return True
                except Exception:
                    return False
            return False

    def release(self):
        try:
            if self._fd is not None:
                os.close(self._fd)
        finally:
            try:
                if os.path.exists(self.path):
                    os.remove(self.path)
            except Exception:
                pass

# --------------------------- Config & DB helpers ---------------------------
load_dotenv()

def _get_float_env(name: str, default: float) -> float:
    try:
        return float(os.getenv(name) or default)
    except Exception:
        return float(default)

def _normalize_db_url(raw: str) -> str:
    u = (raw or "").strip()
    if not u:
        return u
    if u.startswith("postgres://"):
        u = "postgresql+psycopg2://" + u[len("postgres://"):]
    return u

DB_URL = _normalize_db_url(os.getenv("DATABASE_URL", ""))
SQLITE_PATH = os.getenv("SQLITE_PATH", "./data/weather.db")

LAT = _get_float_env("LAT", 41.89)
LON = _get_float_env("LON", 12.49)
OW_API_KEY = os.getenv("OW_API_KEY", "") or os.getenv("OWM_API_KEY", "") or os.getenv("OPENWEATHER_API_KEY", "")
STATION_CSV = os.getenv("STATION_CSV", "").strip()
STATION_TZ = os.getenv("STATION_TZ", "UTC")

def engine():
    if DB_URL:
        return create_engine(DB_URL, future=True)
    p = Path(SQLITE_PATH); p.parent.mkdir(parents=True, exist_ok=True)
    return create_engine(f"sqlite:///{p}", future=True)

def ensure_meta(eng):
    with eng.begin() as con:
        con.execute(text("CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT)"))

def touch_last_ingest(eng):
    now = pd.Timestamp.now(tz="UTC").isoformat()
    ensure_meta(eng)
    with eng.begin() as con:
        con.execute(text(
            "INSERT INTO meta(k,v) VALUES ('last_ingest', :v) "
            "ON CONFLICT(k) DO UPDATE SET v=excluded.v"
        ), {"v": now})

# --------------------------- Stazione: CSV parsing ---------------------------
def _pick_time_col(df: pd.DataFrame) -> str:
    if "Time" in df.columns: return "Time"
    candidates = ["datetime","DateTime","date_time","time","timestamp","Timestamp"]
    for c in candidates:
        if c in df.columns: return c
    return df.columns[0]

def read_station_from_csv(csv_path: str) -> pd.DataFrame:
    files = []
    if any(ch in csv_path for ch in ["*", "?", "["]):
        files = sorted(glob.glob(csv_path))
    elif Path(csv_path).exists():
        files = [csv_path]
    if not files: 
        return pd.DataFrame()

    df = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)

    tcol = _pick_time_col(df)
    df = df.rename(columns={tcol: "Time"})
    # parse or localize
    df["Time"] = pd.to_datetime(df["Time"], errors="coerce")
    try:
        import zoneinfo
        tz = zoneinfo.ZoneInfo(STATION_TZ)
        if df["Time"].dt.tz is None:
            df["Time"] = df["Time"].dt.tz_localize(tz).dt.tz_convert("UTC")
        else:
            df["Time"] = df["Time"].dt.tz_convert("UTC")
    except Exception:
        df["Time"] = pd.to_datetime(df["Time"], utc=True, errors="coerce")

    # mappa colonne comuni -> target
    colmap = {
        os.getenv("STATION_TEMP_COL","temp"): "Temp_C",
        os.getenv("STATION_HUM_COL","humidity"): "Humidity",
        os.getenv("STATION_PRESS_COL","pressure"): "Pressure_hPa",
        os.getenv("STATION_WIND_COL","wind"): "Wind_kmh",
        os.getenv("STATION_GUST_COL","gust"): "WindGust_kmh",
        os.getenv("STATION_RAIN_COL","rain"): "Rain_mm",
    }
    for src, dst in list(colmap.items()):
        for c in [src, src.capitalize(), src.upper(), src.replace("_"," ").title()]:
            if c in df.columns:
                df.rename(columns={c: dst}, inplace=True); break

    # cast numerici
    for c in ["Temp_C","Humidity","Pressure_hPa","Rain_mm","Wind_kmh","WindGust_kmh"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    keep = [c for c in ["Time","Temp_C","Humidity","Pressure_hPa","Wind_kmh","WindGust_kmh","Rain_mm"] if c in df.columns]
    df = df[keep].dropna(subset=["Time"]).sort_values("Time")
    return df

# --------------------------- OpenWeather forecast ---------------------------
def fetch_openweather_forecast(lat: float, lon: float, api_key: str) -> pd.DataFrame:
    if not api_key: 
        return pd.DataFrame()
    import requests
    r = requests.get("https://api.openweathermap.org/data/2.5/forecast",
                     params={"lat": lat, "lon": lon, "appid": api_key, "units":"metric", "lang":"it"},
                     timeout=20)
    if not r.ok:
        return pd.DataFrame()
    j = r.json()
    rows = []
    for it in j.get("list", []):
        ts = pd.to_datetime(it.get("dt", None), unit="s", utc=True, errors="coerce")
        main = it.get("main", {}) or {}
        wind = it.get("wind", {}) or {}
        rain = it.get("rain", {}) or {}
        snow = it.get("snow", {}) or {}
        clouds = it.get("clouds", {}) or {}
        rows.append({
            "Time": ts,
            "Temp_C": main.get("temp"),
            "Humidity": main.get("humidity"),
            "Pressure_hPa": main.get("pressure"),
            "Wind_mps": wind.get("speed"),
            "WindDir": wind.get("deg"),
            "Rain_mm": (rain.get("3h") if isinstance(rain, dict) else None),
            "Snow_mm": (snow.get("3h") if isinstance(snow, dict) else None),
            "Clouds": clouds.get("all")
        })
    return pd.DataFrame(rows)

# --------------------------- DB upsert helpers ---------------------------
def upsert_table(df: pd.DataFrame, table: str, eng):
    if df is None or df.empty:
        return
    # crea tabella se manca (schema minimale)
    with eng.begin() as con:
        con.exec_driver_sql(f"CREATE TABLE IF NOT EXISTS {table} (dummy TEXT)")
    # sovrascrivo il contenuto per semplicità/velocità
    df.to_sql(table, eng, if_exists="replace", index=False)

# --------------------------- main ---------------------------
def main():
    lock = FileLock()
    if not lock.acquire():
        print("Ingest già in corso: esco.")
        sys.exit(1)
    try:
        eng = engine()
        Path("./data").mkdir(parents=True, exist_ok=True)

        # STAZIONE (da CSV) -> aggregazione 3H
        if STATION_CSV:
            raw = read_station_from_csv(STATION_CSV)
            if not raw.empty:
                raw = raw.sort_values("Time")
                st3h = raw.set_index("Time").resample("3H").agg({
                    "Temp_C":"mean",
                    "Humidity":"mean",
                    "Pressure_hPa":"mean",
                    "Wind_kmh":"mean",
                    "WindGust_kmh":"max",
                    "Rain_mm":"sum"
                }).reset_index()
                upsert_table(st3h, "station_3h", eng)

        # FORECAST OWM
        fc = fetch_openweather_forecast(LAT, LON, OW_API_KEY)
        if not fc.empty:
            upsert_table(fc, "forecast_ow", eng)

        # META
        touch_last_ingest(eng)
        print("Ingest completato.")
    finally:
        lock.release()

if __name__ == "__main__":
    main()
