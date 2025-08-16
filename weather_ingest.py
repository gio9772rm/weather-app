# -*- coding: utf-8 -*-
"""
weather_ingest.py — Ingest locale (CSV) + forecast OpenWeather
- Normalizzazione pressione (hPa reali) & scrittura station_raw
- Aggregazione station_3h da station_raw
- Lock cross-platform
"""

import os, sys, glob, time
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# --------------------------- Lock ---------------------------
def _lock_path() -> str:
    return os.path.join(os.getenv("TEMP", "."), "ingest.lock") if os.name == "nt" else "/tmp/ingest.lock"

class FileLock:
    def __init__(self, path: Optional[str] = None, stale_seconds: int = 900):
        self.path = path or _lock_path()
        self.stale_seconds = int(stale_seconds)
        self._fd = None
    def acquire(self) -> bool:
        flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
        try:
            self._fd = os.open(self.path, flags, 0o644)
            os.write(self._fd, str(int(time.time())).encode("ascii","ignore"))
            return True
        except FileExistsError:
            try:
                ts = int(Path(self.path).read_text().strip() or "0")
            except Exception:
                ts = 0
            if ts and (time.time() - ts) > self.stale_seconds:
                try: os.remove(self.path)
                except Exception: pass
                try:
                    self._fd = os.open(self.path, flags, 0o644)
                    os.write(self._fd, str(int(time.time())).encode("ascii","ignore"))
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

# --------------------------- Config & DB ---------------------------
load_dotenv()

def _get_float_env(name: str, default: float) -> float:
    try: return float(os.getenv(name) or default)
    except Exception: return float(default)

def _normalize_db_url(raw: str) -> str:
    u = (raw or "").strip()
    if not u: return u
    if u.startswith("postgres://"): u = "postgresql+psycopg2://" + u[len("postgres://"):]
    return u

DB_URL = _normalize_db_url(os.getenv("DATABASE_URL", ""))
SQLITE_PATH = os.getenv("SQLITE_PATH", "./data/weather.db")

LAT = _get_float_env("LAT", 41.89)
LON = _get_float_env("LON", 12.49)
OW_API_KEY = os.getenv("OW_API_KEY", "") or os.getenv("OWM_API_KEY", "") or os.getenv("OPENWEATHER_API_KEY", "")
STATION_CSV = os.getenv("STATION_CSV", "").strip()
STATION_TZ = os.getenv("STATION_TZ", "UTC")

def engine():
    if DB_URL: return create_engine(DB_URL, future=True)
    p = Path(SQLITE_PATH); p.parent.mkdir(parents=True, exist_ok=True)
    return create_engine(f"sqlite:///{p}", future=True)

def ensure_schema(eng):
    with eng.begin() as con:
        con.execute(text("""CREATE TABLE IF NOT EXISTS station_raw (
          Time TIMESTAMPTZ PRIMARY KEY,
          Temp_C REAL, Humidity REAL, Pressure_hPa REAL,
          Wind_kmh REAL, WindGust_kmh REAL, WindDir REAL, Rain_mm REAL
        );"""))
        con.execute(text("""CREATE TABLE IF NOT EXISTS station_3h (
          Time TIMESTAMPTZ PRIMARY KEY,
          Temp_C REAL, Humidity REAL, Pressure_hPa REAL,
          Wind_kmh REAL, WindGust_kmh REAL, Rain_mm REAL
        );"""))
        con.execute(text("CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT)"))

def touch_last_ingest(eng):
    with eng.begin() as con:
        con.execute(text(
            "INSERT INTO meta(k,v) VALUES ('last_ingest', :v) "
            "ON CONFLICT(k) DO UPDATE SET v=excluded.v"
        ), {"v": pd.Timestamp.now(tz='UTC').isoformat()})

# --------------------------- Normalizzazioni ---------------------------
def _fix_pressure(v):
    if v is None: return None
    try: x = float(v)
    except: return None
    if 800.0 <= x <= 1100.0:  # hPa ok
        return x
    if 8000.0 <= x <= 11000.0:  # hPa * 10
        return x / 10.0
    if 80000.0 <= x <= 110000.0:  # Pa
        return x / 100.0
    if 50.0 <= x <= 200.0:  # kPa
        return x * 10.0
    if 20.0 <= x <= 40.0:   # inHg
        return x * 33.8638866667
    # fallback: porta nel range plausibile
    for _ in range(3):
        if 800.0 <= x <= 1100.0: break
        x = x/10.0 if x>1100.0 else x*10.0
    return x

# --------------------------- CSV → station_raw ---------------------------
def _pick_time_col(df: pd.DataFrame) -> str:
    if "Time" in df.columns: return "Time"
    for c in ["datetime","DateTime","date_time","time","timestamp","Timestamp"]:
        if c in df.columns: return c
    return df.columns[0]

def read_station_from_csv(csv_path: str) -> pd.DataFrame:
    files = []
    if any(ch in csv_path for ch in ["*", "?", "["]): files = sorted(glob.glob(csv_path))
    elif Path(csv_path).exists(): files = [csv_path]
    if not files: return pd.DataFrame()

    df = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)
    tcol = _pick_time_col(df)
    df = df.rename(columns={tcol: "Time"})
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

    # map colonne comuni
    colmap = {
        os.getenv("STATION_TEMP_COL","temp"): "Temp_C",
        os.getenv("STATION_HUM_COL","humidity"): "Humidity",
        os.getenv("STATION_PRESS_COL","pressure"): "Pressure_hPa",
        os.getenv("STATION_WIND_COL","wind"): "Wind_kmh",
        os.getenv("STATION_GUST_COL","gust"): "WindGust_kmh",
        os.getenv("STATION_WDIR_COL","winddir"): "WindDir",
        os.getenv("STATION_RAIN_COL","rain"): "Rain_mm",
    }
    for src, dst in list(colmap.items()):
        for c in [src, src.capitalize(), src.upper(), src.replace("_"," ").title()]:
            if c in df.columns:
                df.rename(columns={c: dst}, inplace=True); break

    # cast numerici
    for c in ["Temp_C","Humidity","Pressure_hPa","Rain_mm","Wind_kmh","WindGust_kmh","WindDir"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # normalizza pressione
    if "Pressure_hPa" in df.columns:
        df["Pressure_hPa"] = df["Pressure_hPa"].map(_fix_pressure)

    keep = [c for c in ["Time","Temp_C","Humidity","Pressure_hPa","Wind_kmh","WindGust_kmh","WindDir","Rain_mm"] if c in df.columns]
    return df[keep].dropna(subset=["Time"]).sort_values("Time")

# --------------------------- OpenWeather forecast ---------------------------
def fetch_openweather_forecast(lat: float, lon: float, api_key: str) -> pd.DataFrame:
    if not api_key: return pd.DataFrame()
    import requests
    r = requests.get("https://api.openweathermap.org/data/2.5/forecast",
                     params={"lat": lat, "lon": lon, "appid": api_key, "units":"metric", "lang":"it"},
                     timeout=20)
    if not r.ok: return pd.DataFrame()
    j = r.json()
    rows = []
    for it in j.get("list", []):
        ts = pd.to_datetime(it.get("dt", None), unit="s", utc=True, errors="coerce")
        main = it.get("main", {}) or {}; wind = it.get("wind", {}) or {}; rain = it.get("rain", {}) or {}; snow = it.get("snow", {}) or {}; clouds = it.get("clouds", {}) or {}
        rows.append({
            "Time": ts, "Temp_C": main.get("temp"), "Humidity": main.get("humidity"), "Pressure_hPa": main.get("pressure"),
            "Wind_mps": wind.get("speed"), "WindDir": wind.get("deg"),
            "Rain_mm": (rain.get("3h") if isinstance(rain, dict) else None),
            "Snow_mm": (snow.get("3h") if isinstance(snow, dict) else None),
            "Clouds": clouds.get("all")
        })
    return pd.DataFrame(rows)

# --------------------------- DB writes ---------------------------
def upsert_raw(df: pd.DataFrame, eng):
    if df is None or df.empty: return 0
    df = df.copy(); df["Time"] = pd.to_datetime(df["Time"], utc=True)
    with eng.begin() as con:
        for _, row in df.iterrows():
            con.execute(text("""
                INSERT INTO station_raw (Time, Temp_C, Humidity, Pressure_hPa, Wind_kmh, WindGust_kmh, WindDir, Rain_mm)
                VALUES (:Time, :Temp_C, :Humidity, :Pressure_hPa, :Wind_kmh, :WindGust_kmh, :WindDir, :Rain_mm)
                ON CONFLICT (Time) DO UPDATE SET
                  Temp_C=excluded.Temp_C, Humidity=excluded.Humidity, Pressure_hPa=excluded.Pressure_hPa,
                  Wind_kmh=excluded.Wind_kmh, WindGust_kmh=excluded.WindGust_kmh, WindDir=excluded.WindDir, Rain_mm=excluded.Rain_mm;
            """), {k:(row.get(k).item() if hasattr(row.get(k),'item') else row.get(k)) for k in ["Time","Temp_C","Humidity","Pressure_hPa","Wind_kmh","WindGust_kmh","WindDir","Rain_mm"]})
    return len(df)

def recompute_station_3h(eng, lookback_hours: int = 96):
    with eng.begin() as con:
        df = pd.read_sql(
            text("SELECT * FROM station_raw WHERE Time >= :t0 ORDER BY Time"),
            con, params={"t0": (pd.Timestamp.utcnow() - pd.Timedelta(hours=lookback_hours)).isoformat()}
        )
    if df.empty: return 0
    df["Time"] = pd.to_datetime(df["Time"], utc=True)
    st3h = (df.set_index("Time")
              .resample("3H")
              .agg({"Temp_C":"mean","Humidity":"mean","Pressure_hPa":"mean",
                    "Wind_kmh":"mean","WindGust_kmh":"max","Rain_mm":"sum"})
              .reset_index())
    with eng.begin() as con:
        st3h.to_sql("station_3h", con.connection, if_exists="replace", index=False)
    return len(st3h)

def upsert_table(df: pd.DataFrame, table: str, eng):
    if df is None or df.empty: return
    with eng.begin() as con:
        con.exec_driver_sql(f"CREATE TABLE IF NOT EXISTS {table} (dummy TEXT)")
    df.to_sql(table, eng, if_exists="replace", index=False)

# --------------------------- main ---------------------------
def main():
    lock = FileLock()
    if not lock.acquire():
        print("Ingest già in corso: esco.")
        sys.exit(1)
    try:
        eng = engine()
        ensure_schema(eng)
        Path("./data").mkdir(parents=True, exist_ok=True)

        # STAZIONE locale (CSV) → station_raw + station_3h
        if STATION_CSV:
            raw = read_station_from_csv(STATION_CSV)
            if not raw.empty:
                n = upsert_raw(raw, eng)
                n3 = recompute_station_3h(eng, lookback_hours=96)
                print(f"Stazione: upsert {n} raw, {n3} bucket 3h")

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
