
# -*- coding: utf-8 -*-
# Ingest meteo: station + OpenWeather → DB (clean, consistent indentation)

import os
import glob
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DB_URL = os.getenv("DATABASE_URL","").strip()
SQLITE_PATH = os.getenv("SQLITE_PATH","./data/weather.db")
LAT = float(os.getenv("LAT","41.89"))
LON = float(os.getenv("LON","12.49"))
OW_API_KEY = os.getenv("OW_API_KEY","") or os.getenv("OWM_API_KEY","") or os.getenv("OPENWEATHER_API_KEY","")
STATION_CSV = os.getenv("STATION_CSV","").strip()
STATION_TZ = os.getenv("STATION_TZ","UTC")

def engine():
    if DB_URL:
        return create_engine(DB_URL, future=True)
    p = Path(SQLITE_PATH); p.parent.mkdir(parents=True, exist_ok=True)
    return create_engine(f"sqlite:///{p}", future=True)

def ensure_meta(eng):
    with eng.begin() as con:
        con.execute(text("CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT)"))

def touch_last_ingest(eng):
    now = pd.Timestamp.now(tz='UTC').isoformat()
    ensure_meta(eng)
    with eng.begin() as con:
        con.execute(text("INSERT INTO meta(k,v) VALUES ('last_ingest', :v) ON CONFLICT(k) DO UPDATE SET v=excluded.v"), {"v": now})

import re as _re
def parse_wind_value(val, default_unit='km/h'):
    if val is None: return None
    try:
        if isinstance(val, (int,float)): return float(val)
        s = str(val).strip().lower().replace(',', '.')
        m = _re.search(r'([-+]?\d*\.?\d+)\s*(km/?h|kph|m/s|ms|kts?|knots?|mph|bft)?', s)
        if not m: return None
        num = float(m.group(1)); unit = (m.group(2) or default_unit).replace('kph','km/h').replace('ms','m/s').replace('kt','kts')
        if unit in ('km/h','kmh','km/h?'): v = num
        elif unit in ('m/s','mps'): v = num*3.6
        elif unit in ('kts','knot','knots'): v = num*1.852
        elif unit == 'mph': v = num*1.60934
        elif unit in ('bft','bf','beaufort'):
            table = [0,1,5,11,19,28,38,49,61,74,88,102,117]; idx = max(0, min(12, int(round(num)))); v = table[idx]
        else: v = num
        return v
    except Exception:
        return None

def normalize_wind(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df
    d = df.copy()
    default_unit = os.getenv("STATION_WIND_UNIT","km/h").lower()
    speed_cols = ["Wind_kmh","Wind","WindSpeed_kmh","wind_kmh","wind_speed","Wind_mps","wind_mps","Wind_ms","wind_ms","Wind_kts","wind_kts","wind","windspeed","wind_avg","avg_wind","wind_speed_avg"]
    chosen = None
    clmap = {c.lower(): c for c in d.columns}
    for k in speed_cols:
        if k.lower() in clmap: chosen = clmap[k.lower()]; break
    if not chosen:
        for c in d.columns:
            cl = c.lower()
            if 'wind' in cl and 'dir' not in cl: chosen = c; break
    if chosen:
        d["Wind_kmh"] = pd.to_numeric(d[chosen], errors="coerce").apply(lambda x: parse_wind_value(x, default_unit=default_unit))
    gust_cols = ["WindGust_kmh","Gust_kmh","Gust","Wind_gust_kmh","wind_gust_kmh","Gust_kts","wind_gust_kts","Gust_mps","wind_gust_mps","WindGust","gust","gust_kmh","wind_gust"]
    cg = None
    for k in gust_cols:
        if k.lower() in clmap: cg = clmap[k.lower()]; break
    if cg:
        d["WindGust_kmh"] = pd.to_numeric(d[cg], errors="coerce").apply(lambda x: parse_wind_value(x, default_unit=default_unit))
    if "WindGust_kmh" in d.columns and "Wind_kmh" in d.columns:
        d["WindGust_kmh"] = d["WindGust_kmh"].fillna(d["Wind_kmh"])
    return d

def read_station_from_csv(csv_path: str) -> pd.DataFrame:
    files = []
    if any(ch in csv_path for ch in ["*", "?", "["]):
        files = sorted(glob.glob(csv_path))
    else:
        if Path(csv_path).exists():
            files = [csv_path]
    if not files: return pd.DataFrame()
    df = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)
    tcol = os.getenv("STATION_TIME_COL","Time")
    if tcol not in df.columns:
        for c in ["datetime","DateTime","date_time","time","timestamp"]:
            if c in df.columns: tcol = c; break
    df = df.rename(columns={tcol:"Time"})
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
    colmap = {
        os.getenv("STATION_TEMP_COL","temp"): "Temp_C",
        os.getenv("STATION_HUM_COL","humidity"): "Humidity",
        os.getenv("STATION_PRESS_COL","pressure"): "Pressure_hPa",
        os.getenv("STATION_WIND_COL","wind"): "Wind_kmh",
        os.getenv("STATION_GUST_COL","gust"): "WindGust_kmh",
        os.getenv("STATION_RAIN_COL","rain"): "Rain_mm",
    }
    for src, dst in list(colmap.items()):
        for c in [src, src.capitalize(), src.upper()]:
            if c in df.columns:
                df.rename(columns={c: dst}, inplace=True); break
    for c in ["Temp_C","Humidity","Pressure_hPa","Rain_mm"]:
        if c in df.columns: df[c] = pd.to_numeric(df[c], errors="coerce")
    df = normalize_wind(df)
    keep = [c for c in ["Time","Temp_C","Humidity","Pressure_hPa","Wind_kmh","WindGust_kmh","Rain_mm"] if c in df.columns]
    return df[keep].dropna(subset=["Time"]).sort_values("Time")

def fetch_openweather_forecast(lat: float, lon: float, api_key: str) -> pd.DataFrame:
    if not api_key: return pd.DataFrame()
    import requests
    r = requests.get("https://api.openweathermap.org/data/2.5/forecast",
                     params={"lat": lat, "lon": lon, "appid": api_key, "units":"metric", "lang":"it"}, timeout=20)
    if not r.ok: return pd.DataFrame()
    j = r.json()
    items = j.get("list", [])
    rows = []
    for it in items:
        ts = pd.to_datetime(it.get("dt", None), unit="s", utc=True, errors="coerce")
        main = it.get("main", {})
        wind = it.get("wind", {})
        rain = it.get("rain", {})
        snow = it.get("snow", {})
        clouds = it.get("clouds", {})
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
    df = pd.DataFrame(rows)
    return df

def upsert_table(df: pd.DataFrame, table: str, eng):
    if df is None or df.empty: return
    with eng.begin() as con:
        con.exec_driver_sql(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM (SELECT NULL as Time) WHERE 0")
    df.to_sql(table, eng, if_exists="replace", index=False)

def main():
    eng = engine()
    Path("./data").mkdir(parents=True, exist_ok=True)
    if STATION_CSV:
        st_df = read_station_from_csv(STATION_CSV)
        if not st_df.empty:
            st_df = st_df.sort_values("Time")
            st30 = st_df.set_index("Time").resample("30T").agg({
                "Temp_C":"mean",
                "Humidity":"mean",
                "Pressure_hPa":"mean",
                "Wind_kmh":"mean",
                "WindGust_kmh":"max",
                "Rain_mm":"sum"
            }).reset_index()
            upsert_table(st30, "station_30m", eng)
    fc = fetch_openweather_forecast(float(os.getenv("LAT","41.89")), float(os.getenv("LON","12.49")), OW_API_KEY)
    if not fc.empty:
        upsert_table(fc, "forecast_ow", eng)
    touch_last_ingest(eng)
    print("Ingest completato.")

if __name__ == "__main__":
    main()
