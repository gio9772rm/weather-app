import os
from datetime import datetime, timedelta, timezone
import pandas as pd
import numpy as np
import requests
from sqlalchemy import create_engine, text as sqltext
from dotenv import load_dotenv

from ecowitt_api import get_real_time, get_history, real_time_to_df, history_to_df

print("=== Ingest Verbose v3 ===")

load_dotenv()
SQLITE_PATH = os.getenv("SQLITE_PATH", "./data/weather.db")
OWM_API_KEY = os.getenv("OWM_API_KEY")
LAT = os.getenv("LAT")
LON = os.getenv("LON")
EC_APP = os.getenv("ECOWITT_APPLICATION_KEY")
EC_KEY = os.getenv("ECOWITT_API_KEY")
EC_MAC = os.getenv("ECOWITT_MAC")

print(f"DB: {SQLITE_PATH}")
print(f"OWM key present: {bool(OWM_API_KEY)}  lat/lon: {LAT},{LON}")
print(f"Ecowitt creds present: {bool(EC_APP and EC_KEY and EC_MAC)} with MAC='{EC_MAC}'")

FORECAST_URL = "https://api.openweathermap.org/data/2.5/forecast"

def ensure_db(engine):
    schema = """
    CREATE TABLE IF NOT EXISTS station_3h (
      Time TEXT PRIMARY KEY,
      Temp_C REAL,
      Humidity REAL,
      Pressure_hPa REAL,
      Wind_kmh REAL,
      WindGust_kmh REAL,
      Rain_mm REAL
    );
    CREATE TABLE IF NOT EXISTS forecast_ow (
      Time TEXT PRIMARY KEY,
      Temp_C REAL,
      Humidity REAL,
      Pressure_hPa REAL,
      Clouds REAL,
      Wind_mps REAL,
      WindDir REAL,
      Rain_mm REAL,
      Snow_mm REAL
    );
    CREATE TABLE IF NOT EXISTS meta (
      k TEXT PRIMARY KEY,
      v TEXT
    );
    """
    with engine.begin() as conn:
        for stmt in schema.split(";"):
            s = stmt.strip()
            if s:
                conn.execute(sqltext(s))

def upsert_table(engine, df, table, pk="Time"):
    if df is None or df.empty:
        return 0
    ins = 0
    with engine.begin() as conn:
        placeholders = ",".join([":" + c for c in df.columns])
        cols = ",".join(df.columns)
        update_clause = ",".join([f"{c}=excluded.{c}" for c in df.columns if c != pk])
        sql = sqltext(f"INSERT INTO {table} ({cols}) VALUES ({placeholders}) "
                   f"ON CONFLICT({pk}) DO UPDATE SET {update_clause}")
        for _, row in df.iterrows():
            conn.execute(sql, row.to_dict())
            ins += 1
    return ins

def fetch_openweather(api_key, lat, lon):
    if not api_key or not lat or not lon:
        print("SKIP OW: missing api_key or lat/lon")
        return pd.DataFrame()
    params = {"lat": lat, "lon": lon, "appid": api_key, "units": "metric"}
    r = requests.get(FORECAST_URL, params=params, timeout=30)
    print("OW status:", r.status_code, r.reason)
    r.raise_for_status()
    data = r.json()
    lst = data.get("list", [])
    print("OW items:", len(lst))
    rows = []
    for item in lst:
        ts = datetime.fromtimestamp(item["dt"], tz=timezone.utc).isoformat()
        main = item.get("main", {}); wind = item.get("wind", {}); clouds = item.get("clouds", {})
        rain = item.get("rain", {}); snow = item.get("snow", {})
        rows.append({
            "Time": ts,
            "Temp_C": main.get("temp"),
            "Humidity": main.get("humidity"),
            "Pressure_hPa": main.get("pressure"),
            "Clouds": clouds.get("all"),
            "Wind_mps": wind.get("speed"),
            "WindDir": wind.get("deg"),
            "Rain_mm": (rain.get("3h") if isinstance(rain, dict) else None),
            "Snow_mm": (snow.get("3h") if isinstance(snow, dict) else None),
        })
    return pd.DataFrame(rows)

def mac_variants(mac):
    if not mac:
        return []
    m = mac.replace(":", "").replace("-", "")
    return [mac, mac.upper(), mac.lower(), m, m.upper(), m.lower()]

def ecowitt_backfill(engine, app, key, mac, days=7):
    total = 0
    now = datetime.now()
    for mac_try in mac_variants(mac):
        print(f"  * Provo MAC: {mac_try}")
        for i in range(days):
            end = now - timedelta(days=i)
            start = end - timedelta(days=1)
            success = False
            for cycle in [None, "30min", "5min", "1hour", "240min"]:
                try:
                    payload = get_history(app, key, mac_try, start, end,
                                          call_back="outdoor,wind,pressure,rainfall",
                                          cycle_type=cycle)
                    df = history_to_df(payload)
                    if df is None or df.empty:
                        continue
                    # convert wind to km/h, resample to 3h and upsert
                    if "Wind_mps" in df.columns:
                        df["Wind_kmh"] = df["Wind_mps"] * 3.6
                    if "Time" not in df.columns:
                        continue
                    df = df.dropna(subset=["Time"]).sort_values("Time").drop_duplicates("Time")
                    df3h = (df.set_index("Time")
                              .resample("3H")
                              .agg({"Temp_C":"mean","Humidity":"mean","Pressure_hPa":"mean",
                                    "Wind_kmh":"mean","Rain_mm":"sum"})
                              .reset_index())
                    if df3h.empty:
                        continue
                    df3h["Time"] = pd.to_datetime(df3h["Time"]).dt.tz_localize("UTC").dt.strftime("%Y-%m-%dT%H:%M:%SZ")
                    df3h["WindGust_kmh"] = None
                    df3h = df3h[["Time","Temp_C","Humidity","Pressure_hPa","Wind_kmh","WindGust_kmh","Rain_mm"]]
                    n = upsert_table(engine, df3h, "station_3h")
                    total += n
                    print(f"    - {start:%Y-%m-%d} cycle={cycle or 'default'} -> {len(df3h)} rows (inserted {n})")
                    success = True
                    break
                except Exception as e:
                    print(f"    x {start:%Y-%m-%d} cycle={cycle or 'default'} ERROR: {e}")
            if not success:
                print(f"    ! Nessun dato per {start:%Y-%m-%d}")
        if total > 0:
            break
    return total

def ecowitt_realtime(engine, app, key, mac):
    for mac_try in mac_variants(mac):
        try:
            payload = get_real_time(app, key, mac_try)
            df = real_time_to_df(payload)
            if df is None or df.empty or "Time" not in df.columns:
                continue
            if "Wind_mps" in df.columns:
                df["Wind_kmh"] = df["Wind_mps"] * 3.6
            df["Time"] = pd.to_datetime(df["Time"]).dt.tz_localize("UTC").dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            df["WindGust_kmh"] = None
            df = df[["Time","Temp_C","Humidity","Pressure_hPa","Wind_kmh","WindGust_kmh","Rain_mm"]]
            n = upsert_table(engine, df, "station_3h")
            if n>0:
                print(f"  * Realtime OK con MAC {mac_try}")
                return n
        except Exception as e:
            print(f"  x Realtime ERROR con MAC {mac_try}: {e}")
    print("  ! Realtime: nessun dato")
    return 0

def main():
    engine = create_engine(f"sqlite:///{SQLITE_PATH}", future=True)
    ensure_db(engine)

    st_rows = 0
    if EC_APP and EC_KEY and EC_MAC:
        print("-> Ecowitt backfill ultimi 7 giorni (multi-MAC, multi-cycle)...")
        st_rows += ecowitt_backfill(engine, EC_APP, EC_KEY, EC_MAC, days=7)
        print("-> Ecowitt realtime...")
        st_rows += ecowitt_realtime(engine, EC_APP, EC_KEY, EC_MAC)

    print("-> OpenWeather forecast...")
    fc_rows = 0
    try:
        ow = fetch_openweather(OWM_API_KEY, LAT, LON)
        fc_rows += upsert_table(engine, ow, "forecast_ow")
    except Exception as e:
        print("OpenWeather ERROR:", e)

    with engine.begin() as conn:
        conn.execute(sqltext("INSERT OR REPLACE INTO meta (k,v) VALUES ('last_ingest', :v)"),
                     {"v": datetime.now(timezone.utc).isoformat()})
    print(f"Upserted -> station_3h: {st_rows}  forecast_ow: {fc_rows}")
    print("DONE")

if __name__ == "__main__":
    main()