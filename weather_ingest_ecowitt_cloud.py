
# -*- coding: utf-8 -*-
"""
Ecowitt Cloud ingest -> Postgres/SQLite
- Pulls real-time data (past 2h) and optionally backfills with 5-min history.
- Normalizes to metric and writes to "station_raw" and updates meta.last_ingest.

Env vars required:
  DATABASE_URL             (postgres://...  or sqlite path via SQLITE_PATH fallback)
  ECOWITT_APP_KEY          (Application Key from ecowitt.net)
  ECOWITT_API_KEY          (API Key from ecowitt.net)
  ECOWITT_MAC              (Station MAC address, colon or dash is fine)
Optional:
  ECOWITT_TZ               (e.g. Europe/Rome), default UTC
  BACKFILL_HOURS           (default 0; set 24 for one-day backfill on first run)
  LOG_LEVEL                (INFO/DEBUG/WARNING), default INFO

Usage:
  python weather_ingest_ecowitt_cloud.py          # one-shot
"""

import os, sys, time, math, logging
from datetime import datetime, timedelta, timezone
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

LOG_LEVEL = os.getenv("LOG_LEVEL","INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO), format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("ingest_ecowitt")

DB_URL = os.getenv("DATABASE_URL","").strip()
SQLITE_PATH = os.getenv("SQLITE_PATH","./data/weather.db").strip()
APP_KEY = os.getenv("ECOWITT_APP_KEY","").strip()
API_KEY = os.getenv("ECOWITT_API_KEY","").strip()
MAC     = os.getenv("ECOWITT_MAC","").strip().replace("-",":").lower()
TZ_NAME = os.getenv("ECOWITT_TZ","UTC").strip() or "UTC"
BACKFILL_HOURS = int(os.getenv("BACKFILL_HOURS","0") or "0")

if not APP_KEY or not API_KEY or not MAC:
    log.error("Missing ECOWITT_* keys (APP_KEY/API_KEY/MAC). Set them in env.")
    sys.exit(2)

def get_engine():
    if DB_URL:
        return create_engine(DB_URL, future=True)
    # fallback to sqlite
    from pathlib import Path
    p = Path(SQLITE_PATH); p.parent.mkdir(parents=True, exist_ok=True)
    return create_engine(f"sqlite:///{p}", future=True)

ENG = get_engine()

def _exec(sql, **params):
    with ENG.begin() as cx:
        cx.execute(text(sql), params)

def _df(df):
    import pandas as pd
    return df if isinstance(df, pd.DataFrame) else pd.DataFrame(df)

def ensure_schema():
    # Create basic tables if missing (both SQLite/Postgres compatible)
    _exec("""
    CREATE TABLE IF NOT EXISTS station_raw (
      Time TIMESTAMPTZ PRIMARY KEY,
      Temp_C REAL,
      Humidity REAL,
      Pressure_hPa REAL,
      Wind_kmh REAL,
      WindGust_kmh REAL,
      WindDir REAL,
      Rain_mm REAL
    );
    """)
    _exec("""
    CREATE TABLE IF NOT EXISTS meta (
      k TEXT PRIMARY KEY,
      v TEXT
    );
    """)

def touch_last_ingest():
    now = pd.Timestamp.utcnow().tz_localize("UTC").isoformat()
    _exec("""INSERT INTO meta (k,v) VALUES ('last_ingest', :v)
             ON CONFLICT (k) DO UPDATE SET v=excluded.v;""", v=now)

def _tz():
    try:
        import zoneinfo
        return zoneinfo.ZoneInfo(TZ_NAME)
    except Exception:
        return timezone.utc

def _to_utc(ts):
    if isinstance(ts, (int, float)):
        return datetime.fromtimestamp(int(ts), tz=timezone.utc)
    if isinstance(ts, str):
        # expected format 'YYYY-MM-DD HH:MM:SS'
        try:
            dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            pass
    if isinstance(ts, datetime):
        return ts.astimezone(timezone.utc) if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc)

def _kmh_from(value, unit_hint=None):
    try:
        v = float(value)
    except Exception:
        return None
    if unit_hint == "m/s":
        return v * 3.6
    if unit_hint == "mph":
        return v * 1.60934
    return v

def _mm_from_inch(val):
    try:
        return float(val) * 25.4
    except Exception:
        return None

def ecowitt_get(path, params):
    url = f"https://api.ecowitt.net/api/v3/{path}"
    p = {"application_key": APP_KEY, "api_key": API_KEY, **params}
    r = requests.get(url, params=p, timeout=20)
    r.raise_for_status()
    return r.json()

def parse_realtime(json_obj):
    import pandas as pd
    data = json_obj.get("data") if isinstance(json_obj, dict) else None
    if not data:
        return pd.DataFrame()

    points = []
    candidates = data.get("list") if isinstance(data, dict) else None
    if isinstance(candidates, list) and candidates:
        iterable = candidates
    else:
        iterable = [data]

    for item in iterable:
        t_raw = item.get("time") or item.get("last_update_time") or item.get("update_time")
        t = _to_utc(t_raw)
        outdoor = item.get("outdoor", {}) or {}
        wind    = item.get("wind", {}) or {}
        rain    = item.get("rainfall", {}) or {}
        press   = item.get("pressure", {}) or {}

        temp_c = outdoor.get("temperature") or outdoor.get("temp_c")
        hum = outdoor.get("humidity")
        p_hpa = press.get("rel") or press.get("relative") or press.get("rel_hpa") or press.get("relative_hpa") or press.get("abs_hpa")
        if p_hpa and p_hpa > 2000:
            p_hpa = p_hpa / 100.0

        wspd = wind.get("speed") or wind.get("windspeed") or wind.get("avg") or wind.get("avg_mps")
        wgst = wind.get("gust") or wind.get("max") or wind.get("gust_mps")
        wdir = wind.get("direction") or wind.get("dir_deg") or wind.get("dir")

        rain_mm = rain.get("rate") or rain.get("rain_rate") or rain.get("rainrate_mm") or rain.get("rainrate")
        if rain_mm is None and "rainrate_in" in rain:
            rain_mm = _mm_from_inch(rain["rainrate_in"])

        rec = dict(
            Time=pd.Timestamp(t).tz_convert("UTC"),
            Temp_C=float(temp_c) if temp_c is not None else None,
            Humidity=float(hum) if hum is not None else None,
            Pressure_hPa=float(p_hpa) if p_hpa is not None else None,
            Wind_kmh=_kmh_from(wspd),
            WindGust_kmh=_kmh_from(wgst),
            WindDir=float(wdir) if wdir is not None else None,
            Rain_mm=float(rain_mm) if rain_mm is not None else None,
        )
        points.append(rec)
    df = pd.DataFrame(points).drop_duplicates(subset=["Time"]).sort_values("Time")
    return df

def parse_history(json_obj):
    import pandas as pd
    data = json_obj.get("data") if isinstance(json_obj, dict) else None
    if not data:
        return pd.DataFrame()
    records = data.get("list") or data.get("data") or []
    if not isinstance(records, list):
        records = [records]
    pts = []
    for item in records:
        t_raw = item.get("time") or item.get("date") or item.get("update_time")
        t = _to_utc(t_raw)
        outdoor = item.get("outdoor", {}) or {}
        wind    = item.get("wind", {}) or {}
        rain    = item.get("rainfall", {}) or {}
        press   = item.get("pressure", {}) or {}

        temp_c = outdoor.get("temperature") or outdoor.get("temp_c")
        hum = outdoor.get("humidity")
        p_hpa = press.get("rel") or press.get("relative") or press.get("rel_hpa") or press.get("relative_hpa") or press.get("abs_hpa")
        if p_hpa and p_hpa > 2000:
            p_hpa = p_hpa/100.0
        wspd = wind.get("speed") or wind.get("windspeed") or wind.get("avg") or wind.get("avg_mps")
        wgst = wind.get("gust") or wind.get("max") or wind.get("gust_mps")
        wdir = wind.get("direction") or wind.get("dir_deg") or wind.get("dir")
        rain_mm = rain.get("rate") or rain.get("rain_rate") or rain.get("rainrate_mm") or rain.get("rainrate")
        if rain_mm is None and "rainrate_in" in rain:
            rain_mm = _mm_from_inch(rain["rainrate_in"])

        rec = dict(
            Time=pd.Timestamp(t).tz_convert("UTC"),
            Temp_C=float(temp_c) if temp_c is not None else None,
            Humidity=float(hum) if hum is not None else None,
            Pressure_hPa=float(p_hpa) if p_hpa is not None else None,
            Wind_kmh=_kmh_from(wspd),
            WindGust_kmh=_kmh_from(wgst),
            WindDir=float(wdir) if wdir is not None else None,
            Rain_mm=float(rain_mm) if rain_mm is not None else None,
        )
        pts.append(rec)
    return pd.DataFrame(pts).drop_duplicates(subset=["Time"]).sort_values("Time")

def upsert(df):
    import pandas as pd
    if df.empty:
        return 0
    df = df.copy()
    df["Time"] = pd.to_datetime(df["Time"], utc=True)
    inserted = 0
    with ENG.begin() as cx:
        for _, row in df.iterrows():
            cx.execute(text("""
                INSERT INTO station_raw (Time, Temp_C, Humidity, Pressure_hPa, Wind_kmh, WindGust_kmh, WindDir, Rain_mm)
                VALUES (:Time, :Temp_C, :Humidity, :Pressure_hPa, :Wind_kmh, :WindGust_kmh, :WindDir, :Rain_mm)
                ON CONFLICT (Time) DO UPDATE SET
                  Temp_C=excluded.Temp_C,
                  Humidity=excluded.Humidity,
                  Pressure_hPa=excluded.Pressure_hPa,
                  Wind_kmh=excluded.Wind_kmh,
                  WindGust_kmh=excluded.WindGust_kmh,
                  WindDir=excluded.WindDir,
                  Rain_mm=excluded.Rain_mm;
            """), {
                "Time": row["Time"].to_pydatetime(),
                "Temp_C": row.get("Temp_C"),
                "Humidity": row.get("Humidity"),
                "Pressure_hPa": row.get("Pressure_hPa"),
                "Wind_kmh": row.get("Wind_kmh"),
                "WindGust_kmh": row.get("WindGust_kmh"),
                "WindDir": row.get("WindDir"),
                "Rain_mm": row.get("Rain_mm"),
            })
            inserted += 1
    return inserted

def main():
    ensure_schema()
    total = 0
    try:
        rt = ecowitt_get("device/real_time", {"mac": MAC, "call_back": "all"})
        df_rt = parse_realtime(rt)
        total += upsert(df_rt.tail(1))
        log.info("Realtime: got %s points, inserted %s", len(df_rt), min(1, len(df_rt)))
    except Exception as e:
        log.warning("Realtime fetch failed: %s", e)

    if BACKFILL_HOURS > 0:
        now = datetime.now(timezone.utc)
        start = now - timedelta(hours=BACKFILL_HOURS)
        day = start
        while day < now:
            day_end = min(datetime(day.year, day.month, day.day, 23,59,59, tzinfo=timezone.utc), now)
            try:
                hist = ecowitt_get("device/history", {
                    "mac": MAC,
                    "start_date": day.strftime("%Y-%m-%d 00:00:00"),
                    "end_date": day_end.strftime("%Y-%m-%d %H:%M:%S"),
                    "call_back": "outdoor,wind,pressure,rainfall"
                })
                df_h = parse_history(hist)
                total += upsert(df_h)
                log.info("History %s: %s points", day.date(), len(df_h))
            except Exception as e:
                log.warning("History fetch failed for %s: %s", day.date(), e)
            day = (day + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

    touch_last_ingest()
    log.info("Done. Upserted rows: %s", total)

if __name__ == "__main__":
    main()
