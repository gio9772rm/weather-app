# -*- coding: utf-8 -*-
"""
weather_ingest_ecowitt_cloud.py
Ingest da Ecowitt Cloud -> DB (Postgres via DATABASE_URL, fallback SQLite).
Compatibile con schema minuscolo:
  station_raw(time, temp_c, humidity, pressure_hpa, wind_kmh, windgust_kmh, winddir, rain_mm)
  station_3h(time, temp_c, humidity, pressure_hpa, wind_kmh, windgust_kmh, rain_mm)
"""

import os
import sys
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
import pandas as pd
from sqlalchemy import text
from dotenv import load_dotenv

from db import get_engine as _get_engine, ensure_schema as _ensure_schema

# -------------------- Setup & log --------------------
load_dotenv()
LOG_LEVEL = (os.getenv("LOG_LEVEL") or "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("ingest_cloud")

DB_URL  = (os.getenv("DATABASE_URL") or "").strip()
SQLITE  = (os.getenv("SQLITE_PATH") or "./data/weather.db").strip()
APP_KEY = (os.getenv("ECOWITT_APP_KEY") or os.getenv("ECOWITT_APPLICATION_KEY") or "").strip()
API_KEY = (os.getenv("ECOWITT_API_KEY") or "").strip()
MAC     = (os.getenv("ECOWITT_MAC") or "").strip().replace("-",":").lower()
BACKFILL_HOURS = int((os.getenv("BACKFILL_HOURS") or "0").strip() or "0")

# -------------------- DB helpers --------------------
def engine():
    """Ritorna l'Engine SQLAlchemy centralizzato (db.py)."""
    return _get_engine()


def ensure_schema():
    """Crea le tabelle se mancanti usando schema.sql (db.py)."""
    _ensure_schema()

# -------------------- Ecowitt API --------------------
def ecowitt_get(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    url = f"https://api.ecowitt.net/api/v3/{path}"
    p = {"application_key": APP_KEY, "api_key": API_KEY, **params}
    r = requests.get(url, params=p, timeout=25)
    r.raise_for_status()
    return r.json()

# -------------------- Parsing utils --------------------
def safe_float(val: Any) -> Optional[float]:
    """Float robusto con virgola, None e stringhe strane."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip()
    if not s:
        return None
    s = s.replace(" ", "")
    # Gestione separatori EU
    if "." in s and "," in s and s.rfind(",") > s.rfind("."):
        s = s.replace(".", "").replace(",", ".")
    else:
        if "," in s and "." not in s:
            s = s.replace(",", ".")
        if s.count(",") > 1 and "." in s:
            s = s.replace(",", "")
        if s.count(".") > 1 and "," not in s:
            s = s.replace(".", "")
    try:
        return float(s)
    except Exception:
        return None

def first(d: Any, keys: List[str]) -> Any:
    """Ritorna il primo campo presente tra keys in un dict (o None)."""
    if not isinstance(d, dict):
        return None
    for k in keys:
        if k in d:
            return d[k]
    return None

def val_and_unit(node: Any) -> (Optional[float], Optional[str]):
    """Accetta sia {value,unit} sia float grezzo."""
    if isinstance(node, dict):
        v = first(node, ["value","val","v"])
        u = first(node, ["unit","u"])
        return safe_float(v), (u or None)
    return safe_float(node), None

def c_from(val: Optional[float], unit: Optional[str]) -> Optional[float]:
    if val is None:
        return None
    u = (unit or "").lower()
    v = float(val)
    if u in ("f","°f","fahrenheit","degf"):
        return (v - 32.0) * 5.0/9.0
    return v  # assumiamo °C

def hpa_from(val: Optional[float], unit: Optional[str]) -> Optional[float]:
    if val is None:
        return None
    u = (unit or "").lower()
    v = float(val)
    # conversioni comuni
    if "inhg" in u:
        return v * 33.8638866667
    if u == "pa":
        return v / 100.0
    if "kpa" in u:
        return v * 10.0
    # correzioni da formati scalati
    if 8000.0 <= v <= 11000.0:
        return v / 10.0
    if v > 2000.0:
        return v / 100.0
    return v  # già hPa

def kmh_from(val: Optional[float], unit: Optional[str]) -> Optional[float]:
    if val is None:
        return None
    u = (unit or "").lower()
    v = float(val)
    if "m/s" in u or u == "mps" or u == "ms":
        return v * 3.6
    if "mph" in u:
        return v * 1.60934
    if "knot" in u or "kt" in u:
        return v * 1.852
    return v  # assumiamo km/h

# -------------------- Parsing payload --------------------
def parse_item(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    # timestamp
    t_raw = first(item, ["time","last_update_time","update_time","date","timestamp"])
    try:
        ts = pd.to_datetime(t_raw, utc=True).to_pydatetime()
    except Exception:
        ts = datetime.now(timezone.utc)

    out  = item.get("outdoor", {}) or {}
    wnd  = item.get("wind", {}) or {}
    prs  = item.get("pressure", {}) or {}
    rain = item.get("rainfall", {}) or {}

    # temperatura/umidità
    t_v, t_u = val_and_unit(first(out, ["temperature","temp_c","temp"]))
    h_v, _   = val_and_unit(first(out, ["humidity","hum"]))
    temp_c   = c_from(t_v, t_u)

    # pressione
    pnode = first(prs, ["rel","relative","relative_hpa","rel_hpa","abs_hpa","abs"])
    p_v, p_u = val_and_unit(pnode)
    p_hpa = hpa_from(p_v, p_u)

    # vento
    wspd_node = (
        first(wnd, ["speed","avg","windspeed","avg_mps","speed_mps","ws","wspd"])
        or wnd.get("wind_speed") or wnd.get("speed_kmh") or wnd.get("wspeed")
    )
    gust_node = (
        first(wnd, ["gust","max","gust_mps","gust_ms"])
        or wnd.get("wind_gust") or wnd.get("gust_kmh")
    )
    wdir_node = first(wnd, ["direction","dir_deg","dir","wdir"])

    w_v, w_u = val_and_unit(wspd_node)
    g_v, g_u = val_and_unit(gust_node)
    d_v, _   = val_and_unit(wdir_node)

    wind_kmh  = kmh_from(w_v, w_u)
    gust_kmh  = kmh_from(g_v, g_u)

    # pioggia (tasso o aggregato breve)
    rnode = first(rain, ["rate","rain_rate","rainrate_mm","rainrate","rainrate_in","rain_last_10min","rain_last_1h"])
    r_v, r_u = val_and_unit(rnode)
    if r_u and r_u.lower() in ("in","inch","inches"):
        rain_mm = r_v * 25.4 if r_v is not None else None
    else:
        rain_mm = r_v

    return {
        "time": pd.Timestamp(ts).tz_convert("UTC").isoformat(),
        "temp_c": temp_c,
        "humidity": safe_float(h_v),
        "pressure_hpa": p_hpa,
        "wind_kmh": wind_kmh,
        "windgust_kmh": gust_kmh,
        "winddir": safe_float(d_v),
        "rain_mm": safe_float(rain_mm),
    }

def parse_payload(j: Dict[str, Any]) -> pd.DataFrame:
    data = j.get("data") if isinstance(j, dict) else None
    if not data:
        return pd.DataFrame()
    items = data.get("list") if isinstance(data.get("list"), list) and data.get("list") else [data]
    rows: List[Dict[str, Any]] = []
    for it in items:
        try:
            rec = parse_item(it)
            if rec:
                rows.append(rec)
        except Exception as e:
            if LOG_LEVEL == "DEBUG":
                log.debug("skip item: %s", e)
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows).drop_duplicates(subset=["time"]).sort_values("time")
    return df

# -------------------- Upsert & aggregazione --------------------
def _chunked(records: List[Dict[str, Any]], chunk_size: int = 5000):
    for i in range(0, len(records), chunk_size):
        yield records[i:i+chunk_size]

def upsert_raw(df: pd.DataFrame) -> int:
    """Upsert bulk (executemany) su station_raw."""
    if df is None or df.empty:
        return 0
    keep = ["time","temp_c","humidity","pressure_hpa","wind_kmh","windgust_kmh","winddir","rain_mm"]
    df = df[keep].copy()
    # normalizza timestamp in ISO UTC (string) per compatibilità sqlite/postgres
    df["time"] = pd.to_datetime(df["time"], utc=True, errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    df = df.dropna(subset=["time"])
    records = df.to_dict("records")
    if not records:
        return 0

    stmt = text("""
        INSERT INTO station_raw (time, temp_c, humidity, pressure_hpa, wind_kmh, windgust_kmh, winddir, rain_mm)
        VALUES (:time, :temp_c, :humidity, :pressure_hpa, :wind_kmh, :windgust_kmh, :winddir, :rain_mm)
        ON CONFLICT (time) DO UPDATE SET
          temp_c=excluded.temp_c,
          humidity=excluded.humidity,
          pressure_hpa=excluded.pressure_hpa,
          wind_kmh=excluded.wind_kmh,
          windgust_kmh=excluded.windgust_kmh,
          winddir=excluded.winddir,
          rain_mm=excluded.rain_mm;
    """)
    with engine().begin() as con:
        for chunk in _chunked(records, chunk_size=5000):
            con.execute(stmt, chunk)
    return len(records)

def recompute_3h(window_start_utc: Optional[pd.Timestamp] = None, lookback_hours: int = 96) -> int:
    """Ricalcolo 3h *incrementale* (upsert solo dei bucket toccati).

    - Se window_start_utc è None: usa now-lookback_hours.
    - Legge station_raw nell'intervallo [t0, now] e upserta station_3h.
    """
    now = pd.Timestamp.utcnow().tz_localize("UTC")
    if window_start_utc is None:
        t0 = now - pd.Timedelta(hours=lookback_hours)
    else:
        t0 = pd.to_datetime(window_start_utc, utc=True, errors="coerce")
        if pd.isna(t0):
            t0 = now - pd.Timedelta(hours=lookback_hours)

    with engine().begin() as con:
        df = pd.read_sql(
            text("SELECT * FROM station_raw WHERE time >= :t0 ORDER BY time"),
            con,
            params={"t0": t0.strftime("%Y-%m-%dT%H:%M:%SZ")}
        )

    if df.empty:
        return 0

    # Column canonicalization (case-insensitive)
    cols = {c.lower(): c for c in df.columns}
    def col(name): return cols.get(name)
    time_col = col("time") or col("ts_utc") or col("timestamp")
    if not time_col:
        return 0

    df[time_col] = pd.to_datetime(df[time_col], utc=True, errors="coerce")
    df = df.dropna(subset=[time_col]).sort_values(time_col)

    # allineo t0 al bordo 3H per aggiornare bucket completo
    t0_aligned = df[time_col].min().floor("3H")
    df = df[df[time_col] >= t0_aligned]

    # rename to canonical expected names
    rename_map = {}
    for k in ["temp_c","humidity","pressure_hpa","wind_kmh","windgust_kmh","rain_mm"]:
        if col(k):
            rename_map[col(k)] = k
    df = df.rename(columns=rename_map)

    agg = (df.set_index(time_col)
             .resample("3H")
             .agg({
                 "temp_c":"mean",
                 "humidity":"mean",
                 "pressure_hpa":"mean",
                 "wind_kmh":"mean",
                 "windgust_kmh":"max",
                 "rain_mm":"sum"
             })
             .reset_index()
          )
    agg = agg.rename(columns={time_col: "time"})
    agg["time"] = pd.to_datetime(agg["time"], utc=True, errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    records = agg.to_dict("records")
    if not records:
        return 0

    stmt = text("""
        INSERT INTO station_3h (time, temp_c, humidity, pressure_hpa, wind_kmh, windgust_kmh, rain_mm)
        VALUES (:time, :temp_c, :humidity, :pressure_hpa, :wind_kmh, :windgust_kmh, :rain_mm)
        ON CONFLICT (time) DO UPDATE SET
          temp_c=excluded.temp_c,
          humidity=excluded.humidity,
          pressure_hpa=excluded.pressure_hpa,
          wind_kmh=excluded.wind_kmh,
          windgust_kmh=excluded.windgust_kmh,
          rain_mm=excluded.rain_mm;
    """)
    with engine().begin() as con:
        for chunk in _chunked(records, chunk_size=2000):
            con.execute(stmt, chunk)
    return len(records)

def touch_last_ingest():
    with engine().begin() as con:
        con.execute(text("""
            INSERT INTO meta (k, v) VALUES ('last_ingest', :v)
            ON CONFLICT (k) DO UPDATE SET v=excluded.v
        """), {"v": pd.Timestamp.utcnow().isoformat()})

# -------------------- Main --------------------
def main():
    if not APP_KEY or not API_KEY or not MAC:
        log.error("Chiavi Ecowitt mancanti (Actions/Render/locale): ECOWITT_APP_KEY/ECOWITT_APPLICATION_KEY, ECOWITT_API_KEY, ECOWITT_MAC")
        sys.exit(2)

    ensure_schema()

    window_start_utc: Optional[pd.Timestamp] = None

    # Realtime
    try:
        rt = ecowitt_get("device/real_time", {"mac": MAC, "call_back": "outdoor,wind,pressure,rainfall"})
        df_rt = parse_payload(rt)
        if not df_rt.empty:
            upsert_raw(df_rt.tail(1))
            try:
                tmin = pd.to_datetime(df_rt['time'].min(), utc=True, errors='coerce')
                if not pd.isna(tmin):
                    window_start_utc = tmin if window_start_utc is None else min(window_start_utc, tmin)
            except Exception:
                pass
            last = df_rt.iloc[-1].to_dict()
            log.info(
                "REALTIME: T=%.2f°C H=%.0f%% P=%.1f hPa V=%.2f km/h G=%s dir=%s",
                (last.get("temp_c") or float("nan")),
                (last.get("humidity") or float("nan")),
                (last.get("pressure_hpa") or float("nan")),
                (last.get("wind_kmh") or 0.0),
                str(last.get("windgust_kmh")),
                str(last.get("winddir")),
            )
        else:
            log.warning("Realtime vuoto.")
    except Exception as e:
        log.warning("Realtime error: %s", e)

    # Backfill (giorno per giorno)
    if BACKFILL_HOURS > 0:
        now = datetime.now(timezone.utc)
        start = now - timedelta(hours=BACKFILL_HOURS)
        day = start.replace(hour=0, minute=0, second=0, microsecond=0)
        while day <= now:
            try:
                hist = ecowitt_get(
                    "device/history",
                    {
                        "mac": MAC,
                        "start_date": day.strftime("%Y-%m-%d 00:00:00"),
                        "end_date": min(now, day + timedelta(days=1) - timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S"),
                        "call_back": "outdoor,wind,pressure,rainfall",
                    },
                )
                df_h = parse_payload(hist)
                cnt = upsert_raw(df_h)
                try:
                    tmin = pd.to_datetime(df_h['time'].min(), utc=True, errors='coerce')
                    if not pd.isna(tmin):
                        window_start_utc = tmin if window_start_utc is None else min(window_start_utc, tmin)
                except Exception:
                    pass
                if not df_h.empty:
                    last = df_h.iloc[-1].to_dict()
                    log.info(
                        "HISTORY %s: rows=%s ultimo T=%.2f°C P=%.1f hPa V=%.2f km/h",
                        day.date(), cnt,
                        (last.get("temp_c") or float("nan")),
                        (last.get("pressure_hpa") or float("nan")),
                        (last.get("wind_kmh") or 0.0),
                    )
            except Exception as e:
                log.warning("History %s error: %s", day.date(), e)
            day += timedelta(days=1)

    # Ricostruisci 3h
    try:
        n3 = recompute_3h(window_start_utc=window_start_utc, lookback_hours=max(96, BACKFILL_HOURS))
        log.info("station_3h ricostruita: %s bucket", n3)
    except Exception as e:
        log.warning("Recompute 3h error: %s", e)

    touch_last_ingest()
    log.info("Done.")

if __name__ == "__main__":
    main()
