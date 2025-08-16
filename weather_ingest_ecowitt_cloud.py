# -*- coding: utf-8 -*-
"""
Ecowitt Cloud → SQLite (compatibilità schema legacy)
- Popola station_raw con colonne: ts_utc, temp_c, hum, press_hpa, wind_ms, winddir, rain_mm
- Ricostruisce station_3h (Temp_C, Humidity, Pressure_hPa, Wind_kmh, WindGust_kmh, Rain_mm)
- Normalizza pressione in hPa e vento in km/h/m/s (wind_ms)
- Log espliciti degli ultimi valori letti (per debug vento/pressione)
Env richiesti: ECOWITT_APP_KEY, ECOWITT_API_KEY, ECOWITT_MAC, SQLITE_PATH (o DATABASE_URL)
Opzionali: BACKFILL_HOURS (es. 24), LOG_LEVEL=DEBUG
"""

import os, sys, logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Any, Optional, List

import requests
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
LOG_LEVEL = (os.getenv("LOG_LEVEL") or "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO),
                    format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("ingest_cloud")

APP_KEY = (os.getenv("ECOWITT_APP_KEY") or os.getenv("ECOWITT_APPLICATION_KEY") or "").strip()
API_KEY = (os.getenv("ECOWITT_API_KEY") or os.getenv("ECOWITT_APP_SECRET") or "").strip()
MAC     = (os.getenv("ECOWITT_MAC") or "").strip().replace("-",":").lower()
BACKFILL_HOURS = int(os.getenv("BACKFILL_HOURS", "0") or "0")

DB_URL  = (os.getenv("DATABASE_URL") or "").strip()
SQLITE  = (os.getenv("SQLITE_PATH") or "./data/weather.db").strip()

if not APP_KEY or not API_KEY or not MAC:
    log.error("Mancano ECOWITT_APP_KEY / ECOWITT_API_KEY / ECOWITT_MAC nel .env")
    sys.exit(2)

def engine():
    if DB_URL:
        return create_engine(DB_URL, future=True, pool_pre_ping=True)
    Path(SQLITE).parent.mkdir(parents=True, exist_ok=True)
    return create_engine(f"sqlite:///{SQLITE}", future=True)

def ensure_schema():
    eng = engine()
    with eng.begin() as con:
        # schema legacy (quello che hai in DB)
        con.execute(text("""
        CREATE TABLE IF NOT EXISTS station_raw (
            ts_utc TEXT PRIMARY KEY,
            temp_c REAL,
            hum REAL,
            press_hpa REAL,
            wind_ms REAL,
            winddir REAL,
            rain_mm REAL
        );
        """))
        con.execute(text("""
        CREATE TABLE IF NOT EXISTS station_3h (
            Time TEXT PRIMARY KEY,
            Temp_C REAL, Humidity REAL, Pressure_hPa REAL,
            Wind_kmh REAL, WindGust_kmh REAL, Rain_mm REAL
        );
        """))
        con.execute(text("CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT)"))

def touch_last_ingest():
    with engine().begin() as con:
        con.execute(text("""
            INSERT INTO meta (k,v) VALUES ('last_ingest', :v)
            ON CONFLICT (k) DO UPDATE SET v=excluded.v
        """), {"v": pd.Timestamp.utcnow().isoformat()})

def to_float(x) -> Optional[float]:
    if x is None: return None
    try:
        return float(str(x).replace(",", "."))
    except Exception:
        return None

def hpa_from(value: Optional[float], unit: Optional[str]) -> Optional[float]:
    if value is None: return None
    u = (unit or "").lower()
    v = float(value)
    if "inhg" in u:   return v * 33.8638866667
    if u == "pa":     return v / 100.0
    if "kpa" in u:    return v * 10.0
    # correzioni comuni senza unit
    if 8000.0 <= v <= 11000.0: return v / 10.0   # hPa*10
    if v > 2000.0:             return v / 100.0  # Pa
    return v

def kmh_from(value: Optional[float], unit: Optional[str]) -> Optional[float]:
    if value is None: return None
    u = (unit or "").lower()
    v = float(value)
    if "m/s" in u or u == "mps": return v * 3.6
    if "mph" in u:               return v * 1.60934
    if "knot" in u or "kt" in u: return v * 1.852
    return v  # assumiamo già km/h

def ecowitt_get(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    url = f"https://api.ecowitt.net/api/v3/{path}"
    p = {"application_key": APP_KEY, "api_key": API_KEY, **params}
    r = requests.get(url, params=p, timeout=25)
    r.raise_for_status()
    return r.json()

def _first(d: dict, keys: List[str]):
    for k in keys:
        if isinstance(d, dict) and k in d:
            return d[k]
    return None

def _val_unit(node: Any):
    """accetta dict {'value':.., 'unit':..} o scalare"""
    if isinstance(node, dict):
        return to_float(_first(node, ["value","val","v"])), (_first(node, ["unit"]) or None)
    return to_float(node), None

def parse_ecowitt_item(item: dict) -> Optional[dict]:
    # timestamp
    t_raw = _first(item, ["time","last_update_time","update_time","date"])
    try:
        ts = pd.to_datetime(t_raw, utc=True).to_pydatetime()
    except Exception:
        ts = datetime.now(timezone.utc)

    outdoor = item.get("outdoor", {}) or {}
    wind    = item.get("wind", {}) or {}
    press   = item.get("pressure", {}) or {}
    rain    = item.get("rainfall", {}) or {}

    # temp / hum
    temp_v, temp_u = _val_unit(_first(outdoor, ["temperature","temp_c","temp"]))

    def c_from(val, unit):
    if val is None: return None
    u = (unit or "").lower()
    if u in ("f","°f","fahrenheit","degf"):
        return (float(val) - 32.0) * 5.0/9.0
    return float(val)
temp_c = c_from(temp_v, temp_u)
...
return {
    "ts_utc": pd.Timestamp(ts).tz_convert("UTC").isoformat(),
    "temp_c": temp_c,
    ...
}
    hum_v,  _      = _val_unit(_first(outdoor, ["humidity"]))

    # pressure (prefer REL)
    pnode = _first(press, ["rel","relative","relative_hpa","rel_hpa","abs_hpa","abs"])
    p_v, p_u = _val_unit(pnode)
    p_hpa = hpa_from(p_v, p_u)

    # wind (supporta molti alias)
    wspd_node = _first(wind, ["speed","avg","windspeed","avg_mps","speed_mps","ws","wspd"])
    gust_node = _first(wind, ["gust","max","gust_mps","gust_ms"])
    wdir_node = _first(wind, ["direction","dir_deg","dir","wdir"])
    wspd_node = wspd_node or wind.get("wind_speed") or wind.get("speed_kmh") or wind.get("wspeed")
    gust_node = gust_node or wind.get("wind_gust") or wind.get("gust_kmh")

    wspd_v, wspd_u = _val_unit(wspd_node)
    gust_v, gust_u = _val_unit(gust_node)
    wdir_v, _      = _val_unit(wdir_node)

    # km/h e m/s
    wind_kmh  = kmh_from(wspd_v, wspd_u)
    gust_kmh  = kmh_from(gust_v, gust_u)
    wind_ms   = (wind_kmh / 3.6) if wind_kmh is not None else None

    # rain (tasso o 3h cumulata, prendiamo tasso istantaneo se presente)
    rain_node = _first(rain, ["rate","rain_rate","rainrate_mm","rainrate","rainrate_in","rain_last_10min","rain_last_1h"])
    rain_v, rain_u = _val_unit(rain_node)
    if rain_u and rain_u.lower() in ("in","inch","inches"):
        rain_mm = rain_v * 25.4 if rain_v is not None else None
    else:
        rain_mm = rain_v

    return {
        "ts_utc": pd.Timestamp(ts).tz_convert("UTC").isoformat(),
        "temp_c": temp_v,
        "hum": hum_v,
        "press_hpa": p_hpa,
        "wind_ms": wind_ms,
        "winddir": wdir_v,
        "rain_mm": rain_mm,
        # extra (solo per log/debug)
        "_wind_kmh": wind_kmh,
        "_gust_kmh": gust_kmh,
        "_raw_units": {"temp": temp_u, "press": p_u, "wspd": wspd_u, "gust": gust_u}
    }

def parse_payload(json_obj: dict) -> pd.DataFrame:
    data = json_obj.get("data") if isinstance(json_obj, dict) else None
    if not data: return pd.DataFrame()
    items = data.get("list") if isinstance(data.get("list"), list) and data.get("list") else [data]
    rows = []
    for it in items:
        try:
            r = parse_ecowitt_item(it)
            if r: rows.append(r)
        except Exception as e:
            log.debug("Skip item: %s", e)
    if not rows: return pd.DataFrame()
    df = pd.DataFrame(rows).drop_duplicates(subset=["ts_utc"]).sort_values("ts_utc")
    return df

def upsert_station_raw(df: pd.DataFrame) -> int:
    if df is None or df.empty: return 0
    keep = ["ts_utc","temp_c","hum","press_hpa","wind_ms","winddir","rain_mm"]
    df = df[keep].copy()
    eng = engine()
    inserted = 0
    with eng.begin() as con:
        for _, row in df.iterrows():
            con.execute(text("""
                INSERT INTO station_raw (ts_utc, temp_c, hum, press_hpa, wind_ms, winddir, rain_mm)
                VALUES (:ts_utc, :temp_c, :hum, :press_hpa, :wind_ms, :winddir, :rain_mm)
                ON CONFLICT (ts_utc) DO UPDATE SET
                  temp_c=excluded.temp_c,
                  hum=excluded.hum,
                  press_hpa=excluded.press_hpa,
                  wind_ms=excluded.wind_ms,
                  winddir=excluded.winddir,
                  rain_mm=excluded.rain_mm;
            """), dict(row))
            inserted += 1
    return inserted

def recompute_station_3h(lookback_hours: int = 96) -> int:
    eng = engine()
    with eng.begin() as con:
        df = pd.read_sql(
            text("SELECT * FROM station_raw WHERE ts_utc >= :t0 ORDER BY ts_utc"),
            con, params={"t0": (pd.Timestamp.utcnow() - pd.Timedelta(hours=lookback_hours)).isoformat()}
        )
    if df.empty: return 0
    df["Time"] = pd.to_datetime(df["ts_utc"], utc=True)
    # derivazioni per grafici compatibili con app
    df["Temp_C"] = pd.to_numeric(df["temp_c"], errors="coerce")
    df["Humidity"] = pd.to_numeric(df["hum"], errors="coerce")
    df["Pressure_hPa"] = pd.to_numeric(df["press_hpa"], errors="coerce")
    df["Wind_kmh"] = pd.to_numeric(df["wind_ms"], errors="coerce") * 3.6
    df["WindGust_kmh"] = df["Wind_kmh"]  # se non c'è gust, usa speed
    df["Rain_mm"] = pd.to_numeric(df["rain_mm"], errors="coerce")

    agg = (df.set_index("Time")
             .resample("3H")
             .agg({"Temp_C":"mean","Humidity":"mean","Pressure_hPa":"mean",
                   "Wind_kmh":"mean","WindGust_kmh":"max","Rain_mm":"sum"})
             .reset_index())
    with eng.begin() as con:
        agg.to_sql("station_3h", con.connection, if_exists="replace", index=False)
    return len(agg)

def log_tail(df: pd.DataFrame, label: str, n: int = 3):
    if df.empty:
        log.info("%s: nessun punto", label); return
    tail = df.tail(n)
    for _, r in tail.iterrows():
        log.info("%s | ts=%s temp=%.2f°C hum=%.0f%% press=%.1f hPa wind=%.2f m/s dir=%s rain=%.1f mm",
                 label,
                 r.get("ts_utc"),
                 (r.get("temp_c") or float('nan')),
                 (r.get("hum") or float('nan')),
                 (r.get("press_hpa") or float('nan')),
                 (r.get("wind_ms") or 0.0),
                 str(r.get("winddir")),
                 (r.get("rain_mm") or 0.0))

def main():
    ensure_schema()
    total = 0

    # 1) ultimo punto realtime
    try:
        rt = ecowitt_get("device/real_time", {
            "mac": MAC,
            "call_back": "outdoor,wind,pressure,rainfall"
        })
        df_rt = parse_payload(rt)
        if not df_rt.empty:
            total += upsert_station_raw(df_rt.tail(1))  # solo l'ultimo
            log_tail(df_rt.tail(1), "REALTIME")
            # debug extra (unità e km/h)
            last = df_rt.tail(1).to_dict(orient="records")[0]
            log.info("DEBUG units: %s", last.get("_raw_units"))
            log.info("DEBUG wind_kmh/gust_kmh: %s / %s", last.get("_wind_kmh"), last.get("_gust_kmh"))
        else:
            log.warning("Realtime vuoto.")
    except Exception as e:
        log.warning("Realtime error: %s", e)

    # 2) backfill (giorno per giorno)
    if BACKFILL_HOURS > 0:
        now = datetime.now(timezone.utc)
        start = now - timedelta(hours=BACKFILL_HOURS)
        day = start.replace(hour=0, minute=0, second=0, microsecond=0)
        while day <= now:
            try:
                hist = ecowitt_get("device/history", {
                    "mac": MAC,
                    "start_date": day.strftime("%Y-%m-%d 00:00:00"),
                    "end_date":   min(now, day+timedelta(days=1)-timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S"),
                    "call_back": "outdoor,wind,pressure,rainfall"
                })
                df_h = parse_payload(hist)
                if not df_h.empty:
                    total += upsert_station_raw(df_h)
                    log_tail(df_h.tail(1), f"HISTORY {day.date()}")
            except Exception as e:
                log.warning("History %s error: %s", day.date(), e)
            day += timedelta(days=1)

    # 3) aggregazione 3h
    try:
        n3 = recompute_station_3h(lookback_hours=96)
        log.info("station_3h ricostruita: %s bucket", n3)
    except Exception as e:
        log.warning("Recompute 3h error: %s", e)

    touch_last_ingest()
    log.info("Completato, upsert=%s", total)

if __name__ == "__main__":
    main()
