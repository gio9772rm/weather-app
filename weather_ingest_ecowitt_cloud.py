# -*- coding: utf-8 -*-
"""
weather_ingest_ecowitt_cloud.py — Ecowitt Cloud → DB
- Normalizza DATABASE_URL (postgres:// → postgresql+psycopg2://, sslmode=require su *.render.com)
- Schema robusto e upsert con ON CONFLICT
- Lock cross‑platform (impedisce corse con altri ingest)
"""

import os, sys, logging, re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import urlsplit, urlunsplit, parse_qs, urlencode
from dotenv import load_dotenv

# --------------------------- Lock cross‑platform ---------------------------
def _lock_path() -> str:
    if os.name == "nt":
        return os.path.join(os.getenv("TEMP", "."), "ingest.lock")
    return "/tmp/ingest.lock"

class FileLock:
    def __init__(self, path=None, stale_seconds: int = 900):
        self.path = path or _lock_path()
        self.stale = int(stale_seconds)
        self._fd = None

    def acquire(self) -> bool:
        flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
        try:
            self._fd = os.open(self.path, flags, 0o644)
            os.write(self._fd, str(int(datetime.now(timezone.utc).timestamp())).encode())
            return True
        except FileExistsError:
            try:
                ts = int(open(self.path, "r").read().strip() or "0")
            except Exception:
                ts = 0
            if ts and (datetime.now(timezone.utc).timestamp() - ts) > self.stale:
                try: os.remove(self.path)
                except Exception: pass
                try:
                    self._fd = os.open(self.path, flags, 0o644)
                    os.write(self._fd, str(int(datetime.now(timezone.utc).timestamp())).encode())
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

# --------------------------- Config & logging ---------------------------
load_dotenv()

LOG_LEVEL = os.getenv("LOG_LEVEL","INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO),
                    format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("ingest_ecowitt")

def normalize_db_url(raw: str) -> str:
    u = (raw or "").strip()
    if not u:
        return u
    if u.startswith("postgres://"):
        u = "postgresql+psycopg2://" + u[len("postgres://"):]
    try:
        sp = urlsplit(u)
        q = parse_qs(sp.query)
        if sp.hostname and "render.com" in sp.hostname.lower():
            if not any(k.lower() == "sslmode" for k in q.keys()):
                q["sslmode"] = ["require"]
                u = urlunsplit((sp.scheme, sp.netloc, sp.path, urlencode(q, doseq=True), sp.fragment))
    except Exception:
        pass
    return u

DB_URL_RAW = os.getenv("DATABASE_URL","").strip()
DB_URL = normalize_db_url(DB_URL_RAW)
SQLITE_PATH = os.getenv("SQLITE_PATH","./data/weather.db").strip()

APP_KEY = (os.getenv("ECOWITT_APP_KEY") or os.getenv("ECOWITT_APPLICATION_KEY") or os.getenv("ECOWITT_APP_ID") or "").strip()
API_KEY = (os.getenv("ECOWITT_API_KEY") or os.getenv("ECOWITT_APP_SECRET") or os.getenv("ECOWITT_API_TOKEN") or "").strip()
MAC     = (os.getenv("ECOWITT_MAC") or os.getenv("ECOWITT_DEVICE_MAC") or os.getenv("STATION_MAC") or "").strip().replace("-",":").lower()
TZ_NAME = os.getenv("ECOWITT_TZ","UTC").strip() or "UTC"
BACKFILL_HOURS = int(os.getenv("BACKFILL_HOURS","0") or "0")

if not APP_KEY or not API_KEY or not MAC:
    log.error("Missing ECOWITT_* keys (APP_KEY/API_KEY/MAC). Set them in env.")
    sys.exit(2)

# --------------------------- DB helpers ---------------------------
def get_engine():
    if DB_URL:
        return create_engine(DB_URL, future=True, pool_pre_ping=True)
    p = Path(SQLITE_PATH); p.parent.mkdir(parents=True, exist_ok=True)
    return create_engine(f"sqlite:///{p}", future=True)

def mask_url(u: str) -> str:
    return re.sub(r"://([^:]+):([^@]+)@", r"://\1:***@", u)

def test_db_connectivity():
    try:
        eng = get_engine()
        with eng.connect() as cx:
            cx.execute(text("select 1"))
        log.info("DB connectivity OK: %s", mask_url(DB_URL or SQLITE_PATH))
        return True
    except Exception as e:
        where = DB_URL and mask_url(DB_URL) or SQLITE_PATH
        log.error("DB connectivity FAILED to %s -> %s", where, e)
        return False

def ensure_schema():
    eng = get_engine()
    with eng.begin() as cx:
        cx.execute(text("""
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
        """))
        cx.execute(text("""
        CREATE TABLE IF NOT EXISTS meta (
          k TEXT PRIMARY KEY,
          v TEXT
        );
        """))

def touch_last_ingest():
    now = pd.Timestamp.now(tz="UTC").isoformat()
    eng = get_engine()
    with eng.begin() as cx:
        cx.execute(text("""
            INSERT INTO meta (k,v) VALUES ('last_ingest', :v)
            ON CONFLICT (k) DO UPDATE SET v=excluded.v;
        """), {"v": now})

# --------------------------- Parse helpers ---------------------------
def _to_float(x):
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    if isinstance(x, str):
        s = x.strip().replace(",", ".")
        try:
            return float(s)
        except Exception:
            return None
    if isinstance(x, dict):
        for k in ("value","val","v","avg","mean","rel","relative","abs","hpa","mm"):
            if k in x:
                y = _to_float(x[k])
                if y is not None:
                    return y
        for v in x.values():
            y = _to_float(v); 
            if y is not None: 
                return y
    return None

def _val_unit(x):
    unit = None
    if isinstance(x, dict):
        if "value" in x:
            val = _to_float(x.get("value"))
            unit = x.get("unit"); unit = unit if isinstance(unit, str) else None
            return val, unit
        for k in ("val","v","avg","mean","rel","relative","abs"):
            if k in x:
                val = _to_float(x.get(k)); unit = x.get("unit")
                unit = unit if isinstance(unit, str) else None
                return val, unit
        return _to_float(x), None
    return _to_float(x), None

def _c_from_f(v): return (v - 32.0) * (5.0/9.0)

def _kmh_from(v, unit_hint=None):
    if v is None: return None
    if unit_hint:
        u = unit_hint.strip().lower()
        if "m/s" in u or u == "mps": return v*3.6
        if "mph" in u: return v*1.60934
        if "knot" in u or "kt" in u: return v*1.852
    return v

def _hpa_from(val, unit_hint=None):
    if val is None: return None
    if unit_hint:
        u = unit_hint.strip().lower()
        if "inhg" in u: return val * 33.8638866667
        if u in ("pa",): return val / 100.0
        if "mb" in u or "hpa" in u: return val
    if val > 2000: return val / 100.0
    return val

def _mm_from(val, unit_hint=None):
    if val is None: return None
    if unit_hint:
        u = unit_hint.strip().lower()
        if u in ("in","inch","inches"): return val * 25.4
    return val

def _c_from(val, unit_hint=None):
    if val is None: return None
    if unit_hint:
        u = unit_hint.strip().lower()
        if u in ("f","°f","degf","fahrenheit"): return _c_from_f(val)
    return val

def _to_utc(ts):
    if isinstance(ts, (int, float)):
        return datetime.fromtimestamp(int(ts), tz=timezone.utc)
    if isinstance(ts, str):
        try:
            return pd.to_datetime(ts, utc=True).to_pydatetime()
        except Exception:
            return datetime.now(timezone.utc)
    if isinstance(ts, datetime):
        return ts.astimezone(timezone.utc) if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc)

def _parse_common(item):
    t_raw = item.get("time") or item.get("last_update_time") or item.get("update_time") or item.get("date")
    t = _to_utc(t_raw)

    outdoor = item.get("outdoor", {}) or {}
    wind    = item.get("wind", {}) or {}
    rain    = item.get("rainfall", {}) or {}
    press   = item.get("pressure", {}) or {}

    temp_v, temp_u = _val_unit(outdoor.get("temperature") or outdoor.get("temp_c") or outdoor.get("temp"))
    temp_c = _c_from(temp_v, temp_u)

    hum_v, hum_u = _val_unit(outdoor.get("humidity"))
    hum = _to_float(hum_v)

    p_val, p_unit = _val_unit(press.get("rel") or press.get("relative") or press.get("rel_hpa") or press.get("relative_hpa") or press.get("abs_hpa") or press.get("abs"))
    p_hpa = _hpa_from(_to_float(p_val), p_unit)

    wspd_v, wspd_u = _val_unit(wind.get("speed") or wind.get("windspeed") or wind.get("avg") or wind.get("avg_mps"))
    wgst_v, wgst_u = _val_unit(wind.get("gust") or wind.get("max") or wind.get("gust_mps"))
    wdir_v, _      = _val_unit(wind.get("direction") or wind.get("dir_deg") or wind.get("dir"))
    wspd = _kmh_from(_to_float(wspd_v), wspd_u)
    wgst = _kmh_from(_to_float(wgst_v), wgst_u)
    wdir = _to_float(wdir_v)

    rain_v, rain_u = _val_unit(rain.get("rate") or rain.get("rain_rate") or rain.get("rainrate_mm") or rain.get("rainrate"))
    if rain_v is None and isinstance(rain.get("rainrate_in"), (int,float,str,dict)):
        rr_v, rr_u = _val_unit(rain.get("rainrate_in"))
        rain_v = _mm_from(rr_v, rr_u or "in")
        rain_u = "mm"
    rain_mm = _mm_from(_to_float(rain_v), rain_u)

    return dict(
        Time=pd.Timestamp(t).tz_convert("UTC"),
        Temp_C=temp_c,
        Humidity=hum,
        Pressure_hPa=p_hpa,
        Wind_kmh=wspd,
        WindGust_kmh=wgst,
        WindDir=wdir,
        Rain_mm=rain_mm,
    )

def parse_generic(json_obj):
    data = json_obj.get("data") if isinstance(json_obj, dict) else None
    if not data:
        return pd.DataFrame()
    iterable = data.get("list") if isinstance(data, dict) and isinstance(data.get("list"), list) and data.get("list") else [data]
    out = []
    for item in iterable:
        try:
            out.append(_parse_common(item))
        except Exception as e:
            if LOG_LEVEL == "DEBUG":
                log.debug("Skip item: %s", e)
            continue
    if not out:
        return pd.DataFrame()
    df = pd.DataFrame(out).drop_duplicates(subset=["Time"]).sort_values("Time")
    return df

def upsert(df):
    if df is None or df.empty: return 0
    df = df.copy(); df["Time"] = pd.to_datetime(df["Time"], utc=True)
    ins = 0
    eng = get_engine()
    with eng.begin() as cx:
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
            ins += 1
    return ins

# --------------------------- Ecowitt API ---------------------------
def ecowitt_get(path, params):
    url = f"https://api.ecowitt.net/api/v3/{path}"
    p = {"application_key": APP_KEY, "api_key": API_KEY, **params}
    r = requests.get(url, params=p, timeout=25)
    r.raise_for_status()
    return r.json()

# --------------------------- main ---------------------------
def main():
    lock = FileLock()
    if not lock.acquire():
        log.warning("Ingest già in corso, esco.")
        sys.exit(1)
    try:
        if not test_db_connectivity():
            log.error("DB non raggiungibile: %s", (DB_URL or SQLITE_PATH))
            sys.exit(3)

        ensure_schema()
        total = 0

        # Realtime
        try:
            rt = ecowitt_get("device/real_time", {"mac": MAC, "call_back": "all"})
            df_rt = parse_generic(rt)
            total += upsert(df_rt.tail(1))
            log.info("Realtime: %s punti (inserito l'ultimo)", len(df_rt))
        except Exception as e:
            log.warning("Realtime fetch failed: %s", e)

        # Backfill opzionale (giorno per giorno)
        if BACKFILL_HOURS > 0:
            now = datetime.now(timezone.utc)
            start = now - timedelta(hours=BACKFILL_HOURS)
            day = start
            while day < now:
                day_end = min(datetime(day.year, day.month, day.day, 23, 59, 59, tzinfo=timezone.utc), now)
                try:
                    hist = ecowitt_get("device/history", {
                        "mac": MAC,
                        "start_date": day.strftime("%Y-%m-%d 00:00:00"),
                        "end_date": day_end.strftime("%Y-%m-%d %H:%M:%S"),
                        "call_back": "outdoor,wind,pressure,rainfall"
                    })
                    df_h = parse_generic(hist)
                    total += upsert(df_h)
                    log.info("History %s: %s punti", day.date(), len(df_h))
                except Exception as e:
                    log.warning("History fetch failed %s: %s", day.date(), e)
                day = (day + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

        try:
            touch_last_ingest()
        except Exception as e:
            log.warning("Failed updating meta.last_ingest: %s", e)
        log.info("Done. Upserted rows: %s", total)
    finally:
        lock.release()

if __name__ == "__main__":
    main()
