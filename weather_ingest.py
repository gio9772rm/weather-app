import os
from datetime import datetime, timezone
import pandas as pd
import requests
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# ---------- ENV ----------
load_dotenv()
SQLITE_PATH = os.getenv("SQLITE_PATH", "./data/weather.db")
DATABASE_URL = os.getenv("DATABASE_URL")  # <- usato su Render (Postgres)
OWM_API_KEY = os.getenv("OWM_API_KEY", "")
LAT = os.getenv("LAT", "")
LON = os.getenv("LON", "")
EC_APP = os.getenv("ECOWITT_APPLICATION_KEY", "")
EC_KEY = os.getenv("ECOWITT_API_KEY", "")
EC_MAC = os.getenv("ECOWITT_MAC", "")

# ---------- DB ----------
def make_engine():
    url = DATABASE_URL if DATABASE_URL else f"sqlite:///{SQLITE_PATH}"
    return create_engine(url, future=True)

engine = make_engine()
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
CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT);
"""
with engine.begin() as conn:
    for stmt in SCHEMA.split(";"):
        s = stmt.strip()
        if s:
            conn.execute(text(s))

def upsert_table(df, table, pk="Time"):
    if df is None or df.empty:
        return 0
    n = 0
    with engine.begin() as conn:
        placeholders = ",".join([":" + c for c in df.columns])
        cols = ",".join(df.columns)
        update = ",".join([f"{c}=excluded.{c}" for c in df.columns if c != pk])
        sql = text(f"INSERT INTO {table} ({cols}) VALUES ({placeholders}) "
                   f"ON CONFLICT({pk}) DO UPDATE SET {update}")
        for _, row in df.iterrows():
            conn.execute(sql, row.to_dict())
            n += 1
    return n

# ---------- Ecowitt helpers ----------
ECO_BASE = "https://api.ecowitt.net/api/v3"

def _pick_value(x):
    """Accetta numero, stringa numerica o dict {time, unit, value} → (float|None, unit|None)"""
    if isinstance(x, dict):
        v = x.get("value"); u = x.get("unit")
        try:
            return (float(v) if v not in (None, "") else None), u
        except Exception:
            return None, u
    try:
        return (float(x) if x not in (None, "") else None), None
    except Exception:
        return None, None

def _to_c(val, unit):
    if val is None: return None
    u = (unit or "").lower()
    if "f" in u:  # °F
        return (val - 32.0) * 5.0/9.0
    return val

def _to_hpa(val, unit):
    if val is None: return None
    u = (unit or "").lower()
    if "inhg" in u: return val * 33.8639
    if "pa" in u and val > 2000: return val / 100.0
    return val

def _wind_to_kmh(val, unit):
    if val is None: return None
    u = (unit or "").lower()
    if "m/s" in u or "mps" in u: return val * 3.6
    if "mph" in u: return val * 1.60934
    return val  # km/h o sconosciuto

def _rain_to_mm(val, unit):
    if val is None: return None
    u = (unit or "").lower()
    if "in" in u: return val * 25.4
    return val

def _ensure_utc(ts_like):
    """Ritorna Timestamp timezone-aware UTC, senza usare tz_localize su aware."""
    t = None
    if ts_like is not None:
        try:
            t = pd.to_datetime(ts_like, utc=True)
        except Exception:
            t = None
    if t is None or pd.isna(t):
        t = pd.Timestamp.now(tz="UTC")
    else:
        if t.tzinfo is None:
            t = t.tz_localize("UTC")
        else:
            t = t.tz_convert("UTC")
    return t

def ecowitt_realtime(app, key, mac):
    if not (app and key and mac):
        return pd.DataFrame()
    r = requests.get(f"{ECO_BASE}/device/real_time", params={
        "application_key": app, "api_key": key, "mac": mac,
        "call_back": "outdoor,wind,pressure,rainfall"
    }, timeout=30)
    r.raise_for_status()
    j = r.json()
    data = j.get("data")
    d = (data[0] if isinstance(data, list) and data else data) or {}
    if not d: return pd.DataFrame()

    # time (bucket 3h)
    t = _ensure_utc(d.get("time"))
    t_bucket = t.floor("3h")      # <-- fix futurewarning: 'h' minuscola
    t_iso = t_bucket.strftime("%Y-%m-%dT%H:%M:%SZ")

    outdoor = d.get("outdoor") or {}
    pressure = d.get("pressure") or {}
    wind = d.get("wind") or {}
    rainfall = d.get("rainfall") or {}

    temp_v, temp_u = _pick_value(outdoor.get("temperature"))
    hum_v, _ = _pick_value(outdoor.get("humidity"))
    p_v, p_u = _pick_value(pressure.get("relative"))
    w_v, w_u = _pick_value(wind.get("speed_avg"))
    r3_v, r3_u = _pick_value(rainfall.get("rain_3h"))
    r1_v, r1_u = _pick_value(rainfall.get("rain_1h"))

    out = {
        "Time": t_iso,  # PK per fascia 3h
        "Temp_C": _to_c(temp_v, temp_u),
        "Humidity": hum_v,
        "Pressure_hPa": _to_hpa(p_v, p_u),
        "Wind_kmh": (_wind_to_kmh(w_v, w_u) or 0.0),
        "WindGust_kmh": None,
        "Rain_mm": (_rain_to_mm(r3_v, r3_u) if r3_v is not None else _rain_to_mm(r1_v, r1_u)),
    }
    return pd.DataFrame([out])

def ecowitt_history_3h(app, key, mac, days=7):
    """Scarica blocchi da 24h con cycle=30min e aggrega a 3h (ultimi N giorni)."""
    if not (app and key and mac):
        return pd.DataFrame()
    frames = []
    now = pd.Timestamp.now(tz="UTC")  # <-- fix tz
    for i in range(days):
        end = now - pd.Timedelta(days=i)
        start = end - pd.Timedelta(days=1)
        start_str = start.strftime("%Y-%m-%d %H:%M:%S")
        end_str = end.strftime("%Y-%m-%d %H:%M:%S")
        r = requests.get(f"{ECO_BASE}/device/history", params={
            "application_key": app, "api_key": key, "mac": mac,
            "start_date": start_str,
            "end_date": end_str,
            "call_back": "outdoor,wind,pressure,rainfall",
            "cycle_type": "30min"
        }, timeout=30)
        r.raise_for_status()
        j = r.json()
        data = j.get("data")

        if isinstance(data, dict) and "time" in data:
            df = pd.DataFrame({"Time": pd.to_datetime(data["time"], utc=True)})
            def put(key, col, conv=None):
                if key in data:
                    s = pd.Series(data[key])
                    if conv:
                        s = s.apply(lambda x: conv(*_pick_value(x)))
                    df[col] = pd.to_numeric(s, errors="coerce")
            put("outdoor.temperature", "Temp_C", _to_c)
            put("outdoor.humidity", "Humidity", lambda v,u: v)
            put("pressure.relative", "Pressure_hPa", _to_hpa)
            put("wind.speed_avg", "Wind_kmh", _wind_to_kmh)
            # pioggia
            if "rainfall.rain_3h" in data:
                df["Rain_mm"] = pd.to_numeric(pd.Series(data["rainfall.rain_3h"]).apply(lambda x: _rain_to_mm(*_pick_value(x))), errors="coerce")
            elif "rainfall.rain_1h" in data:
                df["Rain_mm"] = pd.to_numeric(pd.Series(data["rainfall.rain_1h"]).apply(lambda x: _rain_to_mm(*_pick_value(x))), errors="coerce")
            elif "rainfall.daily" in data:
                df["Rain_mm"] = pd.to_numeric(pd.Series(data["rainfall.daily"]).apply(lambda x: _rain_to_mm(*_pick_value(x))), errors="coerce")
        elif isinstance(data, list):
            rows = []
            for d in data:
                t = _ensure_utc(d.get("time"))
                outdoor = d.get("outdoor") or {}
                pressure = d.get("pressure") or {}
                wind = d.get("wind") or {}
                rainfall = d.get("rainfall") or {}
                tv, tu = _pick_value(outdoor.get("temperature"))
                hv, _ = _pick_value(outdoor.get("humidity"))
                pv, pu = _pick_value(pressure.get("relative"))
                wv, wu = _pick_value(wind.get("speed_avg"))
                r3, ru3 = _pick_value(rainfall.get("rain_3h"))
                r1, ru1 = _pick_value(rainfall.get("rain_1h"))
                rows.append({
                    "Time": t,
                    "Temp_C": _to_c(tv, tu),
                    "Humidity": hv,
                    "Pressure_hPa": _to_hpa(pv, pu),
                    "Wind_kmh": _wind_to_kmh(wv, wu),
                    "Rain_mm": _rain_to_mm(r3, ru3) if r3 is not None else _rain_to_mm(r1, ru1),
                })
            df = pd.DataFrame(rows)
        else:
            df = pd.DataFrame()

        if df.empty:
            continue

        df = df.dropna(subset=["Time"]).sort_values("Time").drop_duplicates("Time")
        df3h = (df.set_index("Time")
                  .resample("3h")   # <-- fix futurewarning: 'h' minuscola
                  .agg({"Temp_C":"mean","Humidity":"mean","Pressure_hPa":"mean",
                        "Wind_kmh":"mean","Rain_mm":"sum"})
                  .reset_index())
        df3h["Time"] = pd.to_datetime(df3h["Time"]).dt.tz_convert("UTC").dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        df3h["WindGust_kmh"] = None
        df3h = df3h[["Time","Temp_C","Humidity","Pressure_hPa","Wind_kmh","WindGust_kmh","Rain_mm"]]
        frames.append(df3h)

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

# ---------- OpenWeather (5 giorni) ----------
def fetch_openweather(api_key, lat, lon):
    if not api_key or not lat or not lon:
        return pd.DataFrame()
    r = requests.get("https://api.openweathermap.org/data/2.5/forecast",
                     params={"lat": lat, "lon": lon, "appid": api_key, "units": "metric"},
                     timeout=30)
    r.raise_for_status()
    data = r.json().get("list", [])
    rows = []
    for it in data:
        ts = datetime.fromtimestamp(it["dt"], tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        main = it.get("main", {}); wind = it.get("wind", {}); clouds = it.get("clouds", {})
        rain = it.get("rain", {}); snow = it.get("snow", {})
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

# ---------- Main ----------
def main():
    # 1) Ecowitt history 7d + realtime
    try:
        hist = ecowitt_history_3h(EC_APP, EC_KEY, EC_MAC, days=7)
        upsert_table(hist, "station_3h")
    except Exception as e:
        print("Ecowitt history error:", e)

    try:
        rt = ecowitt_realtime(EC_APP, EC_KEY, EC_MAC)
        upsert_table(rt, "station_3h")
    except Exception as e:
        print("Ecowitt realtime error:", e)

    # 2) OpenWeather 5d
    try:
        fc = fetch_openweather(OWM_API_KEY, LAT, LON)
        upsert_table(fc, "forecast_ow")
    except Exception as e:
        print("OpenWeather error:", e)

    with engine.begin() as conn:
       conn.execute(
           text("""
               INSERT INTO meta (k, v) VALUES ('last_ingest', :v)
               ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v
           """),
           {"v": datetime.now(timezone.utc).isoformat()}
       )


if __name__ == "__main__":
    main()
