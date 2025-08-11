import os, threading
from datetime import datetime, timezone
from urllib.parse import unquote_plus
from flask import Flask, request, jsonify
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
SQLITE_PATH = os.getenv("SQLITE_PATH", "./data/weather.db")
PORT = int(os.getenv("RECEIVER_PORT", "8080"))

app = Flask(__name__)
engine = create_engine(f"sqlite:///{SQLITE_PATH}", future=True)

SCHEMA = """
CREATE TABLE IF NOT EXISTS station_raw (
  ts_utc TEXT PRIMARY KEY,
  temp_c REAL, hum REAL, press_hpa REAL,
  wind_ms REAL, winddir REAL, rain_mm REAL
);
CREATE TABLE IF NOT EXISTS station_3h (
  Time TEXT PRIMARY KEY,
  Temp_C REAL,
  Humidity REAL,
  Pressure_hPa REAL,
  Wind_kmh REAL,
  WindGust_kmh REAL,
  Rain_mm REAL
);
"""
with engine.begin() as conn:
    for stmt in SCHEMA.split(";"):
        s = stmt.strip()
        if s:
            conn.execute(text(s))

def upsert_raw(row):
    with engine.begin() as conn:
        conn.execute(text("""
        INSERT INTO station_raw (ts_utc, temp_c, hum, press_hpa, wind_ms, winddir, rain_mm)
        VALUES (:ts_utc, :temp_c, :hum, :press_hpa, :wind_ms, :winddir, :rain_mm)
        ON CONFLICT(ts_utc) DO UPDATE SET
          temp_c=excluded.temp_c, hum=excluded.hum, press_hpa=excluded.press_hpa,
          wind_ms=excluded.wind_ms, winddir=excluded.winddir, rain_mm=excluded.rain_mm
        """), row)

def aggregate_3h():
    df = pd.read_sql_query(text("SELECT * FROM station_raw"), engine, parse_dates=["ts_utc"])
    if df.empty:
        return 0
    df = df.set_index("ts_utc").sort_index()
    agg = df.resample("3H").agg({
        "temp_c":"mean", "hum":"mean", "press_hpa":"mean",
        "wind_ms":"mean", "rain_mm":"sum"
    }).reset_index()
    agg["Time"] = pd.to_datetime(agg["ts_utc"]).dt.tz_localize("UTC").dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    agg["Wind_kmh"] = agg["wind_ms"] * 3.6
    agg["WindGust_kmh"] = None
    agg = agg[["Time","temp_c","hum","press_hpa","Wind_kmh","WindGust_kmh","rain_mm"]]
    agg.columns = ["Time","Temp_C","Humidity","Pressure_hPa","Wind_kmh","WindGust_kmh","Rain_mm"]
    with engine.begin() as conn:
        for _, r in agg.iterrows():
            conn.execute(text("""
            INSERT INTO station_3h (Time, Temp_C, Humidity, Pressure_hPa, Wind_kmh, WindGust_kmh, Rain_mm)
            VALUES (:Time,:Temp_C,:Humidity,:Pressure_hPa,:Wind_kmh,:WindGust_kmh,:Rain_mm)
            ON CONFLICT(Time) DO UPDATE SET
              Temp_C=excluded.Temp_C, Humidity=excluded.Humidity, Pressure_hPa=excluded.Pressure_hPa,
              Wind_kmh=excluded.Wind_kmh, WindGust_kmh=excluded.WindGust_kmh, Rain_mm=excluded.Rain_mm
            """), r.to_dict())
    return len(agg)

def parse_ecowitt_params(args):
    # Ecowitt "Customized" typically sends GET with many fields; we map the common ones.
    # Fields vary by firmware; we try multiple aliases.
    def getf(*names, cast=float):
        for n in names:
            if n in args:
                try:
                    return cast(args.get(n))
                except Exception:
                    try:
                        return cast(unquote_plus(args.get(n)))
                    except Exception:
                        return None
        return None

    # Time: 'dateutc' like '2025-08-10 14:30:00'
    dateutc = args.get("dateutc") or args.get("time") or args.get("timestamp")
    try:
        ts = pd.to_datetime(dateutc, utc=True)
    except Exception:
        ts = pd.Timestamp.utcnow().tz_localize("UTC")
    # Temperature: metric fields often provided as tempc; fallback from tempf
    temp_c = getf("tempc", "temp_c", "outdoor_temp_c", "temp", cast=float)
    if temp_c is None:
        temp_f = getf("tempf", cast=float)
        if temp_f is not None:
            temp_c = (temp_f - 32.0) * 5.0/9.0
    # Humidity
    hum = getf("humidity", "outdoor_humidity", "hum", cast=float)
    # Pressure (relative)
    press_hpa = getf("baromabsin", "baromrelin", "pressure_hpa", "barometer", cast=float)
    if press_hpa and "baromabsin" in args or "baromrelin" in args:
        # inches of mercury -> hPa
        press_hpa = press_hpa * 33.8639
    # Wind avg
    wind_ms = getf("windspeedms", "windspeed", "wind_ms", cast=float)
    if wind_ms is None:
        wind_kmh = getf("windspeedkmh", "wind_kmh", cast=float)
        if wind_kmh is not None:
            wind_ms = wind_kmh / 3.6
        else:
            wind_mph = getf("windspeedmph", cast=float)
            if wind_mph is not None:
                wind_ms = wind_mph * 0.44704
    # Wind dir
    winddir = getf("winddir", "winddirection", cast=float)
    # Rain (we try rain rate over interval; firmware differs)
    rain_mm = getf("rainrate", "rainmm", "rain_1h", "rain", cast=float)

    return {
        "ts_utc": ts.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ"),
        "temp_c": temp_c,
        "hum": hum,
        "press_hpa": press_hpa,
        "wind_ms": wind_ms,
        "winddir": winddir,
        "rain_mm": rain_mm,
    }

@app.get("/report")
def report():
    row = parse_ecowitt_params(request.args)
    upsert_raw(row)
    # Aggregate lazily to keep it simple
    aggregate_3h()
    return jsonify({"status":"ok","stored":row}), 200

if __name__ == "__main__":
    print(f"Receiver listening on http://0.0.0.0:{PORT}/report")
    app.run(host="0.0.0.0", port=PORT, debug=False)