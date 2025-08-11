import requests
import pandas as pd
from datetime import datetime

ECO_BASE = "https://api.ecowitt.net/api/v3"

def _req(endpoint, params):
    r = requests.get(f"{ECO_BASE}/{endpoint}", params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def get_real_time(application_key, api_key, mac, call_back="outdoor,wind,pressure,rainfall"):
    return _req("device/real_time", {
        "application_key": application_key,
        "api_key": api_key,
        "mac": mac,
        "call_back": call_back
    })

def get_history(application_key, api_key, mac, start, end, call_back="outdoor,wind,pressure,rainfall", cycle_type=None):
    if isinstance(start, datetime):
        start = start.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(end, datetime):
        end = end.strftime("%Y-%m-%d %H:%M:%S")
    params = {
        "application_key": application_key,
        "api_key": api_key,
        "mac": mac,
        "start_date": start,
        "end_date": end,
        "call_back": call_back
    }
    if cycle_type:
        params["cycle_type"] = cycle_type
    return _req("device/history", params)

def _first_data(payload):
    """Ecowitt sometimes returns {"data": {...}} or {"data": [ {...}, ... ]}"""
    data = payload.get("data")
    if isinstance(data, list):
        return (data[0] if data else {})
    elif isinstance(data, dict):
        return data
    return {}

def real_time_to_df(payload):
    d = _first_data(payload)
    if not d:
        return pd.DataFrame()
    # time can be iso string
    t = pd.to_datetime(d.get("time"))
    # fields
    outdoor = d.get("outdoor", {}) or {}
    wind = d.get("wind", {}) or {}
    pressure = d.get("pressure", {}) or {}
    rainfall = d.get("rainfall", {}) or {}
    row = {
        "Time": t,
        "Temp_C": outdoor.get("temperature"),
        "Humidity": outdoor.get("humidity"),
        "Pressure_hPa": pressure.get("relative"),
        "Wind_mps": wind.get("speed_avg"),
        "WindDir": wind.get("direction"),
        # prefer 3h, then 1h, then daily
        "Rain_mm": rainfall.get("rain_3h") or rainfall.get("rain_1h") or rainfall.get("daily")
    }
    df = pd.DataFrame([row]).dropna(subset=["Time"])
    return df

def history_to_df(payload):
    data = payload.get("data")
    # Case 1: dict of arrays
    if isinstance(data, dict) and "time" in data:
        df = pd.DataFrame({"Time": pd.to_datetime(data["time"])})
        def put(src, col):
            if src in data:
                df[col] = data[src]
        put("outdoor.temperature", "Temp_C")
        put("outdoor.humidity", "Humidity")
        put("pressure.relative", "Pressure_hPa")
        put("wind.speed_avg", "Wind_mps")
        put("wind.direction", "WindDir")
        for k in ["rainfall.rain_3h", "rainfall.rain_1h", "rainfall.daily"]:
            if k in data:
                df["Rain_mm"] = data[k]
                break
        return df
    # Case 2: list of dict rows (each with time + sub-objects)
    if isinstance(data, list):
        rows = []
        for d in data:
            t = pd.to_datetime(d.get("time"))
            outdoor = d.get("outdoor", {}) or {}
            wind = d.get("wind", {}) or {}
            pressure = d.get("pressure", {}) or {}
            rainfall = d.get("rainfall", {}) or {}
            rows.append({
                "Time": t,
                "Temp_C": outdoor.get("temperature"),
                "Humidity": outdoor.get("humidity"),
                "Pressure_hPa": pressure.get("relative"),
                "Wind_mps": wind.get("speed_avg"),
                "WindDir": wind.get("direction"),
                "Rain_mm": rainfall.get("rain_3h") or rainfall.get("rain_1h") or rainfall.get("daily"),
            })
        return pd.DataFrame(rows).dropna(subset=["Time"])
    return pd.DataFrame()
