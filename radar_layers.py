import os
import requests
from typing import Optional

RAINVIEWER_JSON = "https://api.rainviewer.com/public/weather-maps.json"

def get_latest_rainviewer_timestamps() -> dict:
    """
    Returns latest timestamps for radar and satellite (infrared) from RainViewer.
    """
    r = requests.get(RAINVIEWER_JSON, timeout=20)
    r.raise_for_status()
    data = r.json()
    # Radar: take latest from 'past' + 'nowcast' if exists
    radar_times = []
    for k in ("past", "nowcast"):
        for f in data.get("radar", {}).get(k, []):
            if "time" in f:
                radar_times.append(f["time"])
    radar_times = sorted(set(radar_times))
    latest_radar = radar_times[-1] if radar_times else None

    # Satellite infrared: latest from 'past'
    sat_times = [f.get("time") for f in data.get("satellite", {}).get("infrared", {}).get("past", []) if "time" in f]
    sat_times = sorted(set(sat_times))
    latest_sat = sat_times[-1] if sat_times else None

    return {"radar": latest_radar, "satellite": latest_sat}

def build_rainviewer_tile(layer: str, ts: int, opacity: float = 0.9, color: int = 3, smooth: int = 1, snow: int = 1) -> str:
    """
    Build RainViewer XYZ tile URL for a given layer and timestamp.
    layer: 'radar' or 'satellite'
    """
    base = f"https://tilecache.rainviewer.com/v2/{layer}/{ts}/256/{{z}}/{{x}}/{{y}}/2/1_1.png"
    # For radar, color/smooth/snow params allowed; satellite ignores extras
    if layer == "radar":
        return f"{base}?color={color}&smooth={smooth}&snow={snow}&opacity={opacity}"
    else:
        return f"{base}?opacity={opacity}"

def build_openweather_tile(layer: str, apikey: str, opacity: float = 0.6) -> str:
    """
    Build OpenWeather XYZ tile URL for a given layer (e.g., 'clouds_new', 'precipitation_new').
    """
    return f"https://tile.openweathermap.org/map/{layer}/{{z}}/{{x}}/{{y}}.png?appid={apikey}&opacity={opacity}"
