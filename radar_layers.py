# radar_layers.py
import requests

RAINVIEWER_JSON = "https://api.rainviewer.com/public/weather-maps.json"

def _safe_json(url: str) -> dict:
    j = requests.get(url, timeout=20).json()
    # A volte la radice è una LISTA
    if isinstance(j, list) and j:
        j = j[0]
    if not isinstance(j, dict):
        raise ValueError("Formato RainViewer inatteso (root non dict)")
    return j

def _extract_times(seq) -> list[int]:
    out = []
    if isinstance(seq, list):
        for f in seq:
            t = f.get("time") if isinstance(f, dict) else None
            if t is not None:
                out.append(int(t))
    return out

def get_latest_rainviewer_timestamps() -> dict:
    """
    Ritorna {'radar': <ts|None>, 'satellite': <ts|None>}
    """
    j = _safe_json(RAINVIEWER_JSON)

    radar = (j.get("radar") or {})
    radar_times = sorted(set(
        _extract_times(radar.get("past") or []) +
        _extract_times(radar.get("nowcast") or [])
    ))

    # satellite.infrared può essere LISTA o DICT con 'past'
    sat_ir = (j.get("satellite") or {}).get("infrared", [])
    if isinstance(sat_ir, dict):
        sat_ir = sat_ir.get("past") or []
    sat_times = sorted(set(_extract_times(sat_ir)))

    return {
        "radar": radar_times[-1] if radar_times else None,
        "satellite": sat_times[-1] if sat_times else None,
    }

def build_rainviewer_tile(layer: str, ts: int, opacity: float = 0.9, color: int = 3, smooth: int = 1, snow: int = 1) -> str:
    """
    XYZ tile RainViewer v2 per radar/satellite.
    """
    base = f"https://tilecache.rainviewer.com/v2/{layer}/{ts}/256/{{z}}/{{x}}/{{y}}"
    if layer == "radar":
        return f"{base}/2/1_1.png?opacity={opacity}&color={color}&smooth={smooth}&snow={snow}"
    else:
        return f"{base}/0/0_0.png?opacity={opacity}"

def build_openweather_tile(layer: str, apikey: str, opacity: float = 0.6) -> str:
    return f"https://tile.openweathermap.org/map/{layer}/{{z}}/{{x}}/{{y}}.png?appid={apikey}&opacity={opacity}"

