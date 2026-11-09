"""Validation shared by the night report, planner and private profiles."""

import re

MAX_POINTS = 721
_KEY = re.compile(r"^(station|city|point):[A-Za-z0-9_:.+/\-]{1,132}$")


def horizon_mask(value):
    if not isinstance(value, dict) or len(value) > MAX_POINTS:
        raise ValueError("L’orizzonte può contenere al massimo 721 punti")
    result = {}
    for azimuth, altitude in value.items():
        azimuth, altitude = float(azimuth), float(altitude)
        if not 0 <= azimuth <= 360 or not -90 <= altitude <= 90:
            raise ValueError("Usa azimut 0–360° e altezza −90–90°")
        direction = round(azimuth, 3) % 360
        result[direction] = max(result.get(direction, -90), round(altitude, 3))
    return result


def saved_horizons(value):
    if not isinstance(value, dict) or len(value) > 16:
        raise ValueError("Puoi conservare fino a sedici orizzonti")
    result = {}
    for key, item in value.items():
        if not _KEY.fullmatch(key) or not isinstance(item, dict):
            raise ValueError("Postazione dell’orizzonte non valida")
        mask = horizon_mask(item["mask"])
        if len(mask) == 1:
            raise ValueError("L’orizzonte richiede almeno due azimut distinti")
        result[key] = {
            "name": " ".join(str(item.get("name", "Orizzonte")).split())[:100],
            "mask": {f"{az:g}": altitude for az, altitude in sorted(mask.items())},
        }
    return result
