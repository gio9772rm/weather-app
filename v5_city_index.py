"""Public city centres used locally to resolve an approximate browser position."""

import gzip
import json
import math
import threading
from pathlib import Path

import numpy as np

from city_weather import CityLocation

_PATH = Path(__file__).parent / "assets" / "geo" / "public-cities.json.gz"
_LOCK = threading.Lock()
_INDEX = None
_RADIUS = 6371.0


def _index():
    global _INDEX
    with _LOCK:
        if _INDEX is None:
            with gzip.open(_PATH, "rt", encoding="utf-8") as source:
                data = json.load(source)
            rows = data["cities"]
            countries = data["countries"]
            lat, lon = np.radians([(r[3], r[4]) for r in rows]).T
            positions = np.column_stack(
                [np.cos(lat) * np.cos(lon), np.cos(lat) * np.sin(lon), np.sin(lat)]
            )
            population = np.fromiter((max(1, r[6]) for r in rows), dtype=float)
            metro_bonus = np.clip((np.log10(population) - 4) * 3, 0, 8)
            _INDEX = (
                rows,
                countries,
                positions,
                metro_bonus,
                {r[0]: i for i, r in enumerate(rows)},
            )
        return _INDEX


def _location(row, countries):
    identifier, name, code, lat, lon, zone, _ = row
    return CityLocation(
        name, countries.get(code, code), "", lat, lon, zone, geonames_id=identifier
    )


def public_city(identifier):
    rows, countries, _, _, ids = _index()
    return _location(rows[ids[identifier]], countries) if identifier in ids else None


def nearby_public_city(latitude, longitude, *, max_km=20):
    """Nearby indexed centre, with a modest preference for metro areas."""
    rows, countries, positions, metro_bonus, _ = _index()
    lat, lon = math.radians(latitude), math.radians(longitude)
    vector = [
        math.cos(lat) * math.cos(lon),
        math.cos(lat) * math.sin(lon),
        math.sin(lat),
    ]
    # An elementwise product avoids starting a BLAS thread pool for this lookup.
    similarity = (
        positions[:, 0] * vector[0]
        + positions[:, 1] * vector[1]
        + positions[:, 2] * vector[2]
    )
    candidates = np.flatnonzero(similarity >= math.cos(max_km / _RADIUS))
    if not candidates.size:
        return None
    km = np.arccos(np.clip(similarity[candidates], -1, 1)) * _RADIUS
    best = int(candidates[(km - metro_bonus[candidates]).argmin()])
    return _location(rows[best], countries)
