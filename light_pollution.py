"""Geolocated zenith-brightness estimate from David Lorenz's 2025 atlas."""

from __future__ import annotations

import gzip
import math
from dataclasses import dataclass

import requests

from forecast_providers import build_session

ATLAS_YEAR = 2025
ATLAS_URL = "https://djlorenz.github.io/astronomy/lp/overlay/dark.html"
TILE_URL = (
    "https://djlorenz.github.io/astronomy/binary_tiles/"
    "{year}/binary_tile_{tile_x}_{tile_y}.dat.gz"
)


class LightPollutionError(RuntimeError):
    """Raised when the public atlas cannot provide a valid point estimate."""


@dataclass(frozen=True)
class LightPollutionEstimate:
    latitude: float
    longitude: float
    year: int
    sqm: float
    lp_index: float
    lp_zone: str
    approximate_bortle: int
    source_url: str = ATLAS_URL


def _grid_position(latitude: float, longitude: float) -> tuple[int, int, int, int]:
    if not math.isfinite(latitude) or not math.isfinite(longitude):
        raise LightPollutionError("Coordinate non valide")
    if latitude < -65 or latitude >= 75:
        raise LightPollutionError("L'atlante copre latitudini da 65°S a 75°N")
    longitude = ((longitude + 180.0) % 360.0) - 180.0
    lon_from_date_line = (longitude + 180.0) % 360.0
    lat_from_start = latitude + 65.0
    tile_x = math.floor(lon_from_date_line / 5.0) + 1
    tile_y = math.floor(lat_from_start / 5.0) + 1
    grid_x = math.floor(
        120.0 * (lon_from_date_line - 5.0 * (tile_x - 1) + 1.0 / 240.0) + 0.5
    )
    grid_y = math.floor(
        120.0 * (lat_from_start - 5.0 * (tile_y - 1) + 1.0 / 240.0) + 0.5
    )
    if not (1 <= grid_x <= 600 and 1 <= grid_y <= 600):
        raise LightPollutionError("Punto non risolvibile nella griglia dell'atlante")
    return tile_x, tile_y, grid_x, grid_y


def classify_light_pollution(lp_index: float) -> tuple[str, int]:
    """Return Lorenz LP zone and a deliberately approximate Bortle label."""
    thresholds = (
        (0.01, "0", 1),
        (0.06, "1a", 2),
        (0.11, "1b", 2),
        (0.19, "2a", 3),
        (0.33, "2b", 3),
        (0.58, "3a", 4),
        (1.00, "3b", 4),
        (1.73, "4a", 5),
        (3.00, "4b", 5),
        (5.20, "5a", 6),
        (9.00, "5b", 6),
        (15.59, "6a", 7),
        (27.00, "6b", 7),
        (46.77, "7a", 8),
    )
    value = max(0.0, float(lp_index))
    for upper_bound, zone, bortle in thresholds:
        if value < upper_bound:
            return zone, bortle
    return "7b", 9


def decode_light_pollution_tile(
    payload: bytes,
    latitude: float,
    longitude: float,
    *,
    year: int = ATLAS_YEAR,
) -> LightPollutionEstimate:
    """Decode one public compressed atlas tile using its published grid formula."""
    _, _, grid_x, grid_y = _grid_position(latitude, longitude)
    try:
        raw = gzip.decompress(payload) if payload.startswith(b"\x1f\x8b") else payload
    except (EOFError, OSError) as exc:
        raise LightPollutionError("Tassello dell'atlante danneggiato") from exc
    if len(raw) < 360_001:
        raise LightPollutionError("Tassello dell'atlante incompleto")
    signed = [value - 256 if value > 127 else value for value in raw]
    first_value = 128 * signed[0] + signed[1]
    change = sum(signed[600 * index + 1] for index in range(1, grid_y))
    change += sum(signed[600 * (grid_y - 1) + 1 + index] for index in range(1, grid_x))
    compressed = first_value + change
    lp_index = max(0.0, (5.0 / 195.0) * (math.exp(0.0195 * compressed) - 1.0))
    sqm = 22.0 - 2.5 * math.log10(1.0 + lp_index)
    zone, bortle = classify_light_pollution(lp_index)
    normalized_longitude = ((longitude + 180.0) % 360.0) - 180.0
    return LightPollutionEstimate(
        latitude=float(latitude),
        longitude=normalized_longitude,
        year=int(year),
        sqm=sqm,
        lp_index=lp_index,
        lp_zone=zone,
        approximate_bortle=bortle,
    )


def fetch_light_pollution(
    latitude: float,
    longitude: float,
    *,
    year: int = ATLAS_YEAR,
    session: requests.Session | None = None,
) -> LightPollutionEstimate:
    """Fetch the single small atlas tile containing the requested coordinates."""
    tile_x, tile_y, _, _ = _grid_position(latitude, longitude)
    url = TILE_URL.format(year=year, tile_x=tile_x, tile_y=tile_y)
    own_session = session is None
    client = session or build_session(retries=2)
    try:
        response = client.get(
            url,
            headers={"User-Agent": "Meteo-V3/1.0 (+personal weather dashboard)"},
            timeout=(5, 15),
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        raise LightPollutionError(
            "Atlante luminoso temporaneamente non raggiungibile"
        ) from exc
    finally:
        if own_session:
            client.close()
    return decode_light_pollution_tile(response.content, latitude, longitude, year=year)
