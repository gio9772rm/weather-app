"""Indicative terrain horizon from a small public Copernicus DEM sample."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import numpy as np
import requests

from astronomy_planner import horizon_altitudes
from forecast_providers import build_session

ELEVATION_API_URL = "https://api.open-meteo.com/v1/elevation"
ELEVATION_DOCS_URL = "https://open-meteo.com/en/docs/elevation-api"
COPERNICUS_DEM_RESOLUTION_M = 90
DEFAULT_AZIMUTHS = tuple(float(value) for value in np.arange(0, 360, 22.5))
DEFAULT_DISTANCES_KM = (0.2, 0.5, 1.0, 2.5, 7.5, 20.0)
EARTH_RADIUS_M = 6_371_000.0


class TerrainHorizonError(RuntimeError):
    """Raised when the optional public elevation service cannot be used."""


@dataclass(frozen=True)
class TerrainHorizonEstimate:
    """Privacy-safe result: exact terrestrial coordinates are deliberately absent."""

    mask: dict[float, float]
    peak_distances_km: dict[float, float]
    local_dem_elevation_m: float
    elevation_difference_m: float
    generated_at: datetime
    resolution_m: int = COPERNICUS_DEM_RESOLUTION_M
    source_url: str = ELEVATION_DOCS_URL


def destination_point(
    latitude: float, longitude: float, azimuth_deg: float, distance_km: float
) -> tuple[float, float]:
    """Return a spherical destination point for one radial DEM sample."""
    if not all(
        math.isfinite(value)
        for value in (latitude, longitude, azimuth_deg, distance_km)
    ):
        raise TerrainHorizonError("Parametri geografici non validi")
    if not -90 <= latitude <= 90 or distance_km <= 0:
        raise TerrainHorizonError("Posizione o distanza non valida")
    angular_distance = distance_km * 1000.0 / EARTH_RADIUS_M
    bearing = math.radians(azimuth_deg % 360)
    latitude_radians = math.radians(latitude)
    longitude_radians = math.radians(longitude)
    destination_latitude = math.asin(
        math.sin(latitude_radians) * math.cos(angular_distance)
        + math.cos(latitude_radians) * math.sin(angular_distance) * math.cos(bearing)
    )
    destination_longitude = longitude_radians + math.atan2(
        math.sin(bearing) * math.sin(angular_distance) * math.cos(latitude_radians),
        math.cos(angular_distance)
        - math.sin(latitude_radians) * math.sin(destination_latitude),
    )
    normalized_longitude = (math.degrees(destination_longitude) + 540.0) % 360.0 - 180.0
    return math.degrees(destination_latitude), normalized_longitude


def radial_samples(
    latitude: float,
    longitude: float,
    *,
    azimuths: tuple[float, ...] = DEFAULT_AZIMUTHS,
    distances_km: tuple[float, ...] = DEFAULT_DISTANCES_KM,
) -> list[tuple[float, float, float, float]]:
    """Build at most 96 radial points, leaving one API slot for the origin."""
    if len(azimuths) * len(distances_km) > 99:
        raise TerrainHorizonError("Campionamento altimetrico troppo esteso")
    samples: list[tuple[float, float, float, float]] = []
    for azimuth in azimuths:
        for distance in distances_km:
            sample_latitude, sample_longitude = destination_point(
                latitude, longitude, azimuth, distance
            )
            samples.append(
                (
                    float(azimuth) % 360,
                    float(distance),
                    sample_latitude,
                    sample_longitude,
                )
            )
    return samples


def terrain_mask_from_elevations(
    samples: list[tuple[float, float, float, float]],
    elevations_m: list[float],
    *,
    local_dem_elevation_m: float,
    sensor_height_m: float,
) -> tuple[dict[float, float], dict[float, float]]:
    """Convert a radial DEM profile into apparent elevation angles."""
    if len(samples) != len(elevations_m):
        raise TerrainHorizonError("Profilo altimetrico incompleto")
    if not math.isfinite(local_dem_elevation_m):
        raise TerrainHorizonError("Quota DEM locale non valida")
    height = float(sensor_height_m)
    if not math.isfinite(height) or not 0 <= height <= 100:
        raise TerrainHorizonError("Altezza del sensore non valida")
    grouped: dict[float, list[tuple[float, float]]] = {}
    for (azimuth, distance_km, _latitude, _longitude), elevation in zip(
        samples, elevations_m, strict=True
    ):
        if not math.isfinite(float(elevation)):
            raise TerrainHorizonError("Quota DEM mancante")
        distance_m = float(distance_km) * 1000.0
        # Standard 7/6 effective Earth radius: a conservative refraction-aware
        # curvature correction for an indicative optical horizon.
        curvature_drop = distance_m**2 / (2 * EARTH_RADIUS_M * 7 / 6)
        relative_height = (
            float(elevation) - float(local_dem_elevation_m) - height - curvature_drop
        )
        angle = math.degrees(math.atan2(relative_height, distance_m))
        grouped.setdefault(float(azimuth), []).append(
            (float(np.clip(angle, 0, 60)), float(distance_km))
        )
    mask: dict[float, float] = {}
    distances: dict[float, float] = {}
    for azimuth, candidates in grouped.items():
        angle, distance = max(candidates, key=lambda item: item[0])
        mask[azimuth] = round(angle, 2)
        distances[azimuth] = distance
    return mask, distances


def _decode_elevations(payload: Any, expected: int) -> list[float]:
    if not isinstance(payload, dict) or not isinstance(payload.get("elevation"), list):
        raise TerrainHorizonError("Risposta altimetrica non valida")
    try:
        elevations = [float(value) for value in payload["elevation"]]
    except (TypeError, ValueError) as exc:
        raise TerrainHorizonError("Quote altimetriche non valide") from exc
    if len(elevations) != expected or not all(map(math.isfinite, elevations)):
        raise TerrainHorizonError("Profilo altimetrico incompleto")
    return elevations


def fetch_terrain_horizon(
    latitude: float,
    longitude: float,
    station_elevation_m: float,
    *,
    sensor_height_m: float = 2.0,
    session: requests.Session | None = None,
) -> TerrainHorizonEstimate:
    """Fetch one small GLO-90 batch and return a coordinate-free horizon mask."""
    samples = radial_samples(latitude, longitude)
    coordinates = [(float(latitude), float(longitude))] + [
        (sample[2], sample[3]) for sample in samples
    ]
    own_session = session is None
    client = session or build_session(retries=2)
    try:
        response = client.get(
            ELEVATION_API_URL,
            params={
                "latitude": ",".join(f"{item[0]:.6f}" for item in coordinates),
                "longitude": ",".join(f"{item[1]:.6f}" for item in coordinates),
            },
            timeout=(8, 30),
        )
        if not response.ok:
            raise TerrainHorizonError(
                f"Servizio altimetrico non disponibile (HTTP {response.status_code})"
            )
        payload = response.json()
    except TerrainHorizonError:
        raise
    except (requests.RequestException, ValueError) as exc:
        raise TerrainHorizonError(
            "Servizio altimetrico temporaneamente non raggiungibile"
        ) from exc
    finally:
        if own_session:
            client.close()
    elevations = _decode_elevations(payload, len(coordinates))
    local_dem_elevation = elevations[0]
    mask, peak_distances = terrain_mask_from_elevations(
        samples,
        elevations[1:],
        local_dem_elevation_m=local_dem_elevation,
        sensor_height_m=sensor_height_m,
    )
    return TerrainHorizonEstimate(
        mask=mask,
        peak_distances_km=peak_distances,
        local_dem_elevation_m=local_dem_elevation,
        elevation_difference_m=float(station_elevation_m) - local_dem_elevation,
        generated_at=datetime.now(timezone.utc),
    )


def combine_horizon_masks(
    manual_mask: dict[float, float] | None,
    terrain_mask: dict[float, float] | None,
) -> dict[float, float]:
    """Combine manual and DEM masks without allowing DEM to lower an obstacle."""
    if not terrain_mask:
        return dict(manual_mask or {})
    directions = np.asarray(sorted(float(value) % 360 for value in terrain_mask))
    manual = horizon_altitudes(directions, manual_mask)
    return {
        float(direction): round(
            max(float(terrain_mask[float(direction)]), float(manual_value)), 2
        )
        for direction, manual_value in zip(directions, manual, strict=True)
    }
