"""Local-only Italian Civil Protection radar and lightning observations.

Only the exact Web-Mercator tiles intersecting a small crop around the station
are downloaded. The database receives point values and aggregate statistics;
tiles, crop pixels, and individual lightning coordinates are never persisted.
"""

from __future__ import annotations

import io
import math
import struct
from dataclasses import dataclass, field
from time import perf_counter
from typing import Any

import numpy as np
import pandas as pd
import requests
from PIL import Image
from sqlalchemy import text
from sqlalchemy.engine import Engine

from config import Settings, settings
from db import get_engine
from forecast_providers import build_session
from source_health import record_source_disabled, record_source_result
from station_registry import normalise_station_id

DPC_API = "https://radar-api.protezionecivile.it"
DPC_CACHE = "https://s3-prod-dpc-radar-webp-cache.s3.eu-south-1.amazonaws.com"
DPC_ORIGIN = "https://radar.protezionecivile.gov.it"
ZOOM = 7
TILE_SIZE = 256


class DpcRadarError(RuntimeError):
    """The official DPC products are temporarily unavailable."""


@dataclass(frozen=True)
class DpcRadarSnapshot:
    station_id: str
    observed_at: pd.Timestamp
    sri_observed_at: pd.Timestamp | None
    vmi_observed_at: pd.Timestamp | None
    lightning_observed_at: pd.Timestamp | None
    sri_point_mm_h: float
    sri_mean_mm_h: float
    sri_max_mm_h: float
    sri_echo_fraction: float
    vmi_point_dbz: float
    vmi_max_dbz: float
    lightning_10km: int
    lightning_25km: int
    lightning_50km: int
    nearest_lightning_km: float | None
    fetched_at: pd.Timestamp
    sri_window: np.ndarray | None = field(default=None, compare=False, repr=False)


def _utc(value: Any) -> pd.Timestamp | None:
    parsed = pd.to_datetime(value, utc=True, errors="coerce")
    return None if pd.isna(parsed) else pd.Timestamp(parsed)


def _product_time(
    product: str, session: requests.Session
) -> pd.Timestamp:
    try:
        response = session.get(
            f"{DPC_API}/findLastProductByType",
            params={"type": product, "lang": "it"},
            headers={"Origin": DPC_ORIGIN, "Referer": f"{DPC_ORIGIN}/"},
            timeout=(8, 25),
        )
    except requests.RequestException as exc:
        raise DpcRadarError("DPC radar non raggiungibile") from exc
    if not response.ok:
        raise DpcRadarError(f"DPC radar: risposta HTTP {response.status_code}")
    try:
        payload = response.json()
        milliseconds = (payload.get("lastProducts") or [])[0]["time"]
    except (ValueError, KeyError, IndexError, TypeError) as exc:
        raise DpcRadarError("DPC radar: metadati non validi") from exc
    timestamp = pd.to_datetime(milliseconds, unit="ms", utc=True, errors="coerce")
    if pd.isna(timestamp):
        raise DpcRadarError("DPC radar: orario prodotto non valido")
    return pd.Timestamp(timestamp)


def _global_pixel(latitude: float, longitude: float) -> tuple[int, int]:
    latitude = max(-85.05112878, min(85.05112878, float(latitude)))
    longitude = ((float(longitude) + 180.0) % 360.0) - 180.0
    scale = (2**ZOOM) * TILE_SIZE
    x = int((longitude + 180.0) / 360.0 * scale)
    latitude_rad = math.radians(latitude)
    y = int(
        (1.0 - math.asinh(math.tan(latitude_rad)) / math.pi) / 2.0 * scale
    )
    return x, y


def _tile_url(product: str, moment: pd.Timestamp, x: int, y: int) -> str:
    utc = moment.tz_convert("UTC")
    return (
        f"{DPC_CACHE}/{product}/{utc:%Y/%m/%d/%H%M}/{ZOOM}/{x}/{y}/"
        f"{product.lower()}.webp"
    )


def _decode_tile(content: bytes, maximum: float) -> np.ndarray:
    try:
        with Image.open(io.BytesIO(content)) as source:
            image = source.convert("RGBA")
    except (OSError, ValueError) as exc:
        raise DpcRadarError("DPC radar: tassello locale non leggibile") from exc
    pixels = np.asarray(image, dtype=float)
    if pixels.shape[:2] != (TILE_SIZE, TILE_SIZE):
        raise DpcRadarError("DPC radar: dimensione tassello inattesa")
    values = pixels[..., 0] / 255.0 * maximum
    # The current DPC WebP renderer stores the normalized physical value in
    # the red channel and marks no-data pixels through alpha.
    values[pixels[..., 3] <= 1.0] = 0.0
    return values


def _local_window(
    product: str,
    moment: pd.Timestamp,
    latitude: float,
    longitude: float,
    radius: int,
    session: requests.Session,
) -> np.ndarray:
    global_x, global_y = _global_pixel(latitude, longitude)
    offsets = range(-radius, radius + 1)
    tile_keys = {
        ((global_x + dx) // TILE_SIZE, (global_y + dy) // TILE_SIZE)
        for dy in offsets
        for dx in offsets
    }
    maximum = 100.0 if product == "SRI" else 60.0
    tiles: dict[tuple[int, int], np.ndarray] = {}
    for tile_x, tile_y in sorted(tile_keys):
        try:
            response = session.get(
                _tile_url(product, moment, tile_x, tile_y),
                headers={"Origin": DPC_ORIGIN, "Referer": f"{DPC_ORIGIN}/"},
                timeout=(8, 25),
            )
        except requests.RequestException as exc:
            raise DpcRadarError("DPC radar: tassello locale non raggiungibile") from exc
        if not response.ok:
            raise DpcRadarError(
                f"DPC radar: tassello locale HTTP {response.status_code}"
            )
        tiles[(tile_x, tile_y)] = _decode_tile(response.content, maximum)

    size = radius * 2 + 1
    output = np.zeros((size, size), dtype=float)
    for row, dy in enumerate(offsets):
        for column, dx in enumerate(offsets):
            x, y = global_x + dx, global_y + dy
            tile = tiles[(x // TILE_SIZE, y // TILE_SIZE)]
            output[row, column] = tile[y % TILE_SIZE, x % TILE_SIZE]
    return output


def parse_lightning_v2(content: bytes) -> list[tuple[float, float]]:
    """Decode the compact public DPC lightning format, version 2."""
    if len(content) < 3 or content[0] != 2:
        return []
    count = struct.unpack_from(">H", content, 1)[0]
    offset = 3
    strikes: list[tuple[float, float]] = []
    for _ in range(count):
        if offset + 6 > len(content):
            break
        lon_low, lat_low, high_bits, marker = struct.unpack_from(">HHBB", content, offset)
        longitude_code = lon_low + ((high_bits >> 6) & 3) * 65536
        latitude_code = lat_low + ((high_bits >> 4) & 3) * 65536
        longitude = longitude_code * 360.0 / 262144.0 - 180.0
        latitude = latitude_code * 180.0 / 262144.0 - 90.0
        strikes.append((latitude, longitude))
        offset += 6 if marker < 255 else 8
    return strikes


def _distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius = 6371.0088
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    value = (
        math.sin(dphi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    )
    return radius * 2 * math.atan2(math.sqrt(value), math.sqrt(1 - value))


def _lightning(
    reference: pd.Timestamp,
    latitude: float,
    longitude: float,
    session: requests.Session,
) -> tuple[pd.Timestamp | None, list[float]]:
    # The official client requests the lightning layer for the exact radar
    # frame. Falling back to an older file would silently mix observations
    # from different times, so an unavailable frame remains explicitly n/d.
    milliseconds = int(reference.timestamp() * 1000)
    url = f"{DPC_CACHE}/LGT/lgt_5min_{milliseconds}.bin"
    try:
        response = session.get(
            url,
            headers={
                "Origin": DPC_ORIGIN,
                "Referer": f"{DPC_ORIGIN}/",
                "Cache-Control": "no-cache",
            },
            timeout=(5, 12),
        )
    except requests.RequestException:
        return None, []
    if not response.ok:
        return None, []
    if len(response.content) < 3 or response.content[0] != 2:
        return None, []
    strikes = parse_lightning_v2(response.content)
    distances = [
        _distance_km(latitude, longitude, strike_lat, strike_lon)
        for strike_lat, strike_lon in strikes
    ]
    return reference, distances


def fetch_dpc_radar_snapshot(
    cfg: Settings = settings,
    session: requests.Session | None = None,
) -> DpcRadarSnapshot:
    own_session = session is None
    session = session or build_session(retries=2)
    try:
        sri_time = _product_time("SRI", session)
        vmi_time = _product_time("VMI", session)
        sri = _local_window(
            "SRI",
            sri_time,
            cfg.latitude,
            cfg.longitude,
            cfg.dpc_radar_crop_radius,
            session,
        )
        vmi = _local_window(
            "VMI",
            vmi_time,
            cfg.latitude,
            cfg.longitude,
            cfg.dpc_radar_crop_radius,
            session,
        )
        lightning_time, distances = _lightning(
            sri_time, cfg.latitude, cfg.longitude, session
        )
        center = cfg.dpc_radar_crop_radius
        return DpcRadarSnapshot(
            station_id=normalise_station_id(cfg.station_id),
            observed_at=max(sri_time, vmi_time),
            sri_observed_at=sri_time,
            vmi_observed_at=vmi_time,
            lightning_observed_at=lightning_time,
            sri_point_mm_h=float(sri[center, center]),
            sri_mean_mm_h=float(np.mean(sri)),
            sri_max_mm_h=float(np.max(sri)),
            sri_echo_fraction=float(np.mean(sri > 0.05)),
            vmi_point_dbz=float(vmi[center, center]),
            vmi_max_dbz=float(np.max(vmi)),
            lightning_10km=sum(value <= 10 for value in distances),
            lightning_25km=sum(value <= 25 for value in distances),
            lightning_50km=sum(value <= 50 for value in distances),
            nearest_lightning_km=min(distances) if distances else None,
            fetched_at=pd.Timestamp.now(tz="UTC"),
            sri_window=sri,
        )
    finally:
        if own_session:
            session.close()


def _iso(value: Any) -> str | None:
    timestamp = _utc(value)
    return None if timestamp is None else timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")


def archive_dpc_radar_snapshot(
    snapshot: DpcRadarSnapshot, engine: Engine | None = None
) -> int:
    engine = engine or get_engine()
    record = {
        "station_id": snapshot.station_id,
        "observed_at": _iso(snapshot.observed_at),
        "sri_observed_at": _iso(snapshot.sri_observed_at),
        "vmi_observed_at": _iso(snapshot.vmi_observed_at),
        "lightning_observed_at": _iso(snapshot.lightning_observed_at),
        "sri_point_mm_h": snapshot.sri_point_mm_h,
        "sri_mean_mm_h": snapshot.sri_mean_mm_h,
        "sri_max_mm_h": snapshot.sri_max_mm_h,
        "sri_echo_fraction": snapshot.sri_echo_fraction,
        "vmi_point_dbz": snapshot.vmi_point_dbz,
        "vmi_max_dbz": snapshot.vmi_max_dbz,
        "lightning_10km": snapshot.lightning_10km,
        "lightning_25km": snapshot.lightning_25km,
        "lightning_50km": snapshot.lightning_50km,
        "nearest_lightning_km": snapshot.nearest_lightning_km,
        "fetched_at": _iso(snapshot.fetched_at),
    }
    columns = list(record)
    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO radar_local_snapshots ("
                + ",".join(columns)
                + ") VALUES ("
                + ",".join(f":{column}" for column in columns)
                + ") ON CONFLICT (station_id,observed_at) DO UPDATE SET "
                + ",".join(
                    f"{column}=excluded.{column}"
                    for column in columns
                    if column not in {"station_id", "observed_at"}
                )
            ),
            record,
        )
    return 1


def refresh_dpc_radar(
    cfg: Settings = settings, engine: Engine | None = None
) -> DpcRadarSnapshot | None:
    engine = engine or get_engine()
    if not cfg.dpc_radar_enabled:
        record_source_disabled("dpc_radar_local", engine)
        record_source_disabled("dpc_lightning_local", engine)
        return None
    started = perf_counter()
    try:
        snapshot = fetch_dpc_radar_snapshot(cfg)
        archive_dpc_radar_snapshot(snapshot, engine)
    except DpcRadarError as exc:
        elapsed = (perf_counter() - started) * 1000
        record_source_result(
            "dpc_radar_local", success=False, latency_ms=elapsed, error=exc, engine=engine
        )
        record_source_result(
            "dpc_lightning_local", success=False, latency_ms=elapsed, error=exc, engine=engine
        )
        raise
    elapsed = (perf_counter() - started) * 1000
    record_source_result(
        "dpc_radar_local",
        success=True,
        rows_received=1,
        last_observation_at=snapshot.observed_at,
        latency_ms=elapsed,
        engine=engine,
    )
    record_source_result(
        "dpc_lightning_local",
        success=True,
        rows_received=snapshot.lightning_50km,
        last_observation_at=snapshot.lightning_observed_at,
        latency_ms=elapsed,
        engine=engine,
    )
    return snapshot
