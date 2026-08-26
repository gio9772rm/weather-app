"""RainViewer metadata and conservative point nowcast for the local dashboard."""

from __future__ import annotations

import io
import math
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
import requests
from PIL import Image

from forecast_providers import build_session

RAINVIEWER_METADATA = "https://api.rainviewer.com/public/weather-maps.json"


class RadarNowcastError(RuntimeError):
    """Radar metadata or imagery are temporarily unavailable."""


@dataclass(frozen=True)
class RadarNowcast:
    status: str
    message: str
    generated_at: pd.Timestamp
    arrival_minutes: int | None = None
    echo_probability: float | None = None
    confidence: str = "orientativa"
    frame_count: int = 0
    attribution: str = "RainViewer"


def _timestamp(value: Any) -> pd.Timestamp:
    return pd.to_datetime(value, unit="s", utc=True, errors="coerce")


def parse_nowcast_frames(payload: dict[str, Any]) -> list[dict[str, Any]]:
    frames = ((payload.get("radar") or {}).get("nowcast") or [])
    parsed = []
    for frame in frames:
        moment = _timestamp(frame.get("time"))
        path = str(frame.get("path") or "")
        if pd.isna(moment) or not path.startswith("/"):
            continue
        parsed.append({"time": moment, "path": path})
    return sorted(parsed, key=lambda item: item["time"])


def _tile_position(latitude: float, longitude: float, zoom: int) -> tuple[int, int, int, int]:
    latitude = max(-85.05112878, min(85.05112878, float(latitude)))
    longitude = ((float(longitude) + 180.0) % 360.0) - 180.0
    scale = 2**zoom
    world_x = (longitude + 180.0) / 360.0 * scale
    latitude_rad = math.radians(latitude)
    world_y = (
        1.0
        - math.asinh(math.tan(latitude_rad)) / math.pi
    ) / 2.0 * scale
    tile_x, tile_y = int(world_x), int(world_y)
    pixel_x = int((world_x - tile_x) * 256)
    pixel_y = int((world_y - tile_y) * 256)
    return tile_x, tile_y, pixel_x, pixel_y


def _echo_fraction(content: bytes, pixel_x: int, pixel_y: int, radius: int = 10) -> float:
    try:
        image = Image.open(io.BytesIO(content)).convert("RGBA")
    except (OSError, ValueError):
        return 0.0
    array = np.asarray(image)
    x0, x1 = max(0, pixel_x - radius), min(array.shape[1], pixel_x + radius + 1)
    y0, y1 = max(0, pixel_y - radius), min(array.shape[0], pixel_y + radius + 1)
    window = array[y0:y1, x0:x1]
    if not window.size:
        return 0.0
    alpha = window[..., 3]
    rgb = window[..., :3]
    # RainViewer's no-rain pixels are transparent.  Requiring both alpha and a
    # non-trivial colour avoids counting a fully transparent black tile.
    echo = (alpha >= 24) & (rgb.max(axis=2) >= 24)
    return float(echo.mean())


def fetch_radar_nowcast(
    latitude: float,
    longitude: float,
    *,
    session: requests.Session | None = None,
) -> RadarNowcast:
    """Return a best-effort rain arrival estimate from published future frames."""
    own_session = session is None
    session = session or build_session(retries=2)
    try:
        try:
            response = session.get(RAINVIEWER_METADATA, timeout=(8, 25))
        except requests.RequestException as exc:
            raise RadarNowcastError("nowcast radar non raggiungibile") from exc
        if not response.ok:
            raise RadarNowcastError(
                f"nowcast radar: risposta HTTP {response.status_code}"
            )
        try:
            payload = response.json()
        except ValueError as exc:
            raise RadarNowcastError("nowcast radar: risposta non valida") from exc
        generated = _timestamp(payload.get("generated"))
        if pd.isna(generated):
            generated = pd.Timestamp.now(tz="UTC")
        frames = parse_nowcast_frames(payload)
        if not frames:
            return RadarNowcast(
                status="unavailable",
                message=(
                    "RainViewer non sta pubblicando fotogrammi previsionali in questo momento; "
                    "il radar osservato qui sotto resta disponibile."
                ),
                generated_at=generated,
                frame_count=0,
            )

        host = str(payload.get("host") or "").rstrip("/")
        if not host.startswith("https://"):
            raise RadarNowcastError("nowcast radar: host non valido")
        zoom = 8
        tile_x, tile_y, pixel_x, pixel_y = _tile_position(
            latitude, longitude, zoom
        )
        samples: list[tuple[pd.Timestamp, float]] = []
        for frame in frames:
            tile_url = (
                f"{host}{frame['path']}/256/{zoom}/{tile_x}/{tile_y}/2/1_1.png"
            )
            try:
                tile = session.get(tile_url, timeout=(8, 25))
            except requests.RequestException:
                continue
            if tile.ok:
                samples.append(
                    (frame["time"], _echo_fraction(tile.content, pixel_x, pixel_y))
                )
        if not samples:
            raise RadarNowcastError("nowcast radar: fotogrammi non leggibili")
        threshold = 0.015
        hit = next((sample for sample in samples if sample[1] >= threshold), None)
        if hit is None:
            return RadarNowcast(
                status="dry",
                message=(
                    f"Nessuna eco di precipitazione rilevata vicino alla stazione nei "
                    f"{len(samples)} fotogrammi previsionali disponibili."
                ),
                generated_at=generated,
                echo_probability=0.0,
                frame_count=len(samples),
            )
        arrival = max(0, round((hit[0] - generated).total_seconds() / 60))
        probability = min(99.0, 35.0 + hit[1] * 800.0)
        return RadarNowcast(
            status="rain",
            message=(
                f"Possibile eco di precipitazione nell’area della stazione tra circa "
                f"{arrival} minuti."
            ),
            generated_at=generated,
            arrival_minutes=arrival,
            echo_probability=probability,
            frame_count=len(samples),
            confidence="orientativa · estrapolazione radar",
        )
    finally:
        if own_session:
            session.close()
