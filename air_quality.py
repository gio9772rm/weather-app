"""Keyless Open-Meteo/CAMS air-quality and pollen forecast client."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests

from forecast_providers import build_session


class AirQualityError(RuntimeError):
    """Air-quality data are unavailable without exposing request internals."""


@dataclass
class AirQualityForecast:
    timezone: str
    current: dict[str, Any]
    hourly: pd.DataFrame
    fetched_at: pd.Timestamp
    source: str = "Open-Meteo · CAMS"


AIR_QUALITY_FIELDS = (
    "european_aqi",
    "pm2_5",
    "pm10",
    "nitrogen_dioxide",
    "ozone",
    "sulphur_dioxide",
    "uv_index",
    "alder_pollen",
    "birch_pollen",
    "grass_pollen",
    "mugwort_pollen",
    "olive_pollen",
    "ragweed_pollen",
)


def _as_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if np.isfinite(number) else None


def _local_times(values: Any, timezone: str) -> pd.Series:
    parsed = pd.Series(pd.to_datetime(values, errors="coerce"))
    try:
        zone = ZoneInfo(timezone)
    except (KeyError, ValueError):
        zone = ZoneInfo("UTC")
    if parsed.dt.tz is None:
        return parsed.dt.tz_localize(zone, ambiguous="NaT", nonexistent="shift_forward")
    return parsed.dt.tz_convert(zone)


def parse_air_quality(
    payload: dict[str, Any], fetched_at: pd.Timestamp | None = None
) -> AirQualityForecast:
    """Normalise the Open-Meteo air-quality response."""
    timezone = str(payload.get("timezone") or "UTC")
    current_source = payload.get("current") or {}
    current = {field: _as_float(current_source.get(field)) for field in AIR_QUALITY_FIELDS}
    current_times = _local_times([current_source.get("time")], timezone)
    current["time"] = current_times.iloc[0] if not current_times.empty else pd.NaT

    hourly_source = payload.get("hourly") or {}
    times = hourly_source.get("time") or []
    hourly = pd.DataFrame({"time": _local_times(times, timezone)})
    for field in AIR_QUALITY_FIELDS:
        values = hourly_source.get(field)
        hourly[field] = (
            pd.to_numeric(pd.Series(values), errors="coerce").to_numpy()
            if isinstance(values, list) and len(values) == len(hourly)
            else np.nan
        )
    hourly = hourly.dropna(subset=["time"]).sort_values("time")

    fetched = pd.Timestamp(fetched_at or pd.Timestamp.now(tz="UTC"))
    fetched = fetched.tz_localize("UTC") if fetched.tzinfo is None else fetched.tz_convert("UTC")
    return AirQualityForecast(
        timezone=timezone,
        current=current,
        hourly=hourly,
        fetched_at=fetched,
    )


def fetch_air_quality(
    latitude: float,
    longitude: float,
    timezone: str,
    *,
    session: requests.Session | None = None,
) -> AirQualityForecast:
    """Fetch five days of CAMS air quality and pollen data without an API key."""
    own_session = session is None
    session = session or build_session(retries=2)
    try:
        response = session.get(
            "https://air-quality-api.open-meteo.com/v1/air-quality",
            params={
                "latitude": float(latitude),
                "longitude": float(longitude),
                "timezone": timezone or "auto",
                "forecast_days": 5,
                "current": ",".join(AIR_QUALITY_FIELDS),
                "hourly": ",".join(AIR_QUALITY_FIELDS),
            },
            timeout=(8, 30),
        )
        if not response.ok:
            raise AirQualityError(
                f"qualità dell'aria: risposta HTTP {response.status_code}"
            )
        payload = response.json()
    except AirQualityError:
        raise
    except (requests.RequestException, ValueError) as exc:
        raise AirQualityError("qualità dell'aria: servizio non raggiungibile") from exc
    finally:
        if own_session:
            session.close()
    if not isinstance(payload, dict) or payload.get("error"):
        raise AirQualityError("qualità dell'aria: risposta non valida")
    return parse_air_quality(payload)
