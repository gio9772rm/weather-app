"""Internet-only weather lookup for arbitrary cities.

The local station pipeline deliberately does not participate here: locations are
geocoded and forecast directly through Open-Meteo.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests

from forecast_providers import WMO_DESCRIPTIONS_IT, build_session


class CityWeatherError(RuntimeError):
    """A city lookup failed without exposing request details."""


@dataclass(frozen=True)
class CityLocation:
    """One geocoding result suitable for weather lookup."""

    name: str
    country: str
    admin1: str
    latitude: float
    longitude: float
    timezone: str
    elevation_m: float | None = None

    @property
    def label(self) -> str:
        parts = [self.name]
        if self.admin1 and self.admin1.casefold() != self.name.casefold():
            parts.append(self.admin1)
        if self.country:
            parts.append(self.country)
        return ", ".join(parts)


@dataclass
class CityForecast:
    """Normalised current, hourly and daily internet forecast."""

    timezone: str
    current: dict[str, Any]
    hourly: pd.DataFrame
    daily: pd.DataFrame
    fetched_at: pd.Timestamp


def _get_json(
    session: requests.Session,
    url: str,
    params: dict[str, Any],
    service: str,
) -> dict[str, Any]:
    try:
        response = session.get(url, params=params, timeout=(8, 30))
        if not response.ok:
            raise CityWeatherError(f"{service}: risposta HTTP {response.status_code}")
        payload = response.json()
    except CityWeatherError:
        raise
    except (requests.RequestException, ValueError) as exc:
        raise CityWeatherError(f"{service}: servizio non raggiungibile") from exc
    if not isinstance(payload, dict):
        raise CityWeatherError(f"{service}: risposta non valida")
    return payload


def _as_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if np.isfinite(number) else None


def search_cities(
    query: str,
    session: requests.Session | None = None,
    limit: int = 8,
) -> list[CityLocation]:
    """Return globally matching cities in Italian, most relevant first."""
    query = " ".join(str(query or "").split())
    if len(query) < 2:
        return []
    own_session = session is None
    session = session or build_session()
    try:
        payload = _get_json(
            session,
            "https://geocoding-api.open-meteo.com/v1/search",
            {
                "name": query,
                "count": max(1, min(20, int(limit))),
                "language": "it",
                "format": "json",
            },
            "Ricerca città",
        )
    finally:
        if own_session:
            session.close()

    locations: list[CityLocation] = []
    for item in payload.get("results") or []:
        latitude = _as_float(item.get("latitude"))
        longitude = _as_float(item.get("longitude"))
        name = str(item.get("name") or "").strip()
        if not name or latitude is None or longitude is None:
            continue
        locations.append(
            CityLocation(
                name=name,
                country=str(item.get("country") or "").strip(),
                admin1=str(item.get("admin1") or "").strip(),
                latitude=latitude,
                longitude=longitude,
                timezone=str(item.get("timezone") or "UTC").strip() or "UTC",
                elevation_m=_as_float(item.get("elevation")),
            )
        )
    return locations


def _local_times(values: Any, timezone: str) -> pd.Series:
    parsed = pd.Series(pd.to_datetime(values, errors="coerce"))
    try:
        zone = ZoneInfo(timezone)
    except (KeyError, ValueError):
        zone = ZoneInfo("UTC")
    if parsed.dt.tz is None:
        return parsed.dt.tz_localize(zone, ambiguous="NaT", nonexistent="shift_forward")
    return parsed.dt.tz_convert(zone)


def _frame_from_block(
    block: dict[str, Any],
    timezone: str,
    rename: dict[str, str],
) -> pd.DataFrame:
    times = block.get("time") or []
    frame = pd.DataFrame({"time": _local_times(times, timezone)})
    for source, target in rename.items():
        values = block.get(source)
        frame[target] = values if isinstance(values, list) and len(values) == len(frame) else np.nan
    if "weather_code" in frame:
        codes = pd.to_numeric(frame["weather_code"], errors="coerce")
        frame["description"] = codes.map(WMO_DESCRIPTIONS_IT).fillna("Variabile")
    return frame.dropna(subset=["time"])


HOURLY_FIELDS = {
    "temperature_2m": "temp_c",
    "apparent_temperature": "feels_like_c",
    "relative_humidity_2m": "humidity",
    "precipitation_probability": "precip_probability",
    "precipitation": "precipitation_mm",
    "rain": "rain_mm",
    "weather_code": "weather_code",
    "cloud_cover": "clouds",
    "pressure_msl": "pressure_hpa",
    "visibility": "visibility_m",
    "wind_speed_10m": "wind_kmh",
    "wind_direction_10m": "wind_dir",
    "wind_gusts_10m": "wind_gust_kmh",
}

DAILY_FIELDS = {
    "weather_code": "weather_code",
    "temperature_2m_max": "temp_max_c",
    "temperature_2m_min": "temp_min_c",
    "apparent_temperature_max": "feels_max_c",
    "apparent_temperature_min": "feels_min_c",
    "sunrise": "sunrise",
    "sunset": "sunset",
    "precipitation_sum": "precipitation_mm",
    "rain_sum": "rain_mm",
    "precipitation_probability_max": "precip_probability",
    "wind_speed_10m_max": "wind_max_kmh",
    "wind_gusts_10m_max": "wind_gust_max_kmh",
    "wind_direction_10m_dominant": "wind_dir",
    "uv_index_max": "uv_index_max",
}

CURRENT_FIELDS = {
    "temperature_2m": "temp_c",
    "apparent_temperature": "feels_like_c",
    "relative_humidity_2m": "humidity",
    "precipitation": "precipitation_mm",
    "rain": "rain_mm",
    "weather_code": "weather_code",
    "cloud_cover": "clouds",
    "pressure_msl": "pressure_hpa",
    "wind_speed_10m": "wind_kmh",
    "wind_direction_10m": "wind_dir",
    "wind_gusts_10m": "wind_gust_kmh",
    "is_day": "is_day",
}


def parse_city_forecast(
    payload: dict[str, Any], fetched_at: pd.Timestamp | None = None
) -> CityForecast:
    """Normalise an Open-Meteo city response for dashboard rendering."""
    timezone = str(payload.get("timezone") or "UTC")
    current_source = payload.get("current") or {}
    current = {
        target: current_source.get(source) for source, target in CURRENT_FIELDS.items()
    }
    current_time = _local_times([current_source.get("time")], timezone)
    current["time"] = current_time.iloc[0] if not current_time.empty else pd.NaT
    code = pd.to_numeric(pd.Series([current.get("weather_code")]), errors="coerce").iloc[0]
    current["description"] = (
        WMO_DESCRIPTIONS_IT.get(int(code), "Variabile") if pd.notna(code) else "Variabile"
    )

    hourly = _frame_from_block(payload.get("hourly") or {}, timezone, HOURLY_FIELDS)
    daily = _frame_from_block(payload.get("daily") or {}, timezone, DAILY_FIELDS)
    for column in ("sunrise", "sunset"):
        if column in daily:
            daily[column] = _local_times(daily[column], timezone)

    fetched = fetched_at or pd.Timestamp.now(tz="UTC")
    fetched = pd.Timestamp(fetched)
    fetched = fetched.tz_localize("UTC") if fetched.tzinfo is None else fetched.tz_convert("UTC")
    return CityForecast(
        timezone=timezone,
        current=current,
        hourly=hourly,
        daily=daily,
        fetched_at=fetched,
    )


def fetch_city_forecast(
    location: CityLocation,
    session: requests.Session | None = None,
) -> CityForecast:
    """Fetch seven days of internet weather for one geocoded city."""
    own_session = session is None
    session = session or build_session()
    params = {
        "latitude": location.latitude,
        "longitude": location.longitude,
        "timezone": "auto",
        "forecast_days": 7,
        "temperature_unit": "celsius",
        "wind_speed_unit": "kmh",
        "precipitation_unit": "mm",
        "current": ",".join(CURRENT_FIELDS),
        "hourly": ",".join(HOURLY_FIELDS),
        "daily": ",".join(DAILY_FIELDS),
    }
    try:
        payload = _get_json(
            session,
            "https://api.open-meteo.com/v1/forecast",
            params,
            "Meteo città",
        )
        return parse_city_forecast(payload)
    finally:
        if own_session:
            session.close()
