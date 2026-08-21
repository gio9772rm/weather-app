"""Internet-only weather lookup for arbitrary cities.

The local station pipeline deliberately does not participate here: locations are
geocoded and forecast directly through Open-Meteo.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from datetime import date
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests
from astral import Observer
from astral.sun import sun

from forecast_providers import WMO_DESCRIPTIONS_IT, build_session


class CityWeatherError(RuntimeError):
    """A city lookup failed without exposing request details."""


LOGGER = logging.getLogger(__name__)


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
    source: str = "Open-Meteo"


def _get_json(
    session: requests.Session,
    url: str,
    params: dict[str, Any],
    service: str,
    headers: dict[str, str] | None = None,
) -> dict[str, Any]:
    try:
        response = session.get(
            url,
            params=params,
            headers=headers,
            timeout=(8, 30),
        )
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
        frame[target] = (
            values if isinstance(values, list) and len(values) == len(frame) else np.nan
        )
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
    code = pd.to_numeric(
        pd.Series([current.get("weather_code")]), errors="coerce"
    ).iloc[0]
    current["description"] = (
        WMO_DESCRIPTIONS_IT.get(int(code), "Variabile")
        if pd.notna(code)
        else "Variabile"
    )

    hourly = _frame_from_block(payload.get("hourly") or {}, timezone, HOURLY_FIELDS)
    daily = _frame_from_block(payload.get("daily") or {}, timezone, DAILY_FIELDS)
    for column in ("sunrise", "sunset"):
        if column in daily:
            daily[column] = _local_times(daily[column], timezone)

    fetched = fetched_at or pd.Timestamp.now(tz="UTC")
    fetched = pd.Timestamp(fetched)
    fetched = (
        fetched.tz_localize("UTC")
        if fetched.tzinfo is None
        else fetched.tz_convert("UTC")
    )
    return CityForecast(
        timezone=timezone,
        current=current,
        hourly=hourly,
        daily=daily,
        fetched_at=fetched,
        source="Open-Meteo",
    )


METNO_DESCRIPTIONS_IT = {
    "clearsky": "Sereno",
    "fair": "Prevalentemente sereno",
    "partlycloudy": "Parzialmente nuvoloso",
    "cloudy": "Coperto",
    "fog": "Nebbia",
    "lightrainshowers": "Rovesci deboli",
    "rainshowers": "Rovesci moderati",
    "heavyrainshowers": "Rovesci forti",
    "lightrainshowersandthunder": "Rovesci e temporali deboli",
    "rainshowersandthunder": "Rovesci e temporali",
    "heavyrainshowersandthunder": "Forti rovesci e temporali",
    "lightsleetshowers": "Deboli rovesci di nevischio",
    "sleetshowers": "Rovesci di nevischio",
    "heavysleetshowers": "Forti rovesci di nevischio",
    "lightsnowshowers": "Deboli rovesci di neve",
    "snowshowers": "Rovesci di neve",
    "heavysnowshowers": "Forti rovesci di neve",
    "lightrain": "Pioggia debole",
    "rain": "Pioggia moderata",
    "heavyrain": "Pioggia forte",
    "lightrainandthunder": "Pioggia debole e temporali",
    "rainandthunder": "Pioggia e temporali",
    "heavyrainandthunder": "Pioggia forte e temporali",
    "lightsleet": "Nevischio debole",
    "sleet": "Nevischio",
    "heavysleet": "Nevischio forte",
    "lightsnow": "Neve debole",
    "snow": "Neve",
    "heavysnow": "Neve forte",
    "lightsnowandthunder": "Neve debole e temporali",
    "snowandthunder": "Neve e temporali",
    "heavysnowandthunder": "Neve forte e temporali",
}


def _metno_description(symbol: Any) -> str:
    value = str(symbol or "").strip().casefold()
    for suffix in ("_polartwilight", "_night", "_day"):
        if value.endswith(suffix):
            value = value[: -len(suffix)]
            break
    return METNO_DESCRIPTIONS_IT.get(value, "Variabile")


def _metno_period(data: dict[str, Any]) -> tuple[dict[str, Any], str]:
    """Use the most detailed available precipitation/summary period."""
    for name in ("next_1_hours", "next_6_hours", "next_12_hours"):
        period = data.get(name)
        if isinstance(period, dict):
            details = dict(period.get("details") or {})
            if details.get("probability_of_precipitation") is None:
                for probability_period in ("next_6_hours", "next_12_hours"):
                    probability = (
                        (data.get(probability_period) or {}).get("details") or {}
                    ).get("probability_of_precipitation")
                    if probability is not None:
                        details["probability_of_precipitation"] = probability
                        break
            symbol = (period.get("summary") or {}).get("symbol_code") or ""
            return details, str(symbol)
    return {}, ""


def _speed_kmh(value: Any) -> float:
    speed = _as_float(value)
    return speed * 3.6 if speed is not None else np.nan


def _metno_hourly(payload: dict[str, Any], timezone: str) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for item in (payload.get("properties") or {}).get("timeseries") or []:
        data = item.get("data") or {}
        details = (data.get("instant") or {}).get("details") or {}
        period, symbol = _metno_period(data)
        rows.append(
            {
                "time": item.get("time"),
                "temp_c": details.get("air_temperature"),
                "feels_like_c": details.get("air_temperature"),
                "humidity": details.get("relative_humidity"),
                "precip_probability": period.get("probability_of_precipitation"),
                "precipitation_mm": period.get("precipitation_amount"),
                "rain_mm": period.get("precipitation_amount"),
                "weather_code": np.nan,
                "description": _metno_description(symbol),
                "clouds": details.get("cloud_area_fraction"),
                "pressure_hpa": details.get("air_pressure_at_sea_level"),
                "visibility_m": np.nan,
                "wind_kmh": _speed_kmh(details.get("wind_speed")),
                "wind_dir": details.get("wind_from_direction"),
                "wind_gust_kmh": _speed_kmh(details.get("wind_speed_of_gust")),
                "uv_index": details.get("ultraviolet_index_clear_sky"),
                "symbol": symbol,
            }
        )
    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(columns=["time", *HOURLY_FIELDS.values()])
    frame["time"] = _local_times(frame["time"], timezone)
    for column in frame.columns.difference(["time", "description", "symbol"]):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame["precipitation_mm"] = frame["precipitation_mm"].clip(lower=0)
    frame["rain_mm"] = frame["rain_mm"].clip(lower=0)
    frame["precip_probability"] = frame["precip_probability"].clip(0, 100)
    return frame.dropna(subset=["time"]).sort_values("time").reset_index(drop=True)


def _circular_mean(values: pd.Series) -> float:
    numbers = pd.to_numeric(values, errors="coerce").dropna()
    if numbers.empty:
        return np.nan
    radians = np.deg2rad(numbers % 360)
    angle = math.degrees(math.atan2(np.sin(radians).mean(), np.cos(radians).mean()))
    return angle % 360


def _sun_times(
    day: date,
    latitude: float,
    longitude: float,
    timezone: str,
) -> tuple[pd.Timestamp, pd.Timestamp]:
    try:
        values = sun(
            Observer(latitude=latitude, longitude=longitude),
            date=day,
            tzinfo=ZoneInfo(timezone),
        )
        return pd.Timestamp(values["sunrise"]), pd.Timestamp(values["sunset"])
    except (KeyError, ValueError):
        return pd.NaT, pd.NaT


def _metno_daily(
    hourly: pd.DataFrame,
    location: CityLocation,
) -> pd.DataFrame:
    if hourly.empty:
        return pd.DataFrame(columns=["time", *DAILY_FIELDS.values()])
    rows: list[dict[str, Any]] = []
    frame = hourly.copy()
    frame["local_date"] = frame["time"].dt.date
    for day, group in frame.groupby("local_date", sort=True):
        midday_index = (group["time"].dt.hour - 12).abs().idxmin()
        midday = group.loc[midday_index]
        sunrise, sunset = _sun_times(
            day,
            location.latitude,
            location.longitude,
            location.timezone,
        )
        rows.append(
            {
                "time": pd.Timestamp(day).tz_localize(ZoneInfo(location.timezone)),
                "weather_code": np.nan,
                "description": midday.get("description", "Variabile"),
                "temp_max_c": pd.to_numeric(group["temp_c"], errors="coerce").max(),
                "temp_min_c": pd.to_numeric(group["temp_c"], errors="coerce").min(),
                "feels_max_c": pd.to_numeric(
                    group["feels_like_c"], errors="coerce"
                ).max(),
                "feels_min_c": pd.to_numeric(
                    group["feels_like_c"], errors="coerce"
                ).min(),
                "sunrise": sunrise,
                "sunset": sunset,
                "precipitation_mm": pd.to_numeric(
                    group["precipitation_mm"], errors="coerce"
                ).sum(min_count=1),
                "rain_mm": pd.to_numeric(group["rain_mm"], errors="coerce").sum(
                    min_count=1
                ),
                "precip_probability": pd.to_numeric(
                    group["precip_probability"], errors="coerce"
                ).max(),
                "wind_max_kmh": pd.to_numeric(group["wind_kmh"], errors="coerce").max(),
                "wind_gust_max_kmh": pd.to_numeric(
                    group["wind_gust_kmh"], errors="coerce"
                ).max(),
                "wind_dir": _circular_mean(group["wind_dir"]),
                "uv_index_max": pd.to_numeric(group["uv_index"], errors="coerce").max(),
            }
        )
    return pd.DataFrame(rows).head(7)


def parse_metno_city_forecast(
    payload: dict[str, Any],
    location: CityLocation,
    fetched_at: pd.Timestamp | None = None,
) -> CityForecast:
    """Normalise MET Norway Locationforecast as a resilient city fallback."""
    hourly = _metno_hourly(payload, location.timezone)
    if hourly.empty:
        raise CityWeatherError("MET Norway: previsione vuota")
    daily = _metno_daily(hourly, location)
    first = hourly.iloc[0]
    symbol = str(first.get("symbol") or "")
    current = {target: first.get(target) for target in CURRENT_FIELDS.values()}
    current.update(
        {
            "time": first["time"],
            "description": first.get("description", "Variabile"),
            "is_day": 0 if symbol.endswith("_night") else 1,
        }
    )
    updated_at = ((payload.get("properties") or {}).get("meta") or {}).get("updated_at")
    fetched = pd.to_datetime(fetched_at or updated_at, utc=True, errors="coerce")
    if pd.isna(fetched):
        fetched = pd.Timestamp.now(tz="UTC")
    return CityForecast(
        timezone=location.timezone,
        current=current,
        hourly=hourly,
        daily=daily,
        fetched_at=pd.Timestamp(fetched),
        source="MET Norway",
    )


def _fetch_metno_city_forecast(
    location: CityLocation,
    session: requests.Session,
) -> CityForecast:
    params: dict[str, Any] = {
        "lat": round(location.latitude, 5),
        "lon": round(location.longitude, 5),
    }
    if location.elevation_m is not None:
        params["altitude"] = round(location.elevation_m)
    payload = _get_json(
        session,
        "https://api.met.no/weatherapi/locationforecast/2.0/compact",
        params,
        "MET Norway",
        headers={"User-Agent": "weather-app-v3/1.0 github.com/gio9772rm/weather-app"},
    )
    return parse_metno_city_forecast(payload, location)


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
        try:
            payload = _get_json(
                session,
                "https://api.open-meteo.com/v1/forecast",
                params,
                "Open-Meteo",
            )
            return parse_city_forecast(payload)
        except CityWeatherError as primary_error:
            LOGGER.warning(
                "Open-Meteo city forecast unavailable; using MET Norway fallback: %s",
                primary_error,
            )
            try:
                return _fetch_metno_city_forecast(location, session)
            except CityWeatherError as fallback_error:
                raise CityWeatherError(
                    "provider meteo temporaneamente non disponibili"
                ) from fallback_error
    finally:
        if own_session:
            session.close()
