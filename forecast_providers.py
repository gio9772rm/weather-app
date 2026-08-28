"""Forecast providers normalised to one internal hourly/three-hourly format."""

from __future__ import annotations

from time import perf_counter
from typing import Any

import numpy as np
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import Settings, settings
from source_health import record_source_disabled, record_source_result

FORECAST_COLUMNS = [
    "provider",
    "model",
    "issued_at",
    "valid_time",
    "interval_hours",
    "lead_hours",
    "temp_c",
    "feels_like_c",
    "humidity",
    "dewpoint_c",
    "pressure_hpa",
    "wind_kmh",
    "wind_gust_kmh",
    "wind_dir",
    "rain_mm",
    "snow_mm",
    "precip_probability",
    "clouds",
    "cloud_low",
    "cloud_mid",
    "cloud_high",
    "visibility_m",
    "cape_j_kg",
    "freezing_level_m",
    "wind_300hpa_kmh",
    "humidity_700hpa",
    "geopotential_500hpa_m",
    "temperature_850hpa_c",
    "weather_code",
    "description",
    "is_day",
    "fetched_at",
]


class ForecastProviderError(RuntimeError):
    """A provider failed without leaking query parameters or API keys."""


def build_session(*, retries: int = 3) -> requests.Session:
    session = requests.Session()
    retries = max(0, min(int(retries), 5))
    retry = Retry(
        total=retries,
        connect=retries,
        read=retries,
        backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        # Every endpoint used by this application is read-only.  EEA exposes
        # some data queries as POST, so those requests can be retried safely too.
        allowed_methods=frozenset({"GET", "HEAD", "POST"}),
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=8, pool_maxsize=8)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"User-Agent": "weather-app-v4/1.0"})
    return session


def _get_json(
    session: requests.Session,
    url: str,
    params: dict[str, Any],
    provider: str,
) -> dict[str, Any]:
    try:
        response = session.get(url, params=params, timeout=(8, 30))
        if not response.ok:
            raise ForecastProviderError(
                f"{provider}: risposta HTTP {response.status_code}"
            )
        payload = response.json()
    except ForecastProviderError:
        raise
    except (requests.RequestException, ValueError) as exc:
        raise ForecastProviderError(f"{provider}: servizio non raggiungibile") from exc
    if not isinstance(payload, dict):
        raise ForecastProviderError(f"{provider}: risposta non valida")
    return payload


def _utc_now() -> pd.Timestamp:
    return pd.Timestamp.now(tz="UTC")


def _dewpoint(temp_c: pd.Series, humidity: pd.Series) -> pd.Series:
    temperature = pd.to_numeric(temp_c, errors="coerce")
    rh = pd.to_numeric(humidity, errors="coerce").clip(1, 100)
    gamma = np.log(rh / 100.0) + (17.625 * temperature) / (243.04 + temperature)
    return 243.04 * gamma / (17.625 - gamma)


WMO_DESCRIPTIONS_IT = {
    0: "Sereno",
    1: "Prevalentemente sereno",
    2: "Parzialmente nuvoloso",
    3: "Coperto",
    45: "Nebbia",
    48: "Nebbia con brina",
    51: "Pioviggine debole",
    53: "Pioviggine",
    55: "Pioviggine intensa",
    56: "Pioviggine gelata debole",
    57: "Pioviggine gelata intensa",
    61: "Pioggia debole",
    63: "Pioggia moderata",
    65: "Pioggia forte",
    66: "Pioggia gelata debole",
    67: "Pioggia gelata forte",
    71: "Neve debole",
    73: "Neve moderata",
    75: "Neve forte",
    77: "Granuli di neve",
    80: "Rovesci deboli",
    81: "Rovesci moderati",
    82: "Rovesci violenti",
    85: "Rovesci di neve deboli",
    86: "Rovesci di neve forti",
    95: "Temporale",
    96: "Temporale con grandine",
    99: "Temporale forte con grandine",
}


def parse_open_meteo(
    payload: dict[str, Any],
    fetched_at: pd.Timestamp | None = None,
    *,
    provider: str = "open_meteo",
    model: str | None = None,
) -> pd.DataFrame:
    hourly = payload.get("hourly") or {}
    times = hourly.get("time") or []
    if not times:
        return pd.DataFrame(columns=FORECAST_COLUMNS)

    fetched = fetched_at or _utc_now()
    fetched = (
        pd.Timestamp(fetched).tz_convert("UTC")
        if pd.Timestamp(fetched).tzinfo
        else pd.Timestamp(fetched).tz_localize("UTC")
    )
    issued = fetched.floor("h")
    field_map = {
        "temperature_2m": "temp_c",
        "apparent_temperature": "feels_like_c",
        "relative_humidity_2m": "humidity",
        "dew_point_2m": "dewpoint_c",
        "pressure_msl": "pressure_hpa",
        "wind_speed_10m": "wind_kmh",
        "wind_gusts_10m": "wind_gust_kmh",
        "wind_direction_10m": "wind_dir",
        "rain": "rain_mm",
        "snowfall": "snow_mm",
        "precipitation_probability": "precip_probability",
        "cloud_cover": "clouds",
        "cloud_cover_low": "cloud_low",
        "cloud_cover_mid": "cloud_mid",
        "cloud_cover_high": "cloud_high",
        "visibility": "visibility_m",
        "cape": "cape_j_kg",
        "freezing_level_height": "freezing_level_m",
        "wind_speed_300hPa": "wind_300hpa_kmh",
        "relative_humidity_700hPa": "humidity_700hpa",
        "geopotential_height_500hPa": "geopotential_500hpa_m",
        "temperature_850hPa": "temperature_850hpa_c",
        "weather_code": "weather_code",
        "is_day": "is_day",
    }
    frame = pd.DataFrame(
        {"valid_time": pd.to_datetime(times, utc=True, errors="coerce")}
    )
    for source, target in field_map.items():
        values = hourly.get(source)
        if isinstance(values, list) and len(values) == len(frame):
            frame[target] = values
        else:
            frame[target] = np.nan

    codes = pd.to_numeric(frame["weather_code"], errors="coerce")
    frame["weather_code"] = codes.astype("Int64").astype("string")
    frame["description"] = codes.map(WMO_DESCRIPTIONS_IT).fillna("Variabile")
    frame["provider"] = provider
    frame["model"] = str(model or payload.get("model") or "best_match")
    frame["issued_at"] = issued
    frame["interval_hours"] = 1.0
    frame["lead_hours"] = (frame["valid_time"] - issued).dt.total_seconds() / 3600.0
    frame["fetched_at"] = fetched
    if frame["dewpoint_c"].isna().all():
        frame["dewpoint_c"] = _dewpoint(frame["temp_c"], frame["humidity"])
    return frame.reindex(columns=FORECAST_COLUMNS).dropna(subset=["valid_time"])


OPEN_METEO_HOURLY = (
    "temperature_2m",
    "apparent_temperature",
    "relative_humidity_2m",
    "dew_point_2m",
    "precipitation_probability",
    "rain",
    "snowfall",
    "weather_code",
    "cloud_cover",
    "cloud_cover_low",
    "cloud_cover_mid",
    "cloud_cover_high",
    "pressure_msl",
    "visibility",
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m",
    "is_day",
    "cape",
    "freezing_level_height",
    "wind_speed_300hPa",
    "relative_humidity_700hPa",
    "geopotential_height_500hPa",
    "temperature_850hPa",
)


def _open_meteo_params(cfg: Settings, model: str) -> dict[str, Any]:
    return {
        "latitude": cfg.latitude,
        "longitude": cfg.longitude,
        "elevation": cfg.elevation_m,
        "timezone": "UTC",
        "forecast_days": cfg.forecast_days,
        "temperature_unit": "celsius",
        "wind_speed_unit": "kmh",
        "precipitation_unit": "mm",
        "models": model,
        "hourly": ",".join(OPEN_METEO_HOURLY),
    }


def fetch_open_meteo(
    cfg: Settings = settings, session: requests.Session | None = None
) -> pd.DataFrame:
    own_session = session is None
    session = session or build_session()
    params = _open_meteo_params(cfg, "best_match")
    try:
        payload = _get_json(
            session,
            "https://api.open-meteo.com/v1/forecast",
            params,
            "Open-Meteo",
        )
        return parse_open_meteo(payload, provider="open_meteo", model="best_match")
    finally:
        if own_session:
            session.close()


def fetch_icon_2i(
    cfg: Settings = settings, session: requests.Session | None = None
) -> pd.DataFrame:
    """Fetch the explicit 2.2 km ItaliaMeteo/ARPAE ICON-2I run."""
    own_session = session is None
    session = session or build_session()
    params = _open_meteo_params(cfg, "italia_meteo_arpae_icon_2i")
    params.pop("forecast_days", None)
    params["forecast_hours"] = 72
    try:
        payload = _get_json(
            session,
            "https://api.open-meteo.com/v1/forecast",
            params,
            "ItaliaMeteo ICON-2I",
        )
        return parse_open_meteo(
            payload,
            provider="italiameteo_icon2i",
            model="icon_2i_2p2km",
        )
    finally:
        if own_session:
            session.close()


def parse_openweather(
    payload: dict[str, Any], fetched_at: pd.Timestamp | None = None
) -> pd.DataFrame:
    items = payload.get("list") or []
    if not items:
        return pd.DataFrame(columns=FORECAST_COLUMNS)
    fetched = fetched_at or _utc_now()
    fetched = (
        pd.Timestamp(fetched).tz_convert("UTC")
        if pd.Timestamp(fetched).tzinfo
        else pd.Timestamp(fetched).tz_localize("UTC")
    )
    issued = fetched.floor("h")
    rows: list[dict[str, Any]] = []
    for item in items:
        main = item.get("main") or {}
        wind = item.get("wind") or {}
        rain = item.get("rain") or {}
        snow = item.get("snow") or {}
        weather = (item.get("weather") or [{}])[0] or {}
        valid_time = pd.to_datetime(item.get("dt"), unit="s", utc=True, errors="coerce")
        rows.append(
            {
                "provider": "openweather",
                "model": "owm_5d_3h",
                "issued_at": issued,
                "valid_time": valid_time,
                "interval_hours": 3.0,
                "lead_hours": (
                    (valid_time - issued).total_seconds() / 3600.0
                    if not pd.isna(valid_time)
                    else np.nan
                ),
                "temp_c": main.get("temp"),
                "feels_like_c": main.get("feels_like"),
                "humidity": main.get("humidity"),
                "dewpoint_c": np.nan,
                "pressure_hpa": main.get("sea_level") or main.get("pressure"),
                "wind_kmh": float(wind.get("speed")) * 3.6
                if wind.get("speed") is not None
                else np.nan,
                "wind_gust_kmh": float(wind.get("gust")) * 3.6
                if wind.get("gust") is not None
                else np.nan,
                "wind_dir": wind.get("deg"),
                "rain_mm": rain.get("3h", 0.0),
                "snow_mm": snow.get("3h", 0.0),
                "precip_probability": float(item.get("pop", 0.0)) * 100.0,
                "clouds": (item.get("clouds") or {}).get("all"),
                "cloud_low": np.nan,
                "cloud_mid": np.nan,
                "cloud_high": np.nan,
                "visibility_m": item.get("visibility"),
                "weather_code": str(weather.get("id") or ""),
                "description": str(
                    weather.get("description") or "Variabile"
                ).capitalize(),
                "is_day": np.nan,
                "fetched_at": fetched,
            }
        )
    frame = pd.DataFrame(rows).reindex(columns=FORECAST_COLUMNS)
    frame["dewpoint_c"] = _dewpoint(frame["temp_c"], frame["humidity"])
    return frame.dropna(subset=["valid_time"])


def fetch_openweather(
    cfg: Settings = settings, session: requests.Session | None = None
) -> pd.DataFrame:
    if not cfg.openweather_api_key:
        return pd.DataFrame(columns=FORECAST_COLUMNS)
    own_session = session is None
    session = session or build_session()
    params = {
        "lat": cfg.latitude,
        "lon": cfg.longitude,
        "appid": cfg.openweather_api_key,
        "units": "metric",
        "lang": "it",
    }
    try:
        payload = _get_json(
            session,
            "https://api.openweathermap.org/data/2.5/forecast",
            params,
            "OpenWeather",
        )
        return parse_openweather(payload)
    finally:
        if own_session:
            session.close()


def fetch_all_forecasts(
    cfg: Settings = settings,
) -> tuple[list[pd.DataFrame], list[str]]:
    """Fetch every configured provider; one failure never hides other results."""
    frames: list[pd.DataFrame] = []
    errors: list[str] = []
    session = build_session()
    try:
        for name, source, enabled, fetcher in (
            (
                "ItaliaMeteo ICON-2I",
                "open_meteo_icon2i",
                True,
                fetch_icon_2i,
            ),
            ("Open-Meteo", "open_meteo", True, fetch_open_meteo),
            (
                "OpenWeather",
                "openweather",
                bool(cfg.openweather_api_key),
                fetch_openweather,
            ),
        ):
            if not enabled:
                record_source_disabled(source)
                continue
            started = perf_counter()
            try:
                frame = fetcher(cfg, session)
                if not frame.empty:
                    frames.append(frame)
                record_source_result(
                    source,
                    success=not frame.empty,
                    rows_received=len(frame),
                    last_observation_at=(
                        frame["valid_time"].max() if not frame.empty else None
                    ),
                    latency_ms=(perf_counter() - started) * 1000,
                    error="risposta valida ma vuota" if frame.empty else "",
                )
            except ForecastProviderError as exc:
                errors.append(str(exc))
                record_source_result(
                    source,
                    success=False,
                    latency_ms=(perf_counter() - started) * 1000,
                    error=exc,
                )
            except Exception:  # noqa: BLE001 - isolate independent providers
                errors.append(f"{name}: errore inatteso")
                record_source_result(
                    source,
                    success=False,
                    latency_ms=(perf_counter() - started) * 1000,
                    error=f"{name}: errore inatteso",
                )
    finally:
        session.close()
    return frames, errors
