"""Centralised and validated application settings."""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


def _first_env(*names: str, default: str = "") -> str:
    for name in names:
        value = (os.getenv(name) or "").strip()
        if value:
            return value
    return default


def _as_float(value: str, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _as_int(value: str, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


@dataclass(frozen=True)
class Settings:
    latitude: float
    longitude: float
    elevation_m: float
    local_timezone: str
    location_name: str
    openweather_api_key: str
    ecowitt_application_key: str
    ecowitt_api_key: str
    ecowitt_mac: str
    forecast_days: int
    forecast_refresh_minutes: int
    score_lookback_days: int
    station_backfill_hours: int
    station_stale_minutes: int
    admin_token: str
    station_auto_backfill_max_hours: int = 168

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            latitude=_as_float(_first_env("LAT", "LATITUDE"), 41.9028),
            longitude=_as_float(_first_env("LON", "LONGITUDE"), 12.4964),
            elevation_m=_as_float(_first_env("ELEVATION_M"), 20.0),
            local_timezone=_first_env("LOCAL_TZ", default="Europe/Rome"),
            location_name=_first_env("LOCATION_NAME", default="Stazione meteo"),
            openweather_api_key=_first_env("OPENWEATHER_API_KEY", "OWM_API_KEY"),
            ecowitt_application_key=_first_env(
                "ECOWITT_APPLICATION_KEY", "ECOWITT_APP_KEY", "APPLICATION_KEY"
            ),
            ecowitt_api_key=_first_env("ECOWITT_API_KEY", "API_KEY"),
            ecowitt_mac=_first_env("ECOWITT_MAC", "MAC"),
            forecast_days=max(2, min(16, _as_int(_first_env("FORECAST_DAYS"), 7))),
            forecast_refresh_minutes=max(
                15, _as_int(_first_env("FORECAST_REFRESH_MINUTES"), 60)
            ),
            score_lookback_days=max(7, _as_int(_first_env("SCORE_LOOKBACK_DAYS"), 60)),
            station_backfill_hours=max(
                1, _as_int(_first_env("STATION_BACKFILL_HOURS"), 2)
            ),
            station_auto_backfill_max_hours=max(
                2,
                min(
                    168,
                    _as_int(_first_env("STATION_AUTO_BACKFILL_MAX_HOURS"), 168),
                ),
            ),
            station_stale_minutes=max(
                10, _as_int(_first_env("STATION_STALE_MINUTES"), 45)
            ),
            admin_token=_first_env("ADMIN_TOKEN"),
        )

    @property
    def has_station_credentials(self) -> bool:
        return bool(
            self.ecowitt_application_key and self.ecowitt_api_key and self.ecowitt_mac
        )


settings = Settings.from_env()
