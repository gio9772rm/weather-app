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


def _as_bool(value: str, default: bool) -> bool:
    normalised = str(value or "").strip().lower()
    if normalised in {"1", "true", "yes", "on", "si", "sì"}:
        return True
    if normalised in {"0", "false", "no", "off"}:
        return False
    return default


def _station_ids(value: str) -> tuple[str, ...]:
    return tuple(
        dict.fromkeys(
            item.strip().upper()
            for item in str(value or "").split(",")
            if item.strip()
        )
    )


def _positive_integer_ids(value: str) -> tuple[int, ...]:
    """Parse a comma-separated list of positive numeric identifiers."""
    identifiers: list[int] = []
    for item in str(value or "").split(","):
        try:
            identifier = int(item.strip())
        except (TypeError, ValueError):
            continue
        if identifier > 0 and identifier not in identifiers:
            identifiers.append(identifier)
    return tuple(identifiers)


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
    station_max_source_age_minutes: int = 20
    official_observations_enabled: bool = True
    metar_station_ids: tuple[str, ...] = ("LIRF", "LIRA")
    official_observation_refresh_minutes: int = 30
    official_observation_lookback_hours: int = 48
    official_score_max_share: float = 0.20
    official_min_overlap_samples: int = 24
    arsial_observations_enabled: bool = True
    arsial_dashboard_url: str = (
        "https://siarl.arsial.it/bi/superset/dashboard/7"
    )
    arsial_station_name: str = "ROMA Lanciani-SEDE ARSIAL"
    arsial_timezone: str = "UTC"
    arsial_station_registry_url: str = (
        "https://dati.lazio.it/dataset/4f4194c2-8432-4f99-aab3-2b07da7df3fd/"
        "resource/1373f7a6-f208-40f0-8d0c-e41cf25d2599/download/"
        "anagraficastazioniagrometeoarsial.csv"
    )
    arsial_csv_url: str = ""
    arsial_chart_ids: tuple[int, ...] = ()
    arsial_cache_hours: int = 72
    cfr_observations_enabled: bool = False
    cfr_observations_url: str = ""
    cfr_api_token: str = ""
    cfr_station_ids: tuple[str, ...] = ()

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            latitude=_as_float(_first_env("LAT", "LATITUDE"), 41.9028),
            longitude=_as_float(_first_env("LON", "LONGITUDE"), 12.4964),
            elevation_m=_as_float(_first_env("ELEVATION_M"), 20.0),
            local_timezone=_first_env("LOCAL_TZ", default="Europe/Rome"),
            location_name=_first_env("LOCATION_NAME", default="Stazione meteo"),
            openweather_api_key=_first_env(
                "OPENWEATHER_API_KEY", "OWM_API_KEY", "OW_API_KEY"
            ),
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
                10, _as_int(_first_env("STATION_STALE_MINUTES"), 20)
            ),
            admin_token=_first_env("ADMIN_TOKEN"),
            station_max_source_age_minutes=max(
                10,
                _as_int(_first_env("STATION_MAX_SOURCE_AGE_MINUTES"), 20),
            ),
            official_observations_enabled=_as_bool(
                _first_env("OFFICIAL_OBSERVATIONS_ENABLED", default="true"), True
            ),
            metar_station_ids=_station_ids(
                _first_env("METAR_STATIONS", default="LIRF,LIRA")
            ),
            official_observation_refresh_minutes=max(
                15,
                _as_int(
                    _first_env("OFFICIAL_OBSERVATION_REFRESH_MINUTES"), 30
                ),
            ),
            official_observation_lookback_hours=max(
                3,
                min(
                    720,
                    _as_int(
                        _first_env("OFFICIAL_OBSERVATION_LOOKBACK_HOURS"), 48
                    ),
                ),
            ),
            official_score_max_share=min(
                0.35,
                max(
                    0.0,
                    _as_float(_first_env("OFFICIAL_SCORE_MAX_SHARE"), 0.20),
                ),
            ),
            official_min_overlap_samples=max(
                6,
                _as_int(_first_env("OFFICIAL_MIN_OVERLAP_SAMPLES"), 24),
            ),
            arsial_observations_enabled=_as_bool(
                _first_env("ARSIAL_OBSERVATIONS_ENABLED", default="true"), True
            ),
            arsial_dashboard_url=_first_env(
                "ARSIAL_DASHBOARD_URL",
                default="https://siarl.arsial.it/bi/superset/dashboard/7",
            ),
            arsial_station_name=_first_env(
                "ARSIAL_STATION_NAME",
                default="ROMA Lanciani-SEDE ARSIAL",
            ),
            arsial_timezone=_first_env("ARSIAL_TZ", default="UTC"),
            arsial_station_registry_url=_first_env(
                "ARSIAL_STATION_REGISTRY_URL",
                default=(
                    "https://dati.lazio.it/dataset/"
                    "4f4194c2-8432-4f99-aab3-2b07da7df3fd/resource/"
                    "1373f7a6-f208-40f0-8d0c-e41cf25d2599/download/"
                    "anagraficastazioniagrometeoarsial.csv"
                ),
            ),
            arsial_csv_url=_first_env("ARSIAL_CSV_URL"),
            arsial_chart_ids=_positive_integer_ids(
                _first_env("ARSIAL_CHART_IDS")
            ),
            arsial_cache_hours=max(
                6,
                min(
                    168,
                    _as_int(_first_env("ARSIAL_CACHE_HOURS"), 72),
                ),
            ),
            cfr_observations_enabled=_as_bool(
                _first_env("CFR_OBSERVATIONS_ENABLED", default="false"), False
            ),
            cfr_observations_url=_first_env("CFR_OBSERVATIONS_URL"),
            cfr_api_token=_first_env("CFR_API_TOKEN"),
            cfr_station_ids=_station_ids(_first_env("CFR_STATION_IDS")),
        )

    @property
    def has_station_credentials(self) -> bool:
        return bool(
            self.ecowitt_application_key and self.ecowitt_api_key and self.ecowitt_mac
        )


settings = Settings.from_env()
