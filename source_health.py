"""Non-blocking operational health tracking for every independent data source."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from config import Settings, settings
from db import get_engine


@dataclass(frozen=True)
class SourceDefinition:
    source: str
    label: str
    enabled: bool
    expected_minutes: int
    category: str


def configured_sources(cfg: Settings = settings) -> tuple[SourceDefinition, ...]:
    """Return the sources that the current deployment knows how to operate."""
    official = cfg.official_observations_enabled
    return (
        SourceDefinition("ecowitt", "Ecowitt", cfg.has_station_credentials, 5, "misure"),
        SourceDefinition("open_meteo", "Open-Meteo", True, cfg.forecast_refresh_minutes, "previsioni"),
        SourceDefinition("openweather", "OpenWeather", bool(cfg.openweather_api_key), cfg.forecast_refresh_minutes, "previsioni"),
        SourceDefinition("awc_metar", "METAR LIRF/LIRA", official, cfg.official_observation_refresh_minutes, "riferimenti"),
        SourceDefinition("arsial_siarl", "ARSIAL/SIARL", official and cfg.arsial_observations_enabled, cfg.official_observation_refresh_minutes, "riferimenti"),
        SourceDefinition("cfr_lazio", "CFR Lazio", official and cfg.cfr_observations_enabled, 15, "riferimenti"),
        SourceDefinition("forecast_blend", "Previsione combinata", True, cfg.forecast_refresh_minutes, "elaborazione"),
        SourceDefinition(
            "database_backup",
            "Ultimo backup verificato",
            True,
            7 * 24 * 60,
            "protezione",
        ),
    )


def _iso(value: Any) -> str | None:
    timestamp = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(timestamp):
        return None
    return timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")


def _safe_text(value: Any, limit: int = 500) -> str:
    return " ".join(str(value or "").split())[:limit]


def record_source_result(
    source: str,
    *,
    success: bool,
    rows_received: int = 0,
    last_observation_at: Any = None,
    latency_ms: float | None = None,
    error: Any = "",
    engine: Engine | None = None,
) -> bool:
    """Upsert one health result; telemetry failure never stops the pipeline."""
    attempted = _iso(pd.Timestamp.now(tz="UTC"))
    observed = _iso(last_observation_at)
    latency = (
        float(latency_ms)
        if latency_ms is not None and math.isfinite(float(latency_ms))
        else None
    )
    statement = text(
        "INSERT INTO source_health ("
        "source,last_attempt_at,last_success_at,last_observation_at,status,"
        "rows_received,latency_ms,consecutive_failures,last_error,updated_at"
        ") VALUES ("
        ":source,:attempted,:succeeded,:observed,:status,:rows,:latency,"
        ":failures,:error,:attempted) "
        "ON CONFLICT (source) DO UPDATE SET "
        "last_attempt_at=excluded.last_attempt_at,"
        "last_success_at=CASE WHEN excluded.status='online' "
        "THEN excluded.last_success_at ELSE source_health.last_success_at END,"
        "last_observation_at=COALESCE(excluded.last_observation_at,source_health.last_observation_at),"
        "status=excluded.status,rows_received=excluded.rows_received,"
        "latency_ms=excluded.latency_ms,"
        "consecutive_failures=CASE WHEN excluded.status='online' THEN 0 "
        "ELSE source_health.consecutive_failures+1 END,"
        "last_error=excluded.last_error,updated_at=excluded.updated_at"
    )
    payload = {
        "source": source,
        "attempted": attempted,
        "succeeded": attempted if success else None,
        "observed": observed,
        "status": "online" if success else "error",
        "rows": max(0, int(rows_received or 0)),
        "latency": latency,
        "failures": 0 if success else 1,
        "error": "" if success else _safe_text(error or "errore sconosciuto"),
    }
    try:
        with (engine or get_engine()).begin() as connection:
            connection.execute(statement, payload)
    except SQLAlchemyError:
        return False
    return True


def record_source_disabled(source: str, engine: Engine | None = None) -> bool:
    now = _iso(pd.Timestamp.now(tz="UTC"))
    try:
        with (engine or get_engine()).begin() as connection:
            connection.execute(
                text(
                    "INSERT INTO source_health (source,status,rows_received,"
                    "consecutive_failures,last_error,updated_at) "
                    "VALUES (:source,'disabled',0,0,'',:now) "
                    "ON CONFLICT (source) DO UPDATE SET status='disabled',"
                    "consecutive_failures=0,last_error='',updated_at=:now"
                ),
                {"source": source, "now": now},
            )
    except SQLAlchemyError:
        return False
    return True
