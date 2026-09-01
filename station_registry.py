"""Additive station registry and station-scoped observation mirror."""

from __future__ import annotations

import logging
import re
from typing import Any

import pandas as pd
from sqlalchemy import DateTime, inspect, text
from sqlalchemy.engine import Engine

from config import Settings, settings
from db import get_engine

log = logging.getLogger("station_registry")


def normalise_station_id(value: Any) -> str:
    identifier = re.sub(r"[^a-z0-9-]+", "-", str(value or "").strip().lower())
    identifier = re.sub(r"-+", "-", identifier).strip("-")
    return identifier[:80] or "primary-station"


def register_station(
    *,
    station_id: str,
    display_name: str,
    latitude: float,
    longitude: float,
    elevation_m: float,
    timezone: str,
    source: str,
    role: str = "secondary",
    enabled: bool = True,
    engine: Engine | None = None,
) -> str:
    """Register a station without exposing its exact location in the UI."""
    engine = engine or get_engine()
    identifier = normalise_station_id(station_id)
    now = pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%dT%H:%M:%SZ")
    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO station_profiles (station_id,display_name,latitude,"
                "longitude,elevation_m,timezone,source,role,enabled,privacy_level,"
                "created_at,updated_at) VALUES (:station_id,:display_name,:latitude,"
                ":longitude,:elevation_m,:timezone,:source,:role,:enabled,"
                "'private_location',:now,:now) ON CONFLICT (station_id) DO UPDATE SET "
                "display_name=excluded.display_name,latitude=excluded.latitude,"
                "longitude=excluded.longitude,elevation_m=excluded.elevation_m,"
                "timezone=excluded.timezone,source=excluded.source,role=excluded.role,"
                "enabled=excluded.enabled,privacy_level=excluded.privacy_level,"
                "updated_at=excluded.updated_at"
            ),
            {
                "station_id": identifier,
                "display_name": str(display_name)[:160],
                "latitude": float(latitude),
                "longitude": float(longitude),
                "elevation_m": float(elevation_m),
                "timezone": str(timezone)[:80],
                "source": str(source)[:80],
                "role": "primary" if role == "primary" else "secondary",
                "enabled": int(bool(enabled)),
                "now": now,
            },
        )
    return identifier


def ensure_primary_station(
    cfg: Settings = settings,
    engine: Engine | None = None,
    *,
    strict: bool = False,
) -> str:
    """Ensure the primary profile exists without blocking live Ecowitt ingestion.

    The registry is an additive convenience layer. Unless ``strict`` is requested,
    a registry/database migration problem is logged and the stable station id is
    returned so the authoritative ``station_raw`` ingestion can continue.
    """
    identifier = normalise_station_id(cfg.station_id)
    try:
        return register_station(
            station_id=identifier,
            display_name=cfg.location_name,
            latitude=cfg.latitude,
            longitude=cfg.longitude,
            elevation_m=cfg.elevation_m,
            timezone=cfg.local_timezone,
            source="ecowitt",
            role="primary",
            enabled=True,
            engine=engine,
        )
    except Exception as exc:
        if strict:
            raise
        log.warning("Registro stazione non aggiornato: %s", str(exc)[:300])
        return identifier


def ensure_secondary_station(
    cfg: Settings,
    engine: Engine | None = None,
    *,
    strict: bool = False,
) -> str:
    """Register an enabled secondary Ecowitt without changing the primary role."""
    identifier = normalise_station_id(cfg.station_id)
    try:
        return register_station(
            station_id=identifier,
            display_name=cfg.location_name,
            latitude=cfg.latitude,
            longitude=cfg.longitude,
            elevation_m=cfg.elevation_m,
            timezone=cfg.local_timezone,
            source="ecowitt",
            role="secondary",
            enabled=True,
            engine=engine,
        )
    except Exception as exc:
        if strict:
            raise
        log.warning("Registro stazione secondaria non aggiornato: %s", str(exc)[:300])
        return identifier


def _raw_station_time_sql(engine: Engine) -> tuple[str, str]:
    """Return compatible raw-time expressions for the installed target schema.

    Older PostgreSQL deployments can already have ``station_observations.time``
    as ``TIMESTAMPTZ`` while fresh V4 databases use the portable text schema.
    SQLite always keeps the ISO-8601 source string unchanged.
    """
    if engine.dialect.name != "postgresql":
        return "r.time", ":cutoff"
    columns = inspect(engine).get_columns("station_observations")
    time_column = next(
        (column for column in columns if str(column.get("name", "")).lower() == "time"),
        None,
    )
    if time_column is not None and isinstance(time_column.get("type"), DateTime):
        cast = "CAST(r.time AS TIMESTAMP WITH TIME ZONE)"
        return cast, "CAST(:cutoff AS TIMESTAMP WITH TIME ZONE)"
    return "r.time", ":cutoff"


def sync_primary_station_history(
    cfg: Settings = settings,
    engine: Engine | None = None,
    *,
    strict: bool = False,
    lookback_hours: int = 168,
) -> int:
    """Mirror primary Ecowitt rows without scanning all history every cycle.

    ``station_raw`` remains the authoritative primary-station store. On the first
    synchronization all available history is copied; later runs only inspect a
    rolling lookback window ending at the newest mirrored observation. This keeps
    ten-minute ingestion light while still recovering late/backfilled samples.
    Mirror failures are isolated from live Ecowitt ingestion unless ``strict=True``.
    """
    engine = engine or get_engine()
    identifier = ensure_primary_station(cfg, engine, strict=strict)
    try:
        raw_time_sql, cutoff_sql = _raw_station_time_sql(engine)
        with engine.begin() as connection:
            latest_mirrored = connection.execute(
                text(
                    "SELECT MAX(time) FROM station_observations "
                    "WHERE station_id=:station_id"
                ),
                {"station_id": identifier},
            ).scalar()

            cutoff = None
            if latest_mirrored:
                latest_ts = pd.to_datetime(latest_mirrored, utc=True, errors="coerce")
                if not pd.isna(latest_ts):
                    cutoff = (
                        latest_ts - pd.Timedelta(hours=max(1, int(lookback_hours)))
                    ).strftime("%Y-%m-%dT%H:%M:%SZ")

            cutoff_clause = (
                "" if cutoff is None else f" AND {raw_time_sql} >= {cutoff_sql}"
            )
            result = connection.execute(
                text(
                    "INSERT INTO station_observations (station_id,time,temp_c,humidity,"
                    "pressure_hpa,wind_kmh,windgust_kmh,winddir,rain_mm,wind_ms,"
                    "rain_rate_mm_h,rain_total_mm,solar_w_m2,uv_index,source,data_quality) "
                    f"SELECT :station_id,{raw_time_sql},r.temp_c,r.humidity,r.pressure_hpa,"
                    "r.wind_kmh,r.windgust_kmh,r.winddir,r.rain_mm,r.wind_ms,"
                    "r.rain_rate_mm_h,r.rain_total_mm,r.solar_w_m2,r.uv_index,"
                    "r.source,r.data_quality FROM station_raw r "
                    "WHERE 1=1"
                    + cutoff_clause
                    + " ON CONFLICT (station_id,time) DO NOTHING"
                ),
                {"station_id": identifier, "cutoff": cutoff},
            )
        return max(0, int(result.rowcount or 0))
    except Exception as exc:
        if strict:
            raise
        log.warning("Mirror stazione non aggiornato: %s", str(exc)[:300])
        return 0
