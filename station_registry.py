"""Additive station registry and station-scoped observation mirror."""

from __future__ import annotations

import re
from typing import Any

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from config import Settings, settings
from db import get_engine


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
    cfg: Settings = settings, engine: Engine | None = None
) -> str:
    return register_station(
        station_id=cfg.station_id,
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


def sync_primary_station_history(
    cfg: Settings = settings, engine: Engine | None = None
) -> int:
    """Incrementally mirror legacy Ecowitt rows into the station-aware store.

    ``station_raw`` remains the authoritative primary-station store.  The mirror is
    additive and must never make the live Ecowitt ingest depend on a full historical
    scan every five minutes.
    """
    engine = engine or get_engine()
    identifier = ensure_primary_station(cfg, engine)
    with engine.begin() as connection:
        latest_mirrored = connection.execute(
            text(
                "SELECT MAX(time) FROM station_observations "
                "WHERE station_id=:station_id"
            ),
            {"station_id": identifier},
        ).scalar()
        result = connection.execute(
            text(
                "INSERT INTO station_observations (station_id,time,temp_c,humidity,"
                "pressure_hpa,wind_kmh,windgust_kmh,winddir,rain_mm,wind_ms,"
                "rain_rate_mm_h,rain_total_mm,solar_w_m2,uv_index,source,data_quality) "
                "SELECT :station_id,r.time,r.temp_c,r.humidity,r.pressure_hpa,"
                "r.wind_kmh,r.windgust_kmh,r.winddir,r.rain_mm,r.wind_ms,"
                "r.rain_rate_mm_h,r.rain_total_mm,r.solar_w_m2,r.uv_index,"
                "r.source,r.data_quality FROM station_raw r "
                "WHERE (:latest_mirrored IS NULL OR r.time > :latest_mirrored) "
                "AND NOT EXISTS (SELECT 1 FROM station_observations s "
                "WHERE s.station_id=:station_id AND s.time=r.time)"
            ),
            {"station_id": identifier, "latest_mirrored": latest_mirrored},
        )
    return max(0, int(result.rowcount or 0))
