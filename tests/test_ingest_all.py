from __future__ import annotations

from dataclasses import replace

import pandas as pd
from sqlalchemy import text

from config import Settings
from ingest_all import adaptive_station_backfill_hours


def _settings() -> Settings:
    return replace(
        Settings.from_env(),
        station_backfill_hours=2,
        station_auto_backfill_max_hours=24,
    )


def _insert_station_row(engine, moment: pd.Timestamp, **values) -> None:
    columns = ["time", *values]
    statement = text(
        f"INSERT INTO station_raw ({','.join(columns)}) "
        f"VALUES ({','.join(f':{column}' for column in columns)})"
    )
    payload = {
        "time": moment.strftime("%Y-%m-%dT%H:%M:%SZ"),
        **values,
    }
    with engine.begin() as connection:
        connection.execute(statement, payload)


def test_adaptive_backfill_keeps_normal_window_for_fresh_primary_sensors(
    sqlite_engine,
):
    now = pd.Timestamp("2026-08-21T12:00:00Z")
    _insert_station_row(
        sqlite_engine,
        now - pd.Timedelta(minutes=10),
        temp_c=25,
        humidity=65,
        pressure_hpa=1012,
        wind_kmh=8,
    )

    assert adaptive_station_backfill_hours(_settings(), now=now) == 2


def test_adaptive_backfill_expands_to_recover_the_oldest_primary_sensor(
    sqlite_engine,
):
    now = pd.Timestamp("2026-08-21T12:00:00Z")
    _insert_station_row(
        sqlite_engine,
        now - pd.Timedelta(hours=4, minutes=20),
        temp_c=24,
    )
    _insert_station_row(
        sqlite_engine,
        now - pd.Timedelta(minutes=10),
        humidity=65,
        pressure_hpa=1012,
        wind_kmh=8,
    )

    assert adaptive_station_backfill_hours(_settings(), now=now) == 6


def test_adaptive_backfill_uses_safe_cap_when_a_sensor_has_never_been_saved(
    sqlite_engine,
):
    now = pd.Timestamp("2026-08-21T12:00:00Z")
    _insert_station_row(
        sqlite_engine,
        now - pd.Timedelta(minutes=10),
        temp_c=25,
        humidity=65,
        pressure_hpa=1012,
    )

    assert adaptive_station_backfill_hours(_settings(), now=now) == 24
