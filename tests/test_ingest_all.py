from __future__ import annotations

from dataclasses import replace

import pandas as pd
from sqlalchemy import text

from config import Settings
from ingest_all import (
    FileLock,
    PipelineLock,
    adaptive_station_backfill_hours,
    station_ingest_is_due,
    station_source_age_minutes,
)


def _settings() -> Settings:
    return replace(
        Settings.from_env(),
        station_backfill_hours=2,
        station_auto_backfill_max_hours=168,
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

    assert adaptive_station_backfill_hours(_settings(), now=now) == 168


def test_daily_reconciliation_keeps_at_least_the_requested_48_hours(
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

    assert adaptive_station_backfill_hours(_settings(), 48, now=now) == 48


def test_default_automatic_recovery_window_is_seven_days(monkeypatch):
    monkeypatch.delenv("STATION_AUTO_BACKFILL_MAX_HOURS", raising=False)

    assert Settings.from_env().station_auto_backfill_max_hours == 168


def test_station_source_age_detects_stale_and_future_samples():
    now = pd.Timestamp("2026-08-21T12:00:00Z")

    assert station_source_age_minutes(
        "2026-08-21T11:42:30Z", now=now
    ) == 17.5
    assert station_source_age_minutes(
        "2026-08-21T12:01:00Z", now=now
    ) == 0.0
    assert station_source_age_minutes("invalid", now=now) == float("inf")


def test_station_ingest_guard_rejects_five_minute_scheduler_path(monkeypatch):
    now = pd.Timestamp("2026-08-21T12:10:00Z")
    monkeypatch.setattr(
        "ingest_all.get_meta",
        lambda key: "2026-08-21T12:05:00Z" if key == "last_station_success" else None,
    )

    assert station_ingest_is_due(_settings(), now=now) is False
    assert station_ingest_is_due(_settings(), force=True, now=now) is True


def test_station_ingest_guard_accepts_the_ten_minute_cycle(monkeypatch):
    now = pd.Timestamp("2026-08-21T12:10:00Z")
    monkeypatch.setattr(
        "ingest_all.get_meta",
        lambda key: "2026-08-21T12:00:00Z" if key == "last_station_success" else None,
    )

    assert station_ingest_is_due(_settings(), now=now) is True


def test_sqlite_pipeline_lock_prevents_overlapping_local_runs(
    sqlite_engine,
    tmp_path,
):
    path = tmp_path / "pipeline.lock"
    first = PipelineLock(FileLock(path=path))
    second = PipelineLock(FileLock(path=path))

    assert first.acquire() is True
    assert second.acquire() is False
    first.release()
    assert second.acquire() is True
    second.release()
