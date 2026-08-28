from __future__ import annotations

from dataclasses import replace

from sqlalchemy import text

from config import Settings
from data_access import load_station_profiles
from station_registry import register_station, sync_primary_station_history


def test_primary_history_is_mirrored_without_changing_legacy_rows(sqlite_engine):
    cfg = replace(
        Settings.from_env(),
        station_id="home-primary",
        location_name="Stazione primaria",
    )
    with sqlite_engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO station_raw (time,temp_c,source,data_quality) "
                "VALUES ('2026-08-27T12:00:00Z',25.0,'ecowitt','ok')"
            )
        )

    assert sync_primary_station_history(cfg, sqlite_engine, strict=True) == 1
    assert sync_primary_station_history(cfg, sqlite_engine, strict=True) == 0

    with sqlite_engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO station_raw (time,temp_c,source,data_quality) "
                "VALUES ('2026-08-27T12:05:00Z',25.2,'ecowitt','ok')"
            )
        )

    # Normal five-minute cycles inspect only a bounded rolling window.
    assert sync_primary_station_history(cfg, sqlite_engine, strict=True) == 1
    assert sync_primary_station_history(cfg, sqlite_engine, strict=True) == 0

    # A delayed/backfilled row that is older than the latest mirrored timestamp
    # is still recovered as long as it falls inside the seven-day lookback.
    with sqlite_engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO station_raw (time,temp_c,source,data_quality) "
                "VALUES ('2026-08-27T11:55:00Z',24.8,'ecowitt','ok')"
            )
        )
    assert sync_primary_station_history(cfg, sqlite_engine, strict=True) == 1
    assert sync_primary_station_history(cfg, sqlite_engine, strict=True) == 0

    with sqlite_engine.connect() as connection:
        assert connection.execute(
            text("SELECT COUNT(*) FROM station_raw")
        ).scalar_one() == 3
        mirrored = connection.execute(
            text(
                "SELECT station_id,temp_c FROM station_observations "
                "WHERE station_id='home-primary' ORDER BY time"
            )
        ).mappings().all()
    assert [row["temp_c"] for row in mirrored] == [24.8, 25.0, 25.2]


def test_primary_history_lookback_avoids_rescanning_very_old_backfills(sqlite_engine):
    cfg = replace(
        Settings.from_env(),
        station_id="home-primary",
        location_name="Stazione primaria",
    )
    with sqlite_engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO station_raw (time,temp_c,source,data_quality) VALUES "
                "('2026-08-27T12:00:00Z',25.0,'ecowitt','ok')"
            )
        )
    assert sync_primary_station_history(cfg, sqlite_engine, strict=True) == 1

    with sqlite_engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO station_raw (time,temp_c,source,data_quality) VALUES "
                "('2026-08-19T11:55:00Z',22.0,'ecowitt','ok')"
            )
        )

    assert sync_primary_station_history(cfg, sqlite_engine, strict=True) == 0
    with sqlite_engine.connect() as connection:
        mirrored_times = connection.execute(
            text(
                "SELECT time FROM station_observations "
                "WHERE station_id='home-primary' ORDER BY time"
            )
        ).scalars().all()
    assert mirrored_times == ["2026-08-27T12:00:00Z"]


def test_second_station_can_share_timestamps_and_profiles_hide_coordinates(
    sqlite_engine,
):
    for identifier, name, role in (
        ("home-primary", "Stazione primaria", "primary"),
        ("north-future", "Stazione Nord", "secondary"),
    ):
        register_station(
            station_id=identifier,
            display_name=name,
            latitude=45.0,
            longitude=9.0,
            elevation_m=100,
            timezone="Europe/Rome",
            source="ecowitt",
            role=role,
            engine=sqlite_engine,
        )
    with sqlite_engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO station_observations (station_id,time,temp_c,source) "
                "VALUES (:station_id,'2026-08-27T12:00:00Z',:temp,'ecowitt')"
            ),
            [
                {"station_id": "home-primary", "temp": 25.0},
                {"station_id": "north-future", "temp": 19.0},
            ],
        )

    profiles = load_station_profiles()

    assert set(profiles["station_id"]) == {"home-primary", "north-future"}
    assert "latitude" not in profiles
    assert "longitude" not in profiles
