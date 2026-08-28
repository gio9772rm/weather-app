from __future__ import annotations

from dataclasses import replace

import pandas as pd
from sqlalchemy import text

from config import Settings
from reference_climatology import (
    archive_reference_climatology,
    parse_reference_payload,
    reference_climatology_is_due,
)


def _payload() -> dict:
    return {
        "daily": {
            "time": [
                "1991-01-01",
                "1991-01-02",
                "1992-01-01",
                "1992-01-02",
                "1991-02-01",
                "1992-02-01",
            ],
            "temperature_2m_mean": [10, 12, 14, 16, 8, 10],
            "temperature_2m_min": [5, 7, 9, 11, 3, 5],
            "temperature_2m_max": [15, 17, 19, 21, 13, 15],
            "precipitation_sum": [1, 2, 3, 4, 5, 7],
        }
    }


def test_reference_payload_uses_monthly_means_across_years():
    frame = parse_reference_payload(
        _payload(), updated_at=pd.Timestamp("2026-08-27T12:00:00Z")
    )
    january = frame[frame["month"].eq(1)].set_index("metric")

    assert january.loc["temp_c_mean", "value"] == 13.0
    assert january.loc["temp_c_min", "value"] == 8.0
    assert january.loc["temp_c_max", "value"] == 18.0
    assert january.loc["rain_mm", "value"] == 5.0
    assert set(january["sample_years"]) == {2}
    assert set(frame["period_start"]) == {1991}
    assert set(frame["period_end"]) == {2020}


def test_reference_archive_is_station_scoped_and_idempotent(sqlite_engine):
    cfg = replace(Settings.from_env(), station_id="north-future")
    frame = parse_reference_payload(_payload())

    assert archive_reference_climatology(frame, cfg, sqlite_engine) == len(frame)
    updated = frame.copy()
    updated.loc[updated["metric"].eq("rain_mm"), "value"] += 1
    assert archive_reference_climatology(updated, cfg, sqlite_engine) == len(updated)

    with sqlite_engine.connect() as connection:
        count = connection.execute(
            text(
                "SELECT COUNT(*) FROM climate_reference_normals "
                "WHERE station_id='north-future'"
            )
        ).scalar_one()
    assert count == len(frame)


def test_reference_refresh_schedule_uses_the_supplied_database(sqlite_engine):
    cfg = replace(
        Settings.from_env(),
        station_id="isolated-reference",
        reference_climatology_enabled=True,
        reference_climatology_refresh_days=30,
    )
    assert reference_climatology_is_due(cfg, sqlite_engine)

    with sqlite_engine.begin() as connection:
        connection.execute(
            text("INSERT INTO meta (k,v) VALUES (:key,:value)"),
            {
                "key": "last_reference_climatology_isolated-reference",
                "value": pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%dT%H:%M:%SZ"),
            },
        )

    assert not reference_climatology_is_due(cfg, sqlite_engine)
