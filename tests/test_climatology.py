from __future__ import annotations

import pandas as pd
from sqlalchemy import text

from climatology import (
    anomaly_snapshot,
    archive_local_normals,
    build_local_normals,
    calculate_local_normals,
)
from config import Settings


def test_local_normals_and_anomaly_are_month_hour_specific(sqlite_engine):
    times = pd.date_range("2025-08-01T10:00:00Z", periods=36, freq="D")
    station = pd.DataFrame(
        {
            "time": times,
            "temp_c": [20.0 + (index % 5) for index in range(len(times))],
            "humidity": [55.0] * len(times),
            "pressure_hpa": [1012.0] * len(times),
            "wind_kmh": [8.0] * len(times),
        }
    )

    normals = build_local_normals(
        station,
        "UTC",
        minimum_samples=20,
        updated_at=pd.Timestamp("2026-08-27T08:00:00Z"),
    )

    august = normals[(normals["month"] == 8) & (normals["hour"] == 10)]
    assert set(august["metric"]) == {
        "temp_c",
        "humidity",
        "pressure_hpa",
        "wind_kmh",
    }
    current = pd.Series(
        {
            "time": pd.Timestamp("2026-08-27T10:00:00Z"),
            "temp_c": 30.0,
            "humidity": 55.0,
            "pressure_hpa": 1012.0,
            "wind_kmh": 8.0,
        }
    )
    snapshot = anomaly_snapshot(current, normals, timezone="UTC")
    assert (
        snapshot.loc[snapshot["metric"] == "temp_c", "state"].iloc[0]
        == "Sopra il consueto"
    )

    archive_local_normals(normals, sqlite_engine)
    with sqlite_engine.connect() as connection:
        assert connection.execute(
            text("SELECT COUNT(*) FROM climate_normals")
        ).scalar_one() == len(normals)


def test_local_normals_read_legacy_mixed_case_sqlite_columns(sqlite_engine):
    rows = []
    for day in range(1, 8):
        for minute in range(0, 60, 10):
            rows.append(
                {
                    "time": f"2026-08-{day:02d}T10:{minute:02d}:00Z",
                    "temp": 25.0 + day / 10,
                }
            )
    with sqlite_engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO station_raw (time,temp_c,humidity,pressure_hpa,wind_kmh) "
                "VALUES (:time,:temp,55,1012,8)"
            ),
            rows,
        )

    normals = calculate_local_normals(Settings.from_env(), sqlite_engine)

    assert not normals.empty
    assert "temp_c" in set(normals["metric"])
