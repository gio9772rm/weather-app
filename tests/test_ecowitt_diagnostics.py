from __future__ import annotations

import pandas as pd
from sqlalchemy import text

from ecowitt_diagnostics import (
    archive_telemetry_safely,
    diagnose_observations,
    extract_telemetry,
    load_ecowitt_diagnostics,
    upsert_telemetry,
)


def test_extracts_only_sanitised_battery_and_signal_values():
    payload = {
        "time": 1787918400,
        "data": {
            "battery": {
                "wh65": {"value": "1.42", "unit": "V"},
                "console": {"value": "76", "unit": "%"},
            },
            "radio": {"rssi": {"value": "-68", "unit": "dBm"}},
            "mac": "00:11:22:33:44:55",
            "outdoor": {"temperature": {"value": 25, "unit": "C"}},
        },
    }

    rows = extract_telemetry(payload, station_id="primary")

    assert {(row["metric"], row["sensor"]) for row in rows} == {
        ("battery", "battery_wh65"),
        ("battery", "battery_console"),
        ("signal", "radio_rssi"),
    }
    rendered = str(rows)
    assert "00:11" not in rendered
    assert all(row["status"] == "ok" for row in rows)


def test_optional_telemetry_failure_never_raises():
    rows = [
        {
            "station_id": "primary",
            "observed_at": "2026-08-28T12:00:00Z",
            "sensor": "battery_wh65",
            "metric": "battery",
            "value": 1.4,
            "unit": "V",
            "status": "ok",
            "fetched_at": "2026-08-28T12:00:00Z",
        }
    ]

    saved, warning = archive_telemetry_safely(rows, object())

    assert saved == 0
    assert "non archiviata" in warning


def test_sensor_diagnostics_detects_fresh_coverage_and_stale_metric():
    now = pd.Timestamp("2026-08-28T12:00:00Z")
    times = pd.date_range(now - pd.Timedelta(hours=1), now, freq="5min")
    frame = pd.DataFrame(
        {
            "time": times,
            "temp_c": range(len(times)),
            "humidity": 55,
            "pressure_hpa": 1012,
            "wind_kmh": 5,
            "rain_rate_mm_h": 0,
            "solar_w_m2": 100,
            "uv_index": 2,
            "data_quality": "ok",
        }
    )
    frame.loc[frame.index[-8:], "humidity"] = None

    diagnostics = diagnose_observations(frame, now=now, hours=1, stale_minutes=20)
    humidity = diagnostics.set_index("metric").loc["humidity"]
    temperature = diagnostics.set_index("metric").loc["temp_c"]

    assert humidity["status"] == "delayed"
    assert temperature["status"] == "online"
    assert temperature["coverage"] == 100


def test_telemetry_is_archived_and_loaded_without_device_identifier(
    sqlite_engine,
):
    now = pd.Timestamp.now(tz="UTC").floor("min")
    with sqlite_engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO station_raw (time,temp_c,humidity,pressure_hpa,wind_kmh,"
                "rain_rate_mm_h,solar_w_m2,uv_index,source,data_quality) VALUES "
                "(:time,22,55,1012,4,0,10,0,'ecowitt_cloud','ok')"
            ),
            {"time": now.strftime("%Y-%m-%dT%H:%M:%SZ")},
        )
    rows = extract_telemetry(
        {
            "time": now.isoformat(),
            "data": {"battery": {"wh65": {"value": 1.4, "unit": "V"}}},
        },
        station_id="primary",
        fetched_at=now,
    )
    assert upsert_telemetry(rows, sqlite_engine) == 1

    sensors, telemetry, summary = load_ecowitt_diagnostics(
        station_id="primary", engine=sqlite_engine, now=now
    )

    assert not sensors.empty
    assert telemetry.iloc[0]["metric"] == "battery"
    assert summary.offline == 0
    assert summary.warning >= 4
