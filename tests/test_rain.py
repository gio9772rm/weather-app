from __future__ import annotations

import pandas as pd
import pytest

from weather_ingest_ecowitt_cloud import RAW_COLUMNS, add_rain_increments, parse_payload


def _frame(rows):
    frame = pd.DataFrame(rows)
    for column in RAW_COLUMNS:
        if column not in frame:
            frame[column] = None
    frame["source"] = "test"
    frame["data_quality"] = "ok"
    return frame[RAW_COLUMNS]


def test_cumulative_rain_becomes_increment(sqlite_engine):
    frame = _frame(
        [
            {
                "time": pd.Timestamp("2026-08-19T10:00:00Z"),
                "rain_total_mm": 10.0,
                "rain_rate_mm_h": 0.0,
            },
            {
                "time": pd.Timestamp("2026-08-19T10:10:00Z"),
                "rain_total_mm": 10.6,
                "rain_rate_mm_h": 3.6,
            },
        ]
    )
    result = add_rain_increments(frame, sqlite_engine)
    assert result.iloc[0]["rain_mm"] == 0.0
    assert result.iloc[1]["rain_mm"] == pytest.approx(0.6)


def test_rate_fallback_uses_elapsed_time(sqlite_engine):
    frame = _frame(
        [
            {"time": pd.Timestamp("2026-08-19T10:00:00Z"), "rain_rate_mm_h": 6.0},
            {"time": pd.Timestamp("2026-08-19T10:10:00Z"), "rain_rate_mm_h": 6.0},
        ]
    )
    result = add_rain_increments(frame, sqlite_engine)
    assert result.iloc[1]["rain_mm"] == pytest.approx(1.0)
    assert result.iloc[1]["data_quality"] == "estimated_rain"


def test_ecowitt_history_sensor_lists_are_aligned_by_timestamp():
    payload = {
        "code": 0,
        "data": {
            "outdoor": {
                "temperature": {"unit": "C", "list": {"1787133600": "24.5"}},
                "humidity": {"unit": "%", "list": {"1787133600": "55"}},
            },
            "pressure": {"relative": {"unit": "hPa", "list": {"1787133600": "1014.2"}}},
            "wind": {
                "wind_speed": {"unit": "m/s", "list": {"1787133600": "2"}},
                "wind_gust": {"unit": "m/s", "list": {"1787133600": "4"}},
                "wind_direction": {"unit": "°", "list": {"1787133600": "180"}},
            },
            "rainfall": {
                "rain_rate": {"unit": "mm/h", "list": {"1787133600": "1.2"}},
                "yearly": {"unit": "mm", "list": {"1787133600": "120.4"}},
            },
        },
    }
    frame = parse_payload(payload)
    assert len(frame) == 1
    row = frame.iloc[0]
    assert row["temp_c"] == 24.5
    assert row["wind_kmh"] == 7.2
    assert row["rain_rate_mm_h"] == 1.2
    assert row["rain_total_mm"] == 120.4
