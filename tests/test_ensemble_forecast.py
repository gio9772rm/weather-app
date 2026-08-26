from __future__ import annotations

import pandas as pd
import pytest
from sqlalchemy import text

from ensemble_forecast import archive_ensemble, parse_open_meteo_ensemble


def test_ensemble_parser_keeps_true_member_distribution():
    payload = {
        "hourly": {
            "time": ["2026-08-26T10:00", "2026-08-26T11:00"],
            "temperature_2m": [20, 21],
            "temperature_2m_member01": [18, 20],
            "temperature_2m_member02": [22, 24],
            "precipitation": [0, 0.4],
            "precipitation_member01": [0, 0],
            "precipitation_member02": [0.2, 0.8],
        }
    }
    frame = parse_open_meteo_ensemble(
        payload, fetched_at=pd.Timestamp("2026-08-26T09:12:00Z")
    )

    temperature = frame[
        (frame["variable"] == "temp_c")
        & (frame["valid_time"] == pd.Timestamp("2026-08-26T10:00:00Z"))
    ].iloc[0]
    rain = frame[
        (frame["variable"] == "rain_mm")
        & (frame["valid_time"] == pd.Timestamp("2026-08-26T11:00:00Z"))
    ].iloc[0]
    assert temperature["member_count"] == 3
    assert temperature["p10"] < temperature["p50"] < temperature["p90"]
    assert rain["event_probability"] == pytest.approx(200 / 3)


def test_ensemble_archive_is_idempotent(sqlite_engine):
    payload = {
        "hourly": {
            "time": ["2026-08-26T10:00"],
            "temperature_2m": [20],
            "temperature_2m_member01": [21],
        }
    }
    frame = parse_open_meteo_ensemble(
        payload, fetched_at=pd.Timestamp("2026-08-26T09:12:00Z")
    )
    assert archive_ensemble(frame, sqlite_engine) == 1
    assert archive_ensemble(frame, sqlite_engine) == 1
    with sqlite_engine.connect() as connection:
        assert (
            connection.execute(
                text("SELECT COUNT(*) FROM forecast_ensemble_runs")
            ).scalar_one()
            == 1
        )
