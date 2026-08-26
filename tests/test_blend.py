from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import text

from forecast_blend import (
    _to_hourly,
    archive_forecast,
    build_blend,
    score_forecasts,
)
from forecast_providers import FORECAST_COLUMNS
from forecast_quality import enforce_physical_bounds


def provider_frame(provider: str, temperatures: list[float]) -> pd.DataFrame:
    issued = pd.Timestamp.now(tz="UTC").floor("h")
    times = pd.date_range(
        issued + pd.Timedelta(hours=1), periods=len(temperatures), freq="h"
    )
    frame = pd.DataFrame(
        {
            "provider": provider,
            "model": "test",
            "issued_at": issued,
            "valid_time": times,
            "interval_hours": 1.0,
            "lead_hours": range(1, len(times) + 1),
            "temp_c": temperatures,
            "feels_like_c": temperatures,
            "humidity": 50,
            "dewpoint_c": 12,
            "pressure_hpa": 1015,
            "wind_kmh": 10,
            "wind_gust_kmh": 15,
            "wind_dir": 180,
            "rain_mm": 0,
            "snow_mm": 0,
            "precip_probability": 5,
            "clouds": 10,
            "cloud_low": 5,
            "cloud_mid": 5,
            "cloud_high": 0,
            "visibility_m": 30000,
            "weather_code": "0",
            "description": "Sereno",
            "is_day": 1,
            "fetched_at": issued,
        }
    )
    return frame.reindex(columns=FORECAST_COLUMNS)


def test_blend_combines_two_providers_and_tracks_uncertainty(sqlite_engine):
    archive_forecast(provider_frame("open_meteo", [20, 21, 22]), sqlite_engine)
    archive_forecast(provider_frame("openweather", [22, 23, 24]), sqlite_engine)
    result = build_blend(sqlite_engine)
    assert len(result) == 3
    assert (result["provider_count"] == 2).all()
    assert np.allclose(result["temp_uncertainty_c"], 2.0)
    assert result["temp_c"].between(20, 24).all()
    assert result["confidence"].between(20, 99).all()
    with sqlite_engine.connect() as connection:
        assert connection.execute(
            text("SELECT COUNT(*) FROM forecast_blend_history")
        ).scalar_one() == 3

    issued = pd.Timestamp.now(tz="UTC").floor("h")
    three_hour = provider_frame("openweather", [20, 21])
    three_hour["valid_time"] = [
        issued + pd.Timedelta(hours=3),
        issued + pd.Timedelta(hours=6),
    ]
    three_hour["interval_hours"] = 3.0
    three_hour["rain_mm"] = [3.0, 6.0]
    timeline = pd.date_range(issued, issued + pd.Timedelta(hours=6), freq="h")
    distributed = _to_hourly(three_hour, timeline)
    assert distributed["rain_mm"].sum() == 9.0


def test_physical_bounds_remove_negative_rain_and_invalid_percentages():
    frame = pd.DataFrame(
        {
            "rain_mm": [-0.008, 1.2],
            "snow_mm": [-1.0, 0.0],
            "precip_probability": [-5.0, 120.0],
            "humidity": [-1.0, 101.0],
        }
    )

    bounded = enforce_physical_bounds(frame)

    assert bounded["rain_mm"].tolist() == [0.0, 1.2]
    assert bounded["snow_mm"].tolist() == [0.0, 0.0]
    assert bounded["precip_probability"].tolist() == [0.0, 100.0]
    assert bounded["humidity"].tolist() == [0.0, 100.0]


def test_recent_holdout_reports_skill_and_rain_reliability(sqlite_engine):
    now = pd.Timestamp.now(tz="UTC").floor("h")
    station_rows = []
    for offset in range(32, 0, -1):
        moment = now - pd.Timedelta(hours=offset)
        station_rows.append(
            {
                "time": moment.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "temp": float(32 - offset),
                "rain": 1.0 if offset % 3 == 0 else 0.0,
            }
        )
    forecast_rows = []
    for offset in range(20, 0, -1):
        valid = now - pd.Timedelta(hours=offset)
        observed_temperature = float(32 - offset)
        forecast_rows.append(
            {
                "issued": (valid - pd.Timedelta(hours=6)).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                ),
                "valid": valid.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "temp": observed_temperature + 1.0,
                "rain": 1.0 if offset % 3 == 0 else 0.0,
                "probability": 80.0 if offset % 3 == 0 else 20.0,
            }
        )
    with sqlite_engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO station_raw (time,temp_c,rain_mm,source,data_quality) "
                "VALUES (:time,:temp,:rain,'test','ok')"
            ),
            station_rows,
        )
        connection.execute(
            text(
                "INSERT INTO forecast_runs (provider,model,issued_at,valid_time,"
                "interval_hours,temp_c,rain_mm,precip_probability,fetched_at) "
                "VALUES ('open_meteo','test',:issued,:valid,1,:temp,:rain,"
                ":probability,:issued)"
            ),
            forecast_rows,
        )

    scores = score_forecasts(engine=sqlite_engine)
    temperature = scores[scores["variable"] == "temp_c"].iloc[0]

    assert temperature["holdout_n"] == 6
    assert temperature["holdout_mae"] == 1.0
    assert temperature["persistence_mae"] == 6.0
    assert temperature["skill_vs_persistence"] == pytest.approx(5 / 6)
    with sqlite_engine.connect() as connection:
        reliability_count = connection.execute(
            text("SELECT SUM(n) FROM forecast_reliability")
        ).scalar_one()
    assert reliability_count == 6
