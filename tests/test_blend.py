from __future__ import annotations

import numpy as np
import pandas as pd

from forecast_blend import _to_hourly, archive_forecast, build_blend
from forecast_providers import FORECAST_COLUMNS


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
