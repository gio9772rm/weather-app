from __future__ import annotations

import pandas as pd

from astro_weather import (
    astronomy_score,
    best_observing_windows,
    daily_astronomy_summary,
    prepare_astronomy,
)
from config import Settings


def _settings() -> Settings:
    return Settings(
        latitude=41.9,
        longitude=12.5,
        elevation_m=20,
        local_timezone="Europe/Rome",
        location_name="Test",
        openweather_api_key="",
        ecowitt_application_key="",
        ecowitt_api_key="",
        ecowitt_mac="",
        forecast_days=7,
        forecast_refresh_minutes=60,
        score_lookback_days=60,
        station_backfill_hours=2,
        station_auto_backfill_max_hours=24,
        station_stale_minutes=45,
        admin_token="",
    )


def test_clear_sky_scores_higher_than_cloudy_windy_sky():
    frame = pd.DataFrame(
        {
            "clouds": [5, 95],
            "cloud_low": [2, 95],
            "cloud_mid": [2, 90],
            "cloud_high": [5, 80],
            "precip_probability": [0, 90],
            "wind_kmh": [5, 40],
            "wind_gust_kmh": [8, 65],
            "visibility_m": [30000, 4000],
            "temp_c": [15, 15],
            "dewpoint_c": [8, 14.5],
        }
    )
    scores = astronomy_score(frame)
    assert scores.iloc[0] > 85
    assert scores.iloc[1] < 30


def test_best_window_groups_consecutive_night_hours():
    times = pd.date_range("2026-08-19T20:00:00Z", periods=4, freq="h")
    frame = pd.DataFrame(
        {
            "valid_time": times,
            "is_day": 0,
            "clouds": 5,
            "cloud_low": 2,
            "cloud_mid": 2,
            "cloud_high": 5,
            "precip_probability": 0,
            "wind_kmh": 5,
            "wind_gust_kmh": 8,
            "visibility_m": 30000,
            "temp_c": 15,
            "dewpoint_c": 8,
        }
    )
    prepared = prepare_astronomy(frame, _settings())
    windows = best_observing_windows(prepared)
    assert len(windows) == 1
    assert windows.iloc[0]["hours"] == 4


def test_daily_astronomy_summary_combines_weather_and_moon():
    times = pd.date_range("2026-08-21T20:00:00Z", periods=3, freq="h")
    frame = pd.DataFrame(
        {
            "valid_time": times,
            "is_day": 0,
            "clouds": [10, 20, 30],
            "precip_probability": 0,
            "wind_kmh": [5, 10, 15],
            "wind_gust_kmh": 18,
            "visibility_m": 20000,
            "temp_c": 18,
            "dewpoint_c": 10,
        }
    )
    prepared = prepare_astronomy(frame, _settings())
    events = pd.DataFrame(
        {
            "date": [prepared.iloc[0]["local_time"].date()],
            "moon_illumination": [42.0],
        }
    )

    daily = daily_astronomy_summary(prepared, events)

    assert len(daily) == 1
    assert daily.iloc[0]["clouds_mean"] == 20
    assert daily.iloc[0]["wind_mean"] == 10
    assert daily.iloc[0]["visibility_km"] == 20
    assert daily.iloc[0]["moon_illumination"] == 42
