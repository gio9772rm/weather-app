from __future__ import annotations

import pandas as pd

from ux_features import best_ventilation_window, daily_city_comparison


def test_ventilation_prefers_dry_clean_moderately_windy_hour():
    times = pd.date_range("2026-08-26T10:00:00Z", periods=4, freq="h")
    forecast = pd.DataFrame(
        {
            "valid_time": times,
            "rain_mm": [1, 0, 0, 0],
            "precip_probability": [90, 10, 10, 10],
            "wind_kmh": [8, 7, 8, 45],
            "wind_gust_kmh": [15, 12, 14, 55],
            "humidity": [80, 55, 55, 50],
        }
    )
    air = pd.DataFrame(
        {
            "time": times,
            "european_aqi": [25, 60, 12, 10],
            "pm2_5": [8, 30, 5, 4],
            "grass_pollen": [12, 40, 4, 2],
        }
    )
    result = best_ventilation_window(
        forecast,
        air,
        timezone="Europe/Rome",
        now=pd.Timestamp("2026-08-26T10:00:00Z"),
    )
    assert result.available
    assert result.timing.startswith("Mer 14:00")
    assert result.score and result.score >= 75


def test_daily_city_comparison_aligns_dates():
    local = pd.DataFrame(
        {
            "date": [pd.Timestamp("2026-08-26").date()],
            "temp_min": [20],
            "temp_max": [30],
            "rain_mm": [0],
            "wind_max": [18],
        }
    )
    city = pd.DataFrame(
        {
            "time": [pd.Timestamp("2026-08-26")],
            "temp_min_c": [16],
            "temp_max_c": [24],
            "precipitation_mm": [2],
            "wind_max_kmh": [25],
        }
    )
    result = daily_city_comparison(local, city, city_label="Milano")
    assert result.iloc[0]["Roma min/max °C"] == "20 / 30"
    assert result.iloc[0]["Milano pioggia mm"] == 2
