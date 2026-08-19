from __future__ import annotations

import pandas as pd

from forecast_providers import parse_open_meteo, parse_openweather

NOW = pd.Timestamp("2026-08-19T10:25:00Z")


def test_parse_open_meteo_hourly_payload():
    payload = {
        "hourly": {
            "time": ["2026-08-19T11:00", "2026-08-19T12:00"],
            "temperature_2m": [25.0, 26.0],
            "apparent_temperature": [25.5, 27.0],
            "relative_humidity_2m": [55, 50],
            "dew_point_2m": [15.3, 15.0],
            "pressure_msl": [1015, 1014],
            "wind_speed_10m": [10, 12],
            "wind_gusts_10m": [20, 22],
            "wind_direction_10m": [180, 190],
            "rain": [0, 1.2],
            "snowfall": [0, 0],
            "precipitation_probability": [5, 70],
            "cloud_cover": [10, 70],
            "cloud_cover_low": [5, 60],
            "cloud_cover_mid": [5, 20],
            "cloud_cover_high": [0, 10],
            "visibility": [30000, 18000],
            "weather_code": [0, 61],
            "is_day": [1, 1],
        }
    }
    frame = parse_open_meteo(payload, NOW)
    assert len(frame) == 2
    assert set(frame["provider"]) == {"open_meteo"}
    assert frame.iloc[1]["description"] == "Pioggia debole"
    assert frame.iloc[1]["rain_mm"] == 1.2
    assert frame.iloc[0]["lead_hours"] == 1.0


def test_parse_openweather_converts_wind_and_probability():
    payload = {
        "list": [
            {
                "dt": 1787137200,
                "main": {
                    "temp": 24,
                    "feels_like": 24.5,
                    "humidity": 60,
                    "pressure": 1012,
                },
                "wind": {"speed": 5, "gust": 8, "deg": 120},
                "rain": {"3h": 3.0},
                "snow": {},
                "clouds": {"all": 80},
                "pop": 0.75,
                "visibility": 10000,
                "weather": [{"id": 500, "description": "pioggia leggera"}],
            }
        ]
    }
    frame = parse_openweather(payload, NOW)
    assert len(frame) == 1
    row = frame.iloc[0]
    assert row["wind_kmh"] == 18.0
    assert row["wind_gust_kmh"] == 28.8
    assert row["precip_probability"] == 75.0
    assert row["interval_hours"] == 3.0
