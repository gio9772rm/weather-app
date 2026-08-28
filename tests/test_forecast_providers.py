from __future__ import annotations

import pandas as pd

from forecast_providers import fetch_icon_2i, parse_open_meteo, parse_openweather

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


class _ForecastResponse:
    ok = True
    status_code = 200

    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


class _ForecastSession:
    def __init__(self, payload):
        self.payload = payload
        self.params = None

    def get(self, url, *, params, timeout):
        del url, timeout
        self.params = params
        return _ForecastResponse(self.payload)


def test_fetch_icon_2i_uses_explicit_model_and_keeps_astronomy_fields():
    payload = {
        "hourly": {
            "time": ["2026-08-27T13:00"],
            "temperature_2m": [25.0],
            "relative_humidity_2m": [50],
            "cape": [120],
            "freezing_level_height": [3_500],
            "wind_speed_300hPa": [90],
            "relative_humidity_700hPa": [35],
            "geopotential_height_500hPa": [5_800],
            "temperature_850hPa": [14],
            "weather_code": [1],
        }
    }
    session = _ForecastSession(payload)

    frame = fetch_icon_2i(session=session)

    assert session.params["models"] == "italia_meteo_arpae_icon_2i"
    assert session.params["forecast_hours"] == 72
    assert set(frame["provider"]) == {"italiameteo_icon2i"}
    assert set(frame["model"]) == {"icon_2i_2p2km"}
    assert frame.iloc[0]["cape_j_kg"] == 120
    assert frame.iloc[0]["wind_300hpa_kmh"] == 90
    assert frame.iloc[0]["temperature_850hpa_c"] == 14
