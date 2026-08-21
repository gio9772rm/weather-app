from __future__ import annotations

import pandas as pd

from city_weather import CityLocation, parse_city_forecast, search_cities


def test_search_cities_parses_labels_and_coordinates() -> None:
    class Response:
        ok = True

        @staticmethod
        def json():
            return {
                "results": [
                    {
                        "name": "Roma",
                        "country": "Italia",
                        "admin1": "Lazio",
                        "latitude": 41.89,
                        "longitude": 12.48,
                        "timezone": "Europe/Rome",
                        "elevation": 20,
                    }
                ]
            }

    class Session:
        def get(self, *_args, **_kwargs):
            return Response()

    results = search_cities("Roma", session=Session())

    assert results == [
        CityLocation(
            name="Roma",
            country="Italia",
            admin1="Lazio",
            latitude=41.89,
            longitude=12.48,
            timezone="Europe/Rome",
            elevation_m=20.0,
        )
    ]
    assert results[0].label == "Roma, Lazio, Italia"


def test_parse_city_forecast_normalises_current_hourly_and_daily() -> None:
    payload = {
        "timezone": "Europe/Rome",
        "current": {
            "time": "2026-08-21T22:00",
            "temperature_2m": 25.0,
            "relative_humidity_2m": 70,
            "weather_code": 2,
            "wind_speed_10m": 8.0,
        },
        "hourly": {
            "time": ["2026-08-21T22:00", "2026-08-21T23:00"],
            "temperature_2m": [25.0, 24.0],
            "apparent_temperature": [26.0, 25.0],
            "relative_humidity_2m": [70, 74],
            "precipitation_probability": [10, 20],
            "precipitation": [0, 0],
            "rain": [0, 0],
            "weather_code": [2, 3],
            "cloud_cover": [45, 70],
            "pressure_msl": [1014, 1014],
            "visibility": [25000, 22000],
            "wind_speed_10m": [8, 7],
            "wind_direction_10m": [90, 100],
            "wind_gusts_10m": [15, 14],
        },
        "daily": {
            "time": ["2026-08-21"],
            "weather_code": [2],
            "temperature_2m_max": [30],
            "temperature_2m_min": [20],
            "apparent_temperature_max": [32],
            "apparent_temperature_min": [20],
            "sunrise": ["2026-08-21T06:20"],
            "sunset": ["2026-08-21T20:00"],
            "precipitation_sum": [0],
            "rain_sum": [0],
            "precipitation_probability_max": [20],
            "wind_speed_10m_max": [12],
            "wind_gusts_10m_max": [20],
            "wind_direction_10m_dominant": [100],
            "uv_index_max": [7],
        },
    }

    result = parse_city_forecast(payload, pd.Timestamp("2026-08-21T20:00:00Z"))

    assert result.current["description"] == "Parzialmente nuvoloso"
    assert result.hourly.iloc[1]["description"] == "Coperto"
    assert str(result.hourly.iloc[0]["time"].tzinfo) == "Europe/Rome"
    assert result.daily.iloc[0]["temp_max_c"] == 30
    assert result.daily.iloc[0]["sunset"].strftime("%H:%M") == "20:00"
