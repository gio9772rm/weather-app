from __future__ import annotations

import pandas as pd

from city_weather import (
    CityLocation,
    fetch_city_forecast,
    parse_city_forecast,
    parse_metno_city_forecast,
    search_cities,
)


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
    assert 19 < result.current["dewpoint_c"] < 20
    assert result.hourly.iloc[1]["description"] == "Coperto"
    assert result.hourly["dewpoint_c"].notna().all()
    assert str(result.hourly.iloc[0]["time"].tzinfo) == "Europe/Rome"
    assert result.daily.iloc[0]["temp_max_c"] == 30
    assert result.daily.iloc[0]["sunset"].strftime("%H:%M") == "20:00"
    assert result.source == "Open-Meteo"


def _metno_payload() -> dict:
    return {
        "properties": {
            "meta": {"updated_at": "2026-08-21T20:00:00Z"},
            "timeseries": [
                {
                    "time": "2026-08-21T20:00:00Z",
                    "data": {
                        "instant": {
                            "details": {
                                "air_temperature": 24.0,
                                "relative_humidity": 71,
                                "air_pressure_at_sea_level": 1014,
                                "cloud_area_fraction": 42,
                                "wind_speed": 3.0,
                                "wind_from_direction": 90,
                                "wind_speed_of_gust": 5.0,
                            }
                        },
                        "next_1_hours": {
                            "summary": {"symbol_code": "partlycloudy_night"},
                            "details": {
                                "precipitation_amount": 0.2,
                            },
                        },
                        "next_6_hours": {
                            "summary": {"symbol_code": "rain_night"},
                            "details": {"probability_of_precipitation": 30},
                        },
                    },
                },
                {
                    "time": "2026-08-21T21:00:00Z",
                    "data": {
                        "instant": {
                            "details": {
                                "air_temperature": 23.0,
                                "relative_humidity": 75,
                                "air_pressure_at_sea_level": 1015,
                                "cloud_area_fraction": 55,
                                "wind_speed": 2.0,
                                "wind_from_direction": 100,
                                "wind_speed_of_gust": 4.0,
                            }
                        },
                        "next_1_hours": {
                            "summary": {"symbol_code": "rain_night"},
                            "details": {
                                "precipitation_amount": 1.1,
                                "probability_of_precipitation": 75,
                            },
                        },
                    },
                },
            ],
        }
    }


def test_parse_metno_city_forecast_normalises_units_and_daily_data() -> None:
    location = CityLocation(
        name="Verona",
        country="Italia",
        admin1="Veneto",
        latitude=45.4384,
        longitude=10.9916,
        timezone="Europe/Rome",
    )

    result = parse_metno_city_forecast(_metno_payload(), location)

    assert result.source == "MET Norway"
    assert result.current["description"] == "Parzialmente nuvoloso"
    assert result.current["wind_kmh"] == 10.8
    assert result.hourly.iloc[0]["precip_probability"] == 30
    assert result.hourly.iloc[1]["precipitation_mm"] == 1.1
    assert result.daily.iloc[0]["precipitation_mm"] == 1.3


def test_fetch_city_forecast_uses_metno_when_open_meteo_is_unavailable() -> None:
    class Response:
        def __init__(self, payload=None, status=200):
            self._payload = payload or {}
            self.status_code = status
            self.ok = status < 400

        def json(self):
            return self._payload

    class Session:
        def __init__(self):
            self.urls = []

        def get(self, url, **_kwargs):
            self.urls.append(url)
            if "open-meteo" in url:
                return Response(status=503)
            return Response(_metno_payload())

    location = CityLocation(
        name="Verona",
        country="Italia",
        admin1="Veneto",
        latitude=45.4384,
        longitude=10.9916,
        timezone="Europe/Rome",
    )
    session = Session()

    result = fetch_city_forecast(location, session=session)

    assert result.source == "MET Norway"
    assert session.urls == [
        "https://api.open-meteo.com/v1/forecast",
        "https://api.met.no/weatherapi/locationforecast/2.0/compact",
    ]
