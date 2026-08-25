from __future__ import annotations

import pandas as pd
import pytest

from air_quality import AirQualityError, fetch_air_quality, parse_air_quality


def _payload() -> dict:
    fields = {
        "european_aqi": [18.0, 22.0],
        "pm2_5": [4.0, 6.0],
        "pm10": [10.0, 12.0],
        "nitrogen_dioxide": [9.0, 11.0],
        "ozone": [52.0, 58.0],
        "sulphur_dioxide": [2.0, 3.0],
        "uv_index": [1.0, 2.0],
        "alder_pollen": [0.0, 0.0],
        "birch_pollen": [0.0, 0.0],
        "grass_pollen": [14.0, 18.0],
        "mugwort_pollen": [3.0, 4.0],
        "olive_pollen": [0.0, 0.0],
        "ragweed_pollen": [7.0, 8.0],
    }
    return {
        "timezone": "Europe/Rome",
        "current": {
            "time": "2026-08-25T10:00",
            **{name: values[0] for name, values in fields.items()},
        },
        "hourly": {
            "time": ["2026-08-25T10:00", "2026-08-25T11:00"],
            **fields,
        },
    }


def test_parse_air_quality_preserves_local_timezone_and_values() -> None:
    result = parse_air_quality(
        _payload(), fetched_at=pd.Timestamp("2026-08-25T08:00:00Z")
    )

    assert result.source == "Open-Meteo · CAMS"
    assert str(result.current["time"].tzinfo) == "Europe/Rome"
    assert result.current["european_aqi"] == 18.0
    assert result.hourly.iloc[1]["grass_pollen"] == 18.0


def test_fetch_air_quality_uses_keyless_official_endpoint() -> None:
    class Response:
        ok = True
        status_code = 200

        @staticmethod
        def json():
            return _payload()

    class Session:
        def __init__(self):
            self.url = ""
            self.params = {}

        def get(self, url, **kwargs):
            self.url = url
            self.params = kwargs["params"]
            return Response()

    session = Session()
    result = fetch_air_quality(41.9, 12.4, "Europe/Rome", session=session)

    assert result.current["european_aqi"] == 18.0
    assert session.url == "https://air-quality-api.open-meteo.com/v1/air-quality"
    assert "european_aqi" in session.params["current"]
    assert "grass_pollen" in session.params["hourly"]


def test_fetch_air_quality_fails_without_affecting_other_sources() -> None:
    class Response:
        ok = False
        status_code = 503

    class Session:
        def get(self, *_args, **_kwargs):
            return Response()

    with pytest.raises(AirQualityError, match="risposta HTTP 503"):
        fetch_air_quality(41.9, 12.4, "Europe/Rome", session=Session())
