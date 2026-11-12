from datetime import datetime, timedelta, timezone

import pandas as pd
from starlette.applications import Starlette
from starlette.testclient import TestClient

import city_comparison as compare
import v5_city
from v5_api import public_routes

NOW = datetime(2026, 11, 12, 10, 5, tzinfo=timezone.utc)
START = NOW.replace(minute=0)
HOUR = timedelta(hours=1)


def series(id, name, via, temp, rain=None, start=START):
    data = {
        start + i * HOUR: {"temp_c": temp + i / 10, "humidity": 70.0} for i in range(30)
    }
    if rain is not None:
        for row in data.values():
            row["precipitation_mm"] = rain
    return compare.Series(id, name, via, data)


def test_primary_model_alignment_and_median_do_not_treat_absence_as_zero():
    times = [int((START + i * HOUR).timestamp()) for i in range(2)]
    block = {"time": times}
    for (_, _, key), v, rain in zip(
        compare.OPEN_MODELS, [11, 13, 27], [0.0, None, 2.0]
    ):
        block[f"temperature_2m_{key}"] = [v, v + 1]
        block[f"precipitation_{key}"] = [6, rain]
    models = compare.parse_open_models({"hourly": block})
    hourly, _ = compare.aggregate(models, "Asia/Kathmandu", NOW)
    point = next(r for r in hourly if r["time"] == "2026-11-12T15:45:00+05:45")
    assert point["values"]["temp_c"] == [13, 11, 27, 3]
    assert point["values"]["precipitation_mm"] == [1, 0, 2, 2]
    assert hourly[0]["values"]["precipitation_mm"] == [6, 6, 6, 3]


def test_daily_totals_only_compare_identical_hours_including_partial_day():
    first = series("ecmwf", "A", "A", 12, 2)
    other = series("icon", "B", "B", 10, 1, start=START + 2 * HOUR)
    partial = series("gfs", "C", "C", 14)
    _, days = compare.aggregate([first, partial, other], "Europe/Rome", NOW)
    day = days[0]
    assert day["rain_hours"] == 11
    assert day["temp_hours"] == 11
    assert day["values"]["precipitation_mm"] == [16.5, 11, 22, 2]
    assert day["values"]["temp_min_c"][3] == 3
    assert "precipitation_mm" not in day["models"][1]


def test_reserve_replaces_a_model_and_keeps_different_kinds_of_data_separate(
    monkeypatch,
):
    monkeypatch.setattr(
        compare, "_open", lambda *_: [series("gfs", "NOAA GFS", "Open-Meteo", 10, 2)]
    )
    monkeypatch.setattr(
        compare,
        "_norway",
        lambda *_: series("ecmwf", "MET Norway", "Locationforecast", 14, 0),
    )
    monkeypatch.setattr(
        compare, "_bright", lambda *_: series("icon", "DWD MOSMIX", "Bright Sky", 16, 4)
    )
    monkeypatch.setattr(
        compare,
        "_seven",
        lambda *_: (_ for _ in ()).throw(AssertionError("duplicate GFS")),
    )
    result = compare.forecast_comparison(v5_city.ROME, now=NOW)
    assert [s["name"] for s in result["sources"]] == [
        "MET Norway",
        "NOAA GFS",
        "DWD MOSMIX",
    ]
    assert result["shared_model"]
    assert result["hourly"][0]["values"]["temp_c"] == [14, 10, 16, 3]


def test_7timer_uses_only_instantaneous_temperature_and_humidity():
    data = {
        "init": "2026111206",
        "dataseries": [
            {
                "timepoint": 3,
                "temp2m": 12,
                "rh2m": "66%",
                "prec_amount": 3,
                "wind10m": {"speed": 7},
            },
            {"timepoint": 6, "temp2m": 15, "rh2m": "72%", "prec_amount": 1},
        ],
    }
    result = compare.parse_7timer(data, NOW)
    assert result.rows[START] == {"temp_c": 13, "humidity": 68}
    assert all("precipitation_mm" not in r for r in result.rows.values())


def test_bright_sky_ignores_observed_and_distant_points():
    data = {
        "sources": [
            {"id": 1, "distance": 5, "observation_type": "current"},
            {"id": 2, "distance": 38000, "observation_type": "forecast"},
            {"id": 3, "distance": 13000, "observation_type": "forecast"},
        ],
        "weather": [
            {
                "source_id": n,
                "timestamp": START.isoformat(),
                "temperature": n * 10,
                "precipitation": n,
            }
            for n in (1, 2, 3)
        ],
    }
    result = compare.parse_bright_sky(data)
    assert result.rows == {START: {"temp_c": 30}, START - HOUR: {"precipitation_mm": 3}}
    assert "13 km" in result.note


def test_rome_observation_uses_only_fresh_unflagged_values():
    now = pd.Timestamp(NOW)
    snapshot = {
        "observed_at": START.isoformat(),
        "current": {
            "temp_c": 20,
            "humidity": 85,
            "wind_kmh": 8,
            "data_quality": "stuck_humidity",
        },
        "insights": {
            "quality": {"measurements": [{"variable": "wind_kmh", "status": "suspect"}]}
        },
        "station": {"latitude": 42, "longitude": 12, "private": "never exported"},
    }
    observation = v5_city.rome_observation(snapshot, now=now)
    assert observation == {"time": START.isoformat(), "values": {"temp_c": 20}}
    assert (
        v5_city.rome_observation(snapshot, now=now + pd.Timedelta(minutes=16)) is None
    )


def test_comparison_endpoint_accepts_public_city_or_coarse_same_origin_location(
    monkeypatch,
):
    seen = []

    def get_comparison(location):
        seen.append(location)
        return {"hourly": [], "daily": [], "sources": []}

    monkeypatch.setattr(v5_city, "forecast_comparison", get_comparison)
    monkeypatch.setattr(v5_city, "_cached", lambda key, load, ttl: load())
    with TestClient(Starlette(routes=public_routes())) as client:
        path = "/api/v5/tools/city-comparison"
        assert client.get(path).status_code == 400
        assert client.get(path + "?geo_id=3169070").json()["geo_id"] == 3169070
        body = {
            "latitude": 41.89 + 191 / 100000,
            "longitude": 12.51 + 301 / 100000,
            "timezone": "Europe/Rome",
        }
        assert client.post(path, json=body).status_code == 403
        result = client.post(path, json=body, headers={"Origin": "http://testserver"})
        assert result.status_code == 200 and result.json()["geo_id"] == 3169070
        assert str(body["latitude"]) not in result.text
        far = {
            "latitude": -27.0028,
            "longitude": -129.0031,
            "timezone": "Pacific/Gambier",
        }
        result = client.post(path, json=far, headers={"Origin": "http://testserver"})
        assert result.status_code == 200 and result.json()["geo_id"] is None
        assert not any(k in result.json() for k in ("latitude", "longitude"))
        assert (seen[-1].latitude, seen[-1].longitude) == (-27.0, -129.0)
    assert seen[:2] == [v5_city.ROME, v5_city.ROME]
