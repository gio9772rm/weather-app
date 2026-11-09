from __future__ import annotations

import json
from dataclasses import replace

import pandas as pd
from starlette.applications import Starlette
from starlette.testclient import TestClient

from config import Settings
from tests.test_v5 import forecast_at
from v5_api import public_routes
from v5_astronomy import observing_forecast
from v5_cielo import ObservingContext, ObservingSite, night_report
from v5_data import records
from v5_sessions import session_limits, target_sessions

EQUIPMENT = {
    "name": "Strumento di prova",
    "telescope": "80/480",
    "camera": "APS-C",
    "aperture_mm": 80,
    "focal_length_mm": 480,
    "sensor_width_mm": 23.5,
    "sensor_height_mm": 15.7,
    "pixel_size_um": 3.76,
}


def test_cielo_preserves_utc_gaps_and_ranks_by_usable_geometry_during_dst():
    cfg = replace(
        Settings.from_env(), latitude=41.9, longitude=12.5, local_timezone="Europe/Rome"
    )
    # The previous evening is still in progress when 02:xx repeats.
    now = pd.Timestamp("2026-10-25T00:25Z")
    gap = pd.Timestamp("2026-10-25T02:00Z")
    times = pd.date_range(now.floor("h"), periods=73, freq="h").difference([gap])
    forecast = forecast_at(times).assign(issued_at=now, cloud_mid=4)
    snapshot = {
        "generated_at": now.isoformat(),
        "forecast": records(forecast),
        "astronomy": {
            "deep_sky": {"hours": records(observing_forecast(forecast, cfg, now=now))}
        },
    }
    context = ObservingContext(ObservingSite.from_station(cfg), snapshot, now)
    options = {"altitude": 0, "moon": 0, "equipment": EQUIPMENT}
    report = night_report(context, options)
    assert [n["date"] for n in report["nights"]] == [
        "2026-10-24",
        "2026-10-25",
        "2026-10-26",
    ]
    assert report["nights"][0]["ongoing"]
    assert report["nights"][0]["unknown_hours"] == 1
    assert report["selected"] == "2026-10-24"
    timeline = report["timeline"]
    utc = pd.to_datetime(timeline["times"], utc=True)
    assert utc.is_monotonic_increasing and utc.is_unique
    assert not utc.tz_convert("Europe/Rome").strftime("%Y-%m-%dT%H:%M").is_unique
    assert timeline["weather"][0]["cloud_low"] is None
    assert timeline["weather"][0]["cloud_mid"] == 4
    assert all(pd.Timestamp(w["start"]) >= now for w in timeline["weather"])
    assert len(timeline["weather_windows"]) == 2
    targets = {t["id"]: t for t in report["targets"]}
    assert targets["M81"]["framing"]["fits"] and targets["M81"]["best_window"]
    assert not targets["M31"]["framing"]["fits"]
    assert targets["M42"]["filters"] == ["broadband", "narrowband"]
    assert len(targets["M81"]["altitude"]) == len(utc)
    assert len(targets["M81"]["windows"]) == 2
    for window in targets["M81"]["windows"]:
        start, end = pd.Timestamp(window["start"]), pd.Timestamp(window["end"])
        assert now <= start < end
        assert end <= gap or start >= gap + pd.Timedelta(hours=1)
    assert report["targets"][0]["framing"]["fits"]
    assert set(report["site"]) == {"id", "name", "timezone"}
    json.dumps(report, allow_nan=False)

    obstructed = night_report(context, {**options, "horizon": {"0": 90}})
    assert obstructed["constraints"]["horizon_configured"]
    assert (
        obstructed["nights"] == report["nights"]
    )  # the same weather, no usable targets
    assert all(t["best_window"] is None for t in obstructed["targets"])


def test_cielo_local_dates_and_geometry_when_forecast_is_unavailable():
    site = ObservingSite(
        "test", "Postazione di prova", "Pacific/Auckland", -36.85, 174.76, 20
    )
    now = pd.Timestamp("2026-11-08T22:00Z")
    report = night_report(ObservingContext(site, {}, now), {"date": "2026-11-10"})
    assert report["nights"][0]["date"] == "2026-11-09"
    assert report["selected"] == "2026-11-10"
    assert report["timeline"]["weather"] == []
    assert report["timeline"]["moon_altitude"]
    assert report["targets"] and all(
        t["best_window"] is None for t in report["targets"]
    )
    assert all(
        n["unknown_hours"] > 0 and n["assessed_hours"] == 0 for n in report["nights"]
    )


def test_moon_constraint_only_applies_when_above_horizon():
    times = pd.date_range("2026-11-09T21:00Z", periods=2, freq="15min")
    tracks = pd.DataFrame(
        {
            "target": "M81",
            "valid_time": times,
            "visible": True,
            "moon_separation": 10,
            "moon_visible": False,
        }
    )
    weather = forecast_at(times[:1]).assign(
        start=times[0], end=times[1], astro_score=95
    )
    limits = session_limits("deep_sky")
    sessions, _ = target_sessions(tracks, weather, limits, 30)
    assert sessions[0]["usable_hours"] == 0.25
    tracks.loc[1, "moon_visible"] = True
    sessions, detail = target_sessions(tracks, weather, limits, 30)
    assert sessions[0]["usable_hours"] == 0
    assert "Distanza dalla Luna" in detail[0]["reasons"]


def test_cielo_api_is_read_only_allowlisted_and_bounded(sqlite_engine, monkeypatch):
    cfg = Settings.from_env()
    monkeypatch.setattr(
        "v5_api.read_snapshot", lambda _: {"generated_at": "2026-11-09T12:00Z"}
    )
    path = "/api/v5/tools/cielo"
    body = {"station_id": cfg.station_id}
    origin = {"Origin": "http://testserver"}
    with TestClient(Starlette(routes=public_routes())) as client:
        assert client.post(path, json=body).status_code == 403
        assert (
            client.post(
                path, json={**body, "station_id": "unknown-site"}, headers=origin
            ).status_code
            == 400
        )
        assert (
            client.post(path, json={**body, "altitude": 95}, headers=origin).status_code
            == 400
        )
        response = client.post(path, json=body, headers=origin)
    assert response.status_code == 200
    assert response.headers["Cache-Control"] == "no-store"
    report = response.json()
    assert report["site"]["id"] == cfg.station_id
    assert report["snapshot_at"] == "2026-11-09T12:00Z"
    assert "latitude" not in response.text and "longitude" not in response.text
