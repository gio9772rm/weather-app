from dataclasses import replace

import pandas as pd
import pytest

from astronomy_planner import TARGET_BY_NAME
from city_weather import CityLocation, fetch_observing_hourly
from config import Settings
from sky_catalog import search_targets
from tests.test_v5 import forecast_at
from v5_cielo import night_report
from v5_extensions import planner
from v5_imaging_budget import budget_options, imaging_budget
from v5_observatories import remote_context
from v53_profiles import validate_observing


def test_catalog_search_finds_aliases_and_preserves_existing_ids():
    assert search_targets("NGC2070")[0]["name"] == "NGC 2070"
    assert search_targets("Tarantola")[0]["name"] == "NGC 2070"
    assert search_targets("Horsehead")[0]["name"] == "B033"
    assert search_targets("M 0042")[0]["name"] == "M42"
    assert len(search_targets("NGC")) <= 12


def test_global_weather_retains_both_utc_hours_at_dst(monkeypatch):
    instants = pd.to_datetime(["2026-10-25T00:00Z", "2026-10-25T01:00Z"])

    def response(session, url, params, service, **kwargs):
        assert params["timeformat"] == "unixtime"
        assert "cloud_cover_low" in params["hourly"]
        return {
            "hourly": {
                "time": [int(t.timestamp()) for t in instants],
                "cloud_cover_low": [5, None],
            }
        }

    monkeypatch.setattr("city_weather._get_json", response)
    frame, source, _ = fetch_observing_hourly(
        CityLocation("Test", "", "", 41.9, 12.5, "Europe/Rome")
    )
    assert source == "Open-Meteo"
    assert list(frame["valid_time"]) == list(instants)
    assert list(frame["valid_time"].dt.tz_convert("Europe/Rome").dt.hour) == [2, 2]
    assert frame["cloud_low"].iloc[0] == 5 and pd.isna(frame["cloud_low"].iloc[1])


def test_world_target_reaches_planner_and_totals_across_nights(monkeypatch):
    now = pd.Timestamp.now(tz="UTC")
    calls = []

    def forecast(location):
        calls.append(location)
        frame = forecast_at(pd.date_range(now.floor("h"), periods=120, freq="h"))
        return frame.assign(cloud_low=2, cloud_mid=4, cloud_high=5), "Open-Meteo", now

    monkeypatch.setattr("v5_observatories.fetch_observing_hourly", forecast)
    cfg = Settings.from_env()
    site = {
        "kind": "point",
        "label": "Atacama · prova",
        "latitude": -23.1,
        "longitude": -68.1,
        "timezone": "America/Santiago",
        "elevation_m": 2500,
    }
    context = remote_context(site, "deep_sky", now)
    date = context.now.tz_convert(site["timezone"]).date() + pd.Timedelta(days=1)
    report = night_report(
        context, {"date": date.isoformat(), "target": "B033", "altitude": 0, "moon": 0}
    )
    assert report["selected"] == date.isoformat()
    assert report["weather_source"] == "Open-Meteo"
    assert report["site"]["timezone"] == "America/Santiago"
    assert set(report["site"]) == {"id", "name", "timezone"}
    assert any(t["id"] == "B033" for t in report["targets"])
    window = next(t["best_window"] for t in report["targets"] if t["id"] == "NGC 2070")
    assert window

    def fail(*args):
        pytest.fail("La postazione globale non deve caricare previsioni locali")

    monkeypatch.setattr("v5_extensions.scoped_forecast", fail)
    result = planner(
        cfg.station_id,
        {
            "site": site,
            "start": window["start"],
            "end": window["end"],
            "targets": ["NGC2070"],
            "altitude": 0,
            "moon": 0,
            "setup_minutes": 0,
            "switch_minutes": 0,
            "minimum_minutes": 15,
            "compare_nights": 2,
            "budget": {
                "filters": [
                    {"name": "Ha", "seconds": 180, "frames": 2},
                    {"name": "OIII", "seconds": 180, "frames": 1},
                ]
            },
        },
    )
    assert len(calls) == 1
    assert result["timezone"] == "America/Santiago"
    assert result["target_catalog"][0]["name"] == "NGC 2070"
    assert len(result["nights"]) == 2
    assert all(n["budget"]["integration_seconds"] > 0 for n in result["nights"])
    assert result["budget"]["integration_seconds"] == sum(
        n["budget"]["integration_seconds"] for n in result["nights"]
    )
    assert (
        result["budget"]["integration_seconds"]
        < sum(n["net_hours"] for n in result["nights"]) * 3600
    )
    profile = validate_observing(
        {"sites": [site], "targets": ["ngc2070", "Nebulosa Tarantola"], "rig": "plan"}
    )
    assert profile["targets"] == ["NGC 2070"] and profile["sites"][0] == site
    with pytest.raises(ValueError):
        validate_observing({"site": {**site, "timezone": "Mars/Base"}})


def test_budget_charges_focus_dither_filter_readout_and_flip(monkeypatch):
    start = pd.Timestamp("2026-09-24T22:00Z")
    end = start + pd.Timedelta(seconds=600)
    schedule = {
        "blocks": [{"target": "M42", "start": start, "end": end}],
        "overhead_minutes": 10,
    }
    target = TARGET_BY_NAME["M42"]
    site = replace(Settings.from_env(), longitude=12.5)
    monkeypatch.setattr(
        "v5_imaging_budget._meridian", lambda *args: start.timestamp() + 245
    )
    recipe = budget_options(
        {
            "filters": [
                {"name": "L", "seconds": 60, "frames": 2},
                {"name": "R", "seconds": 60, "frames": 1},
            ],
            "focus_seconds": 30,
            "focus_every_minutes": 0,
            "readout_seconds": 5,
            "filter_change_seconds": 5,
            "dither_every_frames": 2,
            "dither_seconds": 10,
            "flip_seconds": 60,
        }
    )
    budget = imaging_budget(schedule, site, [target], recipe)
    assert {f["name"]: f["count"] for f in budget["filters"]} == {"L": 4, "R": 2}
    assert budget["overhead"] == {
        "flip": 60,
        "readout": 30,
        "filter": 20,
        "focus": 60,
        "dither": 20,
    }
    assert budget["integration_seconds"] == 360
    assert budget["idle_seconds"] == 50
    assert budget["preparation_seconds"] == 600
