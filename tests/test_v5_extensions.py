from dataclasses import replace
from datetime import timedelta

import pandas as pd
import pytest
from sqlalchemy import text
from starlette.applications import Starlette
from starlette.testclient import TestClient

from config import Settings
from ingest_all import cycle_wait_seconds, pipeline_cycle_is_due
from v5_api import public_routes
from v5_extensions import planner, read_product, refresh_product
from v5_verification import verification_scores


def test_jitter_waits_in_current_slot_and_force_cannot_bypass(sqlite_engine):
    with sqlite_engine.begin() as con:
        con.execute(
            text(
                "INSERT INTO meta VALUES('last_pipeline_cycle_started','2026-09-24T18:01:12Z')"
            )
        )
    now = pd.Timestamp("2026-09-24T18:10:30Z")
    cfg = replace(Settings.from_env(), station_refresh_minutes=10)
    assert cycle_wait_seconds(cfg, now) == 42
    assert not pipeline_cycle_is_due(cfg, force=True, now=now)
    assert pipeline_cycle_is_due(cfg, now=now + pd.Timedelta(seconds=42))
    assert not pipeline_cycle_is_due(cfg, now=now + pd.Timedelta(seconds=41))


def test_product_failure_retains_date_and_obeys_cooldown(sqlite_engine):
    with sqlite_engine.begin() as con:
        con.execute(
            text(
                "INSERT INTO v5_products VALUES('radar_frames','2020-01-01T00:00Z',:p)"
            ),
            {"p": '{"fetched_at":"2020-01-01","frames":[]}'},
        )

    def failure():
        raise RuntimeError("unavailable")

    with pytest.raises(RuntimeError):
        refresh_product("radar_frames", failure, 600)
    assert read_product("radar_frames")["fetched_at"] == "2020-01-01"
    refresh_product("radar_frames", lambda: pytest.fail("Retry before cooldown"), 600)


def test_secondary_persistence_is_past_only_and_correction_has_no_holdout_leak():
    times = pd.date_range("2026-09-01T03:00Z", periods=50, freq="h")
    obs = pd.DataFrame(
        {
            "time": pd.date_range(
                times[0] - pd.Timedelta(hours=3), periods=53, freq="h"
            ),
            "temp_c": 20.0,
        }
    )
    forecast = pd.DataFrame(
        {
            "valid_time": times,
            "issued_at": times - pd.Timedelta(hours=3),
            "temp_c": [22.0] * 40 + [28.0] * 10,
        }
    )
    score = verification_scores(forecast, obs, times[-1] + pd.Timedelta(hours=1))[0]
    assert score["persistence_mae"] == 0
    assert score["training_bias"] == 2
    assert score["holdout_mae"] == 8
    assert score["candidate_holdout_mae"] == 6
    assert score["calibration_applied"] is False
    assert score["calibration_status"] == "collecting"
    # Even a nearby observation *after* issuance may not be the baseline.
    sparse = obs.copy()
    sparse["time"] += pd.Timedelta(minutes=1)
    other = verification_scores(forecast, sparse, times[-1] + pd.Timedelta(hours=1))[0]
    assert other["persistence_mae"] is None


def test_planner_limits_and_real_geometry(sqlite_engine, monkeypatch):
    from astronomy_planner import TARGETS
    from tests.test_v5 import forecast_at

    cfg = Settings.from_env()
    now = pd.Timestamp.now(tz="UTC")
    tomorrow = now + timedelta(days=1)
    forecast = forecast_at(pd.date_range(now.floor("h"), periods=72, freq="h"))
    monkeypatch.setattr("v5_extensions.scoped_forecast", lambda _: forecast)
    values = {
        "targets": [TARGETS[0].name],
        "start": tomorrow.isoformat(),
        "end": (tomorrow + timedelta(hours=5)).isoformat(),
        "equipment": {
            "name": "Test",
            "telescope": "80/480",
            "camera": "APS-C",
            "aperture_mm": 80,
            "focal_length_mm": 480,
            "sensor_width_mm": 23.5,
            "sensor_height_mm": 15.7,
            "pixel_size_um": 3.76,
        },
    }
    result = planner(cfg.station_id, values)
    assert len(result["summary"]) == 1
    assert 2 < result["field"]["width_deg"] < 3
    assert len(result["geometry"][TARGETS[0].name]["sensor_x"]) == 5
    assert "latitude" not in str(result) and "longitude" not in str(result)
    for mutation in (
        {"targets": ["not-a-target"]},
        {"end": (tomorrow + timedelta(hours=17)).isoformat()},
        {"horizon": {"90": float("nan")}},
        {"altitude": 99},
    ):
        with pytest.raises(ValueError):
            planner(cfg.station_id, {**values, **mutation})


def test_public_tools_require_bounded_origin_and_widget_is_allowlisted(
    sqlite_engine, monkeypatch
):
    cfg = Settings.from_env()
    with TestClient(Starlette(routes=public_routes())) as client:
        assert client.get("/api/v5/tools/catalog").json()["targets"]
        assert client.post("/api/v5/tools/planner", json={}).status_code == 403
        assert (
            client.post(
                "/api/v5/tools/planner",
                json={},
                headers={"Origin": "http://testserver"},
            ).status_code
            == 400
        )
        assert (
            client.post(
                "/api/v5/tools/planner",
                content=b"x" * 17000,
                headers={
                    "Origin": "http://testserver",
                    "Content-Type": "application/json",
                },
            ).status_code
            == 413
        )
        assert client.get("/api/v5/tools/cities?q=a").status_code == 400
        assert client.get("/api/v5/tools/city?id=unknown").status_code == 400
        assert client.get("/api/v5/widget/unknown").status_code == 404
        result = client.get("/api/v5/widget/" + cfg.station_id).json()
        assert set(result) == {
            "station_id",
            "name",
            "generated_at",
            "observed_at",
            "temp_c",
            "humidity",
            "wind_kmh",
            "pressure_hpa",
        }
        assert (
            client.get("/.well-known/assetlinks.json").json()[0]["target"][
                "package_name"
            ]
            == "com.gio9772rm.meteov4"
        )
