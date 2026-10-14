from copy import deepcopy

import pandas as pd
import pytest
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.responses import PlainTextResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from pro_updates import calibration_active, upcoming_windows
from web_response import WeatherResponseMiddleware


def published():
    return {
        "window_probabilities": {
            "station_id": "roma-primary",
            "model": "icon",
            "acquired_at": "2026-10-14T20:00:00Z",
            "rows": [
                {
                    "kind": "dry",
                    "start": "2026-10-15T00:00:00Z",
                    "end": "2026-10-15T02:00:00Z",
                    "probability": 90,
                    "raw_probability": 70,
                    "calibration_id": "test-model",
                }
            ],
        },
        "window_calibration": {
            "status": "active",
            "method": "dry-2h-logistic-v1",
            "station_id": "roma-primary",
            "model": "icon",
            "model_id": "test-model",
            "evaluated_at": "2026-10-14T20:00:00Z",
            "expires_at": "2026-10-15T02:00:00Z",
        },
    }


def test_expired_calibration_reverts_to_original_without_mutating_publication():
    value = published()
    original = deepcopy(value)
    now = pd.Timestamp("2026-10-14T22:00:00Z")
    assert calibration_active(value, now)
    assert upcoming_windows(value, now)[0]["probability"] == 90
    value["window_calibration"]["expires_at"] = "2026-10-14T21:00:00Z"
    assert not calibration_active(value, now)
    assert upcoming_windows(value, now)[0]["probability"] == 70
    assert value["window_probabilities"] == original["window_probabilities"]


@pytest.mark.parametrize(
    "field,value",
    [
        ("station_id", "other-station"),
        ("model", "other-model"),
        ("method", "other-method"),
        ("evaluated_at", "2026-10-15T00:00:00Z"),
    ],
)
def test_calibration_is_scoped_to_the_current_station_model_and_time(field, value):
    snapshot = published()
    snapshot["window_calibration"][field] = value
    now = pd.Timestamp("2026-10-14T22:00:00Z")
    assert not calibration_active(snapshot, now)
    assert upcoming_windows(snapshot, now)[0]["probability"] == 70


def test_old_ensemble_probabilities_are_hidden():
    assert upcoming_windows(published(), pd.Timestamp("2026-10-15T12:01:00Z")) == []


def test_application_responses_have_identity_including_errors():
    async def route(request):
        return PlainTextResponse("temporarily unavailable", status_code=503)

    app = Starlette(
        routes=[Route("/pro/", route)],
        middleware=[Middleware(WeatherResponseMiddleware)],
    )
    with TestClient(app) as client:
        response = client.get("/pro/")
        assert response.status_code == 503
        assert response.headers["X-Meteo-App"] == "v5"


def test_pro_does_not_rebuild_a_missing_publication(sqlite_engine, monkeypatch):
    import v5_data
    from config import Settings

    def forbidden(*args, **kwargs):
        pytest.fail("Opening Pro must not rebuild forecasts")

    monkeypatch.setattr(v5_data, "build_snapshot", forbidden)
    assert (
        v5_data.read_snapshot(Settings.from_env().station_id, published_only=True) == {}
    )


@pytest.mark.parametrize(
    "tab,title",
    [("windows", "Quanto può durare la finestra?"), ("precision", "Quanto ci prende?")],
)
def test_latest_panels_are_native_and_other_tabs_do_not_run(
    tmp_path, monkeypatch, request, tab, title
):
    from pathlib import Path

    import streamlit as st
    from streamlit.testing.v1 import AppTest

    from db import reset_engine_cache

    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("SQLITE_PATH", str(tmp_path / "pro-panels.db"))
    reset_engine_cache()
    st.cache_data.clear()
    request.addfinalizer(reset_engine_cache)
    request.addfinalizer(st.cache_data.clear)
    app = AppTest.from_file(
        Path(__file__).parents[1] / "app_pro.py", default_timeout=30
    )
    app.query_params["tab"] = tab
    app.run()
    assert not app.exception
    assert title in {item.value for item in app.subheader}
    assert not app.get("plotly_chart")
    assert not any("Meteo V4" in item.value for item in app.markdown)
