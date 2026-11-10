from dataclasses import replace

import pandas as pd
import pytest

from config import Settings
from tests.test_v5 import forecast_at
from v5_astronomy import observing_forecast
from v5_cielo import ObservingContext, ObservingSite, night_report
from v5_data import records
from v5_decision import cloud_disagreement, quick_check
from v5_projects import project_spec, saved_projects
from v53_windows import (
    missing_rain_slots,
    rain_slots,
    window_rain_hourly,
    window_rain_input,
)


def test_quick_check_separates_actual_radar_from_forecast_and_remote_sites():
    now = pd.Timestamp("2026-11-10T19:20Z")
    snapshot = {
        "forecast": [
            {"valid_time": "2026-11-10T18:00Z", "rain_mm": 10},
            {"valid_time": "2026-11-10T19:00Z", "rain_mm": 2, "clouds": 42},
            {"valid_time": "2026-11-10T20:00Z", "rain_mm": None},
        ],
        "radar": [
            {"sri_observed_at": "2026-11-10T19:10Z", "sri_point_mm_h": 0},
            {"sri_observed_at": "2026-11-10T19:25Z", "sri_point_mm_h": 8},
        ],
    }
    result = quick_check(snapshot, now)
    assert result["radar"]["at"] == pd.Timestamp("2026-11-10T19:10Z")
    assert result["radar"]["rate_mm_h"] == 0
    assert [r["rain_mm"] for r in result["upcoming"]] == [2, None]
    assert result["upcoming"][0]["start"] == now.floor("h")
    assert quick_check(snapshot, now, local=False)["radar"] is None


def test_model_disagreement_compares_the_same_layer_and_hour():
    at = "2026-11-10T20:00:00Z"
    row = {"valid_time": at, "provider": "example", "issued_at": "2026-11-10T12:00:00Z"}
    result = cloud_disagreement(
        [
            {**row, "model": "one", "cloud_high": 20, "cloud_low": 95},
            {
                **row,
                "model": "one",
                "issued_at": "2026-11-10T11:00:00Z",
                "cloud_high": 99,
            },
            {**row, "model": "two", "cloud_high": 70},
            {**row, "provider": "canonical", "model": "baseline", "cloud_high": 100},
            {
                **row,
                "valid_time": "2026-11-10T21:00:00Z",
                "model": "two",
                "cloud_low": 1,
            },
        ]
    )
    assert result == [
        {
            "valid_time": at,
            "field": "cloud_high",
            "low": 20,
            "high": 70,
            "low_model": "example/one",
            "high_model": "example/two",
            "spread": 50,
        }
    ]


def test_exclusions_distinguish_absent_slots_and_unusable_rain_without_reclassifying_dry():
    start = pd.Timestamp("2026-11-10T20:00Z")
    end = start + pd.Timedelta(hours=2)
    observations = pd.DataFrame(
        {
            "time": pd.date_range(start + pd.Timedelta(minutes=5), end, freq="5min"),
            "rain_mm": 0.0,
            "data_quality": "stuck_humidity",
        }
    ).drop(index=2)
    observations.loc[18, "data_quality"] = "estimated_rain"
    details = missing_rain_slots(
        rain_slots(window_rain_input(observations)), start, end
    )
    assert details == {
        "missing_slots": 1,
        "unusable_slots": 1,
        "windows_with_gaps": 1,
        "windows_with_unusable_rain": 1,
        "missing_slots_with_nearby_measurement": 0,
    }
    assert window_rain_hourly(observations)["rain_mm"].isna().all()


def test_comparison_applies_each_horizon_and_skips_the_full_graph():
    cfg = replace(
        Settings.from_env(), latitude=41.9, longitude=12.5, local_timezone="Europe/Rome"
    )
    now = pd.Timestamp("2026-11-10T17:30Z")
    forecast = forecast_at(pd.date_range(now.floor("h"), periods=73, freq="h"))
    snapshot = {
        "forecast": records(forecast),
        "astronomy": {
            "deep_sky": {"hours": records(observing_forecast(forecast, cfg, now=now))}
        },
    }
    context = ObservingContext(ObservingSite.from_station(cfg), snapshot, now)
    project = {
        "target": "M81",
        "filters": [
            {
                "name": "L",
                "desired_seconds": 7200,
                "acquired_seconds": 3600,
                "exposure_seconds": 120,
            },
        ],
    }
    options = {"compare_only": True, "project": project, "altitude": 0, "moon": 0}
    clear = night_report(context, options)
    blocked = night_report(context, {**options, "horizon": {"0": 90, "180": 90}})
    assert clear["timeline"] is None and not clear["targets"]
    assert clear["project"]["best"]["useful_seconds"] > 0
    assert blocked["project"]["best"] is None
    assert any(n["limiting_factor"] for n in blocked["project"]["nights"])


def test_private_profile_preserves_valid_imports_and_public_project_drops_receipts():
    receipt = {
        "day": "2026-11-10",
        "batch": "Principale",
        "digest": "a" * 64,
        "filters": [{"name": "L", "frames": 5, "seconds": 600}],
    }
    project = {
        "target": "M81",
        "filters": [
            {
                "name": "L",
                "desired_seconds": 7200,
                "acquired_seconds": 0,
                "exposure_seconds": 120,
            }
        ],
        "imports": [receipt],
    }
    assert saved_projects([project])[0]["imports"] == [receipt]
    assert "imports" not in project_spec(project)
    with pytest.raises(ValueError):
        saved_projects(
            [{**project, "imports": [receipt, {**receipt, "day": "2026-11-09"}]}]
        )
