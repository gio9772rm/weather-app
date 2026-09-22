from __future__ import annotations

import json
from dataclasses import replace

import pandas as pd
import pytest
from sqlalchemy import text

from config import Settings
from v5_astronomy import observing_forecast, summarize_observing_nights
from v5_data import build_snapshot, history_coverage, station_settings


def forecast_at(times, clouds=10):
    return pd.DataFrame(
        {
            "valid_time": pd.to_datetime(times, utc=True),
            "issued_at": pd.Timestamp("2026-09-20T12:00Z"),
            "temp_c": 18,
            "dewpoint_c": 10,
            "humidity": 60,
            "clouds": clouds,
            "wind_kmh": 4,
            "wind_gust_kmh": 6,
            "precip_probability": 0,
            "rain_mm": 0,
            "is_day": 0,
        }
    )


def test_v5_remaining_hours_exclude_past_and_preserve_fractional_window():
    cfg = replace(
        Settings.from_env(), latitude=41.9, longitude=12.5, local_timezone="Europe/Rome"
    )
    rows = forecast_at(["2026-09-20T20:00Z", "2026-09-20T21:00Z", "2026-09-20T22:00Z"])
    result = observing_forecast(rows, cfg, now=pd.Timestamp("2026-09-20T21:30Z"))
    nights = summarize_observing_nights(result)
    assert result["start"].min() == pd.Timestamp("2026-09-20T21:30Z")
    assert nights.iloc[0].remaining_good_hours == pytest.approx(1.5)
    assert nights.iloc[0].continuous_hours == pytest.approx(1.5)


def test_v5_cloud_cover_cannot_give_eleven_good_hours():
    cfg = Settings.from_env()
    rows = forecast_at(
        pd.date_range("2026-09-20T20:00Z", periods=10, freq="h"), clouds=69
    )
    rows["cloud_high"], rows["cloud_low"], rows["cloud_mid"] = 69, 0, 0
    result = observing_forecast(rows, cfg, now=pd.Timestamp("2026-09-20T19:00Z"))
    assert (result.astro_score < 65).all()
    assert summarize_observing_nights(result).remaining_good_hours.sum() == 0


def test_v5_missing_weather_has_no_invented_quality():
    rows = forecast_at(["2026-09-20T21:00Z"])
    rows["clouds"] = None
    result = observing_forecast(
        rows, Settings.from_env(), now=pd.Timestamp("2026-09-20T20:00Z")
    )
    assert not result.iloc[0].complete
    assert pd.isna(result.iloc[0].astro_score)
    assert result.iloc[0].astro_label == "Valutazione incompleta"


def test_v5_windows_never_bridge_a_missing_hour():
    rows = forecast_at(["2026-09-20T20:00Z", "2026-09-20T22:00Z"])
    result = observing_forecast(
        rows, Settings.from_env(), now=pd.Timestamp("2026-09-20T19:00Z")
    )
    night = summarize_observing_nights(result).iloc[0]
    assert night.continuous_hours == pytest.approx(1)
    assert len(night.windows) == 2


def test_v5_coverage_counts_dst_and_distinguishes_daily_import():
    frame = pd.DataFrame(
        [
            {
                "station_id": "test",
                "local_date": "2026-10-25",
                "sample_count": 300,
                "source": "ecowitt",
                "data_quality": "live_daily_aggregate",
            },
            {
                "station_id": "test",
                "local_date": "2026-10-27",
                "sample_count": None,
                "source": "ecowitt_daily_export",
                "data_quality": "historical_daily_summary",
            },
        ]
    )
    coverage = history_coverage(
        frame, "test", "Europe/Rome", pd.Timestamp("2026-10-28T12:00Z")
    )
    assert coverage["calendar"][0]["expected"] == 300
    assert coverage["calendar"][0]["status"] == "complete"
    assert coverage["calendar"][1]["status"] == "missing"
    assert coverage["calendar"][2]["status"] == "imported"
    assert coverage["days"] == 2


def test_v5_snapshot_does_not_expose_private_configuration(sqlite_engine, monkeypatch):
    monkeypatch.setenv("ECOWITT_API_KEY", "test-secret-value")
    result = build_snapshot(Settings.from_env().station_id)
    serialized = json.dumps(result, allow_nan=False)
    for secret in (
        "test-secret-value",
        "latitude",
        "longitude",
        "ecowitt_api_key",
        "admin_token",
    ):
        assert secret not in serialized
    assert result["refresh_seconds"] == 600
    assert result["live"] is False


def test_v5_unknown_station_is_not_primary_fallback(sqlite_engine):
    with pytest.raises(ValueError):
        station_settings("not-configured")
    with sqlite_engine.connect() as con:
        assert con.execute(text("SELECT COUNT(*) FROM public_snapshots")).scalar() == 0


def test_v5_subhour_intervals_do_not_overlap():
    rows = forecast_at(["2026-09-20T20:00Z", "2026-09-20T20:30Z", "2026-09-20T21:00Z"])
    result = observing_forecast(
        rows, Settings.from_env(), now=pd.Timestamp("2026-09-20T19:00Z")
    )
    assert summarize_observing_nights(result).remaining_good_hours.sum() == 2


def test_verification_excludes_hindsight_and_duplicate_emissions():
    from v5_verification import verification_scores

    times = pd.date_range("2026-09-01T12:00Z", periods=16, freq="h")
    original = pd.DataFrame(
        {"valid_time": times, "issued_at": times - pd.Timedelta(hours=3), "temp_c": 22}
    )
    duplicate = original.copy()
    duplicate["issued_at"] += pd.Timedelta(minutes=10)
    hindsight = original.copy()
    hindsight["issued_at"] += pd.Timedelta(hours=4)
    hindsight["temp_c"] = 90
    observations = pd.DataFrame({"time": times, "temp_c": 20})
    scores = verification_scores(
        pd.concat([original, duplicate, hindsight]),
        observations,
        pd.Timestamp("2026-09-10T12:00Z"),
    )
    assert len(scores) == 1
    assert scores[0]["n"] == 16 and scores[0]["mae"] == 2


def test_unknown_rain_is_not_a_dry_measurement():
    from station_daily import canonicalize_observations

    result = canonicalize_observations(
        pd.DataFrame({"time": ["2026-09-20T20:00Z"], "temp_c": [20]})
    )
    assert result.rain_mm.isna().all()
