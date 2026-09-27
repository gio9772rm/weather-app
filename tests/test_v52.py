from __future__ import annotations

import json

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import text

from v5_calibration import apply_calibration, archive_runs, observed_hourly, verify
from v5_insights import forecast_revisions, monthly_archive, quality_report
from v5_sessions import session_limits, target_sessions
from v5_sources import historical_time, parse_previous_runs, parse_weathernext


def test_event_replay_preserves_changed_publication_and_excludes_hindsight(
    sqlite_engine, monkeypatch
):
    from v5_calibration import archive_publication
    from v5_insights import event_replay

    monkeypatch.setenv("STATION_ID", "replay-test")
    now = pd.Timestamp.now(tz="UTC").floor("h")
    forecast = pd.DataFrame(
        {
            "issued_at": [now - pd.Timedelta(hours=5)],
            "valid_time": [now - pd.Timedelta(hours=1)],
            "temp_c": [20],
        }
    )
    first, revised, later = [now - pd.Timedelta(hours=h) for h in (4, 3, 1)]
    assert archive_publication("replay-test", forecast, first) == 1
    assert (
        archive_publication("replay-test", forecast, first + pd.Timedelta(minutes=10))
        == 0
    )
    assert archive_publication("replay-test", forecast.assign(temp_c=21), revised) == 1
    archive_publication("replay-test", forecast.assign(temp_c=99), later)
    archive_publication("other-station", forecast.assign(temp_c=70), revised)
    result = event_replay("replay-test", now - pd.Timedelta(hours=2), now)
    assert result["forecast"][0]["temp_c"] == 21
    assert pd.Timestamp(result["forecast_known_at"]) == revised
    local = lambda at: at.tz_convert("Europe/Rome").strftime("%Y-%m-%dT%H:%M")
    same = event_replay("replay-test", local(now - pd.Timedelta(hours=2)), local(now))
    assert same["forecast"] == result["forecast"]


def test_quality_reads_original_rain_without_clipping(sqlite_engine, monkeypatch):
    from v5_insights import station_quality

    monkeypatch.setenv("STATION_ID", "quality-test")
    now = pd.Timestamp.now(tz="UTC").floor("min")
    with sqlite_engine.begin() as con:
        con.execute(
            text(
                "INSERT INTO station_raw(time,temp_c,humidity,rain_mm,source) VALUES(:at,20,60,-1,'ecowitt')"
            ),
            {"at": now.strftime("%Y-%m-%dT%H:%M:%SZ")},
        )
    rain = next(
        r
        for r in station_quality("quality-test", now)["measurements"]
        if r["variable"] == "rain_mm"
    )
    assert rain["value"] == -1 and rain["status"] == "suspect"


def validation_data(days=50):
    times = pd.date_range("2026-07-01T00:00Z", periods=days * 24 * 12, freq="5min")
    temp = 20 + 5 * np.sin(np.arange(len(times)) * 2 * np.pi / 288)
    obs = pd.DataFrame(
        {"time": times, "temp_c": temp, "humidity": 60, "wind_kmh": 5, "rain_mm": 0}
    )
    targets = obs[obs.time.dt.minute.eq(0)].iloc[4:].copy()
    forecast = pd.DataFrame(
        {
            "valid_time": targets.time,
            "issued_at": targets.time - pd.Timedelta(hours=3),
            "temp_c": targets.temp_c + 3,
            "humidity": 60,
            "wind_kmh": 5,
            "rain_mm": 0,
            "interval_hours": 1,
        }
    )
    return forecast, obs, times[-1] + pd.Timedelta(hours=1)


def test_calibration_uses_later_holdout_and_a_causal_persistence():
    forecast, obs, now = validation_data()
    scores = verify(forecast, obs, now)
    temperature = next(s for s in scores if s["variable"] == "temp_c")
    assert temperature["applied"]
    assert temperature["training_bias"] == pytest.approx(3)
    assert temperature["corrected_mae"] < 0.01
    assert temperature["persistence_mae"] > 0
    assert temperature["training_end"] < temperature["validation_start"] - pd.Timedelta(
        hours=3
    )
    assert temperature["days"] >= 30 and temperature["holdout_n"] >= 30


def test_short_history_and_failed_validation_never_activate():
    forecast, obs, now = validation_data(10)
    assert not any(s["applied"] for s in verify(forecast, obs, now))
    forecast, obs, now = validation_data()
    forecast.loc[forecast.index[int(len(forecast) * 0.8) :], "temp_c"] -= 6
    result = next(s for s in verify(forecast, obs, now) if s["variable"] == "temp_c")
    assert not result["applied"] and result["training_bias"] == pytest.approx(3)


def test_research_models_and_reconstructed_archives_cannot_activate():
    forecast, obs, now = validation_data()
    forecast["provider"], forecast["model"] = "experimental", "new-model"
    scores = verify(forecast, obs, now)
    assert not any(s["applied"] for s in scores)
    forecast["provider"], forecast["basis"] = "canonical", "previous_run"
    assert not any(s["applied"] for s in verify(forecast, obs, now))


def test_late_acquisition_and_duplicate_emissions_do_not_inflate_skill():
    forecast, obs, now = validation_data(5)
    expected = verify(forecast, obs, now)
    duplicate = forecast.copy()
    duplicate.issued_at += pd.Timedelta(minutes=10)
    late = forecast.copy()
    late["acquired_at"] = late.valid_time + pd.Timedelta(hours=1)
    result = verify(pd.concat([forecast, duplicate, late], ignore_index=True), obs, now)
    # Fill explicit acquisition for the two live copies, not a missing timestamp.
    combined = pd.concat(
        [
            forecast.assign(acquired_at=forecast.issued_at),
            duplicate.assign(acquired_at=duplicate.issued_at),
            late,
        ],
        ignore_index=True,
    )
    result = verify(combined, obs, now)
    assert [(s["variable"], s["n"]) for s in result] == [
        (s["variable"], s["n"]) for s in expected
    ]


def test_hourly_rain_alignment_and_missing_coverage():
    times = pd.date_range("2026-09-01T00:05Z", periods=24, freq="5min")
    frame = pd.DataFrame({"time": times, "rain_mm": 0.1})
    result = observed_hourly(frame)
    assert result.iloc[0].time == pd.Timestamp("2026-09-01T01:00Z")
    assert result.iloc[0].rain_mm == pytest.approx(1.2)
    frame.loc[12:16, "rain_mm"] = None
    assert pd.isna(observed_hourly(frame).iloc[-1].rain_mm)
    duplicated = pd.concat([frame, frame], ignore_index=True)
    assert observed_hourly(duplicated).iloc[0].rain_mm == pytest.approx(1.2)


def test_apply_preserves_originals_bounds_and_stale_fallback():
    now = pd.Timestamp("2026-09-01T00:00Z")
    frame = pd.DataFrame(
        {
            "issued_at": [now],
            "valid_time": [now + pd.Timedelta(hours=3)],
            "temp_c": [21.0],
            "humidity": [98.0],
            "rain_mm": [0.0],
        }
    )
    rules = [
        {
            "provider": "canonical",
            "basis": "live",
            "applied": True,
            "variable": var,
            "horizon": "0–6 h",
            "training_bias": bias,
            "rain_factor": 2.0,
            "corrected_mae": 0.3,
            "holdout_n": 50,
        }
        for var, bias in (("temp_c", 2), ("humidity", -8), ("rain_mm", 0))
    ]
    result = apply_calibration(frame, {"evaluated_at": now, "scores": rules}, now)
    assert result.iloc[0].temp_c == 19 and result.iloc[0].humidity == 100
    assert result.iloc[0].rain_mm == 0
    assert result.iloc[0].local_corrections["temp_c"]["original"] == 21
    assert frame.iloc[0].temp_c == 21
    assert result.iloc[0].dewpoint_c <= result.iloc[0].temp_c
    stale = apply_calibration(
        frame, {"evaluated_at": now - pd.Timedelta(days=2), "scores": rules}, now
    )
    pd.testing.assert_frame_equal(stale, frame)


def test_archiving_is_station_scoped_idempotent_and_allowlisted(sqlite_engine):
    frame, _, now = validation_data(1)
    frame = frame.iloc[:2].copy()
    frame["latitude"], frame["api_key"] = 1.23, "must-not-leak"
    assert archive_runs("rome-test", frame, acquired_at=now) == 2
    archive_runs("rome-test", frame.assign(temp_c=99), acquired_at=now)
    archive_runs("comacchio-test", frame.assign(temp_c=5), acquired_at=now)
    with sqlite_engine.connect() as con:
        rows = (
            con.execute(text("SELECT station_id,payload FROM location_model_runs"))
            .mappings()
            .all()
        )
    assert len(rows) == 4
    assert all(
        "latitude" not in r["payload"] and "must-not-leak" not in r["payload"]
        for r in rows
    )
    rome = [json.loads(r["payload"])[0] for r in rows if r["station_id"] == "rome-test"]
    assert all(r["temp_c"] != 99 for r in rome)


def test_six_hour_rainfall_is_never_verified_as_one_hour():
    forecast, obs, now = validation_data(5)
    forecast["interval_hours"] = 6
    assert "rain_mm" not in {s["variable"] for s in verify(forecast, obs, now)}


def test_quality_distinguishes_frozen_stale_previous_and_missing():
    now = pd.Timestamp("2026-09-01T04:00Z")
    times = pd.date_range(now - pd.Timedelta(hours=4), now, freq="5min")
    data = pd.DataFrame(
        {
            "time": times,
            "temp_c": 20.0,
            "humidity": 50.0 + np.arange(len(times)) / 10,
            "pressure_hpa": 1000.0 + np.arange(len(times)) / 10,
        }
    )
    data.loc[data.index[-1], "humidity"] = None
    data.loc[data.index[-8:], "pressure_hpa"] = None
    report = quality_report(data, now)
    rows = {r["variable"]: r for r in report["measurements"]}
    assert rows["temp_c"]["status"] == "suspect"
    assert rows["humidity"]["status"] == "previous_sample"
    assert rows["pressure_hpa"]["status"] == "stale"
    assert rows["rain_mm"]["status"] == "missing" and rows["rain_mm"]["value"] is None
    assert data.temp_c.eq(20).all()


def test_quality_reports_future_clock_and_raw_negative_rain():
    now = pd.Timestamp("2026-09-01T04:00Z")
    report = quality_report(
        pd.DataFrame({"time": [now, now + pd.Timedelta(hours=2)], "rain_mm": [-1, 0]}),
        now,
    )
    assert any(issue["kind"] == "future_timestamp" for issue in report["issues"])
    assert (
        next(r for r in report["measurements"] if r["variable"] == "rain_mm")["status"]
        == "suspect"
    )


def test_monthly_totals_exclude_partial_days_and_break_dry_spells():
    calendar = [
        {
            "date": f"2026-09-0{i}",
            "status": state,
            "rain_mm": rain,
            "temp_min_c": 10,
            "temp_max_c": 20,
            "temp_mean_c": 15,
        }
        for i, (state, rain) in enumerate(
            [
                ("complete", 0),
                ("complete", 0),
                ("missing", None),
                ("imported", 0),
                ("partial", 100),
                ("complete", 4),
            ],
            start=1,
        )
    ]
    result = monthly_archive(calendar, "Europe/Rome", pd.Timestamp("2026-09-10T12:00Z"))
    month = result["months"][0]
    assert month["rain_sum_mm"] == 4 and month["longest_dry_spell_days"] == 2
    assert month["expected_days"] == 9 and month["is_partial"]
    assert month["imported_days"] == 1 and month["partial_days"] == 1
    assert result["records"]["rain_mm"]["value"] == 4


def test_monthly_unknown_rain_is_neither_zero_nor_dry():
    result = monthly_archive(
        [{"date": "2026-09-01", "status": "complete", "rain_mm": None}],
        "Europe/Rome",
        pd.Timestamp("2026-09-03T12:00Z"),
    )
    row = result["months"][0]
    assert (
        pd.isna(row["rain_sum_mm"])
        and row["rainy_days"] is None
        and row["longest_dry_spell_days"] is None
    )


def test_revision_compares_only_shared_future_hours():
    now = pd.Timestamp("2026-09-01T12:00Z")
    a = pd.DataFrame(
        {
            "valid_time": [now, now + pd.Timedelta(hours=1)],
            "temp_c": [25, 40],
            "issued_at": now,
        }
    )
    b = pd.DataFrame(
        {
            "valid_time": [now, now - pd.Timedelta(hours=1)],
            "temp_c": [22, 5],
            "issued_at": now - pd.Timedelta(hours=2),
        }
    )
    result = forecast_revisions(a, b, now)
    assert result["compared_hours"] == 1 and result["changes"][0]["delta"] == 3


def test_weathernext_retains_native_intervals_and_rejects_interpolation():
    now = pd.Timestamp("2026-09-01T00:00Z")
    payload = {
        "hourly": {
            "time": ["2026-09-01T06:00", "2026-09-01T12:00"],
            "temperature_2m": [21, 25],
            "rain": [6, 0],
        }
    }
    frame = parse_weathernext(payload, now)
    assert frame.interval_hours.eq(6).all() and frame.iloc[0].rain_mm == 6
    payload["hourly"]["time"][1] = "2026-09-01T07:00"
    with pytest.raises(ValueError):
        parse_weathernext(payload, now)


def test_previous_runs_preserve_valid_times_and_fixed_leads():
    payload = {
        "hourly": {
            "time": ["2026-09-01T12:00"],
            "temperature_2m_previous_day1": [20],
            "temperature_2m_previous_day3": [21],
        }
    }
    frame = parse_previous_runs(
        payload, "icon_seamless", pd.Timestamp("2026-09-03T00:00Z")
    )
    assert len(frame) == 2
    assert (frame.valid_time - frame.issued_at).dt.total_seconds().tolist() == [
        86400,
        3 * 86400,
    ]


def session_data():
    times = pd.date_range("2026-09-01T21:00Z", periods=5, freq="15min")
    tracks = pd.DataFrame(
        {"target": "M31", "valid_time": times, "visible": True, "moon_separation": 60.0}
    )
    weather = pd.DataFrame(
        {
            "start": [times[0]],
            "end": [times[-1]],
            "clouds": 10.0,
            "wind_kmh": 5.0,
            "wind_gust_kmh": 7.0,
            "temp_c": 20.0,
            "dewpoint_c": 12.0,
            "precip_probability": 0.0,
            "rain_mm": 0.0,
            "astro_score": 90.0,
            "limiting_factor": "Nuvole",
        }
    )
    return tracks, weather


def test_session_combines_geometry_moon_weather_and_equipment():
    tracks, weather = session_data()
    result, detail = target_sessions(tracks, weather, session_limits("deep_sky"), 30)
    assert result[0]["continuous_hours"] == 1 and all(r["usable"] for r in detail)
    limited, _ = target_sessions(
        tracks, weather, session_limits("deep_sky", {"wind_kmh": 4}), 30
    )
    assert limited[0]["usable_hours"] == 0
    moon, _ = target_sessions(tracks, weather, session_limits("deep_sky"), 70)
    assert moon[0]["usable_hours"] == 0 and "Luna" in moon[0]["limiting_factor"]


def test_session_never_bridges_weather_gaps_or_missing_gusts():
    tracks, weather = session_data()
    first, last = weather.copy(), weather.copy()
    first["end"] = tracks.valid_time.iloc[1]
    last["start"] = tracks.valid_time.iloc[3]
    result, _ = target_sessions(
        tracks, pd.concat([first, last]), session_limits("deep_sky"), 30
    )
    assert result[0]["continuous_hours"] == 0.25 and result[0]["usable_hours"] == 0.5
    weather["wind_gust_kmh"] = None
    assert (
        target_sessions(tracks, weather, session_limits("deep_sky"), 30)[0][0][
            "usable_hours"
        ]
        == 0
    )


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -1, 101])
def test_session_limits_reject_nonfinite_and_unphysical(value):
    with pytest.raises(ValueError):
        session_limits("visual", {"clouds": value})


def test_dpc_history_is_bounded_and_rounds_down():
    now = pd.Timestamp("2026-09-20T12:00Z")
    assert historical_time("2026-09-19T11:13Z", now).minute == 10
    for invalid in ["2026-09-21T00:00Z", "2026-09-01T00:00Z", "NaT"]:
        with pytest.raises(ValueError):
            historical_time(invalid, now)
