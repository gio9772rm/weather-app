import json

import pandas as pd
import pytest
from sqlalchemy import text

from v53_windows import sample_readiness, validate_windows
from v55_alert_verification import published_candidates, threshold_scores


def observations(start, hours):
    return pd.DataFrame(
        {
            "time": pd.date_range(
                start + pd.Timedelta(minutes=5), periods=12 * hours, freq="5min"
            ),
            "rain_mm": 0.0,
            "windgust_kmh": 10.0,
        }
    )


def test_readiness_requires_diverse_weather_and_never_applies_calibration():
    times = pd.date_range("2026-08-01T00:00Z", periods=30 * 12, freq="2h")
    all_dry = {t: (0.8, 1) for t in times}
    assert sample_readiness(all_dry)["status"] == "collecting"
    mixed = {t: (0.8, int(i % 10 != 0)) for i, t in enumerate(times)}
    result = sample_readiness(mixed)
    assert result["status"] == "ready_for_study" and not result["applied"]
    assert result["wet"] == 36 and result["wet_days"] >= 5


def test_window_counts_deduplicate_and_exclude_missing_and_pending(
    sqlite_engine, monkeypatch
):
    now = pd.Timestamp("2026-09-02T12:00Z")
    start = now - pd.Timedelta(hours=8)
    rows = [
        {
            "kind": "dry",
            "start": t.isoformat(),
            "end": (t + pd.Timedelta(hours=2)).isoformat(),
            "probability": p,
        }
        for t, p in (
            (start, 80),
            (start + pd.Timedelta(hours=2), None),
            (start + pd.Timedelta(hours=4), 90),
        )
    ]
    pending = {
        "kind": "dry",
        "start": (now + pd.Timedelta(hours=2)).isoformat(),
        "end": (now + pd.Timedelta(hours=4)).isoformat(),
        "probability": 80,
    }
    with sqlite_engine.begin() as con:
        for cycle, acquired, forecasts in (
            ("a", start - pd.Timedelta(hours=7), rows),
            ("b", start - pd.Timedelta(hours=6), rows),
            ("c", now - pd.Timedelta(hours=5), [pending]),
        ):
            con.execute(
                text(
                    "INSERT INTO window_predictions VALUES('one',:cycle,:at,:payload)"
                ),
                {
                    "cycle": cycle,
                    "at": acquired.isoformat(),
                    "payload": json.dumps({"rows": forecasts}),
                },
            )
    obs = observations(start, 6)
    obs.loc[50, "rain_mm"] = None
    monkeypatch.setattr("data_access.load_station", lambda *_: obs)
    result = validate_windows("one", now)
    assert result["n"] == 1 and result["brier"] == pytest.approx(0.04)
    c = result["collection"]
    assert c["cycles"] == 3 and c["eligible_windows"] == 4
    assert c["pending"] == c["missing_forecast"] == c["missing_observations"] == 1
    assert validate_windows("other", now)["collection"]["cycles"] == 0


def test_threshold_replay_includes_misses_and_correct_absences():
    start = pd.Timestamp("2026-09-01T00:00Z")
    obs = observations(start, 4)
    obs.loc[[0, 24], "rain_mm"] = 0.2
    forecasts = [
        {
            "valid_time": start + pd.Timedelta(hours=i + 1),
            "precip_probability": pop,
            "rain_mm": 0.2,
            "wind_gust_kmh": 35,
        }
        for i, pop in enumerate((70, 70, 20, 20))
    ]
    result = threshold_scores(forecasts, obs, start + pd.Timedelta(hours=5))
    rain = next(
        r for r in result["rows"] if r["kind"] == "rain" and r["threshold"] == 60
    )
    assert [
        rain[k] for k in ("hits", "false_alarms", "misses", "correct_negatives")
    ] == [1, 1, 1, 1]
    assert rain["precision_percent"] == rain["recall_percent"] == 50
    high = next(
        r for r in result["rows"] if r["kind"] == "rain" and r["threshold"] == 80
    )
    assert high["n"] == rain["n"] and high["precision_percent"] is None
    assert high["misses"] == 2 and not result["rules_changed"]


def test_incomplete_or_suspect_hours_never_count_as_correct_absences():
    start = pd.Timestamp("2026-09-01T00:00Z")
    obs = observations(start, 3)
    obs.loc[1, ["rain_mm", "windgust_kmh"]] = None
    obs["data_quality"] = "ok"
    obs.loc[15, "data_quality"] = "suspect"
    forecasts = [
        {
            "valid_time": start + pd.Timedelta(hours=i + 1),
            "precip_probability": 10,
            "rain_mm": 0,
            "wind_gust_kmh": 10,
        }
        for i in range(3)
    ]
    for row in threshold_scores(forecasts, obs, start + pd.Timedelta(hours=4))["rows"]:
        assert row["n"] == row["correct_negatives"] == 1
        assert row["missing_observations"] == 2
        assert row["recall_percent"] is None


def test_published_selection_is_causal_unique_and_station_scoped(sqlite_engine):
    now = pd.Timestamp("2026-09-02T12:00Z")
    target = now - pd.Timedelta(hours=1)
    with sqlite_engine.begin() as con:
        for station, hours, pop in (
            ("one", 7, 80),
            ("one", 4, 60),
            ("one", 2, 95),
            ("other", 3, 99),
        ):
            acquired = target - pd.Timedelta(hours=hours)
            payload = [
                {
                    "valid_time": target.isoformat(),
                    "forecast_issued_at": acquired.isoformat(),
                    "precip_probability": pop,
                }
            ]
            con.execute(
                text(
                    "INSERT INTO location_model_runs VALUES(:station,'canonical','published',:at,:at,'published',:payload)"
                ),
                {
                    "station": station,
                    "at": acquired.isoformat(),
                    "payload": json.dumps(payload),
                },
            )
    result = published_candidates("one", now)
    assert len(result) == 1 and result[0]["precip_probability"] == 60
    assert published_candidates("missing", now) == []
