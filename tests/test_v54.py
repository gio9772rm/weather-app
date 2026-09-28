"""Station isolation and honest comparisons for the V5.4 reliability summary."""

import pandas as pd
from sqlalchemy import text

from v5_calibration import archive_runs
from v54_reliability import model_spread, read_model_spread

NOW = pd.Timestamp("2026-09-28T20:00:00Z")


def run(model, value, **changes):
    return {
        "provider": "example",
        "model": model,
        "issued_at": "2026-09-28T18:00:00Z",
        "acquired_at": "2026-09-28T18:10:00Z",
        "basis": "live",
        "rows": [{"valid_time": NOW.isoformat(), "temp_c": value}],
        **changes,
    }


def test_only_same_hour_distinct_fresh_models_are_compared():
    runs = [run("a", 20), run("b", 23)]
    runs += [
        run("a", 30, issued_at="2026-09-28T17:00Z"),
        run("historical", 40, basis="previous_run"),
        run("future", 40, acquired_at="2026-09-28T21:00Z"),
        run("future-issued", 40, issued_at="2026-09-28T21:00Z"),
        run("old", 40, issued_at="2026-09-27T20:00Z"),
        run("unknown", 40, issued_at=None),
        run("canonical", 40, provider="canonical"),
        run("invalid", float("nan")),
        run("other-hour", 40, rows=[{"valid_time": "2026-09-28T21:00Z", "temp_c": 40}]),
    ]
    rows = model_spread(runs, NOW)["rows"]
    assert len(rows) == 1
    assert rows[0]["model_count"] == 2
    assert rows[0]["spread_c"] == 3


def test_latest_emission_wins_and_no_models_is_unknown():
    assert model_spread([run("a", 20), run("a", 21)], NOW)["rows"] == []
    r = model_spread(
        [
            run("a", 20),
            run(
                "a", 24, issued_at="2026-09-28T19:00Z", acquired_at="2026-09-28T19:10Z"
            ),
            run("b", 23),
        ],
        NOW,
    )
    assert r["rows"][0]["spread_c"] == 1
    assert model_spread([], NOW)["rows"] == []


def test_model_archive_is_station_scoped(sqlite_engine, monkeypatch):
    monkeypatch.setenv("STATION_ID", "rome-test")
    for station, values in [("rome-test", [20, 23]), ("coast-test", [10, 11])]:
        for model, value in zip(["a", "b"], values):
            frame = pd.DataFrame(
                [
                    {
                        **run(model, value)["rows"][0],
                        "provider": "example",
                        "model": model,
                        "issued_at": "2026-09-28T18:00Z",
                    }
                ]
            )
            archive_runs(station, frame, acquired_at=NOW)
    assert read_model_spread("rome-test", NOW)["rows"][0]["spread_c"] == 3
    assert read_model_spread("coast-test", NOW)["rows"][0]["spread_c"] == 1
    assert read_model_spread("missing-test", NOW)["rows"] == []


def test_primary_legacy_sources_are_used_only_for_primary(sqlite_engine, monkeypatch):
    monkeypatch.setenv("STATION_ID", "rome-test")
    with sqlite_engine.begin() as con:
        for model, value in [("a", 18), ("b", 22)]:
            con.execute(
                text(
                    "INSERT INTO forecast_runs(provider,model,issued_at,fetched_at,valid_time,temp_c) VALUES('source',:model,'2026-09-28T18:00:00Z','2026-09-28T18:05:00Z','2026-09-28T20:00:00Z',:value)"
                ),
                {"model": model, "value": value},
            )
    assert read_model_spread("rome-test", NOW)["rows"][0]["spread_c"] == 4
    assert read_model_spread("coast-test", NOW)["rows"] == []
