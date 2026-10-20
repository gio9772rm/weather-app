"""Secondary verification must not materialise hundreds of full forecast runs."""

import json

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, event, text

import v5_verification as verification


def archive(engine, owner, issued, rows):
    with engine.begin() as con:
        con.execute(
            text("INSERT INTO location_forecasts VALUES (:id,:issued,:payload)"),
            {"id": owner, "issued": issued.isoformat(), "payload": json.dumps(rows)},
        )


def database(monkeypatch):
    engine = create_engine("sqlite://")
    with engine.begin() as con:
        con.execute(
            text(
                "CREATE TABLE location_forecasts (station_id TEXT, issued_at TEXT, payload TEXT)"
            )
        )
    monkeypatch.setattr(verification, "get_engine", lambda: engine)
    return engine


def test_secondary_stream_preserves_every_score_and_releases_unused_payload(
    monkeypatch,
):
    engine = database(monkeypatch)
    targets = pd.date_range("2026-10-01T00:00Z", periods=240, freq="h")
    now = targets[-1] + pd.Timedelta(hours=1)
    times = pd.date_range(targets[0] - pd.Timedelta(days=8), now, freq="h")
    observations = pd.DataFrame(
        {
            "time": times,
            "temp_c": 20 + np.sin(np.arange(len(times)) / 6),
            "humidity": 60,
            "pressure_hpa": 1010,
            "wind_kmh": 5,
        }
    )
    rows = []
    for index, target in enumerate(targets):
        for lead in (1, 3, 6, 7, 12, 24, 25, 48, 72, 73, 96, 168):
            rows.append(
                {
                    "valid_time": target.isoformat(),
                    "issued_at": (target - pd.Timedelta(hours=lead)).isoformat(),
                    "temp_c": 21 + np.sin(index / 6) + lead / 20,
                    "humidity": 61,
                    "pressure_hpa": 1011,
                    "wind_kmh": 6,
                    "unused": "x" * 1200,
                }
            )
    archive(engine, "secondary", targets[0], rows)
    archive(
        engine,
        "other-station",
        targets[0],
        [{**row, "temp_c": -1000} for row in rows[:30]],
    )
    options = []

    @event.listens_for(engine, "before_cursor_execute")
    def execution_options(connection, cursor, statement, parameters, context, many):
        options.append(context.execution_options)

    full = pd.DataFrame(rows)
    selected = verification.load_secondary_verification_runs("secondary", now)
    assert options[0].get("yield_per") == 1
    assert options[0].get("stream_results")
    assert len(selected) == len(targets) * 4
    assert (
        selected.memory_usage(deep=True).sum() < full.memory_usage(deep=True).sum() / 10
    )
    expected = verification.verification_scores(full, observations, now)
    actual = verification.verification_scores(selected, observations, now)
    assert expected and actual == expected
    assert {score["horizon"] for score in actual} == {
        "1–6 h",
        "6–24 h",
        "24–72 h",
        "oltre 72 h",
    }
    engine.dispose()


def test_secondary_stream_keeps_owner_lookback_and_720_run_limit(monkeypatch):
    engine = database(monkeypatch)
    now = pd.Timestamp("2026-10-20T12:00Z")
    target = now - pd.Timedelta(hours=1)
    for index in range(721):
        issued = now - pd.Timedelta(hours=2, minutes=index)
        archive(
            engine,
            "secondary",
            issued,
            [
                {
                    "valid_time": target.isoformat(),
                    "issued_at": issued.isoformat(),
                    "temp_c": index,
                }
            ],
        )
    old = now - pd.Timedelta(days=40)
    archive(
        engine,
        "secondary",
        old,
        [
            {
                "valid_time": target.isoformat(),
                "issued_at": old.isoformat(),
                "temp_c": 9000,
            }
        ],
    )
    other = now - pd.Timedelta(days=5)
    archive(
        engine,
        "other-station",
        other,
        [
            {
                "valid_time": target.isoformat(),
                "issued_at": other.isoformat(),
                "temp_c": 8000,
            }
        ],
    )
    selected = verification.load_secondary_verification_runs("secondary", now)
    assert set(selected.temp_c) == {300, 719}
    assert selected.valid_time.eq(target).all()
    assert not verification.load_secondary_verification_runs("missing", now).size
    engine.dispose()


def test_secondary_stream_ignores_future_invalid_and_short_leads(monkeypatch):
    engine = database(monkeypatch)
    now = pd.Timestamp("2026-10-20T12:00Z")
    target = now - pd.Timedelta(hours=1)
    rows = [
        {
            "valid_time": target.isoformat(),
            "issued_at": (target - pd.Timedelta(hours=lead)).isoformat(),
            "temp_c": lead,
        }
        for lead in (0.5, 1, 6, 24, 72, 168)
    ]
    rows.extend(
        [
            {
                "valid_time": now.isoformat(),
                "issued_at": (now - pd.Timedelta(hours=1)).isoformat(),
                "temp_c": -1000,
            },
            {"valid_time": "invalid", "issued_at": target.isoformat(), "temp_c": -2000},
        ]
    )
    archive(engine, "secondary", target, rows)
    selected = verification.load_secondary_verification_runs("secondary", now)
    assert set(selected.temp_c) == {6, 24, 72, 168}
    engine.dispose()
