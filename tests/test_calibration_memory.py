"""Streaming calibration must preserve the scores of the full archive reader."""

import json

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import create_engine, text

import v5_calibration as calibration


@pytest.mark.parametrize("station_id", ["primary", "secondary"])
def test_streamed_history_preserves_scores_and_discards_repeated_full_runs(
    monkeypatch, station_id
):
    engine = create_engine("sqlite://")
    monkeypatch.setattr(calibration, "get_engine", lambda: engine)
    monkeypatch.setenv("STATION_ID", "primary")
    targets = pd.date_range("2026-07-01T00:00Z", periods=40 * 24, freq="h")
    now = targets[-1] + pd.Timedelta(hours=2)
    observations = pd.DataFrame(
        {
            "time": targets,
            "temp_c": 20 + np.sin(np.arange(len(targets)) / 4),
            "humidity": 60,
            "wind_kmh": 5,
            "rain_mm": 0,
        }
    )
    rows = []
    for i, target in enumerate(targets):
        for lead in (1, 3, 6, 7, 12, 24, 25, 48, 72):
            issued = target - pd.Timedelta(hours=lead)
            rows.append(
                {
                    "valid_time": target.isoformat(),
                    "issued_at": issued.isoformat(),
                    "interval_hours": 1,
                    "temp_c": observations.iloc[i].temp_c + lead / 10,
                    "humidity": 60,
                    "wind_kmh": 5,
                    "rain_mm": 0,
                    "unused_large_payload": "x" * 1000,
                }
            )
    # An ineligible late acquisition and a research run acquired after its target.
    with engine.begin() as con:
        con.execute(
            text(
                "CREATE TABLE location_model_runs "
                "(station_id TEXT, provider TEXT, model TEXT, issued_at TEXT, "
                "acquired_at TEXT, basis TEXT, payload TEXT)"
            )
        )
        for owner, basis, acquisition in (
            (station_id, "live", targets[0] - pd.Timedelta(days=4)),
            (station_id, "live", now),
            (station_id, "previous_run", now),
            ("unrelated", "live", targets[0] - pd.Timedelta(days=4)),
            (station_id, "published", now),
        ):
            con.execute(
                text(
                    "INSERT INTO location_model_runs VALUES "
                    "(:station, 'provider', 'model', :issued, :acquired, :basis, :payload)"
                ),
                {
                    "station": owner,
                    "issued": targets[0].isoformat(),
                    "acquired": acquisition.isoformat(),
                    "basis": basis,
                    "payload": json.dumps(rows),
                },
            )
    native = pd.DataFrame(rows).drop(columns="unused_large_payload")
    native["provider"], native["model"] = "native", "model"
    native["fetched_at"] = native.issued_at
    native.to_sql("forecast_runs", engine, index=False)
    native.drop(columns=["provider", "model", "fetched_at", "interval_hours"]).to_sql(
        "forecast_blend_history", engine, index=False
    )
    pd.DataFrame(
        [
            {
                "station_id": station_id,
                "issued_at": targets[0].isoformat(),
                "payload": json.dumps(rows),
            }
        ]
    ).to_sql("location_forecasts", engine, index=False)
    full = calibration.load_runs(station_id, now)
    streamed = calibration.load_verification_runs(station_id, now)
    assert len(streamed) < len(full) / 3
    assert (
        streamed.memory_usage(deep=True).sum() < full.memory_usage(deep=True).sum() / 5
    )
    assert "unused_large_payload" not in streamed
    assert (
        not streamed.acquired_at.gt(streamed.valid_time)
        .where(streamed.basis.ne("previous_run"), False)
        .any()
    )
    expected = calibration.verify(full, observations, now)
    actual = calibration.verify(streamed, observations, now)
    assert expected
    assert actual == expected
    engine.dispose()
