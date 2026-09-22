"""Measure second-station forecast errors without touching Rome's calibration."""

from __future__ import annotations

import json

import numpy as np
import pandas as pd
from sqlalchemy import text

from data_access import load_station
from db import get_engine


def verification_scores(
    forecast: pd.DataFrame, observations: pd.DataFrame, now: pd.Timestamp
) -> list[dict]:
    if forecast.empty or observations.empty:
        return []
    forecast = forecast.copy()
    for col in ("valid_time", "issued_at"):
        forecast[col] = pd.to_datetime(forecast[col], utc=True, errors="coerce")
    lead = (forecast.valid_time - forecast.issued_at).dt.total_seconds() / 3600
    forecast["horizon"] = pd.cut(
        lead,
        [1, 6, 24, 72, np.inf],
        labels=["1–6 h", "6–24 h", "24–72 h", "oltre 72 h"],
        include_lowest=True,
    )
    forecast = forecast[
        (lead >= 1) & forecast.valid_time.lt(now) & forecast.horizon.notna()
    ]
    # One forecast per target/hour and horizon; hourly emissions are not
    # independent observations. Use the longest archived lead in each bucket.
    forecast = (
        forecast.sort_values("issued_at")
        .drop_duplicates(["valid_time", "horizon"])
        .sort_values("valid_time")
    )
    if forecast.empty:
        return []
    observations = observations.copy()
    observations["time"] = pd.to_datetime(
        observations["time"], utc=True, errors="coerce"
    )
    observations = observations[observations.time.le(now)].sort_values("time")
    pairs = pd.merge_asof(
        forecast,
        observations,
        left_on="valid_time",
        right_on="time",
        tolerance=pd.Timedelta(minutes=20),
        direction="nearest",
        suffixes=("_forecast", "_observed"),
    )
    result = []
    for horizon, group in pairs.groupby("horizon", observed=True):
        for variable in ("temp_c", "humidity", "pressure_hpa", "wind_kmh"):
            columns = [variable + "_forecast", variable + "_observed"]
            if not set(columns).issubset(group.columns):
                continue
            pair = group[columns].apply(pd.to_numeric, errors="coerce").dropna()
            if len(pair) < 12:
                continue
            error = pair.iloc[:, 0] - pair.iloc[:, 1]
            holdout = error.iloc[int(len(error) * 0.8) :]
            result.append(
                {
                    "provider": "selezione_indipendente",
                    "model": "ICON-2I / best-match",
                    "variable": variable,
                    "horizon": str(horizon),
                    "n": len(error),
                    "mae": float(error.abs().mean()),
                    "rmse": float(np.sqrt((error**2).mean())),
                    "bias": float(error.mean()),
                    "holdout_n": len(holdout),
                    "holdout_mae": float(holdout.abs().mean()),
                    "skill_vs_persistence": None,
                }
            )
    return result


def update_secondary_scores(station_id: str) -> None:
    now = pd.Timestamp.now(tz="UTC")
    with get_engine().connect() as con:
        payloads = (
            con.execute(
                text(
                    "SELECT payload FROM location_forecasts WHERE station_id=:id AND issued_at>=:cutoff ORDER BY issued_at DESC LIMIT 336"
                ),
                {"id": station_id, "cutoff": (now - pd.Timedelta(days=14)).isoformat()},
            )
            .scalars()
            .all()
        )
    forecast = (
        pd.concat([pd.DataFrame(json.loads(p)) for p in payloads], ignore_index=True)
        if payloads
        else pd.DataFrame()
    )
    scores = verification_scores(forecast, load_station(24 * 14, station_id), now)
    with get_engine().begin() as con:
        con.execute(
            text(
                "INSERT INTO location_scores(station_id,evaluated_at,payload) VALUES(:id,:at,:payload) ON CONFLICT(station_id) DO UPDATE SET evaluated_at=excluded.evaluated_at,payload=excluded.payload"
            ),
            {
                "id": station_id,
                "at": now.isoformat(),
                "payload": json.dumps(scores, allow_nan=False),
            },
        )
