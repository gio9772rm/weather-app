"""Measure second-station forecast errors without touching Rome's calibration."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime

import numpy as np
import pandas as pd
from sqlalchemy import text

from data_access import load_station
from db import get_engine

log = logging.getLogger(__name__)


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
    # Persistence is what was already measured at issuance, never a later value.
    baseline = observations.rename(
        columns={c: c + "_baseline" for c in observations.columns if c != "time"}
    )
    forecast = pd.merge_asof(
        forecast.sort_values("issued_at"),
        baseline,
        left_on="issued_at",
        right_on="time",
        direction="backward",
        tolerance=pd.Timedelta(minutes=20),
    ).drop(columns="time")
    forecast = forecast.sort_values("valid_time")
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
            baseline_column = variable + "_baseline"
            available = columns + (
                [baseline_column] if baseline_column in group else []
            )
            pair = (
                group[available]
                .apply(pd.to_numeric, errors="coerce")
                .dropna(subset=columns)
            )
            if len(pair) < 12:
                continue
            error = pair.iloc[:, 0] - pair.iloc[:, 1]
            holdout = error.iloc[int(len(error) * 0.8) :]
            training = error.iloc[: int(len(error) * 0.8)]
            learned_bias = float(training.mean())
            corrected = float((holdout - learned_bias).abs().mean())
            baseline_pair = pair.iloc[int(len(error) * 0.8) :].dropna()
            persistence = (
                float(
                    (baseline_pair[baseline_column] - baseline_pair[columns[1]])
                    .abs()
                    .mean()
                )
                if baseline_column in baseline_pair and not baseline_pair.empty
                else None
            )
            days = group.loc[pair.index, "valid_time"].dt.date.nunique()
            skill = (
                1 - float(holdout.abs().mean()) / persistence
                if persistence and persistence > 0
                else None
            )
            eligible = (
                days >= 30
                and len(holdout) >= 30
                and persistence is not None
                and corrected < min(float(holdout.abs().mean()), persistence) * 0.9
            )
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
                    "skill_vs_persistence": skill,
                    "persistence_mae": persistence,
                    "training_bias": learned_bias,
                    "candidate_holdout_mae": corrected,
                    "validation_days": int(days),
                    "calibration_status": "candidate_for_review"
                    if eligible
                    else "collecting",
                    "calibration_applied": False,
                }
            )
    return result


def load_secondary_verification_runs(
    station_id: str, now: pd.Timestamp
) -> pd.DataFrame:
    """Select the same independent samples before materialising a DataFrame.

    Preserve the latest 720 runs in the last 30 days, including leads beyond
    72 hours. Only the earliest emission for each target and horizon is used
    by verification_scores; retaining full, repeated runs wastes hundreds of
    MiB on a 512 MiB cron worker. Stream one JSON payload at a time instead.
    """
    now = pd.to_datetime(now, utc=True)
    stop = now.to_pydatetime()
    selected = {}
    count = 0

    def stamp(value):
        try:
            result = (
                value
                if isinstance(value, datetime)
                else datetime.fromisoformat(str(value))
            )
            return result.replace(tzinfo=UTC) if result.tzinfo is None else result
        except (ValueError, TypeError):
            return None

    with get_engine().connect().execution_options(yield_per=1) as con:
        payloads = con.execute(
            text(
                "SELECT payload FROM location_forecasts WHERE station_id=:id AND issued_at>=:cutoff ORDER BY issued_at DESC LIMIT 720"
            ),
            {"id": station_id, "cutoff": (now - pd.Timedelta(days=30)).isoformat()},
        ).scalars()
        for payload in payloads:
            count += 1
            for row in json.loads(payload):
                target, issued = (
                    stamp(row.get("valid_time")),
                    stamp(row.get("issued_at")),
                )
                if target is None or issued is None or target >= stop:
                    continue
                lead = (target - issued).total_seconds() / 3600
                if lead < 1:
                    continue
                horizon = (
                    0 if lead <= 6 else 1 if lead <= 24 else 2 if lead <= 72 else 3
                )
                key = (target, horizon)
                previous = selected.get(key)
                if previous is not None and previous["issued_at"] <= issued:
                    continue
                selected[key] = {
                    "valid_time": target,
                    "issued_at": issued,
                    **{
                        field: row[field]
                        for field in ("temp_c", "humidity", "pressure_hpa", "wind_kmh")
                        if field in row
                    },
                }
    log.info(
        "Verifica secondaria: %s emissioni lette, %s campioni indipendenti",
        count,
        len(selected),
    )
    return pd.DataFrame(selected.values())


def update_secondary_scores(station_id: str) -> None:
    now = pd.Timestamp.now(tz="UTC")
    forecast = load_secondary_verification_runs(station_id, now)
    scores = verification_scores(forecast, load_station(24 * 31, station_id), now)
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
