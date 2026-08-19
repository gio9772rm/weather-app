"""Archive, verify and locally calibrate forecasts from multiple providers."""

from __future__ import annotations

import json
import math
from typing import Any

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from config import Settings, settings
from db import get_engine
from forecast_providers import FORECAST_COLUMNS

NUMERIC_COLUMNS = [
    "temp_c",
    "feels_like_c",
    "humidity",
    "dewpoint_c",
    "pressure_hpa",
    "wind_kmh",
    "wind_gust_kmh",
    "wind_dir",
    "rain_mm",
    "snow_mm",
    "precip_probability",
    "clouds",
    "cloud_low",
    "cloud_mid",
    "cloud_high",
    "visibility_m",
    "is_day",
]

PRIOR_WEIGHTS = {"open_meteo": 0.60, "openweather": 0.40}
ERROR_FLOORS = {
    "temp_c": 0.5,
    "humidity": 3.0,
    "pressure_hpa": 1.0,
    "wind_kmh": 2.0,
    "rain_mm": 0.3,
}


def _iso(value: Any) -> str | None:
    timestamp = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(timestamp):
        return None
    return timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")


def _native(value: Any) -> Any:
    if value is None or value is pd.NA:
        return None
    if isinstance(value, (np.floating, float)) and (
        math.isnan(float(value)) or math.isinf(float(value))
    ):
        return None
    if isinstance(value, np.generic):
        return value.item()
    return value


def archive_forecast(frame: pd.DataFrame, engine: Engine | None = None) -> int:
    if frame is None or frame.empty:
        return 0
    engine = engine or get_engine()
    data = frame.reindex(columns=FORECAST_COLUMNS).copy()
    records: list[dict[str, Any]] = []
    for row in data.to_dict("records"):
        row["issued_at"] = _iso(row.get("issued_at"))
        row["valid_time"] = _iso(row.get("valid_time"))
        row["fetched_at"] = _iso(row.get("fetched_at"))
        clean = {key: _native(value) for key, value in row.items()}
        if clean["issued_at"] and clean["valid_time"] and clean["fetched_at"]:
            clean["provider"] = str(clean.get("provider") or "unknown")
            clean["model"] = str(clean.get("model") or "default")
            records.append(clean)
    if not records:
        return 0
    columns = FORECAST_COLUMNS
    insert = text(
        "INSERT INTO forecast_runs ("
        + ",".join(columns)
        + ") VALUES ("
        + ",".join(f":{column}" for column in columns)
        + ") "
        + "ON CONFLICT (provider, model, issued_at, valid_time) DO UPDATE SET "
        + ",".join(
            f"{column}=excluded.{column}"
            for column in columns
            if column not in {"provider", "model", "issued_at", "valid_time"}
        )
    )
    with engine.begin() as connection:
        for start in range(0, len(records), 1000):
            connection.execute(insert, records[start : start + 1000])
    return len(records)


def _horizon_bucket(hours: pd.Series) -> pd.Series:
    return pd.cut(
        hours,
        bins=[-0.001, 24, 72, float("inf")],
        labels=["0-24h", "24-72h", "72h+"],
        include_lowest=True,
    ).astype("string")


def score_forecasts(
    cfg: Settings = settings, engine: Engine | None = None
) -> pd.DataFrame:
    """Compare archived forecasts with station observations and persist errors."""
    engine = engine or get_engine()
    cutoff = (
        pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=cfg.score_lookback_days)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")
    now_iso = pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%dT%H:%M:%SZ")
    with engine.connect() as connection:
        forecasts = pd.read_sql(
            text(
                "SELECT * FROM forecast_runs WHERE valid_time >= :cutoff "
                "AND valid_time <= :now ORDER BY valid_time"
            ),
            connection,
            params={"cutoff": cutoff, "now": now_iso},
        )
        observations = pd.read_sql(
            text(
                "SELECT * FROM station_raw WHERE time >= :cutoff "
                "AND source IS NOT NULL ORDER BY time"
            ),
            connection,
            params={"cutoff": cutoff},
        )
    if forecasts.empty or observations.empty:
        return pd.DataFrame()

    forecasts.columns = [column.lower() for column in forecasts.columns]
    observations.columns = [column.lower() for column in observations.columns]
    forecasts["valid_time"] = pd.to_datetime(
        forecasts["valid_time"], utc=True, errors="coerce"
    )
    forecasts["issued_at"] = pd.to_datetime(
        forecasts["issued_at"], utc=True, errors="coerce"
    )
    observations["time"] = pd.to_datetime(
        observations["time"], utc=True, errors="coerce"
    )
    forecasts = forecasts.dropna(subset=["valid_time", "issued_at"]).sort_values(
        "valid_time"
    )
    observations = observations.dropna(subset=["time"]).sort_values("time")
    # Prefer one observation closest to each forecast target, within 40 minutes.
    merged = pd.merge_asof(
        forecasts,
        observations,
        left_on="valid_time",
        right_on="time",
        direction="nearest",
        tolerance=pd.Timedelta(minutes=40),
        suffixes=("_fc", "_obs"),
    )
    merged["lead_hours"] = (
        merged["valid_time"] - merged["issued_at"]
    ).dt.total_seconds() / 3600.0
    merged = merged[merged["lead_hours"] >= 0].copy()
    merged["horizon"] = _horizon_bucket(merged["lead_hours"])
    mappings = {
        "temp_c": ("temp_c_fc", "temp_c_obs"),
        "humidity": ("humidity_fc", "humidity_obs"),
        "pressure_hpa": ("pressure_hpa_fc", "pressure_hpa_obs"),
        "wind_kmh": ("wind_kmh_fc", "wind_kmh_obs"),
        "rain_mm": ("rain_mm_fc", "rain_mm_obs"),
    }
    evaluated = pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%dT%H:%M:%SZ")
    rows: list[dict[str, Any]] = []
    for (provider, model, horizon), group in merged.groupby(
        ["provider", "model", "horizon"], dropna=True
    ):
        if not horizon:
            continue
        for variable, (forecast_col, observed_col) in mappings.items():
            if forecast_col not in group or observed_col not in group:
                continue
            pair = (
                group[[forecast_col, observed_col]]
                .apply(pd.to_numeric, errors="coerce")
                .dropna()
            )
            if pair.empty:
                continue
            error = pair[forecast_col] - pair[observed_col]
            record = {
                "evaluated_at": evaluated,
                "provider": provider,
                "model": model,
                "variable": variable,
                "horizon": str(horizon),
                "n": len(pair),
                "bias": float(error.mean()),
                "mae": float(error.abs().mean()),
                "rmse": float(np.sqrt(np.mean(np.square(error)))),
                "brier": None,
            }
            if variable == "rain_mm" and "precip_probability" in group:
                probability = (
                    pd.to_numeric(
                        group.loc[pair.index, "precip_probability"], errors="coerce"
                    )
                    / 100.0
                )
                event = (pair[observed_col] >= 0.1).astype(float)
                valid = probability.notna()
                if valid.any():
                    record["brier"] = float(
                        np.mean(np.square(probability[valid] - event[valid]))
                    )
            rows.append(record)
    scores = pd.DataFrame(rows)
    if scores.empty:
        return scores
    insert = text(
        "INSERT INTO forecast_scores "
        "(evaluated_at,provider,model,variable,horizon,n,bias,mae,rmse,brier) "
        "VALUES (:evaluated_at,:provider,:model,:variable,:horizon,:n,:bias,:mae,:rmse,:brier) "
        "ON CONFLICT (evaluated_at,provider,model,variable,horizon) DO UPDATE SET "
        "n=excluded.n,bias=excluded.bias,mae=excluded.mae,rmse=excluded.rmse,brier=excluded.brier"
    )
    with engine.begin() as connection:
        connection.execute(insert, scores.to_dict("records"))
    return scores


def _latest_scores(engine: Engine) -> pd.DataFrame:
    with engine.connect() as connection:
        frame = pd.read_sql(
            text(
                "SELECT * FROM forecast_scores WHERE evaluated_at = "
                "(SELECT MAX(evaluated_at) FROM forecast_scores)"
            ),
            connection,
        )
    if not frame.empty:
        frame.columns = [column.lower() for column in frame.columns]
    return frame


def _latest_provider_runs(engine: Engine) -> pd.DataFrame:
    now = pd.Timestamp.now(tz="UTC").floor("h").strftime("%Y-%m-%dT%H:%M:%SZ")
    query = text(
        "SELECT f.* FROM forecast_runs f JOIN ("
        " SELECT provider, model, MAX(issued_at) AS issued_at FROM forecast_runs"
        " GROUP BY provider, model"
        ") latest ON f.provider=latest.provider AND f.model=latest.model "
        "AND f.issued_at=latest.issued_at WHERE f.valid_time >= :now ORDER BY f.valid_time"
    )
    with engine.connect() as connection:
        frame = pd.read_sql(query, connection, params={"now": now})
    if frame.empty:
        return frame
    frame.columns = [column.lower() for column in frame.columns]
    for column in ("issued_at", "valid_time", "fetched_at"):
        frame[column] = pd.to_datetime(frame[column], utc=True, errors="coerce")
    for column in NUMERIC_COLUMNS + ["interval_hours", "lead_hours"]:
        if column in frame:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame.dropna(subset=["valid_time", "issued_at"])


def _score_lookup(
    scores: pd.DataFrame, provider: str, variable: str, horizon: str
) -> tuple[float, float | None]:
    if scores.empty:
        return 0.0, None
    subset = scores[
        (scores["provider"] == provider)
        & (scores["variable"] == variable)
        & (scores["horizon"] == horizon)
    ]
    if subset.empty:
        return 0.0, None
    row = subset.sort_values("n", ascending=False).iloc[0]
    bias = float(row["bias"]) if pd.notna(row["bias"]) else 0.0
    mae = float(row["mae"]) if pd.notna(row["mae"]) else None
    return bias, mae


def _to_hourly(
    provider_frame: pd.DataFrame, timeline: pd.DatetimeIndex
) -> pd.DataFrame:
    frame = provider_frame.set_index("valid_time").sort_index()
    numeric = [column for column in NUMERIC_COLUMNS if column in frame]
    output = frame[numeric].reindex(frame.index.union(timeline)).sort_index()
    interval = float(
        pd.to_numeric(frame["interval_hours"], errors="coerce").median() or 1.0
    )
    continuous = [
        column for column in numeric if column not in {"rain_mm", "snow_mm", "is_day"}
    ]
    output[continuous] = output[continuous].interpolate(method="time", limit=3)
    for column in ("rain_mm", "snow_mm"):
        if column not in output:
            continue
        if interval > 1:
            # The three-hour amount belongs to the interval ending at valid_time.
            distributed = pd.Series(np.nan, index=timeline, dtype=float)
            span = max(1, round(interval))
            source = pd.to_numeric(frame[column], errors="coerce")
            for valid_time, amount in source.dropna().items():
                for offset in range(span):
                    target = valid_time - pd.Timedelta(hours=span - 1 - offset)
                    if target in distributed.index:
                        current = distributed.loc[target]
                        distributed.loc[target] = (
                            0.0 if pd.isna(current) else current
                        ) + float(amount) / span
            output[column] = distributed.reindex(output.index)
        else:
            output[column] = pd.to_numeric(output[column], errors="coerce").clip(
                lower=0
            )
    if "is_day" in output:
        output["is_day"] = output["is_day"].ffill(limit=3).bfill(limit=3)
    return output.reindex(timeline)


def _station_correction(blend: pd.DataFrame, engine: Engine) -> pd.DataFrame:
    cutoff = (pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=3)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    with engine.connect() as connection:
        observation = (
            connection.execute(
                text(
                    "SELECT time,temp_c,humidity,pressure_hpa,wind_kmh FROM station_raw "
                    "WHERE time >= :cutoff ORDER BY time DESC LIMIT 1"
                ),
                {"cutoff": cutoff},
            )
            .mappings()
            .first()
        )
    if not observation or blend.empty:
        return blend
    observed_at = pd.to_datetime(observation["time"], utc=True, errors="coerce")
    if pd.isna(observed_at):
        return blend
    nearest_position = int(np.argmin(np.abs(blend.index - observed_at)))
    variables = {
        "temp_c": 5.0,
        "humidity": 20.0,
        "pressure_hpa": 8.0,
        "wind_kmh": 15.0,
    }
    for variable, maximum in variables.items():
        observed = pd.to_numeric(
            pd.Series([observation.get(variable)]), errors="coerce"
        ).iloc[0]
        reference = blend.iloc[nearest_position].get(variable)
        if pd.isna(observed) or pd.isna(reference):
            continue
        anomaly = float(np.clip(observed - reference, -maximum, maximum))
        hours = np.maximum((blend.index - observed_at).total_seconds() / 3600.0, 0)
        decay = np.clip(1.0 - hours / 12.0, 0.0, 1.0)
        blend[variable] = blend[variable] + anomaly * decay
    return blend


def build_blend(engine: Engine | None = None) -> pd.DataFrame:
    """Create an hourly bias-corrected ensemble and replace the derived cache."""
    engine = engine or get_engine()
    forecasts = _latest_provider_runs(engine)
    if forecasts.empty:
        return pd.DataFrame()
    scores = _latest_scores(engine)
    interval_coverage = pd.to_timedelta(
        (forecasts["interval_hours"].fillna(1.0) - 1.0).clip(lower=0),
        unit="h",
    )
    start = (forecasts["valid_time"] - interval_coverage).min().floor("h")
    end = forecasts["valid_time"].max().floor("h")
    timeline = pd.date_range(start, end, freq="h", tz="UTC")
    providers = sorted(forecasts["provider"].dropna().unique())
    hourly = {
        provider: _to_hourly(forecasts[forecasts["provider"] == provider], timeline)
        for provider in providers
    }
    result = pd.DataFrame(index=timeline)
    issued_at = forecasts["issued_at"].max()
    lead_hours = (timeline - issued_at).total_seconds() / 3600.0
    horizons = np.where(
        lead_hours <= 24, "0-24h", np.where(lead_hours <= 72, "24-72h", "72h+")
    )

    weighted_variables = [
        "temp_c",
        "feels_like_c",
        "humidity",
        "dewpoint_c",
        "pressure_hpa",
        "wind_kmh",
        "wind_gust_kmh",
        "rain_mm",
        "snow_mm",
        "precip_probability",
        "clouds",
        "cloud_low",
        "cloud_mid",
        "cloud_high",
        "visibility_m",
    ]
    weights_by_time: list[dict[str, float]] = []
    provider_counts: list[int] = []
    temp_spreads: list[float] = []
    for position, (valid_time, horizon) in enumerate(zip(timeline, horizons)):
        time_weights: dict[str, float] = {}
        temp_values: list[float] = []
        for provider in providers:
            value = hourly[provider].iloc[position].get("temp_c")
            if pd.notna(value):
                _, mae = _score_lookup(scores, provider, "temp_c", str(horizon))
                prior = PRIOR_WEIGHTS.get(provider, 0.35)
                time_weights[provider] = prior / max(mae or 1.5, ERROR_FLOORS["temp_c"])
                temp_values.append(float(value))
        total = sum(time_weights.values())
        if total:
            time_weights = {key: value / total for key, value in time_weights.items()}
        weights_by_time.append(time_weights)
        provider_counts.append(len(time_weights))
        temp_spreads.append(float(np.ptp(temp_values)) if len(temp_values) > 1 else 0.0)

    for variable in weighted_variables:
        values: list[float] = []
        for position, horizon in enumerate(horizons):
            numerator = 0.0
            denominator = 0.0
            for provider in providers:
                if variable not in hourly[provider]:
                    continue
                value = hourly[provider].iloc[position].get(variable)
                if pd.isna(value):
                    continue
                score_variable = variable if variable in ERROR_FLOORS else "temp_c"
                bias, mae = _score_lookup(
                    scores, provider, score_variable, str(horizon)
                )
                corrected = float(value) - (bias if variable in ERROR_FLOORS else 0.0)
                prior = PRIOR_WEIGHTS.get(provider, 0.35)
                weight = prior / max(
                    mae or ERROR_FLOORS.get(score_variable, 1.0),
                    ERROR_FLOORS.get(score_variable, 0.5),
                )
                numerator += corrected * weight
                denominator += weight
            values.append(numerator / denominator if denominator else np.nan)
        result[variable] = values

    result = _station_correction(result, engine)
    result["wind_dir"] = np.nan
    result["is_day"] = np.nan
    descriptions: list[str] = []
    codes: list[str] = []
    for position in range(len(timeline)):
        source = "open_meteo" if "open_meteo" in providers else providers[0]
        source_rows = (
            forecasts[forecasts["provider"] == source]
            .set_index("valid_time")
            .sort_index()
        )
        if source_rows.empty:
            descriptions.append("Variabile")
            codes.append("")
            continue
        nearest = source_rows.index.get_indexer([timeline[position]], method="nearest")[
            0
        ]
        row = source_rows.iloc[nearest]
        descriptions.append(str(row.get("description") or "Variabile"))
        codes.append(str(row.get("weather_code") or ""))
        if "wind_dir" in hourly[source]:
            result.iloc[position, result.columns.get_loc("wind_dir")] = (
                hourly[source].iloc[position].get("wind_dir")
            )
        if "is_day" in hourly[source]:
            result.iloc[position, result.columns.get_loc("is_day")] = (
                hourly[source].iloc[position].get("is_day")
            )
    result["description"] = descriptions
    result["weather_code"] = codes
    result["temp_uncertainty_c"] = temp_spreads
    horizon_penalty = np.clip(np.maximum(lead_hours, 0) / 168.0 * 18.0, 0, 18)
    provider_penalty = np.where(np.asarray(provider_counts) >= 2, 0.0, 18.0)
    result["confidence"] = np.clip(
        100.0 - np.asarray(temp_spreads) * 13.0 - horizon_penalty - provider_penalty,
        20.0,
        99.0,
    )
    result["provider_count"] = provider_counts
    result["provider_weights"] = [
        json.dumps(item, sort_keys=True) for item in weights_by_time
    ]
    result["method"] = "inverse_mae+bias+station_decay_v1"
    result["issued_at"] = issued_at
    result = result.reset_index(names="valid_time")

    db_columns = [
        "valid_time",
        "issued_at",
        "temp_c",
        "feels_like_c",
        "humidity",
        "dewpoint_c",
        "pressure_hpa",
        "wind_kmh",
        "wind_gust_kmh",
        "wind_dir",
        "rain_mm",
        "snow_mm",
        "precip_probability",
        "clouds",
        "cloud_low",
        "cloud_mid",
        "cloud_high",
        "visibility_m",
        "weather_code",
        "description",
        "is_day",
        "temp_uncertainty_c",
        "confidence",
        "provider_count",
        "provider_weights",
        "method",
    ]
    records: list[dict[str, Any]] = []
    for row in result.reindex(columns=db_columns).to_dict("records"):
        row["valid_time"] = _iso(row["valid_time"])
        row["issued_at"] = _iso(row["issued_at"])
        records.append({key: _native(value) for key, value in row.items()})
    placeholders = ",".join(f":{column}" for column in db_columns)
    with engine.begin() as connection:
        connection.execute(text("DELETE FROM forecast_blend"))
        connection.execute(
            text(
                "INSERT INTO forecast_blend ("
                + ",".join(db_columns)
                + ") VALUES ("
                + placeholders
                + ")"
            ),
            records,
        )
    return result
