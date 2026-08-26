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
from forecast_quality import enforce_physical_bounds

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
    "dewpoint_c": 0.8,
    "humidity": 3.0,
    "pressure_hpa": 1.0,
    "wind_kmh": 2.0,
    "wind_gust_kmh": 3.0,
    "wind_dir": 15.0,
    "rain_mm": 0.3,
    "precip_probability": 10.0,
    "clouds": 10.0,
    "visibility_m": 1500.0,
}

LOCAL_OBSERVATION_MAPPINGS = {
    "temp_c": ("temp_c_fc", "temp_c_obs"),
    "dewpoint_c": ("dewpoint_c_fc", "dewpoint_c_obs"),
    "humidity": ("humidity_fc", "humidity_obs"),
    "pressure_hpa": ("pressure_hpa_fc", "pressure_hpa_obs"),
    "wind_kmh": ("wind_kmh_fc", "wind_kmh_obs"),
    "wind_gust_kmh": ("wind_gust_kmh_fc", "wind_gust_kmh_obs"),
    "wind_dir": ("wind_dir_fc", "wind_dir_obs"),
    "rain_mm": ("rain_mm_fc", "rain_mm_obs"),
}

REFERENCE_OBSERVATION_VARIABLES = (
    "temp_c",
    "dewpoint_c",
    "humidity",
    "pressure_hpa",
    "wind_kmh",
    "wind_gust_kmh",
    "wind_dir",
    "rain_mm",
    "clouds",
    "visibility_m",
)


def _enabled_reference_frame(frame: pd.DataFrame, cfg: Settings) -> pd.DataFrame:
    """Exclude disabled sources, including scores left by an earlier activation."""
    if frame.empty or not cfg.official_observations_enabled or "source" not in frame:
        return frame.iloc[0:0].copy()
    enabled_sources = {"awc_metar"}
    if cfg.arsial_observations_enabled:
        enabled_sources.add("arsial_siarl")
    if cfg.cfr_observations_enabled:
        enabled_sources.add("cfr_lazio")
    return frame[frame["source"].isin(enabled_sources)].copy()


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


def _dewpoint_from_temperature_humidity(
    temperature: pd.Series, humidity: pd.Series
) -> pd.Series:
    temp = pd.to_numeric(temperature, errors="coerce")
    rh = pd.to_numeric(humidity, errors="coerce").clip(1, 100)
    gamma = np.log(rh / 100.0) + (17.625 * temp) / (243.04 + temp)
    return 243.04 * gamma / (17.625 - gamma)


def _variable_error(
    forecast: pd.Series, observed: pd.Series, variable: str
) -> pd.Series:
    error = forecast - observed
    if variable == "wind_dir":
        error = (error + 180.0).mod(360.0) - 180.0
    return error


def _circular_mean_degrees(values: pd.Series) -> float:
    radians = np.deg2rad(pd.to_numeric(values, errors="coerce").dropna())
    if len(radians) == 0:
        return 0.0
    return float(np.rad2deg(np.arctan2(np.sin(radians).mean(), np.cos(radians).mean())))


def _score_record(
    pair: pd.DataFrame,
    forecast_col: str,
    observed_col: str,
    variable: str,
) -> dict[str, float | int | None]:
    error = _variable_error(pair[forecast_col], pair[observed_col], variable)
    return {
        "n": len(pair),
        "bias": (
            _circular_mean_degrees(error)
            if variable == "wind_dir"
            else float(error.mean())
        ),
        "mae": float(error.abs().mean()),
        "rmse": float(np.sqrt(np.mean(np.square(error)))),
        "brier": None,
    }


def _holdout_pair(pair: pd.DataFrame) -> pd.DataFrame:
    """Return a recent, untouched validation tail for walk-forward reporting."""
    if len(pair) < 12:
        return pair.iloc[0:0]
    size = max(6, math.ceil(len(pair) * 0.20))
    return pair.tail(size)


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
    observations["dewpoint_c"] = _dewpoint_from_temperature_humidity(
        observations.get("temp_c"), observations.get("humidity")
    )
    observations = observations.rename(
        columns={"windgust_kmh": "wind_gust_kmh", "winddir": "wind_dir"}
    )
    forecasts = forecasts.dropna(subset=["valid_time", "issued_at"])
    observations = observations.dropna(subset=["time"]).sort_values("time")
    persistence_columns = sorted(
        {
            observed.removesuffix("_obs")
            for _, observed in LOCAL_OBSERVATION_MAPPINGS.values()
            if observed.removesuffix("_obs") in observations
        }
    )
    persistence = observations[["time", *persistence_columns]].rename(
        columns={column: f"{column}_persistence" for column in persistence_columns}
    )
    forecasts = pd.merge_asof(
        forecasts.sort_values("issued_at"),
        persistence,
        left_on="issued_at",
        right_on="time",
        direction="backward",
        tolerance=pd.Timedelta(hours=3),
    ).drop(columns=["time"], errors="ignore")
    forecasts = forecasts.sort_values("valid_time")
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
    if "rain_mm_fc" in merged and "interval_hours" in merged:
        interval = pd.to_numeric(merged["interval_hours"], errors="coerce").clip(
            lower=1
        )
        merged["rain_mm_fc"] = (
            pd.to_numeric(merged["rain_mm_fc"], errors="coerce") / interval
        )
    if "rain_mm" in observations and "rain_mm_obs" in merged:
        rain = observations[["time", "rain_mm"]].copy()
        rain["rain_mm"] = pd.to_numeric(rain["rain_mm"], errors="coerce").clip(
            lower=0
        )
        hourly_rain = rain.set_index("time")["rain_mm"].resample("h").sum(min_count=1)
        merged["rain_mm_obs"] = merged["valid_time"].dt.floor("h").map(hourly_rain)
    merged["lead_hours"] = (
        merged["valid_time"] - merged["issued_at"]
    ).dt.total_seconds() / 3600.0
    merged = merged[merged["lead_hours"] >= 0].copy()
    merged["horizon"] = _horizon_bucket(merged["lead_hours"])
    evaluated = pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%dT%H:%M:%SZ")
    rows: list[dict[str, Any]] = []
    reliability_rows: list[dict[str, Any]] = []
    for (provider, model, horizon), group in merged.groupby(
        ["provider", "model", "horizon"], dropna=True
    ):
        if not horizon:
            continue
        for variable, (forecast_col, observed_col) in LOCAL_OBSERVATION_MAPPINGS.items():
            if forecast_col not in group or observed_col not in group:
                continue
            persistence_col = f"{observed_col.removesuffix('_obs')}_persistence"
            pair_columns = [forecast_col, observed_col]
            if persistence_col in group:
                pair_columns.append(persistence_col)
            pair = (
                group.sort_values("valid_time")[pair_columns]
                .apply(pd.to_numeric, errors="coerce")
                .dropna(subset=[forecast_col, observed_col])
            )
            if pair.empty:
                continue
            holdout = _holdout_pair(pair)
            record = {
                "evaluated_at": evaluated,
                "provider": provider,
                "model": model,
                "variable": variable,
                "horizon": str(horizon),
                **_score_record(pair, forecast_col, observed_col, variable),
                "holdout_n": len(holdout),
                "holdout_mae": None,
                "persistence_mae": None,
                "skill_vs_persistence": None,
                "reliability_gap": None,
            }
            if not holdout.empty:
                holdout_error = _variable_error(
                    holdout[forecast_col], holdout[observed_col], variable
                )
                record["holdout_mae"] = float(holdout_error.abs().mean())
                if persistence_col in holdout:
                    persistence_pair = holdout.dropna(subset=[persistence_col])
                    if not persistence_pair.empty:
                        persistence_error = _variable_error(
                            persistence_pair[persistence_col],
                            persistence_pair[observed_col],
                            variable,
                        )
                        persistence_mae = float(persistence_error.abs().mean())
                        record["persistence_mae"] = persistence_mae
                        if persistence_mae > 0:
                            record["skill_vs_persistence"] = float(
                                1.0 - record["holdout_mae"] / persistence_mae
                            )
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
                if not holdout.empty:
                    holdout_probability = (
                        pd.to_numeric(
                            group.loc[holdout.index, "precip_probability"],
                            errors="coerce",
                        ).clip(0, 100)
                        / 100.0
                    )
                    holdout_event = (holdout[observed_col] >= 0.1).astype(float)
                    valid_holdout = holdout_probability.notna()
                    if valid_holdout.any():
                        calibrated = pd.DataFrame(
                            {
                                "probability": holdout_probability[valid_holdout],
                                "event": holdout_event[valid_holdout],
                            }
                        )
                        record["reliability_gap"] = float(
                            calibrated["probability"].mean()
                            - calibrated["event"].mean()
                        )
                        calibrated["probability_bin"] = (
                            np.floor(calibrated["probability"] * 10.0)
                            .clip(0, 9)
                            .astype(int)
                            * 10
                        )
                        for probability_bin, calibration in calibrated.groupby(
                            "probability_bin"
                        ):
                            reliability_rows.append(
                                {
                                    "evaluated_at": evaluated,
                                    "provider": provider,
                                    "model": model,
                                    "horizon": str(horizon),
                                    "probability_bin": int(probability_bin),
                                    "n": len(calibration),
                                    "mean_probability": float(
                                        calibration["probability"].mean()
                                    ),
                                    "observed_frequency": float(
                                        calibration["event"].mean()
                                    ),
                                    "brier": float(
                                        np.mean(
                                            np.square(
                                                calibration["probability"]
                                                - calibration["event"]
                                            )
                                        )
                                    ),
                                }
                            )
            rows.append(record)
    scores = pd.DataFrame(rows)
    if scores.empty:
        return scores
    insert = text(
        "INSERT INTO forecast_scores "
        "(evaluated_at,provider,model,variable,horizon,n,bias,mae,rmse,brier,"
        "holdout_n,holdout_mae,persistence_mae,skill_vs_persistence,reliability_gap) "
        "VALUES (:evaluated_at,:provider,:model,:variable,:horizon,:n,:bias,:mae,:rmse,:brier,"
        ":holdout_n,:holdout_mae,:persistence_mae,:skill_vs_persistence,:reliability_gap) "
        "ON CONFLICT (evaluated_at,provider,model,variable,horizon) DO UPDATE SET "
        "n=excluded.n,bias=excluded.bias,mae=excluded.mae,rmse=excluded.rmse,"
        "brier=excluded.brier,holdout_n=excluded.holdout_n,"
        "holdout_mae=excluded.holdout_mae,persistence_mae=excluded.persistence_mae,"
        "skill_vs_persistence=excluded.skill_vs_persistence,"
        "reliability_gap=excluded.reliability_gap"
    )
    with engine.begin() as connection:
        connection.execute(insert, scores.to_dict("records"))
        if reliability_rows:
            connection.execute(
                text(
                    "INSERT INTO forecast_reliability (evaluated_at,provider,model,"
                    "horizon,probability_bin,n,mean_probability,observed_frequency,brier) "
                    "VALUES (:evaluated_at,:provider,:model,:horizon,:probability_bin,"
                    ":n,:mean_probability,:observed_frequency,:brier) "
                    "ON CONFLICT (evaluated_at,provider,model,horizon,probability_bin) "
                    "DO UPDATE SET n=excluded.n,"
                    "mean_probability=excluded.mean_probability,"
                    "observed_frequency=excluded.observed_frequency,brier=excluded.brier"
                ),
                reliability_rows,
            )
    return scores


def _reference_transfer(
    reference: pd.DataFrame,
    local: pd.DataFrame,
    variable: str,
    minimum_samples: int,
) -> tuple[float, float, int, float] | None:
    """Learn how a remote official sensor maps to the local Ecowitt sensor."""
    if variable not in reference or variable not in local:
        return None
    ref = reference[["time", variable]].rename(columns={variable: "reference"})
    station = local[["time", variable]].rename(columns={variable: "local"})
    matched = pd.merge_asof(
        ref.dropna().sort_values("time"),
        station.dropna().sort_values("time"),
        on="time",
        direction="nearest",
        tolerance=pd.Timedelta(minutes=40),
    ).dropna()
    if len(matched) < minimum_samples:
        return None
    difference = _variable_error(
        matched["reference"], matched["local"], variable
    )
    transfer_bias = (
        _circular_mean_degrees(difference)
        if variable == "wind_dir"
        else float(difference.median())
    )
    residual = difference - transfer_bias
    if variable == "wind_dir":
        residual = (residual + 180.0).mod(360.0) - 180.0
        radians = np.deg2rad(difference)
        correlation = float(
            np.hypot(np.cos(radians).mean(), np.sin(radians).mean())
        )
    else:
        correlation = float(matched["reference"].corr(matched["local"]))
        if not math.isfinite(correlation):
            correlation = 0.0
    return transfer_bias, float(residual.abs().median()), len(matched), correlation


def _adjust_reference(series: pd.Series, variable: str, bias: float) -> pd.Series:
    adjusted = series - bias
    return adjusted.mod(360.0) if variable == "wind_dir" else adjusted


def score_forecasts_against_references(
    cfg: Settings = settings, engine: Engine | None = None
) -> pd.DataFrame:
    """Score providers against official sensors after mapping them to Ecowitt.

    Temperature, humidity, pressure and wind are admitted only after enough
    simultaneous Ecowitt samples establish the persistent site difference.
    Cloud, visibility and precipitation occurrence have no local equivalent and
    therefore enter with a deliberately smaller distance-based weight.
    """
    if not cfg.official_observations_enabled:
        return pd.DataFrame()
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
        references = pd.read_sql(
            text(
                "SELECT * FROM official_observations WHERE time >= :cutoff "
                "ORDER BY source,station_id,time"
            ),
            connection,
            params={"cutoff": cutoff},
        )
        local = pd.read_sql(
            text("SELECT * FROM station_raw WHERE time >= :cutoff ORDER BY time"),
            connection,
            params={"cutoff": cutoff},
        )
    if forecasts.empty or references.empty:
        return pd.DataFrame()

    for frame in (forecasts, references, local):
        frame.columns = [column.lower() for column in frame.columns]
    references = _enabled_reference_frame(references, cfg)
    if references.empty:
        return pd.DataFrame()
    forecasts["valid_time"] = pd.to_datetime(
        forecasts["valid_time"], utc=True, errors="coerce"
    )
    forecasts["issued_at"] = pd.to_datetime(
        forecasts["issued_at"], utc=True, errors="coerce"
    )
    references["time"] = pd.to_datetime(
        references["time"], utc=True, errors="coerce"
    )
    forecasts = forecasts.dropna(subset=["valid_time", "issued_at"]).sort_values(
        "valid_time"
    )
    references = references.dropna(subset=["time"]).sort_values("time")
    if not local.empty:
        local["time"] = pd.to_datetime(local["time"], utc=True, errors="coerce")
        local = local.rename(
            columns={"windgust_kmh": "wind_gust_kmh", "winddir": "wind_dir"}
        )
        local["dewpoint_c"] = _dewpoint_from_temperature_humidity(
            local.get("temp_c"), local.get("humidity")
        )
        local = local.dropna(subset=["time"]).sort_values("time")

    evaluated = pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%dT%H:%M:%SZ")
    rows: list[dict[str, Any]] = []
    for (source, station_id), station_reference in references.groupby(
        ["source", "station_id"]
    ):
        station_reference = station_reference.sort_values("time")
        distance = pd.to_numeric(
            station_reference.get("distance_km"), errors="coerce"
        ).median()
        distance = float(distance) if pd.notna(distance) else 100.0
        distance_weight = 1.0 / (1.0 + (max(0.0, distance) / 20.0) ** 2)
        merged = pd.merge_asof(
            forecasts,
            station_reference,
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

        transfers: dict[str, tuple[float, float, int, float] | None] = {}
        for variable in REFERENCE_OBSERVATION_VARIABLES:
            transfers[variable] = (
                _reference_transfer(
                    station_reference,
                    local,
                    variable,
                    cfg.official_min_overlap_samples,
                )
                if not local.empty and variable not in {"clouds", "visibility_m", "rain_mm"}
                else None
            )

        for (provider, model, horizon), group in merged.groupby(
            ["provider", "model", "horizon"], dropna=True
        ):
            if not horizon:
                continue
            for variable in REFERENCE_OBSERVATION_VARIABLES:
                forecast_col = f"{variable}_fc"
                observed_col = f"{variable}_obs"
                if forecast_col not in group or observed_col not in group:
                    continue
                transfer = transfers[variable]
                if variable not in {"clouds", "visibility_m", "rain_mm"} and transfer is None:
                    continue
                pair = group[[forecast_col, observed_col]].apply(
                    pd.to_numeric, errors="coerce"
                ).dropna()
                if len(pair) < 6:
                    continue
                transfer_bias, transfer_mae, site_correlation = (0.0, None, None)
                if transfer is not None:
                    transfer_bias, transfer_mae, _, site_correlation = transfer
                    pair[observed_col] = _adjust_reference(
                        pair[observed_col], variable, transfer_bias
                    )
                    residual_weight = min(
                        1.0,
                        ERROR_FLOORS.get(variable, 1.0)
                        / max(transfer_mae, ERROR_FLOORS.get(variable, 1.0)),
                    )
                    correlation_weight = float(
                        np.clip((site_correlation - 0.10) / 0.70, 0.0, 1.0)
                    )
                    quality_weight = residual_weight * correlation_weight
                else:
                    quality_weight = 0.20 if variable != "rain_mm" else 0.10
                record = {
                    "evaluated_at": evaluated,
                    "provider": provider,
                    "model": model,
                    "source": source,
                    "station_id": station_id,
                    "variable": variable,
                    "horizon": str(horizon),
                    **_score_record(pair, forecast_col, observed_col, variable),
                    "transfer_bias": transfer_bias,
                    "transfer_mae": transfer_mae,
                    "site_correlation": site_correlation,
                    "reference_weight": distance_weight * quality_weight,
                }
                rows.append(record)

            probability_col = "precip_probability"
            event_col = "precip_observed"
            if probability_col in group and event_col in group:
                pair = group[[probability_col, event_col]].apply(
                    pd.to_numeric, errors="coerce"
                ).dropna()
                if len(pair) >= 6:
                    probability = pair[probability_col].clip(0, 100) / 100.0
                    event = pair[event_col].clip(0, 1)
                    percentage_error = probability * 100.0 - event * 100.0
                    rows.append(
                        {
                            "evaluated_at": evaluated,
                            "provider": provider,
                            "model": model,
                            "source": source,
                            "station_id": station_id,
                            "variable": "precip_probability",
                            "horizon": str(horizon),
                            "n": len(pair),
                            "bias": float(percentage_error.mean()),
                            "mae": float(percentage_error.abs().mean()),
                            "rmse": float(np.sqrt(np.mean(np.square(percentage_error)))),
                            "brier": float(np.mean(np.square(probability - event))),
                            "transfer_bias": None,
                            "transfer_mae": None,
                            "site_correlation": None,
                            "reference_weight": distance_weight * 0.15,
                        }
                    )

    scores = pd.DataFrame(rows)
    if scores.empty:
        return scores
    columns = [
        "evaluated_at",
        "provider",
        "model",
        "source",
        "station_id",
        "variable",
        "horizon",
        "n",
        "bias",
        "mae",
        "rmse",
        "brier",
        "transfer_bias",
        "transfer_mae",
        "site_correlation",
        "reference_weight",
    ]
    insert = text(
        "INSERT INTO forecast_reference_scores ("
        + ",".join(columns)
        + ") VALUES ("
        + ",".join(f":{column}" for column in columns)
        + ") ON CONFLICT (evaluated_at,provider,model,source,station_id,variable,horizon) "
        "DO UPDATE SET n=excluded.n,bias=excluded.bias,mae=excluded.mae,"
        "rmse=excluded.rmse,brier=excluded.brier,transfer_bias=excluded.transfer_bias,"
        "transfer_mae=excluded.transfer_mae,site_correlation=excluded.site_correlation,"
        "reference_weight=excluded.reference_weight"
    )
    with engine.begin() as connection:
        connection.execute(insert, scores.reindex(columns=columns).to_dict("records"))
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
        for column in (
            "n",
            "bias",
            "mae",
            "rmse",
            "brier",
            "holdout_n",
            "holdout_mae",
            "persistence_mae",
            "skill_vs_persistence",
            "reliability_gap",
        ):
            if column in frame:
                frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame


def _latest_reference_scores(engine: Engine) -> pd.DataFrame:
    with engine.connect() as connection:
        frame = pd.read_sql(
            text(
                "SELECT * FROM forecast_reference_scores WHERE evaluated_at = "
                "(SELECT MAX(evaluated_at) FROM forecast_reference_scores)"
            ),
            connection,
        )
    if not frame.empty:
        frame.columns = [column.lower() for column in frame.columns]
        for column in (
            "n",
            "bias",
            "mae",
            "rmse",
            "brier",
            "transfer_bias",
            "transfer_mae",
            "site_correlation",
            "reference_weight",
        ):
            if column in frame:
                frame[column] = pd.to_numeric(frame[column], errors="coerce")
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


def _latest_ensemble(engine: Engine) -> pd.DataFrame:
    """Load the newest probabilistic emission without treating members as providers."""
    query = text(
        "SELECT * FROM forecast_ensemble_runs WHERE issued_at = "
        "(SELECT MAX(issued_at) FROM forecast_ensemble_runs) ORDER BY valid_time,variable"
    )
    with engine.connect() as connection:
        frame = pd.read_sql(query, connection)
    if frame.empty:
        return frame
    frame.columns = [column.lower() for column in frame.columns]
    for column in ("issued_at", "valid_time", "fetched_at"):
        frame[column] = pd.to_datetime(frame[column], utc=True, errors="coerce")
    for column in (
        "p10",
        "p25",
        "p50",
        "p75",
        "p90",
        "mean",
        "member_count",
        "event_probability",
    ):
        frame[column] = pd.to_numeric(frame.get(column), errors="coerce")
    newest = frame["issued_at"].max()
    if pd.isna(newest) or pd.Timestamp.now(tz="UTC") - newest > pd.Timedelta(hours=8):
        return frame.iloc[0:0]
    return frame.dropna(subset=["valid_time"])


def _score_lookup(
    scores: pd.DataFrame,
    provider: str,
    variable: str,
    horizon: str,
    reference_scores: pd.DataFrame | None = None,
    official_max_share: float = 0.20,
) -> tuple[float, float | None]:
    local_bias = 0.0
    local_mae: float | None = None
    if not scores.empty:
        subset = scores[
            (scores["provider"] == provider)
            & (scores["variable"] == variable)
            & (scores["horizon"] == horizon)
        ]
        if not subset.empty:
            row = subset.sort_values("n", ascending=False).iloc[0]
            local_bias = float(row["bias"]) if pd.notna(row["bias"]) else 0.0
            validated = (
                "holdout_mae" in row
                and pd.notna(row.get("holdout_mae"))
                and float(row.get("holdout_n") or 0) >= 6
            )
            error_value = row["holdout_mae"] if validated else row["mae"]
            local_mae = float(error_value) if pd.notna(error_value) else None

    if reference_scores is None or reference_scores.empty:
        return local_bias, local_mae
    external = reference_scores[
        (reference_scores["provider"] == provider)
        & (reference_scores["variable"] == variable)
        & (reference_scores["horizon"] == horizon)
        & (reference_scores["n"] >= 6)
    ].dropna(subset=["mae", "reference_weight"])
    if external.empty:
        return local_bias, local_mae
    evidence = (
        external["reference_weight"].clip(lower=0)
        * np.sqrt(external["n"].clip(lower=1))
    )
    total_evidence = float(evidence.sum())
    if total_evidence <= 0:
        return local_bias, local_mae
    external_bias = float(
        np.average(external["bias"].fillna(0.0), weights=evidence)
    )
    external_mae = float(np.average(external["mae"], weights=evidence))
    if local_mae is None:
        # The secondary network can initialise a parameter only after enough
        # evidence has accumulated; it never creates a local observation.
        if int(external["n"].sum()) < 24:
            return 0.0, None
        return external_bias, external_mae
    maximum = max(0.0, official_max_share)
    share = min(maximum, maximum * min(1.0, total_evidence / 12.0))
    return (
        local_bias * (1.0 - share) + external_bias * share,
        local_mae * (1.0 - share) + external_mae * share,
    )


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


def build_blend(
    engine: Engine | None = None, cfg: Settings | None = None
) -> pd.DataFrame:
    """Create an hourly bias-corrected ensemble and replace the derived cache."""
    engine = engine or get_engine()
    cfg = cfg or Settings.from_env()
    forecasts = _latest_provider_runs(engine)
    if forecasts.empty:
        return pd.DataFrame()
    scores = _latest_scores(engine)
    ensemble = _latest_ensemble(engine) if cfg.ensemble_forecast_enabled else pd.DataFrame()
    reference_scores = _enabled_reference_frame(_latest_reference_scores(engine), cfg)
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
        "wind_dir",
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
                _, mae = _score_lookup(
                    scores,
                    provider,
                    "temp_c",
                    str(horizon),
                    reference_scores,
                    cfg.official_score_max_share,
                )
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
            vector_x = 0.0
            vector_y = 0.0
            for provider in providers:
                if variable not in hourly[provider]:
                    continue
                value = hourly[provider].iloc[position].get(variable)
                if pd.isna(value):
                    continue
                score_variable = variable if variable in ERROR_FLOORS else {
                    "feels_like_c": "temp_c",
                    "snow_mm": "rain_mm",
                    "cloud_low": "clouds",
                    "cloud_mid": "clouds",
                    "cloud_high": "clouds",
                }.get(variable, "temp_c")
                bias, mae = _score_lookup(
                    scores,
                    provider,
                    score_variable,
                    str(horizon),
                    reference_scores,
                    cfg.official_score_max_share,
                )
                corrected = float(value) - (
                    bias if score_variable == variable else 0.0
                )
                if variable == "wind_dir":
                    corrected %= 360.0
                prior = PRIOR_WEIGHTS.get(provider, 0.35)
                weight = prior / max(
                    mae or ERROR_FLOORS.get(score_variable, 1.0),
                    ERROR_FLOORS.get(score_variable, 0.5),
                )
                if variable == "wind_dir":
                    radians = math.radians(corrected)
                    vector_x += math.cos(radians) * weight
                    vector_y += math.sin(radians) * weight
                else:
                    numerator += corrected * weight
                denominator += weight
            if not denominator:
                values.append(np.nan)
            elif variable == "wind_dir":
                values.append(math.degrees(math.atan2(vector_y, vector_x)) % 360.0)
            else:
                values.append(numerator / denominator)
        result[variable] = values

    result = _station_correction(result, engine)
    ensemble_used = False
    if not ensemble.empty:
        temperature_ensemble = ensemble[ensemble["variable"] == "temp_c"].set_index(
            "valid_time"
        )
        if not temperature_ensemble.empty:
            probabilistic_spread = (
                pd.to_numeric(temperature_ensemble["p90"], errors="coerce")
                - pd.to_numeric(temperature_ensemble["p10"], errors="coerce")
            ).clip(lower=0) / 2.0
            aligned_spread = probabilistic_spread.reindex(timeline).to_numpy()
            temp_spreads = np.fmax(
                np.asarray(temp_spreads, dtype=float),
                np.nan_to_num(aligned_spread, nan=0.0),
            ).tolist()
            ensemble_used = True
        rain_ensemble = ensemble[ensemble["variable"] == "rain_mm"].set_index(
            "valid_time"
        )
        if not rain_ensemble.empty:
            ensemble_probability = pd.to_numeric(
                rain_ensemble["event_probability"], errors="coerce"
            ).reindex(timeline)
            deterministic_probability = pd.to_numeric(
                result["precip_probability"], errors="coerce"
            )
            has_ensemble = ensemble_probability.notna().to_numpy()
            result.loc[has_ensemble, "precip_probability"] = (
                deterministic_probability[has_ensemble].fillna(
                    ensemble_probability[has_ensemble]
                )
                * 0.80
                + ensemble_probability[has_ensemble] * 0.20
            )
            ensemble_used = True
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
    result["method"] = (
        "inverse_mae+bias+ecowitt_primary+official_reference_v2"
        + ("+ensemble_guidance_v1" if ensemble_used else "")
    )
    result["issued_at"] = issued_at
    result = enforce_physical_bounds(result)
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
        # Rebuilding the same hourly emission is idempotent.  A new emission is
        # appended and becomes the basis for the user-facing change summary.
        connection.execute(
            text("DELETE FROM forecast_blend_history WHERE issued_at=:issued_at"),
            {"issued_at": records[0]["issued_at"]},
        )
        connection.execute(
            text(
                "INSERT INTO forecast_blend_history ("
                + ",".join(db_columns)
                + ") VALUES ("
                + placeholders
                + ")"
            ),
            records,
        )
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
