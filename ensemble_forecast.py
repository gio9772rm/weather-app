"""Probabilistic Open-Meteo ensemble guidance, isolated from the main blend."""

from __future__ import annotations

from time import perf_counter
from typing import Any

import numpy as np
import pandas as pd
import requests
from sqlalchemy import text
from sqlalchemy.engine import Engine

from config import Settings, settings
from db import ensure_schema, get_engine
from forecast_providers import build_session
from source_health import record_source_disabled, record_source_result


class EnsembleForecastError(RuntimeError):
    """The optional ensemble feed failed without exposing request internals."""


VARIABLES = {
    "temperature_2m": "temp_c",
    "relative_humidity_2m": "humidity",
    "pressure_msl": "pressure_hpa",
    "precipitation": "rain_mm",
    "wind_speed_10m": "wind_kmh",
    "wind_gusts_10m": "wind_gust_kmh",
}

ENSEMBLE_COLUMNS = [
    "source",
    "model",
    "issued_at",
    "valid_time",
    "variable",
    "p10",
    "p25",
    "p50",
    "p75",
    "p90",
    "mean",
    "member_count",
    "event_probability",
    "fetched_at",
]


def _utc(value: Any) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    return (
        timestamp.tz_localize("UTC")
        if timestamp.tzinfo is None
        else timestamp.tz_convert("UTC")
    )


def _member_columns(hourly: dict[str, Any], variable: str) -> list[str]:
    """Return the control plus numbered members in a deterministic order."""
    return sorted(
        key
        for key, values in hourly.items()
        if (key == variable or key.startswith(f"{variable}_member"))
        and isinstance(values, list)
    )


def parse_open_meteo_ensemble(
    payload: dict[str, Any],
    *,
    model: str = "icon_seamless",
    fetched_at: pd.Timestamp | None = None,
) -> pd.DataFrame:
    """Reduce individual perturbed members to robust hourly quantiles."""
    hourly = payload.get("hourly") or {}
    times = pd.to_datetime(hourly.get("time") or [], utc=True, errors="coerce")
    if not len(times):
        return pd.DataFrame(columns=ENSEMBLE_COLUMNS)

    fetched = _utc(pd.Timestamp.now(tz="UTC") if fetched_at is None else fetched_at)
    issued = fetched.floor("h")
    rows: list[dict[str, Any]] = []
    for api_name, internal_name in VARIABLES.items():
        columns = _member_columns(hourly, api_name)
        vectors = [
            pd.to_numeric(pd.Series(hourly[column]), errors="coerce").to_numpy()
            for column in columns
            if len(hourly[column]) == len(times)
        ]
        if not vectors:
            continue
        matrix = np.column_stack(vectors).astype(float)
        for position, valid_time in enumerate(times):
            if pd.isna(valid_time):
                continue
            values = matrix[position]
            values = values[np.isfinite(values)]
            if not len(values):
                continue
            quantiles = np.quantile(values, [0.10, 0.25, 0.50, 0.75, 0.90])
            event_probability = (
                float(np.mean(values >= 0.1) * 100.0)
                if internal_name == "rain_mm"
                else np.nan
            )
            rows.append(
                {
                    "source": "open_meteo_ensemble",
                    "model": model,
                    "issued_at": issued,
                    "valid_time": valid_time,
                    "variable": internal_name,
                    "p10": float(quantiles[0]),
                    "p25": float(quantiles[1]),
                    "p50": float(quantiles[2]),
                    "p75": float(quantiles[3]),
                    "p90": float(quantiles[4]),
                    "mean": float(np.mean(values)),
                    "member_count": len(values),
                    "event_probability": event_probability,
                    "fetched_at": fetched,
                }
            )
    return pd.DataFrame(rows).reindex(columns=ENSEMBLE_COLUMNS)


def fetch_open_meteo_ensemble(
    cfg: Settings = settings,
    session: requests.Session | None = None,
) -> pd.DataFrame:
    """Fetch one European ensemble family; failure never blocks main forecasts."""
    own_session = session is None
    session = session or build_session(retries=2)
    try:
        response = session.get(
            "https://ensemble-api.open-meteo.com/v1/ensemble",
            params={
                "latitude": cfg.latitude,
                "longitude": cfg.longitude,
                "elevation": cfg.elevation_m,
                "timezone": "UTC",
                "forecast_days": cfg.forecast_days,
                "wind_speed_unit": "kmh",
                "precipitation_unit": "mm",
                "models": cfg.ensemble_model,
                "hourly": ",".join(VARIABLES),
            },
            timeout=(8, 45),
        )
        if not response.ok:
            raise EnsembleForecastError(
                f"ensemble: risposta HTTP {response.status_code}"
            )
        payload = response.json()
    except EnsembleForecastError:
        raise
    except (requests.RequestException, ValueError) as exc:
        raise EnsembleForecastError("ensemble: servizio non raggiungibile") from exc
    finally:
        if own_session:
            session.close()
    if not isinstance(payload, dict) or payload.get("error"):
        raise EnsembleForecastError("ensemble: risposta non valida")
    frame = parse_open_meteo_ensemble(payload, model=cfg.ensemble_model)
    if frame.empty:
        raise EnsembleForecastError("ensemble: risposta valida ma vuota")
    return frame


def _iso(value: Any) -> str:
    return _utc(value).strftime("%Y-%m-%dT%H:%M:%SZ")


def archive_ensemble(frame: pd.DataFrame, engine: Engine | None = None) -> int:
    """Append one idempotent ensemble emission to the probabilistic archive."""
    if frame.empty:
        return 0
    ensure_schema()
    engine = engine or get_engine()
    records: list[dict[str, Any]] = []
    for row in frame.reindex(columns=ENSEMBLE_COLUMNS).to_dict("records"):
        payload = dict(row)
        for column in ("issued_at", "valid_time", "fetched_at"):
            payload[column] = _iso(payload[column])
        for column in ("p10", "p25", "p50", "p75", "p90", "mean", "event_probability"):
            value = payload.get(column)
            payload[column] = None if pd.isna(value) else float(value)
        payload["member_count"] = int(payload.get("member_count") or 0)
        records.append(payload)
    placeholders = ",".join(f":{column}" for column in ENSEMBLE_COLUMNS)
    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO forecast_ensemble_runs ("
                + ",".join(ENSEMBLE_COLUMNS)
                + ") VALUES ("
                + placeholders
                + ") ON CONFLICT (source,model,issued_at,valid_time,variable) "
                "DO UPDATE SET p10=excluded.p10,p25=excluded.p25,p50=excluded.p50,"
                "p75=excluded.p75,p90=excluded.p90,mean=excluded.mean,"
                "member_count=excluded.member_count,event_probability=excluded.event_probability,"
                "fetched_at=excluded.fetched_at"
            ),
            records,
        )
    return len(records)


def refresh_ensemble(
    cfg: Settings = settings, engine: Engine | None = None
) -> tuple[pd.DataFrame, str | None]:
    """Fetch, archive and report health while keeping the source non-blocking."""
    if not cfg.ensemble_forecast_enabled:
        record_source_disabled("open_meteo_ensemble", engine=engine)
        return pd.DataFrame(columns=ENSEMBLE_COLUMNS), None
    started = perf_counter()
    try:
        frame = fetch_open_meteo_ensemble(cfg)
        archive_ensemble(frame, engine)
    except EnsembleForecastError as exc:
        record_source_result(
            "open_meteo_ensemble",
            success=False,
            latency_ms=(perf_counter() - started) * 1000,
            error=exc,
            engine=engine,
        )
        return pd.DataFrame(columns=ENSEMBLE_COLUMNS), str(exc)
    record_source_result(
        "open_meteo_ensemble",
        success=True,
        rows_received=len(frame),
        last_observation_at=frame["valid_time"].max(),
        latency_ms=(perf_counter() - started) * 1000,
        engine=engine,
    )
    return frame, None
