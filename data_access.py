"""Read-only, cached-friendly data access for the dashboard."""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from config import Settings, settings
from db import ensure_schema, get_engine


def _read(query: str, params: dict[str, Any] | None = None) -> pd.DataFrame:
    try:
        ensure_schema()
        with get_engine().connect() as connection:
            frame = pd.read_sql(text(query), connection, params=params or {})
    except SQLAlchemyError:
        return pd.DataFrame()
    frame.columns = [column.lower() for column in frame.columns]
    return frame


def load_station(hours: int = 240) -> pd.DataFrame:
    cutoff = (pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=hours)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    frame = _read(
        "SELECT * FROM station_raw WHERE time >= :cutoff ORDER BY time",
        {"cutoff": cutoff},
    )
    if frame.empty:
        return frame
    frame["time"] = pd.to_datetime(frame["time"], utc=True, errors="coerce")
    numeric = [
        "temp_c",
        "humidity",
        "pressure_hpa",
        "wind_kmh",
        "windgust_kmh",
        "winddir",
        "rain_mm",
        "rain_rate_mm_h",
        "rain_total_mm",
        "solar_w_m2",
        "uv_index",
    ]
    for column in numeric:
        if column in frame:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    # V1/V2 stored rain rate in Rain_mm. Never present it as an amount.
    if "source" in frame and "rain_mm" in frame:
        legacy = frame["source"].isna() | frame["source"].astype(
            "string"
        ).str.strip().eq("")
        frame.loc[legacy, "rain_mm"] = np.nan
        if "data_quality" in frame:
            frame.loc[legacy, "data_quality"] = "legacy_unknown_rain"
    return frame.dropna(subset=["time"]).sort_values("time")


def _legacy_forecast() -> pd.DataFrame:
    frame = _read("SELECT * FROM forecast_ow ORDER BY time")
    if frame.empty:
        return frame
    rename = {
        "time": "valid_time",
        "wind_mps": "wind_mps",
    }
    frame = frame.rename(columns=rename)
    frame["valid_time"] = pd.to_datetime(frame["valid_time"], utc=True, errors="coerce")
    frame["issued_at"] = pd.Timestamp.now(tz="UTC").floor("h")
    if "wind_mps" in frame:
        frame["wind_kmh"] = pd.to_numeric(frame["wind_mps"], errors="coerce") * 3.6
    frame["precip_probability"] = np.nan
    frame["confidence"] = 40.0
    frame["temp_uncertainty_c"] = np.nan
    frame["provider_count"] = 1
    frame["description"] = "Previsione precedente"
    frame["method"] = "legacy_openweather"
    return frame


def load_forecast(hours: int = 192) -> pd.DataFrame:
    start = (pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=3)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    end = (pd.Timestamp.now(tz="UTC") + pd.Timedelta(hours=hours)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    frame = _read(
        "SELECT * FROM forecast_blend WHERE valid_time BETWEEN :start AND :end ORDER BY valid_time",
        {"start": start, "end": end},
    )
    if frame.empty:
        return _legacy_forecast()
    for column in ("valid_time", "issued_at"):
        frame[column] = pd.to_datetime(frame[column], utc=True, errors="coerce")
    numeric = [
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
        "temp_uncertainty_c",
        "confidence",
        "provider_count",
    ]
    for column in numeric:
        if column in frame:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame.dropna(subset=["valid_time"]).sort_values("valid_time")


def load_provider_scores() -> pd.DataFrame:
    frame = _read(
        "SELECT * FROM forecast_scores WHERE evaluated_at = "
        "(SELECT MAX(evaluated_at) FROM forecast_scores) ORDER BY variable,horizon,mae"
    )
    for column in ("n", "bias", "mae", "rmse", "brier"):
        if column in frame:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame


def load_recent_logs(limit: int = 12) -> pd.DataFrame:
    # limit is constrained here rather than interpolating user input.
    limit = max(1, min(int(limit), 50))
    frame = _read(
        f"SELECT started_at,finished_at,component,status,rows_written,message "
        f"FROM ingest_log ORDER BY started_at DESC LIMIT {limit}"
    )
    for column in ("started_at", "finished_at"):
        if column in frame:
            frame[column] = pd.to_datetime(frame[column], utc=True, errors="coerce")
    return frame


def health_snapshot(cfg: Settings = settings) -> dict[str, Any]:
    station_frame = _read(
        "SELECT MAX(time) AS station_time FROM station_raw"
    )
    blend_frame = _read(
        "SELECT MAX(issued_at) AS forecast_issued, "
        "MAX(valid_time) AS forecast_until FROM forecast_blend"
    )
    now = pd.Timestamp.now(tz="UTC")

    station_time = _timestamp_from_frame(station_frame, "station_time")
    forecast_issued = _timestamp_from_frame(blend_frame, "forecast_issued")
    forecast_until = _timestamp_from_frame(blend_frame, "forecast_until")

    # Query the legacy table only when V3 has no usable timestamps. Keeping these
    # reads independent prevents one incompatible legacy table from hiding the
    # healthy station and V3 forecast status.
    if pd.isna(forecast_issued) or pd.isna(forecast_until):
        legacy_frame = _read(
            "SELECT MAX(time) AS legacy_time FROM forecast_ow"
        )
        legacy_time = _timestamp_from_frame(legacy_frame, "legacy_time")
        if pd.isna(forecast_issued):
            forecast_issued = legacy_time
        if pd.isna(forecast_until):
            forecast_until = legacy_time
    station_age = (
        (now - station_time).total_seconds() / 60
        if not pd.isna(station_time)
        else float("inf")
    )
    forecast_age = (
        (now - forecast_issued).total_seconds() / 60
        if not pd.isna(forecast_issued)
        else float("inf")
    )
    station_status = (
        "online"
        if station_age <= cfg.station_stale_minutes
        else "delayed"
        if station_age <= cfg.station_stale_minutes * 3
        else "offline"
    )
    forecast_status = (
        "online"
        if forecast_age <= max(180, cfg.forecast_refresh_minutes * 3)
        else "delayed"
        if forecast_age <= 720
        else "offline"
    )
    return {
        "station_status": station_status,
        "forecast_status": forecast_status,
        "station_time": station_time,
        "forecast_issued": forecast_issued,
        "forecast_until": forecast_until,
        "station_age_minutes": station_age,
        "forecast_age_minutes": forecast_age,
    }


def _timestamp_from_frame(frame: pd.DataFrame, column: str) -> Any:
    if frame.empty or column not in frame:
        return pd.NaT
    return pd.to_datetime(frame.iloc[0].get(column), utc=True, errors="coerce")


def daily_forecast(frame: pd.DataFrame, timezone_name: str) -> pd.DataFrame:
    if frame.empty:
        return frame
    data = frame.copy()
    data["local_time"] = data["valid_time"].dt.tz_convert(timezone_name)
    data["date"] = data["local_time"].dt.date
    aggregations: dict[str, tuple[str, str]] = {
        "temp_min": ("temp_c", "min"),
        "temp_max": ("temp_c", "max"),
        "rain_mm": ("rain_mm", "sum"),
        "pop_max": ("precip_probability", "max"),
        "wind_max": ("wind_gust_kmh", "max"),
        "clouds_mean": ("clouds", "mean"),
        "confidence": ("confidence", "mean"),
    }
    available = {key: value for key, value in aggregations.items() if value[0] in data}
    daily = data.groupby("date").agg(**available).reset_index()
    descriptions = (
        data.assign(_hour=data["local_time"].dt.hour)
        .sort_values("_hour", key=lambda values: (values - 13).abs())
        .groupby("date")["description"]
        .first()
        if "description" in data
        else pd.Series(dtype="object")
    )
    if not descriptions.empty:
        daily["description"] = daily["date"].map(descriptions)
    return daily
