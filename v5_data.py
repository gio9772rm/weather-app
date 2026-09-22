"""Station-scoped read models and atomic public snapshots for Meteo Pro V5."""

from __future__ import annotations

import json
import logging
import math
from dataclasses import asdict, replace
from datetime import date, datetime

import numpy as np
import pandas as pd
from sqlalchemy import text

from config import Settings
from data_access import (
    daily_forecast,
    load_forecast,
    load_forecast_history,
    load_latest_dpc_radar,
    load_provider_scores,
    load_station,
    load_station_daily_summaries,
)
from db import ensure_schema, get_engine
from forecast_change import summarize_forecast_change
from v5_astronomy import PROFILES, observing_forecast, summarize_observing_nights

FORECAST_PUBLIC = (
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
    "precip_probability",
    "probability_source",
    "clouds",
    "cloud_low",
    "cloud_mid",
    "cloud_high",
    "visibility_m",
    "confidence",
    "description",
    "is_day",
    "temp_uncertainty_c",
    "provider_count",
    "method",
    "interval_hours",
)
OBSERVATION_PUBLIC = (
    "time",
    "temp_c",
    "feels_like_c",
    "humidity",
    "dewpoint_c",
    "pressure_hpa",
    "wind_kmh",
    "windgust_kmh",
    "winddir",
    "rain_mm",
    "rain_rate_mm_h",
    "solar_w_m2",
    "uv_index",
    "data_quality",
)


def clean_json(value):
    if isinstance(value, dict):
        return {str(k): clean_json(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [clean_json(v) for v in value]
    if value is None or value is pd.NA or value is pd.NaT:
        return None
    if isinstance(value, (pd.Timestamp, datetime, date)):
        return value.isoformat()
    if isinstance(value, np.generic):
        return clean_json(value.item())
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def records(frame: pd.DataFrame, columns=None):
    if frame.empty:
        return []
    return clean_json(
        frame.reindex(
            columns=[c for c in columns if c in frame] if columns else frame.columns
        ).to_dict("records")
    )


def station_settings(station_id: str, cfg: Settings | None = None) -> Settings:
    """Resolve private coordinates on the server, never from a browser parameter."""
    cfg = cfg or Settings.from_env()
    if station_id == cfg.station_id:
        return cfg
    ensure_schema()
    with get_engine().connect() as con:
        row = (
            con.execute(
                text(
                    "SELECT * FROM station_profiles WHERE station_id=:id AND enabled=1"
                ),
                {"id": station_id},
            )
            .mappings()
            .first()
        )
    if (
        not row
        or station_id != cfg.secondary_station_id
        or not cfg.secondary_station_enabled
    ):
        raise ValueError("Località non disponibile")
    if row["latitude"] is None or row["longitude"] is None:
        raise ValueError("Località non configurata")
    return replace(
        cfg,
        station_id=station_id,
        location_name=row["display_name"],
        latitude=float(row["latitude"]),
        longitude=float(row["longitude"]),
        elevation_m=float(row["elevation_m"] or 0),
        local_timezone=row["timezone"],
        official_observations_enabled=False,
        eea_air_observations_enabled=False,
        feature_measured_pollen_enabled=False,
        feature_official_alerts_enabled=False,
        ensemble_forecast_enabled=False,
    )


def public_stations(cfg: Settings | None = None):
    cfg = cfg or Settings.from_env()
    result = [
        {
            "id": cfg.station_id,
            "name": cfg.location_name,
            "timezone": cfg.local_timezone,
            "role": "primary",
        }
    ]
    if cfg.secondary_station_enabled:
        try:
            other = station_settings(cfg.secondary_station_id, cfg)
            result.append(
                {
                    "id": other.station_id,
                    "name": other.location_name,
                    "timezone": other.local_timezone,
                    "role": "secondary",
                }
            )
        except ValueError:
            pass
    return result


def scoped_health(station_id: str) -> dict:
    from data_access import health_snapshot

    cfg = station_settings(station_id)
    if station_id == Settings.from_env().station_id:
        return health_snapshot(cfg)
    samples = load_station(240, station_id)
    forecast = scoped_forecast(station_id)
    now = pd.Timestamp.now(tz="UTC")

    def age(value):
        return (now - value).total_seconds() / 60 if pd.notna(value) else float("inf")

    def status(minutes):
        return (
            "online"
            if minutes <= cfg.station_stale_minutes
            else "delayed"
            if minutes <= 60
            else "offline"
        )

    latest = samples.time.max() if not samples.empty else pd.NaT
    issued = forecast.issued_at.max() if not forecast.empty else pd.NaT
    freshness = {}
    for name, columns in {
        "temperature": ["temp_c"],
        "humidity": ["humidity"],
        "pressure": ["pressure_hpa"],
        "wind": ["wind_kmh"],
        "rain": ["rain_mm", "rain_rate_mm_h"],
        "solar": ["solar_w_m2", "uv_index"],
    }.items():
        available = [c for c in columns if c in samples]
        observed = (
            samples.loc[samples[available].notna().any(axis=1), "time"].max()
            if available
            else pd.NaT
        )
        freshness[name] = {
            "time": observed,
            "age_minutes": age(observed),
            "status": status(age(observed)),
        }
    return {
        "station_status": status(age(latest)),
        "forecast_status": "online"
        if age(issued) <= 180
        else "delayed"
        if age(issued) <= 720
        else "offline",
        "station_time": latest,
        "station_age_minutes": age(latest),
        "station_sample_age_minutes": age(latest),
        "forecast_issued": issued,
        "forecast_until": forecast.valid_time.max() if not forecast.empty else pd.NaT,
        "forecast_age_minutes": age(issued),
        "measurement_stale_minutes": cfg.station_stale_minutes,
        "measurement_freshness": freshness,
    }


def scoped_forecast(station_id: str, history=False) -> pd.DataFrame:
    cfg = Settings.from_env()
    if station_id == cfg.station_id:
        return load_forecast_history() if history else load_forecast()
    ensure_schema()
    with get_engine().connect() as con:
        rows = (
            con.execute(
                text(
                    "SELECT payload FROM location_forecasts WHERE station_id=:id ORDER BY issued_at DESC LIMIT :n"
                ),
                {"id": station_id, "n": 2 if history else 1},
            )
            .scalars()
            .all()
        )
    frames = [pd.DataFrame(json.loads(row)) for row in rows]
    frame = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    for col in ("valid_time", "issued_at"):
        if col in frame:
            frame[col] = pd.to_datetime(frame[col], utc=True, errors="coerce")
    return frame


def history_coverage(
    daily: pd.DataFrame, station_id: str, timezone: str, now: pd.Timestamp | None = None
) -> dict:
    now = now if now is not None else pd.Timestamp.now(tz="UTC")
    if daily.empty or "station_id" not in daily:
        return {
            "first": None,
            "last": None,
            "days": 0,
            "max_gap_days": 0,
            "calendar": [],
        }
    frame = daily[daily["station_id"].eq(station_id)].copy()
    if frame.empty:
        return {
            "first": None,
            "last": None,
            "days": 0,
            "max_gap_days": 0,
            "calendar": [],
        }
    frame["local_date"] = frame["local_date"].astype(str)
    frame = frame.drop_duplicates("local_date").set_index("local_date")
    today = now.tz_convert(timezone).date().isoformat()
    rows, gap, max_gap = [], 0, 0
    for day in pd.date_range(frame.index.min(), max(frame.index.max(), today)):
        label = str(day.date())
        row = frame.loc[label].to_dict() if label in frame.index else {}
        start = day.tz_localize(timezone)
        end = (day + pd.Timedelta(days=1)).tz_localize(timezone)
        expected = int(
            (end.tz_convert("UTC") - start.tz_convert("UTC")).total_seconds() / 300
        )
        count = row.get("sample_count")
        imported = "historical_daily_summary" in str(
            row.get("data_quality", "")
        ) or "export" in str(row.get("source", ""))
        if not row:
            status = "missing"
            gap += 1
            max_gap = max(max_gap, gap)
        else:
            gap = 0
            status = (
                "imported"
                if imported
                else "complete"
                if pd.notna(count) and count >= expected * 0.9 and label < today
                else "partial"
            )
        rows.append(
            {
                "date": label,
                "status": status,
                "samples": count,
                "expected": expected,
                "temp_min_c": row.get("temp_min_c"),
                "temp_max_c": row.get("temp_max_c"),
                "temp_mean_c": row.get("temp_mean_c"),
                "humidity_mean": row.get("humidity_mean"),
                "rain_mm": row.get("rain_mm"),
            }
        )
    return clean_json(
        {
            "first": frame.index.min(),
            "last": frame.index.max(),
            "days": len(frame),
            "max_gap_days": max_gap,
            "calendar": rows,
        }
    )


def build_snapshot(
    station_id: str,
    *,
    daily: pd.DataFrame | None = None,
    now: pd.Timestamp | None = None,
) -> dict:
    """Read-only; only allowlisted fields are serialized to public/offline clients."""
    cfg = station_settings(station_id)
    now = now if now is not None else pd.Timestamp.now(tz="UTC")
    station = load_station(240, station_id)
    forecast = scoped_forecast(station_id)
    history = scoped_forecast(station_id, history=True)
    daily = load_station_daily_summaries(365) if daily is None else daily
    current = records(station.tail(1), OBSERVATION_PUBLIC)
    latest = station["time"].max() if not station.empty else pd.NaT
    age = (now - latest).total_seconds() / 60 if pd.notna(latest) else None
    rain = (
        station.loc[station["time"].ge(now - pd.Timedelta(hours=24)), "rain_mm"]
        if not station.empty
        else pd.Series(dtype=float)
    )
    future = (
        forecast[forecast["valid_time"].ge(now.floor("h"))]
        if not forecast.empty
        else forecast
    )
    astro = {}
    for profile, info in PROFILES.items():
        hours = observing_forecast(forecast, cfg, profile, now)
        astro[profile] = {
            "label": info["label"],
            "nights": records(summarize_observing_nights(hours)),
            "hours": records(
                hours,
                (
                    "start",
                    "end",
                    "date",
                    "astro_score",
                    "astro_label",
                    "clouds",
                    "wind_kmh",
                    "limiting_factor",
                    "penalties",
                    "missing_fields",
                    "probability_source",
                    "complete",
                ),
            ),
        }
    change = asdict(summarize_forecast_change(history, now=now))
    primary = station_id == Settings.from_env().station_id
    with get_engine().connect() as con:
        environment = con.execute(
            text("SELECT payload FROM location_environment WHERE station_id=:id"),
            {"id": station_id},
        ).scalar()
        local_scores = con.execute(
            text("SELECT payload FROM location_scores WHERE station_id=:id"),
            {"id": station_id},
        ).scalar()
    scores = (
        records(
            load_provider_scores(),
            (
                "provider",
                "model",
                "variable",
                "horizon",
                "n",
                "mae",
                "rmse",
                "bias",
                "holdout_n",
                "holdout_mae",
                "skill_vs_persistence",
            ),
        )
        if primary
        else json.loads(local_scores)
        if local_scores
        else []
    )
    payload = {
        "version": "5.0.0",
        "station": {
            "id": station_id,
            "name": cfg.location_name,
            "timezone": cfg.local_timezone,
            "role": "primary" if primary else "secondary",
        },
        "generated_at": now,
        "refresh_seconds": 600,
        "observed_at": latest,
        "age_minutes": age,
        "live": age is not None and 0 <= age <= cfg.station_stale_minutes,
        "current": current[0] if current else {},
        "rain_24h_mm": rain.sum(min_count=1),
        "observations": records(station.tail(576), OBSERVATION_PUBLIC),
        "forecast": records(future, FORECAST_PUBLIC),
        "daily": records(daily_forecast(future, cfg.local_timezone)),
        "history": history_coverage(daily, station_id, cfg.local_timezone, now),
        "astronomy": astro,
        "forecast_change": change,
        "scores": scores,
        "air": json.loads(environment) if environment else None,
        "radar": records(
            load_latest_dpc_radar(station_id),
            (
                "time",
                "observed_at",
                "sri_point_mm_h",
                "vmi_point_dbz",
                "lightning_10km",
                "lightning_25km",
                "lightning_50km",
                "source",
            ),
        ),
        "calibration": "Calibrazione locale Roma"
        if primary
        else "Previsione indipendente; calibrazione locale non ancora attiva",
        "notices": [
            "Comacchio non modifica la calibrazione di Roma.",
            "Le misure e le previsioni hanno origini e orari distinti.",
        ],
    }
    return clean_json(payload)


def publish_snapshots(cfg: Settings | None = None) -> int:
    """One atomic publish per successful ingest cycle, independent station failures."""
    cfg = cfg or Settings.from_env()
    daily = load_station_daily_summaries(365)
    count = 0
    for station in public_stations(cfg):
        try:
            payload = build_snapshot(station["id"], daily=daily)
        except Exception:  # noqa: BLE001 - station failures are independent
            logging.getLogger(__name__).warning("Snapshot V5 rinviato per una stazione")
            continue
        with get_engine().begin() as con:
            con.execute(
                text(
                    "INSERT INTO public_snapshots(station_id,generated_at,payload) VALUES(:id,:at,:payload) ON CONFLICT(station_id) DO UPDATE SET generated_at=excluded.generated_at,payload=excluded.payload"
                ),
                {
                    "id": station["id"],
                    "at": payload["generated_at"],
                    "payload": json.dumps(payload, ensure_ascii=False, allow_nan=False),
                },
            )
        count += 1
    return count


def read_snapshot(station_id: str) -> dict:
    station_settings(station_id)  # Disabled/unconfigured stations are not exposed.
    with get_engine().connect() as con:
        payload = con.execute(
            text("SELECT payload FROM public_snapshots WHERE station_id=:id"),
            {"id": station_id},
        ).scalar()
    return json.loads(payload) if payload else build_snapshot(station_id)
