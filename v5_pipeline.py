"""Independent second-location forecasting and V5 publication within the cron."""

from __future__ import annotations

import json
import logging
from dataclasses import replace

import pandas as pd
from sqlalchemy import text

from config import Settings
from db import get_engine
from forecast_providers import fetch_all_forecasts
from v5_data import (
    clean_json,
    public_stations,
    publish_snapshots,
    records,
    station_settings,
)

log = logging.getLogger(__name__)


def select_secondary_forecast(frames: list[pd.DataFrame]) -> pd.DataFrame:
    """Prefer ICON-2I, supplement only missing probability at identical hours."""
    combined = pd.concat(frames, ignore_index=True)
    combined["valid_time"] = pd.to_datetime(combined["valid_time"], utc=True)
    combined["_rank"] = (
        combined["model"]
        .astype(str)
        .str.contains("icon.*2i|icon_italia", case=False, regex=True)
        .astype(int)
    )
    selected = (
        combined.sort_values(["valid_time", "_rank", "issued_at"])
        .drop_duplicates("valid_time", keep="last")
        .drop(columns="_rank")
        .sort_values("valid_time")
        .copy()
    )
    selected["probability_source"] = (
        selected["model"]
        .map({"best_match": "Open-Meteo best-match", "icon_2i_2p2km": "ICON-2I"})
        .where(selected["precip_probability"].notna())
    )
    # ICON-2I is deterministic and often has no precipitation probability.
    # Reuse the same-location best-match fetched in this cycle, never Rome's
    # forecast, a different target hour or an inferred zero from dry rain totals.
    probability = (
        combined[
            combined["model"].eq("best_match")
            & combined["interval_hours"].eq(1)
            & combined["precip_probability"].between(0, 100)
        ]
        .sort_values("issued_at")
        .drop_duplicates("valid_time", keep="last")
        .set_index("valid_time")["precip_probability"]
    )
    supplement = selected["valid_time"].map(probability)
    missing = (
        selected["precip_probability"].isna()
        & selected["interval_hours"].eq(1)
        & supplement.notna()
    )
    selected.loc[missing, "precip_probability"] = supplement[missing]
    selected.loc[missing, "probability_source"] = "Open-Meteo best-match"
    return selected


def refresh_secondary_forecast(cfg: Settings, *, force=False) -> int:
    if not cfg.secondary_station_enabled:
        return 0
    scoped = station_settings(cfg.secondary_station_id, cfg)
    now = pd.Timestamp.now(tz="UTC")
    with get_engine().connect() as con:
        latest = con.execute(
            text("SELECT MAX(issued_at) FROM location_forecasts WHERE station_id=:id"),
            {"id": scoped.station_id},
        ).scalar()
    issued = pd.to_datetime(latest, utc=True, errors="coerce")
    if (
        not force
        and pd.notna(issued)
        and now - issued < pd.Timedelta(minutes=cfg.forecast_refresh_minutes)
    ):
        return 0
    # The second site never consumes the primary provider's optional paid key
    # and never writes to primary forecast_runs/scores/blend or health entries.
    scoped = replace(scoped, openweather_api_key="")
    frames, warnings = fetch_all_forecasts(
        scoped, source_prefix=f"station:{scoped.station_id}:"
    )
    if not frames:
        raise RuntimeError("Previsione secondaria non disponibile")
    combined = select_secondary_forecast(frames)
    combined["issued_at"] = now
    combined["confidence"] = None
    combined["provider_count"] = 1
    combined["method"] = "independent_uncalibrated"
    cutoff = (now - pd.Timedelta(days=90)).isoformat()
    with get_engine().begin() as con:
        con.execute(
            text(
                "INSERT INTO location_forecasts(station_id,issued_at,payload) VALUES(:id,:at,:payload) ON CONFLICT(station_id,issued_at) DO UPDATE SET payload=excluded.payload"
            ),
            {
                "id": scoped.station_id,
                "at": now.isoformat(),
                "payload": json.dumps(clean_json(records(combined)), allow_nan=False),
            },
        )
        # Only obsolete derived emissions, never station measurements/imports.
        con.execute(
            text(
                "DELETE FROM location_forecasts WHERE station_id=:id AND issued_at<:cutoff"
            ),
            {"id": scoped.station_id, "cutoff": cutoff},
        )
    if warnings:
        log.warning("Previsione secondaria: %d provider non disponibili", len(warnings))
    return len(combined)


def run_v5_publication(cfg: Settings, *, force=False) -> dict:
    result = {"secondary_forecast_rows": 0, "snapshots": 0, "warnings": []}
    try:
        result["secondary_forecast_rows"] = refresh_secondary_forecast(cfg, force=force)
        if result["secondary_forecast_rows"]:
            from v5_verification import update_secondary_scores

            update_secondary_scores(cfg.secondary_station_id)
    except Exception:  # noqa: BLE001 - isolate optional provider errors, never log private URLs
        log.warning("Previsione secondaria V5 rinviata; Roma resta indipendente")
        result["warnings"].append("Previsione secondaria rinviata")
    for station in public_stations(cfg):
        try:
            refresh_environment(station_settings(station["id"], cfg))
        except Exception:  # noqa: BLE001 - retain last environmental snapshot
            log.warning("Previsione ambientale V5 rinviata")
    try:
        result["snapshots"] = publish_snapshots(cfg)
    except Exception:  # noqa: BLE001 - existing live ingestion remains independent
        log.warning("Pubblicazione V5 rinviata; snapshot precedente conservato")
        result["warnings"].append("Snapshot V5 rinviato")
    try:
        from v5_push import send_pending

        result["notifications"] = send_pending()
    except Exception:  # noqa: BLE001 - never log push endpoint/key details
        log.warning("Avvisi V5 rinviati")
    return result


def refresh_environment(cfg: Settings) -> None:
    from air_quality import fetch_air_quality

    now = pd.Timestamp.now(tz="UTC")
    with get_engine().begin() as con:
        latest = con.execute(
            text("SELECT attempted_at FROM location_environment WHERE station_id=:id"),
            {"id": cfg.station_id},
        ).scalar()
        if latest and now - pd.Timestamp(latest) < pd.Timedelta(hours=1):
            return
        con.execute(
            text(
                "INSERT INTO location_environment(station_id,attempted_at) VALUES(:id,:at) ON CONFLICT(station_id) DO UPDATE SET attempted_at=excluded.attempted_at"
            ),
            {"id": cfg.station_id, "at": now.isoformat()},
        )
    air = fetch_air_quality(cfg.latitude, cfg.longitude, cfg.local_timezone)
    payload = clean_json(
        {
            "source": air.source,
            "fetched_at": air.fetched_at,
            "current": air.current,
            "hourly": records(air.hourly),
        }
    )
    with get_engine().begin() as con:
        con.execute(
            text(
                "UPDATE location_environment SET payload=:payload WHERE station_id=:id"
            ),
            {"id": cfg.station_id, "payload": json.dumps(payload, allow_nan=False)},
        )
