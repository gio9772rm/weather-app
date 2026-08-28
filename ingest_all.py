"""Single scheduled entry point for station data and multi-provider forecasts."""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import tempfile
import time
import uuid
from pathlib import Path
from time import perf_counter
from typing import Any

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.exc import SQLAlchemyError

from climatology import refresh_climatology
from config import Settings
from db import ensure_schema, get_engine, get_meta, set_meta
from dpc_radar import refresh_dpc_radar
from ensemble_forecast import refresh_ensemble
from forecast_blend import (
    archive_forecast,
    build_blend,
    score_forecasts,
    score_forecasts_against_references,
)
from forecast_providers import fetch_all_forecasts
from measured_pollen import refresh_measured_pollen
from observed_air import refresh_observed_air
from official_alerts import refresh_official_alerts
from official_observations import ingest_official_observations
from reference_climatology import refresh_reference_climatology
from source_health import record_source_result
from station_registry import ensure_primary_station, sync_primary_station_history
from weather_ingest_ecowitt_cloud import run_station_ingest

load_dotenv()
logging.basicConfig(
    level=getattr(logging, (os.getenv("LOG_LEVEL") or "INFO").upper(), logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("ingest_all")


class FileLock:
    def __init__(self, stale_seconds: int = 1800, path: Path | None = None):
        self.path = path or Path(tempfile.gettempdir()) / "weather_app_v3_ingest.lock"
        self.stale_seconds = stale_seconds
        self.file_descriptor: int | None = None

    def acquire(self) -> bool:
        flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
        try:
            self.file_descriptor = os.open(self.path, flags, 0o644)
            os.write(self.file_descriptor, str(int(time.time())).encode("ascii"))
            return True
        except FileExistsError:
            try:
                age = time.time() - int(self.path.read_text(encoding="ascii") or "0")
            except (OSError, ValueError):
                age = self.stale_seconds + 1
            if age <= self.stale_seconds:
                return False
            try:
                self.path.unlink(missing_ok=True)
            except OSError:
                return False
            return self.acquire()

    def release(self) -> None:
        if self.file_descriptor is not None:
            os.close(self.file_descriptor)
            self.file_descriptor = None
        try:
            self.path.unlink(missing_ok=True)
        except OSError:
            pass


class PipelineLock:
    """Prevent overlapping ingestion across Render, GitHub and local launches.

    PostgreSQL advisory locks coordinate independent machines and disappear
    automatically if a process is interrupted. SQLite keeps the existing local
    file lock because it has no cross-process advisory-lock primitive.
    """

    advisory_key = 0x4D4554454F5633  # ASCII-ish stable key: ``METEOV3``.

    def __init__(self, file_lock: FileLock | None = None):
        self.file_lock = file_lock or FileLock()
        self.connection: Connection | None = None
        self.uses_database_lock = False

    def acquire(self) -> bool:
        engine = get_engine()
        if engine.dialect.name != "postgresql":
            return self.file_lock.acquire()

        connection = engine.connect()
        try:
            acquired = bool(
                connection.execute(
                    text("SELECT pg_try_advisory_lock(:key)"),
                    {"key": self.advisory_key},
                ).scalar_one()
            )
        except Exception:
            connection.close()
            raise
        if not acquired:
            connection.close()
            return False
        self.connection = connection
        self.uses_database_lock = True
        return True

    def release(self) -> None:
        if self.uses_database_lock and self.connection is not None:
            try:
                self.connection.execute(
                    text("SELECT pg_advisory_unlock(:key)"),
                    {"key": self.advisory_key},
                )
            finally:
                self.connection.close()
                self.connection = None
                self.uses_database_lock = False
            return
        self.file_lock.release()


def _safe_message(value: Any) -> str:
    message = str(value).replace("\n", " ").strip()
    # Provider exceptions are already redacted; truncation protects the public log.
    return message[:500]


def _log_start(component: str) -> tuple[str, str]:
    identifier = uuid.uuid4().hex
    started = pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%dT%H:%M:%SZ")
    with get_engine().begin() as connection:
        connection.execute(
            text(
                "INSERT INTO ingest_log (id,started_at,component,status,rows_written,message) "
                "VALUES (:id,:started,:component,'running',0,'')"
            ),
            {"id": identifier, "started": started, "component": component},
        )
    return identifier, started


def _log_finish(identifier: str, status: str, rows: int, message: Any = "") -> None:
    finished = pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%dT%H:%M:%SZ")
    with get_engine().begin() as connection:
        connection.execute(
            text(
                "UPDATE ingest_log SET finished_at=:finished,status=:status,"
                "rows_written=:rows,message=:message WHERE id=:id"
            ),
            {
                "id": identifier,
                "finished": finished,
                "status": status,
                "rows": int(rows),
                "message": _safe_message(message),
            },
        )


def forecast_is_due(cfg: Settings, force: bool = False) -> bool:
    if force:
        return True
    last = pd.to_datetime(get_meta("last_forecast_success"), utc=True, errors="coerce")
    if pd.isna(last):
        return True
    return pd.Timestamp.now(tz="UTC") - last >= pd.Timedelta(
        minutes=cfg.forecast_refresh_minutes
    )


def official_observations_are_due(cfg: Settings, force: bool = False) -> bool:
    if not cfg.official_observations_enabled:
        return False
    if force:
        return True
    last = pd.to_datetime(
        get_meta("last_official_observation_success"), utc=True, errors="coerce"
    )
    if pd.isna(last):
        return True
    return pd.Timestamp.now(tz="UTC") - last >= pd.Timedelta(
        minutes=cfg.official_observation_refresh_minutes
    )


def dpc_radar_is_due(cfg: Settings, force: bool = False) -> bool:
    if not cfg.dpc_radar_enabled:
        return False
    if force:
        return True
    last = pd.to_datetime(
        get_meta("last_dpc_radar_success"), utc=True, errors="coerce"
    )
    return pd.isna(last) or (
        pd.Timestamp.now(tz="UTC") - last
        >= pd.Timedelta(minutes=cfg.dpc_radar_refresh_minutes)
    )


def run_forecast_pipeline(cfg: Settings) -> dict[str, Any]:
    frames, warnings = fetch_all_forecasts(cfg)
    if not frames:
        raise RuntimeError("Nessun provider di previsione ha restituito dati")
    archived = sum(archive_forecast(frame) for frame in frames)
    try:
        ensemble, ensemble_warning = refresh_ensemble(cfg)
    except Exception as exc:  # noqa: BLE001 - optional guidance cannot stop forecasts
        ensemble = pd.DataFrame()
        ensemble_warning = _safe_message(exc) or "errore interno isolato"
    if ensemble_warning:
        warnings.append(
            f"Guida probabilistica rinviata: {_safe_message(ensemble_warning)}"
        )
    try:
        scores = score_forecasts(cfg)
        local_score_rows = len(scores)
    except Exception as exc:  # noqa: BLE001 - scoring must not discard a valid forecast
        warnings.append(f"Verifica errori rinviata: {_safe_message(exc)}")
        local_score_rows = 0
    try:
        reference_scores = score_forecasts_against_references(cfg)
        reference_score_rows = len(reference_scores)
    except Exception as exc:  # noqa: BLE001 - the official network is secondary
        warnings.append(f"Verifica rete ufficiale rinviata: {_safe_message(exc)}")
        reference_score_rows = 0
    blend = build_blend(cfg=cfg)
    if blend.empty:
        raise RuntimeError("Previsioni archiviate ma combinazione finale vuota")
    now = pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%dT%H:%M:%SZ")
    set_meta("last_forecast_success", now)
    set_meta("last_forecast_issued_at", blend["issued_at"].max())
    set_meta(
        "forecast_providers",
        json.dumps(sorted({str(frame.iloc[0]["provider"]) for frame in frames})),
    )
    try:
        observed_air, air_warning = refresh_observed_air(cfg)
    except Exception as exc:  # noqa: BLE001 - optional environment feed is isolated
        observed_air = pd.DataFrame()
        air_warning = _safe_message(exc) or "errore interno isolato"
    if air_warning:
        warnings.append(
            f"Aria ufficiale osservata rinviata: {_safe_message(air_warning)}"
        )
    try:
        measured_pollen, pollen_warning = refresh_measured_pollen(cfg)
    except Exception as exc:  # noqa: BLE001 - institutional feed stays isolated
        measured_pollen = pd.DataFrame()
        pollen_warning = _safe_message(exc) or "errore interno isolato"
    if pollen_warning:
        warnings.append(f"Pollini misurati rinviati: {_safe_message(pollen_warning)}")
    try:
        official_alerts, alerts_warning = refresh_official_alerts(cfg)
    except Exception as exc:  # noqa: BLE001 - bulletins never stop the forecast
        official_alerts = pd.DataFrame()
        alerts_warning = _safe_message(exc) or "errore interno isolato"
    if alerts_warning:
        warnings.append(
            f"Bollettini ufficiali rinviati: {_safe_message(alerts_warning)}"
        )
    try:
        climate_normals, climate_warning = refresh_climatology(cfg)
    except Exception as exc:  # noqa: BLE001 - local baseline is an optional layer
        climate_normals = pd.DataFrame()
        climate_warning = _safe_message(exc) or "errore interno isolato"
    if climate_warning:
        warnings.append(
            f"Climatologia locale rinviata: {_safe_message(climate_warning)}"
        )
    try:
        reference_normals, reference_warning = refresh_reference_climatology(cfg)
    except Exception as exc:  # noqa: BLE001 - long-period context is isolated
        reference_normals = pd.DataFrame()
        reference_warning = _safe_message(exc) or "errore interno isolato"
    if reference_warning:
        warnings.append(
            f"Riferimento climatico 1991-2020 rinviato: "
            f"{_safe_message(reference_warning)}"
        )
    return {
        "archived": archived,
        "blend_rows": len(blend),
        "score_rows": local_score_rows + reference_score_rows,
        "local_score_rows": local_score_rows,
        "reference_score_rows": reference_score_rows,
        "ensemble_rows": len(ensemble),
        "observed_air_rows": len(observed_air),
        "measured_pollen_rows": len(measured_pollen),
        "official_alert_rows": len(official_alerts),
        "climate_normal_rows": len(climate_normals),
        "reference_climate_rows": len(reference_normals),
        "warnings": warnings,
    }


def prune_derived_history() -> None:
    """Bound database growth while retaining more history than scoring needs."""
    now = pd.Timestamp.now(tz="UTC")
    forecast_cutoff = (now - pd.Timedelta(days=120)).strftime("%Y-%m-%dT%H:%M:%SZ")
    score_cutoff = (now - pd.Timedelta(days=180)).strftime("%Y-%m-%dT%H:%M:%SZ")
    observation_cutoff = (now - pd.Timedelta(days=180)).strftime("%Y-%m-%dT%H:%M:%SZ")
    log_cutoff = (now - pd.Timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
    with get_engine().begin() as connection:
        connection.execute(
            text("DELETE FROM forecast_runs WHERE issued_at < :cutoff"),
            {"cutoff": forecast_cutoff},
        )
        connection.execute(
            text("DELETE FROM forecast_blend_history WHERE issued_at < :cutoff"),
            {"cutoff": forecast_cutoff},
        )
        connection.execute(
            text("DELETE FROM forecast_ensemble_runs WHERE issued_at < :cutoff"),
            {"cutoff": forecast_cutoff},
        )
        connection.execute(
            text("DELETE FROM forecast_scores WHERE evaluated_at < :cutoff"),
            {"cutoff": score_cutoff},
        )
        connection.execute(
            text("DELETE FROM forecast_regime_scores WHERE evaluated_at < :cutoff"),
            {"cutoff": score_cutoff},
        )
        connection.execute(
            text("DELETE FROM forecast_reference_scores WHERE evaluated_at < :cutoff"),
            {"cutoff": score_cutoff},
        )
        connection.execute(
            text("DELETE FROM official_observations WHERE time < :cutoff"),
            {"cutoff": observation_cutoff},
        )
        connection.execute(
            text("DELETE FROM environment_observations WHERE time < :cutoff"),
            {"cutoff": observation_cutoff},
        )
        connection.execute(
            text("DELETE FROM official_alerts WHERE issued_at < :cutoff"),
            {"cutoff": score_cutoff},
        )
        connection.execute(
            text("DELETE FROM radar_local_snapshots WHERE observed_at < :cutoff"),
            {
                "cutoff": (now - pd.Timedelta(days=14)).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                )
            },
        )
        connection.execute(
            text("DELETE FROM ingest_log WHERE started_at < :cutoff"),
            {"cutoff": log_cutoff},
        )


def adaptive_station_backfill_hours(
    cfg: Settings,
    requested_hours: int | None = None,
    *,
    now: pd.Timestamp | None = None,
) -> int:
    """Expand the Ecowitt history window when a primary sensor has gone stale."""
    base = (
        cfg.station_backfill_hours
        if requested_hours is None
        else max(0, int(requested_hours))
    )
    cap = max(base, cfg.station_auto_backfill_max_hours)
    current = now if now is not None else pd.Timestamp.now(tz="UTC")
    query = text(
        "SELECT MAX(CASE WHEN temp_c IS NOT NULL THEN time END) AS temperature_time, "
        "MAX(CASE WHEN humidity IS NOT NULL THEN time END) AS humidity_time, "
        "MAX(CASE WHEN pressure_hpa IS NOT NULL THEN time END) AS pressure_time, "
        "MAX(CASE WHEN wind_kmh IS NOT NULL THEN time END) AS wind_time "
        "FROM station_raw"
    )
    try:
        with get_engine().connect() as connection:
            latest = connection.execute(query).mappings().first()
    except SQLAlchemyError as exc:
        log.warning(
            "Backfill adattivo non calcolabile, uso %s ore: %s",
            base,
            _safe_message(exc),
        )
        return base
    if not latest:
        return cap

    timestamps = [
        pd.to_datetime(latest.get(column), utc=True, errors="coerce")
        for column in (
            "temperature_time",
            "humidity_time",
            "pressure_time",
            "wind_time",
        )
    ]
    if any(pd.isna(timestamp) for timestamp in timestamps):
        expanded = cap
    else:
        oldest = min(timestamps)
        age_hours = max(0.0, (current - oldest).total_seconds() / 3600)
        expanded = min(cap, max(base, math.ceil(age_hours) + 1))
    if expanded > base:
        log.warning(
            "Dati primari incompleti o arretrati: backfill Ecowitt ampliato da %s a %s ore",
            base,
            expanded,
        )
    return expanded


def station_source_age_minutes(
    latest_station_time: Any,
    *,
    now: pd.Timestamp | None = None,
) -> float:
    """Return the age of the newest source sample, never a negative value."""
    latest = pd.to_datetime(latest_station_time, utc=True, errors="coerce")
    if pd.isna(latest):
        return float("inf")
    current = now if now is not None else pd.Timestamp.now(tz="UTC")
    return max(0.0, (current - latest).total_seconds() / 60.0)


def run_all(
    *,
    backfill_hours: int | None = None,
    skip_station: bool = False,
    force_forecast: bool = False,
    max_station_age_minutes: int | None = None,
) -> dict[str, Any]:
    ensure_schema()
    cfg = Settings.from_env()
    ensure_primary_station(cfg)
    sync_primary_station_history(cfg)
    result: dict[str, Any] = {
        "station": None,
        "official": None,
        "radar": None,
        "forecast": None,
        "errors": [],
    }

    if not skip_station:
        identifier, _ = _log_start("station")
        station_started = perf_counter()
        rows_written = 0
        try:
            effective_backfill = adaptive_station_backfill_hours(cfg, backfill_hours)
            station = run_station_ingest(effective_backfill, cfg)
            station["station_rows_synced"] = sync_primary_station_history(cfg)
            station["backfill_hours"] = effective_backfill
            station["source_age_minutes"] = station_source_age_minutes(
                station.get("latest_station_time")
            )
            result["station"] = station
            rows_written = int(station.get("rows") or 0)
            if (
                max_station_age_minutes is not None
                and station["source_age_minutes"] > max_station_age_minutes
            ):
                raise RuntimeError(
                    "Ecowitt ha risposto, ma il campione più recente ha "
                    f"{station['source_age_minutes']:.1f} minuti "
                    f"(limite {max_station_age_minutes})"
                )
            _log_finish(
                identifier, "success", station["rows"], "; ".join(station["warnings"])
            )
            record_source_result(
                "ecowitt",
                success=True,
                rows_received=station["rows"],
                last_observation_at=station.get("latest_station_time"),
                latency_ms=(perf_counter() - station_started) * 1000,
            )
        except Exception as exc:  # noqa: BLE001 - log the component and continue with forecast
            message = _safe_message(exc)
            result["errors"].append(message)
            _log_finish(identifier, "error", rows_written, message)
            record_source_result(
                "ecowitt",
                success=False,
                rows_received=rows_written,
                latency_ms=(perf_counter() - station_started) * 1000,
                error=message,
            )

    if dpc_radar_is_due(cfg, force_forecast):
        identifier, _ = _log_start("dpc_radar_local")
        try:
            radar = refresh_dpc_radar(cfg)
            result["radar"] = {
                "rows": int(radar is not None),
                "observed_at": None if radar is None else radar.observed_at,
                "lightning_50km": 0 if radar is None else radar.lightning_50km,
            }
            if radar is not None:
                set_meta(
                    "last_dpc_radar_success",
                    pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%dT%H:%M:%SZ"),
                )
            _log_finish(identifier, "success", int(radar is not None))
        except Exception as exc:  # noqa: BLE001 - the optional DPC feed is isolated
            message = _safe_message(exc)
            result["radar"] = {"rows": 0, "warning": message}
            _log_finish(identifier, "warning", 0, message)
    else:
        result["radar"] = {"skipped": True, "reason": "aggiornamento non dovuto"}

    if official_observations_are_due(cfg, force_forecast):
        identifier, _ = _log_start("official_observations")
        try:
            official = ingest_official_observations(cfg)
            result["official"] = official
            status = "success" if not official["warnings"] else "warning"
            _log_finish(
                identifier,
                status,
                official["rows"],
                "; ".join(official["warnings"]),
            )
            if official["rows"]:
                now = pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%dT%H:%M:%SZ")
                set_meta("last_official_observation_success", now)
                set_meta(
                    "official_observation_stations", json.dumps(official["stations"])
                )
        except Exception as exc:  # noqa: BLE001 - official feeds never stop Ecowitt
            message = _safe_message(exc)
            result["official"] = {"rows": 0, "stations": [], "warnings": [message]}
            _log_finish(identifier, "warning", 0, message)
    else:
        result["official"] = {
            "skipped": True,
            "reason": (
                "rete ufficiale disattivata"
                if not cfg.official_observations_enabled
                else "aggiornamento non ancora dovuto"
            ),
        }

    if forecast_is_due(cfg, force_forecast):
        identifier, _ = _log_start("forecast")
        forecast_started = perf_counter()
        try:
            forecast = run_forecast_pipeline(cfg)
            result["forecast"] = forecast
            _log_finish(
                identifier,
                "success",
                forecast["blend_rows"],
                "; ".join(forecast["warnings"]),
            )
            record_source_result(
                "forecast_blend",
                success=True,
                rows_received=forecast["blend_rows"],
                last_observation_at=get_meta("last_forecast_issued_at"),
                latency_ms=(perf_counter() - forecast_started) * 1000,
            )
        except Exception as exc:  # noqa: BLE001 - log the component and keep station result
            message = _safe_message(exc)
            result["errors"].append(message)
            _log_finish(identifier, "error", 0, message)
            record_source_result(
                "forecast_blend",
                success=False,
                latency_ms=(perf_counter() - forecast_started) * 1000,
                error=message,
            )
    else:
        result["forecast"] = {
            "skipped": True,
            "reason": "aggiornamento non ancora dovuto",
        }

    set_meta(
        "last_pipeline_run", pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%dT%H:%M:%SZ")
    )
    prune_derived_history()
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Pipeline Meteo V4.3")
    parser.add_argument("--backfill-hours", type=int, default=None)
    parser.add_argument("--skip-station", action="store_true")
    parser.add_argument("--force-forecast", action="store_true")
    parser.add_argument(
        "--max-station-age-minutes",
        type=int,
        default=Settings.from_env().station_max_source_age_minutes,
        help="fallisce se Ecowitt non restituisce un campione abbastanza recente",
    )
    args = parser.parse_args()

    lock = PipelineLock()
    if not lock.acquire():
        log.warning("Un'altra pipeline è già in esecuzione")
        return 0
    try:
        result = run_all(
            backfill_hours=args.backfill_hours,
            skip_station=args.skip_station,
            force_forecast=args.force_forecast,
            max_station_age_minutes=args.max_station_age_minutes,
        )
    finally:
        lock.release()

    station = result.get("station")
    official = result.get("official")
    forecast = result.get("forecast")
    if station:
        log.info(
            "Stazione: %s righe; ultimo dato %s",
            station["rows"],
            station["latest_station_time"],
        )
    if official and not official.get("skipped"):
        log.info(
            "Rete ufficiale: %s righe; stazioni %s",
            official["rows"],
            ",".join(official["stations"]) or "nessuna",
        )
    if forecast and not forecast.get("skipped"):
        log.info(
            "Previsioni: %s archiviate, %s ore combinate, %s punteggi",
            forecast["archived"],
            forecast["blend_rows"],
            forecast["score_rows"],
        )
    for error in result["errors"]:
        log.error("%s", error)
    return 1 if result["errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
