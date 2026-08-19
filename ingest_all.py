"""Single scheduled entry point for station data and multi-provider forecasts."""

from __future__ import annotations

import argparse
import json
import logging
import os
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text

from config import Settings
from db import ensure_schema, get_engine, get_meta, set_meta
from forecast_blend import archive_forecast, build_blend, score_forecasts
from forecast_providers import fetch_all_forecasts
from weather_ingest_ecowitt_cloud import run_station_ingest

load_dotenv()
logging.basicConfig(
    level=getattr(logging, (os.getenv("LOG_LEVEL") or "INFO").upper(), logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("ingest_all")


class FileLock:
    def __init__(self, stale_seconds: int = 1800):
        self.path = Path(tempfile.gettempdir()) / "weather_app_v3_ingest.lock"
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


def run_forecast_pipeline(cfg: Settings) -> dict[str, Any]:
    frames, warnings = fetch_all_forecasts(cfg)
    if not frames:
        raise RuntimeError("Nessun provider di previsione ha restituito dati")
    archived = sum(archive_forecast(frame) for frame in frames)
    try:
        scores = score_forecasts(cfg)
        score_rows = len(scores)
    except Exception as exc:  # noqa: BLE001 - scoring must not discard a valid forecast
        warnings.append(f"Verifica errori rinviata: {_safe_message(exc)}")
        score_rows = 0
    blend = build_blend()
    if blend.empty:
        raise RuntimeError("Previsioni archiviate ma combinazione finale vuota")
    now = pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%dT%H:%M:%SZ")
    set_meta("last_forecast_success", now)
    set_meta("last_forecast_issued_at", blend["issued_at"].max())
    set_meta(
        "forecast_providers",
        json.dumps(sorted({str(frame.iloc[0]["provider"]) for frame in frames})),
    )
    return {
        "archived": archived,
        "blend_rows": len(blend),
        "score_rows": score_rows,
        "warnings": warnings,
    }


def prune_derived_history() -> None:
    """Bound database growth while retaining more history than scoring needs."""
    now = pd.Timestamp.now(tz="UTC")
    forecast_cutoff = (now - pd.Timedelta(days=120)).strftime("%Y-%m-%dT%H:%M:%SZ")
    score_cutoff = (now - pd.Timedelta(days=180)).strftime("%Y-%m-%dT%H:%M:%SZ")
    log_cutoff = (now - pd.Timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
    with get_engine().begin() as connection:
        connection.execute(
            text("DELETE FROM forecast_runs WHERE issued_at < :cutoff"),
            {"cutoff": forecast_cutoff},
        )
        connection.execute(
            text("DELETE FROM forecast_scores WHERE evaluated_at < :cutoff"),
            {"cutoff": score_cutoff},
        )
        connection.execute(
            text("DELETE FROM ingest_log WHERE started_at < :cutoff"),
            {"cutoff": log_cutoff},
        )


def run_all(
    *,
    backfill_hours: int | None = None,
    skip_station: bool = False,
    force_forecast: bool = False,
) -> dict[str, Any]:
    ensure_schema()
    cfg = Settings.from_env()
    result: dict[str, Any] = {"station": None, "forecast": None, "errors": []}

    if not skip_station:
        identifier, _ = _log_start("station")
        try:
            station = run_station_ingest(backfill_hours, cfg)
            result["station"] = station
            _log_finish(
                identifier, "success", station["rows"], "; ".join(station["warnings"])
            )
        except Exception as exc:  # noqa: BLE001 - log the component and continue with forecast
            message = _safe_message(exc)
            result["errors"].append(message)
            _log_finish(identifier, "error", 0, message)

    if forecast_is_due(cfg, force_forecast):
        identifier, _ = _log_start("forecast")
        try:
            forecast = run_forecast_pipeline(cfg)
            result["forecast"] = forecast
            _log_finish(
                identifier,
                "success",
                forecast["blend_rows"],
                "; ".join(forecast["warnings"]),
            )
        except Exception as exc:  # noqa: BLE001 - log the component and keep station result
            message = _safe_message(exc)
            result["errors"].append(message)
            _log_finish(identifier, "error", 0, message)
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
    parser = argparse.ArgumentParser(description="Pipeline Meteo V3")
    parser.add_argument("--backfill-hours", type=int, default=None)
    parser.add_argument("--skip-station", action="store_true")
    parser.add_argument("--force-forecast", action="store_true")
    args = parser.parse_args()

    lock = FileLock()
    if not lock.acquire():
        log.warning("Un'altra pipeline è già in esecuzione")
        return 0
    try:
        result = run_all(
            backfill_hours=args.backfill_hours,
            skip_station=args.skip_station,
            force_forecast=args.force_forecast,
        )
    finally:
        lock.release()

    station = result.get("station")
    forecast = result.get("forecast")
    if station:
        log.info(
            "Stazione: %s righe; ultimo dato %s",
            station["rows"],
            station["latest_station_time"],
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
