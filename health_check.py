"""Independent application and data-readiness health check.

Render already probes Streamlit's process endpoint.  This module adds the
second layer that a TCP/HTTP liveness check cannot provide: database access,
fresh Ecowitt measurements and a usable combined forecast.  Optional sources
remain visible as degraded without taking the whole application offline.
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import asdict, dataclass
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import pandas as pd
import requests

from config import Settings
from data_access import health_snapshot, load_source_health
from db import ensure_schema
from forecast_providers import build_session
from source_health import record_source_result


@dataclass(frozen=True)
class HealthReport:
    status: str
    critical_failures: tuple[str, ...]
    warnings: tuple[str, ...]
    checked_sources: int
    app_reachable: bool | None = None

    @property
    def exit_code(self) -> int:
        return 1 if self.status == "unhealthy" else 0


def _status(value: Any) -> str:
    return str(value or "offline").strip().lower()


def evaluate_health(
    snapshot: dict[str, Any],
    sources: pd.DataFrame,
    *,
    app_reachable: bool | None = None,
    app_error: str = "",
) -> HealthReport:
    """Classify core failures separately from isolated optional-source issues."""
    failures: list[str] = []
    warnings: list[str] = []

    station_status = _status(snapshot.get("station_status"))
    forecast_status = _status(snapshot.get("forecast_status"))
    if station_status == "offline":
        failures.append("misure Ecowitt non aggiornate")
    elif station_status == "delayed":
        warnings.append("misure Ecowitt in ritardo")
    if forecast_status == "offline":
        failures.append("previsione combinata non aggiornata")
    elif forecast_status == "delayed":
        warnings.append("previsione combinata in ritardo")
    if app_reachable is False:
        failures.append(app_error or "endpoint web non raggiungibile")

    ignored = {"ecowitt", "forecast_blend", "system_health"}
    if not sources.empty:
        for row in sources.to_dict("records"):
            source = str(row.get("source") or "")
            if source in ignored or not bool(row.get("enabled", True)):
                continue
            display_status = _status(row.get("display_status"))
            if display_status in {
                "cached",
                "delayed",
                "external_unavailable",
                "offline",
            }:
                label = str(row.get("label") or source)
                warnings.append(f"{label}: {display_status}")

    failures = list(dict.fromkeys(failures))
    warnings = list(dict.fromkeys(warnings))
    overall = "unhealthy" if failures else "degraded" if warnings else "healthy"
    return HealthReport(
        status=overall,
        critical_failures=tuple(failures),
        warnings=tuple(warnings),
        checked_sources=len(sources),
        app_reachable=app_reachable,
    )


def health_endpoint(app_url: str) -> str:
    """Return a safe Streamlit health URL without retaining query parameters."""
    parsed = urlsplit(app_url.strip())
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("APP_HEALTH_URL non valido")
    if parsed.path.rstrip("/").endswith("/_stcore/health"):
        path = parsed.path
    else:
        path = parsed.path.rstrip("/") + "/_stcore/health"
    return urlunsplit((parsed.scheme, parsed.netloc, path, "", ""))


def probe_app(
    app_url: str,
    session: requests.Session | None = None,
) -> tuple[bool, str]:
    """Probe Streamlit through retries; return a redacted diagnostic."""
    own_session = session is None
    session = session or build_session(retries=4)
    try:
        response = session.get(health_endpoint(app_url), timeout=(5, 45))
        if 200 <= response.status_code < 400:
            return True, ""
        return False, f"endpoint web HTTP {response.status_code}"
    except (requests.RequestException, ValueError) as exc:
        if isinstance(exc, ValueError):
            return False, str(exc)
        return False, "endpoint web non raggiungibile"
    finally:
        if own_session:
            session.close()


def run_check(app_url: str = "") -> HealthReport:
    cfg = Settings.from_env()
    ensure_schema()
    snapshot = health_snapshot(cfg)
    sources = load_source_health(cfg)
    app_reachable: bool | None = None
    app_error = ""
    if app_url.strip():
        app_reachable, app_error = probe_app(app_url)
    report = evaluate_health(
        snapshot,
        sources,
        app_reachable=app_reachable,
        app_error=app_error,
    )
    detail = "; ".join((*report.critical_failures, *report.warnings))[:500]
    record_source_result(
        "system_health",
        success=report.status != "unhealthy",
        rows_received=report.checked_sources,
        last_observation_at=pd.Timestamp.now(tz="UTC"),
        error=detail,
    )
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="Controllo salute Meteo V4.6")
    parser.add_argument(
        "--app-url",
        default=os.getenv("APP_HEALTH_URL") or os.getenv("APP_URL") or "",
        help="URL pubblico facoltativo della dashboard Render",
    )
    parser.add_argument(
        "--fail-on-degraded",
        action="store_true",
        help="considera bloccante anche una fonte opzionale in fallback",
    )
    args = parser.parse_args()
    try:
        report = run_check(args.app_url)
    except Exception as exc:  # noqa: BLE001 - the command must emit one safe result
        payload = {
            "status": "unhealthy",
            "critical_failures": [
                f"database o controllo non disponibile: {type(exc).__name__}"
            ],
            "warnings": [],
            "checked_sources": 0,
            "app_reachable": None,
        }
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        return 1
    print(json.dumps(asdict(report), ensure_ascii=False, sort_keys=True))
    if args.fail_on_degraded and report.status == "degraded":
        return 1
    return report.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
