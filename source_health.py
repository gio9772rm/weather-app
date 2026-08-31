"""Non-blocking operational health tracking for every independent data source."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from config import Settings, settings
from db import get_engine


@dataclass(frozen=True)
class SourceDefinition:
    source: str
    label: str
    enabled: bool
    expected_minutes: int
    category: str
    cache_minutes: int = 0
    continuity: str = "Nessun fallback configurato"


def configured_sources(cfg: Settings = settings) -> tuple[SourceDefinition, ...]:
    """Return the sources that the current deployment knows how to operate."""
    official = cfg.official_observations_enabled
    return (
        SourceDefinition(
            "ecowitt",
            "Ecowitt",
            cfg.has_station_credentials,
            5,
            "misure",
            continuity="Recupero automatico Ecowitt fino a 7 giorni",
        ),
        SourceDefinition(
            "open_meteo",
            "Open-Meteo",
            True,
            cfg.forecast_refresh_minutes,
            "previsioni",
            cache_minutes=12 * 60,
            continuity="Ultima emissione valida nel blend multi-provider",
        ),
        SourceDefinition(
            "open_meteo_icon2i",
            "ItaliaMeteo ICON-2I · 2,2 km",
            True,
            cfg.forecast_refresh_minutes,
            "previsioni",
            cache_minutes=12 * 60,
            continuity="Open-Meteo e OpenWeather restano alternativi",
        ),
        SourceDefinition(
            "openweather",
            "OpenWeather",
            bool(cfg.openweather_api_key),
            cfg.forecast_refresh_minutes,
            "previsioni",
            cache_minutes=12 * 60,
            continuity="Open-Meteo e ICON-2I restano alternativi",
        ),
        SourceDefinition(
            "open_meteo_ensemble",
            "Open-Meteo Ensemble",
            cfg.ensemble_forecast_enabled,
            cfg.forecast_refresh_minutes,
            "probabilistica",
            cache_minutes=12 * 60,
            continuity="Blend deterministico disponibile senza ensemble",
        ),
        SourceDefinition(
            "awc_metar",
            "METAR LIRF/LIRA",
            official,
            cfg.official_observation_refresh_minutes,
            "riferimenti",
            cache_minutes=6 * 60,
            continuity="Ecowitt primaria + CFR Lazio indipendente",
        ),
        SourceDefinition(
            "arsial_siarl",
            "ARSIAL/SIARL",
            cfg.arsial_polling_enabled,
            (
                cfg.arsial_probe_hours * 60
                if cfg.arsial_auto_probe
                else cfg.official_observation_refresh_minutes
            ),
            "riferimenti",
            cache_minutes=cfg.arsial_cache_hours * 60,
            continuity=(
                f"Verifica automatica ogni {cfg.arsial_probe_hours} h; "
                "CFR Lazio operativo; archivio SIARL se valido"
                if cfg.arsial_auto_probe
                else "CFR Lazio operativo; archivio SIARL se valido"
            ),
        ),
        SourceDefinition(
            "cfr_lazio",
            "CFR Lazio via MeteoHub",
            official and cfg.cfr_observations_enabled,
            15,
            "riferimenti",
            cache_minutes=6 * 60,
            continuity="METAR + ultimo dato CFR archiviato",
        ),
        SourceDefinition(
            "dpc_radar_local",
            "DPC · radar locale SRI/VMI",
            cfg.dpc_radar_enabled,
            cfg.dpc_radar_refresh_minutes,
            "misure",
            cache_minutes=30,
            continuity="Ultimo riassunto locale + RainViewer",
        ),
        SourceDefinition(
            "dpc_lightning_local",
            "DPC · fulmini entro 50 km",
            cfg.dpc_radar_enabled,
            cfg.dpc_radar_refresh_minutes,
            "sicurezza",
            cache_minutes=30,
            continuity="Mostra n/d se il frame ufficiale non esiste",
        ),
        SourceDefinition(
            "forecast_blend",
            "Previsione combinata",
            True,
            cfg.forecast_refresh_minutes,
            "elaborazione",
            cache_minutes=12 * 60,
            continuity="Conserva l'ultima previsione combinata valida",
        ),
        SourceDefinition(
            "eea_utd_air",
            "EEA UTD · aria osservata",
            cfg.eea_air_observations_enabled,
            max(60, cfg.forecast_refresh_minutes),
            "ambiente",
            cache_minutes=24 * 60,
            continuity="Previsione CAMS sempre distinta dalla misura",
        ),
        SourceDefinition(
            "pollnet",
            "POLLnet · pollini misurati",
            cfg.feature_measured_pollen_enabled,
            14 * 24 * 60,
            "ambiente",
            cache_minutes=30 * 24 * 60,
            continuity="CAMS orientativo se POLLnet è in validazione",
        ),
        SourceDefinition(
            "official_alerts",
            "DPC + Regione Lazio · bollettini",
            cfg.feature_official_alerts_enabled,
            24 * 60,
            "sicurezza",
            cache_minutes=7 * 24 * 60,
            continuity="Ultimi documenti DPC/Lazio archiviati con data",
        ),
        SourceDefinition(
            "climatology_local",
            "Baseline climatologica locale",
            cfg.feature_climatology_enabled,
            max(60, cfg.forecast_refresh_minutes),
            "elaborazione",
            cache_minutes=90 * 24 * 60,
            continuity="Ricalcolo dalla serie Ecowitt conservata",
        ),
        SourceDefinition(
            "climatology_era5_land",
            "Copernicus ERA5-Land · riferimento 1991–2020",
            cfg.reference_climatology_enabled,
            cfg.reference_climatology_refresh_days * 24 * 60,
            "elaborazione",
            cache_minutes=365 * 24 * 60,
            continuity="Baseline locale disponibile senza Copernicus",
        ),
        SourceDefinition(
            "system_health",
            "Controllo salute automatico",
            True,
            60,
            "protezione",
            cache_minutes=3 * 60,
            continuity="Render riavvia l'app; GitHub verifica dati e DB",
        ),
        SourceDefinition(
            "database_backup",
            "Backup generato e verificato",
            True,
            24 * 60,
            "protezione",
            continuity="ZIP portatile con manifest e checksum",
        ),
        SourceDefinition(
            "github_backup",
            "Backup cloud GitHub cifrato",
            True,
            24 * 60,
            "protezione",
            continuity="Artefatto cifrato con scadenza automatica dopo 30 giorni",
        ),
    )


def _iso(value: Any) -> str | None:
    timestamp = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(timestamp):
        return None
    return timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")


def _safe_text(value: Any, limit: int = 500) -> str:
    return " ".join(str(value or "").split())[:limit]


def record_source_result(
    source: str,
    *,
    success: bool,
    status: str | None = None,
    rows_received: int = 0,
    last_observation_at: Any = None,
    latency_ms: float | None = None,
    error: Any = "",
    engine: Engine | None = None,
) -> bool:
    """Upsert one health result; telemetry failure never stops the pipeline."""
    resolved_status = str(status or ("online" if success else "error")).lower()
    if resolved_status not in {"online", "error", "cached"}:
        resolved_status = "online" if success else "error"
    is_online = resolved_status == "online"
    attempted = _iso(pd.Timestamp.now(tz="UTC"))
    observed = _iso(last_observation_at)
    latency = (
        float(latency_ms)
        if latency_ms is not None and math.isfinite(float(latency_ms))
        else None
    )
    statement = text(
        "INSERT INTO source_health ("
        "source,last_attempt_at,last_success_at,last_observation_at,status,"
        "rows_received,latency_ms,consecutive_failures,last_error,updated_at"
        ") VALUES ("
        ":source,:attempted,:succeeded,:observed,:status,:rows,:latency,"
        ":failures,:error,:attempted) "
        "ON CONFLICT (source) DO UPDATE SET "
        "last_attempt_at=excluded.last_attempt_at,"
        "last_success_at=CASE WHEN excluded.status='online' "
        "THEN excluded.last_success_at ELSE source_health.last_success_at END,"
        "last_observation_at=COALESCE(excluded.last_observation_at,source_health.last_observation_at),"
        "status=excluded.status,rows_received=excluded.rows_received,"
        "latency_ms=excluded.latency_ms,"
        "consecutive_failures=CASE WHEN excluded.status='online' THEN 0 "
        "ELSE source_health.consecutive_failures+1 END,"
        "last_error=excluded.last_error,updated_at=excluded.updated_at"
    )
    payload = {
        "source": source,
        "attempted": attempted,
        "succeeded": attempted if is_online else None,
        "observed": observed,
        "status": resolved_status,
        "rows": max(0, int(rows_received or 0)),
        "latency": latency,
        "failures": 0 if is_online else 1,
        "error": "" if is_online else _safe_text(error or "errore sconosciuto"),
    }
    try:
        with (engine or get_engine()).begin() as connection:
            connection.execute(statement, payload)
    except SQLAlchemyError:
        return False
    return True


def record_source_disabled(source: str, engine: Engine | None = None) -> bool:
    now = _iso(pd.Timestamp.now(tz="UTC"))
    try:
        with (engine or get_engine()).begin() as connection:
            connection.execute(
                text(
                    "INSERT INTO source_health (source,status,rows_received,"
                    "consecutive_failures,last_error,updated_at) "
                    "VALUES (:source,'disabled',0,0,'',:now) "
                    "ON CONFLICT (source) DO UPDATE SET status='disabled',"
                    "consecutive_failures=0,last_error='',updated_at=:now"
                ),
                {"source": source, "now": now},
            )
    except SQLAlchemyError:
        return False
    return True
