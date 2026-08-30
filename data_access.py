"""Read-only, cached-friendly data access for the dashboard."""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from config import Settings, settings
from db import ensure_schema, get_engine
from forecast_quality import enforce_physical_bounds
from rain_consistency import reportable_rain_series
from source_health import configured_sources


def _read(query: str, params: dict[str, Any] | None = None) -> pd.DataFrame:
    try:
        ensure_schema()
        with get_engine().connect() as connection:
            frame = pd.read_sql(text(query), connection, params=params or {})
    except SQLAlchemyError:
        return pd.DataFrame()
    frame.columns = [column.lower() for column in frame.columns]
    return frame


def load_station(hours: int = 240, station_id: str | None = None) -> pd.DataFrame:
    cutoff = (pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=hours)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    if station_id:
        frame = _read(
            "SELECT * FROM station_observations WHERE station_id=:station_id "
            "AND time >= :cutoff ORDER BY time",
            {"station_id": station_id, "cutoff": cutoff},
        )
        if frame.empty and station_id == settings.station_id:
            frame = _read(
                "SELECT * FROM station_raw WHERE time >= :cutoff ORDER BY time",
                {"cutoff": cutoff},
            )
    else:
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
    for column in ("rain_mm", "rain_rate_mm_h", "rain_total_mm"):
        if column in frame:
            frame[column] = frame[column].clip(lower=0)
    # V1/V2 stored rain rate in Rain_mm. Never present it as an amount.
    if "source" in frame and "rain_mm" in frame:
        legacy = frame["source"].isna() | frame["source"].astype(
            "string"
        ).str.strip().eq("")
        frame.loc[legacy, "rain_mm"] = np.nan
        if "data_quality" in frame:
            frame.loc[legacy, "data_quality"] = "legacy_unknown_rain"
    return frame.dropna(subset=["time"]).sort_values("time")


def load_station_profiles() -> pd.DataFrame:
    """Return public profile labels only; exact coordinates remain private."""
    frame = _read(
        "SELECT station_id,display_name,timezone,source,role,enabled,updated_at "
        "FROM station_profiles WHERE enabled=1 ORDER BY role,display_name"
    )
    if frame.empty:
        return frame
    frame["updated_at"] = pd.to_datetime(
        frame.get("updated_at"), utc=True, errors="coerce"
    )
    return frame


def load_station_month(
    station_id: str, year: int, month: int, timezone: str
) -> pd.DataFrame:
    start = pd.Timestamp(year=int(year), month=int(month), day=1, tz=timezone)
    end = start + pd.offsets.MonthBegin(1)
    params = {
        "station_id": station_id,
        "start": start.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ"),
        "end": end.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    frame = _read(
        "SELECT * FROM station_observations WHERE station_id=:station_id "
        "AND time>=:start AND time<:end ORDER BY time",
        params,
    )
    if frame.empty and station_id == settings.station_id:
        frame = _read(
            "SELECT * FROM station_raw WHERE time>=:start AND time<:end ORDER BY time",
            params,
        )
    if frame.empty:
        return frame
    frame["time"] = pd.to_datetime(frame["time"], utc=True, errors="coerce")
    for column in (
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
    ):
        if column in frame:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame.dropna(subset=["time"]).sort_values("time")


def available_station_months(station_id: str, timezone: str) -> list[tuple[int, int]]:
    frame = _read(
        "SELECT time FROM station_observations WHERE station_id=:station_id ORDER BY time",
        {"station_id": station_id},
    )
    if frame.empty and station_id == settings.station_id:
        frame = _read("SELECT time FROM station_raw ORDER BY time")
    if frame.empty:
        return []
    times = pd.to_datetime(frame["time"], utc=True, errors="coerce").dropna()
    local = times.dt.tz_convert(timezone)
    return sorted(
        set(zip(local.dt.year.astype(int), local.dt.month.astype(int))), reverse=True
    )


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
    return enforce_physical_bounds(frame)


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
    archived_tail = _read(
        "SELECT history.* FROM forecast_blend_history history JOIN ("
        " SELECT valid_time,MAX(issued_at) AS issued_at FROM forecast_blend_history"
        " WHERE valid_time BETWEEN :start AND :now AND issued_at<=valid_time"
        " GROUP BY valid_time"
        ") latest ON history.valid_time=latest.valid_time "
        "AND history.issued_at=latest.issued_at ORDER BY history.valid_time",
        {
            "start": start,
            "now": pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%dT%H:%M:%SZ"),
        },
    )
    frame["chart_origin"] = "blend_corrente"
    if not archived_tail.empty:
        archived_tail["chart_origin"] = "previsione_archiviata"
        # A fresh blend can retain the previous valid hour even though it was
        # issued after that hour and some providers already return nulls for it.
        # For elapsed times the archived emission is the truthful comparison:
        # it was available no later than the instant it forecast.  Append it
        # last so it wins an overlapping valid_time instead of being replaced
        # by a retrospective/null row from the current blend.
        frame = (
            pd.concat([frame, archived_tail], ignore_index=True)
            .drop_duplicates("valid_time", keep="last")
            .sort_values("valid_time")
            .reset_index(drop=True)
        )
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
        "cape_j_kg",
        "freezing_level_m",
        "wind_300hpa_kmh",
        "humidity_700hpa",
        "geopotential_500hpa_m",
        "temperature_850hpa_c",
        "is_day",
        "temp_uncertainty_c",
        "confidence",
        "provider_count",
    ]
    for column in numeric:
        if column in frame:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return enforce_physical_bounds(
        frame.dropna(subset=["valid_time"]).sort_values("valid_time")
    )


def load_forecast_history(hours: int = 48, emissions: int = 2) -> pd.DataFrame:
    """Load matching future hours for the newest archived blend emissions."""
    emissions = max(2, min(int(emissions), 12))
    start = pd.Timestamp.now(tz="UTC").floor("h").strftime("%Y-%m-%dT%H:%M:%SZ")
    end = (pd.Timestamp.now(tz="UTC") + pd.Timedelta(hours=hours)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    frame = _read(
        "SELECT * FROM forecast_blend_history WHERE issued_at IN ("
        " SELECT DISTINCT issued_at FROM forecast_blend_history "
        f" ORDER BY issued_at DESC LIMIT {emissions}"
        ") AND valid_time BETWEEN :start AND :end ORDER BY issued_at,valid_time",
        {"start": start, "end": end},
    )
    if frame.empty:
        return frame
    for column in ("issued_at", "valid_time"):
        frame[column] = pd.to_datetime(frame[column], utc=True, errors="coerce")
    for column in (
        "temp_c",
        "humidity",
        "pressure_hpa",
        "wind_kmh",
        "wind_gust_kmh",
        "rain_mm",
        "precip_probability",
        "confidence",
        "cape_j_kg",
        "freezing_level_m",
        "wind_300hpa_kmh",
        "humidity_700hpa",
        "geopotential_500hpa_m",
        "temperature_850hpa_c",
    ):
        if column in frame:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame.dropna(subset=["issued_at", "valid_time"])


def load_ensemble(hours: int = 192) -> pd.DataFrame:
    """Return long-form quantiles from the latest probabilistic emission."""
    start = pd.Timestamp.now(tz="UTC").floor("h").strftime("%Y-%m-%dT%H:%M:%SZ")
    end = (pd.Timestamp.now(tz="UTC") + pd.Timedelta(hours=hours)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    frame = _read(
        "SELECT * FROM forecast_ensemble_runs WHERE issued_at = "
        "(SELECT MAX(issued_at) FROM forecast_ensemble_runs) "
        "AND valid_time BETWEEN :start AND :end ORDER BY valid_time,variable",
        {"start": start, "end": end},
    )
    if frame.empty:
        return frame
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
    return frame.dropna(subset=["valid_time", "variable"])


def load_observed_air() -> pd.DataFrame:
    """Load the latest real EEA/Italian measurement for every pollutant."""
    frame = _read(
        "SELECT e.* FROM environment_observations e JOIN ("
        " SELECT source,metric,MAX(time) AS time FROM environment_observations "
        " WHERE source='eea_utd_air' GROUP BY source,metric"
        ") latest ON e.source=latest.source AND e.metric=latest.metric "
        "AND e.time=latest.time ORDER BY e.metric"
    )
    if frame.empty:
        return frame
    for column in ("time", "fetched_at"):
        frame[column] = pd.to_datetime(frame[column], utc=True, errors="coerce")
    for column in ("value", "latitude", "longitude", "distance_km", "is_modelled"):
        frame[column] = pd.to_numeric(frame.get(column), errors="coerce")
    return frame.dropna(subset=["time", "metric", "value"])


def load_measured_pollen() -> pd.DataFrame:
    """Load every botanical family from the latest POLLnet measurement date."""
    frame = _read(
        "SELECT * FROM environment_observations WHERE source='pollnet' "
        "AND time=(SELECT MAX(time) FROM environment_observations "
        "WHERE source='pollnet') ORDER BY value DESC,metric"
    )
    if frame.empty:
        return frame
    for column in ("time", "fetched_at"):
        frame[column] = pd.to_datetime(frame[column], utc=True, errors="coerce")
    for column in ("value", "latitude", "longitude", "distance_km"):
        frame[column] = pd.to_numeric(frame.get(column), errors="coerce")
    frame["family"] = (
        frame["metric"]
        .astype("string")
        .str.removeprefix("pollen_")
        .str.replace("_", " ", regex=False)
        .str.title()
    )
    return frame.dropna(subset=["time", "metric", "value"])


def load_climate_normals(source: str = "ecowitt_local") -> pd.DataFrame:
    """Load the current local month/hour baseline."""
    frame = _read(
        "SELECT * FROM climate_normals WHERE source=:source ORDER BY month,hour,metric",
        {"source": source},
    )
    if frame.empty:
        return frame
    frame["updated_at"] = pd.to_datetime(frame["updated_at"], utc=True, errors="coerce")
    for column in ("month", "day", "hour", "p10", "p50", "p90", "sample_years"):
        frame[column] = pd.to_numeric(frame.get(column), errors="coerce")
    return frame.dropna(subset=["month", "hour", "metric", "p50"])


def load_reference_climate_normals(
    station_id: str | None = None,
    source: str = "copernicus_era5_land",
) -> pd.DataFrame:
    frame = _read(
        "SELECT * FROM climate_reference_normals WHERE station_id=:station_id "
        "AND source=:source ORDER BY month,metric",
        {"station_id": station_id or settings.station_id, "source": source},
    )
    if frame.empty:
        return frame
    frame["updated_at"] = pd.to_datetime(
        frame.get("updated_at"), utc=True, errors="coerce"
    )
    for column in ("period_start", "period_end", "month", "value", "sample_years"):
        frame[column] = pd.to_numeric(frame.get(column), errors="coerce")
    return frame.dropna(subset=["month", "metric", "value"])


def load_latest_dpc_radar(station_id: str | None = None) -> pd.DataFrame:
    frame = _read(
        "SELECT * FROM radar_local_snapshots WHERE station_id=:station_id "
        "ORDER BY observed_at DESC LIMIT 1",
        {"station_id": station_id or settings.station_id},
    )
    if frame.empty:
        return frame
    for column in (
        "observed_at",
        "sri_observed_at",
        "vmi_observed_at",
        "lightning_observed_at",
        "fetched_at",
    ):
        frame[column] = pd.to_datetime(frame.get(column), utc=True, errors="coerce")
    for column in (
        "sri_point_mm_h",
        "sri_mean_mm_h",
        "sri_max_mm_h",
        "sri_echo_fraction",
        "vmi_point_dbz",
        "vmi_max_dbz",
        "lightning_10km",
        "lightning_25km",
        "lightning_50km",
        "nearest_lightning_km",
    ):
        if column in frame:
            frame[column] = pd.to_numeric(frame.get(column), errors="coerce")
    return frame


def load_official_alerts(days: int = 45) -> pd.DataFrame:
    """Load recent institutional bulletins, newest first."""
    days = max(2, min(int(days), 180))
    cutoff = (pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=days)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    frame = _read(
        "SELECT * FROM official_alerts WHERE issued_at>=:cutoff "
        "ORDER BY issued_at DESC LIMIT 30",
        {"cutoff": cutoff},
    )
    if frame.empty:
        return frame
    for column in ("issued_at", "starts_at", "ends_at", "fetched_at"):
        frame[column] = pd.to_datetime(frame[column], utc=True, errors="coerce")
    return frame.dropna(subset=["issued_at", "title", "source_url"])


def load_provider_scores() -> pd.DataFrame:
    frame = _read(
        "SELECT * FROM forecast_scores WHERE evaluated_at = "
        "(SELECT MAX(evaluated_at) FROM forecast_scores) ORDER BY variable,horizon,mae"
    )
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


def load_regime_scores() -> pd.DataFrame:
    frame = _read(
        "SELECT * FROM forecast_regime_scores WHERE evaluated_at = "
        "(SELECT MAX(evaluated_at) FROM forecast_regime_scores) "
        "ORDER BY variable,horizon,regime,mae"
    )
    for column in ("n", "bias", "mae", "rmse", "brier"):
        if column in frame:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame


def load_forecast_reliability() -> pd.DataFrame:
    frame = _read(
        "SELECT * FROM forecast_reliability WHERE evaluated_at = "
        "(SELECT MAX(evaluated_at) FROM forecast_reliability) "
        "ORDER BY provider,horizon,probability_bin"
    )
    for column in (
        "probability_bin",
        "n",
        "mean_probability",
        "observed_frequency",
        "brier",
    ):
        if column in frame:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame


def load_reference_scores() -> pd.DataFrame:
    frame = _read(
        "SELECT * FROM forecast_reference_scores WHERE evaluated_at = "
        "(SELECT MAX(evaluated_at) FROM forecast_reference_scores) "
        "ORDER BY variable,horizon,provider,station_id"
    )
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


def load_source_health(cfg: Settings = settings) -> pd.DataFrame:
    """Return every configured source, including sources not attempted yet."""
    current = _read("SELECT * FROM source_health ORDER BY source")
    if "source" not in current:
        current = pd.DataFrame(
            columns=[
                "source",
                "last_attempt_at",
                "last_success_at",
                "last_observation_at",
                "status",
                "rows_received",
                "latency_ms",
                "consecutive_failures",
                "last_error",
            ]
        )
    catalog = pd.DataFrame(
        [
            {
                "source": item.source,
                "label": item.label,
                "enabled": item.enabled,
                "expected_minutes": item.expected_minutes,
                "category": item.category,
                "cache_minutes": item.cache_minutes,
                "continuity": item.continuity,
            }
            for item in configured_sources(cfg)
        ]
    )
    frame = catalog.merge(current, on="source", how="left")
    for column in ("last_attempt_at", "last_success_at", "last_observation_at"):
        frame[column] = pd.to_datetime(frame.get(column), utc=True, errors="coerce")
    for column in ("rows_received", "latency_ms", "consecutive_failures"):
        frame[column] = pd.to_numeric(frame.get(column), errors="coerce").fillna(0)
    frame["last_error"] = frame.get("last_error", "").fillna("").astype(str)
    now = pd.Timestamp.now(tz="UTC")
    frame["age_minutes"] = frame["last_success_at"].map(
        lambda value: _age_minutes(now, value)
    )
    frame["observation_age_minutes"] = frame["last_observation_at"].map(
        lambda value: max(0.0, _age_minutes(now, value))
    )

    def state(row: pd.Series) -> str:
        raw_status = row.get("status")
        stored_status = "" if pd.isna(raw_status) else str(raw_status).strip().lower()
        has_telemetry = bool(stored_status) or pd.notna(row["last_attempt_at"])
        source = str(row.get("source"))
        cache_minutes = max(0.0, float(row.get("cache_minutes") or 0.0))
        raw_observation_age = row.get("observation_age_minutes")
        observation_age = (
            float(raw_observation_age)
            if pd.notna(raw_observation_age)
            else float("inf")
        )
        cache_available = cache_minutes > 0 and observation_age <= cache_minutes
        # Health and backups are GitHub-scheduled processes. Before their first
        # run, describe the plan rather than showing a false outage.
        if (
            source in {"system_health", "database_backup", "github_backup"}
            and not has_telemetry
        ):
            return "scheduled"
        # SIARL is an optional connector. When explicitly suspended, do not
        # keep showing historical portal failures while the operational CFR
        # Lazio reference continues to run.
        if source == "arsial_siarl" and not bool(row["enabled"]):
            return "disabled"
        # The dashboard and the Render Cron Job can intentionally have
        # different environment variables. Once the ingest process has
        # reported telemetry, trust that shared database state instead of
        # calling a live source disabled merely because the web process does
        # not hold its API key.
        if stored_status == "disabled":
            return "disabled"
        if not bool(row["enabled"]) and not has_telemetry:
            return "disabled"
        if stored_status == "cached":
            if cache_available:
                return "cached"
            return "external_unavailable" if source == "arsial_siarl" else "offline"
        failures = int(row["consecutive_failures"])
        if failures >= 2:
            if cache_available:
                return "cached"
            # ARSIAL is an optional institutional cross-check. A portal outage
            # must remain visible without looking like a failure of Ecowitt or
            # of the forecast pipeline itself.
            if source == "arsial_siarl":
                return "external_unavailable"
            return "offline"
        if failures == 1:
            return "delayed"
        if pd.isna(row["last_success_at"]):
            return "waiting"
        age = float(row["age_minutes"])
        expected = max(1.0, float(row["expected_minutes"]))
        if age <= expected * 1.75:
            return "online"
        if age <= expected * 3.5:
            return "delayed"
        if cache_available:
            return "cached"
        return "offline"

    frame["display_status"] = frame.apply(state, axis=1)
    return frame


def data_completeness_snapshot(hours: int = 24) -> dict[str, Any]:
    """Calculate five-minute coverage and anomaly counts without DB-specific SQL."""
    hours = max(1, min(int(hours), 168))
    cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=hours)
    frame = _read(
        "SELECT time,data_quality FROM station_raw WHERE time >= :cutoff ORDER BY time",
        {"cutoff": cutoff.strftime("%Y-%m-%dT%H:%M:%SZ")},
    )
    expected = hours * 12
    if frame.empty:
        return {
            "hours": hours,
            "expected": expected,
            "observed": 0,
            "coverage": 0.0,
            "largest_gap_minutes": None,
            "anomalies": 0,
        }
    times = pd.to_datetime(frame["time"], utc=True, errors="coerce").dropna()
    buckets = times.dt.floor("5min").drop_duplicates()
    gaps = times.sort_values().diff().dt.total_seconds().div(60).dropna()
    quality = frame.get("data_quality", pd.Series(dtype="string")).fillna("ok")
    anomalies = int((~quality.astype(str).isin({"ok", "estimated_rain"})).sum())
    return {
        "hours": hours,
        "expected": expected,
        "observed": len(buckets),
        "coverage": min(100.0, len(buckets) / expected * 100.0),
        "largest_gap_minutes": float(gaps.max()) if not gaps.empty else 0.0,
        "anomalies": anomalies,
    }


def load_official_station_status() -> pd.DataFrame:
    frame = _read(
        "SELECT o.* FROM official_observations o JOIN ("
        " SELECT source,station_id,MAX(time) AS time FROM official_observations "
        " GROUP BY source,station_id"
        ") latest ON o.source=latest.source AND o.station_id=latest.station_id "
        "AND o.time=latest.time ORDER BY o.distance_km"
    )
    if frame.empty:
        return frame
    frame["time"] = pd.to_datetime(frame["time"], utc=True, errors="coerce")
    for column in ("distance_km", "temp_c", "humidity", "pressure_hpa", "wind_kmh"):
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
        "SELECT MAX(time) AS station_time, "
        "MAX(CASE WHEN temp_c IS NOT NULL THEN time END) AS temperature_time, "
        "MAX(CASE WHEN humidity IS NOT NULL THEN time END) AS humidity_time, "
        "MAX(CASE WHEN pressure_hpa IS NOT NULL THEN time END) AS pressure_time, "
        "MAX(CASE WHEN wind_kmh IS NOT NULL THEN time END) AS wind_time, "
        "MAX(CASE WHEN rain_mm IS NOT NULL OR rain_rate_mm_h IS NOT NULL "
        "THEN time END) AS rain_time, "
        "MAX(CASE WHEN solar_w_m2 IS NOT NULL OR uv_index IS NOT NULL "
        "THEN time END) AS solar_time "
        "FROM station_raw"
    )
    blend_frame = _read(
        "SELECT MAX(issued_at) AS forecast_issued, "
        "MAX(valid_time) AS forecast_until FROM forecast_blend"
    )
    now = pd.Timestamp.now(tz="UTC")

    station_time = _timestamp_from_frame(station_frame, "station_time")
    measurement_times = {
        "temperature": _metric_timestamp(
            station_frame, "temperature_time", station_time
        ),
        "humidity": _metric_timestamp(station_frame, "humidity_time", station_time),
        "pressure": _metric_timestamp(station_frame, "pressure_time", station_time),
        "wind": _metric_timestamp(station_frame, "wind_time", station_time),
        "rain": _metric_timestamp(station_frame, "rain_time", station_time),
        "solar": _metric_timestamp(station_frame, "solar_time", station_time),
    }
    forecast_issued = _timestamp_from_frame(blend_frame, "forecast_issued")
    forecast_until = _timestamp_from_frame(blend_frame, "forecast_until")

    # Query the legacy table only when V3 has no usable timestamps. Keeping these
    # reads independent prevents one incompatible legacy table from hiding the
    # healthy station and V3 forecast status.
    if pd.isna(forecast_issued) or pd.isna(forecast_until):
        legacy_frame = _read("SELECT MAX(time) AS legacy_time FROM forecast_ow")
        legacy_time = _timestamp_from_frame(legacy_frame, "legacy_time")
        if pd.isna(forecast_issued):
            forecast_issued = legacy_time
        if pd.isna(forecast_until):
            forecast_until = legacy_time
    measurement_stale_minutes = min(cfg.station_stale_minutes, 30)
    measurement_freshness = {
        name: {
            "time": timestamp,
            "age_minutes": _age_minutes(now, timestamp),
            "status": _freshness_status(
                _age_minutes(now, timestamp), measurement_stale_minutes
            ),
        }
        for name, timestamp in measurement_times.items()
    }
    core_measurements = ("temperature", "humidity", "pressure", "wind")
    station_age = max(
        (measurement_freshness[name]["age_minutes"] for name in core_measurements),
        default=float("inf"),
    )
    station_sample_age = _age_minutes(now, station_time)
    forecast_age = (
        (now - forecast_issued).total_seconds() / 60
        if not pd.isna(forecast_issued)
        else float("inf")
    )
    status_rank = {"online": 0, "delayed": 1, "offline": 2}
    station_status = max(
        (measurement_freshness[name]["status"] for name in core_measurements),
        key=status_rank.get,
        default="offline",
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
        "station_sample_age_minutes": station_sample_age,
        "forecast_age_minutes": forecast_age,
        "measurement_stale_minutes": measurement_stale_minutes,
        "measurement_freshness": measurement_freshness,
    }


def _timestamp_from_frame(frame: pd.DataFrame, column: str) -> Any:
    if frame.empty or column not in frame:
        return pd.NaT
    return pd.to_datetime(frame.iloc[0].get(column), utc=True, errors="coerce")


def _metric_timestamp(
    frame: pd.DataFrame, column: str, legacy_fallback: pd.Timestamp
) -> pd.Timestamp:
    """Read a metric timestamp, preserving compatibility with older test/query data."""
    if column not in frame:
        return legacy_fallback
    return _timestamp_from_frame(frame, column)


def _age_minutes(now: pd.Timestamp, timestamp: Any) -> float:
    return (
        (now - timestamp).total_seconds() / 60
        if not pd.isna(timestamp)
        else float("inf")
    )


def _freshness_status(age_minutes: float, stale_minutes: int) -> str:
    if age_minutes <= stale_minutes:
        return "online"
    if age_minutes <= stale_minutes * 3:
        return "delayed"
    return "offline"


def daily_forecast(frame: pd.DataFrame, timezone_name: str) -> pd.DataFrame:
    if frame.empty:
        return frame
    data = frame.copy()
    data["local_time"] = data["valid_time"].dt.tz_convert(timezone_name)
    data["date"] = data["local_time"].dt.date
    if "rain_mm" in data:
        data["reportable_rain_mm"] = reportable_rain_series(data)
    aggregations: dict[str, tuple[str, str]] = {
        "temp_min": ("temp_c", "min"),
        "temp_max": ("temp_c", "max"),
        "rain_mm": ("reportable_rain_mm", "sum"),
        "pop_max": ("precip_probability", "max"),
        "humidity_mean": ("humidity", "mean"),
        "wind_mean": ("wind_kmh", "mean"),
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
