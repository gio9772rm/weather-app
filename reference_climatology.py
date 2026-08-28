"""1991-2020 monthly reference based on Copernicus ERA5-Land reanalysis."""

from __future__ import annotations

import math
from time import perf_counter
from typing import Any

import pandas as pd
import requests
from sqlalchemy import text
from sqlalchemy.engine import Engine

from config import Settings, settings
from db import get_engine
from forecast_providers import build_session
from source_health import record_source_disabled, record_source_result
from station_registry import normalise_station_id

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
SOURCE = "copernicus_era5_land"
PERIOD_START = 1991
PERIOD_END = 2020
METRICS = {
    "temperature_2m_mean": ("temp_c_mean", "C"),
    "temperature_2m_min": ("temp_c_min", "C"),
    "temperature_2m_max": ("temp_c_max", "C"),
    "precipitation_sum": ("rain_mm", "mm"),
}


class ReferenceClimatologyError(RuntimeError):
    """The historical reanalysis endpoint failed safely."""


def parse_reference_payload(
    payload: dict[str, Any], *, updated_at: pd.Timestamp | None = None
) -> pd.DataFrame:
    daily = payload.get("daily") or {}
    times = daily.get("time") or []
    if not times:
        return pd.DataFrame()
    dates = pd.to_datetime(times, errors="coerce")
    base = pd.DataFrame({"date": dates}).dropna(subset=["date"])
    if base.empty:
        return pd.DataFrame()
    rows: list[dict[str, Any]] = []
    refreshed = pd.to_datetime(updated_at or pd.Timestamp.now(tz="UTC"), utc=True)
    for source_field, (metric, unit) in METRICS.items():
        values = daily.get(source_field)
        if not isinstance(values, list) or len(values) != len(times):
            continue
        series = pd.DataFrame(
            {
                "date": dates,
                "value": pd.to_numeric(pd.Series(values), errors="coerce"),
            }
        ).dropna()
        if series.empty:
            continue
        series["year"] = series["date"].dt.year
        series["month"] = series["date"].dt.month
        if metric == "rain_mm":
            annual_months = (
                series.groupby(["year", "month"])["value"]
                .sum(min_count=1)
                .reset_index()
            )
        else:
            annual_months = (
                series.groupby(["year", "month"])["value"].mean().reset_index()
            )
        for month, group in annual_months.groupby("month"):
            rows.append(
                {
                    "source": SOURCE,
                    "period_start": PERIOD_START,
                    "period_end": PERIOD_END,
                    "month": int(month),
                    "metric": metric,
                    "value": float(group["value"].mean()),
                    "unit": unit,
                    "sample_years": int(group["year"].nunique()),
                    "updated_at": refreshed,
                }
            )
    return pd.DataFrame(rows)


def fetch_reference_climatology(
    cfg: Settings = settings, session: requests.Session | None = None
) -> pd.DataFrame:
    own_session = session is None
    session = session or build_session(retries=2)
    try:
        try:
            response = session.get(
                ARCHIVE_URL,
                params={
                    "latitude": cfg.latitude,
                    "longitude": cfg.longitude,
                    "elevation": cfg.elevation_m,
                    "start_date": f"{PERIOD_START}-01-01",
                    "end_date": f"{PERIOD_END}-12-31",
                    "models": "era5_land",
                    "timezone": "UTC",
                    "daily": ",".join(METRICS),
                },
                timeout=(10, 90),
            )
        except requests.RequestException as exc:
            raise ReferenceClimatologyError(
                "ERA5-Land: servizio storico non raggiungibile"
            ) from exc
        if not response.ok:
            raise ReferenceClimatologyError(
                f"ERA5-Land: risposta HTTP {response.status_code}"
            )
        try:
            payload = response.json()
        except ValueError as exc:
            raise ReferenceClimatologyError("ERA5-Land: risposta non valida") from exc
        frame = parse_reference_payload(payload)
        if frame.empty or len(frame) < 36:
            raise ReferenceClimatologyError(
                "ERA5-Land: riferimento 1991-2020 incompleto"
            )
        return frame
    finally:
        if own_session:
            session.close()


def archive_reference_climatology(
    frame: pd.DataFrame,
    cfg: Settings = settings,
    engine: Engine | None = None,
) -> int:
    if frame is None or frame.empty:
        return 0
    engine = engine or get_engine()
    station_id = normalise_station_id(cfg.station_id)
    records: list[dict[str, Any]] = []
    for row in frame.to_dict("records"):
        value = pd.to_numeric(pd.Series([row.get("value")]), errors="coerce").iloc[0]
        if pd.isna(value) or not math.isfinite(float(value)):
            continue
        updated = pd.to_datetime(row.get("updated_at"), utc=True, errors="coerce")
        records.append(
            {
                "station_id": station_id,
                "source": str(row.get("source") or SOURCE),
                "period_start": int(row.get("period_start") or PERIOD_START),
                "period_end": int(row.get("period_end") or PERIOD_END),
                "month": int(row["month"]),
                "metric": str(row["metric"]),
                "value": float(value),
                "unit": str(row.get("unit") or ""),
                "sample_years": int(row.get("sample_years") or 0),
                "updated_at": updated.strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
        )
    if not records:
        return 0
    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO climate_reference_normals (station_id,source,period_start,"
                "period_end,month,metric,value,unit,sample_years,updated_at) VALUES ("
                ":station_id,:source,:period_start,:period_end,:month,:metric,:value,"
                ":unit,:sample_years,:updated_at) ON CONFLICT (station_id,source,"
                "period_start,period_end,month,metric) DO UPDATE SET value=excluded.value,"
                "unit=excluded.unit,sample_years=excluded.sample_years,"
                "updated_at=excluded.updated_at"
            ),
            records,
        )
    return len(records)


def reference_climatology_is_due(
    cfg: Settings = settings, engine: Engine | None = None
) -> bool:
    if not cfg.reference_climatology_enabled:
        return False
    engine = engine or get_engine()
    key = f"last_reference_climatology_{normalise_station_id(cfg.station_id)}"
    with engine.connect() as connection:
        value = connection.execute(
            text("SELECT v FROM meta WHERE k=:key"), {"key": key}
        ).scalar_one_or_none()
    last = pd.to_datetime(value, utc=True, errors="coerce")
    return pd.isna(last) or (
        pd.Timestamp.now(tz="UTC") - last
        >= pd.Timedelta(days=cfg.reference_climatology_refresh_days)
    )


def refresh_reference_climatology(
    cfg: Settings = settings, engine: Engine | None = None
) -> tuple[pd.DataFrame, str | None]:
    engine = engine or get_engine()
    if not cfg.reference_climatology_enabled:
        record_source_disabled("climatology_era5_land", engine)
        return pd.DataFrame(), None
    if not reference_climatology_is_due(cfg, engine):
        return pd.DataFrame(), None
    started = perf_counter()
    try:
        frame = fetch_reference_climatology(cfg)
        rows = archive_reference_climatology(frame, cfg, engine)
    except ReferenceClimatologyError as exc:
        record_source_result(
            "climatology_era5_land",
            success=False,
            latency_ms=(perf_counter() - started) * 1000,
            error=exc,
            engine=engine,
        )
        return pd.DataFrame(), str(exc)
    now = pd.Timestamp.now(tz="UTC")
    meta_key = f"last_reference_climatology_{normalise_station_id(cfg.station_id)}"
    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO meta (k,v) VALUES (:key,:value) "
                "ON CONFLICT (k) DO UPDATE SET v=excluded.v"
            ),
            {"key": meta_key, "value": now.strftime("%Y-%m-%dT%H:%M:%SZ")},
        )
    record_source_result(
        "climatology_era5_land",
        success=True,
        rows_received=rows,
        last_observation_at=pd.Timestamp(f"{PERIOD_END}-12-31", tz="UTC"),
        latency_ms=(perf_counter() - started) * 1000,
        engine=engine,
    )
    return frame, None
