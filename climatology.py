"""Local Ecowitt climatology and anomaly helpers.

The baseline is deliberately built from the user's station archive.  It never
changes the primary observation and it is labelled as a local baseline until
several complete years are available.
"""

from __future__ import annotations

from time import perf_counter
from typing import Any

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from config import Settings, settings
from db import ensure_schema, get_engine
from source_health import record_source_disabled, record_source_result

SOURCE = "ecowitt_local"
METRICS = {
    "temp_c": "temp_c",
    "humidity": "humidity",
    "pressure_hpa": "pressure_hpa",
    "wind_kmh": "wind_kmh",
}
NORMAL_COLUMNS = [
    "source",
    "month",
    "day",
    "hour",
    "metric",
    "p10",
    "p50",
    "p90",
    "sample_years",
    "updated_at",
]


class ClimatologyError(RuntimeError):
    """The local archive cannot currently produce a useful baseline."""


def _utc(value: Any) -> str:
    timestamp = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(timestamp):
        raise ValueError("timestamp climatologico non valido")
    return timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")


def build_local_normals(
    station: pd.DataFrame,
    timezone: str,
    *,
    minimum_samples: int = 24,
    updated_at: pd.Timestamp | None = None,
) -> pd.DataFrame:
    """Build robust month/hour percentiles from the local station archive.

    Month/hour cells are more honest than daily normals when the archive has
    less than a full 30-year climate period.  The UI therefore calls this a
    *baseline locale* and reports the number of represented years.
    """
    if station.empty or "time" not in station:
        return pd.DataFrame(columns=NORMAL_COLUMNS)
    frame = station.copy()
    frame["time"] = pd.to_datetime(frame["time"], utc=True, errors="coerce")
    frame = frame.dropna(subset=["time"])
    if "data_quality" in frame:
        quality = frame["data_quality"].fillna("").astype(str).str.lower().str.strip()
        frame = frame[quality.isin({"", "ok", "estimated_rain"})]
    if frame.empty:
        return pd.DataFrame(columns=NORMAL_COLUMNS)
    try:
        local = frame["time"].dt.tz_convert(timezone)
    except (KeyError, ValueError):
        local = frame["time"]
    frame["month"] = local.dt.month
    frame["hour"] = local.dt.hour
    frame["year"] = local.dt.year

    now = pd.Timestamp(updated_at or pd.Timestamp.now(tz="UTC"))
    now = now.tz_localize("UTC") if now.tzinfo is None else now.tz_convert("UTC")
    rows: list[dict[str, Any]] = []
    for column, metric in METRICS.items():
        if column not in frame:
            continue
        values = pd.to_numeric(frame[column], errors="coerce")
        usable = frame.assign(value=values).dropna(subset=["value"])
        for (month, hour), group in usable.groupby(["month", "hour"]):
            if len(group) < minimum_samples:
                continue
            quantiles = group["value"].quantile([0.10, 0.50, 0.90])
            rows.append(
                {
                    "source": SOURCE,
                    "month": int(month),
                    "day": 0,
                    "hour": int(hour),
                    "metric": metric,
                    "p10": float(quantiles.loc[0.10]),
                    "p50": float(quantiles.loc[0.50]),
                    "p90": float(quantiles.loc[0.90]),
                    "sample_years": int(group["year"].nunique()),
                    "updated_at": now,
                }
            )
    return pd.DataFrame(rows, columns=NORMAL_COLUMNS)


def calculate_local_normals(
    cfg: Settings = settings, engine: Engine | None = None
) -> pd.DataFrame:
    ensure_schema()
    engine = engine or get_engine()
    with engine.connect() as connection:
        station = pd.read_sql(
            text(
                "SELECT time,temp_c,humidity,pressure_hpa,wind_kmh,data_quality "
                "FROM station_raw ORDER BY time"
            ),
            connection,
        )
    # SQLite preserves the historical mixed-case identifiers from schema.sql,
    # while PostgreSQL returns lowercase names.  Normalising here keeps both
    # production and local/CI databases on the same path.
    station.columns = [str(column).lower() for column in station.columns]
    normals = build_local_normals(station, cfg.local_timezone)
    if normals.empty:
        raise ClimatologyError(
            "climatologia locale: archivio ancora insufficiente per una baseline"
        )
    return normals


def archive_local_normals(frame: pd.DataFrame, engine: Engine | None = None) -> int:
    if frame.empty:
        return 0
    ensure_schema()
    engine = engine or get_engine()
    records: list[dict[str, Any]] = []
    for row in frame.reindex(columns=NORMAL_COLUMNS).to_dict("records"):
        payload = dict(row)
        payload["updated_at"] = _utc(payload["updated_at"])
        for column in ("p10", "p50", "p90"):
            value = payload.get(column)
            payload[column] = None if value is None or pd.isna(value) else float(value)
        payload["sample_years"] = int(payload.get("sample_years") or 0)
        records.append(payload)
    placeholders = ",".join(f":{column}" for column in NORMAL_COLUMNS)
    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO climate_normals ("
                + ",".join(NORMAL_COLUMNS)
                + ") VALUES ("
                + placeholders
                + ") ON CONFLICT (source,month,day,hour,metric) DO UPDATE SET "
                "p10=excluded.p10,p50=excluded.p50,p90=excluded.p90,"
                "sample_years=excluded.sample_years,updated_at=excluded.updated_at"
            ),
            records,
        )
    return len(records)


def refresh_climatology(
    cfg: Settings = settings, engine: Engine | None = None
) -> tuple[pd.DataFrame, str | None]:
    """Refresh the local baseline without ever blocking the forecast pipeline."""
    if not cfg.feature_climatology_enabled:
        record_source_disabled("climatology_local", engine=engine)
        return pd.DataFrame(columns=NORMAL_COLUMNS), None
    started = perf_counter()
    try:
        frame = calculate_local_normals(cfg, engine)
        archive_local_normals(frame, engine)
    except (ClimatologyError, ValueError) as exc:
        record_source_result(
            "climatology_local",
            success=False,
            latency_ms=(perf_counter() - started) * 1000,
            error=exc,
            engine=engine,
        )
        return pd.DataFrame(columns=NORMAL_COLUMNS), str(exc)
    record_source_result(
        "climatology_local",
        success=True,
        rows_received=len(frame),
        last_observation_at=frame["updated_at"].max(),
        latency_ms=(perf_counter() - started) * 1000,
        engine=engine,
    )
    return frame, None


def anomaly_snapshot(
    current: pd.Series | dict[str, Any],
    normals: pd.DataFrame,
    timezone: str = "UTC",
) -> pd.DataFrame:
    """Compare current values with the matching month/hour local baseline."""
    if normals.empty:
        return pd.DataFrame()
    observed_time = pd.to_datetime(current.get("time"), utc=True, errors="coerce")
    if pd.isna(observed_time):
        return pd.DataFrame()
    try:
        observed_time = observed_time.tz_convert(timezone)
    except (KeyError, ValueError):
        pass
    matching = normals[
        (normals["month"] == observed_time.month)
        & (normals["hour"] == observed_time.hour)
    ]
    labels = {
        "temp_c": ("Temperatura", "°C", 1),
        "humidity": ("Umidità", "%", 0),
        "pressure_hpa": ("Pressione", "hPa", 1),
        "wind_kmh": ("Vento", "km/h", 1),
    }
    rows: list[dict[str, Any]] = []
    for row in matching.to_dict("records"):
        metric = str(row["metric"])
        value = pd.to_numeric(pd.Series([current.get(metric)]), errors="coerce").iloc[0]
        if pd.isna(value) or metric not in labels:
            continue
        median = float(row["p50"])
        delta = float(value) - median
        low, high = float(row["p10"]), float(row["p90"])
        state = (
            "Sotto il consueto"
            if value < low
            else "Sopra il consueto"
            if value > high
            else "Nella fascia consueta"
        )
        label, unit, decimals = labels[metric]
        rows.append(
            {
                "metric": metric,
                "label": label,
                "unit": unit,
                "decimals": decimals,
                "value": float(value),
                "normal": median,
                "delta": delta,
                "p10": low,
                "p90": high,
                "state": state,
                "sample_years": int(row.get("sample_years") or 0),
            }
        )
    return pd.DataFrame(rows)
