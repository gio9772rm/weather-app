"""Reliable Ecowitt Cloud ingestion with rain increments and quality checks."""

from __future__ import annotations

import argparse
import logging
import math
import os
from collections.abc import Iterable
from datetime import datetime, timedelta, timezone
from typing import Any

import numpy as np
import pandas as pd
import requests
from dotenv import load_dotenv
from sqlalchemy import text

from config import Settings
from db import ensure_schema, get_engine, set_meta
from forecast_providers import build_session

load_dotenv()
LOG_LEVEL = (os.getenv("LOG_LEVEL") or "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("ecowitt")


RAW_COLUMNS = [
    "time",
    "temp_c",
    "humidity",
    "pressure_hpa",
    "wind_kmh",
    "windgust_kmh",
    "winddir",
    "rain_mm",
    "wind_ms",
    "rain_rate_mm_h",
    "rain_total_mm",
    "solar_w_m2",
    "uv_index",
    "source",
    "data_quality",
]


class EcowittError(RuntimeError):
    """A safe error that never contains credentials or a query string."""


def _first(mapping: Any, names: Iterable[str]) -> Any:
    if not isinstance(mapping, dict):
        return None
    lowered = {str(key).lower(): value for key, value in mapping.items()}
    for name in names:
        if name in mapping:
            return mapping[name]
        if name.lower() in lowered:
            return lowered[name.lower()]
    return None


def safe_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, dict):
        value = _first(value, ("value", "val", "v"))
    if isinstance(value, (int, float, np.number)):
        number = float(value)
        return number if math_is_finite(number) else None
    raw = str(value).strip().replace(" ", "")
    if not raw:
        return None
    if "," in raw and "." in raw:
        raw = (
            raw.replace(".", "").replace(",", ".")
            if raw.rfind(",") > raw.rfind(".")
            else raw.replace(",", "")
        )
    elif "," in raw:
        raw = raw.replace(",", ".")
    try:
        number = float(raw)
    except ValueError:
        return None
    return number if math_is_finite(number) else None


def math_is_finite(value: float) -> bool:
    return not (math_is_nan(value) or value in (float("inf"), float("-inf")))


def math_is_nan(value: float) -> bool:
    return math.isnan(value)


def _value_unit(node: Any) -> tuple[float | None, str | None]:
    if isinstance(node, dict):
        return safe_float(_first(node, ("value", "val", "v"))), str(
            _first(node, ("unit", "u")) or ""
        ) or None
    return safe_float(node), None


def _temperature_c(value: float | None, unit: str | None) -> float | None:
    if value is None:
        return None
    unit = (unit or "").lower().replace("º", "°")
    return (value - 32.0) * 5.0 / 9.0 if "f" in unit else value


def _pressure_hpa(value: float | None, unit: str | None) -> float | None:
    if value is None:
        return None
    unit = (unit or "").lower()
    if "inhg" in unit:
        return value * 33.8638866667
    if "kpa" in unit:
        return value * 10.0
    if unit.strip() == "pa":
        return value / 100.0
    if 8000 <= value <= 11000:
        return value / 10.0
    if value > 2000:
        return value / 100.0
    return value


def _wind_kmh(value: float | None, unit: str | None) -> float | None:
    if value is None:
        return None
    unit = (unit or "").lower()
    if "m/s" in unit or unit in {"mps", "ms"}:
        return value * 3.6
    if "mph" in unit:
        return value * 1.609344
    if "knot" in unit or unit in {"kt", "kts"}:
        return value * 1.852
    return value


def _rain_mm(value: float | None, unit: str | None) -> float | None:
    if value is None:
        return None
    unit = (unit or "").lower()
    return value * 25.4 if unit in {"in", "inch", "inches", "in/h", "inch/h"} else value


def _timestamp(value: Any, fallback: Any = None) -> pd.Timestamp:
    candidate = value if value is not None else fallback
    if isinstance(candidate, str) and candidate.strip().isdigit():
        candidate = int(candidate.strip())
    if isinstance(candidate, (int, float, np.number)):
        timestamp = pd.to_datetime(candidate, unit="s", utc=True, errors="coerce")
    else:
        timestamp = pd.to_datetime(candidate, utc=True, errors="coerce")
    return timestamp if not pd.isna(timestamp) else pd.Timestamp.now(tz="UTC")


def _sensor(group: dict[str, Any], *names: str) -> Any:
    return _first(group, names)


def parse_item(item: dict[str, Any], fallback_time: Any = None) -> dict[str, Any]:
    """Parse an Ecowitt real-time object."""
    outdoor = item.get("outdoor") or {}
    wind = item.get("wind") or {}
    pressure = item.get("pressure") or {}
    rainfall = item.get("rainfall") or item.get("rainfall_piezo") or {}
    solar = item.get("solar_and_uvi") or item.get("solar") or {}

    temp_value, temp_unit = _value_unit(
        _sensor(outdoor, "temperature", "temp_c", "temp")
    )
    humidity_value, _ = _value_unit(_sensor(outdoor, "humidity", "hum"))
    pressure_value, pressure_unit = _value_unit(
        _sensor(
            pressure, "relative", "rel", "relative_hpa", "rel_hpa", "abs", "absolute"
        )
    )
    wind_value, wind_unit = _value_unit(
        _sensor(wind, "wind_speed", "speed", "avg", "windspeed", "speed_kmh", "ws")
    )
    gust_value, gust_unit = _value_unit(
        _sensor(wind, "wind_gust", "gust", "max", "gust_kmh")
    )
    direction_value, _ = _value_unit(
        _sensor(wind, "wind_direction", "direction", "dir", "winddir")
    )
    rate_value, rate_unit = _value_unit(
        _sensor(rainfall, "rain_rate", "rate", "rainrate", "rainrate_mm")
    )
    total_value, total_unit = _value_unit(
        _sensor(rainfall, "total", "yearly", "rain_total", "year", "daily")
    )
    solar_value, _ = _value_unit(
        _sensor(solar, "solar", "solar_irradiance", "radiation")
    )
    uv_value, _ = _value_unit(_sensor(solar, "uvi", "uv", "uv_index"))
    timestamp = _timestamp(
        _first(item, ("time", "last_update_time", "update_time", "date", "timestamp")),
        fallback_time,
    )
    wind_kmh = _wind_kmh(wind_value, wind_unit)
    return {
        "time": timestamp,
        "temp_c": _temperature_c(temp_value, temp_unit),
        "humidity": humidity_value,
        "pressure_hpa": _pressure_hpa(pressure_value, pressure_unit),
        "wind_kmh": wind_kmh,
        "windgust_kmh": _wind_kmh(gust_value, gust_unit),
        "winddir": direction_value,
        "rain_mm": None,
        "wind_ms": wind_kmh / 3.6 if wind_kmh is not None else None,
        "rain_rate_mm_h": _rain_mm(rate_value, rate_unit),
        "rain_total_mm": _rain_mm(total_value, total_unit),
        "solar_w_m2": solar_value,
        "uv_index": uv_value,
        "source": "ecowitt_cloud",
        "data_quality": "ok",
    }


def _series_values(node: Any) -> tuple[dict[str, Any], str | None]:
    if not isinstance(node, dict):
        return {}, None
    unit = str(_first(node, ("unit", "u")) or "") or None
    values = _first(node, ("list", "values", "data"))
    if isinstance(values, dict):
        return values, unit
    if isinstance(values, list):
        mapped: dict[str, Any] = {}
        for entry in values:
            if isinstance(entry, dict):
                key = _first(entry, ("time", "timestamp", "date"))
                if key is not None:
                    mapped[str(key)] = _first(entry, ("value", "val", "v"))
        return mapped, unit
    return {}, unit


def _history_field(
    rows: dict[pd.Timestamp, dict[str, Any]],
    node: Any,
    field: str,
    converter,
) -> None:
    values, unit = _series_values(node)
    for raw_time, raw_value in values.items():
        timestamp = _timestamp(raw_time)
        row = rows.setdefault(timestamp, {"time": timestamp})
        value, embedded_unit = _value_unit(raw_value)
        row[field] = converter(value, embedded_unit or unit)


def _parse_history_data(data: dict[str, Any]) -> pd.DataFrame:
    rows: dict[pd.Timestamp, dict[str, Any]] = {}
    outdoor = data.get("outdoor") or {}
    wind = data.get("wind") or {}
    pressure = data.get("pressure") or {}
    rainfall = data.get("rainfall") or data.get("rainfall_piezo") or {}
    solar = data.get("solar_and_uvi") or data.get("solar") or {}
    _history_field(
        rows, _sensor(outdoor, "temperature", "temp"), "temp_c", _temperature_c
    )
    _history_field(
        rows, _sensor(outdoor, "humidity", "hum"), "humidity", lambda value, unit: value
    )
    _history_field(
        rows,
        _sensor(pressure, "relative", "rel", "absolute", "abs"),
        "pressure_hpa",
        _pressure_hpa,
    )
    _history_field(
        rows,
        _sensor(wind, "wind_speed", "speed", "windspeed"),
        "wind_kmh",
        _wind_kmh,
    )
    _history_field(
        rows,
        _sensor(wind, "wind_gust", "gust", "max"),
        "windgust_kmh",
        _wind_kmh,
    )
    _history_field(
        rows,
        _sensor(wind, "wind_direction", "direction", "dir"),
        "winddir",
        lambda value, unit: value,
    )
    _history_field(
        rows,
        _sensor(rainfall, "rain_rate", "rate", "rainrate"),
        "rain_rate_mm_h",
        _rain_mm,
    )
    _history_field(
        rows,
        _sensor(rainfall, "total", "yearly", "rain_total", "year", "daily"),
        "rain_total_mm",
        _rain_mm,
    )
    _history_field(
        rows,
        _sensor(solar, "solar", "solar_irradiance", "radiation"),
        "solar_w_m2",
        lambda value, unit: value,
    )
    _history_field(
        rows,
        _sensor(solar, "uvi", "uv", "uv_index"),
        "uv_index",
        lambda value, unit: value,
    )
    records = []
    for row in rows.values():
        row.setdefault("rain_mm", None)
        if row.get("wind_kmh") is not None:
            row["wind_ms"] = row["wind_kmh"] / 3.6
        row["source"] = "ecowitt_cloud_history"
        row["data_quality"] = "ok"
        records.append(row)
    return pd.DataFrame(records).reindex(columns=RAW_COLUMNS)


def parse_payload(payload: dict[str, Any]) -> pd.DataFrame:
    data = payload.get("data") if isinstance(payload, dict) else None
    if not data:
        return pd.DataFrame(columns=RAW_COLUMNS)
    if isinstance(data, dict):
        # Historical responses store a timestamped list inside each sensor node.
        has_series = any(
            isinstance(node, dict)
            and isinstance(_first(node, ("list", "values", "data")), (dict, list))
            for group in data.values()
            if isinstance(group, dict)
            for node in group.values()
            if isinstance(node, dict)
        )
        if has_series:
            frame = _parse_history_data(data)
        else:
            items = data.get("list") if isinstance(data.get("list"), list) else [data]
            frame = pd.DataFrame(
                [
                    parse_item(item, payload.get("time"))
                    for item in items
                    if isinstance(item, dict)
                ]
            )
    elif isinstance(data, list):
        frame = pd.DataFrame(
            [
                parse_item(item, payload.get("time"))
                for item in data
                if isinstance(item, dict)
            ]
        )
    else:
        frame = pd.DataFrame()
    if frame.empty:
        return pd.DataFrame(columns=RAW_COLUMNS)
    frame = frame.reindex(columns=RAW_COLUMNS)
    frame["time"] = pd.to_datetime(frame["time"], utc=True, errors="coerce")
    return (
        frame.dropna(subset=["time"])
        .drop_duplicates("time", keep="last")
        .sort_values("time")
    )


def _append_quality_flag(
    frame: pd.DataFrame, mask: pd.Series, flag: str
) -> None:
    """Append a stable, de-duplicated quality flag to the selected rows."""
    selected = mask.reindex(frame.index, fill_value=False).fillna(False)
    if not selected.any():
        return

    def append(value: Any) -> str:
        current = [
            item
            for item in str(value or "ok").split(";")
            if item and item != "ok"
        ]
        if flag not in current:
            current.append(flag)
        return ";".join(current) or "ok"

    frame.loc[selected, "data_quality"] = frame.loc[
        selected, "data_quality"
    ].map(append)


def _quality_check(frame: pd.DataFrame) -> pd.DataFrame:
    """Apply physical, temporal and cross-sensor plausibility checks.

    Impossible physical values are cleared. Temporal anomalies remain available
    for inspection, but are explicitly marked so they are not mistaken for fully
    validated observations.
    """
    checks = {
        "temp_c": (-60, 60),
        "humidity": (0, 100),
        "pressure_hpa": (800, 1150),
        "wind_kmh": (0, 300),
        "windgust_kmh": (0, 350),
        "winddir": (0, 360),
        "rain_mm": (0, 500),
        "rain_rate_mm_h": (0, 500),
        "rain_total_mm": (0, 100000),
        "solar_w_m2": (0, 1800),
        "uv_index": (0, 30),
    }
    output = frame.copy().sort_values("time")
    output["data_quality"] = output.get(
        "data_quality", pd.Series("ok", index=output.index)
    ).fillna("ok")
    invalid = pd.Series(False, index=output.index)
    for column, (minimum, maximum) in checks.items():
        output[column] = pd.to_numeric(output[column], errors="coerce")
        bad = output[column].notna() & ~output[column].between(minimum, maximum)
        invalid |= bad
        output.loc[bad, column] = np.nan
    _append_quality_flag(output, invalid, "range_filtered")

    timestamps = pd.to_datetime(output["time"], utc=True, errors="coerce")
    elapsed_hours = timestamps.diff().dt.total_seconds().div(3600.0)
    plausible_interval = elapsed_hours.between(1 / 120, 1.0)
    # Conservative rates: high enough for severe fronts, low enough to catch
    # decoder/unit errors and isolated sensor jumps.
    rate_limits = {
        "temp_c": 15.0,
        "humidity": 60.0,
        "pressure_hpa": 15.0,
    }
    for column, hourly_limit in rate_limits.items():
        rate = output[column].diff().abs().div(elapsed_hours)
        _append_quality_flag(
            output,
            plausible_interval & rate.gt(hourly_limit),
            f"spike_{column}",
        )

    # A perfectly unchanged environmental sensor can be suspicious, but
    # pressure is naturally slow and Ecowitt rounds it to 0.1 hPa. Requiring
    # six flat hours for pressure avoids flagging normal stable weather while
    # retaining the one-hour detector for temperature and humidity.
    tolerances = {"temp_c": 0.01, "humidity": 0.01, "pressure_hpa": 0.01}
    stuck_durations = {
        "temp_c": pd.Timedelta(minutes=60),
        "humidity": pd.Timedelta(minutes=60),
        "pressure_hpa": pd.Timedelta(hours=6),
    }
    for column, tolerance in tolerances.items():
        values = pd.to_numeric(output[column], errors="coerce")
        changed = values.isna() | values.diff().abs().gt(tolerance)
        groups = changed.cumsum()
        duration = timestamps.groupby(groups).transform("max") - timestamps.groupby(
            groups
        ).transform("min")
        _append_quality_flag(
            output,
            values.notna() & duration.ge(stuck_durations[column]),
            f"stuck_{column}",
        )

    gust = pd.to_numeric(output["windgust_kmh"], errors="coerce")
    wind = pd.to_numeric(output["wind_kmh"], errors="coerce")
    _append_quality_flag(
        output,
        gust.notna() & wind.notna() & gust.add(0.2).lt(wind),
        "gust_below_mean_wind",
    )
    return output


def add_rain_increments(frame: pd.DataFrame, engine=None) -> pd.DataFrame:
    """Convert a cumulative rain counter to per-sample millimetres.

    When a counter is unavailable, rate × elapsed time is used and the sample is
    explicitly marked as estimated. This avoids summing mm/h values as if they
    were precipitation amounts.
    """
    if frame.empty:
        return frame
    engine = engine or get_engine()
    output = frame.sort_values("time").copy()
    earliest = pd.to_datetime(output["time"].min(), utc=True).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    with engine.connect() as connection:
        previous = (
            connection.execute(
                text(
                    "SELECT time,rain_total_mm FROM station_raw WHERE time < :time "
                    "AND rain_total_mm IS NOT NULL ORDER BY time DESC LIMIT 1"
                ),
                {"time": earliest},
            )
            .mappings()
            .first()
        )
    previous_time = (
        pd.to_datetime(previous["time"], utc=True, errors="coerce")
        if previous
        else pd.NaT
    )
    previous_total = safe_float(previous["rain_total_mm"]) if previous else None
    increments: list[float] = []
    qualities: list[str] = []
    for _, row in output.iterrows():
        current_time = pd.to_datetime(row["time"], utc=True)
        current_total = safe_float(row.get("rain_total_mm"))
        rate = safe_float(row.get("rain_rate_mm_h"))
        amount: float | None = None
        estimated = False
        if current_total is not None and previous_total is not None:
            delta = current_total - previous_total
            # Negative values indicate a daily/yearly counter reset.
            amount = current_total if delta < 0 and current_total <= 100 else delta
            if amount is not None and (amount < 0 or amount > 500):
                amount = None
        if amount is None and rate is not None and not pd.isna(previous_time):
            elapsed = max(
                0.0, min((current_time - previous_time).total_seconds() / 3600.0, 1.0)
            )
            amount = max(0.0, rate * elapsed)
            estimated = True
        if amount is None:
            amount = 0.0
            estimated = rate is not None
        increments.append(float(amount))
        base_quality = str(row.get("data_quality") or "ok")
        qualities.append(
            "estimated_rain" if estimated and base_quality == "ok" else base_quality
        )
        if current_total is not None:
            previous_total = current_total
        previous_time = current_time
    output["rain_mm"] = increments
    output["data_quality"] = qualities
    return _quality_check(output)


def _clean_records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    data = frame.reindex(columns=RAW_COLUMNS).copy()
    data["time"] = pd.to_datetime(data["time"], utc=True, errors="coerce").dt.strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    data = data.dropna(subset=["time"])
    records = []
    for row in data.to_dict("records"):
        records.append(
            {
                key: None
                if value is None
                or value is pd.NA
                or (isinstance(value, float) and math_is_nan(value))
                else value.item()
                if isinstance(value, np.generic)
                else value
                for key, value in row.items()
            }
        )
    return records


def upsert_raw(frame: pd.DataFrame, engine=None) -> int:
    if frame is None or frame.empty:
        return 0
    engine = engine or get_engine()
    records = _clean_records(frame)
    if not records:
        return 0
    columns = RAW_COLUMNS
    update_columns = [column for column in columns if column != "time"]
    statement = text(
        "INSERT INTO station_raw ("
        + ",".join(columns)
        + ") VALUES ("
        + ",".join(f":{column}" for column in columns)
        + ") ON CONFLICT (time) DO UPDATE SET "
        + ",".join(f"{column}=excluded.{column}" for column in update_columns)
    )
    with engine.begin() as connection:
        for start in range(0, len(records), 2000):
            connection.execute(statement, records[start : start + 2000])
    return len(records)


def _circular_mean(values: pd.Series) -> float:
    radians = np.deg2rad(pd.to_numeric(values, errors="coerce").dropna())
    if radians.empty:
        return np.nan
    return float(
        np.rad2deg(np.arctan2(np.sin(radians).mean(), np.cos(radians).mean())) % 360
    )


def recompute_3h(
    window_start_utc: pd.Timestamp | None = None,
    lookback_hours: int = 96,
    engine=None,
) -> int:
    engine = engine or get_engine()
    now = pd.Timestamp.now(tz="UTC")
    start = (
        pd.to_datetime(window_start_utc, utc=True, errors="coerce")
        if window_start_utc is not None
        else pd.NaT
    )
    if pd.isna(start):
        start = now - pd.Timedelta(hours=lookback_hours)
    start = start.floor("3h")
    with engine.connect() as connection:
        frame = pd.read_sql(
            text("SELECT * FROM station_raw WHERE time >= :start ORDER BY time"),
            connection,
            params={"start": start.strftime("%Y-%m-%dT%H:%M:%SZ")},
        )
    if frame.empty:
        return 0
    frame.columns = [column.lower() for column in frame.columns]
    frame["time"] = pd.to_datetime(frame["time"], utc=True, errors="coerce")
    for column in (
        "temp_c",
        "humidity",
        "pressure_hpa",
        "wind_kmh",
        "windgust_kmh",
        "winddir",
        "rain_mm",
    ):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    if "source" in frame:
        legacy = frame["source"].isna() | frame["source"].astype(
            "string"
        ).str.strip().eq("")
        frame.loc[legacy, "rain_mm"] = np.nan
    frame = frame.dropna(subset=["time"]).set_index("time")
    grouped = frame.resample("3h")
    aggregate = grouped.agg(
        temp_c=("temp_c", "mean"),
        humidity=("humidity", "mean"),
        pressure_hpa=("pressure_hpa", "mean"),
        wind_kmh=("wind_kmh", "mean"),
        windgust_kmh=("windgust_kmh", "max"),
        rain_mm=("rain_mm", "sum"),
        sample_count=("temp_c", "count"),
    )
    aggregate["winddir"] = grouped["winddir"].apply(_circular_mean)
    aggregate = aggregate[aggregate["sample_count"] > 0].reset_index()
    aggregate["time"] = aggregate["time"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    records = []
    for row in aggregate.to_dict("records"):
        records.append(
            {
                key: (
                    None
                    if pd.isna(value)
                    else value.item()
                    if isinstance(value, np.generic)
                    else value
                )
                for key, value in row.items()
            }
        )
    statement = text(
        "INSERT INTO station_3h (time,temp_c,humidity,pressure_hpa,wind_kmh,windgust_kmh,winddir,rain_mm,sample_count) "
        "VALUES (:time,:temp_c,:humidity,:pressure_hpa,:wind_kmh,:windgust_kmh,:winddir,:rain_mm,:sample_count) "
        "ON CONFLICT (time) DO UPDATE SET temp_c=excluded.temp_c,humidity=excluded.humidity,"
        "pressure_hpa=excluded.pressure_hpa,wind_kmh=excluded.wind_kmh,"
        "windgust_kmh=excluded.windgust_kmh,winddir=excluded.winddir,rain_mm=excluded.rain_mm,"
        "sample_count=excluded.sample_count"
    )
    with engine.begin() as connection:
        connection.execute(statement, records)
    return len(records)


def ecowitt_get(
    path: str,
    params: dict[str, Any],
    cfg: Settings,
    session: requests.Session,
) -> dict[str, Any]:
    request_params = {
        "application_key": cfg.ecowitt_application_key,
        "api_key": cfg.ecowitt_api_key,
        **params,
    }
    try:
        response = session.get(
            f"https://api.ecowitt.net/api/v3/{path}",
            params=request_params,
            timeout=(8, 30),
        )
        if not response.ok:
            raise EcowittError(f"Ecowitt {path}: risposta HTTP {response.status_code}")
        payload = response.json()
    except EcowittError:
        raise
    except (requests.RequestException, ValueError) as exc:
        raise EcowittError(f"Ecowitt {path}: servizio non raggiungibile") from exc
    if not isinstance(payload, dict):
        raise EcowittError(f"Ecowitt {path}: risposta non valida")
    code = payload.get("code")
    if code not in (None, 0, "0"):
        raise EcowittError(f"Ecowitt {path}: API code {code}")
    return payload


def _history_windows(hours: int) -> list[tuple[datetime, datetime]]:
    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=hours)
    windows = []
    cursor = start
    while cursor < now:
        end = min(now, cursor.replace(hour=23, minute=59, second=59, microsecond=0))
        if end <= cursor:
            end = min(now, cursor + timedelta(days=1))
        windows.append((cursor, end))
        cursor = end + timedelta(seconds=1)
    return windows


def run_station_ingest(
    backfill_hours: int | None = None,
    cfg: Settings | None = None,
) -> dict[str, Any]:
    cfg = cfg or Settings.from_env()
    if not cfg.has_station_credentials:
        raise EcowittError(
            "Credenziali Ecowitt mancanti: configurare application key, API key e MAC"
        )
    ensure_schema()
    backfill = (
        cfg.station_backfill_hours
        if backfill_hours is None
        else max(0, int(backfill_hours))
    )
    frames: list[pd.DataFrame] = []
    warnings: list[str] = []
    session = build_session()
    callback = "outdoor,wind,pressure,rainfall,rainfall_piezo,solar_and_uvi"
    try:
        try:
            payload = ecowitt_get(
                "device/real_time",
                {
                    "mac": cfg.ecowitt_mac.replace("-", ":").lower(),
                    "call_back": callback,
                },
                cfg,
                session,
            )
            realtime = parse_payload(payload)
            if not realtime.empty:
                frames.append(realtime.tail(1))
            else:
                warnings.append("Realtime Ecowitt vuoto")
        except EcowittError as exc:
            warnings.append(str(exc))

        if backfill > 0:
            for start, end in _history_windows(backfill):
                try:
                    payload = ecowitt_get(
                        "device/history",
                        {
                            "mac": cfg.ecowitt_mac.replace("-", ":").lower(),
                            "start_date": start.strftime("%Y-%m-%d %H:%M:%S"),
                            "end_date": end.strftime("%Y-%m-%d %H:%M:%S"),
                            "call_back": callback,
                        },
                        cfg,
                        session,
                    )
                    history = parse_payload(payload)
                    if not history.empty:
                        frames.append(history)
                except EcowittError as exc:
                    warnings.append(str(exc))
    finally:
        session.close()

    if not frames:
        raise EcowittError(
            "Nessun dato Ecowitt ricevuto"
            + (f" ({'; '.join(warnings)})" if warnings else "")
        )
    combined = (
        pd.concat(frames, ignore_index=True)
        .drop_duplicates("time", keep="last")
        .sort_values("time")
    )
    engine = get_engine()
    combined = add_rain_increments(combined, engine)
    rows = upsert_raw(combined, engine)
    if rows <= 0:
        raise EcowittError("Dati Ecowitt ricevuti ma nessuna riga valida salvata")
    start = pd.to_datetime(combined["time"].min(), utc=True)
    buckets = recompute_3h(start, max(96, backfill), engine)
    finished = pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%dT%H:%M:%SZ")
    latest = pd.to_datetime(combined["time"].max(), utc=True).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    set_meta("last_ingest", finished)
    set_meta("last_station_success", finished)
    set_meta("last_station_time", latest)
    return {
        "rows": rows,
        "buckets_3h": buckets,
        "latest_station_time": latest,
        "warnings": warnings,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Importa dati Ecowitt Cloud")
    parser.add_argument(
        "--backfill-hours",
        type=int,
        default=int(
            os.getenv("BACKFILL_HOURS") or Settings.from_env().station_backfill_hours
        ),
    )
    args = parser.parse_args()
    try:
        summary = run_station_ingest(args.backfill_hours)
    except EcowittError as exc:
        log.error("%s", exc)
        return 2
    log.info(
        "Ecowitt completato: %s righe, %s bucket 3h, ultimo dato %s",
        summary["rows"],
        summary["buckets_3h"],
        summary["latest_station_time"],
    )
    for warning in summary["warnings"]:
        log.warning("%s", warning)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
