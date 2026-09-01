"""Daily station summaries for private imports and homogeneous comparisons."""

from __future__ import annotations

import base64
import binascii
import gzip
import hashlib
import io
import math
import os
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from db import get_engine

DAILY_COLUMNS = [
    "station_id",
    "local_date",
    "temp_mean_c",
    "temp_min_c",
    "temp_max_c",
    "feels_like_mean_c",
    "dewpoint_mean_c",
    "humidity_mean",
    "humidity_min",
    "humidity_max",
    "pressure_mean_hpa",
    "pressure_min_hpa",
    "pressure_max_hpa",
    "wind_mean_kmh",
    "wind_gust_max_kmh",
    "wind_dir_deg",
    "rain_mm",
    "rain_rate_max_mm_h",
    "solar_max_w_m2",
    "uv_max",
    "sample_count",
    "source",
    "data_quality",
    "imported_at",
]

STATION_DAILY_BOOTSTRAP_ENV = "SECONDARY_STATION_DAILY_GZIP_B64"
MAX_BOOTSTRAP_ENCODED_BYTES = 300_000
MAX_BOOTSTRAP_CSV_BYTES = 2_000_000
MAX_BOOTSTRAP_ROWS = 5_000


ECOWITT_DAILY_MAPPING = {
    ("Outdoor", "Temperature(℃)"): "temp_mean_c",
    ("Outdoor", "Temperature Low(℃)"): "temp_min_c",
    ("Outdoor", "Temperature High(℃)"): "temp_max_c",
    ("Outdoor", "Feels Like(℃)"): "feels_like_mean_c",
    ("Outdoor", "Dew Point(℃)"): "dewpoint_mean_c",
    ("Outdoor", "Humidity(%)"): "humidity_mean",
    ("Outdoor", "Humidity Low(%)"): "humidity_min",
    ("Outdoor", "Humidity High(%)"): "humidity_max",
    ("Solar and UVI", "Solar(W/m²)"): "solar_max_w_m2",
    ("Solar and UVI", "UVI"): "uv_max",
    ("Rainfall", "Rain Rate(mm/hr)"): "rain_rate_max_mm_h",
    ("Rainfall", "Daily(mm)"): "rain_mm",
    ("Wind", "Wind Speed(km/h)"): "wind_mean_kmh",
    ("Wind", "Wind Gust(km/h)"): "wind_gust_max_kmh",
    ("Wind", "Wind Direction(º)"): "wind_dir_deg",
    ("Pressure", "Relative(hPa)"): "pressure_mean_hpa",
    ("Pressure", "Relative Low(hPa)"): "pressure_min_hpa",
    ("Pressure", "Relative High(hPa)"): "pressure_max_hpa",
}


def _append_flag(value: Any, flag: str) -> str:
    flags = [item for item in str(value or "").split(";") if item]
    if flag not in flags:
        flags.append(flag)
    return ";".join(flags)


def _filter_range(
    frame: pd.DataFrame, column: str, minimum: float, maximum: float
) -> None:
    values = pd.to_numeric(frame[column], errors="coerce")
    invalid = values.notna() & ~values.between(minimum, maximum)
    frame[column] = values.mask(invalid)
    if invalid.any():
        frame.loc[invalid, "data_quality"] = frame.loc[invalid, "data_quality"].map(
            lambda value: _append_flag(value, f"range_filtered_{column}")
        )


def parse_ecowitt_daily_export(path: str | Path) -> pd.DataFrame:
    """Read Ecowitt's two-row XLSX daily export without inventing live samples."""
    source_path = Path(path)
    frame = pd.read_excel(source_path, sheet_name="result_list", header=[0, 1])
    time_column = next(
        (column for column in frame.columns if str(column[0]).strip() == "Time"), None
    )
    if time_column is None:
        raise ValueError("Colonna Time non trovata nell'export Ecowitt")

    output = pd.DataFrame(index=frame.index)
    parsed_dates = pd.to_datetime(frame[time_column], errors="coerce")
    # Ecowitt moves the export boundary between 01:00 and 02:00 UTC across DST.
    # The calendar component is the intended local day and must not be treated as
    # a measured instant.
    output["local_date"] = parsed_dates.dt.strftime("%Y-%m-%d")
    for source_column, target_column in ECOWITT_DAILY_MAPPING.items():
        output[target_column] = (
            pd.to_numeric(frame[source_column], errors="coerce")
            if source_column in frame
            else np.nan
        )

    output["sample_count"] = None
    output["source"] = "ecowitt_daily_export"
    output["data_quality"] = "historical_daily_summary"
    output["imported_at"] = pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%dT%H:%M:%SZ")

    for column in (
        "temp_mean_c",
        "temp_min_c",
        "temp_max_c",
        "feels_like_mean_c",
        "dewpoint_mean_c",
    ):
        _filter_range(output, column, -60, 60)
    for column in ("humidity_mean", "humidity_min", "humidity_max"):
        _filter_range(output, column, 0, 100)
    for column in (
        "pressure_mean_hpa",
        "pressure_min_hpa",
        "pressure_max_hpa",
    ):
        _filter_range(output, column, 800, 1150)
    _filter_range(output, "wind_mean_kmh", 0, 300)
    _filter_range(output, "wind_gust_max_kmh", 0, 350)
    _filter_range(output, "wind_dir_deg", 0, 360)
    _filter_range(output, "rain_mm", 0, 500)
    _filter_range(output, "rain_rate_max_mm_h", 0, 500)
    _filter_range(output, "solar_max_w_m2", 0, 1800)
    _filter_range(output, "uv_max", 0, 30)

    absolute_column = ("Pressure", "Absolute(hPa)")
    if absolute_column in frame:
        absolute = pd.to_numeric(frame[absolute_column], errors="coerce")
        pressure_offset = (output["pressure_mean_hpa"] - absolute).abs()
        review = pressure_offset.gt(5)
        output.loc[review, "data_quality"] = output.loc[review, "data_quality"].map(
            lambda value: _append_flag(value, "pressure_calibration_review")
        )

    return (
        output.dropna(subset=["local_date"])
        .drop_duplicates("local_date", keep="last")
        .sort_values("local_date")
        .reset_index(drop=True)
    )


def _database_value(value: Any) -> Any:
    if value is None or value is pd.NA or pd.isna(value):
        return None
    if isinstance(value, np.generic):
        return value.item()
    return value


def upsert_daily_summaries(
    frame: pd.DataFrame,
    station_id: str,
    engine: Engine | None = None,
) -> int:
    if frame is None or frame.empty:
        return 0
    engine = engine or get_engine()
    prepared = frame.copy()
    prepared["station_id"] = station_id
    for column in DAILY_COLUMNS:
        if column not in prepared:
            prepared[column] = None
    records = [
        {key: _database_value(value) for key, value in row.items()}
        for row in prepared[DAILY_COLUMNS].to_dict("records")
    ]
    update_columns = [
        column for column in DAILY_COLUMNS if column not in {"station_id", "local_date"}
    ]
    statement = text(
        "INSERT INTO station_daily_summaries ("
        + ",".join(DAILY_COLUMNS)
        + ") VALUES ("
        + ",".join(f":{column}" for column in DAILY_COLUMNS)
        + ") ON CONFLICT (station_id,local_date) DO UPDATE SET "
        + ",".join(f"{column}=excluded.{column}" for column in update_columns)
    )
    with engine.begin() as connection:
        for start in range(0, len(records), 1000):
            connection.execute(statement, records[start : start + 1000])
    return len(records)


def _normalise_bootstrap_quality(value: Any) -> str:
    allowed = {"historical_daily_summary", "pressure_calibration_review"}
    flags = []
    for flag in str(value or "").split(";"):
        cleaned = flag.strip()
        if cleaned in allowed or (
            cleaned.startswith("range_filtered_")
            and cleaned.removeprefix("range_filtered_") in DAILY_COLUMNS
        ):
            flags.append(cleaned)
    if "historical_daily_summary" not in flags:
        flags.insert(0, "historical_daily_summary")
    return ";".join(dict.fromkeys(flags))


def decode_daily_bootstrap_payload(encoded: str) -> pd.DataFrame:
    """Decode and validate a private compressed CSV supplied by deployment."""
    payload = str(encoded or "").strip()
    if not payload:
        raise ValueError("Payload storico giornaliero vuoto")
    if len(payload) > MAX_BOOTSTRAP_ENCODED_BYTES:
        raise ValueError("Payload storico giornaliero troppo grande")
    try:
        compressed = base64.b64decode(payload, validate=True)
        with gzip.GzipFile(fileobj=io.BytesIO(compressed)) as archive:
            csv_bytes = archive.read(MAX_BOOTSTRAP_CSV_BYTES + 1)
    except (binascii.Error, EOFError, gzip.BadGzipFile, OSError) as exc:
        raise ValueError("Payload storico giornaliero non valido") from exc
    if len(csv_bytes) > MAX_BOOTSTRAP_CSV_BYTES:
        raise ValueError("CSV storico giornaliero troppo grande")
    try:
        frame = pd.read_csv(io.BytesIO(csv_bytes))
    except (UnicodeDecodeError, pd.errors.ParserError) as exc:
        raise ValueError("CSV storico giornaliero non leggibile") from exc
    if frame.empty or len(frame) > MAX_BOOTSTRAP_ROWS:
        raise ValueError("Numero di giornate nello storico non valido")
    if "local_date" not in frame:
        raise ValueError("Colonna local_date mancante nello storico")

    allowed_columns = [
        column for column in DAILY_COLUMNS if column not in {"station_id"}
    ]
    frame = frame[
        [column for column in frame.columns if column in allowed_columns]
    ].copy()
    dates = pd.to_datetime(frame["local_date"], format="%Y-%m-%d", errors="coerce")
    if dates.isna().any() or dates.min() < pd.Timestamp("1900-01-01"):
        raise ValueError("Date non valide nello storico giornaliero")
    if dates.max() > pd.Timestamp.now().tz_localize(None) + pd.Timedelta(days=2):
        raise ValueError("Lo storico giornaliero contiene date future")
    frame["local_date"] = dates.dt.strftime("%Y-%m-%d")

    for column in allowed_columns:
        if column not in frame:
            frame[column] = None
    numeric_columns = [
        column
        for column in allowed_columns
        if column
        not in {"local_date", "source", "data_quality", "imported_at"}
    ]
    for column in numeric_columns:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame["source"] = "ecowitt_daily_export"
    frame["data_quality"] = frame["data_quality"].map(
        _normalise_bootstrap_quality
    )
    frame["imported_at"] = pd.Timestamp.now(tz="UTC").strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )

    for column in (
        "temp_mean_c",
        "temp_min_c",
        "temp_max_c",
        "feels_like_mean_c",
        "dewpoint_mean_c",
    ):
        _filter_range(frame, column, -60, 60)
    for column in ("humidity_mean", "humidity_min", "humidity_max"):
        _filter_range(frame, column, 0, 100)
    for column in (
        "pressure_mean_hpa",
        "pressure_min_hpa",
        "pressure_max_hpa",
    ):
        _filter_range(frame, column, 800, 1150)
    _filter_range(frame, "wind_mean_kmh", 0, 300)
    _filter_range(frame, "wind_gust_max_kmh", 0, 350)
    _filter_range(frame, "wind_dir_deg", 0, 360)
    _filter_range(frame, "rain_mm", 0, 500)
    _filter_range(frame, "rain_rate_max_mm_h", 0, 500)
    _filter_range(frame, "solar_max_w_m2", 0, 1800)
    _filter_range(frame, "uv_max", 0, 30)
    return (
        frame[allowed_columns]
        .drop_duplicates("local_date", keep="last")
        .sort_values("local_date")
        .reset_index(drop=True)
    )


def import_daily_bootstrap_from_env(
    station_id: str,
    engine: Engine | None = None,
    *,
    payload: str | None = None,
) -> dict[str, Any] | None:
    """Import a private deployment payload once, identified by its SHA-256."""
    encoded = (
        os.getenv(STATION_DAILY_BOOTSTRAP_ENV, "") if payload is None else payload
    ).strip()
    if not encoded:
        return None
    target = engine or get_engine()
    digest = hashlib.sha256(encoded.encode("ascii")).hexdigest()
    marker = f"station_daily_bootstrap_sha256_{str(station_id)[:80]}"
    with target.connect() as connection:
        previous = connection.execute(
            text("SELECT v FROM meta WHERE k=:key"), {"key": marker}
        ).scalar_one_or_none()
    if previous == digest:
        return None

    frame = decode_daily_bootstrap_payload(encoded)
    rows = upsert_daily_summaries(frame, station_id, target)
    with target.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO meta (k,v) VALUES (:key,:value) "
                "ON CONFLICT (k) DO UPDATE SET v=excluded.v"
            ),
            {"key": marker, "value": digest},
        )
    return {
        "station_id": station_id,
        "rows": rows,
        "first_date": str(frame["local_date"].min()),
        "last_date": str(frame["local_date"].max()),
        "payload_sha256": digest[:12],
    }


def _circular_mean(values: pd.Series) -> float:
    numeric = pd.to_numeric(values, errors="coerce").dropna()
    if numeric.empty:
        return np.nan
    radians = np.deg2rad(numeric)
    return float(
        np.rad2deg(np.arctan2(np.sin(radians).mean(), np.cos(radians).mean())) % 360
    )


def aggregate_observations_daily(
    frame: pd.DataFrame,
    station_id: str,
    timezone_name: str,
) -> pd.DataFrame:
    """Aggregate true timestamped samples into the same daily comparison shape."""
    if frame is None or frame.empty:
        return pd.DataFrame(columns=DAILY_COLUMNS)
    observations = frame.copy()
    observations["time"] = pd.to_datetime(
        observations["time"], utc=True, errors="coerce"
    )
    observations = observations.dropna(subset=["time"])
    if observations.empty:
        return pd.DataFrame(columns=DAILY_COLUMNS)
    observations["local_date"] = (
        observations["time"].dt.tz_convert(timezone_name).dt.strftime("%Y-%m-%d")
    )
    for column in (
        "temp_c",
        "feels_like_c",
        "dewpoint_c",
        "humidity",
        "pressure_hpa",
        "wind_kmh",
        "windgust_kmh",
        "winddir",
        "rain_mm",
        "rain_rate_mm_h",
        "solar_w_m2",
        "uv_index",
    ):
        if column not in observations:
            observations[column] = np.nan
        observations[column] = pd.to_numeric(observations[column], errors="coerce")
    observations["rain_mm"] = observations["rain_mm"].clip(lower=0)

    grouped = observations.groupby("local_date", sort=True)
    daily = grouped.agg(
        temp_mean_c=("temp_c", "mean"),
        temp_min_c=("temp_c", "min"),
        temp_max_c=("temp_c", "max"),
        feels_like_mean_c=("feels_like_c", "mean"),
        dewpoint_mean_c=("dewpoint_c", "mean"),
        humidity_mean=("humidity", "mean"),
        humidity_min=("humidity", "min"),
        humidity_max=("humidity", "max"),
        pressure_mean_hpa=("pressure_hpa", "mean"),
        pressure_min_hpa=("pressure_hpa", "min"),
        pressure_max_hpa=("pressure_hpa", "max"),
        wind_mean_kmh=("wind_kmh", "mean"),
        wind_gust_max_kmh=("windgust_kmh", "max"),
        rain_mm=("rain_mm", "sum"),
        rain_rate_max_mm_h=("rain_rate_mm_h", "max"),
        solar_max_w_m2=("solar_w_m2", "max"),
        uv_max=("uv_index", "max"),
        sample_count=("time", "count"),
    )
    daily["wind_dir_deg"] = grouped["winddir"].apply(_circular_mean)
    daily = daily.reset_index()
    daily["station_id"] = station_id
    daily["source"] = "station_observations"
    daily["data_quality"] = "live_daily_aggregate"
    daily["imported_at"] = pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%dT%H:%M:%SZ")
    return daily.reindex(columns=DAILY_COLUMNS)


def combine_daily_sources(imported: pd.DataFrame, live: pd.DataFrame) -> pd.DataFrame:
    """Prefer live aggregates when both sources cover the same station/day."""
    available = [
        frame for frame in (imported, live) if frame is not None and not frame.empty
    ]
    if not available:
        return pd.DataFrame(columns=DAILY_COLUMNS)
    normalised = [frame.dropna(axis=1, how="all") for frame in available]
    combined = pd.concat(normalised, ignore_index=True).reindex(columns=DAILY_COLUMNS)
    combined["local_date"] = pd.to_datetime(combined["local_date"], errors="coerce")
    combined = combined.dropna(subset=["station_id", "local_date"])
    source_rank = (
        combined["source"].isin({"station_observations", "station_raw"}).astype(int)
    )
    combined = (
        combined.assign(_source_rank=source_rank)
        .sort_values(["station_id", "local_date", "_source_rank"])
        .drop_duplicates(["station_id", "local_date"], keep="last")
        .drop(columns="_source_rank")
        .sort_values(["local_date", "station_id"])
        .reset_index(drop=True)
    )
    return combined


def pressure_review_share(frame: pd.DataFrame) -> float:
    if frame is None or frame.empty or "data_quality" not in frame:
        return 0.0
    flagged = (
        frame["data_quality"]
        .astype(str)
        .str.contains("pressure_calibration_review", regex=False)
    )
    return float(flagged.mean()) if len(flagged) else 0.0


def finite_or_none(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None
