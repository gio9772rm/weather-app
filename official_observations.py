"""Trusted public observations used as secondary forecast references.

Ecowitt remains the local source of truth.  Records stored here are kept in a
separate table so an airport or regional sensor can never be presented as a
measurement made by the user's station.
"""

from __future__ import annotations

import json
import math
import re
from typing import Any

import numpy as np
import pandas as pd
import requests
from sqlalchemy import text
from sqlalchemy.engine import Engine

from config import Settings, settings
from db import get_engine
from forecast_providers import build_session

METAR_URL = "https://aviationweather.gov/api/data/metar"
SOURCE_AWC_METAR = "awc_metar"
KNOT_TO_KMH = 1.852
INCH_TO_MM = 25.4
MILE_TO_METRE = 1609.344

OBSERVATION_COLUMNS = [
    "source",
    "station_id",
    "time",
    "station_name",
    "latitude",
    "longitude",
    "elevation_m",
    "distance_km",
    "temp_c",
    "dewpoint_c",
    "humidity",
    "pressure_hpa",
    "wind_kmh",
    "wind_gust_kmh",
    "wind_dir",
    "rain_mm",
    "precip_observed",
    "clouds",
    "visibility_m",
    "quality_flag",
    "raw_observation",
    "fetched_at",
]


class OfficialObservationError(RuntimeError):
    """An official feed failed without exposing request details."""


def _number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _iso(value: Any) -> str | None:
    timestamp = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(timestamp):
        return None
    return timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")


def _native(value: Any) -> Any:
    if value is None or value is pd.NA:
        return None
    if isinstance(value, (np.floating, float)) and not math.isfinite(float(value)):
        return None
    if isinstance(value, np.generic):
        return value.item()
    return value


def _distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius = 6371.0088
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    a = (
        math.sin(delta_phi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
    )
    return radius * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def relative_humidity(temp_c: Any, dewpoint_c: Any) -> float | None:
    """Derive relative humidity from METAR temperature and dew point."""
    temperature = _number(temp_c)
    dewpoint = _number(dewpoint_c)
    if temperature is None or dewpoint is None:
        return None
    numerator = math.exp((17.625 * dewpoint) / (243.04 + dewpoint))
    denominator = math.exp((17.625 * temperature) / (243.04 + temperature))
    return float(np.clip(100.0 * numerator / denominator, 0.0, 100.0))


def _visibility_metres(value: Any) -> float | None:
    if isinstance(value, str):
        match = re.search(r"\d+(?:\.\d+)?", value)
        value = match.group(0) if match else None
    miles = _number(value)
    return None if miles is None else max(0.0, miles * MILE_TO_METRE)


def _cloud_cover(payload: dict[str, Any]) -> float | None:
    cover_values = {
        "SKC": 0.0,
        "CLR": 0.0,
        "CAVOK": 0.0,
        "NSC": 0.0,
        "FEW": 25.0,
        "SCT": 50.0,
        "BKN": 75.0,
        "OVC": 100.0,
        "VV": 100.0,
    }
    values: list[float] = []
    top_level = str(payload.get("cover") or "").upper()
    if top_level in cover_values:
        values.append(cover_values[top_level])
    for layer in payload.get("clouds") or []:
        if not isinstance(layer, dict):
            continue
        code = str(layer.get("cover") or "").upper()
        if code in cover_values:
            values.append(cover_values[code])
    return max(values) if values else None


def _precipitation_observed(payload: dict[str, Any]) -> int:
    if (_number(payload.get("precip")) or 0.0) > 0:
        return 1
    weather = str(payload.get("wxString") or "").upper()
    if not weather:
        # Restrict the raw fallback to the report body, before optional remarks.
        weather = str(payload.get("rawOb") or "").upper().split(" RMK ", 1)[0]
    codes = ("RA", "DZ", "SN", "SG", "PL", "GR", "GS", "UP")
    return int(any(re.search(rf"(?:^|\s)[+-]?(?:SH|TS|FZ)?{code}(?:\s|$)", weather) for code in codes))


def parse_metar_payload(
    payload: list[dict[str, Any]],
    cfg: Settings = settings,
    fetched_at: pd.Timestamp | None = None,
) -> pd.DataFrame:
    """Normalise decoded Aviation Weather METAR JSON."""
    fetched = fetched_at or pd.Timestamp.now(tz="UTC")
    fetched = pd.to_datetime(fetched, utc=True)
    allowed = {station.upper() for station in cfg.metar_station_ids}
    rows: list[dict[str, Any]] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        station_id = str(item.get("icaoId") or "").upper().strip()
        if not station_id or (allowed and station_id not in allowed):
            continue
        epoch = _number(item.get("obsTime"))
        observed = (
            pd.to_datetime(epoch, unit="s", utc=True, errors="coerce")
            if epoch is not None
            else pd.to_datetime(item.get("reportTime"), utc=True, errors="coerce")
        )
        latitude = _number(item.get("lat"))
        longitude = _number(item.get("lon"))
        if pd.isna(observed) or latitude is None or longitude is None:
            continue
        precipitation = _number(item.get("precip"))
        direction = _number(item.get("wdir"))
        speed = _number(item.get("wspd"))
        gust = _number(item.get("wgst"))
        temp = _number(item.get("temp"))
        dewpoint = _number(item.get("dewp"))
        pressure = _number(item.get("slp")) or _number(item.get("altim"))
        qc = item.get("qcField")
        rows.append(
            {
                "source": SOURCE_AWC_METAR,
                "station_id": station_id,
                "time": observed,
                "station_name": str(item.get("name") or station_id),
                "latitude": latitude,
                "longitude": longitude,
                "elevation_m": _number(item.get("elev")),
                "distance_km": _distance_km(
                    cfg.latitude, cfg.longitude, latitude, longitude
                ),
                "temp_c": temp,
                "dewpoint_c": dewpoint,
                "humidity": relative_humidity(temp, dewpoint),
                "pressure_hpa": pressure,
                "wind_kmh": None if speed is None else speed * KNOT_TO_KMH,
                "wind_gust_kmh": None if gust is None else gust * KNOT_TO_KMH,
                "wind_dir": None if direction is None else direction % 360.0,
                "rain_mm": (
                    None if precipitation is None else max(0.0, precipitation * INCH_TO_MM)
                ),
                "precip_observed": _precipitation_observed(item),
                "clouds": _cloud_cover(item),
                "visibility_m": _visibility_metres(item.get("visib")),
                "quality_flag": "official" if qc in (None, 0) else f"official_qc:{qc}",
                "raw_observation": str(item.get("rawOb") or "")[:1000],
                "fetched_at": fetched,
            }
        )
    if not rows:
        return pd.DataFrame(columns=OBSERVATION_COLUMNS)
    frame = pd.DataFrame(rows).reindex(columns=OBSERVATION_COLUMNS)
    return frame.sort_values("time").drop_duplicates(
        ["source", "station_id", "time"], keep="last"
    )


def fetch_metar_observations(
    cfg: Settings = settings,
    session: requests.Session | None = None,
) -> pd.DataFrame:
    if not cfg.metar_station_ids:
        return pd.DataFrame(columns=OBSERVATION_COLUMNS)
    own_session = session is None
    session = session or build_session()
    try:
        try:
            response = session.get(
                METAR_URL,
                params={
                    "ids": ",".join(cfg.metar_station_ids),
                    "format": "json",
                    "hours": cfg.official_observation_lookback_hours,
                },
                timeout=(8, 30),
            )
            if response.status_code == 204:
                return pd.DataFrame(columns=OBSERVATION_COLUMNS)
            if not response.ok:
                raise OfficialObservationError(
                    f"Aviation Weather: risposta HTTP {response.status_code}"
                )
            payload = response.json()
        except OfficialObservationError:
            raise
        except (requests.RequestException, ValueError) as exc:
            raise OfficialObservationError(
                "Aviation Weather: servizio non raggiungibile"
            ) from exc
        if not isinstance(payload, list):
            raise OfficialObservationError("Aviation Weather: risposta non valida")
        return parse_metar_payload(payload, cfg)
    finally:
        if own_session:
            session.close()


def archive_official_observations(
    frame: pd.DataFrame, engine: Engine | None = None
) -> int:
    if frame is None or frame.empty:
        return 0
    engine = engine or get_engine()
    records: list[dict[str, Any]] = []
    for row in frame.reindex(columns=OBSERVATION_COLUMNS).to_dict("records"):
        row["time"] = _iso(row.get("time"))
        row["fetched_at"] = _iso(row.get("fetched_at"))
        clean = {key: _native(value) for key, value in row.items()}
        if clean["source"] and clean["station_id"] and clean["time"]:
            records.append(clean)
    if not records:
        return 0
    immutable = {"source", "station_id", "time"}
    insert = text(
        "INSERT INTO official_observations ("
        + ",".join(OBSERVATION_COLUMNS)
        + ") VALUES ("
        + ",".join(f":{column}" for column in OBSERVATION_COLUMNS)
        + ") ON CONFLICT (source,station_id,time) DO UPDATE SET "
        + ",".join(
            f"{column}=excluded.{column}"
            for column in OBSERVATION_COLUMNS
            if column not in immutable
        )
    )
    with engine.begin() as connection:
        for start in range(0, len(records), 1000):
            connection.execute(insert, records[start : start + 1000])
    return len(records)


def ingest_official_observations(
    cfg: Settings = settings, engine: Engine | None = None
) -> dict[str, Any]:
    """Fetch every enabled official feed; individual failures remain non-fatal."""
    engine = engine or get_engine()
    frames: list[pd.DataFrame] = []
    warnings: list[str] = []
    try:
        frames.append(fetch_metar_observations(cfg))
    except OfficialObservationError as exc:
        warnings.append(str(exc))
    valid = [frame for frame in frames if not frame.empty]
    combined = (
        pd.concat(valid, ignore_index=True)
        if valid
        else pd.DataFrame(columns=OBSERVATION_COLUMNS)
    )
    rows = archive_official_observations(combined, engine)
    stations = sorted(
        set(combined.get("station_id", pd.Series(dtype="string")).dropna().astype(str))
    )
    return {"rows": rows, "stations": stations, "warnings": warnings}


def observation_sources_json(frame: pd.DataFrame) -> str:
    """Small deterministic summary suitable for the metadata table."""
    if frame is None or frame.empty:
        return "[]"
    sources = sorted(
        {
            f"{row.source}:{row.station_id}"
            for row in frame[["source", "station_id"]].drop_duplicates().itertuples()
        }
    )
    return json.dumps(sources)
