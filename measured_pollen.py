"""Measured pollen observations from the official ISPRA/SNPA POLLnet WFS."""

from __future__ import annotations

import math
import re
import unicodedata
from time import perf_counter
from typing import Any

import pandas as pd
import requests
from sqlalchemy import text
from sqlalchemy.engine import Engine

from config import Settings, settings
from db import ensure_schema, get_engine
from forecast_providers import build_session
from source_health import record_source_disabled, record_source_result

POLLNET_WFS = "https://sdi.isprambiente.it/geoserver/om/ows"
POLLNET_SOURCE_URL = "https://pollnet.isprambiente.it/opendata/"
SOURCE = "pollnet"
OBSERVATION_COLUMNS = [
    "source",
    "station_id",
    "time",
    "metric",
    "value",
    "unit",
    "station_name",
    "latitude",
    "longitude",
    "distance_km",
    "quality_flag",
    "is_modelled",
    "fetched_at",
]


class MeasuredPollenError(RuntimeError):
    """POLLnet is unavailable or returned data that cannot be interpreted."""


def _distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius = 6371.0088
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    value = (
        math.sin(dphi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    )
    return radius * 2 * math.atan2(math.sqrt(value), math.sqrt(max(0.0, 1 - value)))


def _slug(value: Any) -> str:
    normalised = unicodedata.normalize("NFKD", str(value or ""))
    ascii_value = normalised.encode("ascii", "ignore").decode("ascii").lower()
    return re.sub(r"[^a-z0-9]+", "_", ascii_value).strip("_") or "non_classificato"


def _json_response(
    session: requests.Session, params: dict[str, Any], *, timeout: int = 45
) -> dict[str, Any]:
    try:
        response = session.get(POLLNET_WFS, params=params, timeout=(8, timeout))
    except requests.RequestException as exc:
        raise MeasuredPollenError(
            "pollini misurati: servizio POLLnet non raggiungibile"
        ) from exc
    if not response.ok:
        raise MeasuredPollenError(
            f"pollini misurati: risposta POLLnet HTTP {response.status_code}"
        )
    try:
        payload = response.json()
    except ValueError as exc:
        raise MeasuredPollenError(
            "pollini misurati: risposta POLLnet non valida"
        ) from exc
    if not isinstance(payload, dict) or not isinstance(payload.get("features"), list):
        raise MeasuredPollenError("pollini misurati: risposta POLLnet incompleta")
    return payload


def parse_stations(payload: dict[str, Any]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for feature in payload.get("features") or []:
        props = feature.get("properties") or {}
        latitude = pd.to_numeric(
            pd.Series([props.get("LATITUDE")]), errors="coerce"
        ).iloc[0]
        longitude = pd.to_numeric(
            pd.Series([props.get("LONGITUDE")]), errors="coerce"
        ).iloc[0]
        station_id = pd.to_numeric(
            pd.Series([props.get("STAT_ID")]), errors="coerce"
        ).iloc[0]
        if pd.isna(latitude) or pd.isna(longitude) or pd.isna(station_id):
            continue
        rows.append(
            {
                "station_id": int(station_id),
                "station_code": str(props.get("STAT_CODE") or int(station_id)),
                "station_name": str(
                    props.get("STAT_NAME_I") or props.get("STAT_CODE") or station_id
                ),
                "region": str(props.get("REGI_NAME_I") or ""),
                "latitude": float(latitude),
                "longitude": float(longitude),
            }
        )
    return pd.DataFrame(rows)


def nearest_station(
    stations: pd.DataFrame, latitude: float, longitude: float
) -> pd.Series:
    if stations.empty:
        raise MeasuredPollenError(
            "pollini misurati: nessuna stazione POLLnet disponibile"
        )
    frame = stations.copy()
    frame["distance_km"] = frame.apply(
        lambda row: _distance_km(
            latitude,
            longitude,
            float(row["latitude"]),
            float(row["longitude"]),
        ),
        axis=1,
    )
    return frame.sort_values(["distance_km", "station_id"]).iloc[0]


def parse_concentrations(
    payload: dict[str, Any], station: pd.Series, fetched_at: pd.Timestamp
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for feature in payload.get("features") or []:
        props = feature.get("properties") or {}
        # Level 2 is the botanical family.  Level 3/4 values are its children;
        # including both would double-count the same physical measurement.
        level = pd.to_numeric(
            pd.Series([props.get("PART_LEVEL")]), errors="coerce"
        ).iloc[0]
        value = pd.to_numeric(
            pd.Series([props.get("REMA_CONCENTRATION")]), errors="coerce"
        ).iloc[0]
        raw_date = str(props.get("REMA_DATE") or "").strip()
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}Z", raw_date):
            raw_date = raw_date[:-1] + "T00:00:00Z"
        measured_at = pd.to_datetime(raw_date, utc=True, errors="coerce")
        family = str(props.get("PART_NAME_L") or "").strip()
        if level != 2 or pd.isna(value) or pd.isna(measured_at) or not family:
            continue
        rows.append(
            {
                "source": SOURCE,
                "station_id": str(station["station_code"]),
                "time": measured_at,
                "metric": f"pollen_{_slug(family)}",
                "value": max(0.0, float(value)),
                "unit": "granuli/m³",
                "station_name": str(station["station_name"]),
                "latitude": float(station["latitude"]),
                "longitude": float(station["longitude"]),
                "distance_km": float(station["distance_km"]),
                "quality_flag": "POLLnet · media giornaliera",
                "is_modelled": 0,
                "fetched_at": fetched_at,
                "family": family,
            }
        )
    if not rows:
        return pd.DataFrame(columns=OBSERVATION_COLUMNS + ["family"])
    return (
        pd.DataFrame(rows)
        .sort_values(["time", "metric"])
        .drop_duplicates(["station_id", "time", "metric"], keep="last")
        .reset_index(drop=True)
    )


def fetch_measured_pollen(
    cfg: Settings = settings, session: requests.Session | None = None
) -> pd.DataFrame:
    """Fetch the nearest station and its latest measured daily concentrations."""
    own_session = session is None
    session = session or build_session(retries=2)
    common = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "outputFormat": "json",
    }
    try:
        station_payload = _json_response(
            session, {**common, "typeName": "om:Stazioni_POLLnet"}
        )
        station = nearest_station(
            parse_stations(station_payload), cfg.latitude, cfg.longitude
        )
        concentration_payload = _json_response(
            session,
            {
                **common,
                "typeName": "om:Concentrazione_pollini_spore",
                "cql_filter": (
                    f"STAT_ID={int(station['station_id'])} AND REMA_DATE IS NOT NULL"
                ),
                "sortBy": "REMA_DATE D",
                "count": 500,
            },
        )
        frame = parse_concentrations(
            concentration_payload, station, pd.Timestamp.now(tz="UTC")
        )
    finally:
        if own_session:
            session.close()
    if frame.empty:
        raise MeasuredPollenError(
            f"pollini misurati: nessun dato interpretabile per {station['station_name']}"
        )
    return frame


def _iso(value: Any) -> str:
    timestamp = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(timestamp):
        raise ValueError("timestamp POLLnet non valido")
    return timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")


def archive_measured_pollen(frame: pd.DataFrame, engine: Engine | None = None) -> int:
    if frame.empty:
        return 0
    ensure_schema()
    engine = engine or get_engine()
    records: list[dict[str, Any]] = []
    for row in frame.reindex(columns=OBSERVATION_COLUMNS).to_dict("records"):
        payload = dict(row)
        payload["time"] = _iso(payload["time"])
        payload["fetched_at"] = _iso(payload["fetched_at"])
        payload["value"] = float(payload["value"])
        payload["is_modelled"] = 0
        records.append(payload)
    placeholders = ",".join(f":{column}" for column in OBSERVATION_COLUMNS)
    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO environment_observations ("
                + ",".join(OBSERVATION_COLUMNS)
                + ") VALUES ("
                + placeholders
                + ") ON CONFLICT (source,station_id,time,metric) DO UPDATE SET "
                "value=excluded.value,unit=excluded.unit,station_name=excluded.station_name,"
                "latitude=excluded.latitude,longitude=excluded.longitude,"
                "distance_km=excluded.distance_km,quality_flag=excluded.quality_flag,"
                "is_modelled=0,fetched_at=excluded.fetched_at"
            ),
            records,
        )
    return len(records)


def refresh_measured_pollen(
    cfg: Settings = settings, engine: Engine | None = None
) -> tuple[pd.DataFrame, str | None]:
    if not cfg.feature_measured_pollen_enabled:
        record_source_disabled(SOURCE, engine=engine)
        return pd.DataFrame(columns=OBSERVATION_COLUMNS), None
    started = perf_counter()
    try:
        frame = fetch_measured_pollen(cfg)
        archive_measured_pollen(frame, engine)
    except (MeasuredPollenError, ValueError) as exc:
        record_source_result(
            SOURCE,
            success=False,
            latency_ms=(perf_counter() - started) * 1000,
            error=exc,
            engine=engine,
        )
        return pd.DataFrame(columns=OBSERVATION_COLUMNS), str(exc)
    record_source_result(
        SOURCE,
        success=True,
        rows_received=len(frame),
        last_observation_at=frame["time"].max(),
        latency_ms=(perf_counter() - started) * 1000,
        engine=engine,
    )
    return frame, None
