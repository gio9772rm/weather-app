"""Official EEA up-to-date air observations kept separate from Ecowitt data."""

from __future__ import annotations

import csv
import io
import math
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
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

EEA_API = "https://eeadmz1-downloads-api-appservice.azurewebsites.net"
EEA_METADATA_URL = (
    "https://discomap.eea.europa.eu/App/AQViewer/download"
    "?fqn=Airquality_Dissem.b2g.measurements&f=csv"
)
POLLUTANTS = {
    1: "so2",
    5: "pm10",
    7: "ozone",
    8: "nitrogen_dioxide",
    6001: "pm2_5",
}
UNITS = {metric: "µg/m³" for metric in POLLUTANTS.values()}
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


class ObservedAirError(RuntimeError):
    """Official air observations are unavailable without leaking internals."""


def _normalise_sampling_point(value: Any) -> str:
    text_value = str(value or "").strip()
    marker = text_value.find("SPO.")
    return text_value[marker:] if marker >= 0 else text_value


def _station_code(sampling_point: str) -> str:
    first = _normalise_sampling_point(sampling_point).split("_", 1)[0]
    return first.removeprefix("SPO.") or "EEA"


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


def _post_json(
    session: requests.Session,
    url: str,
    body: dict[str, Any],
    *,
    timeout: int = 75,
) -> requests.Response:
    try:
        response = session.post(
            url,
            json=body,
            headers={"Accept": "application/json, text/csv, text/*"},
            timeout=(10, timeout),
        )
    except requests.RequestException as exc:
        raise ObservedAirError("aria osservata EEA: servizio non raggiungibile") from exc
    if not response.ok:
        raise ObservedAirError(
            f"aria osservata EEA: risposta HTTP {response.status_code}"
        )
    return response


def _request_body(cfg: Settings, start: pd.Timestamp, end: pd.Timestamp) -> dict[str, Any]:
    return {
        "countries": [cfg.eea_air_country],
        "cities": [cfg.eea_air_city],
        "pollutants": ["PM2.5", "PM10", "NO2", "O3", "SO2"],
        "dataset": 1,
        "dateTimeStart": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "dateTimeEnd": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "aggregationType": "hour",
        "compress": False,
    }


def _parquet_urls(response: requests.Response) -> list[str]:
    text_value = response.content.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text_value))
    urls = [str(row.get("ParquetFileUrl") or "").strip() for row in reader]
    return list(dict.fromkeys(url for url in urls if url.startswith("https://")))


def _download_parquet(url: str) -> pd.DataFrame:
    session = build_session(retries=1)
    try:
        response = session.get(url, timeout=(10, 60))
        if not response.ok:
            return pd.DataFrame()
        frame = pd.read_parquet(
            io.BytesIO(response.content),
            columns=[
                "Samplingpoint",
                "Pollutant",
                "Start",
                "Value",
                "Unit",
                "Validity",
                "Verification",
                "ResultTime",
            ],
        )
    except Exception:  # noqa: BLE001 - a single station file must not fail the feed
        return pd.DataFrame()
    finally:
        session.close()
    return frame


def _latest_valid_measurements(
    frames: list[pd.DataFrame], cutoff: pd.Timestamp
) -> pd.DataFrame:
    prepared: list[pd.DataFrame] = []
    for frame in frames:
        if frame.empty:
            continue
        current = frame.copy()
        current["time"] = pd.to_datetime(current.get("Start"), utc=True, errors="coerce")
        current["pollutant_id"] = pd.to_numeric(
            current.get("Pollutant"), errors="coerce"
        ).astype("Int64")
        current["value"] = pd.to_numeric(current.get("Value"), errors="coerce")
        validity = pd.to_numeric(current.get("Validity"), errors="coerce")
        current = current[
            (current["time"] >= cutoff)
            & current["pollutant_id"].isin(POLLUTANTS)
            & current["value"].notna()
            & ((validity == 1) | validity.isna())
        ]
        if current.empty:
            continue
        current["sampling_point"] = current["Samplingpoint"].map(
            _normalise_sampling_point
        )
        prepared.append(current)
    if not prepared:
        return pd.DataFrame()
    result = pd.concat(prepared, ignore_index=True)
    return (
        result.sort_values("time")
        .drop_duplicates(["sampling_point", "pollutant_id"], keep="last")
        .reset_index(drop=True)
    )


def _metadata_map(content: bytes, wanted: set[str]) -> dict[str, dict[str, Any]]:
    metadata: dict[str, dict[str, Any]] = {}
    try:
        archive = zipfile.ZipFile(io.BytesIO(content))
        member = next(name for name in archive.namelist() if name.endswith("DataExtract.csv"))
        stream = io.TextIOWrapper(archive.open(member), encoding="utf-8-sig", newline="")
        for row in csv.DictReader(stream):
            sampling_point = _normalise_sampling_point(row.get("Sampling Point Id"))
            if sampling_point not in wanted:
                continue
            try:
                latitude = float(row.get("Latitude") or "nan")
                longitude = float(row.get("Longitude") or "nan")
            except ValueError:
                latitude = longitude = float("nan")
            metadata[sampling_point] = {
                "station_id": str(row.get("Air Quality Station EoI Code") or "").strip()
                or _station_code(sampling_point),
                "station_name": str(row.get("Air Quality Station Name") or "").strip()
                or _station_code(sampling_point),
                "latitude": latitude,
                "longitude": longitude,
            }
    except (zipfile.BadZipFile, StopIteration, OSError, UnicodeError, csv.Error):
        return {}
    return metadata


def normalise_eea_observations(
    measurements: pd.DataFrame,
    metadata: dict[str, dict[str, Any]],
    *,
    latitude: float,
    longitude: float,
    fetched_at: pd.Timestamp,
) -> pd.DataFrame:
    """Select the nearest fresh official station independently per pollutant."""
    if measurements.empty:
        return pd.DataFrame(columns=OBSERVATION_COLUMNS)
    rows: list[dict[str, Any]] = []
    for item in measurements.to_dict("records"):
        sampling_point = _normalise_sampling_point(item.get("sampling_point"))
        details = metadata.get(sampling_point, {})
        station_lat = pd.to_numeric(
            pd.Series([details.get("latitude")]), errors="coerce"
        ).iloc[0]
        station_lon = pd.to_numeric(
            pd.Series([details.get("longitude")]), errors="coerce"
        ).iloc[0]
        distance = (
            _distance_km(latitude, longitude, float(station_lat), float(station_lon))
            if pd.notna(station_lat) and pd.notna(station_lon)
            else float("inf")
        )
        pollutant_id = int(item["pollutant_id"])
        rows.append(
            {
                "source": "eea_utd_air",
                "station_id": details.get("station_id") or _station_code(sampling_point),
                "time": pd.Timestamp(item["time"]),
                "metric": POLLUTANTS[pollutant_id],
                "value": float(item["value"]),
                "unit": str(item.get("Unit") or UNITS[POLLUTANTS[pollutant_id]]),
                "station_name": details.get("station_name") or _station_code(sampling_point),
                "latitude": None if pd.isna(station_lat) else float(station_lat),
                "longitude": None if pd.isna(station_lon) else float(station_lon),
                "distance_km": None if not math.isfinite(distance) else distance,
                "_distance_sort": distance,
                "quality_flag": "UTD_preliminare",
                "is_modelled": 0,
                "fetched_at": fetched_at,
            }
        )
    candidates = pd.DataFrame(rows)
    # Prefer proximity first, then the freshest station when distances are equal.
    selected = (
        candidates.sort_values(["metric", "_distance_sort", "time"], ascending=[True, True, False])
        .drop_duplicates("metric", keep="first")
        .drop(columns="_distance_sort")
    )
    return selected.reindex(columns=OBSERVATION_COLUMNS).reset_index(drop=True)


def fetch_eea_observed_air(
    cfg: Settings = settings,
    session: requests.Session | None = None,
) -> pd.DataFrame:
    """Fetch preliminary hourly measurements reported by Italy to the EEA."""
    own_session = session is None
    session = session or build_session(retries=1)
    end = pd.Timestamp.now(tz="UTC")
    start = end - pd.Timedelta(hours=36)
    body = _request_body(cfg, start, end)
    try:
        url_response = _post_json(session, f"{EEA_API}/ParquetFile/urls", body)
        urls = _parquet_urls(url_response)
        if not urls:
            raise ObservedAirError("aria osservata EEA: nessuna serie per Roma")
        # A city request is normally ~45 small series.  The cap protects against
        # an accidental unfiltered response from the external service.
        urls = urls[:80]
        frames: list[pd.DataFrame] = []
        with ThreadPoolExecutor(max_workers=6) as executor:
            futures = [executor.submit(_download_parquet, url) for url in urls]
            for future in as_completed(futures):
                frame = future.result()
                if not frame.empty:
                    frames.append(frame)
        measurements = _latest_valid_measurements(frames, start)
        if measurements.empty:
            raise ObservedAirError("aria osservata EEA: nessuna misura recente valida")

        metadata_response = _post_json(
            session,
            EEA_METADATA_URL,
            {
                "Page": 0,
                "RequestFilter": {
                    "Country": {"FieldName": "Country", "Values": ["Italy"]}
                },
            },
            timeout=120,
        )
        metadata = _metadata_map(
            metadata_response.content, set(measurements["sampling_point"])
        )
        frame = normalise_eea_observations(
            measurements,
            metadata,
            latitude=cfg.latitude,
            longitude=cfg.longitude,
            fetched_at=end,
        )
    finally:
        if own_session:
            session.close()
    if frame.empty:
        raise ObservedAirError("aria osservata EEA: dati non interpretabili")
    return frame


def _iso(value: Any) -> str:
    timestamp = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(timestamp):
        raise ValueError("timestamp EEA non valido")
    return timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")


def archive_observed_air(
    frame: pd.DataFrame, engine: Engine | None = None
) -> int:
    if frame.empty:
        return 0
    ensure_schema()
    engine = engine or get_engine()
    records: list[dict[str, Any]] = []
    for row in frame.reindex(columns=OBSERVATION_COLUMNS).to_dict("records"):
        payload = dict(row)
        payload["time"] = _iso(payload["time"])
        payload["fetched_at"] = _iso(payload["fetched_at"])
        for column in ("latitude", "longitude", "distance_km", "value"):
            value = payload.get(column)
            payload[column] = None if value is None or pd.isna(value) else float(value)
        payload["is_modelled"] = int(payload.get("is_modelled") or 0)
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
                "is_modelled=excluded.is_modelled,fetched_at=excluded.fetched_at"
            ),
            records,
        )
    return len(records)


def refresh_observed_air(
    cfg: Settings = settings, engine: Engine | None = None
) -> tuple[pd.DataFrame, str | None]:
    """Refresh observed air independently; any error remains a visible warning."""
    if not cfg.eea_air_observations_enabled:
        record_source_disabled("eea_utd_air", engine=engine)
        return pd.DataFrame(columns=OBSERVATION_COLUMNS), None
    started = perf_counter()
    try:
        frame = fetch_eea_observed_air(cfg)
        archive_observed_air(frame, engine)
    except ObservedAirError as exc:
        record_source_result(
            "eea_utd_air",
            success=False,
            latency_ms=(perf_counter() - started) * 1000,
            error=exc,
            engine=engine,
        )
        return pd.DataFrame(columns=OBSERVATION_COLUMNS), str(exc)
    record_source_result(
        "eea_utd_air",
        success=True,
        rows_received=len(frame),
        last_observation_at=frame["time"].max(),
        latency_ms=(perf_counter() - started) * 1000,
        engine=engine,
    )
    return frame, None
