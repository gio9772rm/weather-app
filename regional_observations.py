"""Regional public observations used as optional secondary references.

ARSIAL publishes anonymous CSV exports through its SIARL/Superset portal.  The
connector discovers the saved charts instead of relying on one short-lived
download URL.  CFR support deliberately stays dormant until Regione Lazio
provides a documented endpoint; once configured it accepts ordinary CSV or
JSON without changing the forecast pipeline.
"""

from __future__ import annotations

import json
import math
import re
import unicodedata
from html.parser import HTMLParser
from io import StringIO
from typing import Any
from urllib.parse import urljoin, urlsplit

import numpy as np
import pandas as pd
import requests

from config import Settings, settings
from official_observations import (
    OBSERVATION_COLUMNS,
    OfficialObservationError,
    _distance_km,
    _number,
)

SOURCE_ARSIAL = "arsial_siarl"
SOURCE_CFR = "cfr_lazio"

ARSIAL_HEADERS = {
    "Accept": "text/html,application/json,text/csv;q=0.9,*/*;q=0.5",
    "Accept-Language": "it-IT,it;q=0.9,en;q=0.6",
    "Cache-Control": "no-cache",
}
ARSIAL_TIMEOUT = (5, 12)


class _SimpleTableParser(HTMLParser):
    """Extract server-rendered tables without adding an HTML dependency."""

    def __init__(self) -> None:
        super().__init__()
        self.tables: list[list[list[str]]] = []
        self._table: list[list[str]] | None = None
        self._row: list[str] | None = None
        self._cell: list[str] | None = None

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
        del attrs
        if tag == "table":
            self._table = []
        elif tag == "tr" and self._table is not None:
            self._row = []
        elif tag in {"th", "td"} and self._row is not None:
            self._cell = []

    def handle_data(self, data: str) -> None:
        if self._cell is not None:
            self._cell.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag in {"th", "td"} and self._cell is not None:
            assert self._row is not None
            self._row.append(" ".join(self._cell).strip())
            self._cell = None
        elif tag == "tr" and self._row is not None:
            if any(value for value in self._row):
                assert self._table is not None
                self._table.append(self._row)
            self._row = None
        elif tag == "table" and self._table is not None:
            if self._table:
                self.tables.append(self._table)
            self._table = None


def _label(value: Any) -> str:
    text_value = unicodedata.normalize("NFKD", str(value or ""))
    text_value = "".join(char for char in text_value if not unicodedata.combining(char))
    return re.sub(r"[^a-z0-9]+", " ", text_value.lower()).strip()


def _slug(value: Any) -> str:
    slug = re.sub(r"[^A-Z0-9]+", "-", _label(value).upper()).strip("-")
    return slug or "UNKNOWN"


def _column(
    frame: pd.DataFrame,
    *names: str,
    contains: tuple[str, ...] = (),
) -> str | None:
    labels = {_label(column): str(column) for column in frame.columns}
    for name in names:
        if _label(name) in labels:
            return labels[_label(name)]
    for normalised, original in labels.items():
        if contains and all(token in normalised for token in contains):
            return original
    return None


def _decimal(value: Any) -> float | None:
    if isinstance(value, str):
        cleaned = value.strip().replace("\u00a0", "")
        if not cleaned or cleaned.lower() in {"nan", "none", "null", "-"}:
            return None
        if "," in cleaned and "." in cleaned:
            if cleaned.rfind(",") > cleaned.rfind("."):
                cleaned = cleaned.replace(".", "").replace(",", ".")
            else:
                cleaned = cleaned.replace(",", "")
        else:
            cleaned = cleaned.replace(",", ".")
        match = re.search(r"[-+]?\d+(?:\.\d+)?", cleaned)
        value = match.group(0) if match else None
    return _number(value)


def _timestamp(value: Any, source_timezone: str) -> pd.Timestamp | None:
    timestamp = pd.to_datetime(value, errors="coerce")
    if pd.isna(timestamp):
        return None
    timestamp = pd.Timestamp(timestamp)
    if timestamp.tzinfo is None:
        try:
            timestamp = timestamp.tz_localize(
                source_timezone,
                ambiguous=False,
                nonexistent="shift_forward",
            )
        except (TypeError, ValueError):
            return None
    return timestamp.tz_convert("UTC")


def _row_timestamp(
    row: pd.Series,
    datetime_column: str | None,
    date_column: str | None,
    hour_column: str | None,
    source_timezone: str,
) -> pd.Timestamp | None:
    if datetime_column:
        parsed = _timestamp(row.get(datetime_column), source_timezone)
        if parsed is not None:
            return parsed
    if not date_column:
        return None
    date_value = str(row.get(date_column) or "").strip()
    hour_value = str(row.get(hour_column) or "00:00").strip() if hour_column else "00:00"
    if re.fullmatch(r"\d{1,2}", hour_value):
        hour_value = f"{int(hour_value):02d}:00"
    return _timestamp(f"{date_value} {hour_value}", source_timezone)


def _dewpoint(temp_c: Any, humidity: Any) -> float | None:
    temperature = _number(temp_c)
    relative = _number(humidity)
    if temperature is None or relative is None or relative <= 0:
        return None
    relative = float(np.clip(relative, 1.0, 100.0))
    gamma = math.log(relative / 100.0) + (17.625 * temperature) / (
        243.04 + temperature
    )
    return 243.04 * gamma / (17.625 - gamma)


def _metric_for_label(value: Any) -> str | None:
    normalised = _label(value)
    if not normalised:
        return None
    if "temperatura" in normalised and any(
        token in normalised for token in ("suolo", "terreno", "soil")
    ):
        return None
    if any(token in normalised for token in ("rugiada", "dew point", "dewpoint")):
        return "dewpoint_c"
    if any(token in normalised for token in ("umidita", "humidity")) and not any(
        token in normalised for token in ("suolo", "terreno", "soil")
    ):
        return "humidity"
    if any(token in normalised for token in ("pression", "pressure")):
        return "pressure_hpa"
    if any(token in normalised for token in ("raffica", "gust")):
        return "wind_gust_kmh"
    if ("direzione" in normalised or "direction" in normalised) and any(
        token in normalised for token in ("vento", "wind")
    ):
        return "wind_dir"
    if any(token in normalised for token in ("vento", "wind")) and any(
        token in normalised for token in ("velocita", "speed", "intensita")
    ):
        return "wind_kmh"
    if any(token in normalised for token in ("precipit", "pioggia", "rain")):
        return "rain_mm"
    if any(token in normalised for token in ("visibil", "visibility")):
        return "visibility_m"
    if any(token in normalised for token in ("nuvol", "cloud")):
        return "clouds"
    if any(token in normalised for token in ("temperatura", "temperature")):
        if any(token in normalised for token in ("massima", "max", "minima", "min")):
            return None
        return "temp_c"
    return None


def _converted_value(metric: str, value: Any, unit: Any = "") -> float | None:
    number = _decimal(value)
    if number is None:
        return None
    normalised_unit = _label(unit)
    if metric in {"wind_kmh", "wind_gust_kmh"} and (
        "m s" in normalised_unit or "mps" in normalised_unit
    ):
        number *= 3.6
    elif metric == "visibility_m" and "km" in normalised_unit:
        number *= 1000.0
    if metric in {"humidity", "clouds"}:
        number = float(np.clip(number, 0.0, 100.0))
    elif metric == "wind_dir":
        number %= 360.0
    elif metric in {"wind_kmh", "wind_gust_kmh", "rain_mm", "visibility_m"}:
        number = max(0.0, number)
    return number


def _frames_from_json(payload: Any) -> list[pd.DataFrame]:
    frames: list[pd.DataFrame] = []
    if isinstance(payload, list):
        if payload and all(isinstance(item, dict) for item in payload):
            frames.append(pd.DataFrame(payload))
        else:
            for item in payload:
                frames.extend(_frames_from_json(item))
    elif isinstance(payload, dict):
        for key in ("data", "records", "items", "values", "result"):
            if key in payload:
                frames.extend(_frames_from_json(payload[key]))
    return [frame for frame in frames if not frame.empty]


def _csv_frame(text_value: str) -> pd.DataFrame | None:
    stripped = text_value.lstrip("\ufeff\r\n\t ")
    if not stripped or stripped.startswith("<"):
        return None
    for separator in (None, ";", ",", "\t"):
        try:
            frame = pd.read_csv(
                StringIO(text_value),
                sep=separator,
                engine="python",
            )
        except (ValueError, pd.errors.ParserError, UnicodeError):
            continue
        frame = frame.loc[:, ~frame.columns.astype(str).str.startswith("Unnamed")]
        if not frame.empty and len(frame.columns) >= 2:
            return frame
    return None


def _html_frames(text_value: str) -> list[pd.DataFrame]:
    parser = _SimpleTableParser()
    try:
        parser.feed(text_value)
    except (AssertionError, ValueError):
        return []
    frames: list[pd.DataFrame] = []
    for rows in parser.tables:
        if len(rows) < 2:
            continue
        width = len(rows[0])
        body = [row for row in rows[1:] if len(row) == width]
        if body:
            frames.append(pd.DataFrame(body, columns=rows[0]))
    return frames


def _response_frames(response: requests.Response) -> list[pd.DataFrame]:
    content_type = str(response.headers.get("content-type") or "").lower()
    if "json" in content_type:
        try:
            frames = _frames_from_json(response.json())
        except ValueError:
            frames = []
        if frames:
            return frames
    content = response.content
    try:
        text_value = content.decode("utf-8-sig")
    except UnicodeDecodeError:
        text_value = content.decode("latin-1", errors="replace")
    frame = _csv_frame(text_value)
    if frame is not None:
        return [frame]
    if "html" in content_type or text_value.lstrip().startswith("<"):
        return _html_frames(text_value)
    return []


def _chart_ids(payload: Any) -> set[int]:
    identifiers: set[int] = set()
    if isinstance(payload, list):
        for item in payload:
            identifiers.update(_chart_ids(item))
    elif isinstance(payload, dict):
        looks_like_chart = any(
            key in payload
            for key in ("slice_name", "viz_type", "form_data", "params")
        )
        for key, value in payload.items():
            normalised_key = _label(key)
            if normalised_key in {"slice id", "chart id"} or (
                normalised_key == "id" and looks_like_chart
            ):
                try:
                    identifiers.add(int(value))
                except (TypeError, ValueError):
                    pass
            elif isinstance(value, (dict, list)):
                identifiers.update(_chart_ids(value))
    return identifiers


def _dashboard_parts(url: str) -> tuple[str, str]:
    split = urlsplit(url)
    match = re.search(r"(?P<prefix>/.*?)/superset/dashboard/(?P<id>[^/?#]+)", split.path)
    if not match:
        match = re.search(r"(?P<prefix>)/superset/dashboard/(?P<id>[^/?#]+)", split.path)
    if not match:
        raise OfficialObservationError("ARSIAL: indirizzo dashboard non valido")
    prefix = match.group("prefix").rstrip("/")
    return f"{split.scheme}://{split.netloc}{prefix}", match.group("id")


def _get(
    session: requests.Session,
    url: str,
    *,
    params: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: tuple[float, float] = (8, 25),
) -> requests.Response:
    try:
        response = session.get(
            url,
            params=params,
            headers=headers,
            timeout=timeout,
        )
    except requests.RequestException as exc:
        raise OfficialObservationError("servizio non raggiungibile") from exc
    return response


def _optional_get(
    session: requests.Session,
    url: str,
    *,
    params: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: tuple[float, float] = (8, 25),
) -> requests.Response | None:
    try:
        return _get(
            session,
            url,
            params=params,
            headers=headers,
            timeout=timeout,
        )
    except OfficialObservationError:
        return None


def _arsial_chart_frames(
    session: requests.Session,
    base: str,
    identifiers: set[int],
) -> list[pd.DataFrame]:
    """Try saved Superset charts without depending on the dashboard page."""
    frames: list[pd.DataFrame] = []
    export_headers = {
        **ARSIAL_HEADERS,
        "Accept": "text/csv,application/json;q=0.9,*/*;q=0.5",
        "X-Requested-With": "XMLHttpRequest",
    }
    for chart_id in sorted(identifiers)[:24]:
        form_data: dict[str, Any] = {"slice_id": chart_id}
        metadata = _optional_get(
            session,
            f"{base}/api/v1/chart/{chart_id}",
            headers=ARSIAL_HEADERS,
            timeout=ARSIAL_TIMEOUT,
        )
        if metadata is not None and metadata.ok:
            try:
                result = metadata.json().get("result") or {}
                params = result.get("params")
                if isinstance(params, str):
                    parsed = json.loads(params)
                    if isinstance(parsed, dict):
                        form_data.update(parsed)
                form_data["slice_id"] = chart_id
            except (ValueError, AttributeError):
                pass
        for endpoint in (
            f"{base}/superset/explore_json/",
            f"{base}/superset/explore_json",
        ):
            exported = _optional_get(
                session,
                endpoint,
                params={"form_data": json.dumps(form_data), "csv": "true"},
                headers=export_headers,
                timeout=ARSIAL_TIMEOUT,
            )
            if exported is not None and exported.ok:
                frames.extend(_response_frames(exported))
                if any(_frame_has_weather_data(frame) for frame in frames):
                    break
    return frames


def _arsial_export_frames(
    cfg: Settings,
    session: requests.Session,
) -> list[pd.DataFrame]:
    if cfg.arsial_csv_url:
        response = _get(session, cfg.arsial_csv_url)
        if not response.ok:
            raise OfficialObservationError(
                f"ARSIAL: risposta HTTP {response.status_code}"
            )
        frames = _response_frames(response)
        if not frames:
            raise OfficialObservationError("ARSIAL: CSV pubblico non interpretabile")
        return frames

    base, dashboard_id = _dashboard_parts(cfg.arsial_dashboard_url)
    identifiers = set(cfg.arsial_chart_ids)
    frames: list[pd.DataFrame] = []
    dashboard_text = ""
    dashboard_response: requests.Response | None = None
    reachable = False

    # Superset's HTML dashboard is occasionally slow while its JSON/chart
    # endpoints remain available. Discover those endpoints first so one page
    # timeout cannot take the entire ARSIAL source offline.
    for endpoint in (
        f"{base}/api/v1/dashboard/{dashboard_id}/charts",
        f"{base}/api/v1/dashboard/{dashboard_id}",
    ):
        candidate = _optional_get(
            session,
            endpoint,
            headers=ARSIAL_HEADERS,
            timeout=ARSIAL_TIMEOUT,
        )
        if candidate is None:
            continue
        reachable = True
        if not candidate.ok:
            continue
        frames.extend(_response_frames(candidate))
        try:
            payload = candidate.json()
        except ValueError:
            continue
        identifiers.update(_chart_ids(payload))

    if identifiers:
        frames.extend(_arsial_chart_frames(session, base, identifiers))
        useful = [frame for frame in frames if _frame_has_weather_data(frame)]
        if useful:
            return useful

    response = _optional_get(
        session,
        cfg.arsial_dashboard_url,
        headers=ARSIAL_HEADERS,
        timeout=ARSIAL_TIMEOUT,
    )
    if response is not None:
        reachable = True
        if response.ok:
            dashboard_response = response
            dashboard_text = response.text
            frames.extend(_response_frames(response))
            base, dashboard_id = _dashboard_parts(
                str(response.url or cfg.arsial_dashboard_url)
            )
            identifiers.update(
                int(value)
                for value in re.findall(
                    r'(?:(?:"|&quot;)(?:slice_id|chartId|chart_id)(?:"|&quot;))'
                    r"\s*:\s*(\d+)",
                    dashboard_text,
                )
            )

    links = re.findall(
        r'href=["\']([^"\']*(?:\.csv|explore_json)[^"\']*)["\']',
        dashboard_text,
        flags=re.IGNORECASE,
    )
    for link in links[:12]:
        export_url = urljoin(
            str(
                dashboard_response.url
                if dashboard_response is not None
                else cfg.arsial_dashboard_url
            ),
            link,
        )
        if urlsplit(export_url).netloc != urlsplit(cfg.arsial_dashboard_url).netloc:
            continue
        exported = _optional_get(
            session,
            export_url,
            headers=ARSIAL_HEADERS,
            timeout=ARSIAL_TIMEOUT,
        )
        if exported is not None and exported.ok:
            frames.extend(_response_frames(exported))

    frames.extend(_arsial_chart_frames(session, base, identifiers))

    useful = [frame for frame in frames if _frame_has_weather_data(frame)]
    if not useful:
        if not reachable:
            raise OfficialObservationError("ARSIAL: servizio non raggiungibile")
        raise OfficialObservationError(
            "ARSIAL: esportazione pubblica temporaneamente non disponibile"
        )
    return useful


def _frame_has_weather_data(frame: pd.DataFrame) -> bool:
    labels = [_label(column) for column in frame.columns]
    has_time = any(
        any(token in label for token in ("data", "ora", "time", "timestamp"))
        for label in labels
    )
    has_value = any(_metric_for_label(label) for label in labels)
    has_long_value = any(label in {"valore", "misura", "value"} for label in labels)
    has_variable = any(
        label in {"grandezza", "variabile", "descrizione grandezza"}
        or "short name" in label
        for label in labels
    )
    return has_time and (has_value or (has_long_value and has_variable))


def _registry_frame(
    cfg: Settings,
    session: requests.Session,
) -> pd.DataFrame:
    if not cfg.arsial_station_registry_url:
        return pd.DataFrame()
    response = _optional_get(session, cfg.arsial_station_registry_url)
    if response is None:
        return pd.DataFrame()
    if not response.ok:
        return pd.DataFrame()
    frames = _response_frames(response)
    return frames[0] if frames else pd.DataFrame()


def _station_metadata(
    registry: pd.DataFrame,
    station_name: str,
) -> dict[str, Any]:
    fallback = {
        "station_id": f"ARSIAL-{_slug(station_name)}",
        "station_name": station_name,
        "latitude": None,
        "longitude": None,
        "elevation_m": None,
    }
    if registry.empty:
        return fallback
    name_column = _column(registry, "Nome stazione", "Stazione")
    if not name_column:
        return fallback
    target = _label(station_name)
    matches = registry[
        registry[name_column].astype(str).map(_label).map(
            lambda value: value == target or target in value or value in target
        )
    ]
    if matches.empty:
        return fallback
    row = matches.iloc[0]
    code_column = _column(registry, "Cod staz", "Codice Stazione")
    latitude_column = _column(registry, "lat", "latitude", "latitudine")
    longitude_column = _column(registry, "lon", "longitude", "longitudine")
    elevation_column = _column(registry, "ALTITUDINE", "elevation", "quota")
    station_code = row.get(code_column) if code_column else None
    if _decimal(station_code) is not None:
        station_code = str(int(float(station_code)))
    return {
        "station_id": f"ARSIAL-{station_code or _slug(row.get(name_column))}",
        "station_name": str(row.get(name_column) or station_name),
        "latitude": _decimal(row.get(latitude_column)) if latitude_column else None,
        "longitude": _decimal(row.get(longitude_column)) if longitude_column else None,
        "elevation_m": _decimal(row.get(elevation_column)) if elevation_column else None,
    }


def _base_observation(
    *,
    source: str,
    station_id: str,
    station_name: str,
    observed: pd.Timestamp,
    fetched: pd.Timestamp,
    latitude: float | None,
    longitude: float | None,
    elevation_m: float | None,
    cfg: Settings,
    quality: str = "official",
    raw: str = "",
) -> dict[str, Any]:
    distance = (
        _distance_km(cfg.latitude, cfg.longitude, latitude, longitude)
        if latitude is not None and longitude is not None
        else None
    )
    row = dict.fromkeys(OBSERVATION_COLUMNS)
    row.update(
        {
            "source": source,
            "station_id": station_id,
            "time": observed,
            "station_name": station_name,
            "latitude": latitude,
            "longitude": longitude,
            "elevation_m": elevation_m,
            "distance_km": distance,
            "quality_flag": quality,
            "raw_observation": raw[:1000],
            "fetched_at": fetched,
        }
    )
    return row


def _table_rows(
    frame: pd.DataFrame,
    *,
    source: str,
    cfg: Settings,
    fetched: pd.Timestamp,
    source_timezone: str,
    default_metadata: dict[str, Any],
    station_filter: str = "",
    allowed_station_ids: tuple[str, ...] = (),
) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    station_column = _column(
        frame,
        "Stazione",
        "Nome stazione",
        "Denominazione stazione",
        "station_name",
    )
    station_id_column = _column(
        frame,
        "Cod staz",
        "Codice Stazione",
        "station_id",
        "elementId",
    )
    datetime_column = _column(
        frame,
        "Data rilevazione",
        "data_ora",
        "datetime",
        "timestamp",
        "time",
    )
    date_column = _column(frame, "Data", "date", "giorno")
    hour_column = _column(frame, "Ora", "hour")
    variable_column = _column(
        frame,
        "Grandezza",
        "Short-Name Grandezza",
        "Descrizione Grandezza",
        "Variabile",
        "variable",
    )
    value_column = _column(frame, "Valore", "Misura", "value")
    unit_column = _column(frame, "Unità di misura", "unita", "unit")
    quality_column = _column(
        frame,
        "Indice di validità",
        "validita",
        "quality",
        "quality_flag",
    )
    latitude_column = _column(frame, "lat", "latitude", "latitudine")
    longitude_column = _column(frame, "lon", "longitude", "longitudine")
    elevation_column = _column(frame, "quota", "altitudine", "elevation_m")

    target = _label(station_filter)
    allowed = {str(value).upper() for value in allowed_station_ids}
    output: list[dict[str, Any]] = []
    for _, source_row in frame.iterrows():
        name = str(
            source_row.get(station_column)
            if station_column
            else default_metadata.get("station_name")
            or ""
        ).strip()
        raw_station_id = (
            source_row.get(station_id_column) if station_id_column else None
        )
        station_id_text = str(raw_station_id or "").strip()
        if target and name and not (
            _label(name) == target
            or target in _label(name)
            or _label(name) in target
        ):
            continue
        if allowed and station_id_text.upper() not in allowed:
            continue
        observed = _row_timestamp(
            source_row,
            datetime_column,
            date_column,
            hour_column,
            source_timezone,
        )
        if observed is None:
            continue
        station_id = station_id_text or str(default_metadata.get("station_id") or "")
        if not station_id:
            station_id = f"{source.upper()}-{_slug(name)}"
        elif source == SOURCE_ARSIAL and not station_id.startswith("ARSIAL-"):
            station_id = f"ARSIAL-{station_id}"
        latitude = (
            _decimal(source_row.get(latitude_column))
            if latitude_column
            else default_metadata.get("latitude")
        )
        longitude = (
            _decimal(source_row.get(longitude_column))
            if longitude_column
            else default_metadata.get("longitude")
        )
        elevation = (
            _decimal(source_row.get(elevation_column))
            if elevation_column
            else default_metadata.get("elevation_m")
        )
        quality_value = source_row.get(quality_column) if quality_column else None
        quality = (
            f"official_qc:{str(quality_value).strip()}"
            if quality_value not in (None, "") and not pd.isna(quality_value)
            else "official"
        )
        raw = json.dumps(
            {str(key): value for key, value in source_row.items()},
            ensure_ascii=False,
            default=str,
        )
        observation = _base_observation(
            source=source,
            station_id=station_id,
            station_name=name or str(default_metadata.get("station_name") or station_id),
            observed=observed,
            fetched=fetched,
            latitude=latitude,
            longitude=longitude,
            elevation_m=elevation,
            cfg=cfg,
            quality=quality,
            raw=raw,
        )

        if variable_column and value_column:
            metric = _metric_for_label(source_row.get(variable_column))
            if metric:
                value = _converted_value(
                    metric,
                    source_row.get(value_column),
                    source_row.get(unit_column) if unit_column else "",
                )
                if value is not None:
                    observation[metric] = value
        else:
            candidates: dict[str, tuple[int, Any, Any]] = {}
            for column in frame.columns:
                metric = _metric_for_label(column)
                if not metric:
                    continue
                normalised = _label(column)
                priority = 1
                if any(token in normalised for token in ("media", "medio", "med")):
                    priority += 3
                if metric == "wind_gust_kmh" and "max" in normalised:
                    priority += 2
                previous = candidates.get(metric)
                if previous is None or priority > previous[0]:
                    candidates[metric] = (priority, source_row.get(column), column)
            for metric, (_, value, label_value) in candidates.items():
                converted = _converted_value(metric, value, label_value)
                if converted is not None:
                    observation[metric] = converted

        if any(observation.get(metric) is not None for metric in _weather_columns()):
            output.append(observation)
    return output


def _weather_columns() -> tuple[str, ...]:
    return (
        "temp_c",
        "dewpoint_c",
        "humidity",
        "pressure_hpa",
        "wind_kmh",
        "wind_gust_kmh",
        "wind_dir",
        "rain_mm",
        "clouds",
        "visibility_m",
    )


def _combine_rows(rows: list[dict[str, Any]]) -> pd.DataFrame:
    combined: dict[tuple[str, str, pd.Timestamp], dict[str, Any]] = {}
    for row in rows:
        key = (str(row["source"]), str(row["station_id"]), row["time"])
        target = combined.setdefault(key, dict(row))
        for column, value in row.items():
            if value is not None and not (isinstance(value, float) and math.isnan(value)):
                target[column] = value
    for row in combined.values():
        if row.get("dewpoint_c") is None:
            row["dewpoint_c"] = _dewpoint(row.get("temp_c"), row.get("humidity"))
        if row.get("rain_mm") is not None:
            row["rain_mm"] = max(0.0, float(row["rain_mm"]))
            row["precip_observed"] = int(row["rain_mm"] > 0)
    if not combined:
        return pd.DataFrame(columns=OBSERVATION_COLUMNS)
    frame = pd.DataFrame(combined.values()).reindex(columns=OBSERVATION_COLUMNS)
    return frame.sort_values("time").drop_duplicates(
        ["source", "station_id", "time"], keep="last"
    )


def parse_arsial_tables(
    tables: list[pd.DataFrame],
    cfg: Settings = settings,
    registry: pd.DataFrame | None = None,
    fetched_at: pd.Timestamp | None = None,
) -> pd.DataFrame:
    fetched = pd.to_datetime(fetched_at or pd.Timestamp.now(tz="UTC"), utc=True)
    metadata = _station_metadata(
        registry if registry is not None else pd.DataFrame(),
        cfg.arsial_station_name,
    )
    rows: list[dict[str, Any]] = []
    for table in tables:
        rows.extend(
            _table_rows(
                table,
                source=SOURCE_ARSIAL,
                cfg=cfg,
                fetched=fetched,
                # The public hourly SIARL dashboard explicitly reports UTC.
                # Keeping storage in UTC avoids a seasonal one/two-hour shift;
                # the UI converts to Europe/Rome only when displaying values.
                source_timezone=cfg.arsial_timezone,
                default_metadata=metadata,
                station_filter=cfg.arsial_station_name,
            )
        )
    return _combine_rows(rows)


def fetch_arsial_observations(
    cfg: Settings = settings,
    session: requests.Session | None = None,
) -> pd.DataFrame:
    if not cfg.arsial_observations_enabled:
        return pd.DataFrame(columns=OBSERVATION_COLUMNS)
    own_session = session is None
    if session is None:
        from forecast_providers import build_session

        session = build_session(retries=1)
    try:
        try:
            tables = _arsial_export_frames(cfg, session)
            registry = _registry_frame(cfg, session)
            frame = parse_arsial_tables(tables, cfg, registry)
        except OfficialObservationError as exc:
            message = str(exc)
            if not message.startswith("ARSIAL:"):
                message = f"ARSIAL: {message}"
            raise OfficialObservationError(message) from exc
        if frame.empty:
            raise OfficialObservationError(
                "ARSIAL: nessuna osservazione oraria valida per la stazione scelta"
            )
        return frame
    finally:
        if own_session:
            session.close()


def parse_cfr_frames(
    tables: list[pd.DataFrame],
    cfg: Settings = settings,
    fetched_at: pd.Timestamp | None = None,
) -> pd.DataFrame:
    fetched = pd.to_datetime(fetched_at or pd.Timestamp.now(tz="UTC"), utc=True)
    rows: list[dict[str, Any]] = []
    for table in tables:
        rows.extend(
            _table_rows(
                table,
                source=SOURCE_CFR,
                cfg=cfg,
                fetched=fetched,
                source_timezone=cfg.local_timezone,
                default_metadata={
                    "station_id": "",
                    "station_name": "Stazione CFR Lazio",
                    "latitude": None,
                    "longitude": None,
                    "elevation_m": None,
                },
                allowed_station_ids=cfg.cfr_station_ids,
            )
        )
    return _combine_rows(rows)


def fetch_cfr_observations(
    cfg: Settings = settings,
    session: requests.Session | None = None,
) -> pd.DataFrame:
    """Read the future official CFR endpoint only after explicit activation."""
    if not cfg.cfr_observations_enabled:
        return pd.DataFrame(columns=OBSERVATION_COLUMNS)
    if not cfg.cfr_observations_url:
        raise OfficialObservationError(
            "CFR Lazio: connettore in attesa dell'endpoint ufficiale"
        )
    if not cfg.cfr_observations_url.lower().startswith("https://"):
        raise OfficialObservationError("CFR Lazio: l'endpoint deve usare HTTPS")
    own_session = session is None
    if session is None:
        from forecast_providers import build_session

        session = build_session()
    headers = {"Accept": "application/json,text/csv;q=0.9,*/*;q=0.5"}
    if cfg.cfr_api_token:
        headers["Authorization"] = f"Bearer {cfg.cfr_api_token}"
    try:
        response = _get(
            session,
            cfg.cfr_observations_url,
            headers=headers,
        )
        if not response.ok:
            raise OfficialObservationError(
                f"CFR Lazio: risposta HTTP {response.status_code}"
            )
        frames = _response_frames(response)
        if not frames:
            raise OfficialObservationError("CFR Lazio: risposta non interpretabile")
        observations = parse_cfr_frames(frames, cfg)
        if observations.empty:
            raise OfficialObservationError(
                "CFR Lazio: nessuna osservazione valida nell'endpoint"
            )
        return observations
    except OfficialObservationError as exc:
        message = str(exc)
        if not message.startswith("CFR Lazio:"):
            message = f"CFR Lazio: {message}"
        raise OfficialObservationError(message) from exc
    finally:
        if own_session:
            session.close()
