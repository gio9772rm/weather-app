"""Sanitised Ecowitt telemetry and sensor-level diagnostics."""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import Any

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from db import ensure_schema, get_engine

SECRET_WORDS = ("api_key", "application_key", "password", "secret", "token", "mac")
METRIC_DEFINITIONS = (
    ("Temperatura", "temp_c", "°C"),
    ("Umidità", "humidity", "%"),
    ("Pressione", "pressure_hpa", "hPa"),
    ("Vento", "wind_kmh", "km/h"),
    ("Pioggia", "rain_rate_mm_h", "mm/h"),
    ("Radiazione solare", "solar_w_m2", "W/m²"),
    ("Indice UV", "uv_index", "UV"),
)
STATUS_RANK = {"online": 0, "warning": 1, "delayed": 2, "offline": 3}


@dataclass(frozen=True)
class EcowittDiagnosticSummary:
    status: str
    online: int
    warning: int
    offline: int
    average_coverage: float


def _safe_number(value: Any) -> float | None:
    if isinstance(value, dict):
        value = value.get("value", value.get("val", value.get("v")))
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _safe_name(value: Any) -> str:
    name = re.sub(r"[^a-z0-9_-]+", "_", str(value or "sensor").lower()).strip("_")
    return (name or "sensor")[:80]


def _battery_text_status(value: Any) -> str | None:
    if isinstance(value, dict):
        value = value.get("value", value.get("val", value.get("v")))
    if value is None or isinstance(value, (int, float)):
        return None
    rendered = str(value).strip().casefold()
    if not rendered:
        return None
    if rendered in {"normal", "normale", "ok", "good", "charged", "carica"}:
        return "ok"
    try:
        float(rendered)
    except ValueError:
        # Ecowitt uses "Normal" for a charged Sensor Array. Any other textual
        # battery state is therefore a real warning, not an unknown value.
        return "critical"
    return None


def _battery_status(value: float | None, unit: str) -> str:
    if value is None:
        return "unknown"
    normalized = unit.casefold()
    if "%" in normalized or "percent" in normalized:
        return "critical" if value <= 20 else "warning" if value <= 35 else "ok"
    if "v" in normalized or 0 < value <= 5:
        if value < 2:
            return "critical" if value <= 1.05 else "warning" if value <= 1.25 else "ok"
        return "critical" if value <= 2.35 else "warning" if value <= 2.55 else "ok"
    return "unknown"


def telemetry_sensor_label(value: Any) -> str:
    """Return a readable device label without exposing a hardware identifier."""
    sensor = _safe_name(value)
    if any(code in sensor for code in ("wh65", "wh69", "wh80", "wh90")):
        return "Sensor Array"
    if "console" in sensor or "gateway" in sensor or "gw" in sensor:
        return "Console / gateway"
    return (
        sensor.replace("battery_", "").replace("radio_", "").replace("_", " ").title()
    )


def _signal_status(value: float | None, unit: str) -> str:
    if value is None:
        return "unknown"
    normalized = unit.casefold()
    if "dbm" in normalized or value < 0:
        return "critical" if value <= -90 else "warning" if value <= -75 else "ok"
    if "%" in normalized:
        return "critical" if value <= 20 else "warning" if value <= 40 else "ok"
    return "unknown"


def extract_telemetry(
    payload: dict[str, Any],
    *,
    station_id: str,
    fetched_at: Any = None,
) -> list[dict[str, Any]]:
    """Extract battery/signal values without retaining identifiers or raw JSON."""
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, dict):
        return []
    fallback_time = pd.to_datetime(
        payload.get("time") or fetched_at or pd.Timestamp.now(tz="UTC"),
        utc=True,
        errors="coerce",
    )
    if pd.isna(fallback_time):
        fallback_time = pd.Timestamp.now(tz="UTC")
    fetched = pd.to_datetime(
        fetched_at or pd.Timestamp.now(tz="UTC"), utc=True, errors="coerce"
    )
    if pd.isna(fetched):
        fetched = pd.Timestamp.now(tz="UTC")
    rows: list[dict[str, Any]] = []

    def walk(node: Any, path: tuple[str, ...], metric_hint: str | None = None) -> None:
        if len(path) > 5:
            return
        if not isinstance(node, dict):
            if metric_hint is None:
                return
            node = {"value": node}
        lowered_path = ".".join(path).casefold()
        if any(word in lowered_path for word in SECRET_WORDS):
            return
        raw_value = node.get("value", node.get("val", node.get("v")))
        value = _safe_number(node)
        text_battery_status = (
            _battery_text_status(raw_value) if metric_hint == "battery" else None
        )
        unit = str(node.get("unit") or node.get("u") or "")[:16]
        if metric_hint and (value is not None or text_battery_status is not None):
            node_time = pd.to_datetime(
                node.get("time") or node.get("timestamp") or fallback_time,
                utc=True,
                errors="coerce",
            )
            if pd.isna(node_time):
                node_time = fallback_time
            sensor_path = path[:-1] if path[-1] in {"value", "val", "v"} else path
            sensor = _safe_name("_".join(sensor_path[-2:]))
            status = (
                text_battery_status or _battery_status(value, unit)
                if metric_hint == "battery"
                else _signal_status(value, unit)
            )
            rows.append(
                {
                    "station_id": _safe_name(station_id),
                    "observed_at": node_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "sensor": sensor,
                    "metric": metric_hint,
                    "value": value,
                    "unit": unit or ("stato" if text_battery_status else ""),
                    "status": status,
                    "fetched_at": fetched.strftime("%Y-%m-%dT%H:%M:%SZ"),
                }
            )
            return
        for key, child in node.items():
            name = str(key).casefold()
            if any(word in name for word in SECRET_WORDS):
                continue
            hint = metric_hint
            if "batt" in name or "battery" in name:
                hint = "battery"
            elif "rssi" in name or "signal" in name:
                hint = "signal"
            walk(child, (*path, str(key)), hint)

    walk(data, ())
    unique: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for row in rows:
        key = (row["station_id"], row["observed_at"], row["sensor"], row["metric"])
        unique[key] = row
    return list(unique.values())


def upsert_telemetry(rows: list[dict[str, Any]], engine: Engine | None = None) -> int:
    if not rows:
        return 0
    target = engine or get_engine()
    statement = text(
        "INSERT INTO ecowitt_telemetry (station_id,observed_at,sensor,metric,value,"
        "unit,status,fetched_at) VALUES (:station_id,:observed_at,:sensor,:metric,"
        ":value,:unit,:status,:fetched_at) ON CONFLICT "
        "(station_id,observed_at,sensor,metric) DO UPDATE SET value=excluded.value,"
        "unit=excluded.unit,status=excluded.status,fetched_at=excluded.fetched_at"
    )
    with target.begin() as connection:
        connection.execute(statement, rows)
    return len(rows)


def archive_telemetry_safely(
    rows: list[dict[str, Any]], engine: Engine | None = None
) -> tuple[int, str]:
    """Persist optional telemetry without ever blocking primary observations."""
    try:
        return upsert_telemetry(rows, engine), ""
    except Exception:  # noqa: BLE001 - diagnostics are deliberately non-blocking
        return 0, "Telemetria diagnostica Ecowitt non archiviata"


def _age_minutes(now: pd.Timestamp, value: Any) -> float:
    timestamp = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(timestamp):
        return float("inf")
    return max(0.0, (now - timestamp).total_seconds() / 60.0)


def diagnose_observations(
    frame: pd.DataFrame,
    *,
    now: pd.Timestamp | None = None,
    hours: int = 24,
    stale_minutes: int = 20,
) -> pd.DataFrame:
    """Evaluate freshness, completeness, gaps and quality flags per sensor."""
    current = now if now is not None else pd.Timestamp.now(tz="UTC")
    expected = max(1, int(hours) * 6)
    if frame.empty:
        return pd.DataFrame(
            [
                {
                    "sensor": label,
                    "metric": column,
                    "status": "offline",
                    "last_time": pd.NaT,
                    "age_minutes": float("inf"),
                    "last_value": float("nan"),
                    "unit": unit,
                    "coverage": 0.0,
                    "largest_gap_minutes": float("nan"),
                    "quality_flags": 0,
                    "quality_note": "Nessun dato disponibile",
                }
                for label, column, unit in METRIC_DEFINITIONS
            ]
        )
    data = frame.copy()
    data["time"] = pd.to_datetime(data.get("time"), utc=True, errors="coerce")
    cutoff = current - pd.Timedelta(hours=hours)
    data = data[data["time"].between(cutoff, current + pd.Timedelta(minutes=10))]
    quality = data.get("data_quality", pd.Series("ok", index=data.index)).fillna("ok")
    rows: list[dict[str, Any]] = []
    for label, column, unit in METRIC_DEFINITIONS:
        values = pd.to_numeric(
            data.get(column, pd.Series(float("nan"), index=data.index)),
            errors="coerce",
        )
        available = data.loc[values.notna(), ["time"]].copy()
        available["value"] = values[values.notna()]
        last_time = available["time"].max() if not available.empty else pd.NaT
        age = _age_minutes(current, last_time)
        buckets = available["time"].dt.floor("10min").nunique()
        gaps = available["time"].sort_values().diff().dt.total_seconds().div(60)
        quality_text = quality.astype(str)
        flag_patterns = {
            "Valore invariato troppo a lungo": rf"stuck_{re.escape(column)}",
            "Variazione troppo rapida": rf"spike_{re.escape(column)}",
            "Valore fuori scala scartato": rf"range_filtered_{re.escape(column)}",
        }
        reason_masks = {
            reason: quality_text.str.contains(pattern, regex=True)
            for reason, pattern in flag_patterns.items()
        }
        flags = pd.Series(False, index=data.index)
        for mask in reason_masks.values():
            flags |= mask
        direct_suspicious = pd.Series(False, index=data.index)
        direct_reason = ""
        if column in {"temp_c", "humidity"} and not available.empty:
            limits = {"temp_c": (-45.0, 55.0), "humidity": (0.0, 100.0)}
            minimum, maximum = limits[column]
            direct_suspicious = values.notna() & ~values.between(minimum, maximum)
            elapsed = data["time"].diff().dt.total_seconds().div(3600)
            rate_limit = 15.0 if column == "temp_c" else 60.0
            direct_suspicious |= values.diff().abs().div(elapsed).gt(
                rate_limit
            ) & elapsed.between(1 / 120, 1.0)
            if direct_suspicious.any():
                direct_reason = "Valore o variazione non plausibile"
        flags |= direct_suspicious
        reasons = [reason for reason, mask in reason_masks.items() if bool(mask.any())]
        if direct_reason and direct_reason not in reasons:
            reasons.append(direct_reason)
        status = "online"
        if not math.isfinite(age) or age > stale_minutes * 3:
            status = "offline"
        elif age > stale_minutes:
            status = "delayed"
        elif int(flags.sum()) > 0 or buckets / expected < 0.8:
            status = "warning"
        rows.append(
            {
                "sensor": label,
                "metric": column,
                "status": status,
                "last_time": last_time,
                "age_minutes": age,
                "last_value": (
                    float(available.sort_values("time").iloc[-1]["value"])
                    if not available.empty
                    else float("nan")
                ),
                "unit": unit,
                "coverage": min(100.0, buckets / expected * 100.0),
                "largest_gap_minutes": float(gaps.max()) if not gaps.empty else 0.0,
                "quality_flags": int(flags.sum()),
                "quality_note": (
                    " · ".join(reasons) if reasons else "Nessuna anomalia recente"
                ),
            }
        )
    return pd.DataFrame(rows)


def load_ecowitt_diagnostics(
    *,
    station_id: str,
    hours: int = 24,
    stale_minutes: int = 20,
    engine: Engine | None = None,
    now: pd.Timestamp | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, EcowittDiagnosticSummary]:
    """Load current sensor and device diagnostics from the shared database."""
    ensure_schema()
    target = engine or get_engine()
    current = now if now is not None else pd.Timestamp.now(tz="UTC")
    cutoff = (current - pd.Timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%SZ")
    with target.connect() as connection:
        station_role = connection.execute(
            text("SELECT role FROM station_profiles WHERE station_id=:station_id"),
            {"station_id": _safe_name(station_id)},
        ).scalar()
        if station_role == "secondary":
            observation_query = text(
                "SELECT * FROM station_observations "
                "WHERE station_id=:station_id AND time>=:cutoff ORDER BY time"
            )
            observation_params = {
                "station_id": _safe_name(station_id),
                "cutoff": cutoff,
            }
        else:
            observation_query = text(
                "SELECT * FROM station_raw WHERE time>=:cutoff ORDER BY time"
            )
            observation_params = {"cutoff": cutoff}
        observations = pd.read_sql(
            observation_query,
            connection,
            params=observation_params,
        )
        telemetry = pd.read_sql(
            text(
                "SELECT * FROM ecowitt_telemetry WHERE station_id=:station_id "
                "ORDER BY observed_at DESC"
            ),
            connection,
            params={"station_id": _safe_name(station_id)},
        )
    observations.columns = [str(column).lower() for column in observations.columns]
    telemetry.columns = [str(column).lower() for column in telemetry.columns]
    sensors = diagnose_observations(
        observations,
        now=current,
        hours=hours,
        stale_minutes=stale_minutes,
    )
    if not telemetry.empty:
        telemetry["observed_at"] = pd.to_datetime(
            telemetry["observed_at"], utc=True, errors="coerce"
        )
        telemetry["value"] = pd.to_numeric(telemetry["value"], errors="coerce")
        telemetry = telemetry.drop_duplicates(["sensor", "metric"], keep="first")
        telemetry["age_minutes"] = telemetry["observed_at"].map(
            lambda value: _age_minutes(current, value)
        )
    counts = sensors["status"].value_counts()
    overall = max(sensors["status"], key=lambda value: STATUS_RANK.get(str(value), 3))
    summary = EcowittDiagnosticSummary(
        status=str(overall),
        online=int(counts.get("online", 0)),
        warning=int(counts.get("warning", 0) + counts.get("delayed", 0)),
        offline=int(counts.get("offline", 0)),
        average_coverage=float(sensors["coverage"].mean()),
    )
    return sensors, telemetry, summary
