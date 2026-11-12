"""Comparable city forecast values from three distinct global model families."""

from __future__ import annotations

import logging
import math
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from itertools import pairwise
from statistics import median
from zoneinfo import ZoneInfo

from city_weather import (
    CityWeatherError,
    _fetch_metno_city_forecast,
    _get_json,
)
from forecast_providers import build_session

UTC = timezone.utc
HOUR = timedelta(hours=1)
TIMEOUT = (7, 8)
TARGET = 3
LOGGER = logging.getLogger(__name__)
OPEN_MODELS = (
    ("ecmwf", "ECMWF IFS", "ecmwf_ifs025"),
    ("gfs", "NOAA GFS", "ncep_gfs_global"),
    ("icon", "DWD ICON", "icon_global"),
)
FIELDS = {
    "temperature_2m": "temp_c",
    "relative_humidity_2m": "humidity",
    "precipitation": "precipitation_mm",
    "wind_speed_10m": "wind_kmh",
    "wind_gusts_10m": "wind_gust_kmh",
    "cloud_cover": "clouds",
    "pressure_msl": "pressure_hpa",
    "dew_point_2m": "dewpoint_c",
    "apparent_temperature": "feels_like_c",
    "cloud_cover_low": "cloud_low",
    "cloud_cover_mid": "cloud_mid",
    "cloud_cover_high": "cloud_high",
    "visibility": "visibility_m",
    "uv_index": "uv_index",
}
BRIGHT_FIELDS = {
    "temperature": "temp_c",
    "relative_humidity": "humidity",
    "precipitation": "precipitation_mm",
    "wind_speed": "wind_kmh",
    "wind_gust_speed": "wind_gust_kmh",
    "cloud_cover": "clouds",
    "pressure_msl": "pressure_hpa",
    "dew_point": "dewpoint_c",
    "visibility": "visibility_m",
}
_INSTANT = ("temp_c", "humidity", "wind_kmh", "clouds", "pressure_hpa", "dewpoint_c")


@dataclass
class Series:
    id: str
    name: str
    via: str
    rows: dict[datetime, dict[str, float]]
    note: str = ""

    def label(self):
        return {"id": self.id, "name": self.name, "via": self.via, "note": self.note}


def number(value):
    if value is None or isinstance(value, bool):
        return None
    try:
        value = float(value)
    except (ValueError, TypeError, OverflowError):
        return None
    return value if math.isfinite(value) else None


def _number(field, value):
    value = number(value)
    if value is None:
        return None
    if field in {"humidity", "clouds", "cloud_low", "cloud_mid", "cloud_high"}:
        return value if 0 <= value <= 100 else None
    if field in {"temp_c", "feels_like_c", "dewpoint_c"}:
        return value if -120 <= value <= 80 else None
    if field == "pressure_hpa":
        return value if 800 <= value <= 1150 else None
    if field in {
        "precipitation_mm",
        "wind_kmh",
        "wind_gust_kmh",
        "visibility_m",
        "uv_index",
    }:
        return value if value >= 0 else None
    return value


def _timestamp(value):
    if not isinstance(value, str):
        return None
    try:
        stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return stamp.astimezone(UTC) if stamp.tzinfo is not None else None
    except (OverflowError, ValueError):
        return None


def _set(rows, stamp, field, value):
    if (value := _number(field, value)) is not None:
        rows.setdefault(stamp, {})[field] = value


def _interpolate(rows, fields, max_step):
    # Interpolate instantaneous values only, and never across larger gaps.
    for field in fields:
        known = sorted((t, r[field]) for t, r in rows.items() if field in r)
        for (start, a), (end, b) in pairwise(known):
            steps = (end - start) / HOUR
            if not 1 < steps <= max_step or steps % 1:
                continue
            for i in range(1, int(steps)):
                _set(rows, start + i * HOUR, field, a + (b - a) * i / steps)
    return rows


def parse_open_models(data):
    block = data.get("hourly") or {}
    timestamps = block.get("time") or []
    if not isinstance(timestamps, list) or len(timestamps) > 216:
        raise CityWeatherError("Open-Meteo: orari non validi")
    times = [
        datetime.fromtimestamp(t, tz=UTC)
        if type(t) is int and 0 < t < 4_102_444_800
        else None
        for t in timestamps
    ]
    series = []
    for identifier, name, upstream in OPEN_MODELS:
        rows = {}
        for key, field in FIELDS.items():
            values = block.get(f"{key}_{upstream}")
            if not isinstance(values, list) or len(values) != len(times):
                continue
            for stamp, value in zip(times, values):
                if stamp is not None:
                    # Open-Meteo totals are for the preceding hour. Use interval
                    # starts everywhere, including for MET Norway and MOSMIX.
                    _set(
                        rows,
                        stamp - HOUR if field == "precipitation_mm" else stamp,
                        field,
                        value,
                    )
        series.append(Series(identifier, name, "Open-Meteo", rows))
    return series


def _open(location, now):
    with build_session(retries=0) as session:
        return parse_open_models(
            _get_json(
                session,
                "https://api.open-meteo.com/v1/forecast",
                {
                    "latitude": round(location.latitude, 3),
                    "longitude": round(location.longitude, 3),
                    "timezone": "GMT",
                    "timeformat": "unixtime",
                    "forecast_days": 8,
                    "wind_speed_unit": "kmh",
                    "hourly": ",".join(FIELDS),
                    "models": ",".join(item[2] for item in OPEN_MODELS),
                },
                "Open-Meteo",
                timeout=TIMEOUT,
            )
        )


def _norway(location, now):
    with build_session(retries=0) as session:
        result = _fetch_metno_city_forecast(location, session, TIMEOUT)
    if now - result.fetched_at.to_pydatetime() > timedelta(hours=18):
        raise CityWeatherError("MET Norway: emissione datata")
    rows = {}
    for r in result.hourly.to_dict(orient="records"):
        t = r["time"].to_pydatetime().astimezone(UTC)
        for field in FIELDS.values():
            if field == "precipitation_mm" and r.get("precipitation_hours") != 1:
                continue
            # MET Norway exposes clear-sky UV, not the all-sky UV requested here.
            if field != "uv_index":
                _set(rows, t, field, r.get(field))
    return Series(
        "ecmwf",
        "MET Norway",
        "Locationforecast",
        _interpolate(rows, _INSTANT, 6),
        "ECMWF nel modello globale; valori istantanei interpolati tra le scadenze disponibili.",
    )


def parse_7timer(data, now):
    try:
        issued = datetime.strptime(data.get("init", ""), "%Y%m%d%H").replace(tzinfo=UTC)
    except (TypeError, ValueError):
        raise CityWeatherError("7Timer: emissione non valida") from None
    if not now - timedelta(hours=18) <= issued <= now + HOUR:
        raise CityWeatherError("7Timer: emissione non attuale")
    rows = {}
    for item in (data.get("dataseries") or [])[:75]:
        if not isinstance(item, dict):
            continue
        lead = item.get("timepoint")
        if type(lead) is not int or not 0 <= lead <= 216:
            continue
        t = issued + lead * HOUR
        _set(rows, t, "temp_c", item.get("temp2m"))
        rh = item.get("rh2m")
        _set(rows, t, "humidity", rh.rstrip("%") if isinstance(rh, str) else rh)
        # Wind, clouds and precipitation are categories, not exact quantities.
    return Series(
        "gfs",
        "NOAA GFS",
        "7Timer!",
        _interpolate(rows, ("temp_c", "humidity"), 3),
        "Temperatura e umidità a intervalli di 3 ore, interpolate; quantità di pioggia non utilizzata.",
    )


def _seven(location, now):
    with build_session(retries=0) as session:
        return parse_7timer(
            _get_json(
                session,
                "https://www.7timer.info/bin/api.pl",
                {
                    "lat": round(location.latitude, 3),
                    "lon": round(location.longitude, 3),
                    "product": "civil",
                    "output": "json",
                },
                "7Timer",
                timeout=TIMEOUT,
            ),
            now,
        )


def parse_bright_sky(data):
    points = {
        item["id"]: item
        for item in data.get("sources") or []
        if item.get("observation_type") == "forecast"
        and type(item.get("id")) is int
        and (distance := number(item.get("distance"))) is not None
        and 0 <= distance <= 35000
    }
    if not points:
        raise CityWeatherError("DWD MOSMIX: nessun punto vicino")
    point = min(points.values(), key=lambda item: item["distance"])
    rows = {}
    for item in (data.get("weather") or [])[:220]:
        if (
            item.get("source_id") != point["id"]
            or (stamp := _timestamp(item.get("timestamp"))) is None
        ):
            continue
        for key, field in BRIGHT_FIELDS.items():
            _set(
                rows,
                stamp - HOUR if field == "precipitation_mm" else stamp,
                field,
                item.get(key),
            )
    km = round(point["distance"] / 1000)
    return Series(
        "icon",
        "DWD MOSMIX",
        "Bright Sky",
        rows,
        f"Punto di previsione a {km} km dal centro; combina ICON ed ECMWF.",
    )


def _bright(location, now):
    with build_session(retries=0) as session:
        return parse_bright_sky(
            _get_json(
                session,
                "https://api.brightsky.dev/weather",
                {
                    "lat": round(location.latitude, 3),
                    "lon": round(location.longitude, 3),
                    "date": (
                        now.replace(minute=0, second=0, microsecond=0) - HOUR
                    ).isoformat(),
                    "last_date": (now.date() + timedelta(days=8)).isoformat(),
                    "tz": "UTC",
                    "max_dist": 35000,
                },
                "Bright Sky",
                timeout=TIMEOUT,
            )
        )


def _usable(series, now):
    start = now.replace(minute=0, second=0, microsecond=0)
    return (
        sum(
            start <= t < start + 24 * HOUR and "temp_c" in v
            for t, v in series.rows.items()
        )
        >= 6
    )


def _safe(loader, location, now):
    try:
        return loader(location, now)
    except (
        CityWeatherError,
        ValueError,
        TypeError,
        AttributeError,
        KeyError,
        OverflowError,
    ) as error:
        LOGGER.info(
            "Confronto città: %s non disponibile (%s)",
            loader.__name__,
            type(error).__name__,
        )
        return None


def _band(values):
    return [round(value, 2) for value in (median(values), min(values), max(values))] + [
        len(values)
    ]


def _bands(rows):
    return {
        key: _band(v)
        for key in set().union(*rows)
        if (v := [r[key] for r in rows if key in r])
    }


def _common(tables):
    return sorted(set.intersection(*(set(table) for table in tables))) if tables else []


def _day_summary(series, instants, zone):
    temperatures = [
        {t: s.rows[t]["temp_c"] for t in instants if "temp_c" in s.rows.get(t, {})}
        for s in series
    ]
    rain = [
        {
            t: s.rows[t]["precipitation_mm"]
            for t in instants
            if "precipitation_mm" in s.rows.get(t, {})
            and (t + HOUR - timedelta(seconds=1)).astimezone(zone).date()
            == t.astimezone(zone).date()
        }
        for s in series
    ]
    minimum = min(4, len(instants))
    common_temp = _common([t for t in temperatures if len(t) >= minimum])
    common_rain = _common([r for r in rain if len(r) >= minimum])
    by_model = []
    for s, temp, wet in zip(series, temperatures, rain):
        row = {"id": s.id}
        if common_temp and len(temp) >= minimum:
            values = [temp[t] for t in common_temp]
            row.update(temp_min_c=min(values), temp_max_c=max(values))
        if common_rain and len(wet) >= minimum:
            row["precipitation_mm"] = round(sum(wet[t] for t in common_rain), 2)
        by_model.append(row)
    start = common_temp[0].astimezone(zone).isoformat() if common_temp else None
    return {
        "values": _bands([{k: v for k, v in r.items() if k != "id"} for r in by_model]),
        "models": by_model,
        "temp_hours": len(common_temp),
        "temp_start": start,
        "rain_hours": len(common_rain),
    }


def aggregate(series, zone, now):
    local_zone = ZoneInfo(zone)
    start = now.replace(minute=0, second=0, microsecond=0) - HOUR
    end_day = now.astimezone(local_zone).date() + timedelta(days=7)
    times = sorted(
        {
            t
            for s in series
            for t in s.rows
            if start <= t and t.astimezone(local_zone).date() < end_day
        }
    )
    dates = {}
    hourly = []
    for t in times:
        local = t.astimezone(local_zone)
        if t >= start + HOUR:
            dates.setdefault(local.date(), []).append(t)
        rows = [s.rows.get(t, {}) for s in series]
        values = _bands(rows)
        hourly.append(
            {
                "time": local.isoformat(),
                "values": values,
                "single": {
                    k: next(s.id for s, row in zip(series, rows) if k in row)
                    for k, a in values.items()
                    if a[3] == 1
                },
            }
        )
    return hourly, [
        {"date": d.isoformat(), **_day_summary(series, hours, local_zone)}
        for d, hours in sorted(dates.items())
    ]


def forecast_comparison(location, *, now=None):
    now = now or datetime.now(UTC)
    primary = _safe(_open, location, now) or []
    found = {s.id: s for s in primary if _usable(s, now)}
    alternatives = [
        (key, loader)
        for key, loader in (("ecmwf", _norway), ("gfs", _seven), ("icon", _bright))
        if key not in found
    ]
    if alternatives:
        with ThreadPoolExecutor(max_workers=len(alternatives)) as pool:
            jobs = [
                pool.submit(_safe, loader, location, now) for _, loader in alternatives
            ]
            for job in jobs:
                value = job.result()
                if value and _usable(value, now):
                    found[value.id] = value
    chosen = [found[key] for key, _, _ in OPEN_MODELS if key in found]
    hours, days = aggregate(chosen, location.timezone, now)
    return {
        "target": TARGET,
        "checked_at": now.isoformat(),
        "sources": [s.label() for s in chosen],
        "shared_model": "ecmwf" in found
        and found.get("icon") is not None
        and found["icon"].via == "Bright Sky",
        "hourly": hours,
        "daily": days,
    }
