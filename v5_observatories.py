"""On-demand weather for observing sites outside the configured stations."""

from __future__ import annotations

import hashlib
import json
import math
import threading
import time
from collections import OrderedDict, deque
from concurrent.futures import Future
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import pandas as pd

from city_weather import (
    CityLocation,
    CityWeatherError,
    city_by_geonames_id,
    fetch_observing_hourly,
)

_CACHE = OrderedDict()
_IN_FLIGHT = {}
_MISS_TIMES = deque()
_LOCK = threading.Lock()
_PROVIDER_SLOTS = threading.BoundedSemaphore(2)


def timezone_name(value):
    if not isinstance(value, str) or not 1 <= len(value) <= 64:
        raise ValueError("Inserisci un fuso IANA, per esempio America/Santiago")
    try:
        return ZoneInfo(value).key
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise ValueError("Fuso orario non valido") from exc


def site_spec(value):
    """Validate saved inputs without a network call or public location lookup."""
    if not isinstance(value, dict):
        raise TypeError("Postazione non valida")
    kind = value.get("kind")
    label = " ".join(str(value.get("label") or "").split())[:100]
    if kind == "city":
        identifier = value.get("geo_id")
        if type(identifier) is not int or not 0 < identifier < 2**31:
            raise ValueError("Scegli una località dalla ricerca")
        return {"kind": "city", "geo_id": identifier, "label": label}
    if kind != "point" or not label:
        raise ValueError("Dai un nome alla postazione")
    lat, lon, height = map(
        float, (value["latitude"], value["longitude"], value.get("elevation_m", 0))
    )
    if not all(math.isfinite(n) for n in (lat, lon, height)) or not (
        -90 < lat < 90 and -180 <= lon <= 180 and -500 <= height <= 9000
    ):
        raise ValueError("Coordinate o quota non valide")
    return {
        "kind": "point",
        "label": label,
        "latitude": round(lat, 6),
        "longitude": round(lon, 6),
        "elevation_m": round(height, 1),
        "timezone": timezone_name(value.get("timezone")),
    }


def _cached_call(key, load, ttl):
    """One request per key, with a shared cap independent of local weather."""
    with _LOCK:
        now = time.monotonic()
        hit = _CACHE.get(key)
        if hit and now - hit[0] < ttl:
            _CACHE.move_to_end(key)
            return hit[1]
        existing = _IN_FLIGHT.get(key)
        if existing is None:
            while _MISS_TIMES and _MISS_TIMES[0] < now - 600:
                _MISS_TIMES.popleft()
            if len(_MISS_TIMES) >= 48 or not _PROVIDER_SLOTS.acquire(blocking=False):
                raise CityWeatherError("Previsione globale occupata; riprova tra poco")
            _MISS_TIMES.append(now)
            _IN_FLIGHT[key] = result = Future()
    if existing is not None:
        return existing.result(timeout=25)
    try:
        value = load()
        with _LOCK:
            _put(key, value)
        result.set_result(value)
        return value
    except Exception as exc:
        result.set_exception(exc)
        raise
    finally:
        with _LOCK:
            _IN_FLIGHT.pop(key, None)
            _PROVIDER_SLOTS.release()


def _put(key, value):
    _CACHE[key] = (time.monotonic(), value)
    _CACHE.move_to_end(key)
    while len(_CACHE) > 48:
        _CACHE.popitem(last=False)


def remember_city(location):
    if location.geonames_id is not None:
        with _LOCK:
            _put(("city", location.geonames_id), location)


def remote_site_weather(spec):
    """Return the site and cached forecast; coordinates remain server-side."""
    from v5_cielo import ObservingSite

    spec = site_spec(spec)
    if spec["kind"] == "city":
        geo_id = spec["geo_id"]
        location = _cached_call(
            ("city", geo_id), lambda: city_by_geonames_id(geo_id), 86400
        )
        identifier = f"city-{geo_id}"
    else:
        location = CityLocation(
            spec["label"],
            "",
            "",
            spec["latitude"],
            spec["longitude"],
            spec["timezone"],
            spec["elevation_m"],
        )
        key = [
            location.latitude,
            location.longitude,
            location.timezone,
            location.elevation_m,
        ]
        identifier = (
            "point-" + hashlib.sha256(json.dumps(key).encode()).hexdigest()[:20]
        )
    site = ObservingSite(
        identifier,
        location.label,
        timezone_name(location.timezone),
        location.latitude,
        location.longitude,
        location.elevation_m or 0,
    )
    key = (
        "weather",
        round(location.latitude, 3),
        round(location.longitude, 3),
        site.local_timezone,
    )
    frame, source, fetched = _cached_call(
        key, lambda: fetch_observing_hourly(location), 900
    )
    return site, frame, source, fetched


def remote_context(spec, profile, now=None):
    from v5_astronomy import observing_forecast
    from v5_cielo import ObservingContext
    from v5_data import FORECAST_PUBLIC, records

    now = now if now is not None else pd.Timestamp.now(tz="UTC")
    site, frame, source, fetched = remote_site_weather(spec)
    hours = observing_forecast(frame, site, profile, now)
    snapshot = {
        "generated_at": fetched,
        "forecast": records(frame, FORECAST_PUBLIC),
        "astronomy": {profile: {"hours": records(hours)}},
    }
    return ObservingContext(site, snapshot, now, source)
