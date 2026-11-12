"""On-demand city forecasts, independent of the station ingestion cycle."""

from __future__ import annotations

import hashlib
import re
import threading
import time
from collections import OrderedDict, deque
from concurrent.futures import Future
from dataclasses import asdict

import pandas as pd

from city_comparison import forecast_comparison, number
from city_weather import (
    CityLocation,
    CityWeatherError,
    city_by_geonames_id,
    fetch_city_forecast,
    search_cities,
)
from forecast_providers import build_session
from v5_city_index import nearby_public_city, public_city
from v5_data import clean_json, read_snapshot, records
from v5_observatories import remember_city, timezone_name

# Public municipality centre, independent of any configured station.
ROME = CityLocation(
    "Roma", "Italia", "Lazio", 41.89193, 12.51133, "Europe/Rome", geonames_id=3169070
)
_LOCK = threading.Lock()
_CACHE = OrderedDict()
_COMPARISONS = OrderedDict()
_LOCATIONS = OrderedDict()
_LEGACY = OrderedDict()
_FLIGHTS = {}
_MISSES = deque()
_SLOTS = threading.BoundedSemaphore(4)
_COMPARE_SLOTS = threading.BoundedSemaphore(2)
_TIMEOUT = (3, 6)


def geo_id(value):
    if type(value) is not int or not 0 < value < 2**31:
        raise ValueError("Scegli una città dalla ricerca")
    return value


def saved_city(value):
    """Only public city identity is synced; an approximate fix is never saved."""
    if not isinstance(value, dict):
        raise TypeError("Città non valida")
    identifier = geo_id(value.get("geo_id"))
    if not isinstance(value.get("name"), str) or not isinstance(
        value.get("label"), str
    ):
        raise TypeError("Nome della città non valido")
    name = " ".join(value["name"].split())[:80]
    label = " ".join(value["label"].split())[:120]
    if not name or not label:
        raise ValueError("Città non valida")
    return {"geo_id": identifier, "name": name, "label": label}


def _put(table, key, value, limit):
    table[key] = value
    table.move_to_end(key)
    while len(table) > limit:
        table.popitem(last=False)


def _cached(key, loader, ttl):
    """Bound misses and concurrency; never hold a lock during provider I/O."""
    comparison = key[0] == "comparison"
    table = _COMPARISONS if comparison else _CACHE
    slots = _COMPARE_SLOTS if comparison else _SLOTS
    with _LOCK:
        now = time.monotonic()
        hit = table.get(key)
        lifespan = (
            min(ttl, 240)
            if comparison and hit and len(hit[1].get("sources", [])) < 3
            else ttl
        )
        if hit and now - hit[0] < lifespan:
            table.move_to_end(key)
            return hit[1]
        flight = _FLIGHTS.get(key)
        if flight is None:
            while _MISSES and now - _MISSES[0] >= 600:
                _MISSES.popleft()
            if len(_MISSES) >= 100 or not slots.acquire(blocking=False):
                raise CityWeatherError("Servizio città occupato; riprova tra poco")
            _MISSES.append(now)
            _FLIGHTS[key] = future = Future()
    if flight is not None:
        return flight.result(timeout=30 if comparison else 24)
    try:
        result = loader()
        with _LOCK:
            _put(table, key, (time.monotonic(), result), 24 if comparison else 64)
        future.set_result(result)
        return result
    except Exception as error:
        future.set_exception(error)
        raise
    finally:
        with _LOCK:
            _FLIGHTS.pop(key, None)
            slots.release()


def _descriptor(location):
    return {
        "geo_id": location.geonames_id,
        "name": location.name,
        "label": location.label,
        "timezone": location.timezone,
    }


def _remember(location):
    # Older cached frontends still use the transient id. The home uses GeoNames.
    token = hashlib.sha256(repr(asdict(location)).encode()).hexdigest()[:24]
    remember_city(location)
    with _LOCK:
        if location.geonames_id is not None:
            _put(_LOCATIONS, location.geonames_id, location, 256)
        _put(_LEGACY, token, location, 256)
    return {"id": token, **_descriptor(location)}


def search_city(query):
    query = " ".join(query.split())
    if not 2 <= len(query) <= 80:
        raise ValueError("Scrivi da 2 a 80 caratteri")

    def load():
        with build_session(retries=0) as session:
            return [
                _remember(city)
                for city in search_cities(query, session=session, timeout=_TIMEOUT)
                if city.geonames_id is not None
            ]

    return _cached(("search", query.casefold()), load, 1800)


def _payload(location, token, *, approximate=False):
    with build_session(retries=0) as session:
        result = fetch_city_forecast(location, session=session, timeout=_TIMEOUT)
    if result.hourly.empty or not pd.notna(result.current.get("temp_c")):
        raise CityWeatherError("Previsione della città incompleta")
    return clean_json(
        {
            **_descriptor(location),
            "id": token,
            "approximate": approximate,
            "timezone": timezone_name(result.timezone),
            "source": result.source,
            "fetched_at": result.fetched_at,
            "current": result.current,
            "hourly": records(result.hourly),
            "daily": records(result.daily),
        }
    )


def _stable_id(stable):
    if not isinstance(stable, str) or not re.fullmatch(r"[1-9][0-9]{0,9}", stable):
        raise ValueError("Identificativo città non valido")
    return geo_id(int(stable))


def _location(city_id):
    if city_id == ROME.geonames_id:
        return ROME
    with _LOCK:
        location = _LOCATIONS.get(city_id)
    if location is None:
        try:
            location = city_by_geonames_id(city_id, timeout=_TIMEOUT)
        except CityWeatherError:
            location = public_city(city_id)
            if location is None:
                raise
    return location


def rome_observation(snapshot, *, now=None):
    """Use the public station only while its observed values are current and valid."""
    current = snapshot.get("current") or {}
    stamp = pd.to_datetime(snapshot.get("observed_at"), utc=True, errors="coerce")
    now = now if now is not None else pd.Timestamp.now(tz="UTC")
    if pd.isna(stamp) or not pd.Timedelta(minutes=-2) <= now - stamp <= pd.Timedelta(
        minutes=20
    ):
        return None
    flagged = str(current.get("data_quality") or "").casefold()
    suspect = {
        item.get("variable")
        for item in (snapshot.get("insights") or {})
        .get("quality", {})
        .get("measurements", [])
        if item.get("status") == "suspect"
    }
    values = {
        k: v
        for k in ("temp_c", "humidity", "wind_kmh", "pressure_hpa")
        if k not in suspect
        and f"stuck_{k}" not in flagged
        and (v := number(current.get(k))) is not None
    }
    return {"time": stamp.isoformat(), "values": values} if values else None


def _with_observation(payload):
    if payload.get("geo_id") != ROME.geonames_id:
        return payload
    try:
        observation = rome_observation(
            read_snapshot("roma-primary", published_only=True)
        )
    except Exception:  # noqa: BLE001 - station access must never block city weather.
        observation = None
    return {**payload, "station_observation": observation}


def city_forecast(identifier="", *, stable=None):
    if stable is not None:
        city_id = _stable_id(stable)

        def load():
            location = _location(city_id)
            _remember(location)
            return _payload(location, str(city_id))

        return _with_observation(_cached(("forecast", city_id), load, 900))

    with _LOCK:
        location = _LEGACY.get(identifier)
    if location is None:
        raise ValueError("Ripeti la ricerca della città: la selezione è scaduta")
    # Reuse the same weather product across legacy and new clients.
    return {**city_forecast(stable=str(location.geonames_id)), "id": identifier}


def _coarse_point(value):
    if not isinstance(value, dict) or set(value) - {
        "latitude",
        "longitude",
        "timezone",
    }:
        raise ValueError("Posizione non valida")
    lat, lon = value.get("latitude"), value.get("longitude")
    if type(lat) not in (int, float) or type(lon) not in (int, float):
        raise ValueError("Posizione non valida")
    if not -90 <= lat <= 90 or not -180 <= lon <= 180:
        raise ValueError("Posizione non valida")
    lat, lon = round(lat, 2), round(lon, 2)
    return lat, lon, timezone_name(value.get("timezone") or "UTC")


def _nearby_location(lat, lon, zone):
    city = nearby_public_city(lat, lon)
    if city is not None:
        if city.geonames_id == ROME.geonames_id:
            return ROME
        with _LOCK:
            return _LOCATIONS.get(city.geonames_id, city)
    return CityLocation("Vicino a te", "", "", lat, lon, zone)


def nearby_city(value):
    lat, lon, zone = _coarse_point(value)

    def load():
        city = _nearby_location(lat, lon, zone)
        if city.geonames_id is not None:
            _remember(city)
            with _LOCK:
                hit = _CACHE.get(("forecast", city.geonames_id))
            if hit and time.monotonic() - hit[0] < 900:
                return hit[1]
            result = _payload(city, str(city.geonames_id))
            with _LOCK:
                _put(
                    _CACHE,
                    ("forecast", city.geonames_id),
                    (time.monotonic(), result),
                    64,
                )
            return result
        return _payload(city, "nearby", approximate=True)

    return _with_observation(_cached(("nearby", lat, lon, zone), load, 900))


def city_comparison(*, stable=None, point=None):
    """Keep the slower comparison separate from the fast city page response."""
    if point is not None:
        lat, lon, zone = _coarse_point(point)
        nearby = _nearby_location(lat, lon, zone)
        key = (
            ("comparison", nearby.geonames_id)
            if nearby.geonames_id
            else ("comparison", lat, lon, zone)
        )

        def load():
            return {"geo_id": nearby.geonames_id, **forecast_comparison(nearby)}
    else:
        city_id = _stable_id(stable)
        key = ("comparison", city_id)

        def load():
            return {"geo_id": city_id, **forecast_comparison(_location(city_id))}

    return _cached(key, load, 1200)
