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

from city_weather import (
    CityLocation,
    CityWeatherError,
    city_by_geonames_id,
    fetch_city_forecast,
    search_cities,
)
from forecast_providers import build_session
from v5_city_index import nearby_public_city, public_city
from v5_data import clean_json, records
from v5_observatories import remember_city, timezone_name

# Public municipality centre, independent of any configured station.
ROME = CityLocation(
    "Roma", "Italia", "Lazio", 41.89193, 12.51133, "Europe/Rome", geonames_id=3169070
)
_LOCK = threading.Lock()
_CACHE = OrderedDict()
_LOCATIONS = OrderedDict()
_LEGACY = OrderedDict()
_FLIGHTS = {}
_MISSES = deque()
_SLOTS = threading.BoundedSemaphore(4)
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
    with _LOCK:
        now = time.monotonic()
        hit = _CACHE.get(key)
        if hit and now - hit[0] < ttl:
            _CACHE.move_to_end(key)
            return hit[1]
        flight = _FLIGHTS.get(key)
        if flight is None:
            while _MISSES and now - _MISSES[0] >= 600:
                _MISSES.popleft()
            if len(_MISSES) >= 100 or not _SLOTS.acquire(blocking=False):
                raise CityWeatherError("Servizio città occupato; riprova tra poco")
            _MISSES.append(now)
            _FLIGHTS[key] = future = Future()
    if flight is not None:
        return flight.result(timeout=24)
    try:
        result = loader()
        with _LOCK:
            _put(_CACHE, key, (time.monotonic(), result), 64)
        future.set_result(result)
        return result
    except Exception as error:
        future.set_exception(error)
        raise
    finally:
        with _LOCK:
            _FLIGHTS.pop(key, None)
            _SLOTS.release()


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


def city_forecast(identifier="", *, stable=None):
    if stable is not None:
        if not re.fullmatch(r"[1-9][0-9]{0,9}", stable):
            raise ValueError("Identificativo città non valido")
        city_id = geo_id(int(stable))

        def load():
            with _LOCK:
                location = _LOCATIONS.get(city_id)
            if location is None:
                if city_id == ROME.geonames_id:
                    location = ROME
                else:
                    try:
                        location = city_by_geonames_id(city_id, timeout=_TIMEOUT)
                    except CityWeatherError:
                        location = public_city(city_id)
                        if location is None:
                            raise
            _remember(location)
            return _payload(location, str(city_id))

        return _cached(("forecast", city_id), load, 900)

    with _LOCK:
        location = _LEGACY.get(identifier)
    if location is None:
        raise ValueError("Ripeti la ricerca della città: la selezione è scaduta")
    # Reuse the same weather product across legacy and new clients.
    return {**city_forecast(stable=str(location.geonames_id)), "id": identifier}


def nearby_city(value):
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
    zone = timezone_name(value.get("timezone") or "UTC")

    def load():
        city = nearby_public_city(lat, lon)
        if city is not None:
            with _LOCK:
                city = _LOCATIONS.get(city.geonames_id, city)
            if city.geonames_id == ROME.geonames_id:
                city = ROME
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
        return _payload(
            CityLocation("Vicino a te", "", "", lat, lon, zone),
            "nearby",
            approximate=True,
        )

    return _cached(("nearby", lat, lon, zone), load, 900)
