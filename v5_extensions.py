"""Bounded public tools for V5; station coordinates stay on the server."""

from __future__ import annotations

import hashlib
import json
import re
import threading
import time
from collections import OrderedDict
from dataclasses import asdict

import pandas as pd
from sqlalchemy import text

from config import Settings
from db import get_engine
from v5_data import clean_json, records, scoped_forecast, station_settings

_CACHE: OrderedDict = OrderedDict()
_LOCK = threading.Lock()
_CITY: OrderedDict = OrderedDict()


def cached(key, loader, ttl=600):
    # Single-flight and bounded: failed requests do not poison the last value.
    with _LOCK:
        now = time.monotonic()
        if key in _CACHE and now - _CACHE[key][0] < ttl:
            return _CACHE[key][1]
        value = loader()
        _CACHE[key] = (now, value)
        _CACHE.move_to_end(key)
        while len(_CACHE) > 128:
            _CACHE.popitem(last=False)
        return value


def search_city(query: str) -> list[dict]:
    from city_weather import search_cities

    query = " ".join(query.split())
    if not 2 <= len(query) <= 80:
        raise ValueError("Scrivi da 2 a 80 caratteri")

    def load():
        found = []
        for city in search_cities(query):
            identifier = hashlib.sha256(
                json.dumps(asdict(city), sort_keys=True).encode()
            ).hexdigest()[:24]
            _CITY[identifier] = city
            _CITY.move_to_end(identifier)
            found.append(
                {"id": identifier, "label": city.label, "timezone": city.timezone}
            )
        while len(_CITY) > 1024:
            _CITY.popitem(last=False)
        return found

    return cached(("search", query.casefold()), load)


def city_forecast(identifier: str) -> dict:
    from city_weather import fetch_city_forecast

    if identifier not in _CITY:
        raise ValueError("Ripeti la ricerca della città: la selezione è scaduta")
    location = _CITY[identifier]

    def load():
        result = fetch_city_forecast(location)
        return clean_json(
            {
                "id": identifier,
                "label": location.label,
                "timezone": result.timezone,
                "source": result.source,
                "fetched_at": result.fetched_at,
                "current": result.current,
                "hourly": records(result.hourly),
                "daily": records(result.daily),
            }
        )

    return cached(("city", identifier), load)


def read_product(key: str):
    with get_engine().connect() as con:
        value = con.execute(
            text("SELECT payload FROM v5_products WHERE product_key=:key"), {"key": key}
        ).scalar()
    return json.loads(value) if value else None


def refresh_product(key: str, loader, interval: int) -> None:
    now = pd.Timestamp.now(tz="UTC")
    with get_engine().begin() as con:
        last = con.execute(
            text("SELECT attempted_at FROM v5_products WHERE product_key=:key"),
            {"key": key},
        ).scalar()
        if last and (now - pd.Timestamp(last)).total_seconds() < interval:
            return
        con.execute(
            text(
                "INSERT INTO v5_products(product_key,attempted_at) VALUES(:key,:at) ON CONFLICT(product_key) DO UPDATE SET attempted_at=excluded.attempted_at"
            ),
            {"key": key, "at": now.isoformat()},
        )
    payload = clean_json(loader())
    with get_engine().begin() as con:
        con.execute(
            text("UPDATE v5_products SET payload=:payload WHERE product_key=:key"),
            {"key": key, "payload": json.dumps(payload, allow_nan=False)},
        )


def radar_frames():
    from forecast_providers import build_session

    with build_session(retries=1) as session:
        response = session.get(
            "https://api.rainviewer.com/public/weather-maps.json", timeout=(5, 15)
        )
        response.raise_for_status()
        payload = response.json()
    frames = []
    for item in (payload.get("radar") or {}).get("past", [])[-13:]:
        path = str(item.get("path", ""))
        stamp = pd.to_datetime(item.get("time"), unit="s", utc=True, errors="coerce")
        if re.fullmatch(r"/v2/radar/\d+", path) and pd.notna(stamp):
            frames.append({"path": path, "time": stamp})
    return clean_json(
        {
            "source": "RainViewer",
            "fetched_at": pd.Timestamp.now(tz="UTC"),
            "frames": sorted(frames, key=lambda r: r["time"]),
            "host": "https://tilecache.rainviewer.com",
            "kind": "observed",
            "nowcast_available": False,
        }
    )


def uncertainty(station_id: str, now: pd.Timestamp) -> dict:
    if station_id != Settings.from_env().station_id:
        return read_product("ensemble:" + station_id) or {
            "rows": [],
            "source": "ICON ensemble",
        }
    with get_engine().connect() as con:
        frame = pd.read_sql(
            text(
                "SELECT model,issued_at,valid_time,variable,p10,p50,p90,member_count FROM forecast_ensemble_runs WHERE issued_at=(SELECT MAX(issued_at) FROM forecast_ensemble_runs) AND valid_time>=:at ORDER BY valid_time,variable"
            ),
            con,
            params={"at": now.floor("h").strftime("%Y-%m-%dT%H:%M:%SZ")},
        )
    return {"rows": records(frame), "source": "Open-Meteo · ICON ensemble"}


def secondary_ensemble(cfg: Settings):
    from ensemble_forecast import fetch_open_meteo_ensemble

    frame = fetch_open_meteo_ensemble(cfg)
    return {
        "rows": records(
            frame,
            [
                "model",
                "issued_at",
                "valid_time",
                "variable",
                "p10",
                "p50",
                "p90",
                "member_count",
            ],
        ),
        "source": "Open-Meteo · ICON ensemble",
        "fetched_at": pd.Timestamp.now(tz="UTC"),
    }


def planner(station_id: str, values: dict) -> dict:
    from astronomy_planner import (
        TARGET_BY_NAME,
        equipment_profile,
        field_of_view,
        framing_geometry,
        night_plan_tracks,
        summarize_night_plan,
    )
    from v5_astronomy import observing_forecast

    cfg = station_settings(station_id)
    now = pd.Timestamp.now(tz="UTC")
    targets = values.get("targets", [])
    if (
        not isinstance(targets, list)
        or not 1 <= len(targets) <= 8
        or any(t not in TARGET_BY_NAME for t in targets)
    ):
        raise ValueError("Scegli da uno a otto oggetti del catalogo")
    start, end = pd.Timestamp(values["start"]), pd.Timestamp(values["end"])
    if start.tzinfo is None:
        start = start.tz_localize(
            cfg.local_timezone, ambiguous="raise", nonexistent="raise"
        )
    if end.tzinfo is None:
        end = end.tz_localize(
            cfg.local_timezone, ambiguous="raise", nonexistent="raise"
        )
    start = max(start.tz_convert("UTC"), now)
    end = end.tz_convert("UTC")
    if not pd.Timedelta(minutes=15) <= end - start <= pd.Timedelta(
        hours=16
    ) or end > now + pd.Timedelta(days=8):
        raise ValueError(
            "Scegli una finestra futura da 15 minuti a 16 ore entro otto giorni"
        )
    minimum = float(values.get("altitude", 25))
    moon = float(values.get("moon", 30))
    rotation = float(values.get("rotation", 0))
    if not 0 <= minimum <= 85 or not 0 <= moon <= 180 or not 0 <= rotation <= 360:
        raise ValueError("Soglie non valide")
    horizon = values.get("horizon", {})
    if not isinstance(horizon, dict) or len(horizon) > 72:
        raise ValueError("Orizzonte non valido")
    horizon = {float(k): float(v) for k, v in horizon.items()}
    if any(not 0 <= a < 360 or not 0 <= h <= 90 for a, h in horizon.items()):
        raise ValueError("Azimut 0–359°, ostacoli 0–90°")
    equipment = (
        equipment_profile(**values["equipment"]) if values.get("equipment") else None
    )
    forecast = scoped_forecast(station_id)
    weather = observing_forecast(forecast, cfg, values.get("profile", "deep_sky"), now)
    tracks = night_plan_tracks(
        weather,
        cfg,
        targets,
        start=start,
        end=end,
        minimum_altitude=minimum,
        minimum_moon_separation=moon,
        horizon_mask=horizon,
    )
    summaries = summarize_night_plan(tracks, equipment=equipment, rotation_deg=rotation)
    geometry = (
        {
            name: framing_geometry(
                TARGET_BY_NAME[name], equipment, rotation_deg=rotation
            )
            for name in targets
        }
        if equipment
        else {}
    )
    return clean_json(
        {
            "station_id": station_id,
            "start": start,
            "end": end,
            "generated_at": now,
            "summary": records(summaries),
            "tracks": records(tracks),
            "field": asdict(field_of_view(equipment)) if equipment else None,
            "geometry": geometry,
        }
    )


def catalog():
    from astronomy_planner import TARGETS

    return [asdict(target) for target in TARGETS]
