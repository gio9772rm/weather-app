"""CAMS column aerosols, separate from turbulence and sky brightness."""

import json
import math

import pandas as pd
from sqlalchemy import text

from db import get_engine
from v5_calibration import utc


def sky_rows(forecast, environment, now):
    from v5_data import clean_json

    environment = environment or {}
    fetched = utc(environment.get("fetched_at"))
    fresh = pd.notna(fetched) and pd.Timedelta(0) <= now - fetched <= pd.Timedelta(
        hours=12
    )
    air = (
        {utc(r.get("time")): r for r in environment.get("hourly", [])} if fresh else {}
    )
    result = []

    def number(row, key, low=0, high=100000):
        try:
            v = float(row.get(key))
            return v if math.isfinite(v) and low <= v <= high else None
        except (TypeError, ValueError):
            return None

    for row in forecast.to_dict("records"):
        stamp = utc(row.get("valid_time"))
        cam = air.get(stamp, {})
        aod, dust = number(cam, "aerosol_optical_depth", high=5), number(cam, "dust")
        clouds = number(row, "clouds", high=100)
        visibility = number(row, "visibility_m")
        factors = []
        if aod is not None and aod >= 0.2:
            factors.append("Aerosol in colonna")
        if dust is not None and dust >= 50:
            factors.append("Polvere al suolo")
        if clouds is not None and clouds > 25:
            factors.append("Copertura nuvolosa")
        if visibility is not None and visibility < 10000:
            factors.append("Foschia / visibilità ridotta")
        score = (
            round(100 * math.exp(-aod) * (1 - clouds / 100))
            if aod is not None and clouds is not None
            else None
        )
        cape, jet = number(row, "cape_j_kg"), number(row, "wind_300hpa_kmh")
        stability = (
            max(0, round(100 - min(cape, 2000) / 25 - max(jet - 80, 0) * 0.12))
            if cape is not None and jet is not None
            else None
        )
        result.append(
            {
                "time": stamp,
                "aod_550nm": aod,
                "dust_ug_m3": dust,
                "aerosol_extinction_mag_airmass": 1.086 * aod
                if aod is not None
                else None,
                "transparency_estimate": score,
                "stability_estimate": stability,
                "sky_brightness": None,
                "limiting_factors": factors,
                "cams_available": bool(cam),
                "visibility_m": visibility,
            }
        )
    return clean_json(
        {
            "source": "Open-Meteo · CAMS",
            "fetched_at": fetched,
            "fresh": bool(fresh),
            "rows": result,
            "note": "Trasparenza: indice euristico da aerosol a 550 nm e nuvole, non una misura ottica. AOD descrive la colonna atmosferica; dust è la concentrazione di polvere vicino al suolo e non misura l’estinzione totale. Stabilità: proxy da CAPE e vento a 300 hPa, non seeing misurato. Luminosità del cielo non misurata: il piano mostra separatamente illuminazione/distanza della Luna; SQM/Bortle di atlante non vengono presentati come misure.",
        }
    )


def atmosphere(station_id, forecast, now):
    with get_engine().connect() as con:
        raw = con.execute(
            text("SELECT payload FROM location_environment WHERE station_id=:id"),
            {"id": station_id},
        ).scalar()
    return sky_rows(forecast, json.loads(raw) if raw else None, now)


def add_cams(forecast, sky):
    result = forecast.copy()
    if not result.empty:
        aod = {utc(r["time"]): r["aod_550nm"] for r in sky["rows"]}
        result["cams_aod"] = utc(result.valid_time).map(aod)
    return result
