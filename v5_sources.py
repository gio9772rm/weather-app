"""Bounded optional research sources. No credentials or coordinates are public."""

from __future__ import annotations

import io
from urllib.parse import urlsplit

import numpy as np
import pandas as pd
from PIL import Image

from config import Settings
from forecast_providers import build_session
from v5_calibration import archive_runs

FIELDS = {
    "temperature_2m": "temp_c",
    "relative_humidity_2m": "humidity",
    "wind_speed_10m": "wind_kmh",
    "rain": "rain_mm",
}
HEADERS = {
    "Origin": "https://radar.protezionecivile.gov.it",
    "Referer": "https://radar.protezionecivile.gov.it/",
}
DPC_BASE = "https://radar-api.protezionecivile.it"


def parse_weathernext(payload, now):
    hourly = payload.get("hourly") or {}
    times = hourly.get("time") or []
    if not times:
        raise ValueError("WeatherNext non disponibile")
    frame = pd.DataFrame(
        {"valid_time": pd.to_datetime(times, utc=True, errors="coerce")}
    )
    for source, target in {
        "temperature_2m": "temp_c",
        "relative_humidity_2m": "humidity",
        "wind_speed_10m": "wind_kmh",
        "rain": "rain_mm",
        "cloud_cover": "clouds",
    }.items():
        values = hourly.get(source)
        frame[target] = pd.to_numeric(
            pd.Series(
                values
                if isinstance(values, list) and len(values) == len(frame)
                else [None] * len(frame)
            ),
            errors="coerce",
        )
    frame = (
        frame.dropna(subset=["valid_time"])
        .sort_values("valid_time")
        .drop_duplicates("valid_time")
    )
    if frame.empty or (frame.valid_time.diff().dropna() != pd.Timedelta(hours=6)).any():
        raise ValueError("Intervallo WeatherNext inatteso")
    frame["provider"], frame["model"] = "google_weathernext2", "ensemble_mean_native6h"
    # This is acquisition time, not an invented model initialisation time.
    frame["issued_at"], frame["interval_hours"] = now, 6.0
    return frame[frame.valid_time.gt(now)]


def refresh_weathernext(cfg):
    from v5_data import records

    now = pd.Timestamp.now(tz="UTC")
    with build_session(retries=1) as session:
        response = session.get(
            "https://ensemble-api.open-meteo.com/v1/ensemble",
            params={
                "latitude": cfg.latitude,
                "longitude": cfg.longitude,
                "hourly": "temperature_2m,relative_humidity_2m,wind_speed_10m,rain,cloud_cover",
                "models": "google_weathernext2_ensemble_mean",
                "temporal_resolution": "native",
                "forecast_days": 7,
                "timezone": "UTC",
                "wind_speed_unit": "kmh",
            },
            timeout=(5, 25),
        )
        response.raise_for_status()
        frame = parse_weathernext(response.json(), now)
    if frame.empty:
        raise ValueError("Nessun intervallo futuro WeatherNext")
    archive_runs(cfg.station_id, frame, acquired_at=now)
    return {
        "source": "Open-Meteo · Google WeatherNext 2",
        "fetched_at": now,
        "mode": "evaluation",
        "native_interval_hours": 6,
        "rows": records(frame),
        "note": "Media ensemble; nuvole e pioggia derivate. Intervalli nativi di sei ore, esclusi dal blend operativo fino a verifica locale.",
    }


def parse_previous_runs(payload, model, now):
    hourly = payload.get("hourly") or {}
    times = pd.to_datetime(hourly.get("time") or [], utc=True, errors="coerce")
    if not len(times):
        raise ValueError("Archivio previsioni non disponibile")
    frames = []
    for days in (1, 3):
        f = pd.DataFrame({"valid_time": times})
        for source, target in FIELDS.items():
            values = hourly.get(source + "_previous_day" + str(days))
            f[target] = pd.to_numeric(
                pd.Series(
                    values
                    if isinstance(values, list) and len(values) == len(f)
                    else [None] * len(f)
                ),
                errors="coerce",
            )
        f["issued_at"] = f.valid_time - pd.Timedelta(days=days)
        f["provider"], f["model"], f["interval_hours"] = "open_meteo_archive", model, 1
        # Each row is a fixed lead, not a complete model run or a local issuance.
        f = f[f.valid_time.lt(now)].dropna(subset=["valid_time"])
        f = f.dropna(subset=list(FIELDS.values()), how="all")
        frames.append(f)
    return pd.concat(frames, ignore_index=True)


def refresh_previous_runs(cfg, model):
    now = pd.Timestamp.now(tz="UTC")
    with build_session(retries=1) as session:
        response = session.get(
            "https://previous-runs-api.open-meteo.com/v1/forecast",
            params={
                "latitude": cfg.latitude,
                "longitude": cfg.longitude,
                "hourly": ",".join(
                    k + "_previous_day" + str(day) for day in (1, 3) for k in FIELDS
                ),
                "models": model,
                "start_date": (now - pd.Timedelta(days=45)).date().isoformat(),
                "end_date": (now - pd.Timedelta(days=1)).date().isoformat(),
                "timezone": "UTC",
                "wind_speed_unit": "kmh",
            },
            timeout=(5, 25),
        )
        response.raise_for_status()
        frame = parse_previous_runs(response.json(), model, now)
    if frame.empty:
        raise ValueError("Archivio senza campioni confrontabili")
    archive_runs(cfg.station_id, frame, basis="previous_run", acquired_at=now)
    return {
        "source": "Open-Meteo · Previous Runs",
        "model": model,
        "fetched_at": now,
        "rows": len(frame),
        "start": frame.valid_time.min(),
        "end": frame.valid_time.max(),
        "lead_days": [1, 3],
        "note": "Previsioni storiche a scadenza fissa 24/72 ore. Valutazione separata: non rappresentano ciò che il sito mostrava allora.",
    }


def historical_time(value, now=None):
    now = pd.Timestamp.now(tz="UTC") if now is None else now
    at = pd.to_datetime(value, utc=True, errors="raise")
    if pd.isna(at) or not now - pd.Timedelta(days=14) <= at <= now:
        raise ValueError("Orario radar fuori dai quattordici giorni disponibili")
    return at.floor("5min")


def dpc_download(value, product="SRI"):
    """Use the documented DPC v2 download API, with an exact S3 allowlist."""
    if product not in {"SRI", "VMI", "SRT1"}:
        raise ValueError("Prodotto radar non valido")
    at = historical_time(value)
    with build_session(retries=1) as session:
        response = session.post(
            DPC_BASE + "/downloadProduct",
            json={"productType": product, "productDate": int(at.timestamp() * 1000)},
            headers=HEADERS,
            timeout=(5, 20),
        )
        response.raise_for_status()
        url = response.json().get("url", "")
    parsed = urlsplit(url)
    if (
        parsed.scheme != "https"
        or parsed.hostname
        not in {
            "dpc-radar.s3.eu-south-1.amazonaws.com",
            "s3-prod-dpc-radar.s3.eu-south-1.amazonaws.com",
        }
        or parsed.username
        or parsed.port not in (None, 443)
    ):
        raise ValueError("URL ufficiale DPC non valido")
    return {
        "url": url,
        "time": at.isoformat(),
        "product": product,
        "source": "Dipartimento della Protezione Civile",
        "licence": "CC BY-SA 4.0",
    }


def dpc_history_image(station_id, value):
    """Render a regional historical tile centered on a PUBLIC town, not a sensor."""
    from dpc_radar import _tile_url
    from v5_data import station_settings

    cfg = station_settings(station_id)
    # Deliberately independent of the private station coordinates.
    town = (
        (41.90, 12.50)
        if station_id == Settings.from_env().station_id
        else (44.69, 12.18)
    )
    at = historical_time(value)
    from dpc_radar import _global_pixel

    x, y = _global_pixel(*town)
    with build_session(retries=1) as session:
        response = session.get(
            _tile_url("SRI", at, x // 256, y // 256), headers=HEADERS, timeout=(5, 15)
        )
        response.raise_for_status()
    with Image.open(io.BytesIO(response.content)) as source:
        pixels = np.asarray(source.convert("RGBA"))
    if pixels.shape != (256, 256, 4):
        raise ValueError("Tassello DPC non valido")
    values = pixels[:, :, 0].astype(float) / 255 * 100
    # Transparent pixels remain missing; they must not become zero rain.
    output = np.zeros((256, 256, 4), dtype=np.uint8)
    for level, color in (
        (0.1, (80, 180, 235)),
        (1, (40, 115, 210)),
        (5, (70, 185, 125)),
        (10, (235, 195, 50)),
        (30, (240, 110, 50)),
        (50, (200, 60, 100)),
    ):
        mask = (values >= level) & (pixels[:, :, 3] > 1)
        output[mask] = (*color, 210)
    # No-data visible as grey, covered/no-echo stays transparent over the map.
    output[pixels[:, :, 3] <= 1] = (100, 105, 115, 150)
    buffer = io.BytesIO()
    Image.fromarray(output).save(buffer, format="PNG")
    return buffer.getvalue(), at, cfg.station_id
