"""Continuous event frequencies on identified ensemble member trajectories."""

import json
import re

import numpy as np
import pandas as pd
from sqlalchemy import text

from db import get_engine
from v5_calibration import observed_hourly, utc

FIELDS = {
    "precipitation": "rain",
    "cloud_cover": "clouds",
    "wind_speed_10m": "wind",
    "wind_gusts_10m": "gust",
    "temperature_2m": "temp",
    "relative_humidity_2m": "rh",
}
HOURLY_MODELS = {"icon_seamless", "icon_eu", "icon_global", "icon_d2"}


def member_paths(payload, model, acquired_at):
    """Keep exact member IDs across variables; no spatial or time interpolation."""
    hourly = payload.get("hourly", {})
    times = utc(hourly.get("time", []))
    positions = [
        i
        for i, t in enumerate(times)
        if pd.notna(t)
        and acquired_at.floor("h") <= t <= acquired_at + pd.Timedelta(hours=48)
    ]
    members = {}
    for external, internal in FIELDS.items():
        for key, values in hourly.items():
            match = re.fullmatch(re.escape(external) + r"(?:_member(\d+))?", key)
            if not match or not isinstance(values, list) or len(values) != len(times):
                continue
            identifier = str(int(match[1])) if match[1] else "0"
            if identifier not in members and len(members) >= 100:
                continue
            numbers = pd.to_numeric(pd.Series(values), errors="coerce")
            members.setdefault(identifier, {})[internal] = [
                float(numbers.iloc[i]) if np.isfinite(numbers.iloc[i]) else None
                for i in positions
            ]
    return {
        "model": model,
        "acquired_at": acquired_at.isoformat(),
        "hourly_native": model in HOURLY_MODELS,
        "times": [times[i].isoformat() for i in positions],
        "members": members,
    }


def window_probabilities(paths, cfg=None):
    if not paths.get("hourly_native"):
        return []
    times = utc(paths["times"])
    now = utc(paths["acquired_at"])
    members = paths.get("members", {})
    rows = []
    for kind, duration in (("dry", 2), ("photo", 3)):
        for i in range(len(times) - duration):
            start, end = times[i], times[i + duration]
            if start < now or any(
                times[k + 1] - times[k] != pd.Timedelta(hours=1)
                for k in range(i, i + duration)
            ):
                continue
            dark = None
            if kind == "photo" and cfg is not None:
                from astral import Observer
                from astral.sun import elevation

                observer = Observer(cfg.latitude, cfg.longitude, cfg.elevation_m)
                dark = all(
                    elevation(observer, at.to_pydatetime()) <= -18
                    for at in pd.date_range(start, end, freq="15min")
                )
                if not dark:
                    continue
            successes = count = 0
            for member in members.values():
                rain = np.array(
                    [
                        member.get("rain", [None] * len(times))[j]
                        for j in range(i + 1, i + duration + 1)
                    ],
                    dtype=float,
                )
                # Open-Meteo precipitation is the preceding-hour total.
                if not np.isfinite(rain).all() or (rain < 0).any():
                    continue
                good = bool((rain < 0.1).all())
                if kind == "photo":
                    required = ["clouds", "wind", "gust", "temp", "rh"]
                    vectors = {
                        k: np.array(
                            member.get(k, [None] * len(times))[i : i + duration + 1],
                            dtype=float,
                        )
                        for k in required
                    }
                    if any(
                        len(v) != duration + 1 or not np.isfinite(v).all()
                        for v in vectors.values()
                    ):
                        continue
                    c, w, g, t, rh = [vectors[k] for k in required]
                    if not (
                        (c >= 0)
                        & (c <= 100)
                        & (w >= 0)
                        & (g >= 0)
                        & (t >= -60)
                        & (t <= 60)
                        & (rh > 0)
                        & (rh <= 100)
                    ).all():
                        continue
                    gamma = np.log(rh / 100) + 17.625 * t / (243.04 + t)
                    dew = 243.04 * gamma / (17.625 - gamma)
                    good = good and bool(
                        ((c <= 35) & (w <= 8) & (g <= 16) & (t - dew >= 2)).all()
                    )
                count += 1
                successes += int(good)
            # Missing trajectories cannot improve an event probability silently.
            available = count >= 10 and count >= 0.8 * len(members)
            rows.append(
                {
                    "kind": kind,
                    "start": start,
                    "end": end,
                    "hours": duration,
                    "members": count,
                    "total_members": len(members),
                    "successful_members": successes,
                    "probability": successes / count * 100 if available else None,
                    "darkness_checked": dark,
                }
            )
    return rows


def archive_paths(station_id, paths, cfg):
    from v5_data import clean_json
    from v56_probability_calibration import apply_to_rows, calibration_key

    acquired = utc(paths["acquired_at"])
    with get_engine().connect() as con:
        saved = con.execute(
            text("SELECT payload FROM v5_products WHERE product_key=:key"),
            {"key": calibration_key(station_id)},
        ).scalar()
    calibration = json.loads(saved) if saved else None
    rows = window_probabilities(paths, cfg)
    apply_to_rows(rows, station_id, paths["model"], acquired, calibration)
    product = clean_json(
        {
            "station_id": station_id,
            "acquired_at": acquired,
            "model": paths["model"],
            "rows": rows,
            "note": "Frequenze dei membri completi dello stesso ensemble. La correzione locale, quando indicata, riguarda solo due ore asciutte previste 6–12 ore prima e supera una prova su giorni successivi. Due ore con meno di 0,1 mm per ora; fotografia: tre ore di buio, nuvole ≤35%, vento ≤8 km/h, raffiche ≤16 km/h, margine rugiada ≥2 °C. Fotografia non calibrata; campioni orari, non garanzia al minuto.",
        }
    )
    with get_engine().begin() as con:
        for key, value in (
            ("member-paths:" + station_id, paths),
            ("windows:" + station_id, product),
        ):
            con.execute(
                text(
                    "INSERT INTO v5_products(product_key,attempted_at,payload) VALUES(:key,:at,:payload) ON CONFLICT(product_key) DO UPDATE SET attempted_at=excluded.attempted_at,payload=excluded.payload"
                ),
                {
                    "key": key,
                    "at": acquired.isoformat(),
                    "payload": json.dumps(value, allow_nan=False),
                },
            )
        con.execute(
            text(
                "INSERT INTO window_predictions(station_id,cycle,acquired_at,payload) VALUES(:id,:cycle,:at,:payload) ON CONFLICT(station_id,cycle) DO NOTHING"
            ),
            {
                "id": station_id,
                "cycle": acquired.floor("6h").isoformat(),
                "at": acquired.isoformat(),
                "payload": json.dumps(product, allow_nan=False),
            },
        )
        # Keep the complete 90-day calibration window hot; archive older rows.


def validation_key(station_id):
    return "window-validation:v56:" + station_id


def sample_readiness(selected, timezone="Europe/Rome"):
    """Operational review gates, not a promise of statistical calibration."""
    days = {t.tz_convert(timezone).date() for t in selected}
    wet_days = {
        t.tz_convert(timezone).date() for t, (_, o) in selected.items() if not o
    }
    dry = sum(o for _, o in selected.values())
    requirements = [
        ("days", "Giorni con riscontri", len(days), 30),
        ("windows", "Finestre verificate", len(selected), 200),
        ("dry", "Finestre asciutte", dry, 30),
        ("wet", "Finestre con pioggia", len(selected) - dry, 30),
        ("wet_days", "Giorni con pioggia nelle finestre", len(wet_days), 5),
    ]
    gates = [
        {
            "key": key,
            "label": label,
            "current": current,
            "minimum": minimum,
            "remaining": max(0, minimum - current),
            "met": current >= minimum,
        }
        for key, label, current, minimum in requirements
    ]
    return {
        "status": "ready_for_study" if all(g["met"] for g in gates) else "collecting",
        "applied": False,
        "days": len(days),
        "wet_days": len(wet_days),
        "dry": dry,
        "wet": len(selected) - dry,
        "requirements": gates,
        "first_verified": min(selected) if selected else None,
        "last_verified": max(selected) if selected else None,
        "note": "Soglie prudenziali del progetto per iniziare la prova. Le finestre dello stesso giorno possono essere correlate. L’attivazione automatica richiede anche un miglioramento su giorni successivi tenuti separati, rispetto alla frequenza originale e a quella di riferimento. I tempi dipendono dalla pioggia e dalla copertura dei riscontri. La fotografia resta senza calibrazione osservativa del cielo.",
    }


def collect_window_samples(station_id, now=None):
    from data_access import load_station

    now = utc(now if now is not None else pd.Timestamp.now(tz="UTC"))
    with get_engine().connect() as con:
        archive = con.execute(
            text(
                "SELECT acquired_at,payload FROM window_predictions WHERE station_id=:id ORDER BY acquired_at DESC LIMIT 360"
            ),
            {"id": station_id},
        ).all()
    hourly = window_rain_hourly(load_station(24 * 92, station_id))
    rain = (
        hourly.set_index("time").rain_mm
        if "rain_mm" in hourly
        else pd.Series(dtype=float)
    )
    candidates = {}
    acquisitions = []
    for acquired, payload in archive:
        acquired = utc(acquired)
        if pd.isna(acquired) or acquired > now:
            continue
        acquisitions.append(acquired)
        product = json.loads(payload)
        for row in product.get("rows", []):
            start, end = utc(row["start"]), utc(row["end"])
            lead = (start - acquired).total_seconds() / 3600
            if (
                row["kind"] != "dry"
                or pd.isna(start)
                or pd.isna(end)
                or start < now - pd.Timedelta(days=90)
                or end - start != pd.Timedelta(hours=2)
                or not 6 <= lead < 12
                or start.hour % 2
                or start != start.floor("h")
            ):
                continue
            if start not in candidates or acquired > candidates[start][0]:
                candidates[start] = (acquired, end, row, product.get("model"))
    selected = {}
    pending = missing_forecast = missing_observations = 0
    for start, (acquired, end, row, model) in candidates.items():
        if end > now:
            pending += 1
            continue
        try:
            probability = float(row.get("raw_probability", row.get("probability")))
        except (TypeError, ValueError):
            probability = float("nan")
        if not np.isfinite(probability) or not 0 <= probability <= 100:
            missing_forecast += 1
            continue
        values = rain.reindex(
            pd.date_range(start + pd.Timedelta(hours=1), end, freq="h")
        )
        if len(values) != 2 or values.isna().any():
            missing_observations += 1
            continue
        selected[start] = {
            "start": start,
            "end": end,
            "known_at": acquired,
            "raw": probability / 100,
            "observed": int(values.lt(0.1).all()),
            "model": model,
            "calibration_id": row.get("calibration_id"),
            "calibrated": row.get("calibrated_probability"),
        }
    return selected, {
        "cycles": len(acquisitions),
        "first_acquired": min(acquisitions) if acquisitions else None,
        "last_acquired": max(acquisitions) if acquisitions else None,
        "eligible_windows": len(candidates),
        "pending": pending,
        "missing_forecast": missing_forecast,
        "missing_observations": missing_observations,
        "coverage_percent": 100 * len(selected) / (len(candidates) - pending)
        if len(candidates) > pending
        else None,
        "retention_days": 90,
    }


def window_rain_hourly(observations):
    """Keep valid rain when a different sensor is flagged, never estimated rain."""
    observations = observations.copy()
    if "data_quality" in observations:
        unrelated = {
            f"{flag}_{variable}"
            for flag in ("stuck", "spike")
            for variable in (
                "temp_c",
                "humidity",
                "pressure_hpa",
                "wind_kmh",
                "windgust_kmh",
            )
        } | {"gust_below_mean_wind"}

        def rain_quality(value):
            flags = [
                flag for flag in str(value or "").split(";") if flag not in unrelated
            ]
            if any(flag in {"estimated_rain", "legacy_unknown_rain"} for flag in flags):
                flags.append("invalid_rain")
            return ";".join(flags)

        observations["data_quality"] = observations.data_quality.fillna("").map(
            rain_quality
        )
    return observed_hourly(
        observations[
            [c for c in ("time", "rain_mm", "data_quality") if c in observations]
        ]
    )


def validate_windows(station_id, now=None):
    from v5_data import clean_json

    now = utc(now if now is not None else pd.Timestamp.now(tz="UTC"))
    samples, collection = collect_window_samples(station_id, now)
    selected = {start: (s["raw"], s["observed"]) for start, s in samples.items()}
    bins = []
    for low in (0, 0.2, 0.4, 0.6, 0.8):
        values = [
            (p, o)
            for p, o in selected.values()
            if low <= p and (p < low + 0.2 or low == 0.8)
        ]
        bins.append(
            {
                "low": low * 100,
                "high": (low + 0.2) * 100,
                "n": len(values),
                "forecast": np.mean([p for p, _ in values]) * 100 if values else None,
                "observed": np.mean([o for _, o in values]) * 100 if values else None,
            }
        )
    return clean_json(
        {
            "evaluated_at": now,
            "n": len(selected),
            "brier": np.mean([(p - o) ** 2 for p, o in selected.values()])
            if selected
            else None,
            "bins": bins,
            "collection": collection,
            "readiness": sample_readiness(selected),
            "note": "Riscontri prospettici di finestre asciutte non sovrapposte, previste 6–12 ore prima; ogni ora osservata richiede 12 campioni validi. Brier e diagramma riguardano le frequenze originali, conservate anche dopo una correzione. La fotografia non è validabile automaticamente senza misure di nuvole e qualità del cielo.",
        }
    )
