"""Equipment-sensitive, continuous target sessions with explicit limiting factors."""

from __future__ import annotations

import math
from itertools import pairwise

import pandas as pd

LIMITS = {
    "deep_sky": {
        "clouds": 35,
        "wind_kmh": 8,
        "gust_kmh": 16,
        "dew_margin_c": 2,
        "rain_probability": 20,
    },
    "visual": {
        "clouds": 45,
        "wind_kmh": 12,
        "gust_kmh": 24,
        "dew_margin_c": 1,
        "rain_probability": 30,
    },
    "planetary": {
        "clouds": 30,
        "wind_kmh": 8,
        "gust_kmh": 16,
        "dew_margin_c": 2,
        "rain_probability": 20,
    },
}


def session_limits(profile, custom=None):
    if profile not in LIMITS or (custom is not None and not isinstance(custom, dict)):
        raise ValueError("Profilo o limiti non validi")
    result = dict(LIMITS[profile])
    bounds = {
        "clouds": (0, 100),
        "wind_kmh": (0, 100),
        "gust_kmh": (0, 150),
        "dew_margin_c": (0, 15),
        "rain_probability": (0, 100),
    }
    for key, value in (custom or {}).items():
        if key not in bounds:
            raise ValueError("Limite non riconosciuto")
        number = float(value)
        low, high = bounds[key]
        if not math.isfinite(number) or not low <= number <= high:
            raise ValueError("Soglia fuori intervallo")
        result[key] = number
    return result


def target_sessions(tracks, weather, limits, moon_separation):
    """Intersect geometry and each exact remaining-darkness forecast interval.

    Geometry is checked at both ends of each <=15-minute segment; uncovered
    weather, missing data and even a short failed segment split the window.
    """
    summaries, detail = [], []
    if tracks.empty:
        return summaries, detail
    for target, group in tracks.groupby("target", sort=False):
        ordered = group.sort_values("valid_time").to_dict("records")
        segments, reasons = [], []
        for left, right in pairwise(ordered):
            t0, t1 = pd.Timestamp(left["valid_time"]), pd.Timestamp(right["valid_time"])
            if t1 - t0 > pd.Timedelta(minutes=15):
                continue
            relevant = (
                weather[(weather.start < t1) & (weather.end > t0)]
                if not weather.empty
                else weather
            )
            if relevant.empty:
                reasons.append("Buio o dati meteo non disponibili")
                detail.append(
                    {
                        "target": target,
                        "start": t0,
                        "end": t1,
                        "usable": False,
                        "reasons": [reasons[-1]],
                    }
                )
                continue
            for _, row in relevant.iterrows():
                start, end = max(t0, row.start), min(t1, row.end)
                blocked = []
                if not left["visible"] or not right["visible"]:
                    blocked.append("Altezza o ostacoli")
                if (
                    min(left["moon_separation"], right["moon_separation"])
                    < moon_separation
                ):
                    blocked.append("Distanza dalla Luna")
                fields = (
                    "clouds",
                    "wind_kmh",
                    "wind_gust_kmh",
                    "temp_c",
                    "dewpoint_c",
                    "precip_probability",
                    "rain_mm",
                )
                missing = [f for f in fields if pd.isna(row.get(f))]
                margin = (
                    float(row.temp_c - row.dewpoint_c)
                    if not any(f in missing for f in ("temp_c", "dewpoint_c"))
                    else None
                )
                if missing:
                    blocked.append("Dati meteo incompleti")
                else:
                    if row.clouds > limits["clouds"]:
                        blocked.append("Nuvole")
                    if (
                        row.wind_kmh > limits["wind_kmh"]
                        or row.wind_gust_kmh > limits["gust_kmh"]
                    ):
                        blocked.append(
                            "Vento o raffiche oltre la soglia dello strumento"
                        )
                    if margin < limits["dew_margin_c"]:
                        blocked.append("Margine dal punto di rugiada ridotto")
                    if (
                        row.precip_probability > limits["rain_probability"]
                        or row.rain_mm > 0
                    ):
                        blocked.append("Rischio pioggia")
                    if pd.isna(row.get("astro_score")) or row.astro_score < 65:
                        blocked.append(
                            str(
                                row.get("limiting_factor")
                                or "Qualità meteo insufficiente"
                            )
                        )
                blocked = list(dict.fromkeys(blocked))
                item = {
                    "target": target,
                    "start": start,
                    "end": end,
                    "usable": not blocked,
                    "reasons": blocked,
                    "dew_margin_c": margin,
                    "cloud_low": row.get("cloud_low"),
                    "cloud_mid": row.get("cloud_mid"),
                    "cloud_high": row.get("cloud_high"),
                    "wind_kmh": row.get("wind_kmh"),
                    "wind_gust_kmh": row.get("wind_gust_kmh"),
                    "moon_separation": min(
                        left["moon_separation"], right["moon_separation"]
                    ),
                }
                detail.append(item)
                if blocked:
                    reasons.extend(blocked)
                else:
                    if segments and start == segments[-1]["end"]:
                        segments[-1]["end"] = end
                    else:
                        segments.append({"start": start, "end": end})
        for item in segments:
            item["hours"] = (item["end"] - item["start"]).total_seconds() / 3600
        best = max(segments, key=lambda r: r["hours"], default={})
        summaries.append(
            {
                "target": target,
                "usable_hours": sum(w["hours"] for w in segments),
                "continuous_hours": best.get("hours", 0),
                "best_start": best.get("start"),
                "best_end": best.get("end"),
                "windows": segments,
                "limiting_factor": pd.Series(reasons).mode().iloc[0]
                if reasons
                else "Nessun limite nelle ore disponibili",
            }
        )
    return summaries, detail
