"""Explainable, future-only observing windows shared by web and mobile.

Scores are weather proxies, not measured seeing/SQM or a safety forecast.
All durations are computed from UTC intervals, including 23/25-hour DST days.
"""

from __future__ import annotations

from datetime import timedelta

import numpy as np
import pandas as pd
from astral import Observer
from astral.sun import dawn, dusk

from astro_weather import astronomy_score, prepare_astronomy, score_label
from config import Settings

PROFILES = {
    "visual": {"label": "Osservazione visuale", "depression": 12, "wind_limit": 12},
    "planetary": {"label": "Luna e pianeti", "depression": 6, "wind_limit": 8},
    "deep_sky": {
        "label": "Fotografia cielo profondo",
        "depression": 18,
        "wind_limit": 8,
    },
}


def observing_forecast(
    frame: pd.DataFrame,
    cfg: Settings,
    profile: str = "deep_sky",
    now: pd.Timestamp | None = None,
) -> pd.DataFrame:
    """Clip forecast intervals to remaining real darkness, never bridge gaps."""
    if frame.empty or "valid_time" not in frame:
        return pd.DataFrame()
    rules = PROFILES.get(profile, PROFILES["deep_sky"])
    now = pd.Timestamp(now if now is not None else pd.Timestamp.now(tz="UTC"))
    now = now.tz_localize("UTC") if now.tzinfo is None else now.tz_convert("UTC")
    data = frame.copy()
    data["valid_time"] = pd.to_datetime(data["valid_time"], utc=True, errors="coerce")
    data = (
        data.dropna(subset=["valid_time"])
        .sort_values("valid_time")
        .drop_duplicates("valid_time", keep="last")
    )
    data = prepare_astronomy(data, cfg)
    data["base_score"] = astronomy_score(data)
    data["next_time"] = data["valid_time"].shift(-1)
    observer = Observer(cfg.latitude, cfg.longitude, cfg.elevation_m)
    first = (now.tz_convert(cfg.local_timezone) - pd.Timedelta(days=1)).date()
    last = data["valid_time"].max().tz_convert(cfg.local_timezone).date()
    nights = []
    for day in pd.date_range(first, last):
        try:
            left = pd.Timestamp(
                dusk(
                    observer,
                    day.date(),
                    depression=rules["depression"],
                    tzinfo=cfg.local_timezone,
                )
            )
            right = pd.Timestamp(
                dawn(
                    observer,
                    day.date() + timedelta(days=1),
                    depression=rules["depression"],
                    tzinfo=cfg.local_timezone,
                )
            )
            nights.append(
                (str(day.date()), left.tz_convert("UTC"), right.tz_convert("UTC"))
            )
        except ValueError:
            # No matching twilight (polar day/night): do not invent a window.
            continue
    output = []
    for _, row in data.reset_index(drop=True).iterrows():
        start = row["valid_time"]
        interval = pd.to_numeric(row.get("interval_hours", 1), errors="coerce")
        interval = (
            min(1.0, float(interval)) if pd.notna(interval) and interval > 0 else 1.0
        )
        end = start + pd.Timedelta(hours=interval)
        if pd.notna(row["next_time"]):
            end = min(end, row["next_time"])
        missing = [
            name
            for name in (
                "clouds",
                "wind_kmh",
                "precip_probability",
                "temp_c",
                "dewpoint_c",
            )
            if pd.isna(row.get(name))
        ]
        clouds = float(row.get("clouds")) if pd.notna(row.get("clouds")) else np.nan
        wind = float(row.get("wind_kmh")) if pd.notna(row.get("wind_kmh")) else np.nan
        pop = (
            float(row.get("precip_probability"))
            if pd.notna(row.get("precip_probability"))
            else np.nan
        )
        spread = pd.to_numeric(
            row.get("temp_c", np.nan), errors="coerce"
        ) - pd.to_numeric(row.get("dewpoint_c", np.nan), errors="coerce")

        def optional(name, fallback, row=row):
            return float(row[name]) if pd.notna(row.get(name)) else fallback

        layered = sum(
            optional(name, clouds) * weight
            for name, weight in (
                ("cloud_low", 0.5),
                ("cloud_mid", 0.3),
                ("cloud_high", 0.2),
            )
        )
        factors = {
            "Nuvole": min(100, max(clouds, layered)) * 0.55,
            "Probabilità pioggia": np.clip(pop, 0, 100) * 0.18,
            "Vento": np.clip(wind - 8, 0, 45) * 0.45,
            "Raffiche": np.clip(optional("wind_gust_kmh", wind) - 18, 0, 60) * 0.22,
            "Condensa": max(0, 3 - max(0, spread)) * 5,
            "Visibilità": max(0, 10000 - optional("visibility_m", 10000)) / 1000 * 1.2,
            "Indicatori atmosferici (proxy)": max(
                0, row["base_score"] - row["astro_score"]
            ),
            "Sensibilità al vento del profilo": max(0, wind - rules["wind_limit"])
            * 0.15,
        }
        if profile == "planetary" and pd.notna(row.get("wind_300hpa_kmh")):
            factors["Corrente in quota (proxy)"] = (
                max(0, 70 - row["jet_quality"]) * 0.25
            )
        score = round(max(0, 100 - sum(factors.values())), 1) if not missing else np.nan
        limiting = max(
            factors,
            key=lambda name: factors[name] if np.isfinite(factors[name]) else -1,
        )
        for day, dark_start, dark_end in nights:
            left, right = max(start, now, dark_start), min(end, dark_end)
            if right <= left:
                continue
            result = row.to_dict()
            result.update(
                date=day,
                start=left,
                end=right,
                remaining_hours=(right - left).total_seconds() / 3600,
                astro_score=score,
                complete=not missing,
                astro_label=score_label(score)
                if not missing
                else "Valutazione incompleta",
                limiting_factor=limiting if not missing else "Dati mancanti",
                missing_fields=missing,
                profile=profile,
                penalties={
                    k: round(v, 1) if np.isfinite(v) else None
                    for k, v in factors.items()
                },
            )
            output.append(result)
    return pd.DataFrame(output)


def summarize_observing_nights(
    frame: pd.DataFrame, threshold: float = 65
) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame()
    summaries = []
    for day, group in frame.groupby("date", sort=True):
        group = group.sort_values("start")
        good = group[group["complete"] & group["astro_score"].ge(threshold)].copy()
        windows = []
        for _, row in good.iterrows():
            if windows and row["start"] <= windows[-1]["end"]:
                windows[-1]["end"] = max(windows[-1]["end"], row["end"])
            else:
                windows.append({"start": row["start"], "end": row["end"]})
        for window in windows:
            window["hours"] = (window["end"] - window["start"]).total_seconds() / 3600
        best = max(windows, key=lambda w: w["hours"], default={})
        complete = group[group["complete"]]
        weights = complete["remaining_hours"]
        mean = (
            np.average(complete["astro_score"], weights=weights)
            if len(complete) and weights.sum()
            else np.nan
        )
        summaries.append(
            {
                "date": day,
                "score": round(mean) if np.isfinite(mean) else None,
                "remaining_good_hours": round(good["remaining_hours"].sum(), 2),
                "best_start": best.get("start"),
                "best_end": best.get("end"),
                "continuous_hours": round(best.get("hours", 0), 2),
                "clouds_mean": group["clouds"].mean() if "clouds" in group else None,
                "wind_mean": group["wind_kmh"].mean(),
                "weather_score_best": complete["astro_score"].max()
                if len(complete)
                else None,
                "complete_hours": round(weights.sum(), 2),
                "incomplete_hours": round(
                    group.loc[~group["complete"], "remaining_hours"].sum(), 2
                ),
                "limiting_factor": group["limiting_factor"].mode().iloc[0],
                "windows": windows,
            }
        )
    return pd.DataFrame(summaries)
