"""Astronomical observing conditions derived from the combined forecast."""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import numpy as np
import pandas as pd

from config import Settings, settings


def astronomy_score(frame: pd.DataFrame) -> pd.Series:
    """Return a transparent 0–100 sky quality score for every forecast hour."""
    if frame.empty:
        return pd.Series(dtype=float)
    index = frame.index
    clouds = pd.to_numeric(
        frame.get("clouds", pd.Series(50, index=index)), errors="coerce"
    ).fillna(50)
    low = pd.to_numeric(frame.get("cloud_low", clouds), errors="coerce").fillna(clouds)
    mid = pd.to_numeric(frame.get("cloud_mid", clouds), errors="coerce").fillna(clouds)
    high = pd.to_numeric(frame.get("cloud_high", clouds), errors="coerce").fillna(
        clouds
    )
    pop = pd.to_numeric(
        frame.get("precip_probability", pd.Series(0, index=index)), errors="coerce"
    ).fillna(0)
    wind = pd.to_numeric(
        frame.get("wind_kmh", pd.Series(0, index=index)), errors="coerce"
    ).fillna(0)
    gust = pd.to_numeric(frame.get("wind_gust_kmh", wind), errors="coerce").fillna(wind)
    visibility = pd.to_numeric(
        frame.get("visibility_m", pd.Series(20000, index=index)), errors="coerce"
    ).fillna(10000)
    temperature = pd.to_numeric(
        frame.get("temp_c", pd.Series(np.nan, index=index)), errors="coerce"
    )
    dewpoint = pd.to_numeric(
        frame.get("dewpoint_c", pd.Series(np.nan, index=index)), errors="coerce"
    )
    dew_spread = (temperature - dewpoint).fillna(5).clip(0, 10)

    effective_cloud = 0.50 * low + 0.30 * mid + 0.20 * high
    score = (
        100
        - effective_cloud.clip(0, 100) * 0.55
        - pop.clip(0, 100) * 0.18
        - np.maximum(wind - 8, 0).clip(0, 45) * 0.45
        - np.maximum(gust - 18, 0).clip(0, 60) * 0.22
        - np.maximum(3.0 - dew_spread, 0) * 5.0
        - np.maximum(10000 - visibility, 0) / 1000.0 * 1.2
    )
    return score.clip(0, 100).round(0)


def score_label(value: float) -> str:
    if value >= 80:
        return "Ottimo"
    if value >= 65:
        return "Buono"
    if value >= 45:
        return "Discreto"
    return "Scarso"


def prepare_astronomy(frame: pd.DataFrame, cfg: Settings = settings) -> pd.DataFrame:
    if frame.empty:
        return frame
    data = frame.copy()
    data["local_time"] = data["valid_time"].dt.tz_convert(cfg.local_timezone)
    if "is_day" in data and data["is_day"].notna().any():
        data["is_night"] = (
            pd.to_numeric(data["is_day"], errors="coerce").fillna(1).eq(0)
        )
    else:
        hour = data["local_time"].dt.hour
        data["is_night"] = (hour >= 20) | (hour < 6)
    data["astro_score"] = astronomy_score(data)
    data["astro_label"] = data["astro_score"].map(score_label)
    return data


def best_observing_windows(
    frame: pd.DataFrame,
    minimum_score: float = 65,
    minimum_hours: int = 2,
) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(
            columns=["start", "end", "hours", "score", "clouds", "wind_kmh"]
        )
    candidates = frame[
        frame["is_night"] & (frame["astro_score"] >= minimum_score)
    ].copy()
    if candidates.empty:
        return pd.DataFrame(
            columns=["start", "end", "hours", "score", "clouds", "wind_kmh"]
        )
    candidates = candidates.sort_values("valid_time")
    candidates["group"] = (
        candidates["valid_time"].diff().gt(pd.Timedelta(hours=1.5)).cumsum()
    )
    rows = []
    for _, group in candidates.groupby("group"):
        hours = len(group)
        if hours < minimum_hours:
            continue
        clouds = (
            pd.to_numeric(group["clouds"], errors="coerce").mean()
            if "clouds" in group
            else np.nan
        )
        wind = (
            pd.to_numeric(group["wind_kmh"], errors="coerce").mean()
            if "wind_kmh" in group
            else np.nan
        )
        rows.append(
            {
                "start": group["local_time"].iloc[0],
                "end": group["local_time"].iloc[-1] + pd.Timedelta(hours=1),
                "hours": hours,
                "score": float(group["astro_score"].mean()),
                "clouds": float(clouds),
                "wind_kmh": float(wind),
            }
        )
    return (
        pd.DataFrame(rows).sort_values(["score", "start"], ascending=[False, True])
        if rows
        else pd.DataFrame()
    )


def daily_astronomy_summary(
    frame: pd.DataFrame, events: pd.DataFrame | None = None
) -> pd.DataFrame:
    """Summarise forecast conditions per local observing night."""
    if frame.empty or "local_time" not in frame or "is_night" not in frame:
        return pd.DataFrame()
    night = frame[frame["is_night"]].copy()
    if night.empty:
        return pd.DataFrame()
    # An observing night crosses midnight: shifting by twelve hours keeps the
    # hours after midnight attached to the preceding evening.
    night["date"] = (night["local_time"] - pd.Timedelta(hours=12)).dt.date
    rows: list[dict[str, Any]] = []
    for observing_date, group in night.groupby("date", sort=True):
        scores = pd.to_numeric(group.get("astro_score"), errors="coerce")
        clouds = pd.to_numeric(
            group.get("clouds", pd.Series(np.nan, index=group.index)),
            errors="coerce",
        )
        wind = pd.to_numeric(
            group.get("wind_kmh", pd.Series(np.nan, index=group.index)),
            errors="coerce",
        )
        visibility = pd.to_numeric(
            group.get("visibility_m", pd.Series(np.nan, index=group.index)),
            errors="coerce",
        )
        rows.append(
            {
                "date": observing_date,
                "weather_score_mean": float(scores.mean()),
                "weather_score_best": float(scores.max()),
                "good_hours": int(scores.ge(65).sum()),
                "clouds_mean": float(clouds.mean()),
                "wind_mean": float(wind.mean()),
                "visibility_km": float(visibility.mean() / 1000.0),
            }
        )
    summary = pd.DataFrame(rows)
    if events is not None and not events.empty and "date" in events:
        lunar = events[["date", "moon_illumination"]].copy()
        lunar["date"] = pd.to_datetime(lunar["date"], errors="coerce").dt.date
        summary = summary.merge(lunar, on="date", how="left")
    return summary


def astronomy_events(
    cfg: Settings = settings, days: int = 7, start_date: date | None = None
) -> pd.DataFrame:
    """Sun and Moon events; returns an empty frame if Astral is unavailable."""
    try:
        from zoneinfo import ZoneInfo

        from astral import Observer
        from astral.moon import moonrise, moonset, phase
        from astral.sun import sun
    except ImportError:
        return pd.DataFrame()
    observer = Observer(cfg.latitude, cfg.longitude, cfg.elevation_m)
    timezone = ZoneInfo(cfg.local_timezone)
    first_day = start_date or pd.Timestamp.now(tz=timezone).date()
    rows: list[dict[str, Any]] = []
    for offset in range(days):
        current = first_day + timedelta(days=offset)
        solar = sun(observer, date=current, tzinfo=timezone)
        try:
            rise = moonrise(observer, date=current, tzinfo=timezone)
        except (ValueError, TypeError):
            rise = None
        try:
            moon_set = moonset(observer, date=current, tzinfo=timezone)
        except (ValueError, TypeError):
            moon_set = None
        moon_phase = float(phase(current))
        illumination = (1.0 - np.cos(2 * np.pi * moon_phase / 28.0)) / 2.0 * 100.0
        rows.append(
            {
                "date": current,
                "sunset": solar["sunset"],
                "dusk": solar["dusk"],
                "dawn": solar["dawn"],
                "sunrise": solar["sunrise"],
                "moonrise": rise,
                "moonset": moon_set,
                "moon_phase": moon_phase,
                "moon_illumination": illumination,
            }
        )
    return pd.DataFrame(rows)
