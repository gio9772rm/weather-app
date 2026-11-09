"""Read-only night planning from a single published weather revision."""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
from datetime import date, timedelta

import numpy as np
import pandas as pd
from astral import Observer

from astronomy_planner import (
    TARGETS,
    field_of_view,
    framing_assessment,
    lunar_track,
    target_geometry,
)
from config import Settings
from v5_astronomy import PROFILES, night_darkness
from v5_data import clean_json, station_settings
from v5_extensions import plan_constraints
from v5_sessions import (
    continuous_windows,
    session_limits,
    target_sessions,
    weather_blockers,
)

STEP = pd.Timedelta(minutes=15)
HOUR = pd.Timedelta(hours=1)
EMISSION_NEBULAE = {"M42", "M27", "M8", "NGC 2237", "NGC 6960", "NGC 7000"}
WEATHER_FIELDS = (
    "clouds",
    "cloud_low",
    "cloud_mid",
    "cloud_high",
    "wind_kmh",
    "wind_gust_kmh",
    "precip_probability",
    "rain_mm",
    "temp_c",
    "dewpoint_c",
    "issued_at",
)
HOURLY_FIELDS = (
    "start",
    "end",
    "astro_score",
    "clouds",
    "wind_kmh",
    "complete",
    "limiting_factor",
    "penalties",
    "missing_fields",
    "probability_source",
)


@dataclass(frozen=True)
class ObservingSite:
    station_id: str
    location_name: str
    local_timezone: str
    latitude: float
    longitude: float
    elevation_m: float

    @classmethod
    def from_station(cls, cfg: Settings):
        return cls(
            cfg.station_id,
            cfg.location_name,
            cfg.local_timezone,
            cfg.latitude,
            cfg.longitude,
            cfg.elevation_m,
        )


@dataclass(frozen=True)
class ObservingContext:
    """Geometry and weather at the same site, with private coordinates."""

    site: ObservingSite
    snapshot: dict
    now: pd.Timestamp


def _forecast_intervals(rows):
    points = {pd.Timestamp(row["valid_time"]): row for row in rows}
    times = sorted(points)
    for i, start in enumerate(times):
        row = points[start]
        hours = row.get("interval_hours")
        end = start + HOUR * (min(1, hours) if hours and hours > 0 else 1)
        if i + 1 < len(times):
            end = min(end, times[i + 1])
        if end > start:
            yield start, end, {key: row.get(key) for key in WEATHER_FIELDS}


def _weather_for_night(hours, intervals, day, dark, now):
    if not dark:
        return []
    rows = []
    for hour in hours:
        if hour["date"] != day:
            continue
        left = max(pd.Timestamp(hour["start"]), dark[0], now)
        right = min(pd.Timestamp(hour["end"]), dark[1])
        for start, end, fields in intervals:
            # Both forecasts use the same publication. An unpaired interval
            # is unknown, even when the astronomical score is present.
            a, b = max(left, start), min(right, end)
            if b > a:
                rows.append(
                    {
                        **fields,
                        "start": a,
                        "end": b,
                        "astro_score": hour["astro_score"],
                        "limiting_factor": hour["limiting_factor"],
                    }
                )
    return sorted(rows, key=lambda row: row["start"])


def _weather_summary(rows, dark, now, limits):
    usable, blockers = [], Counter()
    assessed = 0.0
    for row in rows:
        reasons, _ = weather_blockers(row, limits)
        hours = (row["end"] - row["start"]) / HOUR
        if reasons != ["Dati meteo incompleti"]:
            assessed += hours
        if reasons:
            for reason in reasons:
                blockers[reason] += hours
        else:
            usable.append((row["start"], row["end"]))
    windows = continuous_windows(usable)
    remaining = max(0, (dark[1] - max(dark[0], now)) / HOUR) if dark else 0
    unknown = max(0, remaining - assessed)
    if unknown > 1e-6:
        blockers["Dati meteo incompleti"] = unknown
    return {
        "usable_hours": round(sum(item["hours"] for item in windows), 2),
        "assessed_hours": round(assessed, 2),
        "unknown_hours": round(unknown, 2),
        "best_window": max(windows, key=lambda item: item["hours"], default=None),
        "limiting_factor": blockers.most_common(1)[0][0] if blockers else None,
    }, windows


def _timeline_weather(intervals, left, right, now):
    return [
        {
            **fields,
            "start": max(start, left, now),
            "end": min(end, right),
            "dew_margin_c": round(fields["temp_c"] - fields["dewpoint_c"], 1)
            if fields["temp_c"] is not None and fields["dewpoint_c"] is not None
            else None,
        }
        for start, end, fields in intervals
        if start < right and end > max(left, now)
    ]


def _target_options(site, times, lunar, weather, dark, now, constraints, limits):
    altitude, separation, rotation, horizon, equipment = constraints
    targets, tracks = [], []
    dark_samples = (times >= max(dark[0], now)) & (times <= dark[1])
    # Keep the endpoints adjacent to the exact twilight/forecast boundaries
    # for the same conservative segment intersection as the planner.
    relevant = (times >= max(dark[0], now) - STEP) & (times <= dark[1] + STEP)
    for target in TARGETS:
        geometry = target_geometry(target, times, site, lunar, altitude, horizon)
        tracks.append(
            pd.DataFrame(
                {
                    "target": target.name,
                    "valid_time": times[relevant],
                    **{
                        key: geometry[key][relevant]
                        for key in ("visible", "moon_visible", "moon_separation")
                    },
                }
            )
        )
        framing = (
            framing_assessment(target, equipment, rotation_deg=rotation)
            if equipment
            else None
        )
        peak = (
            np.max(geometry["altitude"][dark_samples]) if dark_samples.any() else None
        )
        targets.append(
            {
                "id": target.name,
                "name": target.common_name,
                "category": target.category,
                "filters": ["broadband", "narrowband"]
                if target.name in EMISSION_NEBULAE
                else ["broadband"],
                "framing": asdict(framing) if framing else None,
                "peak_altitude": round(peak, 1) if peak is not None else None,
                "altitude": geometry["altitude"].round(1).tolist(),
                "minimum_altitude": np.maximum(altitude, geometry["horizon_altitude"])
                .round(1)
                .tolist(),
                "moon_separation": geometry["moon_separation"].round(1).tolist(),
            }
        )
    sessions, _ = target_sessions(
        pd.concat(tracks, ignore_index=True),
        pd.DataFrame(weather),
        limits,
        separation,
    )
    by_target = {row["target"]: row for row in sessions}
    for target in targets:
        session = by_target.get(target["id"], {})
        windows = []
        for window in session.get("windows", []):
            start, end = window["start"].ceil("min"), window["end"].floor("min")
            if end - start >= STEP:
                # The existing planner accepts at most sixteen real hours.
                end = min(end, start + 16 * HOUR)
                windows.append(
                    {"start": start, "end": end, "hours": (end - start) / HOUR}
                )
        target.update(
            usable_hours=round(session.get("usable_hours", 0), 2),
            windows=windows,
            best_window=max(windows, key=lambda window: window["hours"], default=None),
            limiting_factor=session.get(
                "limiting_factor", "Finestra futura troppo breve"
            ),
        )

    def rank(target):
        fit = (target["framing"] or {}).get("fits")
        window = target["best_window"]
        peak = target["peak_altitude"]
        return (
            window is None,
            fit is False,
            fit is None,
            -(window["hours"] if window else 0),
            -target["usable_hours"],
            -(peak if peak is not None else -90),
            target["id"],
        )

    return sorted(targets, key=rank)


def night_report(context: ObservingContext, values: dict) -> dict:
    site, snapshot, now = context.site, context.snapshot, context.now
    profile = values.get("profile", "deep_sky")
    limits = session_limits(profile, values.get("limits"))
    constraints = plan_constraints(values)
    altitude, separation, rotation, horizon, equipment = constraints
    requested = (
        date.fromisoformat(values["date"]).isoformat() if values.get("date") else None
    )
    depression = PROFILES[profile]["depression"]
    observer = Observer(site.latitude, site.longitude, site.elevation_m)
    today = now.tz_convert(site.local_timezone).date()
    intervals = list(_forecast_intervals(snapshot.get("forecast", [])))
    hours = snapshot.get("astronomy", {}).get(profile, {}).get("hours", [])
    nights, detail = [], {}
    for offset in (-1, 0, 1, 2):
        day = today + timedelta(days=offset)
        civil = night_darkness(observer, day, site.local_timezone, 6)
        dark = (
            civil
            if depression == 6
            else night_darkness(observer, day, site.local_timezone, depression)
        )
        bounds = dark or civil
        if offset < 0 and (not bounds or bounds[1] <= now):
            continue
        rows = _weather_for_night(hours, intervals, day.isoformat(), dark, now)
        summary, windows = _weather_summary(rows, dark, now, limits)
        nights.append(
            {
                "date": day.isoformat(),
                "darkness": dark,
                "ongoing": bool(bounds and bounds[0] <= now < bounds[1]),
                **summary,
            }
        )
        detail[day.isoformat()] = (civil or dark, dark, rows, windows)
        if len(nights) == 3:
            break
    selected = requested if requested in detail else nights[0]["date"]
    bounds, dark, weather, windows = detail[selected]
    timeline, targets = None, []
    if bounds and bounds[0] < bounds[1]:
        left, right = bounds
        times = pd.date_range(left.floor("min"), right.ceil("min"), freq=STEP)
        if times[-1] < right:
            times = times.append(pd.DatetimeIndex([right.ceil("min")]))
        lunar = lunar_track(times, site)
        timeline = {
            "start": left,
            "end": right,
            "darkness": dark,
            "times": times.tolist(),
            "moon_altitude": lunar["altitude"].round(1).tolist(),
            "moon_illumination": lunar["illumination"].round(1).tolist(),
            "weather": _timeline_weather(intervals, left, right, now),
            "weather_windows": windows,
            "hourly": [
                {key: row.get(key) for key in HOURLY_FIELDS}
                for row in hours
                if row["date"] == selected and pd.Timestamp(row["end"]) > now
            ],
        }
        if profile != "planetary" and dark and dark[1] > now:
            targets = _target_options(
                site, times, lunar, weather, dark, now, constraints, limits
            )
    return clean_json(
        {
            "schema": 1,
            "site": {
                "id": site.station_id,
                "name": site.location_name,
                "timezone": site.local_timezone,
            },
            "snapshot_at": snapshot.get("generated_at"),
            "as_of": now,
            "profile": profile,
            "depression": depression,
            "nights": nights,
            "selected": selected,
            "timeline": timeline,
            "targets": targets,
            "field": asdict(field_of_view(equipment)) if equipment else None,
            "constraints": {
                "altitude": altitude,
                "moon": separation,
                "rotation": rotation,
                "horizon_configured": bool(horizon),
                "limits": limits,
            },
        }
    )


def observing_report(station_id: str, values: dict, snapshot: dict):
    return night_report(
        ObservingContext(
            ObservingSite.from_station(station_settings(station_id)),
            snapshot,
            pd.Timestamp.now(tz="UTC"),
        ),
        values,
    )
