"""Personal observing planner for fixed deep-sky targets."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

import numpy as np
import pandas as pd

from config import Settings


@dataclass(frozen=True)
class SkyTarget:
    name: str
    common_name: str
    category: str
    ra_deg: float
    dec_deg: float
    magnitude: float


TARGETS = (
    SkyTarget("M31", "Galassia di Andromeda", "Galassia", 10.6847, 41.2692, 3.4),
    SkyTarget("M42", "Nebulosa di Orione", "Nebulosa", 83.8221, -5.3911, 4.0),
    SkyTarget("M45", "Pleiadi", "Ammasso", 56.7500, 24.1167, 1.6),
    SkyTarget("M51", "Galassia Vortice", "Galassia", 202.4696, 47.1952, 8.4),
    SkyTarget("M81", "Galassia di Bode", "Galassia", 148.8882, 69.0653, 6.9),
    SkyTarget("M82", "Galassia Sigaro", "Galassia", 148.9683, 69.6797, 8.4),
    SkyTarget("M13", "Ammasso di Ercole", "Ammasso", 250.4235, 36.4613, 5.8),
    SkyTarget("M27", "Nebulosa Manubrio", "Nebulosa", 299.9010, 22.7210, 7.4),
    SkyTarget("M8", "Nebulosa Laguna", "Nebulosa", 270.9250, -24.3800, 6.0),
    SkyTarget("NGC 2237", "Nebulosa Rosetta", "Nebulosa", 97.9800, 4.9800, 9.0),
    SkyTarget("NGC 6960", "Velo occidentale", "Nebulosa", 312.7500, 30.7200, 7.0),
    SkyTarget("NGC 7000", "Nord America", "Nebulosa", 314.0000, 44.5000, 4.0),
)
TARGET_BY_NAME = {target.name: target for target in TARGETS}


def target_labels() -> list[str]:
    return [f"{target.name} · {target.common_name}" for target in TARGETS]


def _julian_date(times: pd.Series | pd.DatetimeIndex) -> np.ndarray:
    timestamps = pd.to_datetime(times, utc=True, errors="coerce")
    values = np.asarray(timestamps.array.asi8, dtype=float)
    values[values == float(pd.NaT.value)] = np.nan
    return values / 86_400_000_000_000.0 + 2_440_587.5


def equatorial_altaz(
    ra_deg: float,
    dec_deg: float,
    times: pd.Series | pd.DatetimeIndex,
    *,
    latitude: float,
    longitude: float,
) -> tuple[np.ndarray, np.ndarray]:
    """Approximate apparent altitude/azimuth, sufficient for hourly planning."""
    julian = _julian_date(times)
    sidereal = (280.46061837 + 360.98564736629 * (julian - 2451545.0)) % 360
    hour_angle = np.deg2rad((sidereal + longitude - ra_deg + 180) % 360 - 180)
    latitude_rad = np.deg2rad(latitude)
    declination = np.deg2rad(dec_deg)
    altitude = np.arcsin(
        np.sin(latitude_rad) * np.sin(declination)
        + np.cos(latitude_rad) * np.cos(declination) * np.cos(hour_angle)
    )
    azimuth = np.arctan2(
        -np.sin(hour_angle),
        np.tan(declination) * np.cos(latitude_rad)
        - np.sin(latitude_rad) * np.cos(hour_angle),
    )
    return np.rad2deg(altitude), (np.rad2deg(azimuth) + 360) % 360


def _moon_position(julian: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Low-precision geocentric Moon position and illumination for planning."""
    days = julian - 2451545.0
    longitude = np.deg2rad(
        (
            218.316
            + 13.176396 * days
            + 6.289 * np.sin(np.deg2rad(134.963 + 13.064993 * days))
        )
        % 360
    )
    latitude = np.deg2rad(5.128 * np.sin(np.deg2rad(93.272 + 13.229350 * days)))
    obliquity = np.deg2rad(23.4393 - 0.0000004 * days)
    x = np.cos(longitude) * np.cos(latitude)
    y = np.sin(longitude) * np.cos(latitude) * np.cos(obliquity) - np.sin(
        latitude
    ) * np.sin(obliquity)
    z = np.sin(longitude) * np.cos(latitude) * np.sin(obliquity) + np.sin(
        latitude
    ) * np.cos(obliquity)
    ra = (np.rad2deg(np.arctan2(y, x)) + 360) % 360
    dec = np.rad2deg(np.arcsin(z))
    phase_age = (julian - 2451550.1) % 29.53058867
    illumination = (1 - np.cos(2 * np.pi * phase_age / 29.53058867)) / 2 * 100
    return ra, dec, illumination


def _angular_separation(
    ra1_deg: float,
    dec1_deg: float,
    ra2_deg: np.ndarray,
    dec2_deg: np.ndarray,
) -> np.ndarray:
    ra1 = np.deg2rad(ra1_deg)
    dec1 = np.deg2rad(dec1_deg)
    ra2 = np.deg2rad(ra2_deg)
    dec2 = np.deg2rad(dec2_deg)
    cosine = np.sin(dec1) * np.sin(dec2) + np.cos(dec1) * np.cos(dec2) * np.cos(
        ra1 - ra2
    )
    return np.rad2deg(np.arccos(np.clip(cosine, -1, 1)))


def _target_names(values: Iterable[str]) -> list[str]:
    selected: list[str] = []
    for value in values:
        name = str(value).split(" · ", 1)[0].strip()
        if name in TARGET_BY_NAME and name not in selected:
            selected.append(name)
    return selected


def plan_targets(
    astronomy: pd.DataFrame,
    cfg: Settings,
    selected_targets: Iterable[str],
    *,
    minimum_altitude: float = 25,
    minimum_moon_separation: float = 30,
) -> pd.DataFrame:
    """Rank the best forecast hour for each selected deep-sky object."""
    names = _target_names(selected_targets)
    columns = [
        "target",
        "name",
        "category",
        "magnitude",
        "best_time",
        "altitude",
        "azimuth",
        "planner_score",
        "weather_score",
        "clouds",
        "dew_risk",
        "moon_separation",
        "moon_illumination",
        "status",
    ]
    if astronomy.empty or not names:
        return pd.DataFrame(columns=columns)
    frame = astronomy.copy()
    frame["valid_time"] = pd.to_datetime(frame["valid_time"], utc=True, errors="coerce")
    if "local_time" not in frame:
        frame["local_time"] = frame["valid_time"].dt.tz_convert(cfg.local_timezone)
    if "is_night" not in frame:
        hours = frame["local_time"].dt.hour
        frame["is_night"] = (hours >= 20) | (hours < 6)
    frame = frame[frame["is_night"] & frame["valid_time"].notna()].copy()
    if frame.empty:
        return pd.DataFrame(columns=columns)
    julian = _julian_date(frame["valid_time"])
    moon_ra, moon_dec, moon_illumination = _moon_position(julian)
    weather_score = (
        pd.to_numeric(frame.get("astro_score"), errors="coerce").fillna(0).to_numpy()
    )
    clouds = pd.to_numeric(frame.get("clouds"), errors="coerce").to_numpy()
    dew_risk = pd.to_numeric(
        frame.get("dew_risk", pd.Series(np.nan, index=frame.index)), errors="coerce"
    ).to_numpy()
    rows: list[dict[str, object]] = []
    for name in names:
        target = TARGET_BY_NAME[name]
        altitude, azimuth = equatorial_altaz(
            target.ra_deg,
            target.dec_deg,
            frame["valid_time"],
            latitude=cfg.latitude,
            longitude=cfg.longitude,
        )
        separation = _angular_separation(
            target.ra_deg, target.dec_deg, moon_ra, moon_dec
        )
        altitude_bonus = np.clip((altitude - minimum_altitude) / 50, 0, 1) * 12
        proximity = np.clip(
            (minimum_moon_separation * 2 - separation)
            / max(minimum_moon_separation * 2, 1),
            0,
            1,
        )
        moon_penalty = moon_illumination / 100 * proximity * 35
        score = np.clip(weather_score + altitude_bonus - moon_penalty, 0, 100)
        eligible = altitude >= minimum_altitude
        if not eligible.any():
            rows.append(
                {
                    "target": target.name,
                    "name": target.common_name,
                    "category": target.category,
                    "magnitude": target.magnitude,
                    "best_time": pd.NaT,
                    "altitude": float(np.nanmax(altitude)),
                    "azimuth": float("nan"),
                    "planner_score": 0.0,
                    "weather_score": 0.0,
                    "clouds": float("nan"),
                    "dew_risk": float("nan"),
                    "moon_separation": float("nan"),
                    "moon_illumination": float("nan"),
                    "status": "Troppo basso",
                }
            )
            continue
        indices = np.flatnonzero(eligible)
        best = int(indices[np.nanargmax(score[eligible])])
        best_score = float(score[best])
        best_separation = float(separation[best])
        status = (
            "Luna vicina"
            if best_separation < minimum_moon_separation
            else "Ottima"
            if best_score >= 80
            else "Favorevole"
            if best_score >= 65
            else "Limitata"
        )
        rows.append(
            {
                "target": target.name,
                "name": target.common_name,
                "category": target.category,
                "magnitude": target.magnitude,
                "best_time": frame.iloc[best]["local_time"],
                "altitude": float(altitude[best]),
                "azimuth": float(azimuth[best]),
                "planner_score": best_score,
                "weather_score": float(weather_score[best]),
                "clouds": float(clouds[best]),
                "dew_risk": float(dew_risk[best]),
                "moon_separation": best_separation,
                "moon_illumination": float(moon_illumination[best]),
                "status": status,
            }
        )
    return pd.DataFrame(rows, columns=columns).sort_values(
        ["planner_score", "altitude"], ascending=[False, False]
    )
