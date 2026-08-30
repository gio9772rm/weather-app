"""Personal observing planner for fixed deep-sky targets."""

from __future__ import annotations

import csv
import hashlib
import io
import json
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

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
    angular_width_arcmin: float | None = None
    angular_height_arcmin: float | None = None


@dataclass(frozen=True)
class EquipmentProfile:
    """Optical train used only for field-of-view and sampling estimates."""

    name: str
    telescope: str
    camera: str
    aperture_mm: float
    focal_length_mm: float
    sensor_width_mm: float
    sensor_height_mm: float
    pixel_size_um: float
    focal_multiplier: float = 1.0


@dataclass(frozen=True)
class FieldOfView:
    effective_focal_length_mm: float
    width_deg: float
    height_deg: float
    diagonal_deg: float
    image_scale_arcsec_px: float


TARGETS = (
    SkyTarget(
        "M31", "Galassia di Andromeda", "Galassia", 10.6847, 41.2692, 3.4, 190, 60
    ),
    SkyTarget("M42", "Nebulosa di Orione", "Nebulosa", 83.8221, -5.3911, 4.0, 85, 60),
    SkyTarget("M45", "Pleiadi", "Ammasso", 56.7500, 24.1167, 1.6, 110, 110),
    SkyTarget("M51", "Galassia Vortice", "Galassia", 202.4696, 47.1952, 8.4, 11, 7),
    SkyTarget("M81", "Galassia di Bode", "Galassia", 148.8882, 69.0653, 6.9, 27, 14),
    SkyTarget("M82", "Galassia Sigaro", "Galassia", 148.9683, 69.6797, 8.4, 11, 5),
    SkyTarget("M13", "Ammasso di Ercole", "Ammasso", 250.4235, 36.4613, 5.8, 20, 20),
    SkyTarget("M27", "Nebulosa Manubrio", "Nebulosa", 299.9010, 22.7210, 7.4, 8, 6),
    SkyTarget("M8", "Nebulosa Laguna", "Nebulosa", 270.9250, -24.3800, 6.0, 90, 40),
    SkyTarget("NGC 2237", "Nebulosa Rosetta", "Nebulosa", 97.9800, 4.9800, 9.0, 80, 60),
    SkyTarget(
        "NGC 6960", "Velo occidentale", "Nebulosa", 312.7500, 30.7200, 7.0, 70, 8
    ),
    SkyTarget("NGC 7000", "Nord America", "Nebulosa", 314.0000, 44.5000, 4.0, 120, 100),
)
TARGET_BY_NAME = {target.name: target for target in TARGETS}


def target_labels(custom_targets: Iterable[SkyTarget] = ()) -> list[str]:
    targets = [*TARGETS, *custom_targets]
    return [f"{target.name} · {target.common_name}" for target in targets]


def custom_target(
    name: str,
    ra_hours: float,
    dec_deg: float,
    *,
    magnitude: float | None = None,
    angular_width_arcmin: float | None = None,
    angular_height_arcmin: float | None = None,
) -> SkyTarget:
    """Validate one private RA/Dec target without calling an external service."""
    clean_name = " ".join(str(name or "").split())
    if not clean_name or len(clean_name) > 80 or not clean_name.isprintable():
        raise ValueError("Il nome deve contenere da 1 a 80 caratteri stampabili")
    ra = float(ra_hours)
    declination = float(dec_deg)
    if not 0 <= ra < 24:
        raise ValueError(
            "L'ascensione retta deve essere compresa fra 0 e meno di 24 ore"
        )
    if not -90 <= declination <= 90:
        raise ValueError("La declinazione deve essere compresa fra -90° e +90°")
    numeric_magnitude = np.nan if magnitude is None else float(magnitude)
    if np.isfinite(numeric_magnitude) and not -30 <= numeric_magnitude <= 40:
        raise ValueError("La magnitudine non è plausibile")

    def optional_size(value: float | None) -> float | None:
        if value is None:
            return None
        number = float(value)
        if not 0 < number <= 1_200:
            raise ValueError("La dimensione apparente deve essere fra 0 e 1200 arcmin")
        return number

    width = optional_size(angular_width_arcmin)
    height = optional_size(angular_height_arcmin)
    if (width is None) != (height is None):
        raise ValueError("Inserisci entrambe le dimensioni apparenti oppure nessuna")
    return SkyTarget(
        name=clean_name,
        common_name="Oggetto personalizzato",
        category="Personalizzato",
        ra_deg=ra * 15.0,
        dec_deg=declination,
        magnitude=numeric_magnitude,
        angular_width_arcmin=width,
        angular_height_arcmin=height,
    )


def equipment_profile(
    *,
    name: str,
    telescope: str,
    camera: str,
    aperture_mm: float,
    focal_length_mm: float,
    sensor_width_mm: float,
    sensor_height_mm: float,
    pixel_size_um: float,
    focal_multiplier: float = 1.0,
) -> EquipmentProfile:
    """Build a bounded optical profile suitable for UI and imported JSON data."""
    clean_name = " ".join(str(name or "").split())[:80] or "Profilo"
    values = {
        "apertura": float(aperture_mm),
        "focale": float(focal_length_mm),
        "larghezza sensore": float(sensor_width_mm),
        "altezza sensore": float(sensor_height_mm),
        "pixel": float(pixel_size_um),
        "moltiplicatore": float(focal_multiplier),
    }
    limits = {
        "apertura": (10, 2_000),
        "focale": (20, 20_000),
        "larghezza sensore": (1, 80),
        "altezza sensore": (1, 80),
        "pixel": (0.5, 30),
        "moltiplicatore": (0.1, 10),
    }
    for label, value in values.items():
        lower, upper = limits[label]
        if not np.isfinite(value) or not lower <= value <= upper:
            raise ValueError(f"Valore non valido per {label}")
    return EquipmentProfile(
        name=clean_name,
        telescope=" ".join(str(telescope or "Ottica").split())[:80],
        camera=" ".join(str(camera or "Camera").split())[:80],
        aperture_mm=values["apertura"],
        focal_length_mm=values["focale"],
        sensor_width_mm=values["larghezza sensore"],
        sensor_height_mm=values["altezza sensore"],
        pixel_size_um=values["pixel"],
        focal_multiplier=values["moltiplicatore"],
    )


def field_of_view(profile: EquipmentProfile) -> FieldOfView:
    """Calculate true angular field and sampling for a rectilinear sensor."""
    effective_focal = profile.focal_length_mm * profile.focal_multiplier
    width = np.rad2deg(2 * np.arctan(profile.sensor_width_mm / (2 * effective_focal)))
    height = np.rad2deg(2 * np.arctan(profile.sensor_height_mm / (2 * effective_focal)))
    diagonal_mm = np.hypot(profile.sensor_width_mm, profile.sensor_height_mm)
    diagonal = np.rad2deg(2 * np.arctan(diagonal_mm / (2 * effective_focal)))
    image_scale = 206.265 * profile.pixel_size_um / effective_focal
    return FieldOfView(
        effective_focal_length_mm=float(effective_focal),
        width_deg=float(width),
        height_deg=float(height),
        diagonal_deg=float(diagonal),
        image_scale_arcsec_px=float(image_scale),
    )


def framing_status(target: SkyTarget, view: FieldOfView | None) -> str:
    """Return a conservative framing hint from indicative catalogue dimensions."""
    if view is None:
        return "Profilo non impostato"
    if target.angular_width_arcmin is None or target.angular_height_arcmin is None:
        return "Dimensione oggetto non indicata"
    width = target.angular_width_arcmin / 60
    height = target.angular_height_arcmin / 60
    if width > view.width_deg * 0.9 or height > view.height_deg * 0.9:
        return "Campo stretto · valuta mosaico/riduttore"
    if width < view.width_deg * 0.08 and height < view.height_deg * 0.08:
        return "Oggetto piccolo nel campo"
    return "Inquadratura compatibile"


def horizon_altitudes(
    azimuth: Any, horizon_mask: dict[float, float] | None
) -> np.ndarray:
    """Circularly interpolate a local horizon mask for arbitrary azimuths."""
    values = np.asarray(azimuth, dtype=float)
    if not horizon_mask:
        return np.zeros_like(values, dtype=float)
    points: list[tuple[float, float]] = []
    for raw_azimuth, raw_altitude in horizon_mask.items():
        direction = float(raw_azimuth) % 360
        altitude = float(raw_altitude)
        if not np.isfinite(direction) or not np.isfinite(altitude):
            raise ValueError("La maschera dell'orizzonte contiene valori non validi")
        points.append((direction, float(np.clip(altitude, 0, 90))))
    deduplicated = dict(points)
    ordered = sorted(deduplicated.items())
    if not ordered:
        return np.zeros_like(values, dtype=float)
    directions = np.asarray([item[0] for item in ordered], dtype=float)
    altitudes = np.asarray([item[1] for item in ordered], dtype=float)
    if len(directions) == 1:
        return np.full_like(values, altitudes[0], dtype=float)
    extended_directions = np.concatenate(
        ([directions[-1] - 360], directions, [directions[0] + 360])
    )
    extended_altitudes = np.concatenate(([altitudes[-1]], altitudes, [altitudes[0]]))
    return np.interp(values % 360, extended_directions, extended_altitudes)


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


def _target_names(values: Iterable[str], targets: dict[str, SkyTarget]) -> list[str]:
    selected: list[str] = []
    for value in values:
        name = str(value).split(" · ", 1)[0].strip()
        if name in targets and name not in selected:
            selected.append(name)
    return selected


def plan_targets(
    astronomy: pd.DataFrame,
    cfg: Settings,
    selected_targets: Iterable[str],
    *,
    minimum_altitude: float = 25,
    minimum_moon_separation: float = 30,
    custom_targets: Iterable[SkyTarget] = (),
    horizon_mask: dict[float, float] | None = None,
    equipment: EquipmentProfile | None = None,
) -> pd.DataFrame:
    """Rank the best forecast hour for each selected deep-sky object."""
    targets = {**TARGET_BY_NAME, **{target.name: target for target in custom_targets}}
    names = _target_names(selected_targets, targets)
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
        "horizon_altitude",
        "horizon_clearance",
        "field_width_deg",
        "field_height_deg",
        "image_scale_arcsec_px",
        "framing",
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
    view = field_of_view(equipment) if equipment is not None else None
    rows: list[dict[str, object]] = []
    for name in names:
        target = targets[name]
        altitude, azimuth = equatorial_altaz(
            target.ra_deg,
            target.dec_deg,
            frame["valid_time"],
            latitude=cfg.latitude,
            longitude=cfg.longitude,
        )
        local_horizon = horizon_altitudes(azimuth, horizon_mask)
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
        required_altitude = np.maximum(float(minimum_altitude), local_horizon)
        eligible = altitude >= required_altitude
        if not eligible.any():
            clear_without_mask = altitude >= float(minimum_altitude)
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
                    "horizon_altitude": float(np.nanmax(local_horizon)),
                    "horizon_clearance": float(np.nanmax(altitude - local_horizon)),
                    "field_width_deg": np.nan if view is None else view.width_deg,
                    "field_height_deg": np.nan if view is None else view.height_deg,
                    "image_scale_arcsec_px": (
                        np.nan if view is None else view.image_scale_arcsec_px
                    ),
                    "framing": framing_status(target, view),
                    "status": (
                        "Dietro ostacolo locale"
                        if clear_without_mask.any()
                        else "Troppo basso"
                    ),
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
                "horizon_altitude": float(local_horizon[best]),
                "horizon_clearance": float(altitude[best] - local_horizon[best]),
                "field_width_deg": np.nan if view is None else view.width_deg,
                "field_height_deg": np.nan if view is None else view.height_deg,
                "image_scale_arcsec_px": (
                    np.nan if view is None else view.image_scale_arcsec_px
                ),
                "framing": framing_status(target, view),
                "status": status,
            }
        )
    return pd.DataFrame(rows, columns=columns).sort_values(
        ["planner_score", "altitude"], ascending=[False, False]
    )


def _ical_escape(value: Any) -> str:
    return (
        str(value or "")
        .replace("\\", "\\\\")
        .replace("\r", "")
        .replace("\n", "\\n")
        .replace(";", "\\;")
        .replace(",", "\\,")
    )


def _fold_ical_line(line: str, limit: int = 73) -> list[str]:
    """Fold iCalendar content lines without emitting invalid bare newlines."""
    if len(line) <= limit:
        return [line]
    pieces = [line[:limit]]
    remaining = line[limit:]
    while remaining:
        pieces.append(" " + remaining[: limit - 1])
        remaining = remaining[limit - 1 :]
    return pieces


def observing_calendar_ics(
    target: str,
    start: Any,
    *,
    duration_minutes: int = 120,
    timezone_name: str = "Europe/Rome",
    description: str = "",
    created_at: Any | None = None,
) -> bytes:
    """Build a portable UTC iCalendar event that opens at the right local hour."""
    moment = pd.Timestamp(start)
    if pd.isna(moment):
        raise ValueError("La sessione non ha un orario valido")
    if moment.tzinfo is None:
        moment = moment.tz_localize(
            timezone_name, ambiguous="raise", nonexistent="raise"
        )
    else:
        moment = moment.tz_convert(timezone_name)
    duration = int(duration_minutes)
    if not 15 <= duration <= 12 * 60:
        raise ValueError("La durata deve essere compresa fra 15 minuti e 12 ore")
    start_utc = moment.tz_convert("UTC")
    end_utc = (moment + pd.Timedelta(minutes=duration)).tz_convert("UTC")
    created = pd.Timestamp(created_at or datetime.now(timezone.utc))
    created = (
        created.tz_localize("UTC")
        if created.tzinfo is None
        else created.tz_convert("UTC")
    )
    clean_target = " ".join(str(target or "Oggetto astronomico").split())[:100]
    uid_source = f"{clean_target}|{start_utc.isoformat()}|{duration}"
    uid = hashlib.sha256(uid_source.encode("utf-8")).hexdigest()[:24]
    local_label = moment.strftime("%d/%m/%Y %H:%M %Z")
    detail = (
        f"Finestra suggerita dal pianificatore Meteo V4. Ora locale: {local_label}."
        + (f" {description.strip()}" if description.strip() else "")
    )
    lines = [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//Meteo V4//Pianificatore Astronomico//IT",
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
        f"X-WR-TIMEZONE:{_ical_escape(timezone_name)}",
        "BEGIN:VEVENT",
        f"UID:{uid}@weather-app.local",
        f"DTSTAMP:{created:%Y%m%dT%H%M%SZ}",
        f"DTSTART:{start_utc:%Y%m%dT%H%M%SZ}",
        f"DTEND:{end_utc:%Y%m%dT%H%M%SZ}",
        f"SUMMARY:{_ical_escape('Osservazione · ' + clean_target)}",
        f"DESCRIPTION:{_ical_escape(detail)}",
        "TRANSP:TRANSPARENT",
        "END:VEVENT",
        "END:VCALENDAR",
    ]
    folded = [piece for line in lines for piece in _fold_ical_line(line)]
    return ("\r\n".join(folded) + "\r\n").encode("utf-8")


OBSERVING_LOG_COLUMNS = (
    "target",
    "planned_start",
    "duration_minutes",
    "status",
    "score",
    "equipment",
    "notes",
)


def observing_log_csv(entries: Iterable[dict[str, Any]]) -> bytes:
    """Export the browser-session diary while neutralising spreadsheet formulas."""

    def safe_cell(value: Any) -> Any:
        if value is None:
            return ""
        rendered = str(value).replace("\r", " ").replace("\n", " ")
        if rendered.lstrip().startswith(("=", "+", "-", "@")):
            return "'" + rendered
        return rendered

    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=OBSERVING_LOG_COLUMNS)
    writer.writeheader()
    for entry in entries:
        writer.writerow(
            {
                column: safe_cell(entry.get(column, ""))
                for column in OBSERVING_LOG_COLUMNS
            }
        )
    return output.getvalue().encode("utf-8-sig")


def planner_configuration_json(
    profiles: Iterable[EquipmentProfile],
    custom_targets: Iterable[SkyTarget],
    horizon_mask: dict[float, float],
) -> bytes:
    """Export reusable planner settings without terrestrial coordinates or notes."""
    exported_targets: list[dict[str, Any]] = []
    for target in custom_targets:
        details = asdict(target)
        if not np.isfinite(float(details.get("magnitude", np.nan))):
            details["magnitude"] = None
        exported_targets.append(details)
    payload = {
        "version": 1,
        "equipment_profiles": {profile.name: asdict(profile) for profile in profiles},
        "custom_targets": exported_targets,
        "horizon_mask": {
            str(float(direction) % 360): float(np.clip(altitude, 0, 90))
            for direction, altitude in horizon_mask.items()
        },
    }
    return json.dumps(
        payload,
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
        allow_nan=False,
    ).encode("utf-8")


def parse_planner_configuration(
    payload: bytes | str,
) -> tuple[dict[str, EquipmentProfile], dict[str, SkyTarget], dict[float, float]]:
    """Validate a small exported planner bundle before restoring browser state."""
    raw = payload.encode("utf-8") if isinstance(payload, str) else bytes(payload)
    if len(raw) > 128 * 1024:
        raise ValueError("La configurazione supera 128 KiB")
    try:
        document = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("Il file JSON non è valido") from exc
    if not isinstance(document, dict) or document.get("version") != 1:
        raise ValueError("Versione configurazione planner non supportata")

    source_profiles = document.get("equipment_profiles") or {}
    source_targets = document.get("custom_targets") or []
    source_horizon = document.get("horizon_mask") or {}
    if not isinstance(source_profiles, dict) or len(source_profiles) > 20:
        raise ValueError("Elenco profili non valido")
    if not isinstance(source_targets, list) or len(source_targets) > 50:
        raise ValueError("Elenco target non valido")
    if not isinstance(source_horizon, dict) or len(source_horizon) > 72:
        raise ValueError("Maschera dell'orizzonte non valida")

    profiles: dict[str, EquipmentProfile] = {}
    for details in source_profiles.values():
        if not isinstance(details, dict):
            raise TypeError("Profilo attrezzatura non valido")
        profile = equipment_profile(
            name=details.get("name", ""),
            telescope=details.get("telescope", ""),
            camera=details.get("camera", ""),
            aperture_mm=details.get("aperture_mm"),
            focal_length_mm=details.get("focal_length_mm"),
            sensor_width_mm=details.get("sensor_width_mm"),
            sensor_height_mm=details.get("sensor_height_mm"),
            pixel_size_um=details.get("pixel_size_um"),
            focal_multiplier=details.get("focal_multiplier", 1.0),
        )
        profiles[profile.name] = profile

    targets: dict[str, SkyTarget] = {}
    for details in source_targets:
        if not isinstance(details, dict):
            raise TypeError("Target personale non valido")
        ra_deg = float(details.get("ra_deg"))
        magnitude = details.get("magnitude")
        if magnitude is not None and not np.isfinite(float(magnitude)):
            magnitude = None
        target = custom_target(
            str(details.get("name") or ""),
            ra_deg / 15,
            float(details.get("dec_deg")),
            magnitude=magnitude,
            angular_width_arcmin=details.get("angular_width_arcmin"),
            angular_height_arcmin=details.get("angular_height_arcmin"),
        )
        targets[target.name] = target

    horizon: dict[float, float] = {}
    for direction, altitude in source_horizon.items():
        numeric_direction = float(direction) % 360
        numeric_altitude = float(altitude)
        if not np.isfinite(numeric_direction) or not np.isfinite(numeric_altitude):
            raise ValueError("Maschera dell'orizzonte non valida")
        horizon[numeric_direction] = float(np.clip(numeric_altitude, 0, 90))
    horizon_altitudes(np.asarray(list(horizon) or [0]), horizon)
    return profiles, targets, horizon
