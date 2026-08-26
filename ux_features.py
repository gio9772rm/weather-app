"""Pure V4.1 helpers for planning, provenance and city comparison UX."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from weather_experience import future_forecast


@dataclass(frozen=True)
class VentilationWindow:
    """Best weather/air window for opening the house, with transparent scoring."""

    available: bool
    score: int | None
    label: str
    timing: str
    detail: str
    tone: str


def _numbers(frame: pd.DataFrame, column: str, default: float = np.nan) -> pd.Series:
    if column not in frame:
        return pd.Series(default, index=frame.index, dtype=float)
    return pd.to_numeric(frame[column], errors="coerce")


def _air_on_forecast_hours(
    forecast: pd.DataFrame, air_hourly: pd.DataFrame | None
) -> pd.DataFrame:
    """Attach the nearest air hour without requiring the two feeds to share a TZ."""
    result = forecast.copy().sort_values("valid_time")
    if air_hourly is None or air_hourly.empty or "time" not in air_hourly:
        return result
    air = air_hourly.copy()
    air["valid_time"] = pd.to_datetime(air["time"], utc=True, errors="coerce")
    air = air.dropna(subset=["valid_time"]).sort_values("valid_time")
    if air.empty:
        return result
    wanted = [
        column
        for column in (
            "valid_time",
            "european_aqi",
            "pm2_5",
            "alder_pollen",
            "birch_pollen",
            "grass_pollen",
            "mugwort_pollen",
            "olive_pollen",
            "ragweed_pollen",
        )
        if column in air
    ]
    return pd.merge_asof(
        result,
        air[wanted],
        on="valid_time",
        direction="nearest",
        tolerance=pd.Timedelta(minutes=70),
    )


def best_ventilation_window(
    forecast: pd.DataFrame,
    air_hourly: pd.DataFrame | None,
    *,
    timezone: str,
    now: pd.Timestamp | None = None,
) -> VentilationWindow:
    """Rank the next 30 hours for ventilation using weather, AQI and pollen.

    The result is intentionally advisory. Missing environmental fields reduce
    certainty but do not invent a measurement or make the weather pipeline fail.
    """
    upcoming = future_forecast(forecast, now=now, hours=30).reset_index(drop=True)
    if upcoming.empty:
        return VentilationWindow(
            False,
            None,
            "In aggiornamento",
            "—",
            "Servono le prossime ore di previsione.",
            "neutral",
        )
    frame = _air_on_forecast_hours(upcoming, air_hourly)
    rain = _numbers(frame, "rain_mm", 0).fillna(0).clip(lower=0)
    probability = _numbers(frame, "precip_probability", 0).fillna(0).clip(0, 100)
    wind = _numbers(frame, "wind_kmh", 8).fillna(8).clip(lower=0)
    gust = _numbers(frame, "wind_gust_kmh", wind).fillna(wind).clip(lower=0)
    humidity = _numbers(frame, "humidity", 60).fillna(60).clip(0, 100)
    aqi = _numbers(frame, "european_aqi", 35).fillna(35).clip(lower=0)
    pm25 = _numbers(frame, "pm2_5", 12).fillna(12).clip(lower=0)
    pollen_columns = [column for column in frame if column.endswith("_pollen")]
    pollen = (
        (
            frame[pollen_columns].apply(pd.to_numeric, errors="coerce").max(axis=1)
            if pollen_columns
            else pd.Series(15.0, index=frame.index)
        )
        .fillna(15)
        .clip(lower=0)
    )

    score = (
        100
        - probability * 0.42
        - rain * 24
        - np.maximum(gust - 28, 0) * 1.25
        - np.maximum(3 - wind, 0) * 5
        - np.maximum(wind - 24, 0) * 2
        - np.maximum(humidity - 78, 0) * 0.7
        - np.maximum(aqi - 20, 0) * 0.55
        - np.maximum(pm25 - 8, 0) * 1.1
        - np.maximum(pollen - 10, 0) * 0.22
    ).clip(0, 100)
    # Avoid recommending an hour with rain or strong wind even if another feed
    # is missing and therefore cannot contribute its usual penalty.
    usable = (probability <= 45) & (rain <= 0.2) & (gust < 50) & (wind < 35)
    candidates = score.where(usable).dropna()
    if candidates.empty:
        return VentilationWindow(
            True,
            round(float(score.max())),
            "Meglio attendere",
            "Nessuna finestra ideale nelle prossime 30 ore",
            "Pioggia, vento o qualità ambientale rendono poco favorevole il ricambio.",
            "warning",
        )
    position = int(candidates.idxmax())
    moment = pd.Timestamp(frame.loc[position, "valid_time"]).tz_convert(timezone)
    value = round(float(candidates.loc[position]))
    if value >= 75:
        label, tone = "Finestra favorevole", "good"
    elif value >= 52:
        label, tone = "Finestra discreta", "warning"
    else:
        label, tone = "Solo se necessario", "warning"
    end = moment + pd.Timedelta(hours=1)
    pollen_note = (
        "pollini n/d"
        if not pollen_columns
        else f"pollini max {pollen.loc[position]:.0f}"
    )
    detail = (
        f"AQI {aqi.loc[position]:.0f} · PM2.5 {pm25.loc[position]:.1f} µg/m³ · "
        f"{pollen_note} · vento {wind.loc[position]:.0f} km/h"
    )
    return VentilationWindow(
        True,
        value,
        label,
        f"{moment:%a %H:%M}–{end:%H:%M}",
        detail,
        tone,
    )


def daily_city_comparison(
    local_daily: pd.DataFrame,
    city_daily: pd.DataFrame,
    *,
    city_label: str,
) -> pd.DataFrame:
    """Return a compact, date-aligned Roma versus selected-city comparison."""
    if local_daily.empty or city_daily.empty:
        return pd.DataFrame()
    local = local_daily.copy()
    city = city_daily.copy()
    local["date"] = pd.to_datetime(local.get("date"), errors="coerce").dt.date
    city["date"] = pd.to_datetime(city.get("time"), errors="coerce").dt.date
    merged = local.merge(city, on="date", how="inner", suffixes=("_local", "_city"))
    if merged.empty:
        return pd.DataFrame()
    result = pd.DataFrame(
        {
            "Data": pd.to_datetime(merged["date"]).dt.strftime("%d/%m"),
            "Roma min/max °C": merged.apply(
                lambda row: (
                    f"{row.get('temp_min', np.nan):.0f} / {row.get('temp_max', np.nan):.0f}"
                ),
                axis=1,
            ),
            f"{city_label} min/max °C": merged.apply(
                lambda row: (
                    f"{row.get('temp_min_c', np.nan):.0f} / {row.get('temp_max_c', np.nan):.0f}"
                ),
                axis=1,
            ),
            "Roma pioggia mm": pd.to_numeric(
                merged.get("rain_mm"), errors="coerce"
            ).clip(lower=0),
            f"{city_label} pioggia mm": pd.to_numeric(
                merged.get("precipitation_mm"), errors="coerce"
            ).clip(lower=0),
            "Roma vento max km/h": pd.to_numeric(
                merged.get("wind_max"), errors="coerce"
            ),
            f"{city_label} vento max km/h": pd.to_numeric(
                merged.get("wind_max_kmh"), errors="coerce"
            ),
        }
    )
    return result.head(7)
