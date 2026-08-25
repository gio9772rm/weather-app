"""Pure helpers for the user-facing Meteo V4 daily experience."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class WeatherInsight:
    """A short, evidence-based message for the daily dashboard."""

    icon: str
    title: str
    value: str
    detail: str
    tone: str = "neutral"


@dataclass(frozen=True)
class ActivityOutlook:
    """An indicative score for one common outdoor activity."""

    icon: str
    activity: str
    score: int
    label: str
    best_time: str
    detail: str
    tone: str


@dataclass(frozen=True)
class DailyBriefing:
    """Compact forecast summary used by the V4 hero."""

    description: str
    headline: str
    detail: str
    rain_probability: float | None
    confidence: float | None


def _as_number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if np.isfinite(number) else None


def _series(frame: pd.DataFrame, column: str, default: float = np.nan) -> pd.Series:
    if column not in frame:
        return pd.Series(default, index=frame.index, dtype=float)
    return pd.to_numeric(frame[column], errors="coerce")


def future_forecast(
    forecast: pd.DataFrame,
    *,
    now: pd.Timestamp | None = None,
    hours: int = 24,
) -> pd.DataFrame:
    """Return a sorted, bounded future forecast using timezone-aware UTC."""
    if forecast.empty or "valid_time" not in forecast:
        return forecast.iloc[0:0].copy()
    reference = pd.Timestamp(now) if now is not None else pd.Timestamp.now(tz="UTC")
    reference = (
        reference.tz_localize("UTC")
        if reference.tzinfo is None
        else reference.tz_convert("UTC")
    )
    valid_time = pd.to_datetime(forecast["valid_time"], utc=True, errors="coerce")
    end = reference + pd.Timedelta(hours=max(1, int(hours)))
    result = forecast.loc[(valid_time >= reference.floor("h")) & (valid_time <= end)].copy()
    result["valid_time"] = pd.to_datetime(result["valid_time"], utc=True, errors="coerce")
    return result.dropna(subset=["valid_time"]).sort_values("valid_time")


def nearest_forecast(
    forecast: pd.DataFrame, now: pd.Timestamp | None = None
) -> pd.Series | None:
    """Return the first current/future forecast point, if available."""
    upcoming = future_forecast(forecast, now=now, hours=6)
    return None if upcoming.empty else upcoming.iloc[0]


def build_daily_briefing(
    forecast: pd.DataFrame,
    *,
    now: pd.Timestamp | None = None,
) -> DailyBriefing:
    """Build a readable summary from the next 24 forecast hours."""
    upcoming = future_forecast(forecast, now=now, hours=24)
    if upcoming.empty:
        return DailyBriefing(
            description="Dati in aggiornamento",
            headline="La previsione sarà disponibile al prossimo aggiornamento.",
            detail="La stazione continua comunque ad archiviare le misure reali.",
            rain_probability=None,
            confidence=None,
        )

    first = upcoming.iloc[0]
    description = str(first.get("description") or "Variabile")
    temperatures = _series(upcoming, "temp_c")
    rain = _series(upcoming, "rain_mm", 0).clip(lower=0)
    probability = _series(upcoming, "precip_probability", 0).clip(0, 100)
    gust = _series(upcoming, "wind_gust_kmh", 0).clip(lower=0)
    confidence_values = _series(upcoming, "confidence")

    minimum = temperatures.min() if temperatures.notna().any() else np.nan
    maximum = temperatures.max() if temperatures.notna().any() else np.nan
    rain_total = rain.sum(min_count=1)
    rain_max = probability.max() if probability.notna().any() else np.nan
    gust_max = gust.max() if gust.notna().any() else np.nan
    confidence = (
        confidence_values.mean() if confidence_values.notna().any() else np.nan
    )

    if pd.notna(rain_max) and rain_max >= 70:
        headline = "Pioggia probabile: conviene pianificare gli spostamenti."
    elif pd.notna(rain_max) and rain_max >= 40:
        headline = "Possibili precipitazioni, soprattutto nelle ore indicate."
    elif pd.notna(gust_max) and gust_max >= 50:
        headline = "Giornata ventosa, con raffiche localmente forti."
    elif pd.notna(maximum) and maximum >= 34:
        headline = "Caldo marcato nelle ore centrali della giornata."
    elif pd.notna(minimum) and minimum <= 3:
        headline = "Temperature basse: attenzione nelle ore più fredde."
    else:
        headline = "Condizioni nel complesso regolari nelle prossime 24 ore."

    details: list[str] = []
    if pd.notna(minimum) and pd.notna(maximum):
        details.append(f"temperature tra {minimum:.0f}° e {maximum:.0f}°")
    if pd.notna(rain_total) and pd.notna(rain_max):
        details.append(f"{rain_total:.1f} mm previsti, rischio massimo {rain_max:.0f}%")
    if pd.notna(gust_max):
        details.append(f"raffiche fino a {gust_max:.0f} km/h")
    detail = " · ".join(details) or "Dettagli in elaborazione"

    return DailyBriefing(
        description=description,
        headline=headline,
        detail=detail,
        rain_probability=None if pd.isna(rain_max) else float(rain_max),
        confidence=None if pd.isna(confidence) else float(confidence),
    )


def weather_insights(
    forecast: pd.DataFrame,
    *,
    timezone: str,
    now: pd.Timestamp | None = None,
) -> list[WeatherInsight]:
    """Return four concise insights for the next 24 hours."""
    upcoming = future_forecast(forecast, now=now, hours=24)
    if upcoming.empty:
        return []
    local_time = upcoming["valid_time"].dt.tz_convert(timezone)
    rain = _series(upcoming, "rain_mm", 0).clip(lower=0)
    probability = _series(upcoming, "precip_probability", 0).clip(0, 100)
    gust = _series(upcoming, "wind_gust_kmh", 0).clip(lower=0)
    temperatures = _series(upcoming, "temp_c")
    confidence = _series(upcoming, "confidence")

    wet = (rain > 0.05) | (probability >= 40)
    if wet.any():
        first_position = int(np.flatnonzero(wet.to_numpy())[0])
        rain_time = local_time.iloc[first_position].strftime("%H:%M")
        rain_value = f"dalle {rain_time}"
        rain_detail = (
            f"rischio fino al {probability.max():.0f}% · {rain.sum():.1f} mm complessivi"
        )
        rain_tone = "warning" if probability.max() < 75 else "danger"
    else:
        rain_value = "Nessuna fase rilevante"
        rain_detail = "rischio sempre inferiore al 40% nelle prossime 24 ore"
        rain_tone = "good"

    if gust.notna().any():
        gust_position = int(gust.fillna(-np.inf).to_numpy().argmax())
        gust_time = local_time.iloc[gust_position].strftime("%H:%M")
        gust_value = f"{gust.iloc[gust_position]:.0f} km/h"
        gust_detail = f"massimo atteso verso le {gust_time}"
        gust_tone = "danger" if gust.max() >= 65 else "warning" if gust.max() >= 45 else "good"
    else:
        gust_value, gust_detail, gust_tone = "—", "dato non disponibile", "neutral"

    if temperatures.notna().any():
        low, high = temperatures.min(), temperatures.max()
        temperature_value = f"{low:.0f}° / {high:.0f}°"
        temperature_detail = f"escursione prevista {high - low:.0f}°"
        temperature_tone = "warning" if low <= 3 or high >= 34 else "good"
    else:
        temperature_value, temperature_detail, temperature_tone = "—", "dato non disponibile", "neutral"

    confidence_value = confidence.mean() if confidence.notna().any() else np.nan
    if pd.isna(confidence_value):
        confidence_text, confidence_detail, confidence_tone = "—", "calibrazione in corso", "neutral"
    else:
        confidence_text = f"{confidence_value:.0f}%"
        confidence_detail = (
            "modelli concordi"
            if confidence_value >= 70
            else "concordanza discreta"
            if confidence_value >= 50
            else "scenario più incerto del normale"
        )
        confidence_tone = "good" if confidence_value >= 70 else "warning" if confidence_value >= 50 else "danger"

    return [
        WeatherInsight("☔", "Prossima pioggia", rain_value, rain_detail, rain_tone),
        WeatherInsight("💨", "Raffica massima", gust_value, gust_detail, gust_tone),
        WeatherInsight("🌡️", "Minima / massima", temperature_value, temperature_detail, temperature_tone),
        WeatherInsight("◎", "Fiducia", confidence_text, confidence_detail, confidence_tone),
    ]


def _activity_label(score: float) -> tuple[str, str]:
    if score >= 78:
        return "Ottimo", "good"
    if score >= 58:
        return "Buono", "good"
    if score >= 38:
        return "Discreto", "warning"
    return "Sconsigliato", "danger"


def _best_activity(
    frame: pd.DataFrame,
    score: pd.Series,
    *,
    timezone: str,
    icon: str,
    activity: str,
    detail: str,
) -> ActivityOutlook:
    usable = score.replace([np.inf, -np.inf], np.nan).dropna()
    if usable.empty:
        return ActivityOutlook(icon, activity, 0, "Non disponibile", "—", detail, "neutral")
    position = usable.idxmax()
    raw_score = float(np.clip(usable.loc[position], 0, 100))
    moment = pd.Timestamp(frame.loc[position, "valid_time"]).tz_convert(timezone)
    label, tone = _activity_label(raw_score)
    return ActivityOutlook(
        icon=icon,
        activity=activity,
        score=round(raw_score),
        label=label,
        best_time=moment.strftime("%a %H:%M").replace("Mon", "Lun").replace("Tue", "Mar").replace("Wed", "Mer").replace("Thu", "Gio").replace("Fri", "Ven").replace("Sat", "Sab").replace("Sun", "Dom"),
        detail=detail,
        tone=tone,
    )


def activity_outlooks(
    forecast: pd.DataFrame,
    *,
    timezone: str,
    now: pd.Timestamp | None = None,
) -> list[ActivityOutlook]:
    """Score common activities using only transparent weather thresholds."""
    upcoming = future_forecast(forecast, now=now, hours=36).reset_index(drop=True)
    if upcoming.empty:
        return []

    rain = _series(upcoming, "rain_mm", 0).clip(lower=0)
    probability = _series(upcoming, "precip_probability", 0).clip(0, 100)
    gust = _series(upcoming, "wind_gust_kmh", 0).clip(lower=0)
    wind = _series(upcoming, "wind_kmh", 0).clip(lower=0)
    humidity = _series(upcoming, "humidity", 60).clip(0, 100)
    clouds = _series(upcoming, "clouds", 50).clip(0, 100)
    temp = _series(upcoming, "temp_c", 20)
    local_hours = upcoming["valid_time"].dt.tz_convert(timezone).dt.hour
    inferred_daylight = local_hours.between(7, 19).astype(float)
    is_day = _series(upcoming, "is_day").where(
        _series(upcoming, "is_day").notna(), inferred_daylight
    ).clip(0, 1)

    walk = 100 - probability * 0.48 - rain * 18 - np.maximum(gust - 28, 0) * 1.2 - (temp - 21).abs() * 1.5
    cycling = 100 - probability * 0.55 - rain * 22 - np.maximum(wind - 18, 0) * 1.8 - np.maximum(gust - 30, 0) * 1.5 - (temp - 20).abs()
    laundry = 92 - probability * 0.75 - rain * 30 - np.maximum(humidity - 65, 0) * 0.8 - clouds * 0.18 - np.maximum(wind - 35, 0) * 1.3
    laundry = laundry.where(is_day >= 0.5, laundry - 35)
    astronomy = 100 - clouds * 0.72 - probability * 0.45 - rain * 25 - np.maximum(wind - 18, 0) * 1.2
    astronomy = astronomy.where(is_day < 0.5, astronomy - 55)

    return [
        _best_activity(upcoming, walk, timezone=timezone, icon="🚶", activity="Passeggiata", detail="considera pioggia, raffiche e comfort termico"),
        _best_activity(upcoming, cycling, timezone=timezone, icon="🚲", activity="Bicicletta", detail="penalizza vento, raffiche e fondo bagnato"),
        _best_activity(upcoming, laundry, timezone=timezone, icon="👕", activity="Bucato", detail="considera pioggia, umidità, nuvole e vento"),
        _best_activity(upcoming, astronomy, timezone=timezone, icon="🔭", activity="Astronomia", detail="considera notte, nuvole, pioggia e vento"),
    ]


def aqi_category(value: Any) -> tuple[str, str]:
    """Return the official European AQI band label and semantic tone."""
    number = _as_number(value)
    if number is None:
        return "Non disponibile", "neutral"
    if number <= 20:
        return "Buona", "good"
    if number <= 40:
        return "Discreta", "good"
    if number <= 60:
        return "Moderata", "warning"
    if number <= 80:
        return "Scarsa", "warning"
    if number <= 100:
        return "Molto scarsa", "danger"
    return "Estremamente scarsa", "danger"


def pollen_category(value: Any) -> tuple[str, str]:
    """Return a simple grains/m³ band used only as a visual orientation."""
    number = _as_number(value)
    if number is None:
        return "Non disponibile", "neutral"
    if number < 10:
        return "Basso", "good"
    if number < 50:
        return "Medio", "warning"
    if number < 100:
        return "Alto", "warning"
    return "Molto alto", "danger"
