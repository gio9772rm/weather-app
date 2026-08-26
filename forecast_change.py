"""Explain how the latest forecast emission differs from the previous one."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class ForecastChangeSummary:
    available: bool
    status: str
    score: float | None
    headline: str
    detail: str
    latest_issued_at: pd.Timestamp | None = None
    previous_issued_at: pd.Timestamp | None = None
    temperature_change_c: float | None = None
    rain_change_mm: float | None = None
    rain_probability_change: float | None = None
    gust_change_kmh: float | None = None
    confidence_change: float | None = None


def _number(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame:
        return pd.Series(np.nan, index=frame.index, dtype=float)
    return pd.to_numeric(frame[column], errors="coerce")


def _safe(value: Any) -> float | None:
    number = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return None if pd.isna(number) else float(number)


def summarize_forecast_change(
    history: pd.DataFrame,
    *,
    now: pd.Timestamp | None = None,
    hours: int = 24,
) -> ForecastChangeSummary:
    """Compare matching future hours from the two newest blend emissions."""
    if history.empty or "issued_at" not in history:
        return ForecastChangeSummary(
            False,
            "collecting",
            None,
            "Confronto in preparazione",
            "Servono due aggiornamenti della previsione per mostrare cosa è cambiato.",
        )
    frame = history.copy()
    frame["issued_at"] = pd.to_datetime(frame["issued_at"], utc=True, errors="coerce")
    frame["valid_time"] = pd.to_datetime(frame["valid_time"], utc=True, errors="coerce")
    emissions = sorted(frame["issued_at"].dropna().unique(), reverse=True)
    if len(emissions) < 2:
        return ForecastChangeSummary(
            False,
            "collecting",
            None,
            "Confronto in preparazione",
            "Il primo aggiornamento è archiviato; il confronto apparirà dal prossimo.",
            latest_issued_at=pd.Timestamp(emissions[0]) if emissions else None,
        )
    latest_time, previous_time = map(pd.Timestamp, emissions[:2])
    reference = now or pd.Timestamp.now(tz="UTC")
    reference = (
        reference.tz_localize("UTC") if reference.tzinfo is None else reference.tz_convert("UTC")
    )
    end = reference + pd.Timedelta(hours=max(6, min(int(hours), 72)))
    latest = frame[
        (frame["issued_at"] == latest_time)
        & (frame["valid_time"] >= reference.floor("h"))
        & (frame["valid_time"] <= end)
    ]
    previous = frame[
        (frame["issued_at"] == previous_time)
        & (frame["valid_time"] >= reference.floor("h"))
        & (frame["valid_time"] <= end)
    ]
    merged = latest.merge(previous, on="valid_time", suffixes=("_new", "_old"))
    if merged.empty:
        return ForecastChangeSummary(
            False,
            "collecting",
            None,
            "Confronto non ancora allineato",
            "Le due emissioni non hanno ore future comuni sufficienti.",
            latest_time,
            previous_time,
        )

    temp_delta = (_number(merged, "temp_c_new") - _number(merged, "temp_c_old")).abs().mean()
    rain_delta = _number(merged, "rain_mm_new").fillna(0).sum() - _number(
        merged, "rain_mm_old"
    ).fillna(0).sum()
    pop_delta = _number(merged, "precip_probability_new").max() - _number(
        merged, "precip_probability_old"
    ).max()
    gust_delta = _number(merged, "wind_gust_kmh_new").max() - _number(
        merged, "wind_gust_kmh_old"
    ).max()
    confidence_delta = _number(merged, "confidence_new").mean() - _number(
        merged, "confidence_old"
    ).mean()

    components = [
        min(1.0, float(temp_delta or 0) / 2.5) * 0.35,
        min(1.0, abs(float(rain_delta or 0)) / 12.0) * 0.25,
        min(1.0, abs(float(pop_delta or 0)) / 50.0) * 0.20,
        min(1.0, abs(float(gust_delta or 0)) / 25.0) * 0.20,
    ]
    score = float(np.clip(100.0 * (1.0 - sum(components)), 0.0, 100.0))
    if score >= 82:
        status, headline = "stable", "Previsione stabile"
    elif score >= 58:
        status, headline = "evolving", "Previsione in evoluzione"
    else:
        status, headline = "changed", "Previsione cambiata"
    direction = "più" if float(rain_delta or 0) > 0 else "meno"
    detail = (
        f"Scarto termico medio {float(temp_delta or 0):.1f} °C; "
        f"{abs(float(rain_delta or 0)):.1f} mm di pioggia {direction}; "
        f"raffiche {float(gust_delta or 0):+.0f} km/h rispetto all’emissione precedente."
    )
    return ForecastChangeSummary(
        True,
        status,
        score,
        headline,
        detail,
        latest_time,
        previous_time,
        _safe(temp_delta),
        _safe(rain_delta),
        _safe(pop_delta),
        _safe(gust_delta),
        _safe(confidence_delta),
    )
