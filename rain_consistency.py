"""Keep user-facing precipitation amounts coherent with their probability."""

from __future__ import annotations

from typing import Any

import pandas as pd

MIN_REPORTABLE_PROBABILITY = 20.0
MIN_REPORTABLE_AMOUNT_MM = 0.05


def reportable_rain_series(
    frame: pd.DataFrame,
    *,
    amount_column: str = "rain_mm",
    probability_column: str = "precip_probability",
) -> pd.Series:
    """Return amounts suitable for totals and summaries.

    A deterministic trace paired with a very low probability is useful in the
    expert feed, but summing it hour after hour produces misleading daily
    totals.  Keep amounts when probability is unavailable or reaches 20%; hide
    only sub-threshold traces from the simplified experience.
    """
    if amount_column not in frame:
        return pd.Series(0.0, index=frame.index, dtype=float)
    rain = pd.to_numeric(frame[amount_column], errors="coerce").clip(lower=0)
    rain = rain.where(rain >= MIN_REPORTABLE_AMOUNT_MM, 0.0)
    if probability_column not in frame:
        return rain
    probability = pd.to_numeric(frame[probability_column], errors="coerce")
    reportable = probability.isna() | (probability >= MIN_REPORTABLE_PROBABILITY)
    return rain.where(reportable, 0.0)


def reportable_rain_amount(amount: Any, probability: Any) -> float:
    """Scalar counterpart used by hourly cards."""
    frame = pd.DataFrame(
        {"rain_mm": [amount], "precip_probability": [probability]}
    )
    value = reportable_rain_series(frame).iloc[0]
    return 0.0 if pd.isna(value) else float(value)
