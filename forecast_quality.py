"""Physical bounds shared by forecast generation and dashboard reads."""

from __future__ import annotations

import pandas as pd

NON_NEGATIVE_COLUMNS = (
    "rain_mm",
    "snow_mm",
    "wind_kmh",
    "wind_gust_kmh",
    "visibility_m",
    "temp_uncertainty_c",
    "cape_j_kg",
    "freezing_level_m",
    "wind_300hpa_kmh",
    "geopotential_500hpa_m",
)
PERCENTAGE_COLUMNS = (
    "humidity",
    "precip_probability",
    "clouds",
    "cloud_low",
    "cloud_mid",
    "cloud_high",
    "confidence",
    "humidity_700hpa",
)


def enforce_physical_bounds(frame: pd.DataFrame) -> pd.DataFrame:
    """Return a copy with impossible negative amounts and percentages removed."""
    if frame.empty:
        return frame.copy()
    bounded = frame.copy()
    for column in NON_NEGATIVE_COLUMNS:
        if column in bounded:
            bounded[column] = pd.to_numeric(
                bounded[column], errors="coerce"
            ).clip(lower=0)
    for column in PERCENTAGE_COLUMNS:
        if column in bounded:
            bounded[column] = pd.to_numeric(
                bounded[column], errors="coerce"
            ).clip(0, 100)
    if "wind_dir" in bounded:
        direction = pd.to_numeric(bounded["wind_dir"], errors="coerce")
        bounded["wind_dir"] = direction.mod(360)
    if "is_day" in bounded:
        bounded["is_day"] = pd.to_numeric(
            bounded["is_day"], errors="coerce"
        ).clip(0, 1)
    return bounded
