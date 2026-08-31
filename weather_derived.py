"""Derived station values that are not directly stored by Ecowitt."""

from __future__ import annotations

import numpy as np
import pandas as pd


def dew_point_c(temperature_c: pd.Series, humidity_percent: pd.Series) -> pd.Series:
    """Return Magnus dew point, preserving missing or physically invalid inputs."""
    temperature = pd.to_numeric(temperature_c, errors="coerce")
    humidity = pd.to_numeric(humidity_percent, errors="coerce")
    valid = temperature.between(-80, 80) & humidity.gt(0) & humidity.le(100)
    clipped_humidity = humidity.clip(lower=0.1, upper=100)
    gamma = np.log(clipped_humidity / 100.0) + (
        17.625 * temperature / (243.04 + temperature)
    )
    result = 243.04 * gamma / (17.625 - gamma)
    return result.where(valid)


def apparent_temperature_c(
    temperature_c: pd.Series,
    humidity_percent: pd.Series,
    wind_kmh: pd.Series,
) -> pd.Series:
    """Estimate shaded apparent temperature from temperature, humidity and wind.

    The formula is the standard vapour-pressure approximation used for apparent
    temperature. Solar exposure and clothing are intentionally not inferred.
    """
    temperature = pd.to_numeric(temperature_c, errors="coerce")
    humidity = pd.to_numeric(humidity_percent, errors="coerce")
    wind = pd.to_numeric(wind_kmh, errors="coerce")
    valid = temperature.between(-80, 80) & humidity.between(0, 100) & wind.ge(0)
    vapour_pressure = (
        humidity / 100.0 * 6.105 * np.exp(17.27 * temperature / (237.7 + temperature))
    )
    result = temperature + 0.33 * vapour_pressure - 0.70 * (wind / 3.6) - 4.0
    return result.where(valid)


def add_station_derived_values(frame: pd.DataFrame) -> pd.DataFrame:
    """Add calculated dew point and apparent temperature without mutating input."""
    if frame.empty:
        return frame.copy()
    output = frame.copy()
    index = output.index
    temperature = output.get("temp_c", pd.Series(np.nan, index=index))
    humidity = output.get("humidity", pd.Series(np.nan, index=index))
    wind = output.get("wind_kmh", pd.Series(np.nan, index=index))
    output["dewpoint_c"] = dew_point_c(temperature, humidity)
    output["feels_like_c"] = apparent_temperature_c(temperature, humidity, wind)
    return output
