from __future__ import annotations

import numpy as np
import pandas as pd

from weather_derived import (
    add_station_derived_values,
    apparent_temperature_c,
    dew_point_c,
)


def test_dew_point_and_apparent_temperature_are_derived_from_station_inputs():
    temperature = pd.Series([20.0])
    humidity = pd.Series([50.0])
    wind = pd.Series([5.0])

    dewpoint = dew_point_c(temperature, humidity).iloc[0]
    apparent = apparent_temperature_c(temperature, humidity, wind).iloc[0]

    assert 9.0 < dewpoint < 9.5
    assert 18.5 < apparent < 19.5


def test_invalid_or_missing_station_inputs_do_not_create_false_values():
    frame = pd.DataFrame(
        {
            "temp_c": [20.0, 20.0],
            "humidity": [0.0, 50.0],
            "wind_kmh": [2.0, np.nan],
        }
    )

    derived = add_station_derived_values(frame)

    assert pd.isna(derived.loc[0, "dewpoint_c"])
    assert pd.isna(derived.loc[1, "feels_like_c"])
    assert "dewpoint_c" not in frame
