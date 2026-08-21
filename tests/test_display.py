from __future__ import annotations

import numpy as np
import pandas as pd

from data_access import daily_forecast
from weather_display import compass_direction, forecast_interval, weather_cell_style


def test_compass_direction_normalizes_degrees() -> None:
    assert compass_direction(0) == "N · 0°"
    assert compass_direction(45) == "NE · 45°"
    assert compass_direction(225) == "SO · 225°"
    assert compass_direction(360) == "N · 0°"
    assert compass_direction(-90) == "O · 270°"
    assert compass_direction(np.nan) == "—"
    assert compass_direction(None) == "—"


def test_daily_forecast_includes_humidity_and_mean_wind() -> None:
    times = pd.date_range("2026-08-20T00:00:00Z", periods=3, freq="h")
    frame = pd.DataFrame(
        {
            "valid_time": times,
            "temp_c": [20.0, 21.0, 22.0],
            "humidity": [60.0, 70.0, 80.0],
            "pressure_hpa": [1012.0, 1013.0, 1014.0],
            "rain_mm": [0.0, 1.0, 0.0],
            "precip_probability": [10.0, 40.0, 20.0],
            "wind_kmh": [6.0, 9.0, 12.0],
            "wind_gust_kmh": [10.0, 14.0, 18.0],
            "clouds": [10.0, 30.0, 50.0],
            "confidence": [80.0, 70.0, 60.0],
            "description": ["Sereno", "Variabile", "Nuvoloso"],
        }
    )

    result = daily_forecast(frame, "Europe/Rome")

    assert len(result) == 1
    assert result.loc[0, "humidity_mean"] == 70.0
    assert result.loc[0, "wind_mean"] == 9.0
    assert result.loc[0, "wind_max"] == 18.0


def test_weather_cell_styles_flag_thresholds() -> None:
    assert "#bbf7d0" in weather_cell_style(22, "temperature")
    assert "#fecaca" in weather_cell_style(36, "temperature")
    assert "#fed7aa" in weather_cell_style(82, "humidity")
    assert "#fecaca" in weather_cell_style(70, "gust")
    assert "#60a5fa" in weather_cell_style(90, "rain_probability")
    assert "#bbf7d0" in weather_cell_style(21.7, "sqm")
    assert "#fecaca" in weather_cell_style(18.2, "sqm")
    assert "#bbf7d0" in weather_cell_style(2, "bortle")
    assert "#fecaca" in weather_cell_style(8, "bortle")
    assert weather_cell_style(None, "wind") == ""


def test_forecast_interval_keeps_first_row_and_requested_step() -> None:
    frame = pd.DataFrame({"value": list(range(8))})

    assert forecast_interval(frame, 1)["value"].tolist() == list(range(8))
    assert forecast_interval(frame, 3)["value"].tolist() == [0, 3, 6]
    assert forecast_interval(frame, 6)["value"].tolist() == [0, 6]
