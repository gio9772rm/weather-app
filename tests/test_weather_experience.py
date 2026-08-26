from __future__ import annotations

import pandas as pd

from weather_experience import (
    activity_outlooks,
    aqi_category,
    build_daily_briefing,
    future_forecast,
    pollen_category,
    weather_insights,
)


def _forecast() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "valid_time": pd.date_range(
                "2026-08-25T08:00:00Z", periods=36, freq="h"
            ),
            "temp_c": [20 + (index % 10) for index in range(36)],
            "rain_mm": [0.0] * 4 + [0.5, 1.0] + [0.0] * 30,
            "precip_probability": [10.0] * 4 + [60.0, 80.0] + [15.0] * 30,
            "wind_kmh": [8.0] * 36,
            "wind_gust_kmh": [14.0] * 10 + [48.0] + [16.0] * 25,
            "humidity": [58.0] * 36,
            "clouds": [25.0] * 18 + [15.0] * 18,
            "confidence": [76.0] * 36,
            "description": ["Sereno"] * 4 + ["Pioggia"] * 2 + ["Variabile"] * 30,
            "is_day": [1.0] * 10 + [0.0] * 9 + [1.0] * 12 + [0.0] * 5,
        }
    )


def test_future_forecast_uses_explicit_utc_window() -> None:
    result = future_forecast(
        _forecast(), now=pd.Timestamp("2026-08-25T09:10:00Z"), hours=3
    )

    assert result["valid_time"].tolist() == [
        pd.Timestamp("2026-08-25T09:00:00Z"),
        pd.Timestamp("2026-08-25T10:00:00Z"),
        pd.Timestamp("2026-08-25T11:00:00Z"),
        pd.Timestamp("2026-08-25T12:00:00Z"),
    ]


def test_daily_briefing_and_insights_explain_rain_and_confidence() -> None:
    now = pd.Timestamp("2026-08-25T08:00:00Z")
    briefing = build_daily_briefing(_forecast(), now=now)
    insights = weather_insights(
        _forecast(), timezone="Europe/Rome", now=now
    )

    assert briefing.rain_probability == 80.0
    assert "Pioggia probabile" in briefing.headline
    assert [item.title for item in insights] == [
        "Prossima pioggia",
        "Raffica massima",
        "Minima / massima",
        "Fiducia",
    ]
    assert insights[0].tone == "danger"
    assert insights[-1].value == "76%"


def test_activity_outlooks_are_bounded_and_choose_local_times() -> None:
    activities = activity_outlooks(
        _forecast(),
        timezone="Europe/Rome",
        now=pd.Timestamp("2026-08-25T08:00:00Z"),
    )

    assert [item.activity for item in activities] == [
        "Passeggiata",
        "Astronomia",
    ]
    assert all(0 <= item.score <= 100 for item in activities)
    assert all(item.best_time != "—" for item in activities)


def test_air_quality_and_pollen_categories_have_semantic_tones() -> None:
    assert aqi_category(18) == ("Buona", "good")
    assert aqi_category(55) == ("Moderata", "warning")
    assert aqi_category(110) == ("Estremamente scarsa", "danger")
    assert pollen_category(5) == ("Basso", "good")
    assert pollen_category(75) == ("Alto", "warning")
    assert pollen_category(None) == ("Non disponibile", "neutral")
