from __future__ import annotations

import pandas as pd
import pytest

from config import Settings
from ensemble_forecast import parse_open_meteo_ensemble
from feature_registry import features
from forecast_change import summarize_forecast_change
from radar_nowcast import parse_nowcast_frames


def test_forecast_change_reports_two_aligned_emissions() -> None:
    now = pd.Timestamp("2026-08-26T12:00:00Z")
    rows = []
    for issued, offset in ((now - pd.Timedelta(hours=1), 0.0), (now, 0.4)):
        for hour in range(6):
            rows.append(
                {
                    "issued_at": issued,
                    "valid_time": now + pd.Timedelta(hours=hour),
                    "temp_c": 25 + offset,
                    "rain_mm": 0.2,
                    "precip_probability": 20,
                    "wind_gust_kmh": 18,
                    "confidence": 80,
                }
            )

    summary = summarize_forecast_change(pd.DataFrame(rows), now=now)

    assert summary.available is True
    assert summary.status == "stable"
    assert summary.temperature_change_c == pytest.approx(0.4)


def test_ensemble_parser_reduces_members_to_quantiles() -> None:
    payload = {
        "hourly": {
            "time": ["2026-08-26T12:00"],
            "temperature_2m": [20.0],
            "temperature_2m_member01": [22.0],
            "precipitation": [0.0],
            "precipitation_member01": [1.0],
        }
    }

    frame = parse_open_meteo_ensemble(
        payload, fetched_at=pd.Timestamp("2026-08-26T11:20:00Z")
    )

    temperature = frame[frame["variable"] == "temp_c"].iloc[0]
    rain = frame[frame["variable"] == "rain_mm"].iloc[0]
    assert temperature["member_count"] == 2
    assert temperature["p50"] == 21.0
    assert rain["event_probability"] == 50.0


def test_nowcast_frames_ignore_invalid_entries_and_sort() -> None:
    frames = parse_nowcast_frames(
        {
            "radar": {
                "nowcast": [
                    {"time": 1_800_000_600, "path": "/later"},
                    {"time": 1_800_000_000, "path": "/first"},
                    {"time": None, "path": "invalid"},
                ]
            }
        }
    )

    assert [frame["path"] for frame in frames] == ["/first", "/later"]


def test_feature_registry_reserves_the_next_four_integrations() -> None:
    registry = {feature.key: feature for feature in features(Settings.from_env())}

    assert registry["forecast_change"].phase == "V4.1"
    assert registry["ensemble"].enabled is True
    assert registry["climatology"].phase == "V4.2-ready"
    assert registry["measured_pollen"].enabled is False
    assert registry["official_alerts"].enabled is False
    assert registry["experience_mode"].enabled is False
