from __future__ import annotations

import pandas as pd

from forecast_change import summarize_forecast_change


def _history(temp_shift: float = 0.2, rain_shift: float = 0.0) -> pd.DataFrame:
    rows = []
    for issued, shift in (
        (pd.Timestamp("2026-08-26T08:00:00Z"), 0.0),
        (pd.Timestamp("2026-08-26T09:00:00Z"), temp_shift),
    ):
        for hour in range(1, 13):
            rows.append(
                {
                    "issued_at": issued,
                    "valid_time": pd.Timestamp("2026-08-26T09:00:00Z")
                    + pd.Timedelta(hours=hour),
                    "temp_c": 25 + shift,
                    "rain_mm": (0.1 + rain_shift if shift else 0.1),
                    "precip_probability": 20 + (rain_shift * 20 if shift else 0),
                    "wind_gust_kmh": 20,
                    "confidence": 75,
                }
            )
    return pd.DataFrame(rows)


def test_change_summary_reports_stable_emissions():
    summary = summarize_forecast_change(
        _history(), now=pd.Timestamp("2026-08-26T09:00:00Z")
    )
    assert summary.available
    assert summary.status == "stable"
    assert "invariata" in summary.detail


def test_change_summary_waits_for_second_emission():
    history = _history()
    one = history[history["issued_at"] == pd.Timestamp("2026-08-26T09:00:00Z")]
    summary = summarize_forecast_change(one)
    assert not summary.available
    assert summary.status == "collecting"
