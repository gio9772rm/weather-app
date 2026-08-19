from __future__ import annotations

import pandas as pd

import data_access
from config import Settings


def test_health_snapshot_keeps_database_timestamp_types_separate(monkeypatch):
    current = pd.Timestamp.now(tz="UTC")
    captured: dict[str, str] = {}

    def fake_read(query: str, params=None):
        captured["query"] = query
        return pd.DataFrame(
            [
                {
                    "station_time": current - pd.Timedelta(minutes=5),
                    "blend_issued": (current - pd.Timedelta(minutes=10)).isoformat(),
                    "blend_until": current + pd.Timedelta(days=7),
                    "legacy_time": (current - pd.Timedelta(hours=1)).to_pydatetime(),
                }
            ]
        )

    monkeypatch.setattr(data_access, "_read", fake_read)

    snapshot = data_access.health_snapshot(Settings.from_env())

    assert "COALESCE" not in captured["query"].upper()
    assert snapshot["station_status"] == "online"
    assert snapshot["forecast_status"] == "online"
    assert snapshot["forecast_until"] == current + pd.Timedelta(days=7)


def test_health_snapshot_falls_back_to_legacy_forecast(monkeypatch):
    current = pd.Timestamp.now(tz="UTC")
    legacy_time = current - pd.Timedelta(minutes=20)

    def fake_read(query: str, params=None):
        return pd.DataFrame(
            [
                {
                    "station_time": current - pd.Timedelta(minutes=5),
                    "blend_issued": None,
                    "blend_until": pd.NaT,
                    "legacy_time": legacy_time,
                }
            ]
        )

    monkeypatch.setattr(data_access, "_read", fake_read)

    snapshot = data_access.health_snapshot(Settings.from_env())

    assert snapshot["forecast_status"] == "online"
    assert snapshot["forecast_issued"] == legacy_time
    assert snapshot["forecast_until"] == legacy_time
