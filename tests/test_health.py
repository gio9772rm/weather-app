from __future__ import annotations

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError

import data_access
from config import Settings


def test_health_snapshot_reads_v3_sources_independently(monkeypatch):
    current = pd.Timestamp.now(tz="UTC")
    queries: list[str] = []

    def fake_read(query: str, params=None):
        queries.append(query)
        if "station_raw" in query:
            return pd.DataFrame([{"station_time": current - pd.Timedelta(minutes=5)}])
        if "forecast_blend" in query:
            return pd.DataFrame(
                [
                    {
                        "forecast_issued": (
                            current - pd.Timedelta(minutes=10)
                        ).isoformat(),
                        "forecast_until": current + pd.Timedelta(days=7),
                    }
                ]
            )
        raise AssertionError("The legacy table must not be queried for V3 data")

    monkeypatch.setattr(data_access, "_read", fake_read)

    snapshot = data_access.health_snapshot(Settings.from_env())

    assert all("COALESCE" not in query.upper() for query in queries)
    assert all("forecast_ow" not in query for query in queries)
    assert snapshot["station_status"] == "online"
    assert snapshot["forecast_status"] == "online"
    assert snapshot["forecast_until"] == current + pd.Timedelta(days=7)
    assert set(snapshot["measurement_freshness"]) == {
        "temperature",
        "humidity",
        "pressure",
        "wind",
        "rain",
        "solar",
    }


def test_health_snapshot_falls_back_to_legacy_forecast(monkeypatch):
    current = pd.Timestamp.now(tz="UTC")
    legacy_time = current - pd.Timedelta(minutes=20)

    def fake_read(query: str, params=None):
        if "station_raw" in query:
            return pd.DataFrame([{"station_time": current - pd.Timedelta(minutes=5)}])
        if "forecast_blend" in query:
            return pd.DataFrame([{"forecast_issued": None, "forecast_until": pd.NaT}])
        if "forecast_ow" in query:
            return pd.DataFrame([{"legacy_time": legacy_time}])
        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(data_access, "_read", fake_read)

    snapshot = data_access.health_snapshot(Settings.from_env())

    assert snapshot["forecast_status"] == "online"
    assert snapshot["forecast_issued"] == legacy_time
    assert snapshot["forecast_until"] == legacy_time


def test_read_handles_database_outage_during_schema_check(monkeypatch):
    def unavailable():
        raise SQLAlchemyError("temporary database outage")

    monkeypatch.setattr(data_access, "ensure_schema", unavailable)

    assert data_access._read("SELECT 1").empty


def test_health_snapshot_does_not_hide_a_stale_sensor_behind_a_fresh_row(monkeypatch):
    current = pd.Timestamp.now(tz="UTC")

    def fake_read(query: str, params=None):
        if "station_raw" in query:
            return pd.DataFrame(
                [
                    {
                        "station_time": current - pd.Timedelta(minutes=5),
                        "temperature_time": current - pd.Timedelta(hours=4),
                        "humidity_time": current - pd.Timedelta(minutes=5),
                        "pressure_time": current - pd.Timedelta(minutes=5),
                        "wind_time": current - pd.Timedelta(minutes=5),
                    }
                ]
            )
        if "forecast_blend" in query:
            return pd.DataFrame(
                [
                    {
                        "forecast_issued": current - pd.Timedelta(minutes=10),
                        "forecast_until": current + pd.Timedelta(days=7),
                    }
                ]
            )
        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(data_access, "_read", fake_read)

    snapshot = data_access.health_snapshot(Settings.from_env())

    assert snapshot["station_sample_age_minutes"] < 10
    assert snapshot["measurement_freshness"]["temperature"]["status"] == "offline"
    assert snapshot["measurement_freshness"]["humidity"]["status"] == "online"
    assert snapshot["station_status"] == "offline"
