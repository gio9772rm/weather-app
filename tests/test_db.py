from __future__ import annotations

from sqlalchemy import inspect, text


def test_schema_contains_v3_tables_and_columns(sqlite_engine):
    inspector = inspect(sqlite_engine)
    tables = set(inspector.get_table_names())
    assert {
        "station_raw",
        "station_3h",
        "forecast_runs",
        "forecast_blend",
        "forecast_scores",
        "ingest_log",
        "meta",
    } <= tables
    raw_columns = {
        column["name"].lower() for column in inspector.get_columns("station_raw")
    }
    assert {
        "rain_rate_mm_h",
        "rain_total_mm",
        "solar_w_m2",
        "data_quality",
    } <= raw_columns


def test_meta_upsert_is_portable(sqlite_engine):
    from db import get_meta, set_meta

    set_meta("answer", 41)
    set_meta("answer", 42)
    assert get_meta("answer") == "42"
    with sqlite_engine.connect() as connection:
        assert (
            connection.execute(
                text("SELECT COUNT(*) FROM meta WHERE k='answer'")
            ).scalar_one()
            == 1
        )
