from __future__ import annotations

from sqlalchemy import inspect, text


def test_schema_contains_v43_tables_and_columns(sqlite_engine):
    inspector = inspect(sqlite_engine)
    tables = set(inspector.get_table_names())
    assert {
        "station_raw",
        "station_3h",
        "forecast_runs",
        "forecast_blend",
        "forecast_blend_history",
        "forecast_ensemble_runs",
        "forecast_scores",
        "forecast_reference_scores",
        "forecast_reliability",
        "official_observations",
        "environment_observations",
        "climate_normals",
        "official_alerts",
        "ingest_log",
        "source_health",
        "station_profiles",
        "station_observations",
        "forecast_regime_scores",
        "climate_reference_normals",
        "radar_local_snapshots",
        "meta",
    } <= tables
    raw_columns = {
        column["name"].lower() for column in inspector.get_columns("station_raw")
    }
    assert {
        "wind_ms",
        "rain_rate_mm_h",
        "rain_total_mm",
        "solar_w_m2",
        "data_quality",
    } <= raw_columns
    score_columns = {
        column["name"].lower()
        for column in inspector.get_columns("forecast_scores")
    }
    assert {
        "holdout_n",
        "holdout_mae",
        "persistence_mae",
        "skill_vs_persistence",
        "reliability_gap",
    } <= score_columns
    forecast_columns = {
        column["name"].lower()
        for column in inspector.get_columns("forecast_blend")
    }
    assert {
        "cape_j_kg",
        "freezing_level_m",
        "wind_300hpa_kmh",
        "humidity_700hpa",
        "geopotential_500hpa_m",
        "temperature_850hpa_c",
    } <= forecast_columns
    with sqlite_engine.connect() as connection:
        assert connection.execute(
            text("SELECT v FROM meta WHERE k='schema_version'")
        ).scalar_one() == "7"


def test_legacy_station_raw_is_extended_without_losing_rows(tmp_path, monkeypatch):
    from db import ensure_schema, get_engine, reset_engine_cache

    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("SQLITE_PATH", str(tmp_path / "legacy-weather.db"))
    reset_engine_cache()
    try:
        engine = get_engine()
        with engine.begin() as connection:
            connection.execute(
                text(
                    "CREATE TABLE station_raw ("
                    "Time TEXT PRIMARY KEY, Temp_C REAL, Humidity REAL, "
                    "Pressure_hPa REAL, Wind_kmh REAL, WindGust_kmh REAL, "
                    "WindDir REAL, Rain_mm REAL)"
                )
            )
            connection.execute(
                text(
                    "INSERT INTO station_raw (Time, Temp_C) "
                    "VALUES ('2026-08-19T20:00:00Z', 25.0)"
                )
            )

        ensure_schema()

        columns = {
            column["name"].lower()
            for column in inspect(engine).get_columns("station_raw")
        }
        assert {
            "wind_ms",
            "rain_rate_mm_h",
            "rain_total_mm",
            "solar_w_m2",
            "uv_index",
            "source",
            "data_quality",
        } <= columns
        with engine.connect() as connection:
            assert connection.execute(
                text("SELECT COUNT(*) FROM station_raw")
            ).scalar_one() == 1
    finally:
        reset_engine_cache()


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
