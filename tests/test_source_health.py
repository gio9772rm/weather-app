from __future__ import annotations

import pandas as pd
from sqlalchemy import text

from data_access import data_completeness_snapshot, load_source_health
from source_health import record_source_result


def test_source_health_preserves_last_success_and_counts_failures(sqlite_engine):
    assert record_source_result(
        "ecowitt",
        success=True,
        rows_received=12,
        last_observation_at="2026-08-24T10:00:00Z",
        latency_ms=125,
        engine=sqlite_engine,
    )
    assert record_source_result(
        "ecowitt", success=False, error="temporaneamente non disponibile", engine=sqlite_engine
    )
    assert record_source_result(
        "ecowitt", success=False, error="secondo tentativo", engine=sqlite_engine
    )

    with sqlite_engine.connect() as connection:
        row = connection.execute(
            text("SELECT * FROM source_health WHERE source='ecowitt'")
        ).mappings().one()
    assert row["last_success_at"] is not None
    assert row["last_observation_at"] == "2026-08-24T10:00:00Z"
    assert row["consecutive_failures"] == 2
    assert row["last_error"] == "secondo tentativo"


def test_completeness_counts_five_minute_buckets_and_quality_flags(sqlite_engine):
    now = pd.Timestamp.now(tz="UTC").floor("5min")
    rows = []
    for offset in range(12):
        rows.append(
            {
                "time": (now - pd.Timedelta(minutes=offset * 5)).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                ),
                "quality": "spike_temp_c" if offset == 3 else "ok",
            }
        )
    with sqlite_engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO station_raw (time,data_quality) VALUES (:time,:quality)"
            ),
            rows,
        )

    snapshot = data_completeness_snapshot(1)
    health = load_source_health()

    assert snapshot["observed"] == 12
    assert snapshot["coverage"] == 100.0
    assert snapshot["anomalies"] == 1
    assert "ecowitt" in set(health["source"])
