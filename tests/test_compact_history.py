import base64
import hashlib
import json
import math
import os
import uuid
import zlib
from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError

from backup_database import _backup_connection, create_backup, restore_backup
from compact_history import (
    CODEC,
    LEGACY_CODEC,
    compact_batch,
    decode_block,
    encode_block,
    iter_history,
    maintain_history,
    reclaim_table,
)
from db import _schema_statements, ensure_schema, reset_engine_cache


@pytest.fixture(params=["sqlite", "postgresql"])
def storage_engine(request):
    if request.param == "sqlite":
        return request.getfixturevalue("sqlite_engine")
    url = os.getenv("TEST_POSTGRES_URL")
    if not url:
        pytest.skip("PostgreSQL integration runs in its dedicated CI service")
    parsed = make_url(url)
    if parsed.host not in {"localhost", "127.0.0.1"} or parsed.database != "meteo_test":
        pytest.fail("Integration tests require an isolated local meteo_test database")
    schema = "test_v56_" + uuid.uuid4().hex
    admin = create_engine(url)
    with admin.begin() as con:
        con.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(url, connect_args={"options": f"-csearch_path={schema}"})
    with engine.begin() as con:
        for sql in _schema_statements():
            con.execute(text(sql))
    request.addfinalizer(lambda: _cleanup_pg(admin, engine, schema))
    return engine


def _cleanup_pg(admin, engine, schema):
    engine.dispose()
    with admin.begin() as con:
        con.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    admin.dispose()


def seed(engine):
    with engine.begin() as con:
        con.execute(
            text(
                "INSERT INTO forecast_ensemble_runs(source,model,issued_at,valid_time,variable,"
                "p10,p50,member_count,event_probability,fetched_at) "
                "VALUES(:source,:model,:at,:valid,'rain_mm',:p10,:p50,40,:prob,:at)"
            ),
            [
                {
                    "source": "source",
                    "model": 'ICON, "Ω"\nensemble',
                    "at": at,
                    "valid": f"2026-10-{i + 1:02}T12:00:00Z",
                    "p10": None,
                    "p50": i * 0.0123456789,
                    "prob": (i % 3) / 3 * 100,
                }
                for i, at in enumerate(
                    ["2026-08-01T06:00:00Z"] * 9 + ["2026-10-06T06:00:00Z"]
                )
            ],
        )
    return list(iter_history("forecast_ensemble_runs", engine=engine))


def test_codec_preserves_special_floats_without_using_invalid_json():
    columns = ["value"]
    rows = [[float("nan")], [float("inf")], [float("-inf")], [None], [-0.0]]
    sha, payload = encode_block(columns, rows)
    raw = zlib.decompress(base64.b64decode(payload)).decode()
    assert "NaN" not in raw and "Infinity" not in raw
    restored = decode_block(payload, sha, len(rows), CODEC)
    values = [row[0] for row in restored["rows"]]
    assert math.isnan(values[0]) and values[1:4] == [float("inf"), float("-inf"), None]
    assert math.copysign(1, values[4]) == -1
    assert encode_block(restored["columns"], restored["rows"]) == (sha, payload)
    old = json.dumps({"columns": columns, "rows": [[None], [0.1]]}).encode()
    old_payload = base64.b64encode(zlib.compress(old)).decode()
    assert decode_block(old_payload, hashlib.sha256(old).hexdigest(), 2, LEGACY_CODEC)[
        "rows"
    ] == [[None], [0.1]]


def test_postgres_legacy_nan_and_infinity_survive_compaction_and_backup(
    storage_engine, tmp_path
):
    engine = storage_engine
    if engine.dialect.name != "postgresql":
        pytest.skip("SQLite maps SQL NaN to NULL; use PostgreSQL's native values")
    with engine.begin() as con:
        con.execute(
            text(
                "INSERT INTO forecast_scores(evaluated_at,provider,model,variable,horizon,n,brier) "
                "VALUES(:at,'legacy','model',:variable,'0-6',5,:brier)"
            ),
            [
                {"at": "2026-08-19T12:00:00Z", "variable": str(i), "brier": value}
                for i, value in enumerate([float("nan"), float("inf"), float("-inf")])
            ]
            + [{"at": "2026-10-06T12:00:00Z", "variable": "current", "brier": 0.25}],
        )
    assert compact_batch("forecast_scores", "2026-10-01", engine=engine) == 3
    backup = create_backup(tmp_path / "special.zip", engine)
    destination = tmp_path / "restored-special.sqlite"
    restore_backup(backup, destination)
    restored = create_engine(f"sqlite:///{destination}")
    try:
        for db in (engine, restored):
            values = {
                r["variable"]: r["brier"]
                for r in iter_history("forecast_scores", engine=db)
            }
            assert (
                math.isnan(values["0"])
                and values["1"] == float("inf")
                and values["2"] == float("-inf")
            )
            assert values["current"] == 0.25
    finally:
        restored.dispose()


def normalized(rows):
    return sorted(json.dumps(row, sort_keys=True, ensure_ascii=False) for row in rows)


def test_lossless_atomic_move_keeps_latest_and_all_cold_rows(storage_engine):
    engine = storage_engine
    original = seed(engine)
    for _ in range(4):
        compact_batch("forecast_ensemble_runs", "2026-10-01", engine=engine, limit=3)
    assert normalized(
        iter_history("forecast_ensemble_runs", engine=engine)
    ) == normalized(original)
    with engine.connect() as con:
        assert (
            con.execute(text("SELECT COUNT(*) FROM forecast_ensemble_runs")).scalar()
            == 1
        )
        blocks = con.execute(
            text("SELECT payload,sha256,row_count FROM compact_archives")
        ).all()
    assert sum(n for _, _, n in blocks) == 9
    assert (
        len(
            list(
                iter_history(
                    "forecast_ensemble_runs", since="2026-10-01", engine=engine
                )
            )
        )
        == 1
    )
    assert compact_batch("forecast_ensemble_runs", "2026-10-01", engine=engine) == 0
    for payload, checksum, n in blocks:
        assert len(decode_block(payload, checksum, n)["rows"]) == n
        with pytest.raises(ValueError, match="checksum"):
            decode_block(payload, "0" * 64, n)


def test_failed_verification_rolls_back_source_deletion(storage_engine, monkeypatch):
    original = seed(storage_engine)

    def fail(*_):
        raise ValueError("verification failed")

    monkeypatch.setattr("compact_history.decode_block", fail)
    with pytest.raises(ValueError, match="verification failed"):
        compact_batch("forecast_ensemble_runs", "2026-10-01", engine=storage_engine)
    with storage_engine.connect() as con:
        assert con.execute(
            text("SELECT COUNT(*) FROM forecast_ensemble_runs")
        ).scalar() == len(original)
        assert con.execute(text("SELECT COUNT(*) FROM compact_archives")).scalar() == 0


def test_cold_history_survives_backup_and_sqlite_restore(storage_engine, tmp_path):
    original = seed(storage_engine)
    compact_batch("forecast_ensemble_runs", "2026-10-01", engine=storage_engine)
    backup = create_backup(tmp_path / "cold.zip", storage_engine)
    destination = tmp_path / "restored.sqlite"
    restore_backup(backup, destination)
    restored = create_engine(f"sqlite:///{destination}")
    try:
        assert normalized(
            iter_history("forecast_ensemble_runs", engine=restored)
        ) == normalized(original)
    finally:
        restored.dispose()


def test_postgres_repack_is_atomic_preserves_indexes_and_honours_backups(
    storage_engine,
):
    engine = storage_engine
    if engine.dialect.name != "postgresql":
        pytest.skip("Physical file reclamation is PostgreSQL-only")
    original = seed(engine)
    indexes = inspect(engine).get_indexes("forecast_ensemble_runs")
    with _backup_connection(engine) as snapshot:
        assert (
            snapshot.execute(text("SELECT COUNT(*) FROM compact_archives")).scalar()
            == 0
        )
        compact_batch("forecast_ensemble_runs", "2026-10-01", engine=engine)
        assert (
            snapshot.execute(
                text("SELECT COUNT(*) FROM forecast_ensemble_runs")
            ).scalar()
            == 10
        )
        assert (
            snapshot.execute(text("SELECT COUNT(*) FROM compact_archives")).scalar()
            == 0
        )
        with pytest.raises(DBAPIError):
            reclaim_table("forecast_ensemble_runs", "2026-10-01", engine=engine)
    assert reclaim_table("forecast_ensemble_runs", "2026-10-01", engine=engine)
    assert normalized(
        iter_history("forecast_ensemble_runs", engine=engine)
    ) == normalized(original)
    assert inspect(engine).get_indexes("forecast_ensemble_runs") == indexes
    assert not reclaim_table("forecast_ensemble_runs", "2026-10-01", engine=engine)


def test_maintenance_is_bounded_and_observations_are_not_targets(storage_engine):
    seed(storage_engine)
    result = maintain_history(
        engine=storage_engine,
        now=datetime(2026, 10, 6, tzinfo=timezone.utc),
        max_batches=1,
    )
    assert result["batches"] == 1 and result["archived_rows"] == 9
    with pytest.raises(ValueError, match="not eligible"):
        compact_batch("station_observations", "2030", engine=storage_engine)


def test_duplicate_index_migration_preserves_primary_key_and_other_indexes(
    sqlite_engine,
):
    with sqlite_engine.begin() as con:
        con.execute(
            text(
                "CREATE INDEX idx_station_observations_time ON station_observations(station_id,time)"
            )
        )
        con.execute(
            text("CREATE INDEX keep_station_observations ON station_observations(time)")
        )
    reset_engine_cache()
    ensure_schema()
    indexes = inspect(sqlite_engine).get_indexes("station_observations")
    assert {i["name"] for i in indexes} == {"keep_station_observations"}
    assert inspect(sqlite_engine).get_pk_constraint("station_observations")[
        "constrained_columns"
    ] == ["station_id", "time"]


def test_latest_run_survives_an_extended_provider_outage(storage_engine):
    original = seed(storage_engine)
    compact_batch("forecast_ensemble_runs", "2030-01-01", engine=storage_engine)
    with storage_engine.connect() as con:
        assert (
            con.execute(text("SELECT COUNT(*) FROM forecast_ensemble_runs")).scalar()
            == 1
        )
    assert normalized(
        iter_history("forecast_ensemble_runs", engine=storage_engine)
    ) == normalized(original)
