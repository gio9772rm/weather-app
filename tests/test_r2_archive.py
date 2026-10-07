import base64
import io
import json
import sqlite3
import time

import pytest
from botocore.stub import Stubber
from sqlalchemy import create_engine, event, inspect, text
from test_compact_history import normalized, seed
from test_compact_history import (
    storage_engine as storage_engine,  # noqa: PLC0414 - pytest fixture re-export
)

import r2_archive
from backup_database import create_backup, restore_backup, verify_backup
from compact_history import compact_batch, iter_history, reclaim_table
from r2_archive import (
    R2Config,
    R2Store,
    offload_history,
    resolve_payload,
    storage_status,
)


class MemoryStore:
    def __init__(self, max_bytes=8_000_000_000):
        self.cfg = R2Config(
            "https://" + "a" * 32 + ".r2.cloudflarestorage.com",
            "meteo-history",
            "test-access",
            "test-secret",
            max_bytes,
        )
        self.objects = {}
        self.puts = 0
        self.inventories = 0
        self.fail = None

    def inventory(self, deadline):
        self.inventories += 1
        if self.fail == "inventory":
            raise ConnectionError("credential secret must not appear in logs")
        return sum(len(data) for data in self.objects.values())

    def exists(self, key):
        return key in self.objects

    def put_new(self, key, data):
        self.puts += 1
        if self.fail == "put":
            raise ConnectionError("test upload failure")
        self.objects.setdefault(key, data)

    def get(self, key, size):
        if self.fail == "get":
            raise ConnectionError("test read failure")
        data = self.objects[key]
        return data if self.fail != "corrupt" else data[:-1] + bytes([data[-1] ^ 1])


def cold(engine):
    original = seed(engine)
    compact_batch("forecast_ensemble_runs", "2026-10-01", engine=engine)
    return original


def block(engine):
    with engine.connect() as con:
        return dict(
            con.execute(text("SELECT * FROM compact_archives LIMIT 1")).mappings().one()
        )


def mock_store(monkeypatch, store):
    monkeypatch.setattr(
        r2_archive.R2Config, "from_env", classmethod(lambda cls: store.cfg)
    )
    monkeypatch.setattr(r2_archive, "R2Store", lambda cfg: store)


def test_disabled_and_missing_credentials_retain_history(storage_engine, monkeypatch):
    cold(storage_engine)
    monkeypatch.delenv("R2_ARCHIVE_ENABLED", raising=False)
    assert offload_history(engine=storage_engine)["state"] == "disabled"
    monkeypatch.setenv("R2_ARCHIVE_ENABLED", "true")
    monkeypatch.delenv("R2_SECRET_ACCESS_KEY", raising=False)
    assert offload_history(engine=storage_engine)["state"] == "deferred"
    assert block(storage_engine)["payload"]


@pytest.mark.parametrize("failure", ["put", "get", "corrupt", "inventory"])
def test_failed_transfer_never_removes_local_payload(storage_engine, failure, caplog):
    original = cold(storage_engine)
    store = MemoryStore()
    store.fail = failure
    before = block(storage_engine)
    state = offload_history(engine=storage_engine, store=store)
    assert state["state"] == "deferred" and block(storage_engine) == before
    assert "credential secret" not in caplog.text
    assert normalized(
        iter_history("forecast_ensemble_runs", engine=storage_engine)
    ) == normalized(original)
    store.fail = None
    assert offload_history(engine=storage_engine, store=store)["blocks"] == 1
    assert len(store.objects) == 1
    assert not block(storage_engine)["payload"]


def test_history_and_three_reference_backups_restore_offline(
    storage_engine, tmp_path, monkeypatch
):
    original = cold(storage_engine)
    store = MemoryStore()
    mock_store(monkeypatch, store)
    state = offload_history(engine=storage_engine, store=store)
    assert state["blocks"] == 1 and state["used_bytes"] == sum(
        map(len, store.objects.values())
    )
    assert normalized(
        iter_history("forecast_ensemble_runs", engine=storage_engine)
    ) == normalized(original)
    assert storage_status(engine=storage_engine)["remote_blocks"] == 1
    assert offload_history(engine=storage_engine, store=store)["blocks"] == 0
    assert store.puts == 1
    # Operational backups verify references without contacting R2.
    store.fail = "get"
    for index in range(3):
        archive = create_backup(tmp_path / f"backup-{index}.zip", storage_engine)
        manifest = verify_backup(archive)
        assert (
            manifest["external_history"]["required"]
            and manifest["external_history"]["blocks"] == 1
        )
        assert restore_backup(archive, tmp_path / f"references-{index}.sqlite")[
            "external_history_required"
        ]
    assert len(store.objects) == 1
    with pytest.raises(ConnectionError):
        restore_backup(archive, tmp_path / "failed.sqlite", materialize_external=True)
    assert not (tmp_path / "failed.sqlite").exists()
    store.fail = None
    offline = tmp_path / "offline.sqlite"
    assert not restore_backup(archive, offline, materialize_external=True)[
        "external_history_required"
    ]
    complete = create_backup(
        tmp_path / "complete.zip", storage_engine, include_external=True
    )
    assert not verify_backup(complete)["external_history"]["required"]
    store.objects.clear()
    restored = tmp_path / "complete.sqlite"
    restore_backup(complete, restored)
    for path in (offline, restored):
        sqlite = create_engine("sqlite:///" + str(path))
        assert normalized(
            iter_history("forecast_ensemble_runs", engine=sqlite)
        ) == normalized(original)
        sqlite.dispose()
        with sqlite3.connect(path) as db:
            assert db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"


def test_capacity_counts_orphans_and_recovers_same_object(storage_engine):
    cold(storage_engine)
    local = block(storage_engine)
    data = base64.b64decode(local["payload"])
    store = MemoryStore(max_bytes=len(data) + 2)
    store.objects["unrelated-file"] = b"123"
    assert offload_history(engine=storage_engine, store=store)["state"] == "capacity"
    assert block(storage_engine)["payload"] == local["payload"] and store.puts == 0
    store.objects[r2_archive._key(local, data)] = data
    with storage_engine.begin() as con:
        con.execute(text("DELETE FROM meta WHERE k='r2_usage'"))
    assert offload_history(engine=storage_engine, store=store)["blocks"] == 1
    assert store.puts == 0


def test_mixed_local_and_external_blocks_keep_integer_csv_references(
    storage_engine, tmp_path, monkeypatch
):
    original = seed(storage_engine)
    compact_batch(
        "forecast_ensemble_runs", "2026-10-01", engine=storage_engine, limit=4
    )
    compact_batch("forecast_ensemble_runs", "2026-10-01", engine=storage_engine)
    store = MemoryStore()
    mock_store(monkeypatch, store)
    assert (
        offload_history(engine=storage_engine, store=store, max_blocks=1)["blocks"] == 1
    )
    backup = create_backup(tmp_path / "mixed.zip", storage_engine)
    assert verify_backup(backup)["external_history"]["blocks"] == 1
    restored = tmp_path / "mixed.sqlite"
    restore_backup(backup, restored, materialize_external=True)
    sqlite = create_engine("sqlite:///" + str(restored))
    assert normalized(
        iter_history("forecast_ensemble_runs", engine=sqlite)
    ) == normalized(original)
    sqlite.dispose()


def test_db_failure_retains_copy_and_retry_reuses_orphan(storage_engine):
    cold(storage_engine)
    before = block(storage_engine)
    store = MemoryStore()

    def fail_update(con, cursor, statement, parameters, context, executemany):
        if statement.startswith("UPDATE compact_archives SET payload="):
            raise RuntimeError("simulated DB failure")

    event.listen(storage_engine, "before_cursor_execute", fail_update)
    try:
        assert (
            offload_history(engine=storage_engine, store=store)["state"] == "deferred"
        )
    finally:
        event.remove(storage_engine, "before_cursor_execute", fail_update)
    assert block(storage_engine) == before and len(store.objects) == 1
    assert offload_history(engine=storage_engine, store=store)["blocks"] == 1
    assert store.puts == 1
    assert store.inventories == 1


def test_reservations_survive_failed_put_and_periodic_inventory_reconciles(
    storage_engine,
):
    cold(storage_engine)
    store = MemoryStore()
    store.fail = "put"
    state = offload_history(engine=storage_engine, store=store)
    reserved = state["used_bytes"]
    assert reserved > 0 and not store.objects
    with storage_engine.begin() as con:
        usage = json.loads(
            con.execute(text("SELECT v FROM meta WHERE k='r2_usage'")).scalar()
        )
        assert usage["bytes"] == reserved
        usage["inventoried_at"] = "2000-01-01T00:00:00+00:00"
        con.execute(
            text("UPDATE meta SET v=:v WHERE k='r2_usage'"), {"v": json.dumps(usage)}
        )
    store.fail = None
    state = offload_history(engine=storage_engine, store=store)
    assert state["blocks"] == 1 and state["used_bytes"] == reserved
    assert store.inventories == 2


def test_wrong_destination_and_missing_object_fail_explicitly(
    storage_engine, monkeypatch
):
    cold(storage_engine)
    store = MemoryStore()
    mock_store(monkeypatch, store)
    offload_history(engine=storage_engine, store=store)
    remote = block(storage_engine)
    wrong = MemoryStore()
    wrong.cfg = R2Config(
        wrong.cfg.endpoint, "another-bucket", "test-access", "test-secret"
    )
    assert offload_history(engine=storage_engine, store=wrong)["state"] == "deferred"
    assert wrong.puts == 0
    with pytest.raises(ValueError, match="destination"):
        resolve_payload(remote, store=wrong)
    store.objects.clear()
    with pytest.raises(KeyError):
        list(iter_history("forecast_ensemble_runs", engine=storage_engine))


def test_external_repack_preserves_metadata_and_indexes(storage_engine):
    if storage_engine.dialect.name != "postgresql":
        pytest.skip("Physical reclaim is PostgreSQL only")
    cold(storage_engine)
    assert not reclaim_table("compact_archives", engine=storage_engine)
    store = MemoryStore()
    offload_history(engine=storage_engine, store=store)
    before = block(storage_engine)
    indexes = inspect(storage_engine).get_indexes("compact_archives")
    assert reclaim_table("compact_archives", engine=storage_engine)
    assert block(storage_engine) == before
    assert inspect(storage_engine).get_indexes("compact_archives") == indexes
    assert not reclaim_table("compact_archives", engine=storage_engine)


def test_sdk_paginates_and_uses_conditional_write_and_bounded_read():
    cfg = MemoryStore().cfg
    store = R2Store(cfg)
    with Stubber(store.client) as stub:
        stub.add_response(
            "list_objects_v2",
            {
                "IsTruncated": True,
                "NextContinuationToken": "next",
                "Contents": [{"Key": "old", "Size": 40}],
            },
            {"Bucket": cfg.bucket, "MaxKeys": 1000},
        )
        stub.add_response(
            "list_objects_v2",
            {"IsTruncated": False, "Contents": [{"Key": "orphan", "Size": 60}]},
            {"Bucket": cfg.bucket, "MaxKeys": 1000, "ContinuationToken": "next"},
        )
        assert store.inventory(time.monotonic() + 20) == 100
        stub.add_response(
            "put_object",
            {},
            {
                "Bucket": cfg.bucket,
                "Key": "test",
                "Body": b"test",
                "ContentType": "application/zlib",
                "IfNoneMatch": "*",
            },
        )
        store.put_new("test", b"test")
        stub.add_response(
            "get_object",
            {"Body": io.BytesIO(b"test"), "ContentLength": 4},
            {"Bucket": cfg.bucket, "Key": "test"},
        )
        assert store.get("test", 4) == b"test"
        stub.assert_no_pending_responses()


def test_config_rejects_untrusted_endpoint_and_budget_over_eight_gb(monkeypatch):
    for key, value in {
        "R2_ENDPOINT": MemoryStore().cfg.endpoint,
        "R2_BUCKET": "meteo-history",
        "R2_ACCESS_KEY_ID": "test-access",
        "R2_SECRET_ACCESS_KEY": "test-secret",
    }.items():
        monkeypatch.setenv(key, value)
    assert R2Config.from_env().max_bytes == 8_000_000_000
    assert "test-secret" not in repr(R2Config.from_env())
    monkeypatch.setenv("R2_ARCHIVE_MAX_BYTES", "10000000001")
    with pytest.raises(ValueError):
        R2Config.from_env()
    monkeypatch.delenv("R2_ARCHIVE_MAX_BYTES")
    monkeypatch.setenv("R2_ENDPOINT", "https://untrusted.example")
    with pytest.raises(ValueError):
        R2Config.from_env()


def test_station_runs_preserved_independently(storage_engine):
    with storage_engine.begin() as con:
        con.execute(
            text(
                "INSERT INTO location_model_runs(station_id,provider,model,issued_at,acquired_at,basis,payload) VALUES(:id,'test','model',:at,:at,'published',:p)"
            ),
            [
                {
                    "id": station,
                    "at": at,
                    "p": json.dumps({"station": station, "values": [24.5, None]}),
                }
                for station, times in [
                    ("primary", ["2025-01-01", "2025-01-02", "2025-01-03"]),
                    ("secondary", ["2026-01-01", "2026-01-02", "2026-01-03"]),
                ]
                for at in times
            ],
        )
    assert compact_batch("location_model_runs", "2027", engine=storage_engine) == 4
    with storage_engine.connect() as con:
        assert list(
            con.execute(
                text(
                    "SELECT station_id,issued_at FROM location_model_runs ORDER BY station_id"
                )
            )
        ) == [("primary", "2025-01-03"), ("secondary", "2026-01-03")]
    assert len(list(iter_history("location_model_runs", engine=storage_engine))) == 6
