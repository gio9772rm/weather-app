"""Lossless cold history, maintained inside the existing ingestion gate.

Recent verification data stays hot; older weather records are preserved in
checked blocks. Raw station measurements remain in their original tables.
Source rows and their compressed replacement change in one transaction.
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import math
import time
import zlib
from datetime import datetime, timedelta, timezone

from sqlalchemy import inspect, text

from db import ensure_schema, get_engine

log = logging.getLogger(__name__)
TARGETS = {
    "forecast_scores": ("evaluated_at", 7),
    "forecast_regime_scores": ("evaluated_at", 7),
    "forecast_reference_scores": ("evaluated_at", 7),
    "forecast_ensemble_runs": ("issued_at", 2),
    "forecast_runs": ("issued_at", 120),
    "forecast_blend_history": ("issued_at", 120),
    "location_model_runs": ("issued_at", 90),
    "location_forecasts": ("issued_at", 90),
    "window_predictions": ("acquired_at", 90),
    "official_observations": ("time", 180),
    "environment_observations": ("time", 180),
    "official_alerts": ("issued_at", 180),
    "radar_local_snapshots": ("observed_at", 14),
}
# Preserve each source/station's last emission through a prolonged outage.
LATEST_GROUPS = {
    "forecast_ensemble_runs": ("source", "model"),
    "forecast_runs": ("provider", "model"),
    "location_model_runs": ("station_id", "provider", "model", "basis"),
    "location_forecasts": ("station_id",),
    "window_predictions": ("station_id",),
    "official_observations": ("source", "station_id"),
    "environment_observations": ("source", "station_id", "metric"),
    "radar_local_snapshots": ("station_id",),
}
BATCH_LIMITS = {
    "location_model_runs": 20,
    "location_forecasts": 5,
    "window_predictions": 50,
}
CODEC = "json-zlib-base64-v2"
LEGACY_CODEC = "json-zlib-base64-v1"
LOCK_KEY = 0x4D4554454F5636


def _target(table):
    if table not in TARGETS:
        raise ValueError("Table is not eligible for cold history")
    return TARGETS[table]


def _older_than_latest(table, column):
    group = LATEST_GROUPS.get(table, ())
    match = " AND ".join(f'latest."{key}"="{table}"."{key}"' for key in group)
    where = " WHERE " + match if match else ""
    return f'"{column}" < (SELECT MAX(latest."{column}") FROM "{table}" latest{where})'


def encode_block(columns, rows):
    def encoded(value):
        if isinstance(value, float) and not math.isfinite(value):
            return {
                "__meteo_float__": "nan"
                if math.isnan(value)
                else "inf"
                if value > 0
                else "-inf"
            }
        return value

    raw = json.dumps(
        {
            "columns": columns,
            "rows": [[encoded(value) for value in row] for row in rows],
        },
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest(), base64.b64encode(
        zlib.compress(raw, 6)
    ).decode("ascii")


def decode_block(payload, checksum, row_count, codec=CODEC):
    if codec not in {CODEC, LEGACY_CODEC}:
        raise ValueError("Unsupported history codec")
    raw = zlib.decompress(base64.b64decode(payload, validate=True))
    if hashlib.sha256(raw).hexdigest() != checksum:
        raise ValueError("Cold history checksum mismatch")
    block = json.loads(raw)
    if len(block["rows"]) != row_count or any(
        len(row) != len(block["columns"]) for row in block["rows"]
    ):
        raise ValueError("Cold history row count mismatch")
    if codec == CODEC:

        def decoded(value):
            if isinstance(value, dict):
                if set(value) != {"__meteo_float__"} or value[
                    "__meteo_float__"
                ] not in {"nan", "inf", "-inf"}:
                    raise ValueError("Cold history scalar type mismatch")
                return float(value["__meteo_float__"])
            return value

        block["rows"] = [[decoded(value) for value in row] for row in block["rows"]]
    return block


def compact_batch(table, cutoff, *, engine=None, limit=2500):
    """Commit a bounded, checked move, or leave all source rows intact."""
    time_column, _ = _target(table)
    engine = engine or get_engine()
    with engine.begin() as con:
        postgres = con.dialect.name == "postgresql"
        if con.dialect.name == "sqlite":
            # sqlite3's legacy transaction mode does not start a transaction
            # for a statement beginning WITH. Make rollback protection real.
            con.exec_driver_sql("BEGIN")
        if postgres:
            con.execute(text("SET LOCAL lock_timeout = '2s'"))
            con.execute(text("SET LOCAL statement_timeout = '20s'"))
            if not con.execute(
                text("SELECT pg_try_advisory_xact_lock(:key)"), {"key": LOCK_KEY}
            ).scalar():
                return 0
        pk = inspect(con).get_pk_constraint(table)["constrained_columns"]
        keys = ",".join(f'"{key}"' for key in pk)
        # RETURNING binds the archived rows to the exact deleted batch, even if
        # a writer adds another old row concurrently. Failure rolls both back.
        result = con.execute(
            text(
                f'WITH batch AS (SELECT {keys} FROM "{table}" '
                f'WHERE "{time_column}" < :cutoff AND {_older_than_latest(table, time_column)} '
                f'ORDER BY "{time_column}",{keys} '
                f"LIMIT :limit {'FOR UPDATE' if postgres else ''}) "
                f'DELETE FROM "{table}" WHERE ({keys}) IN (SELECT {keys} FROM batch) RETURNING *'
            ),
            {"cutoff": cutoff, "limit": max(1, min(2500, int(limit)))},
        )
        columns = list(result.keys())
        rows = [list(row) for row in result]
        if not rows:
            return 0
        rows.sort(key=lambda row: tuple(row[columns.index(key)] for key in pk))
        checksum, payload = encode_block(columns, rows)
        block = decode_block(payload, checksum, len(rows))
        if encode_block(block["columns"], block["rows"]) != (checksum, payload):
            raise ValueError("Cold history round trip mismatch")
        times = [row[columns.index(time_column)] for row in rows]
        con.execute(
            text(
                "INSERT INTO compact_archives(archive_key,source_table,time_min,time_max,"
                "codec,row_count,sha256,payload,archived_at) "
                "VALUES(:key,:table,:start,:end,:codec,:n,:sha,:payload,:at)"
            ),
            {
                "key": table + ":" + checksum,
                "table": table,
                "start": min(times),
                "end": max(times),
                "codec": CODEC,
                "n": len(rows),
                "sha": checksum,
                "payload": payload,
                "at": datetime.now(timezone.utc).isoformat(),
            },
        )
        return len(rows)


def iter_history(table, *, since=None, until=None, engine=None):
    """Stream verified cold rows (local or R2) and hot rows for export/research."""
    time_column, _ = _target(table)
    engine = engine or get_engine()
    with engine.connect() as con:
        if con.dialect.name == "postgresql":
            con = con.execution_options(isolation_level="REPEATABLE READ")
        with con.begin():
            if con.dialect.name == "sqlite":
                con.exec_driver_sql("BEGIN")
            else:
                con.execute(
                    text(f'LOCK TABLE "{table}",compact_archives IN ACCESS SHARE MODE')
                )
            blocks = con.execute(
                text(
                    "SELECT * FROM compact_archives "
                    "WHERE source_table=:table AND (CAST(:since AS TEXT) IS NULL OR time_max>=:since) "
                    "AND (CAST(:until AS TEXT) IS NULL OR time_min<:until) ORDER BY time_min,archive_key"
                ),
                {"table": table, "since": since, "until": until},
            )
            from r2_archive import resolve_payload

            external_store = None
            for archived in blocks.mappings():
                if not archived["payload"] and external_store is None:
                    from r2_archive import R2Config, R2Store

                    external_store = R2Store(R2Config.from_env())
                block = decode_block(
                    resolve_payload(archived, store=external_store),
                    archived["sha256"],
                    archived["row_count"],
                    archived["codec"],
                )
                for values in block["rows"]:
                    row = dict(zip(block["columns"], values, strict=True))
                    stamp = row[time_column]
                    if (since is None or stamp >= since) and (
                        until is None or stamp < until
                    ):
                        yield row
            for row in con.execute(
                text(
                    f'SELECT * FROM "{table}" WHERE (CAST(:since AS TEXT) IS NULL OR "{time_column}">=:since) '
                    f'AND (CAST(:until AS TEXT) IS NULL OR "{time_column}"<:until)'
                ),
                {"since": since, "until": until},
            ).mappings():
                yield dict(row)


def reclaim_table(table, cutoff=None, *, engine=None):
    """Free PostgreSQL files with a small, atomic hot-table copy, never VACUUM FULL.

    Called only after all eligible rows are safe in cold storage. A short lock
    timeout postpones this work if a reader/backup is busy. Unsupported schema,
    dependencies, inadequate headroom or any mismatch leaves the original.
    """
    external_index = table == "compact_archives"
    time_column = None if external_index else _target(table)[0]
    engine = engine or get_engine()
    if engine.dialect.name != "postgresql":
        return False
    marker = ("r2-repacked:v1:" if external_index else "compact-repacked:v56:") + table
    shadow = "_compact_" + table
    with engine.begin() as con:
        con.execute(text("SET LOCAL lock_timeout = '2s'"))
        con.execute(text("SET LOCAL statement_timeout = '30s'"))
        if not con.execute(
            text("SELECT pg_try_advisory_xact_lock(:key)"), {"key": LOCK_KEY}
        ).scalar():
            return False
        if con.execute(
            text("SELECT v FROM meta WHERE k=:key"), {"key": marker}
        ).scalar():
            return False
        con.execute(text(f'LOCK TABLE "{table}" IN ACCESS EXCLUSIVE MODE'))
        if external_index:
            from r2_archive import validate_reference

            if con.execute(
                text("SELECT 1 FROM compact_archives WHERE payload<>'' LIMIT 1")
            ).first():
                return False
            blocks = (
                con.execute(text("SELECT * FROM compact_archives")).mappings().all()
            )
            if not blocks:
                return False
            for block in blocks:
                validate_reference(block)
        elif con.execute(
            text(
                f'SELECT 1 FROM "{table}" WHERE "{time_column}"<:cutoff AND '
                f"{_older_than_latest(table, time_column)} LIMIT 1"
            ),
            {"cutoff": cutoff},
        ).first():
            return False
        if (
            not external_index
            and not con.execute(
                text(
                    "SELECT 1 FROM compact_archives WHERE source_table=:table LIMIT 1"
                ),
                {"table": table},
            ).first()
        ):
            return False
        # These application-owned tables have no custom privileges, triggers,
        # RLS, sequences or foreign keys. Decline a rewrite if that changes.
        unsupported = con.execute(
            text(
                "SELECT relacl IS NOT NULL OR relrowsecurity OR EXISTS "
                "(SELECT 1 FROM pg_trigger WHERE tgrelid=pg_class.oid AND NOT tgisinternal) "
                "OR EXISTS (SELECT 1 FROM pg_constraint WHERE contype='f' "
                "AND (conrelid=pg_class.oid OR confrelid=pg_class.oid)) "
                "FROM pg_class WHERE oid=CAST(:table AS regclass)"
            ),
            {"table": table},
        ).scalar()
        if unsupported:
            return False
        n, raw_bytes = con.execute(
            text(
                f'SELECT COUNT(*), COALESCE(SUM(octet_length(row_to_json(t)::text)),0) FROM "{table}" t'
            )
        ).one()
        size = con.execute(text("SELECT pg_database_size(current_database())")).scalar()
        # Leave a reserve for WAL and internal files on the existing 1 GB disk.
        if n > 100_000 or size + raw_bytes * 3 > 850 * 1024 * 1024:
            return False
        original = inspect(con)
        old_pk = original.get_pk_constraint(table)["name"]
        old_indexes = original.get_indexes(table)
        con.execute(text(f'CREATE TABLE "{shadow}" (LIKE "{table}" INCLUDING ALL)'))
        con.execute(text(f'INSERT INTO "{shadow}" SELECT * FROM "{table}"'))
        mismatch = con.execute(
            text(
                f'SELECT EXISTS((SELECT * FROM "{table}" EXCEPT SELECT * FROM "{shadow}") '
                f'UNION ALL (SELECT * FROM "{shadow}" EXCEPT SELECT * FROM "{table}"))'
            )
        ).scalar()
        if mismatch:
            raise ValueError("Hot history copy mismatch")
        cloned = inspect(con)
        new_pk = cloned.get_pk_constraint(shadow)["name"]
        new_indexes = cloned.get_indexes(shadow)
        renames = [(new_pk, old_pk)]
        for old in old_indexes:
            matching = [
                idx
                for idx in new_indexes
                if idx["column_names"] == old["column_names"]
                and idx["unique"] == old["unique"]
            ]
            if len(matching) != 1:
                raise ValueError("Hot history index mismatch")
            renames.append((matching[0]["name"], old["name"]))
        # RESTRICT (the default) also protects any unexpected dependent view.
        con.execute(text(f'DROP TABLE "{table}"'))
        con.execute(text(f'ALTER TABLE "{shadow}" RENAME TO "{table}"'))
        quote = con.dialect.identifier_preparer.quote
        for before, after in renames:
            con.execute(text(f"ALTER INDEX {quote(before)} RENAME TO {quote(after)}"))
        con.execute(
            text("INSERT INTO meta(k,v) VALUES(:key,:value)"),
            {
                "key": marker,
                "value": json.dumps(
                    {"at": datetime.now(timezone.utc).isoformat(), "hot_rows": n}
                ),
            },
        )
        return True


def maintain_history(*, engine=None, now=None, max_batches=20, seconds=35):
    """Resume incremental maintenance; a failure never breaks weather ingestion."""
    if engine is None:
        ensure_schema()
        engine = get_engine()
    now = now or datetime.now(timezone.utc)
    deadline = time.monotonic() + seconds
    batches = moved = 0
    reclaimed = []
    errors = []
    for table, (_, days) in TARGETS.items():
        if table in {
            "forecast_runs",
            "forecast_blend_history",
            "official_observations",
        }:
            from config import Settings

            days = max(days, Settings.from_env().score_lookback_days + 7)
        cutoff = (now - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%S+00:00")
        try:
            while batches < max_batches and time.monotonic() < deadline:
                n = compact_batch(
                    table, cutoff, engine=engine, limit=BATCH_LIMITS.get(table, 2500)
                )
                if not n:
                    if reclaim_table(table, cutoff, engine=engine):
                        reclaimed.append(table)
                    break
                batches += 1
                moved += n
        except Exception as exc:  # noqa: BLE001 - rollback, retry on the next normal cycle
            log.warning(
                "Manutenzione storico rinviata: %s (%s)", table, type(exc).__name__
            )
            errors.append(table)
        if batches >= max_batches or time.monotonic() >= deadline:
            break
    return {
        "archived_rows": moved,
        "batches": batches,
        "reclaimed": reclaimed,
        "deferred": errors,
    }
