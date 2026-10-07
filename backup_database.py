"""Create and verify portable, non-destructive database backup archives."""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import re
import sqlite3
import tempfile
import zipfile
from contextlib import ExitStack, contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

from db import ensure_schema, get_engine
from source_health import record_source_result

BACKUP_TABLES = (
    "compact_archives",
    "window_predictions",
    "personal_accounts",
    "personal_profiles",
    "personal_alerts",
    "personal_devices",
    "station_raw",
    "station_3h",
    "forecast_ow",
    "forecast_runs",
    "forecast_scores",
    "forecast_regime_scores",
    "official_observations",
    "forecast_reference_scores",
    "forecast_reliability",
    "forecast_blend",
    "forecast_blend_history",
    "forecast_ensemble_runs",
    "environment_observations",
    "climate_normals",
    "climate_reference_normals",
    "station_profiles",
    "station_observations",
    "station_daily_summaries",
    "location_forecasts",
    "location_model_runs",
    "location_scores",
    "location_environment",
    "public_snapshots",
    "v5_products",
    "push_config",
    "push_subscriptions",
    "push_deliveries",
    "ecowitt_telemetry",
    "radar_local_snapshots",
    "official_alerts",
    "ingest_log",
    "source_health",
    "meta",
    "user_prefs",
)

BACKUP_FORMAT = "meteo-v4-portable-backup"
LEGACY_BACKUP_FORMATS = {"meteo-v3-portable-backup"}


@contextmanager
def _backup_csv_limit(archive: zipfile.ZipFile):
    """Allow snapshot fields up to their CSV entry size, then restore the limit."""
    previous = csv.field_size_limit()
    csv.field_size_limit(
        max(
            [
                previous,
                *(
                    item.file_size
                    for item in archive.infolist()
                    if item.filename.endswith(".csv")
                ),
            ]
        )
    )
    try:
        yield
    finally:
        csv.field_size_limit(previous)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


@contextmanager
def _csv_reader(archive, filename):
    """Stream large cold-history files instead of loading the entire CSV."""
    with (
        archive.open(filename) as binary,
        io.TextIOWrapper(binary, encoding="utf-8", newline="") as decoded,
    ):
        yield csv.DictReader(decoded)


def _destination(output: str | Path) -> Path:
    destination = Path(output)
    if destination.suffix.lower() == ".zip":
        destination.parent.mkdir(parents=True, exist_ok=True)
        return destination
    destination.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return destination / f"meteo-database-{stamp}.zip"


@contextmanager
def _backup_connection(engine):
    """One snapshot covers hot rows and cold blocks during concurrent moves."""
    from compact_history import TARGETS

    # Discover outside the snapshot: locks must precede its first SELECT.
    tables = set(inspect(engine).get_table_names())
    with engine.connect() as connection:
        if engine.dialect.name == "postgresql":
            connection = connection.execution_options(isolation_level="REPEATABLE READ")
        with connection.begin():
            if engine.dialect.name == "postgresql":
                connection.execute(text("SET TRANSACTION READ ONLY"))
                # Also prevent a table swap between the archive and hot-table
                # exports. DELETE-based moves remain concurrent and MVCC-safe.
                hot = ",".join(
                    f'"{table}"'
                    for table in (*TARGETS, "compact_archives")
                    if table in tables
                )
                if hot:
                    connection.execute(text(f"LOCK TABLE {hot} IN ACCESS SHARE MODE"))
            elif engine.dialect.name == "sqlite":
                connection.exec_driver_sql("BEGIN")
            yield connection


def create_backup(
    output: str | Path = "backups",
    engine: Engine | None = None,
    *,
    include_external=False,
) -> Path:
    """Export known tables to CSV plus a checksummed manifest in one ZIP file."""
    if engine is None:
        ensure_schema()
        engine = get_engine()
    destination = _destination(output)
    existing = set(inspect(engine).get_table_names())
    manifest: dict[str, Any] = {
        "format": BACKUP_FORMAT,
        "version": 3,
        "application": "Meteo Pro V5",
        "schema_version": 15,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "database_dialect": engine.dialect.name,
        "tables": {},
        "external_history": {
            "blocks": 0,
            "bytes": 0,
            "stores": [],
            "verification": "references-only",
        },
    }
    external_store = None
    try:
        with tempfile.TemporaryDirectory(
            prefix=".meteo-backup-", dir=destination.parent
        ) as temporary:
            temporary_path = Path(temporary)
            staged_archive = temporary_path / "database-backup.zip"
            with _backup_connection(engine) as connection:
                for table in BACKUP_TABLES:
                    if table not in existing:
                        continue
                    csv_path = temporary_path / f"{table}.csv"
                    rows = 0
                    first = True
                    for chunk in pd.read_sql(
                        text(f'SELECT * FROM "{table}"'),
                        connection,
                        chunksize=25 if table == "compact_archives" else 10_000,
                    ):
                        if table == "compact_archives":
                            if "object_bytes" in chunk:
                                chunk["object_bytes"] = pd.to_numeric(
                                    chunk["object_bytes"]
                                ).astype("Int64")
                            from r2_archive import (
                                R2Config,
                                R2Store,
                                resolve_payload,
                                validate_reference,
                            )

                            for index, row in chunk.iterrows():
                                if row["payload"]:
                                    continue
                                block = row.to_dict()
                                if include_external:
                                    external_store = external_store or R2Store(
                                        R2Config.from_env()
                                    )
                                    chunk.loc[index, "payload"] = resolve_payload(
                                        block, store=external_store
                                    )
                                    for column in (
                                        "object_key",
                                        "object_bytes",
                                        "object_store",
                                    ):
                                        chunk.loc[index, column] = None
                                else:
                                    validate_reference(block)
                                    external = manifest["external_history"]
                                    external["blocks"] += 1
                                    external["bytes"] += int(block["object_bytes"])
                                    if block["object_store"] not in external["stores"]:
                                        external["stores"].append(block["object_store"])
                        chunk.to_csv(
                            csv_path,
                            mode="w" if first else "a",
                            header=first,
                            index=False,
                            encoding="utf-8",
                        )
                        rows += len(chunk)
                        first = False
                    if first:
                        columns = [
                            item["name"] for item in inspect(engine).get_columns(table)
                        ]
                        pd.DataFrame(columns=columns).to_csv(
                            csv_path, index=False, encoding="utf-8"
                        )
                    manifest["tables"][table] = {
                        "file": csv_path.name,
                        "rows": rows,
                        "sha256": _sha256(csv_path),
                    }

            schema_path = Path(__file__).with_name("schema.sql")
            manifest["external_history"]["required"] = bool(
                manifest["external_history"]["blocks"]
            )
            with zipfile.ZipFile(
                staged_archive,
                "w",
                compression=zipfile.ZIP_DEFLATED,
                compresslevel=9,
            ) as archive:
                archive.write(schema_path, "schema.sql")
                for details in manifest["tables"].values():
                    archive.write(temporary_path / details["file"], details["file"])
                archive.writestr(
                    "manifest.json",
                    json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True),
                )
            verify_backup(staged_archive)
            staged_archive.replace(destination)
    except Exception:
        record_source_result(
            "database_backup", success=False, error="backup non riuscito", engine=engine
        )
        raise
    record_source_result(
        "database_backup",
        success=True,
        rows_received=sum(item["rows"] for item in manifest["tables"].values()),
        last_observation_at=manifest["created_at"],
        engine=engine,
    )
    return destination


def verify_backup(archive_path: str | Path) -> dict[str, Any]:
    """Validate archive paths, hashes and CSV row counts without restoring data."""
    archive_path = Path(archive_path)
    with zipfile.ZipFile(archive_path) as archive, _backup_csv_limit(archive):
        names = set(archive.namelist())
        if any(Path(name).is_absolute() or ".." in Path(name).parts for name in names):
            raise ValueError("Archivio non valido: percorso non sicuro")
        if not {"manifest.json", "schema.sql"} <= names:
            raise ValueError("Archivio non valido: manifest o schema mancanti")
        manifest = json.loads(archive.read("manifest.json"))
        if manifest.get("format") not in {BACKUP_FORMAT, *LEGACY_BACKUP_FORMATS}:
            raise ValueError("Formato backup non riconosciuto")
        external_count = external_bytes = 0
        external_stores = set()
        for table, details in manifest.get("tables", {}).items():
            if table not in BACKUP_TABLES:
                raise ValueError(f"Tabella inattesa nel manifest: {table}")
            filename = details.get("file")
            if filename not in names:
                raise ValueError(f"File mancante: {filename}")
            digest = hashlib.sha256()
            with archive.open(filename) as handle:
                for data in iter(lambda: handle.read(1024 * 1024), b""):
                    digest.update(data)
            if digest.hexdigest() != details.get("sha256"):
                raise ValueError(f"Checksum non valido: {filename}")
            with _csv_reader(archive, filename) as reader:
                row_count = sum(1 for _ in reader)
            if row_count != int(details.get("rows", -1)):
                raise ValueError(f"Conteggio righe non valido: {filename}")
            if table == "compact_archives":
                from compact_history import decode_block

                with _csv_reader(archive, filename) as reader:
                    blocks = reader
                    for block in blocks:
                        if block["payload"]:
                            decode_block(
                                block["payload"],
                                block["sha256"],
                                int(block["row_count"]),
                                block["codec"],
                            )
                        else:
                            from r2_archive import validate_reference

                            validate_reference(block)
                            external_count += 1
                            external_bytes += int(block["object_bytes"])
                            external_stores.add(block["object_store"])
        external = manifest.get("external_history", {})
        if external_count and manifest.get("version", 1) < 3:
            raise ValueError("Backup requires an explicit external history manifest")
        if (
            external_count != int(external.get("blocks", 0))
            or external_bytes != int(external.get("bytes", 0))
            or external_stores != set(external.get("stores", []))
            or bool(external_count) != bool(external.get("required", False))
        ):
            raise ValueError("External history manifest mismatch")
    return manifest


def restore_backup(
    archive_path: str | Path,
    destination_sqlite: str | Path,
    *,
    materialize_external=False,
) -> dict[str, Any]:
    """Restore a verified archive into a new disposable SQLite database.

    The destination is intentionally required to be absent: the recovery drill
    can never overwrite the live database or an existing local copy.
    """
    archive_path = Path(archive_path)
    destination = Path(destination_sqlite)
    if destination.exists():
        raise FileExistsError("Il database di destinazione esiste già")
    destination.parent.mkdir(parents=True, exist_ok=True)
    manifest = verify_backup(archive_path)
    staged_descriptor, staged_name = tempfile.mkstemp(
        prefix=".meteo-restore-",
        suffix=".sqlite",
        dir=destination.parent,
    )
    os.close(staged_descriptor)
    staged = Path(staged_name)
    restored_rows = 0
    external_store = None
    try:
        with (
            zipfile.ZipFile(archive_path) as archive,
            _backup_csv_limit(archive),
            sqlite3.connect(staged) as db,
            ExitStack() as csv_streams,
        ):
            db.execute("PRAGMA foreign_keys=OFF")
            db.executescript(archive.read("schema.sql").decode("utf-8"))
            for table, details in manifest.get("tables", {}).items():
                if table not in BACKUP_TABLES or not re.fullmatch(
                    r"[A-Za-z_][A-Za-z0-9_]*", table
                ):
                    raise ValueError(f"Tabella non ripristinabile: {table}")
                reader = csv_streams.enter_context(
                    _csv_reader(archive, details["file"])
                )
                columns = reader.fieldnames or []
                if not columns or any(
                    not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", column)
                    for column in columns
                ):
                    raise ValueError(f"Intestazione CSV non valida: {table}")
                quoted_columns = ",".join(f'"{column}"' for column in columns)
                placeholders = ",".join("?" for _ in columns)
                statement = (
                    f'INSERT INTO "{table}" ({quoted_columns}) VALUES ({placeholders})'
                )
                batch: list[tuple[Any, ...]] = []
                table_rows = 0
                for row in reader:
                    if (
                        table == "compact_archives"
                        and not row["payload"]
                        and materialize_external
                    ):
                        from r2_archive import R2Config, R2Store, resolve_payload

                        external_store = external_store or R2Store(R2Config.from_env())
                        row["payload"] = resolve_payload(row, store=external_store)
                        for key in ("object_key", "object_bytes", "object_store"):
                            row[key] = ""
                    batch.append(
                        tuple(
                            ""
                            if table == "compact_archives"
                            and column == "payload"
                            and row.get(column, "") == ""
                            else None
                            if row.get(column, "") == ""
                            else row[column]
                            for column in columns
                        )
                    )
                    if len(batch) >= (1 if table == "compact_archives" else 2_000):
                        db.executemany(statement, batch)
                        table_rows += len(batch)
                        batch.clear()
                if batch:
                    db.executemany(statement, batch)
                    table_rows += len(batch)
                if table_rows != int(details.get("rows", -1)):
                    raise ValueError(f"Conteggio ripristinato non valido: {table}")
                restored_rows += table_rows
            integrity = db.execute("PRAGMA integrity_check").fetchone()
            if not integrity or str(integrity[0]).lower() != "ok":
                raise ValueError("Controllo integrità SQLite non superato")
            db.commit()
        os.replace(staged, destination)
    except Exception:
        staged.unlink(missing_ok=True)
        raise
    return {
        "path": str(destination.resolve()),
        "tables": len(manifest.get("tables", {})),
        "rows": restored_rows,
        "created_at": manifest.get("created_at"),
        "external_history_required": bool(
            manifest.get("external_history", {}).get("required")
        )
        and not materialize_external,
    }


def record_cloud_backup_result(
    *,
    success: bool,
    rows: int = 0,
    observed_at: Any = None,
    error: str = "",
    engine: Engine | None = None,
) -> bool:
    """Record the upload separately from local archive creation/verification."""
    return record_source_result(
        "github_backup",
        success=success,
        rows_received=max(0, int(rows or 0)),
        last_observation_at=observed_at,
        error=error or ("" if success else "caricamento cloud non riuscito"),
        engine=engine,
    )


def _append_github_outputs(
    output_file: str | Path,
    destination: Path,
    manifest: dict[str, Any],
) -> None:
    rows = sum(int(item.get("rows") or 0) for item in manifest["tables"].values())
    created = pd.to_datetime(manifest.get("created_at"), utc=True, errors="coerce")
    local_date = (
        created.tz_convert("Europe/Rome").strftime("%Y-%m-%d")
        if pd.notna(created)
        else datetime.now(timezone.utc).strftime("%Y-%m-%d")
    )
    with Path(output_file).open("a", encoding="utf-8") as handle:
        handle.write(f"path={destination.resolve()}\n")
        handle.write(f"encrypted_path={destination.resolve()}.enc\n")
        handle.write(f"created_at={manifest['created_at']}\n")
        handle.write(f"local_date={local_date}\n")
        handle.write(f"rows={rows}\n")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Backup portatile e verificato del database Meteo V4.5"
    )
    parser.add_argument("--output", default="backups", help="Cartella o file ZIP")
    parser.add_argument("--verify", help="Verifica un archivio esistente")
    parser.add_argument("--restore", help="Ripristina un archivio verificato")
    parser.add_argument(
        "--include-external-history",
        action="store_true",
        help="Crea uno ZIP autosufficiente, scaricando e verificando lo storico R2",
    )
    parser.add_argument(
        "--materialize-external-history",
        action="store_true",
        help="Durante il ripristino verifica e scarica tutti i blocchi R2 nel nuovo SQLite",
    )
    parser.add_argument(
        "--restore-sqlite",
        help="Nuovo file SQLite di destinazione (non deve esistere)",
    )
    parser.add_argument(
        "--github-output",
        help="Scrive percorso e metadati nel file output di GitHub Actions",
    )
    parser.add_argument(
        "--record-cloud-status",
        choices=("success", "error"),
        help="Registra l'esito del caricamento cifrato senza creare un nuovo ZIP",
    )
    parser.add_argument("--rows", type=int, default=0)
    parser.add_argument("--observed-at")
    parser.add_argument("--error", default="")
    args = parser.parse_args()
    if args.record_cloud_status:
        ok = record_cloud_backup_result(
            success=args.record_cloud_status == "success",
            rows=args.rows,
            observed_at=args.observed_at,
            error=args.error,
        )
        return 0 if ok else 2
    if bool(args.restore) != bool(args.restore_sqlite):
        parser.error("--restore e --restore-sqlite devono essere usati insieme")
    if args.restore:
        summary = restore_backup(
            args.restore,
            args.restore_sqlite,
            materialize_external=args.materialize_external_history,
        )
        print(
            f"Ripristino valido: {summary['tables']} tabelle, "
            f"{summary['rows']} righe, destinazione {summary['path']}"
        )
        if summary["external_history_required"]:
            print(
                "Lo storico esterno richiede il bucket R2 indicato nel backup; usare --materialize-external-history per una copia offline completa."
            )
        return 0
    if args.verify:
        manifest = verify_backup(args.verify)
        print(
            f"Backup valido: {len(manifest.get('tables', {}))} tabelle, "
            f"creato {manifest.get('created_at', '—')}"
        )
        return 0
    destination = create_backup(
        args.output, include_external=args.include_external_history
    )
    manifest = verify_backup(destination)
    if args.github_output:
        _append_github_outputs(args.github_output, destination, manifest)
    print(f"Backup creato e verificato: {destination}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
