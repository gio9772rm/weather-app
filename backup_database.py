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
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

from db import ensure_schema, get_engine
from source_health import record_source_result

BACKUP_TABLES = (
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


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _destination(output: str | Path) -> Path:
    destination = Path(output)
    if destination.suffix.lower() == ".zip":
        destination.parent.mkdir(parents=True, exist_ok=True)
        return destination
    destination.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return destination / f"meteo-database-{stamp}.zip"


def create_backup(output: str | Path = "backups", engine: Engine | None = None) -> Path:
    """Export known tables to CSV plus a checksummed manifest in one ZIP file."""
    if engine is None:
        ensure_schema()
        engine = get_engine()
    destination = _destination(output)
    existing = set(inspect(engine).get_table_names())
    manifest: dict[str, Any] = {
        "format": BACKUP_FORMAT,
        "version": 2,
        "application": "Meteo V4.4",
        "schema_version": 8,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "database_dialect": engine.dialect.name,
        "tables": {},
    }
    try:
        with tempfile.TemporaryDirectory(
            prefix=".meteo-backup-", dir=destination.parent
        ) as temporary:
            temporary_path = Path(temporary)
            staged_archive = temporary_path / "database-backup.zip"
            with engine.connect() as connection:
                for table in BACKUP_TABLES:
                    if table not in existing:
                        continue
                    csv_path = temporary_path / f"{table}.csv"
                    rows = 0
                    first = True
                    for chunk in pd.read_sql(
                        text(f'SELECT * FROM "{table}"'),
                        connection,
                        chunksize=10_000,
                    ):
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
    with zipfile.ZipFile(archive_path) as archive:
        names = set(archive.namelist())
        if any(Path(name).is_absolute() or ".." in Path(name).parts for name in names):
            raise ValueError("Archivio non valido: percorso non sicuro")
        if not {"manifest.json", "schema.sql"} <= names:
            raise ValueError("Archivio non valido: manifest o schema mancanti")
        manifest = json.loads(archive.read("manifest.json"))
        if manifest.get("format") not in {BACKUP_FORMAT, *LEGACY_BACKUP_FORMATS}:
            raise ValueError("Formato backup non riconosciuto")
        for table, details in manifest.get("tables", {}).items():
            if table not in BACKUP_TABLES:
                raise ValueError(f"Tabella inattesa nel manifest: {table}")
            filename = details.get("file")
            if filename not in names:
                raise ValueError(f"File mancante: {filename}")
            payload = archive.read(filename)
            if hashlib.sha256(payload).hexdigest() != details.get("sha256"):
                raise ValueError(f"Checksum non valido: {filename}")
            decoded = payload.decode("utf-8").splitlines()
            row_count = max(0, sum(1 for _ in csv.reader(decoded)) - 1)
            if row_count != int(details.get("rows", -1)):
                raise ValueError(f"Conteggio righe non valido: {filename}")
    return manifest


def restore_backup(
    archive_path: str | Path,
    destination_sqlite: str | Path,
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
    try:
        with zipfile.ZipFile(archive_path) as archive, sqlite3.connect(staged) as db:
            db.execute("PRAGMA foreign_keys=OFF")
            db.executescript(archive.read("schema.sql").decode("utf-8"))
            for table, details in manifest.get("tables", {}).items():
                if table not in BACKUP_TABLES or not re.fullmatch(
                    r"[A-Za-z_][A-Za-z0-9_]*", table
                ):
                    raise ValueError(f"Tabella non ripristinabile: {table}")
                decoded = archive.read(details["file"]).decode("utf-8")
                reader = csv.DictReader(io.StringIO(decoded))
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
                    batch.append(
                        tuple(
                            None if row.get(column, "") == "" else row[column]
                            for column in columns
                        )
                    )
                    if len(batch) >= 2_000:
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
        description="Backup portatile e verificato del database Meteo V4.4"
    )
    parser.add_argument("--output", default="backups", help="Cartella o file ZIP")
    parser.add_argument("--verify", help="Verifica un archivio esistente")
    parser.add_argument("--restore", help="Ripristina un archivio verificato")
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
        summary = restore_backup(args.restore, args.restore_sqlite)
        print(
            f"Ripristino valido: {summary['tables']} tabelle, "
            f"{summary['rows']} righe, destinazione {summary['path']}"
        )
        return 0
    if args.verify:
        manifest = verify_backup(args.verify)
        print(
            f"Backup valido: {len(manifest.get('tables', {}))} tabelle, "
            f"creato {manifest.get('created_at', '—')}"
        )
        return 0
    destination = create_backup(args.output)
    manifest = verify_backup(destination)
    if args.github_output:
        _append_github_outputs(args.github_output, destination, manifest)
    print(f"Backup creato e verificato: {destination}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
