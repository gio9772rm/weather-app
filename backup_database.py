"""Create and verify portable, non-destructive database backup archives."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
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
        "application": "Meteo V4.3",
        "schema_version": 7,
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


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Backup portatile e verificato del database Meteo V4.3"
    )
    parser.add_argument("--output", default="backups", help="Cartella o file ZIP")
    parser.add_argument("--verify", help="Verifica un archivio esistente")
    args = parser.parse_args()
    if args.verify:
        manifest = verify_backup(args.verify)
        print(
            f"Backup valido: {len(manifest.get('tables', {}))} tabelle, "
            f"creato {manifest.get('created_at', '—')}"
        )
        return 0
    destination = create_backup(args.output)
    print(f"Backup creato e verificato: {destination}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
