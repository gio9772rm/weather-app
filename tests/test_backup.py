from __future__ import annotations

import json
import zipfile

from sqlalchemy import text

from backup_database import (
    _append_github_outputs,
    create_backup,
    record_cloud_backup_result,
    restore_backup,
    verify_backup,
)


def test_backup_is_portable_checksummed_and_contains_no_connection_string(
    sqlite_engine, tmp_path
):
    with sqlite_engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO station_raw (time,temp_c,source,data_quality) "
                "VALUES ('2026-08-24T10:00:00Z',24.5,'test','ok')"
            )
        )

    destination = create_backup(tmp_path / "daily.zip", sqlite_engine)
    manifest = verify_backup(destination)

    assert manifest["format"] == "meteo-v4-portable-backup"
    assert manifest["version"] == 2
    assert manifest["schema_version"] == 8
    assert manifest["tables"]["station_raw"]["rows"] == 1
    assert {
        "station_profiles",
        "station_observations",
        "forecast_regime_scores",
        "climate_reference_normals",
        "radar_local_snapshots",
        "ecowitt_telemetry",
    } <= set(manifest["tables"])
    with zipfile.ZipFile(destination) as archive:
        assert {"manifest.json", "schema.sql", "station_raw.csv"} <= set(
            archive.namelist()
        )
        manifest_text = archive.read("manifest.json").decode("utf-8")
        parsed = json.loads(manifest_text)
        assert "database_url" not in manifest_text.casefold()
        assert parsed["database_dialect"] == "sqlite"

    with sqlite_engine.connect() as connection:
        backup_health = (
            connection.execute(
                text(
                    "SELECT status,last_success_at,last_observation_at "
                    "FROM source_health WHERE source='database_backup'"
                )
            )
            .mappings()
            .one()
        )
    assert backup_health["status"] == "online"
    assert backup_health["last_success_at"] is not None
    assert backup_health["last_observation_at"] is not None


def test_github_outputs_and_cloud_upload_have_independent_health(
    sqlite_engine, tmp_path
):
    destination = create_backup(tmp_path / "daily.zip", sqlite_engine)
    manifest = verify_backup(destination)
    output_file = tmp_path / "github-output.txt"

    _append_github_outputs(output_file, destination, manifest)
    outputs = dict(
        line.split("=", 1)
        for line in output_file.read_text(encoding="utf-8").splitlines()
    )
    assert outputs["path"] == str(destination.resolve())
    assert outputs["encrypted_path"].endswith("daily.zip.enc")
    assert int(outputs["rows"]) >= 0

    assert record_cloud_backup_result(
        success=True,
        rows=int(outputs["rows"]),
        observed_at=outputs["created_at"],
        engine=sqlite_engine,
    )
    with sqlite_engine.connect() as connection:
        cloud = (
            connection.execute(
                text(
                    "SELECT status,last_success_at FROM source_health "
                    "WHERE source='github_backup'"
                )
            )
            .mappings()
            .one()
        )
    assert cloud["status"] == "online"
    assert cloud["last_success_at"] is not None


def test_verified_backup_restores_to_new_disposable_sqlite(sqlite_engine, tmp_path):
    with sqlite_engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO station_raw (time,temp_c,source,data_quality) "
                "VALUES ('2026-08-28T10:00:00Z',23.5,'test','ok')"
            )
        )
    archive = create_backup(tmp_path / "restore-source.zip", sqlite_engine)
    destination = tmp_path / "restored.sqlite"

    summary = restore_backup(archive, destination)

    assert summary["tables"] >= 20
    assert destination.exists()
    import sqlite3

    with sqlite3.connect(destination) as database:
        assert database.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert database.execute("SELECT temp_c FROM station_raw").fetchone()[0] == 23.5
