from __future__ import annotations

import json
import zipfile

from sqlalchemy import text

from backup_database import create_backup, verify_backup


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
    assert manifest["schema_version"] == 7
    assert manifest["tables"]["station_raw"]["rows"] == 1
    assert {
        "station_profiles",
        "station_observations",
        "forecast_regime_scores",
        "climate_reference_normals",
        "radar_local_snapshots",
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
        backup_health = connection.execute(
            text(
                "SELECT status,last_success_at,last_observation_at "
                "FROM source_health WHERE source='database_backup'"
            )
        ).mappings().one()
    assert backup_health["status"] == "online"
    assert backup_health["last_success_at"] is not None
    assert backup_health["last_observation_at"] is not None
