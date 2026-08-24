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

    assert manifest["tables"]["station_raw"]["rows"] == 1
    with zipfile.ZipFile(destination) as archive:
        assert {"manifest.json", "schema.sql", "station_raw.csv"} <= set(
            archive.namelist()
        )
        manifest_text = archive.read("manifest.json").decode("utf-8")
        parsed = json.loads(manifest_text)
        assert "database_url" not in manifest_text.casefold()
        assert parsed["database_dialect"] == "sqlite"
