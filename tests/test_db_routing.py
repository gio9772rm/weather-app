from __future__ import annotations

import logging
import os

import pytest
from sqlalchemy.engine import make_url

from db import get_db_url, get_engine, reset_engine_cache

INTERNAL = "dpg-testdatabase-a"
PUBLIC = INTERNAL + ".frankfurt-postgres.render.com"


@pytest.mark.parametrize("scheme", ["postgres", "postgresql", "postgresql+psycopg"])
def test_private_routing_preserves_credentials_and_original_secret(monkeypatch, scheme):
    original = (
        f"{scheme}://meteo:p%40ss%3Aword%2F%25@{PUBLIC}:5432/weather"
        "?application_name=ingest&connect_timeout=7&host=public-pool.example"
        "&hostaddr=192.0.2.1&sslmode=prefer"
    )
    monkeypatch.setenv("DATABASE_URL", original)
    monkeypatch.setenv("DATABASE_INTERNAL_HOST", INTERNAL)

    resolved = make_url(get_db_url())

    assert resolved.host == INTERNAL
    assert resolved.username == "meteo"
    assert resolved.password == "p@ss:word/%"
    assert resolved.database == "weather"
    assert resolved.port == 5432
    assert resolved.drivername == "postgresql+psycopg"
    assert resolved.query == {
        "application_name": "ingest",
        "connect_timeout": "7",
        "sslmode": "require",
    }
    # Backup encryption and external GitHub clients keep the same secret.
    assert os.environ["DATABASE_URL"] == original


def test_external_clients_keep_public_url_when_not_opted_in(monkeypatch):
    original = f"postgresql://meteo:password@{PUBLIC}/weather?sslmode=verify-full"
    monkeypatch.setenv("DATABASE_URL", original)
    monkeypatch.delenv("DATABASE_INTERNAL_HOST", raising=False)

    resolved = make_url(get_db_url())

    assert resolved.host == PUBLIC
    assert resolved.query["sslmode"] == "verify-full"


@pytest.mark.parametrize(
    "internal_host",
    ["dpg-otherdatabase-a", "dpg-testdatabase-a.attacker.example", "not a hostname"],
)
def test_private_routing_rejects_wrong_database_without_exposing_credentials(
    monkeypatch, internal_host
):
    monkeypatch.setenv("DATABASE_URL", f"postgresql://meteo:secret@{PUBLIC}/weather")
    monkeypatch.setenv("DATABASE_INTERNAL_HOST", internal_host)

    with pytest.raises(ValueError) as error:
        get_db_url()

    assert str(error.value) == "DATABASE_INTERNAL_HOST does not match DATABASE_URL"


def test_already_private_postgres_keeps_its_database(monkeypatch):
    monkeypatch.setenv(
        "DATABASE_URL", f"postgresql://meteo:password@{INTERNAL}/weather"
    )
    monkeypatch.setenv("DATABASE_INTERNAL_HOST", INTERNAL)

    resolved = make_url(get_db_url())

    assert resolved.host == INTERNAL
    assert resolved.database == "weather"
    assert resolved.query["sslmode"] == "require"


def test_sqlite_is_not_redirected(monkeypatch, tmp_path):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("SQLITE_PATH", str(tmp_path / "local.db"))
    monkeypatch.setenv("DATABASE_INTERNAL_HOST", INTERNAL)

    assert get_db_url() == f"sqlite:///{tmp_path / 'local.db'}"


def test_route_diagnostic_does_not_log_connection_details(monkeypatch, caplog):
    import db

    class FakeEngine:
        def dispose(self):
            pass

    monkeypatch.setenv("DATABASE_URL", f"postgresql://meteo:secret@{PUBLIC}/weather")
    monkeypatch.setenv("DATABASE_INTERNAL_HOST", INTERNAL)
    monkeypatch.setattr(db, "create_engine", lambda *args, **kwargs: FakeEngine())
    reset_engine_cache()
    try:
        with caplog.at_level(logging.INFO, logger="db"):
            get_engine()
            get_engine()
        assert caplog.text.count("Render private network") == 1
        assert "configured endpoint: public" in caplog.text
        for private_value in ("secret", "meteo", PUBLIC, INTERNAL):
            assert private_value not in caplog.text
    finally:
        reset_engine_cache()
