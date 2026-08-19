"""Database helpers shared by ingestion, scoring and the Streamlit UI.

The project supports PostgreSQL in production and SQLite for local development.
All timestamps are stored as UTC ISO-8601 strings so both backends behave in the
same way and existing installations can be migrated without destructive changes.
"""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine

load_dotenv()

_ENGINE_CACHE: dict[tuple[str, bool], Engine] = {}
_SCHEMA_READY: set[str] = set()


def get_db_url() -> str:
    """Return a SQLAlchemy URL, preferring PostgreSQL when configured."""
    url = (os.getenv("DATABASE_URL") or "").strip()
    if url and "USER:PASS@HOST:PORT/DBNAME" not in url:
        if url.startswith("postgres://"):
            url = url.replace("postgres://", "postgresql+psycopg://", 1)
        elif url.startswith("postgresql://"):
            url = url.replace("postgresql://", "postgresql+psycopg://", 1)
        return url

    sqlite_path = Path((os.getenv("SQLITE_PATH") or "data/weather.db").strip())
    sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    return f"sqlite:///{sqlite_path}"


def get_engine(echo: bool = False) -> Engine:
    """Return a reusable SQLAlchemy engine for the active environment."""
    db_url = get_db_url()
    key = (db_url, bool(echo))
    if key in _ENGINE_CACHE:
        return _ENGINE_CACHE[key]

    connect_args: dict[str, object] = {}
    if db_url.startswith("sqlite"):
        connect_args["check_same_thread"] = False

    engine = create_engine(
        db_url,
        echo=echo,
        pool_pre_ping=True,
        future=True,
        connect_args=connect_args,
    )
    _ENGINE_CACHE[key] = engine
    return engine


def reset_engine_cache() -> None:
    """Dispose cached engines (mainly useful for tests and local tooling)."""
    for engine in _ENGINE_CACHE.values():
        engine.dispose()
    _ENGINE_CACHE.clear()
    _SCHEMA_READY.clear()


def _schema_statements() -> list[str]:
    schema_path = Path(__file__).with_name("schema.sql")
    ddl = schema_path.read_text(encoding="utf-8")
    return [statement.strip() for statement in ddl.split(";") if statement.strip()]


def _additive_migrations(engine: Engine) -> None:
    """Add V3 observation columns to pre-existing V1/V2 databases."""
    additions = {
        "station_raw": {
            "rain_rate_mm_h": "REAL",
            "rain_total_mm": "REAL",
            "solar_w_m2": "REAL",
            "uv_index": "REAL",
            "source": "TEXT",
            "data_quality": "TEXT",
        },
        "station_3h": {
            "winddir": "REAL",
            "sample_count": "INTEGER",
        },
    }
    inspector = inspect(engine)
    tables = set(inspector.get_table_names())
    with engine.begin() as connection:
        for table, columns in additions.items():
            if table not in tables:
                continue
            existing = {
                column["name"].lower() for column in inspector.get_columns(table)
            }
            for name, sql_type in columns.items():
                if name.lower() not in existing:
                    connection.execute(
                        text(f"ALTER TABLE {table} ADD COLUMN {name} {sql_type}")
                    )


def ensure_schema() -> None:
    """Create all tables and safely extend databases made by older releases."""
    database_url = get_db_url()
    if database_url in _SCHEMA_READY:
        return
    engine = get_engine()
    with engine.begin() as connection:
        for statement in _schema_statements():
            connection.execute(text(statement))
    _additive_migrations(engine)
    _SCHEMA_READY.add(database_url)


def get_meta(key: str, default: str | None = None) -> str | None:
    ensure_schema()
    with get_engine().connect() as connection:
        value = connection.execute(
            text("SELECT v FROM meta WHERE k = :key"), {"key": key}
        ).scalar_one_or_none()
    return default if value is None else str(value)


def set_meta(key: str, value: object) -> None:
    ensure_schema()
    with get_engine().begin() as connection:
        connection.execute(
            text(
                "INSERT INTO meta (k, v) VALUES (:key, :value) "
                "ON CONFLICT (k) DO UPDATE SET v = excluded.v"
            ),
            {"key": key, "value": str(value)},
        )
