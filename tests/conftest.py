from __future__ import annotations

import pytest

from db import ensure_schema, get_engine, reset_engine_cache


@pytest.fixture
def sqlite_engine(tmp_path, monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("SQLITE_PATH", str(tmp_path / "weather-test.db"))
    reset_engine_cache()
    ensure_schema()
    engine = get_engine()
    yield engine
    reset_engine_cache()
