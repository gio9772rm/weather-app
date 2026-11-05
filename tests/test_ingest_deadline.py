from __future__ import annotations

import sqlite3
import subprocess
import sys

import pytest

from ingest_runtime import run_with_watchdog


def test_stalled_worker_exits_and_keeps_committed_observations(tmp_path):
    database = tmp_path / "deadline.sqlite"
    script = """
import sqlite3
import sys
import time
from ingest_runtime import run_with_watchdog

connection = sqlite3.connect(sys.argv[1])
connection.execute('CREATE TABLE observations (value INTEGER)')
connection.execute('INSERT INTO observations VALUES (1)')
connection.commit()
connection.execute('INSERT INTO observations VALUES (2)')
run_with_watchdog(lambda: time.sleep(10), timeout_seconds=0.1)
"""
    completed = subprocess.run(
        [sys.executable, "-c", script, str(database)],
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )
    assert completed.returncode == 124
    assert "durata massima" in completed.stderr
    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT value FROM observations").fetchall() == [(1,)]
        connection.execute("INSERT INTO observations VALUES (3)")


@pytest.mark.parametrize("fails", [False, True])
def test_watchdog_is_cancelled_after_normal_completion_or_error(fails):
    script = """
import sys
import time
from ingest_runtime import run_with_watchdog

def entrypoint():
    if sys.argv[1] == 'True':
        raise ValueError('ordinary failure')
    return 3

try:
    assert run_with_watchdog(entrypoint, timeout_seconds=0.1) == 3
except ValueError:
    pass
time.sleep(0.2)
print('watchdog cancelled')
"""
    completed = subprocess.run(
        [sys.executable, "-c", script, str(fails)],
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    assert "watchdog cancelled" in completed.stdout


@pytest.mark.parametrize("value", ["0", "-1", "nan", "inf", "invalid"])
def test_invalid_runtime_budget_fails_before_starting_work(value, monkeypatch):
    monkeypatch.setenv("INGEST_MAX_RUNTIME_SECONDS", value)
    with pytest.raises(ValueError):
        run_with_watchdog(lambda: pytest.fail("invalid deadline must not start work"))
