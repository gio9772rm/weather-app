"""The Render ingest worker intentionally has no web framework installed."""

import os
import subprocess
import sys


def test_personal_alerts_run_without_web_dependencies(tmp_path):
    environment = os.environ.copy()
    environment.pop("DATABASE_URL", None)
    environment["SQLITE_PATH"] = str(tmp_path / "ingest.sqlite")
    script = """
import importlib.abc
import sys

class WithoutWeb(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {'starlette', 'streamlit'}:
            raise ModuleNotFoundError('Web dependency absent from cron: ' + fullname)

sys.meta_path.insert(0, WithoutWeb())
from db import ensure_schema
from v53_alerts import refresh_plan_alerts
ensure_schema()
refresh_plan_alerts()
print('cron alerts ready')
"""
    completed = subprocess.run(
        [sys.executable, "-c", script],
        env=environment,
        text=True,
        capture_output=True,
        check=False,
        timeout=30,
    )
    assert completed.returncode == 0, completed.stderr
    assert "cron alerts ready" in completed.stdout
