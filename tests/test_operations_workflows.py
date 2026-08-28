from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).parents[1]


def test_daily_backup_is_off_device_encrypted_and_keeps_thirty_copies():
    workflow = (ROOT / ".github/workflows/daily_backup.yml").read_text(encoding="utf-8")

    assert 'cron: "0 22 * * *"' in workflow
    assert 'timezone: "Europe/Rome"' in workflow
    assert "aes-256-cbc" in workflow
    assert "steps.backup.outputs.encrypted_path" in workflow
    assert "retention-days: 30" in workflow
    assert "actions: write" not in workflow
    assert "gh api --method DELETE" not in workflow
    assert "path: ${{ steps.backup.outputs.encrypted_path }}" in workflow
    assert "path: ${{ steps.backup.outputs.path }}" not in workflow


def test_health_check_runs_away_from_the_top_of_the_hour():
    workflow = (ROOT / ".github/workflows/health_check.yml").read_text(encoding="utf-8")

    assert 'cron: "17,47 * * * *"' in workflow
    assert "python health_check.py" in workflow
    assert "secrets.DATABASE_URL" in workflow
