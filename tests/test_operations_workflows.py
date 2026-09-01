from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).parents[1]


def test_daily_backup_is_off_device_encrypted_and_keeps_thirty_copies():
    workflow = (ROOT / ".github/workflows/daily_backup.yml").read_text(encoding="utf-8")

    assert 'cron: "7 22 * * *"' in workflow
    assert 'timezone: "Europe/Rome"' in workflow
    assert "aes-256-cbc" in workflow
    assert "steps.backup.outputs.encrypted_path" in workflow
    assert "retention-days: 30" in workflow
    assert "actions: write" not in workflow
    assert "gh api --method DELETE" not in workflow
    assert "path: ${{ steps.backup.outputs.encrypted_path }}" in workflow
    assert "path: ${{ steps.backup.outputs.path }}" not in workflow
    assert "operations_alert.py --key daily-backup" in workflow


def test_health_check_runs_away_from_the_top_of_the_hour():
    workflow = (ROOT / ".github/workflows/health_check.yml").read_text(encoding="utf-8")

    assert 'cron: "17,47 * * * *"' in workflow
    assert "branches: [main]" in workflow
    assert "python health_check.py" in workflow
    assert "secrets.DATABASE_URL" in workflow
    assert "weather-app-v3-w2jd.onrender.com" in workflow
    assert "operations_alert.py --key health" in workflow


def test_monthly_restore_drill_uses_latest_encrypted_artifact_and_disposable_db():
    workflow = (ROOT / ".github/workflows/monthly_restore_drill.yml").read_text(
        encoding="utf-8"
    )

    assert 'cron: "23 4 1 * *"' in workflow
    assert "actions: read" in workflow
    assert "expired == false" in workflow
    assert "openssl enc -d -aes-256-cbc" in workflow
    assert "--restore-sqlite" in workflow
    assert "operations_alert.py --key restore-drill" in workflow
    assert "upload-artifact" not in workflow


def test_all_workflow_actions_are_pinned_to_immutable_commit_shas():
    import re

    workflows = list((ROOT / ".github/workflows").glob("*.yml"))
    references = []
    for workflow in workflows:
        references.extend(
            re.findall(
                r"uses:\s*[^\s@]+@([^\s#]+)", workflow.read_text(encoding="utf-8")
            )
        )

    assert references
    assert all(re.fullmatch(r"[0-9a-f]{40}", reference) for reference in references)


def test_live_ingestion_uses_one_ten_minute_cadence_and_passes_secondary_station():
    render = (ROOT / "render.yaml").read_text(encoding="utf-8")
    workflow = (ROOT / ".github/workflows/cloud_ingest.yml").read_text(
        encoding="utf-8"
    )

    assert 'schedule: "*/10 * * * *"' in render
    assert 'schedule: "*/5 * * * *"' not in render
    assert 'value: "10"' in render
    assert 'STATION_REFRESH_MINUTES: "10"' in workflow
    assert workflow.count("SECONDARY_ECOWITT_API_KEY:") == 2
    assert workflow.count("SECONDARY_ECOWITT_MAC:") == 2
    assert workflow.count("SECONDARY_STATION_LAT:") == 2


def test_secondary_location_is_supplied_as_a_secret_to_github_workflows():
    for name in ("cloud_ingest.yml", "cloud_backfill.yml"):
        workflow = (ROOT / ".github/workflows" / name).read_text(encoding="utf-8")
        for variable in (
            "SECONDARY_STATION_LAT",
            "SECONDARY_STATION_LON",
            "SECONDARY_STATION_ELEVATION_M",
        ):
            assert f"${{{{ secrets.{variable} }}}}" in workflow
            assert f"${{{{ vars.{variable} }}}}" not in workflow
