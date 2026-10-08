from __future__ import annotations

import json
import subprocess
import sys
from unittest.mock import Mock

import pytest

import backup_retention
from backup_retention import (
    VERIFIED_STEPS,
    WORKFLOW_PATH,
    BackupRetentionError,
    github_api,
    latest_verified_backup,
    rotate_backups,
)

DIGEST = "a" * 64


def artifact(identifier, *, branch="main", name=None, expired=False):
    return {
        "id": identifier,
        "name": name or f"meteo-db-2026-10-07-{identifier}",
        "expired": expired,
        "size_in_bytes": 1000,
        "created_at": f"2026-10-07T00:00:{identifier % 60:02d}Z",
        "digest": "sha256:" + DIGEST,
        "workflow_run": {"id": identifier, "head_branch": branch},
    }


def fake_api(artifacts, *, unverified=(), foreign=(), fail_path=None):
    calls = []

    def api(path, method="GET"):
        calls.append((path, method))
        if fail_path and fail_path in path:
            raise BackupRetentionError("GitHub non raggiungibile")
        if method == "DELETE":
            return {}
        if "artifacts?" in path:
            page = int(path.rsplit("=", 1)[1])
            return {"artifacts": artifacts[(page - 1) * 100 : page * 100]}
        run = int(path.split("/runs/")[1].split("/")[0])
        if "/jobs?" in path:
            return {
                "jobs": [
                    {
                        "name": "backup",
                        "steps": [
                            {
                                "name": step,
                                "conclusion": "failure"
                                if run in unverified
                                else "success",
                            }
                            for step in VERIFIED_STEPS
                        ],
                    }
                ]
            }
        return {
            "path": ".github/workflows/other.yml" if run in foreign else WORKFLOW_PATH
        }

    api.calls = calls
    return api


def deleted(api):
    return [
        int(path.rsplit("/", 1)[1]) for path, method in api.calls if method == "DELETE"
    ]


def test_retains_current_and_two_verified_predecessors_without_touching_other_artifacts():
    values = [artifact(i) for i in (1, 4, 2, 5, 3)]
    values.extend(
        [
            artifact(6, name="meteo-v5-visual-6"),
            artifact(7, branch="feature"),
            artifact(8, expired=True),
            artifact(9),  # Another workflow using a similar name.
        ]
    )
    api = fake_api(values, foreign={9})
    result = rotate_backups("owner/repo", 5, 5, DIGEST, api=api)
    assert result["kept"] == [5, 4, 3]
    assert deleted(api) == [2, 1]


@pytest.mark.parametrize("change", ["missing", "digest", "newer", "wrong-run", "empty"])
def test_missing_unconfirmed_or_superseded_upload_cannot_delete_anything(change):
    values = [artifact(i) for i in range(1, 6)]
    if change == "missing":
        values.pop()
    elif change == "digest":
        values[-1]["digest"] = "sha256:" + "b" * 64
    elif change == "newer":
        values.append(artifact(6))
    elif change == "wrong-run":
        values[-1]["workflow_run"]["id"] = 99
    elif change == "empty":
        values[-1]["size_in_bytes"] = 0
    api = fake_api(values)
    with pytest.raises(BackupRetentionError):
        rotate_backups("owner/repo", 5, 5, DIGEST, api=api)
    assert deleted(api) == []


def test_failed_validation_is_skipped_in_favour_of_an_older_verified_backup():
    api = fake_api([artifact(i) for i in range(1, 6)], unverified={4})
    result = rotate_backups("owner/repo", 5, 5, DIGEST, api=api)
    assert result["kept"] == [5, 3, 2]
    assert deleted(api) == [4, 1]


def test_not_enough_verified_predecessors_preserves_all_existing_copies():
    api = fake_api([artifact(i) for i in range(1, 6)], unverified={1, 2, 3})
    result = rotate_backups("owner/repo", 5, 5, DIGEST, api=api)
    assert result["kept"] == [5, 4]
    assert result["obsolete"] == []
    assert deleted(api) == []


def test_reads_every_page_before_deleting_older_backups():
    values = [artifact(i, name=f"test-{i}") for i in range(10, 110)]
    values.extend(artifact(i) for i in range(1, 6))
    api = fake_api(values)
    rotate_backups("owner/repo", 5, 5, DIGEST, api=api)
    second_page = next(
        i for i, (path, _) in enumerate(api.calls) if path.endswith("page=2")
    )
    first_delete = next(
        i for i, (_, method) in enumerate(api.calls) if method == "DELETE"
    )
    assert second_page < first_delete
    assert deleted(api) == [2, 1]


def test_api_failure_during_verification_prevents_any_deletion():
    api = fake_api([artifact(i) for i in range(1, 6)], fail_path="runs/4/jobs")
    with pytest.raises(BackupRetentionError):
        rotate_backups("owner/repo", 5, 5, DIGEST, api=api)
    assert deleted(api) == []


def test_dry_run_calculates_rotation_without_deleting_any_artifact():
    api = fake_api([artifact(i) for i in range(1, 6)])
    result = rotate_backups(
        "owner/repo", 5, 5, "sha256:" + DIGEST, api=api, dry_run=True
    )
    assert result["obsolete"] == [2, 1]
    assert deleted(api) == []


def test_failed_deletion_cannot_affect_any_of_the_three_retained_backups():
    api = fake_api([artifact(i) for i in range(1, 6)], fail_path="artifacts/2")
    with pytest.raises(BackupRetentionError):
        rotate_backups("owner/repo", 5, 5, DIGEST, api=api)
    assert deleted(api) == [2]


def test_subprocess_failure_does_not_expose_authentication_details(monkeypatch):
    run = Mock(
        return_value=subprocess.CompletedProcess([], 1, "", "Bearer private-token")
    )
    monkeypatch.setattr(subprocess, "run", run)
    with pytest.raises(BackupRetentionError) as error:
        github_api("repos/owner/repo/actions/artifacts")
    assert "private-token" not in str(error.value)
    assert run.call_args.kwargs["capture_output"] is True


def test_recovery_selects_latest_verified_main_backup_without_deleting_copies():
    values = [artifact(i) for i in range(1, 6)]
    values.extend(
        [
            artifact(6, branch="feature"),
            artifact(7, expired=True),
            artifact(8, name="meteo-db-unrelated"),
            artifact(9),
        ]
    )
    api = fake_api(values, unverified={5}, foreign={9})
    assert latest_verified_backup("owner/repo", api=api) == {
        "artifact_id": 4,
        "run_id": 4,
        "digest": "sha256:" + DIGEST,
    }
    assert all(method == "GET" for _, method in api.calls)


def test_recovery_can_find_verified_backup_beyond_first_artifact_page():
    values = [artifact(i, name=f"visual-{i}") for i in range(10, 110)]
    values.append(artifact(5))
    api = fake_api(values)
    assert latest_verified_backup("owner/repo", api=api)["artifact_id"] == 5
    assert any(path.endswith("artifacts?per_page=100&page=2") for path, _ in api.calls)
    assert deleted(api) == []


@pytest.mark.parametrize("digest", [None, "", "sha256:invalid", "sha512:" + DIGEST])
def test_recovery_skips_backups_without_a_usable_download_checksum(digest):
    values = [artifact(4), artifact(5)]
    values[-1]["digest"] = digest
    api = fake_api(values)
    assert latest_verified_backup("owner/repo", api=api)["artifact_id"] == 4
    assert deleted(api) == []


def test_recovery_does_not_accept_an_artifact_with_a_different_run_in_its_name():
    values = [artifact(4), artifact(5)]
    values[-1]["workflow_run"]["id"] = 99
    api = fake_api(values)
    assert latest_verified_backup("owner/repo", api=api)["artifact_id"] == 4


@pytest.mark.parametrize("mode", ["recovery", "retention"])
def test_verification_checks_jobs_beyond_first_page(mode):
    api = fake_api([artifact(i) for i in range(1, 6)])
    original = api

    def paginated(path, method="GET"):
        if "/runs/4/jobs?" in path:
            page = int(path.rsplit("=", 1)[1])
            if page == 1:
                original.calls.append((path, method))
                return {"jobs": [{"name": "unrelated", "steps": []}] * 100}
        return original(path, method)

    if mode == "recovery":
        # The newest artifact has not completed the three required steps.
        values = [artifact(i) for i in range(1, 6)]
        original = fake_api(values, unverified={5})
        result = latest_verified_backup("owner/repo", api=paginated)
        assert result["artifact_id"] == 4
    else:
        result = rotate_backups("owner/repo", 5, 5, DIGEST, api=paginated)
        assert result["kept"] == [5, 4, 3]
    assert any(
        path.endswith("runs/4/jobs?per_page=100&page=2") for path, _ in original.calls
    )


def test_recovery_reports_missing_verified_backup_without_changing_artifacts():
    api = fake_api([artifact(i) for i in range(1, 4)], unverified={1, 2, 3})
    with pytest.raises(BackupRetentionError, match="Nessun backup"):
        latest_verified_backup("owner/repo", api=api)
    assert deleted(api) == []


def test_recovery_does_not_fall_back_after_an_api_verification_error():
    api = fake_api([artifact(i) for i in range(1, 6)], fail_path="runs/5/jobs")
    with pytest.raises(BackupRetentionError):
        latest_verified_backup("owner/repo", api=api)
    assert deleted(api) == []


def test_recovery_rejects_an_incomplete_artifact_inventory():
    def api(path, method="GET"):
        return {"artifacts": [artifact(5)] * 100}

    with pytest.raises(BackupRetentionError, match="incompleto"):
        latest_verified_backup("owner/repo", api=api)


def test_recovery_cli_does_not_require_rotation_credentials_or_delete_artifacts(
    monkeypatch, capsys
):
    monkeypatch.setenv("GITHUB_REPOSITORY", "owner/repo")
    for key in ("BACKUP_ARTIFACT_ID", "GITHUB_RUN_ID", "BACKUP_ARTIFACT_DIGEST"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setattr(sys, "argv", ["backup_retention.py", "--latest-verified"])
    api = fake_api([artifact(5)])
    monkeypatch.setattr(
        backup_retention,
        "latest_verified_backup",
        lambda repository: latest_verified_backup(repository, api=api),
    )
    assert backup_retention.main() == 0
    assert json.loads(capsys.readouterr().out)["artifact_id"] == 5
    assert deleted(api) == []
