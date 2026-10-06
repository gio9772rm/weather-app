from __future__ import annotations

import subprocess
from unittest.mock import Mock

import pytest

from backup_retention import (
    VERIFIED_STEPS,
    WORKFLOW_PATH,
    BackupRetentionError,
    github_api,
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
                                "conclusion": "failure" if run in unverified else "success",
                            }
                            for step in VERIFIED_STEPS
                        ],
                    }
                ]
            }
        return {"path": ".github/workflows/other.yml" if run in foreign else WORKFLOW_PATH}

    api.calls = calls
    return api


def deleted(api):
    return [int(path.rsplit("/", 1)[1]) for path, method in api.calls if method == "DELETE"]


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
    second_page = next(i for i, (path, _) in enumerate(api.calls) if path.endswith("page=2"))
    first_delete = next(i for i, (_, method) in enumerate(api.calls) if method == "DELETE")
    assert second_page < first_delete
    assert deleted(api) == [2, 1]


def test_api_failure_during_verification_prevents_any_deletion():
    api = fake_api([artifact(i) for i in range(1, 6)], fail_path="runs/4/jobs")
    with pytest.raises(BackupRetentionError):
        rotate_backups("owner/repo", 5, 5, DIGEST, api=api)
    assert deleted(api) == []


def test_dry_run_calculates_rotation_without_deleting_any_artifact():
    api = fake_api([artifact(i) for i in range(1, 6)])
    result = rotate_backups("owner/repo", 5, 5, "sha256:" + DIGEST, api=api, dry_run=True)
    assert result["obsolete"] == [2, 1]
    assert deleted(api) == []


def test_failed_deletion_cannot_affect_any_of_the_three_retained_backups():
    api = fake_api([artifact(i) for i in range(1, 6)], fail_path="artifacts/2")
    with pytest.raises(BackupRetentionError):
        rotate_backups("owner/repo", 5, 5, DIGEST, api=api)
    assert deleted(api) == [2]


def test_subprocess_failure_does_not_expose_authentication_details(monkeypatch):
    run = Mock(return_value=subprocess.CompletedProcess([], 1, "", "Bearer private-token"))
    monkeypatch.setattr(subprocess, "run", run)
    with pytest.raises(BackupRetentionError) as error:
        github_api("repos/owner/repo/actions/artifacts")
    assert "private-token" not in str(error.value)
    assert run.call_args.kwargs["capture_output"] is True
