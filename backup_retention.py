"""Keep the newly verified cloud backup and two verified predecessors."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
from collections.abc import Callable
from typing import Any

WORKFLOW_PATH = ".github/workflows/daily_backup.yml"
VERIFIED_STEPS = {
    "Create and verify portable backup",
    "Encrypt backup before leaving the runner",
    "Store encrypted backup on GitHub",
}


class BackupRetentionError(RuntimeError):
    """Safe error without token, response body or subprocess stderr."""


def github_api(path: str, method: str = "GET") -> Any:
    result = subprocess.run(
        ["gh", "api", "--method", method, path],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    if result.returncode:
        raise BackupRetentionError("API GitHub non disponibile: rotazione rinviata")
    try:
        return json.loads(result.stdout) if result.stdout.strip() else {}
    except json.JSONDecodeError as exc:
        raise BackupRetentionError("Risposta GitHub non valida") from exc


def rotate_backups(
    repository: str,
    artifact_id: int,
    run_id: int,
    digest: str,
    *,
    api: Callable[..., Any] = github_api,
    dry_run: bool = False,
) -> dict:
    """Delete only older backups after upload and predecessor checks succeed."""
    if not re.fullmatch(r"[\w.-]+/[\w.-]+", repository):
        raise ValueError("Repository non valido")
    if artifact_id <= 0 or run_id <= 0:
        raise ValueError("Identificativo backup non valido")
    digest = digest.removeprefix("sha256:")
    if not re.fullmatch(r"[0-9a-f]{64}", digest):
        raise ValueError("Checksum upload non valido")
    root = f"repos/{repository}/actions"
    artifacts = []
    # Read every page before deleting anything; deleting while paginating skips rows.
    for page in range(1, 1001):
        batch = api(f"{root}/artifacts?per_page=100&page={page}")["artifacts"]
        artifacts.extend(batch)
        if len(batch) < 100:
            break
    else:
        raise BackupRetentionError("Elenco incompleto: rotazione rinviata")

    backups = []
    for artifact in artifacts:
        match = re.fullmatch(r"meteo-db-\d{4}-\d{2}-\d{2}-(\d+)", artifact["name"])
        run = artifact.get("workflow_run", {})
        if (
            not match
            or artifact.get("expired")
            or artifact.get("size_in_bytes", 0) <= 0
            or run.get("head_branch") != "main"
            or int(match[1]) != run.get("id")
        ):
            continue
        details = api(f"{root}/runs/{run['id']}")
        if details.get("path", "").split("@", 1)[0] == WORKFLOW_PATH:
            backups.append(artifact)
    backups.sort(key=lambda value: (value["created_at"], value["id"]), reverse=True)
    if not backups or backups[0]["id"] != artifact_id:
        raise BackupRetentionError("Nuovo backup assente o superato: nessuna cancellazione")
    newest = backups[0]
    if (
        newest["workflow_run"]["id"] != run_id
        or newest.get("digest", "").removeprefix("sha256:") != digest
    ):
        raise BackupRetentionError("Nuovo backup non confermato: nessuna cancellazione")

    retained = [artifact_id]
    for artifact in backups[1:]:
        jobs = api(f"{root}/runs/{artifact['workflow_run']['id']}/jobs?per_page=100")
        verified = any(
            job.get("name") == "backup"
            and VERIFIED_STEPS
            <= {
                step["name"]
                for step in job.get("steps", [])
                if step.get("conclusion") == "success"
            }
            for job in jobs["jobs"]
        )
        if verified:
            retained.append(artifact["id"])
        if len(retained) == 3:
            break
    # Failed/missing predecessor evidence must not discard the only older copies.
    obsolete = (
        [artifact["id"] for artifact in backups if artifact["id"] not in retained]
        if len(retained) == 3
        else []
    )
    if not dry_run:
        for identifier in obsolete:
            api(f"{root}/artifacts/{identifier}", "DELETE")
    return {"kept": retained, "obsolete": obsolete, "dry_run": dry_run}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    try:
        result = rotate_backups(
            os.environ["GITHUB_REPOSITORY"],
            int(os.environ["BACKUP_ARTIFACT_ID"]),
            int(os.environ["GITHUB_RUN_ID"]),
            os.environ["BACKUP_ARTIFACT_DIGEST"],
            dry_run=args.dry_run,
        )
    except (BackupRetentionError, KeyError, ValueError, OSError, subprocess.TimeoutExpired):
        print("::error::Rotazione backup rinviata; copie protette conservate")
        return 2
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
