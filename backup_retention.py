"""Select verified recovery backups and retain the latest three copies."""

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
        raise BackupRetentionError("API GitHub non disponibile")
    try:
        return json.loads(result.stdout) if result.stdout.strip() else {}
    except json.JSONDecodeError as exc:
        raise BackupRetentionError("Risposta GitHub non valida") from exc


def _list_pages(root: str, collection: str, key: str, api: Callable[..., Any]) -> list:
    values = []
    for page in range(1, 1001):
        response = api(f"{root}/{collection}?per_page=100&page={page}")
        batch = response.get(key) if isinstance(response, dict) else None
        if not isinstance(batch, list) or any(
            not isinstance(item, dict) for item in batch
        ):
            raise BackupRetentionError("Elenco GitHub non valido")
        values.extend(batch)
        if len(batch) < 100:
            return values
    raise BackupRetentionError("Elenco GitHub incompleto")


def _backup_artifacts(repository: str, api: Callable[..., Any]) -> list:
    if not re.fullmatch(r"[\w.-]+/[\w.-]+", repository):
        raise ValueError("Repository non valido")
    root = f"repos/{repository}/actions"
    # Finish pagination before any deletion; modifying a page skips later rows.
    artifacts = _list_pages(root, "artifacts", "artifacts", api)
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
    return backups


def _has_verified_backup(root: str, run_id: int, api: Callable[..., Any]) -> bool:
    jobs = _list_pages(root, f"runs/{run_id}/jobs", "jobs", api)
    return any(
        job.get("name") == "backup"
        and VERIFIED_STEPS
        <= {
            step["name"]
            for step in job.get("steps", [])
            if step.get("conclusion") == "success"
        }
        for job in jobs
    )


def latest_verified_backup(
    repository: str, *, api: Callable[..., Any] = github_api
) -> dict:
    """Select a main-branch daily backup without deleting or downloading data."""
    backups = _backup_artifacts(repository, api)
    root = f"repos/{repository}/actions"
    for artifact in backups:
        digest = (artifact.get("digest") or "").removeprefix("sha256:")
        if not re.fullmatch(r"[0-9a-f]{64}", digest):
            continue
        run_id = artifact["workflow_run"]["id"]
        if _has_verified_backup(root, run_id, api):
            return {
                "artifact_id": artifact["id"],
                "run_id": run_id,
                "digest": "sha256:" + digest,
            }
    raise BackupRetentionError("Nessun backup giornaliero verificato disponibile")


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
    if artifact_id <= 0 or run_id <= 0:
        raise ValueError("Identificativo backup non valido")
    digest = digest.removeprefix("sha256:")
    if not re.fullmatch(r"[0-9a-f]{64}", digest):
        raise ValueError("Checksum upload non valido")
    root = f"repos/{repository}/actions"
    backups = _backup_artifacts(repository, api)
    if not backups or backups[0]["id"] != artifact_id:
        raise BackupRetentionError(
            "Nuovo backup assente o superato: nessuna cancellazione"
        )
    newest = backups[0]
    if (
        newest["workflow_run"]["id"] != run_id
        or newest.get("digest", "").removeprefix("sha256:") != digest
    ):
        raise BackupRetentionError("Nuovo backup non confermato: nessuna cancellazione")

    retained = [artifact_id]
    for artifact in backups[1:]:
        if _has_verified_backup(root, artifact["workflow_run"]["id"], api):
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
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--latest-verified", action="store_true")
    args = parser.parse_args()
    try:
        if args.latest_verified:
            result = latest_verified_backup(os.environ["GITHUB_REPOSITORY"])
        else:
            result = rotate_backups(
                os.environ["GITHUB_REPOSITORY"],
                int(os.environ["BACKUP_ARTIFACT_ID"]),
                int(os.environ["GITHUB_RUN_ID"]),
                os.environ["BACKUP_ARTIFACT_DIGEST"],
                dry_run=args.dry_run,
            )
    except (
        BackupRetentionError,
        KeyError,
        ValueError,
        OSError,
        subprocess.TimeoutExpired,
    ):
        message = (
            "Selezione backup per ripristino non riuscita; copie conservate"
            if args.latest_verified
            else "Rotazione backup rinviata; copie protette conservate"
        )
        print("::error::" + message)
        return 2
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
