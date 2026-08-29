"""Create one deduplicated GitHub issue for every operational workflow failure.

The helper deliberately sends only workflow metadata. Database URLs, provider
responses and exception text never leave the runner through this channel.
"""

from __future__ import annotations

import argparse
import json
import os
import re
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

API_ROOT = "https://api.github.com"
ALERT_TITLES = {
    "health": "Controllo salute",
    "daily-backup": "Backup giornaliero",
    "cloud-ingest": "Pipeline acquisizione",
    "restore-drill": "Prova ripristino mensile",
    "visual-regression": "Controllo visuale",
}


class OperationsAlertError(RuntimeError):
    """Safe operational-alert error without credentials or response bodies."""


def alert_title(key: str) -> str:
    """Return a stable title so repeated failures reuse the same issue."""
    normalized = str(key or "").strip().lower()
    if not re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,63}", normalized):
        raise ValueError("chiave avviso non valida")
    label = ALERT_TITLES.get(normalized, normalized.replace("-", " ").title())
    return f"[Meteo Ops] {label}"


def alert_body(
    key: str,
    status: str,
    run_url: str,
    *,
    checked_at: datetime | None = None,
) -> str:
    """Build a public-safe issue body from runner metadata only."""
    moment = checked_at or datetime.now(timezone.utc)
    normalized_status = str(status or "failure").strip().lower()
    outcome = "ripristinato" if normalized_status == "success" else "non riuscito"
    safe_url = run_url if run_url.startswith("https://github.com/") else ""
    lines = [
        "<!-- weather-app-operations-alert -->",
        f"Il controllo **{ALERT_TITLES.get(key, key)}** è {outcome}.",
        "",
        f"- Stato workflow: `{normalized_status}`",
        f"- Verifica UTC: `{moment.astimezone(timezone.utc).isoformat(timespec='seconds')}`",
    ]
    if safe_url:
        lines.append(f"- Esecuzione: {safe_url}")
    lines.extend(
        [
            "",
            (
                "L'avviso è deduplicato: i fallimenti successivi aggiornano questa "
                "stessa issue e il primo controllo riuscito la chiude automaticamente."
            ),
            "",
            "Per sicurezza non sono inclusi log grezzi, coordinate, credenziali o dati meteo.",
        ]
    )
    return "\n".join(lines)


def _request_json(
    url: str,
    *,
    token: str,
    method: str = "GET",
    payload: dict[str, Any] | None = None,
    opener: Callable[..., Any] = urlopen,
) -> Any:
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    request = Request(
        url,
        data=data,
        method=method,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "weather-app-operations-alert",
        },
    )
    try:
        with opener(request, timeout=20) as response:
            raw = response.read()
    except (HTTPError, URLError, OSError) as exc:
        raise OperationsAlertError("GitHub Issues non raggiungibile") from exc
    if not raw:
        return {}
    try:
        return json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise OperationsAlertError("Risposta GitHub Issues non valida") from exc


def _matching_issue(issues: Any, title: str) -> dict[str, Any] | None:
    if not isinstance(issues, list):
        return None
    return next(
        (
            issue
            for issue in issues
            if isinstance(issue, dict)
            and issue.get("title") == title
            and "pull_request" not in issue
        ),
        None,
    )


def sync_alert(
    *,
    repository: str,
    token: str,
    key: str,
    status: str,
    run_url: str = "",
    opener: Callable[..., Any] = urlopen,
) -> str:
    """Open/update an issue on failure and close it on recovery."""
    if not re.fullmatch(r"[^/\s]+/[^/\s]+", str(repository or "")):
        raise ValueError("repository GitHub non valido")
    if not token:
        raise ValueError("token GitHub mancante")
    normalized = str(status or "failure").strip().lower()
    title = alert_title(key)
    body = alert_body(key, normalized, run_url)
    issues_url = f"{API_ROOT}/repos/{repository}/issues"
    issues = _request_json(
        f"{issues_url}?state=all&per_page=100",
        token=token,
        opener=opener,
    )
    current = _matching_issue(issues, title)
    if normalized == "success":
        if not current or current.get("state") != "open":
            return "already-healthy"
        _request_json(
            f"{issues_url}/{int(current['number'])}",
            token=token,
            method="PATCH",
            payload={"state": "closed", "body": body},
            opener=opener,
        )
        return "closed"

    if current:
        _request_json(
            f"{issues_url}/{int(current['number'])}",
            token=token,
            method="PATCH",
            payload={"state": "open", "body": body},
            opener=opener,
        )
        return "reopened" if current.get("state") == "closed" else "updated"

    owner = repository.split("/", 1)[0]
    _request_json(
        issues_url,
        token=token,
        method="POST",
        payload={"title": title, "body": body, "assignees": [owner]},
        opener=opener,
    )
    return "created"


def main() -> int:
    parser = argparse.ArgumentParser(description="Avvisi operativi GitHub deduplicati")
    parser.add_argument("--key", required=True)
    parser.add_argument("--status", required=True)
    parser.add_argument("--run-url", default="")
    args = parser.parse_args()
    try:
        result = sync_alert(
            repository=os.getenv("GITHUB_REPOSITORY", ""),
            token=os.getenv("GITHUB_TOKEN", ""),
            key=args.key,
            status=args.status,
            run_url=args.run_url,
        )
    except (OperationsAlertError, ValueError) as exc:
        print(f"Avviso operativo non aggiornato: {exc}")
        return 2
    print(f"Avviso operativo: {result}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
