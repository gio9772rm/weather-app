"""Fail CI when the tracked tree contains common secrets or unsafe action refs."""

from __future__ import annotations

import re
import subprocess
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Finding:
    path: str
    rule: str


SECRET_PATTERNS = {
    "github-token": re.compile(
        r"\b(?:gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,})\b"
    ),
    "aws-access-key": re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b"),
    "private-key": re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    "database-credentials": re.compile(
        r"(?:postgres(?:ql)?|mysql|mariadb)://[^\s:/]+:[^\s@/]+@[^\s/]+",
        re.IGNORECASE,
    ),
}
PLACEHOLDER_WORDS = {
    "user",
    "pass",
    "password",
    "example",
    "host",
    "changeme",
    "xxxxx",
    "yyyyy",
}
ACTION_REFERENCE = re.compile(r"^\s*-?\s*uses:\s*[^\s@]+@([^\s#]+)", re.MULTILINE)
PRECISE_COORDINATE = re.compile(
    r"\b(?:lat|lon|latitude|longitude)\b[^\n]{0,36}?"
    r"(?P<value>[-+]?\d{1,2}\.\d{5,})",
    re.IGNORECASE,
)
PUBLIC_COORDINATE_VALUES = {"41.94889", "12.44056", "-85.05112878", "85.05112878"}


def _tracked_files(root: Path) -> list[Path]:
    result = subprocess.run(
        ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
        cwd=root,
        check=True,
        capture_output=True,
    )
    return [root / item.decode("utf-8") for item in result.stdout.split(b"\0") if item]


def _is_placeholder(match: str) -> bool:
    lowered = match.casefold()
    return any(word in lowered for word in PLACEHOLDER_WORDS)


def audit_repository(root: str | Path) -> list[Finding]:
    base = Path(root).resolve()
    findings: list[Finding] = []
    for path in _tracked_files(base):
        relative = path.relative_to(base).as_posix()
        if not path.is_file():
            continue
        payload = path.read_bytes()
        if b"\0" in payload:
            continue
        try:
            content = payload.decode("utf-8")
        except UnicodeDecodeError:
            continue
        for rule, pattern in SECRET_PATTERNS.items():
            if any(
                not _is_placeholder(match.group(0))
                for match in pattern.finditer(content)
            ):
                findings.append(Finding(relative, rule))
        for match in PRECISE_COORDINATE.finditer(content):
            if match.group("value") not in PUBLIC_COORDINATE_VALUES:
                findings.append(Finding(relative, "precise-coordinate"))
                break
        if relative.startswith(".github/workflows/"):
            for match in ACTION_REFERENCE.finditer(content):
                if not re.fullmatch(r"[0-9a-f]{40}", match.group(1)):
                    findings.append(Finding(relative, "mutable-action-reference"))
                    break
    render_path = base / "render.yaml"
    if render_path.exists():
        render_text = render_path.read_text(encoding="utf-8")
        private_variables = (
            "DATABASE_URL",
            "LAT",
            "LON",
            "ECOWITT_APPLICATION_KEY",
            "ECOWITT_API_KEY",
            "ECOWITT_MAC",
            "OPENWEATHER_API_KEY",
            "SECONDARY_STATION_LAT",
            "SECONDARY_STATION_LON",
            "SECONDARY_STATION_ELEVATION_M",
            "SECONDARY_ECOWITT_APPLICATION_KEY",
            "SECONDARY_ECOWITT_API_KEY",
            "SECONDARY_ECOWITT_MAC",
            "SECONDARY_STATION_DAILY_GZIP_B64",
        )
        for variable in private_variables:
            blocks = list(
                re.finditer(
                    rf"^\s{{6}}- key:\s*{re.escape(variable)}\s*$\n"
                    rf"(?P<body>(?:^\s{{8,}}[^\n]+\n?){{0,4}})",
                    render_text,
                    re.MULTILINE,
                )
            )
            if not blocks or any(
                "sync: false" not in block.group("body") for block in blocks
            ):
                findings.append(
                    Finding(
                        "render.yaml",
                        f"private-{variable.casefold().replace('_', '-')}-not-external",
                    )
                )
            if any(
                re.search(r"\bvalue:\s*\S", block.group("body")) for block in blocks
            ):
                findings.append(
                    Finding(
                        "render.yaml",
                        f"private-{variable.casefold().replace('_', '-')}-embedded",
                    )
                )
    return sorted(set(findings), key=lambda item: (item.path, item.rule))


def main() -> int:
    findings = audit_repository(Path(__file__).parent)
    if findings:
        for finding in findings:
            print(f"{finding.path}: {finding.rule}")
        return 1
    print("Privacy audit: tracked tree clean and action references immutable")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
