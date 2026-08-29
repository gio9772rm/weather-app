from __future__ import annotations

import json

from operations_alert import alert_body, alert_title, sync_alert


class FakeResponse:
    def __init__(self, payload):
        self.payload = json.dumps(payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def read(self):
        return self.payload


class FakeOpener:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.requests = []

    def __call__(self, request, timeout):
        self.requests.append(
            {
                "url": request.full_url,
                "method": request.get_method(),
                "body": json.loads(request.data) if request.data else None,
                "timeout": timeout,
            }
        )
        return FakeResponse(self.responses.pop(0))


def test_failure_creates_one_public_safe_issue():
    opener = FakeOpener([], {"number": 12})

    result = sync_alert(
        repository="owner/weather-app",
        token="masked-token",
        key="daily-backup",
        status="failure",
        run_url="https://github.com/owner/weather-app/actions/runs/123",
        opener=opener,
    )

    assert result == "created"
    assert opener.requests[-1]["method"] == "POST"
    assert opener.requests[-1]["body"]["title"] == ("[Meteo Ops] Backup giornaliero")
    assert opener.requests[-1]["body"]["assignees"] == ["owner"]
    assert "masked-token" not in opener.requests[-1]["body"]["body"]


def test_recovery_closes_the_existing_open_issue():
    title = alert_title("health")
    opener = FakeOpener(
        [{"number": 7, "title": title, "state": "open"}],
        {"number": 7, "state": "closed"},
    )

    result = sync_alert(
        repository="owner/weather-app",
        token="masked-token",
        key="health",
        status="success",
        opener=opener,
    )

    assert result == "closed"
    assert opener.requests[-1]["method"] == "PATCH"
    assert opener.requests[-1]["body"]["state"] == "closed"


def test_alert_body_rejects_non_github_run_links():
    body = alert_body("health", "failure", "https://example.test/?secret=yes")

    assert "example.test" not in body
    assert "coordinate" in body
