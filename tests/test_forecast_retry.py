from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread

import pandas as pd
import pytest
from urllib3.exceptions import MaxRetryError
from urllib3.response import HTTPResponse

import forecast_providers
from config import Settings
from forecast_providers import build_session, fetch_all_forecasts


@pytest.mark.parametrize(
    "header",
    ["86400", format_datetime(datetime.now(UTC) + timedelta(days=1), usegmt=True)],
)
def test_long_retry_after_ends_the_request_without_sleeping(header, monkeypatch):
    sleeps = []
    monkeypatch.setattr("urllib3.util.retry.time.sleep", sleeps.append)
    with build_session() as session:
        retry = session.get_adapter("https://").max_retries
        response = HTTPResponse(status=429, headers={"Retry-After": header})
        # urllib3 clones the retry policy for every response; the bound must
        # survive increment(), rather than protecting only the initial object.
        retry = retry.increment(method="GET", url="/forecast", response=response)
        with pytest.raises(MaxRetryError, match="provider cooldown"):
            retry.sleep(response)
    assert sleeps == []


def test_short_provider_cooldown_is_respected(monkeypatch):
    sleeps = []
    monkeypatch.setattr("urllib3.util.retry.time.sleep", sleeps.append)
    with build_session() as session:
        retry = session.get_adapter("https://").max_retries
        retry.sleep(HTTPResponse(status=503, headers={"Retry-After": "5"}))
    assert sleeps == [5]


@pytest.mark.parametrize("status", [429, 503])
def test_long_provider_cooldown_preserves_other_forecasts(status, monkeypatch):
    requests_received = []
    sleeps = []

    class CooldownServer(BaseHTTPRequestHandler):
        def do_GET(self):
            requests_received.append(self.path)
            self.send_response(status)
            self.send_header("Retry-After", "86400")
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), CooldownServer)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    source_results = []
    frame = pd.DataFrame({"valid_time": [pd.Timestamp("2026-11-05T19:00:00Z")]})

    def unavailable_provider(_cfg, session):
        return forecast_providers._get_json(
            session,
            f"http://127.0.0.1:{server.server_port}/forecast",
            {},
            "ItaliaMeteo ICON-2I",
        )

    monkeypatch.setattr("urllib3.util.retry.time.sleep", sleeps.append)
    monkeypatch.setattr(forecast_providers, "fetch_icon_2i", unavailable_provider)
    monkeypatch.setattr(forecast_providers, "fetch_open_meteo", lambda *_: frame)
    monkeypatch.setattr(forecast_providers, "record_source_disabled", lambda *_: None)
    monkeypatch.setattr(
        forecast_providers,
        "record_source_result",
        lambda source, **result: source_results.append((source, result["success"])),
    )
    try:
        frames, errors = fetch_all_forecasts(
            replace(Settings.from_env(), openweather_api_key="")
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)

    assert len(frames) == 1
    pd.testing.assert_frame_equal(frames[0], frame)
    assert errors == ["ItaliaMeteo ICON-2I: servizio non raggiungibile"]
    assert source_results == [("open_meteo_icon2i", False), ("open_meteo", True)]
    assert requests_received == ["/forecast"]
    assert sleeps == []
