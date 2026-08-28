from __future__ import annotations

import pandas as pd

from health_check import evaluate_health, health_endpoint, probe_app


def _sources(status: str = "online") -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "source": "ecowitt",
                "label": "Ecowitt",
                "enabled": True,
                "display_status": "online",
            },
            {
                "source": "forecast_blend",
                "label": "Previsione combinata",
                "enabled": True,
                "display_status": "online",
            },
            {
                "source": "eea_utd_air",
                "label": "EEA UTD",
                "enabled": True,
                "display_status": status,
            },
        ]
    )


def test_core_health_can_stay_available_while_optional_source_uses_cache():
    report = evaluate_health(
        {"station_status": "online", "forecast_status": "online"},
        _sources("cached"),
        app_reachable=True,
    )

    assert report.status == "degraded"
    assert not report.critical_failures
    assert report.exit_code == 0
    assert report.warnings == ("EEA UTD: cached",)


def test_stale_primary_measurements_make_health_check_fail():
    report = evaluate_health(
        {"station_status": "offline", "forecast_status": "online"},
        _sources(),
    )

    assert report.status == "unhealthy"
    assert report.exit_code == 1
    assert "Ecowitt" in report.critical_failures[0]


def test_health_endpoint_removes_query_and_reuses_streamlit_path():
    assert health_endpoint("https://example.test/app?token=secret") == (
        "https://example.test/app/_stcore/health"
    )
    assert health_endpoint("https://example.test/_stcore/health") == (
        "https://example.test/_stcore/health"
    )


def test_probe_app_accepts_success_without_closing_injected_session():
    class Response:
        status_code = 200

    class Session:
        def __init__(self):
            self.calls = []

        def get(self, url, timeout):
            self.calls.append((url, timeout))
            return Response()

    session = Session()
    ok, error = probe_app("https://example.test", session=session)

    assert ok
    assert not error
    assert session.calls == [("https://example.test/_stcore/health", (5, 20))]
