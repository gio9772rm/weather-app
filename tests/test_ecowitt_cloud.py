from __future__ import annotations

from dataclasses import replace

import pytest

from config import Settings
from weather_ingest_ecowitt_cloud import EcowittError, ecowitt_get


class _ApiErrorResponse:
    ok = True

    @staticmethod
    def json():
        return {
            "code": 40011,
            "msg": "MAC AA:BB:CC:DD:EE:FF is not authorized for api_key api-secret",
        }


class _ApiErrorSession:
    @staticmethod
    def get(*args, **kwargs):
        return _ApiErrorResponse()


def test_ecowitt_api_error_keeps_diagnosis_but_redacts_identifiers():
    cfg = replace(
        Settings.from_env(),
        ecowitt_application_key="application-secret",
        ecowitt_api_key="api-secret",
        ecowitt_mac="AA:BB:CC:DD:EE:FF",
    )

    with pytest.raises(EcowittError) as raised:
        ecowitt_get("device/real_time", {"mac": cfg.ecowitt_mac}, cfg, _ApiErrorSession())

    message = str(raised.value)
    assert "API code 40011" in message
    assert "not authorized" in message
    assert "[redacted]" in message
    assert cfg.ecowitt_api_key not in message
    assert cfg.ecowitt_mac not in message
