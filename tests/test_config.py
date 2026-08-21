from __future__ import annotations

from config import Settings


def test_legacy_render_openweather_key_name_is_supported(monkeypatch):
    monkeypatch.delenv("OPENWEATHER_API_KEY", raising=False)
    monkeypatch.delenv("OWM_API_KEY", raising=False)
    monkeypatch.setenv("OW_API_KEY", "legacy-render-key")

    assert Settings.from_env().openweather_api_key == "legacy-render-key"
