from __future__ import annotations

from config import Settings


def test_legacy_render_openweather_key_name_is_supported(monkeypatch):
    monkeypatch.delenv("OPENWEATHER_API_KEY", raising=False)
    monkeypatch.delenv("OWM_API_KEY", raising=False)
    monkeypatch.setenv("OW_API_KEY", "legacy-render-key")

    assert Settings.from_env().openweather_api_key == "legacy-render-key"


def test_rome_official_observation_defaults(monkeypatch):
    for name in (
        "OFFICIAL_OBSERVATIONS_ENABLED",
        "METAR_STATIONS",
        "OFFICIAL_SCORE_MAX_SHARE",
    ):
        monkeypatch.delenv(name, raising=False)

    configured = Settings.from_env()

    assert configured.official_observations_enabled is True
    assert configured.metar_station_ids == ("LIRF", "LIRA")
    assert configured.official_score_max_share == 0.20
