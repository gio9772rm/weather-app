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
        "ARSIAL_OBSERVATIONS_ENABLED",
        "ARSIAL_STATION_NAME",
        "ARSIAL_TZ",
        "CFR_OBSERVATIONS_ENABLED",
        "CFR_OBSERVATIONS_URL",
    ):
        monkeypatch.delenv(name, raising=False)

    configured = Settings.from_env()

    assert configured.official_observations_enabled is True
    assert configured.metar_station_ids == ("LIRF", "LIRA")
    assert configured.official_score_max_share == 0.20
    assert configured.arsial_observations_enabled is True
    assert configured.arsial_station_name == "ROMA Lanciani-SEDE ARSIAL"
    assert configured.arsial_timezone == "UTC"
    assert configured.cfr_observations_enabled is False
    assert configured.cfr_observations_url == ""


def test_cfr_requires_explicit_activation(monkeypatch):
    monkeypatch.setenv("CFR_OBSERVATIONS_ENABLED", "true")
    monkeypatch.setenv("CFR_OBSERVATIONS_URL", "https://example.test/cfr")
    monkeypatch.setenv("CFR_STATION_IDS", "37081, 13137")

    configured = Settings.from_env()

    assert configured.cfr_observations_enabled is True
    assert configured.cfr_observations_url == "https://example.test/cfr"
    assert configured.cfr_station_ids == ("37081", "13137")
