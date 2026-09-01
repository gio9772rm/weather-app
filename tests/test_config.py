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
        "ARSIAL_OBSERVATIONS_MODE",
        "ARSIAL_PROBE_HOURS",
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
    assert configured.arsial_observations_enabled is False
    assert configured.arsial_observations_mode == "auto"
    assert configured.arsial_probe_hours == 6
    assert configured.arsial_polling_enabled is True
    assert configured.arsial_auto_probe is True
    assert configured.arsial_station_name == "ROMA Lanciani-SEDE ARSIAL"
    assert configured.arsial_timezone == "UTC"
    assert configured.cfr_observations_enabled is True
    assert configured.cfr_observations_url == ""
    assert configured.cfr_meteohub_base_url == "https://meteohub.agenziaitaliameteo.it"
    assert configured.dpc_radar_enabled is True
    assert configured.reference_climatology_enabled is True
    assert configured.station_id == "roma-primary"


def test_arsial_mode_supports_explicit_disable_and_forced_enable(monkeypatch):
    monkeypatch.setenv("ARSIAL_OBSERVATIONS_ENABLED", "false")
    monkeypatch.setenv("ARSIAL_OBSERVATIONS_MODE", "disabled")
    assert Settings.from_env().arsial_polling_enabled is False

    monkeypatch.setenv("ARSIAL_OBSERVATIONS_ENABLED", "true")
    configured = Settings.from_env()
    assert configured.arsial_polling_enabled is True
    assert configured.arsial_auto_probe is False


def test_cfr_custom_endpoint_can_override_public_meteohub(monkeypatch):
    monkeypatch.setenv("CFR_OBSERVATIONS_ENABLED", "true")
    monkeypatch.setenv("CFR_OBSERVATIONS_URL", "https://example.test/cfr")
    monkeypatch.setenv("CFR_STATION_IDS", "37081, 13137")

    configured = Settings.from_env()

    assert configured.cfr_observations_enabled is True
    assert configured.cfr_observations_url == "https://example.test/cfr"
    assert configured.cfr_station_ids == ("37081", "13137")


def test_v42_daily_features_are_enabled_by_default(monkeypatch):
    for name in (
        "FEATURE_CLIMATOLOGY_ENABLED",
        "FEATURE_MEASURED_POLLEN_ENABLED",
        "FEATURE_OFFICIAL_ALERTS_ENABLED",
        "FEATURE_EXPERIENCE_MODE_ENABLED",
    ):
        monkeypatch.delenv(name, raising=False)

    configured = Settings.from_env()

    assert configured.feature_climatology_enabled is True
    assert configured.feature_measured_pollen_enabled is True
    assert configured.feature_official_alerts_enabled is True
    assert configured.feature_experience_mode_enabled is True


def test_secondary_station_is_opt_in_and_uses_dedicated_credentials(monkeypatch):
    monkeypatch.setenv("ECOWITT_APPLICATION_KEY", "shared-application")
    monkeypatch.setenv("SECONDARY_STATION_ENABLED", "true")
    monkeypatch.setenv("SECONDARY_STATION_ID", "secondary-one")
    monkeypatch.setenv("SECONDARY_STATION_NAME", "Stazione secondaria")
    monkeypatch.setenv("SECONDARY_STATION_LAT", "44.8")
    monkeypatch.setenv("SECONDARY_STATION_LON", "12.1")
    monkeypatch.setenv("SECONDARY_STATION_ELEVATION_M", "-1")
    monkeypatch.setenv("SECONDARY_ECOWITT_API_KEY", "secondary-api")
    monkeypatch.setenv("SECONDARY_ECOWITT_MAC", "00:11:22:33:44:55")

    configured = Settings.from_env()
    secondary = configured.secondary_station_settings()

    assert configured.has_secondary_station_credentials is True
    assert secondary is not None
    assert secondary.station_id == "secondary-one"
    assert secondary.location_name == "Stazione secondaria"
    assert secondary.elevation_m == -1
    assert secondary.ecowitt_application_key == "shared-application"
    assert secondary.ecowitt_api_key == "secondary-api"
    assert secondary.ecowitt_mac == "00:11:22:33:44:55"
    assert secondary.secondary_station_enabled is False


def test_refresh_defaults_are_never_faster_than_ten_minutes(monkeypatch):
    monkeypatch.setenv("STATION_REFRESH_MINUTES", "5")
    monkeypatch.setenv("DPC_RADAR_REFRESH_MINUTES", "5")

    configured = Settings.from_env()

    assert configured.station_refresh_minutes == 10
    assert configured.dpc_radar_refresh_minutes == 10
