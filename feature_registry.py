"""V4 capability registry: active increments and deliberately dormant hooks."""

from __future__ import annotations

from dataclasses import dataclass

from config import Settings


@dataclass(frozen=True)
class Feature:
    key: str
    title: str
    phase: str
    enabled: bool
    storage: str


def features(cfg: Settings) -> tuple[Feature, ...]:
    """Declare rollout state in one place so future work does not fork the UI."""
    return (
        Feature(
            "forecast_change", "Cosa è cambiato", "V4.1", True, "forecast_blend_history"
        ),
        Feature(
            "ensemble",
            "Previsione probabilistica",
            "V4.1",
            cfg.ensemble_forecast_enabled,
            "forecast_ensemble_runs",
        ),
        Feature(
            "radar_nowcast",
            "Nowcast radar",
            "V4.1",
            cfg.radar_nowcast_enabled,
            "runtime cache",
        ),
        Feature(
            "observed_air",
            "Aria osservata EEA",
            "V4.1",
            cfg.eea_air_observations_enabled,
            "environment_observations",
        ),
        Feature(
            "climatology",
            "Climatologia e anomalie",
            "V4.2-ready",
            cfg.feature_climatology_enabled,
            "climate_normals",
        ),
        Feature(
            "measured_pollen",
            "Pollini misurati",
            "V4.2-ready",
            cfg.feature_measured_pollen_enabled,
            "environment_observations",
        ),
        Feature(
            "official_alerts",
            "Bollettini ufficiali in pagina",
            "V4.2-ready",
            cfg.feature_official_alerts_enabled,
            "official_alerts",
        ),
        Feature(
            "experience_mode",
            "Modalità semplice/esperta",
            "V4.2-ready",
            cfg.feature_experience_mode_enabled,
            "user_prefs",
        ),
    )
