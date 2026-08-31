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
            "V4.2",
            cfg.feature_climatology_enabled,
            "climate_normals",
        ),
        Feature(
            "measured_pollen",
            "Pollini misurati",
            "V4.2",
            cfg.feature_measured_pollen_enabled,
            "environment_observations",
        ),
        Feature(
            "official_alerts",
            "Bollettini ufficiali in pagina",
            "V4.2",
            cfg.feature_official_alerts_enabled,
            "official_alerts",
        ),
        Feature(
            "experience_mode",
            "Modalità semplice/esperta",
            "V4.2",
            cfg.feature_experience_mode_enabled,
            "URL / sessione",
        ),
        Feature(
            "icon_2i",
            "ICON-2I esplicito · 2,2 km",
            "V4.3",
            True,
            "forecast_runs",
        ),
        Feature(
            "calibration_v2",
            "Calibrazione per orizzonte e regime",
            "V4.3",
            True,
            "forecast_regime_scores",
        ),
        Feature(
            "dpc_radar_local",
            "Radar e fulmini DPC locali",
            "V4.3",
            cfg.dpc_radar_enabled,
            "radar_local_snapshots",
        ),
        Feature(
            "climate_reference",
            "Riferimento ERA5-Land 1991–2020",
            "V4.3",
            cfg.reference_climatology_enabled,
            "climate_reference_normals",
        ),
        Feature(
            "multistation",
            "Registro multi-stazione",
            "V4.3",
            True,
            "station_profiles + station_observations",
        ),
        Feature(
            "monthly_reports",
            "Rapporti mensili PDF/CSV",
            "V4.3",
            True,
            "download su richiesta",
        ),
        Feature(
            "astronomy_pro",
            "Astronomia Pro",
            "V4.3",
            True,
            "forecast_blend",
        ),
        Feature(
            "astronomy_planner_pro",
            "Planner RA/Dec, attrezzatura, orizzonte e calendario",
            "V4.5",
            True,
            "sessione browser + export ICS/CSV/JSON",
        ),
        Feature(
            "institutional_source_recovery",
            "Recupero automatico fonti istituzionali",
            "V4.6",
            True,
            "source_health + fallback CFR Lazio",
        ),
        Feature(
            "health_overview",
            "Salute automatica e continuità visibili",
            "V4.6",
            True,
            "source_health + GitHub Actions + Render",
        ),
        Feature(
            "contrast_contracts",
            "Contrasto WCAG dei controlli nativi",
            "V4.6",
            True,
            "contratti browser desktop/mobile",
        ),
        Feature(
            "astronomy_framing_atlas",
            "Campo inquadrato geometrico e atlante CDS opzionale",
            "V4.7",
            True,
            "sessione browser + CDS Aladin Lite su richiesta",
        ),
        Feature(
            "astronomy_night_tracks",
            "Piano notturno multi-target in ora locale",
            "V4.7",
            True,
            "forecast_blend + calcolo astronomico + CSV",
        ),
    )
