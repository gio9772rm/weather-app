from __future__ import annotations

import csv
import io

import numpy as np
import pandas as pd

from astronomy_planner import (
    custom_target,
    equatorial_altaz,
    equipment_profile,
    field_of_view,
    horizon_altitudes,
    observing_calendar_ics,
    observing_log_csv,
    parse_planner_configuration,
    plan_targets,
    planner_configuration_json,
    target_labels,
)
from config import Settings


def test_polar_object_stays_close_to_observer_latitude():
    times = pd.date_range("2026-08-28T20:00:00Z", periods=6, freq="h")
    altitude, _ = equatorial_altaz(
        37.95,
        89.264,
        times,
        latitude=41.9,
        longitude=12.5,
    )

    assert np.all((altitude > 40) & (altitude < 44))


def test_planner_ranks_selected_targets_without_exposing_coordinates():
    cfg = Settings.from_env()
    times = pd.date_range("2026-08-28T18:00:00Z", periods=24, freq="h")
    frame = pd.DataFrame(
        {
            "valid_time": times,
            "local_time": times.tz_convert(cfg.local_timezone),
            "is_night": [
                hour >= 20 or hour < 6
                for hour in times.tz_convert(cfg.local_timezone).hour
            ],
            "astro_score": 80,
            "clouds": 10,
            "dew_risk": 20,
        }
    )

    plan = plan_targets(frame, cfg, target_labels()[:3], minimum_altitude=10)

    assert set(plan["target"]) == {"M31", "M42", "M45"}
    assert plan["planner_score"].between(0, 100).all()
    assert "latitude" not in plan.columns
    assert "longitude" not in plan.columns


def test_custom_target_converts_ra_hours_and_validates_declination():
    target = custom_target(
        "IC 434",
        5.683,
        -2.45,
        magnitude=7.3,
        angular_width_arcmin=60,
        angular_height_arcmin=10,
    )

    assert target.ra_deg == 5.683 * 15
    assert target.dec_deg == -2.45
    assert target_labels([target])[-1] == "IC 434 · Oggetto personalizzato"

    with np.testing.assert_raises(ValueError):
        custom_target("oltre polo", 1.0, 91.0)


def test_equipment_field_of_view_and_sampling_use_effective_focal_length():
    profile = equipment_profile(
        name="Ridotto",
        telescope="Rifrattore",
        camera="APS-C",
        aperture_mm=80,
        focal_length_mm=500,
        sensor_width_mm=23.5,
        sensor_height_mm=15.6,
        pixel_size_um=3.76,
        focal_multiplier=0.8,
    )

    view = field_of_view(profile)

    assert view.effective_focal_length_mm == 400
    assert 3.3 < view.width_deg < 3.4
    assert 2.2 < view.height_deg < 2.3
    assert 1.9 < view.image_scale_arcsec_px < 2.0


def test_horizon_mask_is_circular_and_can_reject_an_otherwise_visible_target():
    interpolated = horizon_altitudes(
        np.asarray([337.5, 0.0, 22.5, 90.0]),
        {0: 20, 45: 0, 90: 10, 315: 40},
    )

    assert np.allclose(interpolated, [30, 20, 10, 10])

    cfg = Settings.from_env()
    times = pd.date_range("2026-08-28T18:00:00Z", periods=24, freq="h")
    frame = pd.DataFrame(
        {
            "valid_time": times,
            "local_time": times.tz_convert(cfg.local_timezone),
            "is_night": True,
            "astro_score": 80,
            "clouds": 10,
            "dew_risk": 20,
        }
    )
    target = custom_target("Mascherato", 2.5, 89.0)
    plan = plan_targets(
        frame,
        cfg,
        ["Mascherato · Oggetto personalizzato"],
        minimum_altitude=10,
        custom_targets=[target],
        horizon_mask={direction: 60 for direction in range(0, 360, 45)},
    )

    assert plan.iloc[0]["status"] == "Dietro ostacolo locale"
    assert "horizon_clearance" in plan


def test_calendar_ics_preserves_local_start_as_utc_and_contains_no_coordinates():
    payload = observing_calendar_ics(
        "M31",
        pd.Timestamp("2026-08-29T22:00:00", tz="Europe/Rome"),
        duration_minutes=120,
        created_at=pd.Timestamp("2026-08-29T12:00:00Z"),
    ).decode("utf-8")

    assert "DTSTART:20260829T200000Z" in payload
    assert "DTEND:20260829T220000Z" in payload
    assert "X-WR-TIMEZONE:Europe/Rome" in payload
    assert "41." not in payload
    assert "12." not in payload
    assert payload.endswith("\r\n")


def test_observing_log_csv_neutralises_spreadsheet_formulas():
    payload = observing_log_csv(
        [
            {
                "target": "M42",
                "planned_start": "2026-08-29T22:00:00+02:00",
                "duration_minutes": 120,
                "status": "Completata",
                "score": 82,
                "equipment": "Setup",
                "notes": "=HYPERLINK('bad')",
            }
        ]
    ).decode("utf-8-sig")
    row = next(csv.DictReader(io.StringIO(payload)))

    assert row["notes"].startswith("'=")


def test_planner_configuration_round_trip_contains_no_terrestrial_coordinates():
    profile = equipment_profile(
        name="Setup",
        telescope="Ottica",
        camera="Camera",
        aperture_mm=80,
        focal_length_mm=400,
        sensor_width_mm=23.5,
        sensor_height_mm=15.6,
        pixel_size_um=3.76,
    )
    target = custom_target("IC 434", 5.683, -2.45)

    payload = planner_configuration_json([profile], [target], {0: 12, 90: 4})
    profiles, targets, horizon = parse_planner_configuration(payload)

    assert profiles["Setup"] == profile
    assert targets["IC 434"].ra_deg == target.ra_deg
    assert horizon == {0.0: 12.0, 90.0: 4.0}
    assert b'"latitude"' not in payload
    assert b'"longitude"' not in payload
