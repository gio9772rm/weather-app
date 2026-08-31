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
    framing_assessment,
    framing_geometry,
    horizon_altitudes,
    local_night_window,
    night_plan_csv,
    night_plan_tracks,
    observing_calendar_ics,
    observing_log_csv,
    parse_planner_configuration,
    plan_targets,
    planner_configuration_json,
    summarize_night_plan,
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


def test_framing_preview_respects_sensor_rotation_and_closes_outline():
    profile = equipment_profile(
        name="APS-C",
        telescope="Rifrattore",
        camera="Camera",
        aperture_mm=80,
        focal_length_mm=400,
        sensor_width_mm=23.5,
        sensor_height_mm=15.6,
        pixel_size_um=3.76,
    )
    target = custom_target(
        "Esteso",
        1.0,
        40.0,
        magnitude=5.0,
        angular_width_arcmin=190,
        angular_height_arcmin=60,
    )

    landscape = framing_assessment(target, profile, rotation_deg=0)
    portrait = framing_assessment(target, profile, rotation_deg=90)
    geometry = framing_geometry(target, profile, rotation_deg=30)

    assert landscape.fits is True
    assert portrait.fits is False
    assert landscape.width_fill_percent > 90
    assert geometry["sensor_x"][0] == geometry["sensor_x"][-1]
    assert geometry["sensor_y"][0] == geometry["sensor_y"][-1]


def test_local_night_window_crosses_midnight_and_preserves_dst_duration():
    start, end = local_night_window(
        pd.Timestamp("2026-10-24").date(),
        pd.Timestamp("20:00").time(),
        pd.Timestamp("06:00").time(),
        "Europe/Rome",
    )

    assert start.strftime("%Y-%m-%d %H:%M %z") == "2026-10-24 20:00 +0200"
    assert end.strftime("%Y-%m-%d %H:%M %z") == "2026-10-25 06:00 +0100"
    assert end.tz_convert("UTC") - start.tz_convert("UTC") == pd.Timedelta(hours=11)


def test_night_track_keeps_astronomy_when_weather_is_uncovered():
    cfg = Settings.from_env()
    target = custom_target("Polare", 2.5, 89.0, magnitude=2.0)
    start, end = local_night_window(
        pd.Timestamp("2026-08-31").date(),
        pd.Timestamp("20:00").time(),
        pd.Timestamp("22:00").time(),
        cfg.local_timezone,
    )

    tracks = night_plan_tracks(
        pd.DataFrame(),
        cfg,
        ["Polare · Oggetto personalizzato"],
        start=start,
        end=end,
        custom_targets=[target],
        sample_minutes=30,
    )
    summary = summarize_night_plan(tracks, custom_targets=[target])
    exported = night_plan_csv(tracks).decode("utf-8-sig")

    assert len(tracks) == 5
    assert tracks["altitude"].between(-90, 90).all()
    assert tracks["azimuth"].between(0, 360).all()
    assert tracks["weather_available"].eq(False).all()
    assert tracks["magnitude"].eq(2.0).all()
    assert summary.iloc[0]["status"] == "Pianificabile"
    assert summary.iloc[0]["weather_coverage"] == 0
    assert "latitude" not in exported.casefold()
    assert "longitude" not in exported.casefold()


def test_night_track_interpolates_covered_weather_in_local_time():
    cfg = Settings.from_env()
    start, end = local_night_window(
        pd.Timestamp("2026-08-31").date(),
        pd.Timestamp("20:00").time(),
        pd.Timestamp("22:00").time(),
        cfg.local_timezone,
    )
    times = pd.date_range(start.tz_convert("UTC"), end.tz_convert("UTC"), freq="h")
    astronomy = pd.DataFrame(
        {
            "valid_time": times,
            "astro_score": [60, 80, 70],
            "clouds": [40, 10, 20],
            "dew_risk": [30, 20, 10],
            "wind_kmh": [8, 6, 5],
        }
    )

    tracks = night_plan_tracks(
        astronomy,
        cfg,
        target_labels()[:2],
        start=start,
        end=end,
        sample_minutes=30,
    )

    assert len(tracks) == 10
    assert tracks["weather_available"].all()
    assert set(pd.to_datetime(tracks["local_time"]).dt.hour) == {20, 21, 22}
    assert tracks["clouds"].between(10, 40).all()
    assert "magnitude" in tracks


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
