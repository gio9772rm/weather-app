from __future__ import annotations

import numpy as np
import pandas as pd

from astronomy_planner import equatorial_altaz, plan_targets, target_labels
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
