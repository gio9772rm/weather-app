from __future__ import annotations

import math

from terrain_horizon import (
    DEFAULT_AZIMUTHS,
    DEFAULT_DISTANCES_KM,
    combine_horizon_masks,
    destination_point,
    fetch_terrain_horizon,
    radial_samples,
    terrain_mask_from_elevations,
)


class _Response:
    ok = True
    status_code = 200

    def __init__(self, elevations):
        self._elevations = elevations

    def json(self):
        return {"elevation": self._elevations}


class _Session:
    def __init__(self, elevations):
        self.elevations = elevations
        self.params = None

    def get(self, _url, *, params, timeout):
        self.params = params
        assert timeout == (8, 30)
        return _Response(self.elevations)


def test_radial_sampling_stays_within_one_elevation_api_batch():
    samples = radial_samples(42.0, 12.0)

    assert len(samples) == len(DEFAULT_AZIMUTHS) * len(DEFAULT_DISTANCES_KM) == 96
    assert len({sample[0] for sample in samples}) == 16
    north = destination_point(42.0, 12.0, 0, 1)
    assert north[0] > 42.0
    assert math.isclose(north[1], 12.0, abs_tol=0.001)


def test_terrain_profile_selects_highest_apparent_obstacle_per_direction():
    samples = radial_samples(
        42.0,
        12.0,
        azimuths=(0.0, 90.0),
        distances_km=(1.0, 2.0),
    )
    elevations = [100.0, 300.0, 100.0, 100.0]

    mask, distances = terrain_mask_from_elevations(
        samples,
        elevations,
        local_dem_elevation_m=100.0,
        sensor_height_m=2.0,
    )

    assert mask[0.0] > 5
    assert distances[0.0] == 2.0
    assert mask[90.0] == 0


def test_fetch_returns_coordinate_free_result_and_combines_manual_mask():
    session = _Session([100.0] + [100.0] * 96)

    estimate = fetch_terrain_horizon(
        42.0,
        12.0,
        125.0,
        sensor_height_m=2.0,
        session=session,
    )
    combined = combine_horizon_masks(
        {0.0: 8.0, 90.0: 4.0, 180.0: 0.0, 270.0: 0.0}, estimate.mask
    )

    assert len(session.params["latitude"].split(",")) == 97
    assert not hasattr(estimate, "latitude")
    assert not hasattr(estimate, "longitude")
    assert combined[0.0] == 8.0
    assert combined[45.0] == 6.0
