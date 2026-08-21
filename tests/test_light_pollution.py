from __future__ import annotations

import gzip
import math

import pytest

from light_pollution import (
    LightPollutionError,
    classify_light_pollution,
    decode_light_pollution_tile,
)


def test_lorenz_tile_decoding_returns_sqm_zone_and_approximate_bortle():
    raw = bytearray(360_001)
    raw[1] = 100

    estimate = decode_light_pollution_tile(
        gzip.compress(bytes(raw)),
        latitude=41.9,
        longitude=12.4,
    )

    expected_index = (5.0 / 195.0) * (math.exp(0.0195 * 100) - 1.0)
    assert estimate.lp_index == pytest.approx(expected_index)
    assert estimate.sqm == pytest.approx(
        22.0 - 2.5 * math.log10(1.0 + expected_index)
    )
    assert estimate.lp_zone == "2a"
    assert estimate.approximate_bortle == 3


@pytest.mark.parametrize(
    ("lp_index", "zone", "bortle"),
    [
        (0.0, "0", 1),
        (0.5, "3a", 4),
        (2.0, "4b", 5),
        (8.0, "5b", 6),
        (20.0, "6b", 7),
        (30.0, "7a", 8),
        (50.0, "7b", 9),
    ],
)
def test_light_pollution_classification(lp_index, zone, bortle):
    assert classify_light_pollution(lp_index) == (zone, bortle)


def test_atlas_rejects_coordinates_outside_its_coverage():
    with pytest.raises(LightPollutionError, match="latitudini"):
        decode_light_pollution_tile(b"", latitude=80, longitude=0)
