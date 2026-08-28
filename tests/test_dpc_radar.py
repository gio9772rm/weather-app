from __future__ import annotations

import io
import struct
from dataclasses import replace

import numpy as np
import pandas as pd
import pytest
from PIL import Image
from sqlalchemy import inspect, text

from config import Settings
from dpc_radar import (
    DPC_ORIGIN,
    DpcRadarSnapshot,
    _decode_tile,
    _lightning,
    _local_window,
    _product_time,
    archive_dpc_radar_snapshot,
    parse_lightning_v2,
)


def _encoded_strike(latitude: float, longitude: float) -> bytes:
    lon_code = round((longitude + 180.0) / 360.0 * 262_144)
    lat_code = round((latitude + 90.0) / 180.0 * 262_144)
    high_bits = ((lon_code >> 16) & 3) << 6 | ((lat_code >> 16) & 3) << 4
    return struct.pack(">HHBB", lon_code & 0xFFFF, lat_code & 0xFFFF, high_bits, 0)


def test_lightning_v2_decodes_public_compact_coordinates():
    payload = b"\x02" + struct.pack(">H", 1) + _encoded_strike(41.9, 12.5)

    strikes = parse_lightning_v2(payload)

    assert len(strikes) == 1
    assert strikes[0][0] == pytest.approx(41.9, abs=0.002)
    assert strikes[0][1] == pytest.approx(12.5, abs=0.002)


class _TileResponse:
    ok = True
    status_code = 200

    def __init__(self, content: bytes) -> None:
        self.content = content


class _TileSession:
    def __init__(self, content: bytes) -> None:
        self.content = content
        self.urls: list[str] = []

    def get(self, url: str, **kwargs):
        del kwargs
        self.urls.append(url)
        return _TileResponse(self.content)


def test_radar_reads_only_tiles_intersecting_the_small_local_crop():
    image = Image.new("RGB", (256, 256), (51, 0, 0))
    payload = io.BytesIO()
    image.save(payload, format="WEBP", lossless=True)
    session = _TileSession(payload.getvalue())

    window = _local_window(
        "SRI",
        pd.Timestamp("2026-08-27T12:00:00Z"),
        41.9,
        12.5,
        10,
        session,
    )

    assert window.shape == (21, 21)
    assert window.mean() == pytest.approx(20.0, abs=0.5)
    assert 1 <= len(session.urls) <= 4
    assert all("/SRI/2026/08/27/1200/7/" in url for url in session.urls)


def test_radar_transparent_pixels_are_treated_as_no_data():
    pixels = np.zeros((256, 256, 4), dtype=np.uint8)
    pixels[..., 0] = 255
    pixels[..., 3] = 0
    pixels[128, 128] = (128, 0, 0, 255)
    image = Image.fromarray(pixels, mode="RGBA")
    payload = io.BytesIO()
    image.save(payload, format="WEBP", lossless=True)

    values = _decode_tile(payload.getvalue(), 100.0)

    assert values[0, 0] == 0.0
    assert values[128, 128] == pytest.approx(50.2, abs=0.5)


class _MetadataResponse:
    ok = True
    status_code = 200

    @staticmethod
    def json():
        return {"lastProducts": [{"time": 1_787_863_200_000}]}


class _MetadataSession:
    def __init__(self) -> None:
        self.request: tuple[str, dict] | None = None

    def get(self, url: str, **kwargs):
        self.request = (url, kwargs)
        return _MetadataResponse()


def test_radar_metadata_uses_the_current_official_web_origin():
    session = _MetadataSession()

    observed_at = _product_time("SRI", session)

    assert observed_at == pd.Timestamp("2026-08-27T20:40:00Z")
    assert session.request is not None
    _, kwargs = session.request
    assert kwargs["params"] == {"type": "SRI", "lang": "it"}
    assert kwargs["headers"] == {
        "Origin": DPC_ORIGIN,
        "Referer": f"{DPC_ORIGIN}/",
    }
    assert DPC_ORIGIN == "https://radar.protezionecivile.gov.it"


class _MissingLightningResponse:
    ok = False
    status_code = 403
    content = b""


class _MissingLightningSession:
    def __init__(self) -> None:
        self.requests: list[tuple[str, dict]] = []

    def get(self, url: str, **kwargs):
        self.requests.append((url, kwargs))
        return _MissingLightningResponse()


def test_lightning_never_substitutes_an_older_radar_frame():
    session = _MissingLightningSession()
    reference = pd.Timestamp("2026-08-27T20:40:00Z")

    observed_at, distances = _lightning(reference, 41.9, 12.5, session)

    assert observed_at is None
    assert distances == []
    assert len(session.requests) == 1
    url, kwargs = session.requests[0]
    assert url.endswith("/LGT/lgt_5min_1787863200000.bin")
    assert kwargs["headers"]["Origin"] == DPC_ORIGIN


def test_radar_archive_persists_summaries_never_pixels(sqlite_engine):
    cfg = replace(Settings.from_env(), station_id="test-primary")
    snapshot = DpcRadarSnapshot(
        station_id=cfg.station_id,
        observed_at=pd.Timestamp("2026-08-27T12:00:00Z"),
        sri_observed_at=pd.Timestamp("2026-08-27T12:00:00Z"),
        vmi_observed_at=pd.Timestamp("2026-08-27T11:55:00Z"),
        lightning_observed_at=pd.Timestamp("2026-08-27T12:00:00Z"),
        sri_point_mm_h=1.2,
        sri_mean_mm_h=0.4,
        sri_max_mm_h=4.8,
        sri_echo_fraction=0.25,
        vmi_point_dbz=28.0,
        vmi_max_dbz=42.0,
        lightning_10km=1,
        lightning_25km=2,
        lightning_50km=3,
        nearest_lightning_km=7.5,
        fetched_at=pd.Timestamp("2026-08-27T12:01:00Z"),
        sri_window=np.ones((21, 21)),
    )

    assert archive_dpc_radar_snapshot(snapshot, sqlite_engine) == 1

    columns = {
        column["name"] for column in inspect(sqlite_engine).get_columns("radar_local_snapshots")
    }
    assert not {"tile", "raster", "pixels", "lightning_coordinates"} & columns
    with sqlite_engine.connect() as connection:
        row = connection.execute(
            text("SELECT * FROM radar_local_snapshots")
        ).mappings().one()
    assert row["sri_point_mm_h"] == 1.2
    assert row["lightning_50km"] == 3
