from __future__ import annotations

import pandas as pd

from radar_nowcast import parse_nowcast_frames


def test_rainviewer_frames_are_validated_and_sorted():
    payload = {
        "radar": {
            "nowcast": [
                {"time": 1787739000, "path": "/late"},
                {"time": 1787738400, "path": "/early"},
                {"time": "bad", "path": "/bad"},
                {"time": 1787739600, "path": "https://unsafe.example"},
            ]
        }
    }
    frames = parse_nowcast_frames(payload)
    assert [item["path"] for item in frames] == ["/early", "/late"]
    assert all(isinstance(item["time"], pd.Timestamp) for item in frames)
