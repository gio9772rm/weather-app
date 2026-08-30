from __future__ import annotations

import json

import pandas as pd
import plotly.graph_objects as go

from chart_data import (
    clip_forecast,
    forecast_chart_window,
    latest_valid_measurements,
    missing_forecast_segments,
    observation_gap_intervals,
    plotly_local_datetime,
    plotly_local_datetimes,
)


def _forecast(start: str, periods: int = 8) -> pd.DataFrame:
    times = pd.date_range(start, periods=periods, freq="h", tz="UTC")
    return pd.DataFrame(
        {
            "valid_time": times,
            "temp_c": range(periods),
            "humidity": [60 + value for value in range(periods)],
            "wind_dir": [(350 + value * 20) % 360 for value in range(periods)],
        }
    )


def test_clip_forecast_starts_exactly_at_requested_boundary():
    forecast = _forecast("2026-08-21T08:00:00Z")
    now = pd.Timestamp("2026-08-21T10:30:00Z")

    future = clip_forecast(forecast, now, now + pd.Timedelta(hours=3))

    assert future.iloc[0]["valid_time"] == now
    assert future["valid_time"].min() >= now
    assert future.iloc[0]["temp_c"] == 2.5


def test_plotly_trace_and_now_marker_keep_same_rome_offset():
    points = plotly_local_datetimes(
        pd.Series(pd.to_datetime(["2026-08-24T10:00:00Z"], utc=True)),
        "Europe/Rome",
    )
    reference = plotly_local_datetime("2026-08-24T10:00:00Z", "Europe/Rome")
    figure = go.Figure(go.Scatter(x=points, y=[1]))
    figure.add_vline(x=reference)
    payload = json.loads(figure.to_json())

    assert reference.isoformat() == "2026-08-24T12:00:00+02:00"
    assert not isinstance(reference, (int, float))
    assert payload["layout"]["shapes"][0]["x0"] == payload["data"][0]["x"][0]


def test_plotly_local_datetimes_preserve_both_offsets_during_dst_fallback():
    points = plotly_local_datetimes(
        pd.to_datetime(["2026-10-25T00:30:00Z", "2026-10-25T01:30:00Z"], utc=True),
        "Europe/Rome",
    )

    assert points[0].isoformat() == "2026-10-25T02:30:00+02:00"
    assert points[1].isoformat() == "2026-10-25T02:30:00+01:00"


def test_chart_window_keeps_two_truthful_forecast_hours_before_now():
    forecast = _forecast("2026-08-21T08:00:00Z", periods=10)
    now = pd.Timestamp("2026-08-21T12:30:00Z")

    visible = forecast_chart_window(forecast, now, future_hours=3)

    assert visible.iloc[0]["valid_time"] == now - pd.Timedelta(hours=2)
    assert visible.iloc[-1]["valid_time"] == now + pd.Timedelta(hours=3)
    assert visible.iloc[0]["temp_c"] == 2.5


def test_complete_observations_never_create_a_forecast_fallback():
    now = pd.Timestamp("2026-08-21T12:00:00Z")
    station = pd.DataFrame(
        {
            "time": pd.date_range("2026-08-21T09:00:00Z", now, freq="10min", tz="UTC"),
            "temp_c": 25.0,
        }
    )

    segments = missing_forecast_segments(
        station,
        _forecast("2026-08-21T08:00:00Z"),
        "temp_c",
        "temp_c",
        now,
    )

    assert segments == []


def test_trailing_station_loss_is_returned_as_an_explicit_estimated_segment():
    now = pd.Timestamp("2026-08-21T12:30:00Z")
    station = pd.DataFrame(
        {
            "time": pd.date_range(
                "2026-08-21T09:30:00Z", periods=4, freq="20min", tz="UTC"
            ),
            "temp_c": [24.0, 24.2, 24.4, 24.5],
        }
    )

    segments = missing_forecast_segments(
        station,
        _forecast("2026-08-21T09:00:00Z"),
        "temp_c",
        "temp_c",
        now,
    )

    assert len(segments) == 1
    assert segments[0].start == station.iloc[-1]["time"]
    assert segments[0].end == now
    assert segments[0].points.iloc[0]["valid_time"] == segments[0].start
    assert segments[0].points.iloc[-1]["valid_time"] == now


def test_internal_measurement_hole_is_detected_from_last_to_next_real_sample():
    station = pd.DataFrame(
        {
            "time": pd.to_datetime(
                [
                    "2026-08-21T09:00:00Z",
                    "2026-08-21T09:10:00Z",
                    "2026-08-21T10:20:00Z",
                    "2026-08-21T10:30:00Z",
                ],
                utc=True,
            ),
            "humidity": [70, 71, 74, 73],
        }
    )

    gaps = observation_gap_intervals(
        station,
        "humidity",
        pd.Timestamp("2026-08-21T09:00:00Z"),
        pd.Timestamp("2026-08-21T10:30:00Z"),
    )

    assert gaps == [
        (
            pd.Timestamp("2026-08-21T09:10:00Z"),
            pd.Timestamp("2026-08-21T10:20:00Z"),
        )
    ]


def test_latest_valid_measurement_is_tracked_independently_per_sensor():
    frame = pd.DataFrame(
        {
            "time": pd.date_range(
                "2026-08-21T10:00:00Z", periods=3, freq="10min", tz="UTC"
            ),
            "temp_c": [25.0, None, None],
            "humidity": [60.0, 61.0, 62.0],
        }
    )

    latest = latest_valid_measurements(frame, ["temp_c", "humidity", "wind_kmh"])

    assert latest["temp_c"] == frame.iloc[0]["time"]
    assert latest["humidity"] == frame.iloc[-1]["time"]
    assert latest["wind_kmh"] is None
