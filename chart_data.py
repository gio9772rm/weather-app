"""Pure helpers for separating observations, forecasts and data-loss fallbacks."""

from __future__ import annotations

from dataclasses import dataclass
from itertools import pairwise
from typing import Any

import pandas as pd

DEFAULT_GAP_TOLERANCE = pd.Timedelta(minutes=30)


@dataclass(frozen=True)
class ForecastGap:
    """A time interval where forecast values temporarily replace missing measures."""

    start: pd.Timestamp
    end: pd.Timestamp
    points: pd.DataFrame


def _utc(value: Any) -> pd.Timestamp:
    return pd.to_datetime(value, utc=True, errors="coerce")


def _prepared_forecast(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or "valid_time" not in frame:
        return pd.DataFrame(columns=frame.columns)
    output = frame.copy()
    output["valid_time"] = pd.to_datetime(
        output["valid_time"], utc=True, errors="coerce"
    )
    return (
        output.dropna(subset=["valid_time"])
        .drop_duplicates("valid_time", keep="last")
        .sort_values("valid_time")
    )


def _interpolated_row(frame: pd.DataFrame, moment: pd.Timestamp) -> pd.Series | None:
    """Return a forecast row at ``moment`` using time-linear numeric interpolation."""
    if frame.empty:
        return None
    exact = frame[frame["valid_time"] == moment]
    if not exact.empty:
        return exact.iloc[-1].copy()
    before = frame[frame["valid_time"] < moment].tail(1)
    after = frame[frame["valid_time"] > moment].head(1)
    if before.empty or after.empty:
        return None
    left, right = before.iloc[0], after.iloc[0]
    span = (right["valid_time"] - left["valid_time"]).total_seconds()
    if span <= 0:
        return None
    ratio = (moment - left["valid_time"]).total_seconds() / span
    row = right.copy()
    for column in frame.columns:
        if column == "valid_time":
            continue
        numeric_column = pd.to_numeric(frame[column], errors="coerce")
        if numeric_column.notna().sum() < 2:
            continue
        left_value = pd.to_numeric(
            pd.Series([left.get(column)]), errors="coerce"
        ).iloc[0]
        right_value = pd.to_numeric(
            pd.Series([right.get(column)]), errors="coerce"
        ).iloc[0]
        if pd.notna(left_value) and pd.notna(right_value):
            if column == "wind_dir":
                angle_delta = (float(right_value) - float(left_value) + 180) % 360 - 180
                row[column] = (float(left_value) + angle_delta * ratio) % 360
            else:
                row[column] = float(left_value) + (
                    float(right_value) - float(left_value)
                ) * ratio
    row["valid_time"] = moment
    return row


def clip_forecast(
    frame: pd.DataFrame,
    start: Any,
    end: Any,
    *,
    add_boundaries: bool = True,
) -> pd.DataFrame:
    """Clip forecasts to a closed UTC interval and optionally interpolate its edges."""
    data = _prepared_forecast(frame)
    start_time, end_time = _utc(start), _utc(end)
    if data.empty or pd.isna(start_time) or pd.isna(end_time) or end_time < start_time:
        return data.iloc[0:0].copy()
    pieces = [
        data[
            (data["valid_time"] >= start_time)
            & (data["valid_time"] <= end_time)
        ]
    ]
    if add_boundaries:
        for moment in (start_time, end_time):
            row = _interpolated_row(data, moment)
            if row is not None:
                pieces.append(pd.DataFrame([row], columns=data.columns))
    return (
        pd.concat(pieces, ignore_index=True)
        .drop_duplicates("valid_time", keep="last")
        .sort_values("valid_time")
        .reset_index(drop=True)
    )


def observation_gap_intervals(
    station: pd.DataFrame,
    value_column: str,
    start: Any,
    end: Any,
    *,
    tolerance: pd.Timedelta = DEFAULT_GAP_TOLERANCE,
) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    """Return intervals where a station variable has no sufficiently recent sample."""
    start_time, end_time = _utc(start), _utc(end)
    if pd.isna(start_time) or pd.isna(end_time) or end_time <= start_time:
        return []
    if station.empty or "time" not in station or value_column not in station:
        return [(start_time, end_time)]
    times = pd.to_datetime(station["time"], utc=True, errors="coerce")
    values = pd.to_numeric(station[value_column], errors="coerce")
    valid = (
        pd.Series(times[values.notna()])
        .dropna()
        .loc[lambda series: series <= end_time]
        .drop_duplicates()
        .sort_values()
        .tolist()
    )
    if not valid:
        return [(start_time, end_time)]

    gaps: list[tuple[pd.Timestamp, pd.Timestamp]] = []
    first_in_window = next((moment for moment in valid if moment >= start_time), None)
    previous = max((moment for moment in valid if moment < start_time), default=None)
    if previous is None and first_in_window is not None:
        leading_end = min(first_in_window, end_time)
        if leading_end - start_time > tolerance:
            gaps.append((start_time, leading_end))

    relevant = [moment for moment in valid if moment >= start_time]
    if previous is not None:
        relevant.insert(0, previous)
    for left, right in pairwise(relevant):
        if right - left <= tolerance:
            continue
        gap_start = max(start_time, left)
        gap_end = min(end_time, right)
        if gap_end > gap_start:
            gaps.append((gap_start, gap_end))

    last = valid[-1]
    trailing_start = max(start_time, last)
    if end_time - last > tolerance and end_time > trailing_start:
        gaps.append((trailing_start, end_time))
    return merge_intervals(gaps)


def missing_forecast_segments(
    station: pd.DataFrame,
    forecast: pd.DataFrame,
    station_column: str,
    forecast_column: str,
    now: Any,
    *,
    lookback_hours: int = 3,
    tolerance: pd.Timedelta = DEFAULT_GAP_TOLERANCE,
) -> list[ForecastGap]:
    """Return coloured-fallback forecast pieces only where measurements are absent."""
    data = _prepared_forecast(forecast)
    now_time = _utc(now)
    if (
        data.empty
        or forecast_column not in data
        or pd.isna(now_time)
        or lookback_hours <= 0
    ):
        return []
    valid_values = pd.to_numeric(data[forecast_column], errors="coerce")
    data = data[valid_values.notna()].copy()
    if data.empty:
        return []
    window_start = max(
        now_time - pd.Timedelta(hours=lookback_hours), data["valid_time"].min()
    )
    gaps = observation_gap_intervals(
        station,
        station_column,
        window_start,
        now_time,
        tolerance=tolerance,
    )
    segments: list[ForecastGap] = []
    for start, end in gaps:
        points = clip_forecast(data, start, end)
        if len(points) >= 2:
            segments.append(ForecastGap(start=start, end=end, points=points))
    return segments


def merge_intervals(
    intervals: list[tuple[pd.Timestamp, pd.Timestamp]],
) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    """Merge overlapping time intervals while preserving chronological order."""
    if not intervals:
        return []
    ordered = sorted(intervals, key=lambda item: item[0])
    merged = [ordered[0]]
    for start, end in ordered[1:]:
        previous_start, previous_end = merged[-1]
        if start <= previous_end:
            merged[-1] = (previous_start, max(previous_end, end))
        else:
            merged.append((start, end))
    return merged


def latest_valid_measurements(
    frame: pd.DataFrame, columns: list[str]
) -> dict[str, pd.Timestamp | None]:
    """Return the latest timestamp containing a real value for every variable."""
    result: dict[str, pd.Timestamp | None] = {column: None for column in columns}
    if frame.empty or "time" not in frame:
        return result
    times = pd.to_datetime(frame["time"], utc=True, errors="coerce")
    for column in columns:
        if column not in frame:
            continue
        values = pd.to_numeric(frame[column], errors="coerce")
        valid_times = times[values.notna()].dropna()
        if not valid_times.empty:
            result[column] = valid_times.max()
    return result
