from copy import deepcopy

import numpy as np
import pandas as pd
import pytest

from v56_probability_calibration import (
    METHOD,
    apply_to_rows,
    assess,
    evaluate_candidate,
    public_windows,
    transform,
)


def samples(days=30, start="2026-08-01"):
    times = pd.date_range(
        start, periods=days * 12, freq="2h", tz="Europe/Rome"
    ).tz_convert("UTC")
    return [
        {
            "start": t,
            "end": t + pd.Timedelta(hours=2),
            "known_at": t - pd.Timedelta(hours=7),
            "model": "icon_seamless",
            "raw": 0.8 if i % 2 else 0.35,
            "observed": i % 2,
            "calibrated": None,
            "calibration_id": None,
        }
        for i, t in enumerate(times)
    ]


def active():
    rows = samples()
    now = rows[-1]["end"] + pd.Timedelta(hours=1)
    return assess("roma", rows, now), now


def test_automatic_activation_uses_exact_tested_model_and_purges_future_targets():
    rows = samples()
    report, coefficients = evaluate_candidate(rows)
    assert report["passed"]
    assert report["train_end"] < report["holdout_first_known"]
    assert report["holdout"]["days"] == 7
    assert (
        report["calibrated_brier"]
        < min(report["raw_brier"], report["reference_brier"]) * 0.9
    )
    assert min(report["lower_benefit_raw"], report["lower_benefit_reference"]) > 0
    c, now = active()
    assert c["status"] == "active" and c["coefficients"] == coefficients
    assert c["trained_at"] == now and c["data_end"] < report["holdout_first_known"]
    assert np.all(np.diff(transform(np.linspace(0, 1, 101), coefficients)) >= 0)


def test_dry_only_insufficient_and_no_benefit_never_activate():
    now = pd.Timestamp("2026-09-05T00:00Z")
    dry = [{**s, "observed": 1} for s in samples()]
    assert assess("one", dry, now)["status"] == "collecting"
    assert assess("one", samples(days=9), now)["status"] == "collecting"
    perfect = [{**s, "raw": float(s["observed"])} for s in samples()]
    result = assess("one", perfect, now)
    assert result["status"] == "waiting_validation" and not result["applied"]
    assert not result["validation"]["passed"]


def test_future_data_and_other_model_cannot_pass_gates():
    now = pd.Timestamp("2026-08-10T00:00Z")
    assert assess("one", samples(), now)["status"] == "collecting"
    mixed = [
        {**s, "model": "other" if i < 12 * 22 else s["model"]}
        for i, s in enumerate(samples())
    ]
    assert (
        assess("one", mixed, pd.Timestamp("2026-09-05T00:00Z"))["status"]
        == "collecting"
    )


def forecast_rows(acquired):
    return [
        {
            "kind": kind,
            "start": acquired + pd.Timedelta(hours=lead),
            "end": acquired + pd.Timedelta(hours=lead + hours),
            "probability": 80,
        }
        for kind, lead, hours in (
            ("dry", 7, 2),
            ("dry", 13, 2),
            ("photo", 7, 3),
            ("dry", 7, 3),
        )
    ]


def test_only_prospective_validated_event_is_corrected_raw_is_preserved():
    c, now = active()
    rows = forecast_rows(now)
    apply_to_rows(rows, "roma", "icon_seamless", now, c)
    assert rows[0]["raw_probability"] == 80
    assert rows[0]["probability"] == rows[0]["calibrated_probability"] != 80
    assert rows[0]["calibration_id"] == c["model_id"]
    assert all(r["probability"] == 80 and "calibration_id" not in r for r in rows[1:])


@pytest.mark.parametrize(
    "station,model,offset,status",
    [
        ("comacchio", "icon_seamless", 0, "active"),
        ("roma", "other", 0, "active"),
        ("roma", "icon_seamless", -1, "active"),
        ("roma", "icon_seamless", 13, "active"),
        ("roma", "icon_seamless", 0, "suspended"),
    ],
)
def test_failed_guards_return_raw(station, model, offset, status):
    c, now = active()
    c["status"] = status
    acquired = now + pd.Timedelta(hours=offset)
    rows = forecast_rows(acquired)
    apply_to_rows(rows, station, model, acquired, c)
    assert all(r["probability"] == 80 and "calibration_id" not in r for r in rows)


def test_prospective_deterioration_suspends_and_expired_product_falls_back():
    c, activated = active()
    future = samples(
        days=8, start=(activated + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    )
    for s in future:
        s.update(
            raw=0.5, calibrated=5 if s["observed"] else 95, calibration_id=c["model_id"]
        )
    now = future[-1]["end"] + pd.Timedelta(hours=1)
    result = assess("roma", samples() + future, now, c)
    assert result["status"] == "suspended" and not result["applied"]
    assert result["monitor"]["lower_deterioration"] > 0
    rows = forecast_rows(activated)
    apply_to_rows(rows, "roma", "icon_seamless", activated, c)
    product = {"station_id": "roma", "model": "icon_seamless", "rows": rows}
    assert (
        public_windows(deepcopy(product), c, activated)["rows"][0]["probability"] != 80
    )
    assert (
        public_windows(deepcopy(product), result, now)["rows"][0]["probability"] == 80
    )
    assert (
        public_windows(deepcopy(product), c, activated + pd.Timedelta(hours=13))[
            "rows"
        ][0]["probability"]
        == 80
    )
    expired = {**c, "evaluated_at": activated + pd.Timedelta(days=31)}
    assert (
        public_windows(deepcopy(product), expired, activated + pd.Timedelta(days=31))[
            "rows"
        ][0]["probability"]
        == 80
    )


def test_expiry_and_changed_model_suspend_without_reusing_validation():
    c, activated = active()
    assert c["method"] == METHOD
    result = assess("roma", samples(), activated + pd.Timedelta(days=31), c)
    assert result["status"] == "suspended"
    changed = [{**s, "model": "icon_global"} for s in samples()]
    assert assess("roma", changed, activated, c)["status"] == "suspended"


def test_rain_verification_ignores_other_sensor_flags_but_keeps_strict_coverage():
    from v53_windows import window_rain_hourly

    frame = pd.DataFrame(
        {
            "time": pd.date_range("2026-09-01T00:05Z", periods=12, freq="5min"),
            "rain_mm": 0.0,
            "data_quality": "stuck_humidity;spike_temp_c",
        }
    )
    assert window_rain_hourly(frame).iloc[0].rain_mm == 0
    for flag in ("estimated_rain", "suspect", "invalid", "stuck_rain_mm"):
        bad = frame.copy()
        bad.loc[2, "data_quality"] = flag
        assert pd.isna(window_rain_hourly(bad).iloc[0].rain_mm)
    assert pd.isna(window_rain_hourly(frame.drop(index=2)).iloc[0].rain_mm)
