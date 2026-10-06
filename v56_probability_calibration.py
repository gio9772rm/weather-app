"""Causal, station-specific calibration of two-hour dry ensemble windows.

One fixed regularized logistic method, an untouched chronological holdout and
day-block uncertainty gates. Missing observations are never filled. Raw forecasts
remain archived alongside any prospective correction. No external requests.
"""

from __future__ import annotations

import hashlib
import json

import numpy as np
import pandas as pd
from sqlalchemy import text

from db import get_engine
from v5_calibration import utc

METHOD = "dry-2h-logistic-v1"


def calibration_key(station_id):
    return "window-calibration:v56:" + station_id


def _day(sample):
    return sample["start"].tz_convert("Europe/Rome").date()


def _logit(p):
    p = np.clip(np.asarray(p, dtype=float), 0.01, 0.99)
    return np.log(p / (1 - p))


def transform(p, coefficients):
    a, b = coefficients
    return 1 / (1 + np.exp(-np.clip(a + b * _logit(p), -30, 30)))


def fit(samples):
    """Shrink to the raw forecast; retain monotonicity without tuning on holdout."""
    x = np.column_stack((np.ones(len(samples)), _logit([s["raw"] for s in samples])))
    y = np.array([s["observed"] for s in samples], dtype=float)
    prior = np.array([0.0, 1.0])
    coefficients = prior.copy()
    penalty = 8.0

    def objective(c):
        z = x @ c
        return np.sum(np.logaddexp(0, z) - y * z) + penalty / 2 * np.sum(
            (c - prior) ** 2
        )

    for _ in range(40):
        p = 1 / (1 + np.exp(-np.clip(x @ coefficients, -30, 30)))
        gradient = x.T @ (p - y) + penalty * (coefficients - prior)
        hessian = x.T @ ((p * (1 - p))[:, None] * x) + penalty * np.eye(2)
        step = np.linalg.solve(hessian, gradient)
        if np.max(np.abs(step)) < 1e-7:
            break
        for scale in (1.0, 0.5, 0.25, 0.125, 0.0625):
            candidate = coefficients - scale * step
            candidate[0] = np.clip(candidate[0], -8, 8)
            candidate[1] = np.clip(candidate[1], 0.1, 5)
            if objective(candidate) <= objective(coefficients):
                coefficients = candidate
                break
        else:
            break
    return coefficients.tolist()


def _counts(samples):
    return {
        "n": len(samples),
        "days": len({_day(s) for s in samples}),
        "dry": sum(s["observed"] for s in samples),
        "wet": sum(1 - s["observed"] for s in samples),
        "wet_days": len({_day(s) for s in samples if not s["observed"]}),
    }


def _day_lower(samples, differences):
    """Fixed-seed 95% one-sided lower bound, resampling whole observed days."""
    days = sorted({_day(s) for s in samples})
    totals = np.array(
        [
            sum(d for s, d in zip(samples, differences, strict=True) if _day(s) == day)
            for day in days
        ]
    )
    counts = np.array([sum(_day(s) == day for s in samples) for day in days])
    draw = np.random.default_rng(5601).integers(0, len(days), (2000, len(days)))
    means = totals[draw].sum(axis=1) / counts[draw].sum(axis=1)
    return float(np.quantile(means, 0.05))


def evaluate_candidate(samples):
    """Train strictly before the first holdout acquisition (including the lead)."""
    samples = sorted(samples, key=lambda s: s["start"])
    days = sorted({_day(s) for s in samples})
    if len(days) < 30:
        return {"passed": False, "reason": "Servono almeno 30 giorni verificati."}, None
    test_days = set(days[-7:])
    test = [s for s in samples if _day(s) in test_days]
    first_known = min(s["known_at"] for s in test)
    train = [s for s in samples if _day(s) not in test_days and s["end"] < first_known]
    tc, vc = _counts(train), _counts(test)
    report = {
        "passed": False,
        "train": tc,
        "holdout": vc,
        "train_end": max((s["end"] for s in train), default=None),
        "holdout_first_known": first_known,
        "holdout_start": min(s["start"] for s in test),
        "holdout_end": max(s["end"] for s in test),
    }
    if (
        tc["n"] < 120
        or tc["days"] < 20
        or min(tc["dry"], tc["wet"]) < 20
        or vc["n"] < 50
        or min(vc["dry"], vc["wet"]) < 10
        or vc["wet_days"] < 2
    ):
        report["reason"] = (
            "La prova separata richiede 7 giorni, 50 finestre e almeno 10 casi per ciascun esito; anche l’apprendimento deve contenere entrambi gli esiti."
        )
        return report, None
    coefficients = fit(train)
    support = [min(s["raw"] for s in train), max(s["raw"] for s in train)]
    report["support"] = support
    raw = np.array([s["raw"] for s in test])
    corrected = transform(raw, coefficients)
    corrected = np.where((raw >= support[0]) & (raw <= support[1]), corrected, raw)
    observed = np.array([s["observed"] for s in test])
    climatology = np.full(len(test), np.mean([s["observed"] for s in train]))
    raw_loss, cal_loss, ref_loss = [
        (p - observed) ** 2 for p in (raw, corrected, climatology)
    ]
    raw_brier, cal_brier, ref_brier = [
        float(np.mean(v)) for v in (raw_loss, cal_loss, ref_loss)
    ]

    def log_loss(p):
        p = np.clip(p, 0.001, 0.999)
        return float(-np.mean(observed * np.log(p) + (1 - observed) * np.log(1 - p)))

    lower_raw = _day_lower(test, raw_loss - cal_loss)
    lower_ref = _day_lower(test, ref_loss - cal_loss)
    report.update(
        {
            "raw_brier": raw_brier,
            "calibrated_brier": cal_brier,
            "reference_brier": ref_brier,
            "raw_log_loss": log_loss(raw),
            "calibrated_log_loss": log_loss(corrected),
            "lower_benefit_raw": lower_raw,
            "lower_benefit_reference": lower_ref,
            "improvement_percent": 100 * (raw_brier - cal_brier) / raw_brier
            if raw_brier
            else 0,
        }
    )
    report["passed"] = bool(
        raw_brier > 0
        and ref_brier > 0
        and cal_brier <= 0.9 * min(raw_brier, ref_brier)
        and log_loss(corrected) <= log_loss(raw)
        and lower_raw > 0
        and lower_ref > 0
    )
    report["reason"] = (
        "Miglioramento confermato sui giorni tenuti separati."
        if report["passed"]
        else "Il miglioramento richiesto non è confermato; restano le frequenze originali."
    )
    return report, coefficients


def assess(station_id, samples, now, previous=None):
    """State machine: collect, prove, activate; fail back to raw on drift/expiry."""
    from v53_windows import sample_readiness

    now = utc(now)
    samples = [
        s
        for s in samples
        if s.get("model") and s["known_at"] <= now and s["end"] <= now
    ]
    # Never mix model families to pass a station's readiness gate.
    model = max(samples, key=lambda s: s["known_at"])["model"] if samples else None
    scoped = [s for s in samples if s["model"] == model]
    readiness = sample_readiness(
        {s["start"]: (s["raw"], s["observed"]) for s in scoped}
    )
    state = {
        "station_id": station_id,
        "model": model,
        "method": METHOD,
        "evaluated_at": now,
        "status": "collecting",
        "applied": False,
        "readiness": readiness,
        "reason": "Raccolta dei riscontri asciutti e piovosi; controllo automatico ogni sei ore.",
    }
    previous = previous or {}
    if previous.get("status") == "active" and previous.get("station_id") == station_id:
        if (
            previous.get("model") != model
            or previous.get("method") != METHOD
            or now >= utc(previous.get("expires_at"))
        ):
            return {
                **state,
                "status": "suspended",
                "reason": "Correzione scaduta o modello cambiato: si usano le frequenze originali.",
                "retry_after": now + pd.Timedelta(days=7),
            }
        prospective = [
            s
            for s in scoped
            if s.get("calibration_id") == previous.get("model_id")
            and s["known_at"] >= utc(previous["trained_at"])
            and s.get("calibrated") is not None
        ]
        monitor = _counts(prospective)
        if (
            monitor["n"] >= 50
            and monitor["days"] >= 7
            and min(monitor["dry"], monitor["wet"]) >= 10
        ):
            obs = np.array([s["observed"] for s in prospective])
            raw_loss = (np.array([s["raw"] for s in prospective]) - obs) ** 2
            cal_loss = (
                np.array([s["calibrated"] / 100 for s in prospective]) - obs
            ) ** 2
            monitor.update(
                {
                    "raw_brier": float(raw_loss.mean()),
                    "calibrated_brier": float(cal_loss.mean()),
                    "lower_deterioration": _day_lower(prospective, cal_loss - raw_loss),
                }
            )
            if (
                cal_loss.mean() > 1.05 * raw_loss.mean()
                and monitor["lower_deterioration"] > 0
            ):
                return {
                    **state,
                    "status": "suspended",
                    "monitor": monitor,
                    "reason": "I nuovi riscontri mostrano un peggioramento: ritorno alle frequenze originali.",
                    "retry_after": now + pd.Timedelta(days=7),
                }
        return {
            **previous,
            "evaluated_at": now,
            "readiness": readiness,
            "monitor": monitor,
        }
    if previous.get("retry_after") and now < utc(previous["retry_after"]):
        return {
            **state,
            "status": previous.get("status", "collecting"),
            "reason": previous.get("reason"),
            "retry_after": previous["retry_after"],
            "validation": previous.get("validation"),
        }
    if readiness["status"] != "ready_for_study":
        return state
    validation, coefficients = evaluate_candidate(scoped)
    state["validation"] = validation
    if not validation["passed"]:
        return {
            **state,
            "status": "waiting_validation",
            "reason": validation["reason"],
            "retry_after": now + pd.Timedelta(days=7),
        }
    # Deploy precisely the coefficients tested on the holdout, not a different
    # refit that has already consumed its own validation targets.
    identifier = hashlib.sha256(
        json.dumps(
            {
                "station": station_id,
                "model": model,
                "at": now.isoformat(),
                "coefficients": coefficients,
            },
            sort_keys=True,
        ).encode()
    ).hexdigest()[:20]
    return {
        **state,
        "status": "active",
        "applied": True,
        "model_id": identifier,
        "coefficients": coefficients,
        "trained_at": now,
        "data_end": validation["train_end"],
        "expires_at": now + pd.Timedelta(days=30),
        "support": validation["support"],
        "reason": "Correzione locale validata, applicata solo alle nuove finestre nel campo verificato.",
    }


def apply_to_rows(rows, station_id, model, acquired, calibration):
    """Preserve raw values; apply only the exact future event/lead/model/station."""
    acquired = utc(acquired)
    c = calibration or {}
    eligible = (
        c.get("status") == "active"
        and c.get("station_id") == station_id
        and c.get("model") == model
        and c.get("method") == METHOD
        and len(c.get("coefficients", [])) == 2
        and len(c.get("support", [])) == 2
    )
    if eligible:
        trained, evaluated, expires = [
            utc(c.get(k)) for k in ("trained_at", "evaluated_at", "expires_at")
        ]
        eligible = (
            all(pd.notna(t) for t in (trained, evaluated, expires))
            and trained <= acquired < expires
            and evaluated <= acquired
            and acquired - evaluated <= pd.Timedelta(hours=12)
        )
    for row in rows:
        raw = row.get("raw_probability", row.get("probability"))
        row["raw_probability"] = raw
        row["probability"] = raw
        row.pop("calibrated_probability", None)
        row.pop("calibration_id", None)
        if not eligible or raw is None or row.get("kind") != "dry":
            continue
        start, end = utc(row["start"]), utc(row["end"])
        lead = (start - acquired).total_seconds() / 3600
        if (
            end - start == pd.Timedelta(hours=2)
            and 6 <= lead < 12
            and np.isfinite(raw)
            and 0 <= raw <= 100
            and c["support"][0] <= raw / 100 <= c["support"][1]
        ):
            row["calibrated_probability"] = (
                float(transform(raw / 100, c["coefficients"])) * 100
            )
            row["probability"] = row["calibrated_probability"]
            row["calibration_id"] = c["model_id"]


def update_calibration(station_id, now=None):
    from v5_data import clean_json
    from v53_windows import collect_window_samples

    now = utc(now if now is not None else pd.Timestamp.now(tz="UTC"))
    with get_engine().connect() as con:
        saved = con.execute(
            text("SELECT payload FROM v5_products WHERE product_key=:key"),
            {"key": calibration_key(station_id)},
        ).scalar()
    samples, _ = collect_window_samples(station_id, now)
    return clean_json(
        assess(
            station_id,
            list(samples.values()),
            now,
            json.loads(saved) if saved else None,
        )
    )


def public_windows(product, calibration, now=None):
    """Suspend saved calibrated values immediately when a guard fails."""
    if not product:
        return product
    now = utc(now if now is not None else pd.Timestamp.now(tz="UTC"))
    c = calibration or {}
    evaluated, expires = utc(c.get("evaluated_at")), utc(c.get("expires_at"))
    valid = (
        c.get("status") == "active"
        and c.get("method") == METHOD
        and c.get("station_id") == product.get("station_id")
        and c.get("model") == product.get("model")
        and pd.notna(evaluated)
        and pd.notna(expires)
        and evaluated <= now < expires
        and now - evaluated <= pd.Timedelta(hours=12)
    )
    for row in product.get("rows", []):
        if row.get("calibration_id") and (
            not valid or row["calibration_id"] != c.get("model_id")
        ):
            row["probability"] = row.get("raw_probability")
            row.pop("calibrated_probability", None)
            row.pop("calibration_id", None)
    return product
