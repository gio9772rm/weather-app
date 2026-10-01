"""Public, station-isolated threshold verification from forecasts published in advance.

Evaluate all eligible hours, including those with no signal. This is a replay
of weather thresholds, not a claim about private push delivery or user rules.
"""

import json
import math
from datetime import datetime, timedelta, timezone

import pandas as pd
from sqlalchemy import text

from db import get_engine
from v5_calibration import observed_hourly, utc


def published_candidates(station_id, now):
    # Hundreds of archived runs contain repeated hours. Parse with datetime in
    # the streaming loop; retain only one row/hour instead of a wide DataFrame.
    stop = now.to_pydatetime()

    def stamp(value):
        try:
            at = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            return at.replace(tzinfo=timezone.utc) if at.tzinfo is None else at
        except (TypeError, ValueError):
            return None

    selected = {}
    with (
        get_engine()
        .connect()
        .execution_options(stream_results=True, max_row_buffer=100) as con
    ):
        rows = con.execute(
            text(
                "SELECT acquired_at,payload FROM location_model_runs "
                "WHERE station_id=:id AND basis='published' AND acquired_at>=:since"
            ),
            {"id": station_id, "since": (now - pd.Timedelta(days=31)).isoformat()},
        )
        for acquired, payload in rows:
            acquired = stamp(acquired)
            if acquired is None or acquired > stop:
                continue
            for row in json.loads(payload):
                target = stamp(row.get("valid_time"))
                issued = stamp(row.get("forecast_issued_at", row.get("issued_at")))
                if target is None or issued is None:
                    continue
                cutoff = target - timedelta(hours=3)
                if (
                    target.minute
                    or target.second
                    or target.microsecond
                    or not stop - timedelta(days=30) <= target <= stop
                    or not cutoff - timedelta(hours=6) <= acquired <= cutoff
                    or not acquired - timedelta(hours=12) <= issued <= acquired
                ):
                    continue
                old = selected.get(target)
                if old is None or acquired > old["known_at"]:
                    selected[target] = {
                        **row,
                        "valid_time": target,
                        "known_at": acquired,
                    }
    return list(selected.values())


def hourly_gust(observations):
    if observations.empty or not {"time", "windgust_kmh"}.issubset(observations):
        return pd.Series(dtype=float)
    frame = observations.copy()
    frame["time"] = utc(frame.time)
    frame = frame.dropna(subset=["time"]).sort_values("time").drop_duplicates("time")
    if "data_quality" in frame:
        frame = frame[
            ~frame.data_quality.fillna("").str.contains(
                "suspect|invalid|stuck", case=False
            )
        ]
    values = pd.to_numeric(frame.set_index("time").windgust_kmh, errors="coerce")
    values = values.where(values.between(0, 300))
    buckets = values.resample("5min", closed="right", label="right").max()
    hours = buckets.resample("h", closed="right", label="right")
    return hours.max().where(hours.count() == 12)


def number(value):
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (TypeError, ValueError):
        return None


def threshold_scores(forecasts, observations, now):
    hourly = observed_hourly(observations)
    rain = (
        hourly.set_index("time").rain_mm
        if "rain_mm" in hourly
        else pd.Series(dtype=float)
    )
    gust = hourly_gust(observations)
    results = []
    for kind, thresholds in (("rain", (30, 40, 60, 80)), ("wind", (30, 40, 60))):
        for threshold in thresholds:
            hits = false_alarms = misses = correct_negatives = missing_forecast = (
                missing_observations
            ) = 0
            times = []
            for row in forecasts:
                at = utc(row["valid_time"])
                predicted = number(
                    row.get("precip_probability" if kind == "rain" else "wind_gust_kmh")
                )
                rain_amount = number(row.get("rain_mm"))
                if (
                    predicted is None
                    or not 0 <= predicted <= (100 if kind == "rain" else 300)
                    or kind == "rain"
                    and (rain_amount is None or not 0 <= rain_amount <= 500)
                ):
                    missing_forecast += 1
                    continue
                measured = number((rain if kind == "rain" else gust).get(at))
                if measured is None:
                    missing_observations += 1
                    continue
                signal = predicted >= threshold and (kind != "rain" or rain_amount > 0)
                event = measured >= (0.1 if kind == "rain" else threshold)
                hits += int(signal and event)
                false_alarms += int(signal and not event)
                misses += int(not signal and event)
                correct_negatives += int(not signal and not event)
                times.append(at)
            n = len(times)
            results.append(
                {
                    "kind": kind,
                    "threshold": threshold,
                    "n": n,
                    "hits": hits,
                    "false_alarms": false_alarms,
                    "misses": misses,
                    "correct_negatives": correct_negatives,
                    "precision_percent": 100 * hits / (hits + false_alarms)
                    if hits + false_alarms
                    else None,
                    "recall_percent": 100 * hits / (hits + misses)
                    if hits + misses
                    else None,
                    "missing_forecast": missing_forecast,
                    "missing_observations": missing_observations,
                    "days": len({t.tz_convert("Europe/Rome").date() for t in times}),
                    "start": min(times) if times else None,
                    "end": max(times) if times else None,
                }
            )
    return {
        "evaluated_at": now,
        "period_days": 30,
        "candidate_hours": len(forecasts),
        "rows": results,
        "rules_changed": False,
        "note": "Simulazione delle soglie su ore già concluse negli ultimi 30 giorni; non è il registro delle notifiche consegnate. Una sola previsione pubblicata per ora, nota almeno due ore prima dell’inizio dell’intervallo e non oltre otto ore prima. Pioggia osservata ≥0,1 mm nell’ora; raffica massima oraria. Servono tutti i 12 campioni da cinque minuti, senza anomalie segnalate. Nessun dato mancante conta come assenza dell’evento. Le ore possono appartenere allo stesso episodio: un campione piccolo non dimostra la soglia migliore. Per il vento, cambiare soglia cambia anche l’evento osservato. Nessuna modifica alle tue notifiche.",
    }


def update_alert_verification(station_id, now=None):
    from data_access import load_station
    from v5_data import clean_json

    now = utc(now if now is not None else pd.Timestamp.now(tz="UTC"))
    forecasts = published_candidates(station_id, now)
    observations = load_station(24 * 31, station_id)
    return clean_json(threshold_scores(forecasts, observations, now))
