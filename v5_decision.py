"""Small decision summaries derived from already available forecasts and radar."""

import math

import pandas as pd
from sqlalchemy import text

CLOUD_LAYERS = ("cloud_high", "cloud_mid", "cloud_low", "clouds")
QUICK_FIELDS = ("clouds", "wind_gust_kmh", "rain_mm", "issued_at")


def timestamp(value):
    return pd.to_datetime(value, utc=True, errors="coerce")


def quick_check(snapshot, now, *, local=True):
    rows = [
        (timestamp(row.get("valid_time")), row) for row in snapshot.get("forecast", [])
    ]
    rows = sorted(
        ((at, row) for at, row in rows if pd.notna(at)), key=lambda item: item[0]
    )
    upcoming = []
    for i, (start, row) in enumerate(rows):
        end = start + pd.Timedelta(hours=row.get("interval_hours") or 1)
        if i + 1 < len(rows):
            end = min(end, rows[i + 1][0])
        if end <= max(start, now) or start >= now + pd.Timedelta(hours=3):
            continue
        upcoming.append(
            {
                "start": start,
                "end": end,
                **{k: row.get(k) for k in QUICK_FIELDS},
            }
        )
    radar = None
    if local:
        samples = [
            (timestamp(row.get("sri_observed_at")), row.get("sri_point_mm_h"))
            for row in snapshot.get("radar", [])
        ]
        samples = [(at, v) for at, v in samples if pd.notna(at) and at <= now]
        if samples:
            at, rate = max(samples, key=lambda row: row[0])
            radar = {"at": at, "rate_mm_h": rate, "source": "Radar DPC"}
        else:
            product = snapshot.get("radar_animation") or {}
            frames = [timestamp(row.get("time")) for row in product.get("frames", [])]
            valid = [at for at in frames if pd.notna(at) and at <= now]
            if valid:
                radar = {"at": max(valid), "source": "Animazione radar RainViewer"}
    return {
        "observed_at": snapshot.get("observed_at") if local else None,
        "snapshot_at": snapshot.get("generated_at"),
        "radar": radar,
        "upcoming": upcoming[:12],
    }


def cloud_disagreement(rows):
    """Compare only different source series for the same hour and cloud layer."""
    groups = {}
    for row in rows:
        model = (row["provider"], row["model"])
        if model[0] == "canonical":
            continue
        by_model = groups.setdefault(row["valid_time"], {})
        prior = by_model.get(model)
        if prior is None or row["issued_at"] > prior["issued_at"]:
            by_model[model] = row
    result = []
    for at, models in sorted(groups.items()):
        differences = []
        for field in CLOUD_LAYERS:
            values = [
                (float(row[field]), "/".join(model))
                for model, row in models.items()
                if row.get(field) is not None
                and math.isfinite(row[field])
                and 0 <= row[field] <= 100
            ]
            if len(values) < 2:
                continue
            low, high = min(values), max(values)
            differences.append(
                {
                    "valid_time": at,
                    "field": field,
                    "low": low[0],
                    "high": high[0],
                    "low_model": low[1],
                    "high_model": high[1],
                    "spread": round(high[0] - low[0], 1),
                }
            )
        if differences:
            result.append(max(differences, key=lambda row: row["spread"]))
    return result


def published_cloud_disagreement(now):
    # Only the main site's native-hour forecasts are in this table. The
    # secondary and worldwide sites must never use these series as substitutes.
    from db import get_engine

    def stamp(at):
        return at.strftime("%Y-%m-%dT%H:%M:%SZ")

    with get_engine().connect() as con:
        rows = (
            con.execute(
                text(
                    "SELECT provider,model,issued_at,valid_time,clouds,cloud_low,cloud_mid,cloud_high "
                    "FROM forecast_runs WHERE interval_hours=1 AND valid_time BETWEEN :start AND :end "
                    "AND issued_at BETWEEN :oldest AND :now AND fetched_at <= :now "
                    "ORDER BY issued_at DESC LIMIT 2048"
                ),
                {
                    "start": stamp(now.floor("h")),
                    "end": stamp(now + pd.Timedelta(hours=72)),
                    "oldest": stamp(now - pd.Timedelta(hours=12)),
                    "now": stamp(now),
                },
            )
            .mappings()
            .all()
        )
    return cloud_disagreement(rows)
