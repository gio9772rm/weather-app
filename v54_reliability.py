"""Compare fresh, live-acquired models at identical times, without scoring skill."""

import json
import math
from collections import defaultdict

import pandas as pd
from sqlalchemy import text

from config import Settings
from db import get_engine


def model_spread(runs, now):
    """One latest value per model/hour; exclude research, stale and future runs."""
    now = pd.Timestamp(now)
    start = now.floor("h")
    latest = {}
    for run in runs:
        if run.get("basis") != "live":
            continue
        issued = pd.to_datetime(run.get("issued_at"), utc=True, errors="coerce")
        acquired = pd.to_datetime(run.get("acquired_at"), utc=True, errors="coerce")
        if pd.isna(issued) or pd.isna(acquired):
            continue
        if not now - pd.Timedelta(hours=12) <= issued <= acquired <= now:
            continue
        identity = (run.get("provider"), run.get("model"))
        if not all(identity) or identity[0] == "canonical":
            continue
        for row in run.get("rows", []):
            valid = pd.to_datetime(row.get("valid_time"), utc=True, errors="coerce")
            try:
                value = float(row.get("temp_c"))
            except (TypeError, ValueError):
                continue
            if (
                pd.isna(valid)
                or not start <= valid < start + pd.Timedelta(hours=12)
                or valid != valid.floor("h")
                or not math.isfinite(value)
                or not -60 <= value <= 60
            ):
                continue
            key = (*identity, valid)
            old = latest.get(key)
            if old is None or (issued, acquired) > old[:2]:
                latest[key] = (issued, acquired, value)
    common = defaultdict(list)
    for (provider, model, valid), (_, _, value) in latest.items():
        common[valid].append(value)
    rows = [
        {
            "valid_time": valid.isoformat(),
            "model_count": len(values),
            "spread_c": round(max(values) - min(values), 2),
        }
        for valid, values in sorted(common.items())
        if len(values) >= 2
    ]
    return {"rows": rows, "evaluated_at": now.isoformat(), "max_age_hours": 12}


def read_model_spread(station_id, now):
    # Primary operational forecasts predate the station-scoped archive.
    # Secondary forecasts must never fall back to those primary rows.
    with get_engine().connect() as con:
        runs = (
            con.execute(
                text(
                    "SELECT provider,model,issued_at,acquired_at,payload "
                    "FROM location_model_runs WHERE station_id=:id AND basis='live' "
                    "AND issued_at>=:since ORDER BY issued_at DESC LIMIT 120"
                ),
                {"id": station_id, "since": (now - pd.Timedelta(hours=13)).isoformat()},
            )
            .mappings()
            .all()
        )
        records = [dict(r, basis="live", rows=json.loads(r["payload"])) for r in runs]
        if station_id == Settings.from_env().station_id:
            raw = (
                con.execute(
                    text(
                        "SELECT provider,model,issued_at,fetched_at,valid_time,temp_c "
                        "FROM forecast_runs WHERE issued_at>=:since AND valid_time>=:start "
                        "AND valid_time<:end ORDER BY issued_at DESC LIMIT 10000"
                    ),
                    {
                        "since": (now - pd.Timedelta(hours=13)).strftime(
                            "%Y-%m-%dT%H:%M:%SZ"
                        ),
                        "start": now.floor("h").strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "end": (now + pd.Timedelta(hours=12)).strftime(
                            "%Y-%m-%dT%H:%M:%SZ"
                        ),
                    },
                )
                .mappings()
                .all()
            )
            records.extend(
                dict(r, basis="live", acquired_at=r["fetched_at"], rows=[dict(r)])
                for r in raw
            )
    return model_spread(records, now)
