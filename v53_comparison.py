"""Paired errors on common observations and a common information cutoff."""

import json
from datetime import datetime, timedelta, timezone
from itertools import combinations

import numpy as np
import pandas as pd

from v5_calibration import VARIABLES, observed_hourly, utc


def load_common_runs(station_id, now):
    """Stream narrow archives, retaining only the 3 relevant lead-time cuts.

    Avoid materialising months of repeated full forecast runs on 512 MiB cron
    workers. At most one row per model/target/cut survives in memory.
    """
    from sqlalchemy import text

    from config import Settings
    from db import get_engine

    selected = {}
    stop = now.to_pydatetime()
    start = stop - timedelta(days=90)

    def stamp(value):
        if isinstance(value, datetime):
            return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value
        try:
            result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            return (
                result.replace(tzinfo=timezone.utc) if result.tzinfo is None else result
            )
        except (ValueError, TypeError):
            return None

    def keep(row, metadata=None):
        r = {**row, **(metadata or {})}
        target, issued = stamp(r.get("valid_time")), stamp(r.get("issued_at"))
        acquired = stamp(r.get("acquired_at", r.get("fetched_at", r.get("issued_at"))))
        if (
            target is None
            or issued is None
            or acquired is None
            or not start <= target < stop
        ):
            return
        basis = r.get("basis", "live")
        known = issued if basis == "previous_run" else max(issued, acquired)
        for lead in (6, 24, 72):
            cutoff = target - timedelta(hours=lead)
            if known > cutoff or issued < cutoff - timedelta(hours=6):
                continue
            key = (
                r.get("provider", "canonical"),
                r.get("model", "baseline"),
                basis,
                target,
                lead,
            )
            old = selected.get(key)
            if old and (issued, acquired) <= (old["issued_at"], old["acquired_at"]):
                continue
            selected[key] = {
                "provider": key[0],
                "model": key[1],
                "basis": basis,
                "valid_time": target,
                "issued_at": issued,
                "acquired_at": acquired,
                "interval_hours": r.get("interval_hours", 1),
                **{v: r.get(v) for v in VARIABLES},
            }

    cutoff = (now - pd.Timedelta(days=94)).strftime("%Y-%m-%dT%H:%M:%SZ")
    params = {"id": station_id, "at": cutoff, "now": now.strftime("%Y-%m-%dT%H:%M:%SZ")}
    with (
        get_engine()
        .connect()
        .execution_options(stream_results=True, max_row_buffer=200) as con
    ):
        for row in con.execute(
            text(
                "SELECT provider,model,issued_at,acquired_at,basis,payload FROM location_model_runs WHERE station_id=:id AND issued_at>=:at AND basis IN ('live','previous_run')"
            ),
            params,
        ).mappings():
            metadata = {
                k: row[k]
                for k in ("provider", "model", "issued_at", "acquired_at", "basis")
            }
            for item in json.loads(row["payload"]):
                keep(item, metadata)
        if station_id == Settings.from_env().station_id:
            for row in con.execute(
                text(
                    "SELECT provider,model,issued_at,valid_time,interval_hours,temp_c,humidity,wind_kmh,rain_mm,fetched_at FROM forecast_runs WHERE issued_at>=:at AND valid_time<:now"
                ),
                params,
            ).mappings():
                keep(row)
            for row in con.execute(
                text(
                    "SELECT issued_at,valid_time,temp_c,humidity,wind_kmh,rain_mm FROM forecast_blend_history WHERE issued_at>=:at AND valid_time<:now"
                ),
                params,
            ).mappings():
                keep(row)
        else:
            for payload in con.execute(
                text(
                    "SELECT payload FROM location_forecasts WHERE station_id=:id AND issued_at>=:at"
                ),
                params,
            ).scalars():
                for row in json.loads(payload):
                    keep(row, {"provider": "canonical", "model": "baseline"})
    return pd.DataFrame(selected.values())


def published_history(station_id, now):
    from sqlalchemy import text

    from db import get_engine

    selected = {}
    with get_engine().connect() as con:
        rows = con.execute(
            text(
                "SELECT acquired_at,payload FROM location_model_runs WHERE station_id=:id AND basis='published' AND acquired_at>=:cutoff ORDER BY acquired_at DESC LIMIT 300"
            ),
            {"id": station_id, "cutoff": (now - pd.Timedelta(hours=48)).isoformat()},
        )
        for acquired, payload in rows:
            for row in json.loads(payload):
                at = utc(row["valid_time"])
                if (
                    now - pd.Timedelta(hours=24) <= at < now
                    and utc(acquired) <= at - pd.Timedelta(hours=1)
                    and at not in selected
                ):
                    selected[at] = {
                        **{
                            k: row.get(k)
                            for k in (
                                "valid_time",
                                "temp_c",
                                "wind_kmh",
                                "rain_mm",
                                "clouds",
                            )
                        },
                        "known_at": acquired,
                    }
    return [selected[k] for k in sorted(selected)]


def paired_comparison(forecasts, observations, now):
    if forecasts.empty or observations.empty:
        return []
    f = forecasts.copy()
    for name in ("issued_at", "valid_time"):
        f[name] = utc(f[name])
    f["acquired_at"] = utc(f.get("acquired_at", f.issued_at))
    for name, default in (
        ("basis", "live"),
        ("provider", "canonical"),
        ("model", "baseline"),
    ):
        if name not in f:
            f[name] = default
    f["interval_hours"] = pd.to_numeric(
        f.get("interval_hours", pd.Series(1, index=f.index)), errors="coerce"
    ).fillna(1)
    f = f[f.valid_time.lt(now) & f.valid_time.ge(now - pd.Timedelta(days=90))]
    obs = observed_hourly(observations)
    if f.empty or obs.empty:
        return []
    f["name"] = f.provider + "/" + f.model
    output = []
    for basis in ("live", "previous_run"):
        base = f[f.basis.eq(basis)].copy()
        for lead in (6, 24, 72):
            cutoff = base.valid_time - pd.Timedelta(hours=lead)
            # A model must actually have been available at the same cutoff.
            # Retrospective fixed-lead archives are evaluated in a separate cohort.
            known = (
                base.issued_at
                if basis == "previous_run"
                else base[["issued_at", "acquired_at"]].max(axis=1)
            )
            chosen = base[
                (known <= cutoff) & (base.issued_at >= cutoff - pd.Timedelta(hours=6))
            ]
            chosen = chosen.sort_values(["issued_at", "acquired_at"]).drop_duplicates(
                ["name", "valid_time"], keep="last"
            )
            for variable, (low, high, _) in VARIABLES.items():
                if variable not in chosen or variable not in obs:
                    continue
                g = chosen.copy()
                g[variable] = pd.to_numeric(g[variable], errors="coerce")
                g = g[g[variable].between(low, high)]
                if variable == "rain_mm":
                    g = g[g.interval_hours.eq(1)]
                pivot = g.pivot(index="valid_time", columns="name", values=variable)
                measured = obs.set_index("time")[variable].reindex(pivot.index)
                names = sorted(pivot.columns)[:10]
                for left, right in combinations(names, 2):
                    pair = pivot[[left, right]].assign(observed=measured).dropna()
                    for days in (7, 30, 90):
                        sample = pair[pair.index >= now - pd.Timedelta(days=days)]
                        if len(sample) < 12:
                            continue
                        errors = (
                            sample[[left, right]].sub(sample.observed, axis=0).abs()
                        )
                        # Resample whole UTC days to retain within-day dependence.
                        daily = (
                            (errors[left] - errors[right])
                            .groupby(sample.index.floor("D"))
                            .agg(["sum", "count"])
                        )
                        interval = [None, None]
                        adequate = len(sample) >= 48 and len(daily) >= 7
                        if adequate:
                            rng = np.random.default_rng(53)
                            draws = rng.integers(0, len(daily), (500, len(daily)))
                            boot = daily["sum"].to_numpy()[draws].sum(axis=1) / daily[
                                "count"
                            ].to_numpy()[draws].sum(axis=1)
                            interval = np.quantile(boot, [0.025, 0.975]).tolist()
                        output.append(
                            {
                                "basis": basis,
                                "lead_hours": lead,
                                "window_days": days,
                                "variable": variable,
                                "left": left,
                                "right": right,
                                "n": len(sample),
                                "days": len(daily),
                                "start": sample.index.min(),
                                "end": sample.index.max(),
                                "mae_left": float(errors[left].mean()),
                                "mae_right": float(errors[right].mean()),
                                "delta_mae": float(
                                    (errors[left] - errors[right]).mean()
                                ),
                                "delta_ci95": interval,
                                "adequate": adequate,
                                "evidence": "left"
                                if adequate and interval[1] < 0
                                else "right"
                                if adequate and interval[0] > 0
                                else "uncertain",
                            }
                        )
    return output


def update_comparison(station_id, now=None):
    from data_access import load_station
    from v5_data import clean_json

    now = utc(now if now is not None else pd.Timestamp.now(tz="UTC"))
    return clean_json(
        {
            "station_id": station_id,
            "evaluated_at": now,
            "rows": paired_comparison(
                load_common_runs(station_id, now),
                load_station(24 * 94, station_id),
                now,
            ),
            "method": "Confronti a coppie sulle stesse ore e misure. Ultima emissione disponibile al taglio comune di 6/24/72 h (anzianità extra massima 6 h). Pioggia solo su intervalli di un’ora completi. Archivio retrospettivo separato dalle acquisizioni operative. IC 95% della differenza MAE con bootstrap a blocchi giornalieri, 500 repliche; almeno 48 ore e 7 giorni. Evidenza esplorativa, senza correzione per confronti multipli; nessuna classifica globale. Copertura effettiva indicata, anche se inferiore a 7/30/90 giorni.",
        }
    )
