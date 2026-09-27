"""Station-isolated, chronological verification and guarded local correction.

Research runs never enter the operational blend automatically. Corrections of
the canonical forecast need a later validation period and a causal persistence
baseline. Original forecasts and station measurements are never overwritten.
"""

from __future__ import annotations

import json

import numpy as np
import pandas as pd
from sqlalchemy import text

from config import Settings
from db import get_engine

VARIABLES = {
    "temp_c": (-60, 60, 4),
    "humidity": (0, 100, 15),
    "wind_kmh": (0, 250, 10),
    "rain_mm": (0, 500, 0),
}
HORIZONS = ["0–6 h", "6–24 h", "24–72 h"]


def utc(value):
    return pd.to_datetime(value, utc=True, errors="coerce")


def archive_runs(station_id, frame, *, basis="live", acquired_at=None):
    from v5_data import clean_json, records

    if frame.empty:
        return 0
    data = frame.copy()
    now = utc(acquired_at if acquired_at is not None else pd.Timestamp.now(tz="UTC"))
    data["issued_at"] = utc(data["issued_at"])
    data["valid_time"] = utc(data["valid_time"])
    data = data.dropna(subset=["issued_at", "valid_time"])
    for field, default in (("provider", "canonical"), ("model", "baseline")):
        if field not in data:
            data[field] = default
    pending = []
    count = 0
    for (provider, model, issued), group in data.groupby(
        ["provider", "model", "issued_at"], observed=True
    ):
        columns = [
            "valid_time",
            "issued_at",
            "forecast_issued_at",
            "interval_hours",
            *VARIABLES,
            "wind_gust_kmh",
            "precip_probability",
            "clouds",
        ]
        payload = json.dumps(clean_json(records(group, columns)), allow_nan=False)
        pending.append(
            {
                "id": station_id,
                "provider": str(provider),
                "model": str(model),
                "issued": issued.isoformat(),
                "at": now.isoformat(),
                "basis": basis,
                "payload": payload,
            }
        )
        count += len(group)
    with get_engine().begin() as con:
        if pending:
            con.execute(
                text(
                    "INSERT INTO location_model_runs(station_id,provider,model,issued_at,acquired_at,basis,payload) "
                    "VALUES(:id,:provider,:model,:issued,:at,:basis,:payload) "
                    "ON CONFLICT(station_id,provider,model,issued_at,basis) DO NOTHING"
                ),
                pending,
            )
        con.execute(
            text(
                "DELETE FROM location_model_runs WHERE station_id=:id AND issued_at<:at"
            ),
            {"id": station_id, "at": (now - pd.Timedelta(days=90)).isoformat()},
        )
    return count


def archive_publication(station_id, forecast, published_at):
    """Keep each changed published forecast, including corrected revisions."""
    if forecast.empty:
        return 0
    from v5_data import clean_json, records

    columns = [
        "valid_time",
        "issued_at",
        *VARIABLES,
        "wind_gust_kmh",
        "precip_probability",
        "clouds",
    ]
    current = [
        {key: row.get(key) for key in columns}
        for row in clean_json(records(forecast, columns))
    ]
    with get_engine().connect() as con:
        previous = con.execute(
            text(
                "SELECT payload FROM location_model_runs WHERE station_id=:id AND basis='published' ORDER BY acquired_at DESC LIMIT 1"
            ),
            {"id": station_id},
        ).scalar()
    if previous:
        old = {}
        for row in json.loads(previous):
            row["issued_at"] = row.pop("forecast_issued_at", row.get("issued_at"))
            old[row["valid_time"]] = {key: row.get(key) for key in columns}
        if all(old.get(row["valid_time"]) == row for row in current):
            return 0
    published = forecast.copy()
    published["forecast_issued_at"] = published.issued_at
    published["issued_at"] = utc(published_at)
    published["provider"], published["model"] = "website", "published"
    return archive_runs(
        station_id, published, basis="published", acquired_at=published_at
    )


def load_runs(station_id, now, days=60):
    """Include pre-V5.2 archives, with strict station ownership."""
    cutoff = (now - pd.Timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%SZ")
    frames = []
    with get_engine().connect() as con:
        rows = (
            con.execute(
                text(
                    "SELECT provider,model,issued_at,acquired_at,basis,payload FROM location_model_runs "
                    "WHERE station_id=:id AND issued_at>=:at AND basis<>'published' "
                    "ORDER BY issued_at DESC LIMIT 8000"
                ),
                {"id": station_id, "at": cutoff},
            )
            .mappings()
            .all()
        )
        for row in rows:
            f = pd.DataFrame(json.loads(row["payload"]))
            if f.empty:
                continue
            for key in ("provider", "model", "acquired_at", "basis"):
                f[key] = row[key]
            frames.append(f)
        if station_id == Settings.from_env().station_id:
            raw = pd.read_sql(
                text(
                    "SELECT provider,model,issued_at,valid_time,interval_hours,temp_c,humidity,wind_kmh,rain_mm,fetched_at "
                    "FROM forecast_runs WHERE issued_at>=:at AND valid_time<:now ORDER BY issued_at DESC LIMIT 100000"
                ),
                con,
                params={"at": cutoff, "now": now.strftime("%Y-%m-%dT%H:%M:%SZ")},
            )
            if not raw.empty:
                raw["basis"], raw["acquired_at"] = "live", raw["fetched_at"]
                frames.append(raw)
            baseline = pd.read_sql(
                text(
                    "SELECT issued_at,valid_time,temp_c,humidity,wind_kmh,rain_mm "
                    "FROM forecast_blend_history WHERE issued_at>=:at AND valid_time<:now ORDER BY issued_at DESC LIMIT 50000"
                ),
                con,
                params={"at": cutoff, "now": now.strftime("%Y-%m-%dT%H:%M:%SZ")},
            )
        else:
            previous = (
                con.execute(
                    text(
                        "SELECT payload FROM location_forecasts WHERE station_id=:id AND issued_at>=:at "
                        "ORDER BY issued_at DESC LIMIT 1500"
                    ),
                    {"id": station_id, "at": cutoff},
                )
                .scalars()
                .all()
            )
            baseline = (
                pd.concat(
                    [pd.DataFrame(json.loads(p)) for p in previous], ignore_index=True
                )
                if previous
                else pd.DataFrame()
            )
        if not baseline.empty:
            baseline["provider"], baseline["model"], baseline["basis"] = (
                "canonical",
                "baseline",
                "live",
            )
            baseline["acquired_at"] = baseline["issued_at"]
            frames.append(baseline)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def observed_hourly(observations):
    """Rain is a preceding-hour sum; missing 5-minute buckets are never dry."""
    if observations.empty or "time" not in observations:
        return pd.DataFrame()
    data = observations.copy()
    data["time"] = utc(data.time)
    data = data.dropna(subset=["time"]).sort_values("time").drop_duplicates("time")
    if "data_quality" in data:
        bad = data.data_quality.fillna("").str.contains(
            "suspect|invalid|stuck", case=False
        )
        data = data[~bad]
    data = data.set_index("time")
    output = pd.DataFrame()
    for var, (low, high, _) in VARIABLES.items():
        if var not in data:
            continue
        values = pd.to_numeric(data[var], errors="coerce")
        values = values.where(values.between(low, high))
        if var == "rain_mm":
            # Deduplicate five-minute buckets before evaluating coverage.
            buckets = values.resample("5min", closed="right", label="right").sum(
                min_count=1
            )
            counts = buckets.resample("h", closed="right", label="right").count()
            output[var] = (
                buckets.resample("h", closed="right", label="right")
                .sum(min_count=1)
                .where(counts >= 10)
            )
        else:
            # Point forecasts compare with the last nearby observed value.
            hourly = values.resample("h").last()
            index = pd.date_range(
                data.index.min().ceil("h"), data.index.max().floor("h"), freq="h"
            )
            aligned = pd.merge_asof(
                pd.DataFrame({"time": index}),
                values.rename(var).reset_index().dropna(),
                on="time",
                direction="backward",
                tolerance=pd.Timedelta(minutes=10),
            )
            output[var] = aligned.set_index("time")[var].reindex(hourly.index)
    return output.reset_index(names="time")


def verify(forecast, observations, now):
    """One target sample per model/horizon, train before validation issuance."""
    if forecast.empty or observations.empty:
        return []
    f = forecast.copy()
    for key in ("valid_time", "issued_at"):
        f[key] = utc(f[key])
    f["acquired_at"] = utc(f.get("acquired_at", f.issued_at))
    for key, default in (
        ("provider", "canonical"),
        ("model", "baseline"),
        ("basis", "live"),
    ):
        if key not in f:
            f[key] = default
    f["interval_hours"] = pd.to_numeric(
        f.get("interval_hours", pd.Series(1, index=f.index)), errors="coerce"
    ).fillna(1)
    lead = (f.valid_time - f.issued_at).dt.total_seconds() / 3600
    f["horizon"] = pd.cut(lead, [0, 6, 24, 72], labels=HORIZONS)
    f = f[
        f.horizon.notna()
        & f.valid_time.lt(now)
        & (f.basis.eq("previous_run") | f.acquired_at.le(f.valid_time))
    ]
    f = f.sort_values("issued_at").drop_duplicates(
        ["provider", "model", "basis", "valid_time", "horizon"]
    )
    obs = observed_hourly(observations)
    if f.empty or obs.empty:
        return []
    pairs = f.merge(obs, left_on="valid_time", right_on="time", suffixes=("_f", "_o"))
    persistence = observations.copy()
    persistence["time"] = utc(persistence.time)
    persistence = persistence.dropna(subset=["time"]).sort_values("time")
    results = []
    for (provider, model, basis, horizon), group in pairs.groupby(
        ["provider", "model", "basis", "horizon"], observed=True
    ):
        for var, (low, high, max_bias) in VARIABLES.items():
            if var + "_f" not in group or var + "_o" not in group:
                continue
            g = group.copy().sort_values("valid_time")
            for col in (var + "_f", var + "_o"):
                g[col] = pd.to_numeric(g[col], errors="coerce")
                g[col] = g[col].where(g[col].between(low, high))
            # Native six-hour rainfall is not comparable with an hourly gauge.
            if var == "rain_mm":
                g = g[g.interval_hours.eq(1)]
            g = g.dropna(subset=[var + "_f", var + "_o"])
            if len(g) < 12:
                continue
            validation = g.iloc[int(len(g) * 0.8) :].copy()
            train = g[g.valid_time < validation.issued_at.min()].copy()
            if train.empty:
                continue
            rain = var == "rain_mm"
            bias = float(
                np.clip(
                    (train[var + "_f"] - train[var + "_o"]).mean(), -max_bias, max_bias
                )
            )
            factor = (
                float(
                    np.clip(train[var + "_o"].sum() / train[var + "_f"].sum(), 0.5, 2)
                )
                if rain and train[var + "_f"].sum() > 0
                else 1.0
            )
            corrected = (
                validation[var + "_f"] * factor
                if rain
                else validation[var + "_f"] - bias
            ).clip(low, high)
            raw_error = (validation[var + "_f"] - validation[var + "_o"]).abs()
            corrected_error = (corrected - validation[var + "_o"]).abs()
            # Use the same holdout targets for both model and persistence.
            base = (
                obs[["time", var]].copy() if rain else persistence[["time", var]].copy()
            )
            base[var] = pd.to_numeric(base[var], errors="coerce")
            base = base[base[var].between(low, high)].sort_values("time")
            check = pd.merge_asof(
                validation.sort_values("issued_at"),
                base.rename(columns={"time": "baseline_at", var: "baseline"}),
                left_on="issued_at",
                right_on="baseline_at",
                direction="backward",
                tolerance=pd.Timedelta(minutes=65 if rain else 20),
            )
            common = check.dropna(subset=["baseline"])
            persistence_mae = (
                float((common.baseline - common[var + "_o"]).abs().mean())
                if len(common)
                else None
            )
            common_corrected = (
                common[var + "_f"] * factor if rain else common[var + "_f"] - bias
            ).clip(low, high)
            common_mae = (
                float((common_corrected - common[var + "_o"]).abs().mean())
                if len(common)
                else None
            )
            days = int(g.valid_time.dt.date.nunique())
            eligible = (
                days >= 30
                and len(train) >= 120
                and len(validation) >= 30
                and len(common) >= 30
                and corrected_error.mean() < raw_error.mean() * 0.9
                and persistence_mae is not None
                and common_mae < persistence_mae * 0.9
            )
            if rain:
                eligible = (
                    eligible
                    and int(train[var + "_o"].gt(0.1).sum()) >= 20
                    and int(validation[var + "_o"].gt(0.1).sum()) >= 8
                )
            errors = g[var + "_f"] - g[var + "_o"]
            results.append(
                {
                    "provider": str(provider),
                    "model": str(model),
                    "basis": str(basis),
                    "variable": var,
                    "horizon": str(horizon),
                    "n": len(g),
                    "days": days,
                    "train_n": len(train),
                    "holdout_n": len(validation),
                    "persistence_n": len(common),
                    "mae": float(errors.abs().mean()),
                    "bias": float(errors.mean()),
                    "holdout_mae": float(raw_error.mean()),
                    "corrected_mae": float(corrected_error.mean()),
                    "persistence_mae": persistence_mae,
                    "training_bias": bias,
                    "rain_factor": factor,
                    "start": g.valid_time.min(),
                    "end": g.valid_time.max(),
                    "training_end": train.valid_time.max(),
                    "validation_start": validation.valid_time.min(),
                    "eligible": bool(eligible),
                    "applied": bool(
                        eligible and provider == "canonical" and basis == "live"
                    ),
                }
            )
    return results


def apply_calibration(frame, product, now=None):
    """A failed/stale validation always falls back to the canonical forecast."""
    if frame.empty or not product:
        return frame
    now = utc(now if now is not None else pd.Timestamp.now(tz="UTC"))
    evaluated = utc(product.get("evaluated_at"))
    if pd.isna(evaluated) or not pd.Timedelta(0) <= now - evaluated <= pd.Timedelta(
        hours=26
    ):
        return frame
    result = frame.copy()
    lead = (utc(result.valid_time) - utc(result.issued_at)).dt.total_seconds() / 3600
    horizons = pd.cut(lead, [0, 6, 24, 72], labels=HORIZONS)
    result["local_corrections"] = [{} for _ in range(len(result))]
    for rule in product.get("scores", []):
        var = rule.get("variable")
        if (
            not rule.get("applied")
            or rule.get("provider") != "canonical"
            or rule.get("basis") != "live"
            or var not in VARIABLES
            or var not in result
        ):
            continue
        mask = horizons.eq(rule["horizon"]) & result[var].notna()
        if not mask.any():
            continue
        raw = pd.to_numeric(result.loc[mask, var], errors="coerce")
        low, high, _ = VARIABLES[var]
        values = (
            raw * rule["rain_factor"]
            if var == "rain_mm"
            else raw - rule["training_bias"]
        )
        result.loc[mask, var] = values.clip(low, high)
        for index, original in raw.items():
            result.at[index, "local_corrections"] = {
                **result.at[index, "local_corrections"],
                var: {
                    "original": float(original),
                    "holdout_mae": rule["corrected_mae"],
                    "holdout_n": rule["holdout_n"],
                },
            }
    # Derived quantities must follow corrected temperature and humidity.
    changed = result.local_corrections.map(
        lambda v: any(k in v for k in ("temp_c", "humidity", "wind_kmh"))
    )
    if changed.any() and {"temp_c", "humidity"}.issubset(result):
        from weather_derived import apparent_temperature_c, dew_point_c

        result.loc[changed, "dewpoint_c"] = dew_point_c(
            result.loc[changed, "temp_c"], result.loc[changed, "humidity"]
        )
        wind = result.get("wind_kmh", pd.Series(np.nan, index=result.index))
        result.loc[changed, "feels_like_c"] = apparent_temperature_c(
            result.loc[changed, "temp_c"],
            result.loc[changed, "humidity"],
            wind.loc[changed],
        )
    return result


def update_verification(station_id, now=None):
    from data_access import load_station
    from v5_data import clean_json

    now = utc(now if now is not None else pd.Timestamp.now(tz="UTC"))
    scores = verify(load_runs(station_id, now), load_station(24 * 63, station_id), now)
    return clean_json(
        {
            "station_id": station_id,
            "evaluated_at": now,
            "scores": scores,
            "method": "80/20 temporale; training concluso prima delle emissioni di validazione; almeno 30 giorni e 30 riscontri finali; vantaggio minimo 10%",
        }
    )
