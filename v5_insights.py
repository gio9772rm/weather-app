"""Explainable read-only quality, revisions, monthly summaries and event replay."""

from __future__ import annotations

import json

import numpy as np
import pandas as pd
from sqlalchemy import text

from db import get_engine

MEASUREMENTS = {
    "temp_c": ("Temperatura", "°C", -60, 60, 8),
    "humidity": ("Umidità", "%", 0, 100, 30),
    "pressure_hpa": ("Pressione", "hPa", 850, 1100, 5),
    "wind_kmh": ("Vento", "km/h", 0, 250, None),
    "windgust_kmh": ("Raffica", "km/h", 0, 300, None),
    "rain_mm": ("Pioggia del campione", "mm", 0, 200, None),
    "dewpoint_c": ("Punto di rugiada", "°C", -80, 60, None),
    "feels_like_c": ("Temperatura percepita", "°C", -90, 90, None),
}


def station_quality(station_id, now, stale_minutes=20):
    """Inspect original samples before display-time rain clipping."""
    from config import Settings
    from weather_derived import add_station_derived_values

    primary = station_id == Settings.from_env().station_id
    source = "station_raw" if primary else "station_observations"
    owner = "" if primary else " AND station_id=:id"
    with get_engine().connect() as con:
        frame = pd.read_sql(
            text(
                f"SELECT time,temp_c,humidity,pressure_hpa,wind_kmh,windgust_kmh,rain_mm,rain_total_mm,data_quality FROM {source} WHERE time>=:at{owner} ORDER BY time"
            ),
            con,
            params={
                "id": station_id,
                "at": (now - pd.Timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            },
        )
    frame.columns = [column.lower() for column in frame.columns]
    return quality_report(add_station_derived_values(frame), now, stale_minutes)


def quality_report(frame, now, stale_minutes=20):
    rows, issues = [], []
    data = frame.copy()
    if "time" not in data:
        data["time"] = pd.Series(dtype="datetime64[ns, UTC]")
    data["time"] = pd.to_datetime(data.time, utc=True, errors="coerce")
    data = data.dropna(subset=["time"]).sort_values("time").drop_duplicates("time")
    future = data.time.gt(now + pd.Timedelta(minutes=2))
    if future.any():
        issues.append(
            {
                "variable": "time",
                "kind": "future_timestamp",
                "message": "Campioni con orario futuro: verificare l’orologio della stazione.",
            }
        )
    data = data[~future]
    latest = data.time.max()
    for key, (label, unit, low, high, jump) in MEASUREMENTS.items():
        values = pd.to_numeric(
            data.get(key, pd.Series(index=data.index, dtype=float)), errors="coerce"
        ).replace([np.inf, -np.inf], np.nan)
        available = data.loc[values.notna()].copy()
        flags = []
        last = available.iloc[-1] if len(available) else None
        stamp = last.time if last is not None else None
        value = float(values.loc[last.name]) if last is not None else None
        age = (now - stamp).total_seconds() / 60 if stamp is not None else None
        if value is not None and not low <= value <= high:
            flags.append("Valore fuori dall’intervallo plausibile")
        recent = data[data.time.ge(now - pd.Timedelta(hours=4))].copy()
        recent_values = values.reindex(recent.index)
        if (
            key in {"temp_c", "humidity", "pressure_hpa"}
            and recent_values.count() >= 36
        ):
            duration = recent.time.max() - recent.time.min()
            if (
                duration >= pd.Timedelta(hours=3)
                and recent_values.nunique() == 1
                and recent.time.diff().max() <= pd.Timedelta(minutes=15)
            ):
                flags.append(
                    "Valore identico da almeno tre ore: sensore da controllare"
                )
        if jump and len(available) >= 2:
            before = available.iloc[-2]
            if (
                stamp - before.time <= pd.Timedelta(minutes=15)
                and abs(value - float(values.loc[before.name])) > jump
            ):
                flags.append("Salto rapido rispetto al campione precedente")
        if key == "rain_mm" and value is not None and value > 30:
            flags.append("Accumulo del campione elevato: verificare il pluviometro")
        if key == "rain_mm" and "rain_total_mm" in data:
            total = pd.to_numeric(data.rain_total_mm, errors="coerce")
            drops = total.diff().lt(0) & data.time.diff().le(pd.Timedelta(minutes=15))
            if drops.tail(3).any():
                flags.append(
                    "Contatore pioggia diminuito: possibile reset, controllare gli incrementi"
                )
        origin = "derived" if key in {"dewpoint_c", "feels_like_c"} else "observed"
        status = (
            "missing"
            if value is None
            else "suspect"
            if flags
            else "stale"
            if age > stale_minutes
            else "previous_sample"
            if stamp < latest
            else "current"
        )
        rows.append(
            {
                "variable": key,
                "label": label,
                "unit": unit,
                "value": value,
                "time": stamp,
                "age_minutes": age,
                "origin": origin,
                "source": "Calcolo da misure Ecowitt"
                if origin == "derived"
                else "Ecowitt",
                "status": status,
                "flags": flags,
            }
        )
        issues.extend(
            {"variable": key, "kind": "suspect", "message": label + ": " + flag}
            for flag in flags
        )
    return {
        "evaluated_at": now,
        "measurements": rows,
        "issues": issues,
        "originals_preserved": True,
        "suspect_count": sum(r["status"] == "suspect" for r in rows),
    }


def forecast_revisions(current, previous, now):
    """Compare matching valid hours, with explicit practical thresholds."""
    if current.empty or previous.empty:
        return {"available": False, "changes": []}
    a, b = current.copy(), previous.copy()
    for frame in (a, b):
        frame["valid_time"] = pd.to_datetime(
            frame.valid_time, utc=True, errors="coerce"
        )
    a = a[
        a.valid_time.between(now.floor("h"), now + pd.Timedelta(hours=24))
    ].drop_duplicates("valid_time")
    b = b.drop_duplicates("valid_time", keep="last")
    pairs = a.merge(b, on="valid_time", suffixes=("_new", "_old"))
    changes = []
    for var, threshold, label, unit in (
        ("temp_c", 2, "Temperatura", "°C"),
        ("wind_gust_kmh", 10, "Raffiche", "km/h"),
        ("precip_probability", 20, "Probabilità pioggia", "punti percentuali"),
        ("clouds", 25, "Nuvolosità", "%"),
    ):
        if var + "_new" not in pairs or var + "_old" not in pairs:
            continue
        delta = pd.to_numeric(pairs[var + "_new"], errors="coerce") - pd.to_numeric(
            pairs[var + "_old"], errors="coerce"
        )
        significant = delta.abs().ge(threshold)
        if significant.any():
            index = delta[significant].abs().idxmax()
            changes.append(
                {
                    "variable": var,
                    "label": label,
                    "unit": unit,
                    "delta": float(delta.loc[index]),
                    "time": pairs.loc[index, "valid_time"],
                    "threshold": threshold,
                }
            )
    for tag, frame in (("new", a), ("old", b[b.valid_time.isin(a.valid_time)])):
        if not {"rain_mm", "precip_probability"}.issubset(frame):
            continue
        wet = frame[
            pd.to_numeric(frame.rain_mm, errors="coerce").ge(0.1)
            & pd.to_numeric(frame.precip_probability, errors="coerce").ge(40)
        ]
        if len(wet):
            if tag == "new":
                new_rain = wet.valid_time.min()
            elif "new_rain" in locals():
                old_rain = wet.valid_time.min()
                delta = (new_rain - old_rain).total_seconds() / 3600
                if abs(delta) >= 1:
                    changes.append(
                        {
                            "variable": "rain_start",
                            "label": "Inizio fase piovosa",
                            "unit": "ore",
                            "delta": delta,
                            "time": new_rain,
                            "threshold": 1,
                        }
                    )
    return {
        "available": bool(len(pairs)),
        "compared_hours": len(pairs),
        "changes": changes,
        "previous_issued_at": previous.issued_at.max()
        if "issued_at" in previous
        else None,
    }


def monthly_archive(calendar, timezone, now):
    """Unknown or incomplete rainfall never extends a dry spell."""
    if not calendar:
        return {"months": [], "records": {}, "period": None}
    frame = pd.DataFrame(calendar).sort_values("date").drop_duplicates("date")
    today = now.tz_convert(timezone).date().isoformat()
    frame = frame[frame.date.lt(today)].copy()
    if frame.empty:
        return {"months": [], "records": {}, "period": None}
    frame["month"] = frame.date.str[:7]
    usable = frame.status.isin(["complete", "imported"])
    for var in ("rain_mm", "temp_min_c", "temp_max_c", "temp_mean_c"):
        frame[var] = pd.to_numeric(
            frame.get(var, pd.Series(index=frame.index, dtype=float)), errors="coerce"
        )
    rows = []
    for month, g in frame.groupby("month", sort=True):
        expected = pd.Period(month, freq="M").days_in_month
        if month == today[:7]:
            expected = int(today[-2:]) - 1
        valid = g[usable.reindex(g.index)]
        rain = valid.rain_mm.where(valid.rain_mm.ge(0))
        dry, maximum, last_date = 0, 0, None
        for _, day in g.iterrows():
            stamp = pd.Timestamp(day.date)
            consecutive = last_date is not None and stamp - last_date == pd.Timedelta(
                days=1
            )
            if (
                day.status in {"complete", "imported"}
                and pd.notna(day.rain_mm)
                and 0 <= day.rain_mm < 0.1
            ):
                dry = dry + 1 if consecutive else 1
                maximum = max(maximum, dry)
            else:
                dry = 0
            last_date = stamp
        rows.append(
            {
                "month": month,
                "expected_days": expected,
                "available_days": int(g.status.ne("missing").sum()),
                "complete_days": int(g.status.eq("complete").sum()),
                "imported_days": int(g.status.eq("imported").sum()),
                "partial_days": int(g.status.eq("partial").sum()),
                "rain_days_available": int(rain.notna().sum()),
                "coverage_percent": round(100 * len(valid) / expected, 1)
                if expected
                else 0,
                "rain_sum_mm": rain.sum(min_count=1),
                "rainy_days": int(rain.ge(0.1).sum()) if rain.notna().any() else None,
                "longest_dry_spell_days": maximum if rain.notna().any() else None,
                "temp_min_c": valid.temp_min_c.min(),
                "temp_max_c": valid.temp_max_c.max(),
                "temp_mean_c": valid.temp_mean_c.mean(),
                "is_partial": len(valid) < expected
                or rain.notna().sum() < expected
                or month == today[:7],
            }
        )
    valid = frame[usable]
    records = {}
    for var, mode in (("temp_min_c", "min"), ("temp_max_c", "max"), ("rain_mm", "max")):
        series = valid[var].dropna()
        if len(series):
            idx = series.idxmin() if mode == "min" else series.idxmax()
            records[var] = {
                "value": float(series.loc[idx]),
                "date": valid.loc[idx, "date"],
                "status": valid.loc[idx, "status"],
            }
    return {
        "months": rows,
        "records": records,
        "period": {"start": frame.date.min(), "end": frame.date.max()},
        "note": "Statistiche del periodo disponibile. Riepiloghi importati distinti; giorni incompleti esclusi dai totali e dai record. Non sono normali climatiche.",
    }


def event_replay(station_id, start, end):
    """Rebuild an event from archived observations and forecasts known at start."""
    from data_access import load_station
    from v5_data import clean_json, records, station_settings

    cfg = station_settings(station_id)
    now = pd.Timestamp.now(tz="UTC")

    def local(value):
        stamp = pd.Timestamp(value)
        try:
            return (
                stamp.tz_localize(
                    cfg.local_timezone, ambiguous="raise", nonexistent="raise"
                ).tz_convert("UTC")
                if stamp.tzinfo is None
                else stamp.tz_convert("UTC")
            )
        except Exception as exc:
            raise ValueError("Ora locale ambigua o inesistente") from exc

    start, end = local(start), local(end)
    if not now - pd.Timedelta(
        days=14
    ) <= start < end <= now or end - start > pd.Timedelta(hours=24):
        raise ValueError("Scegli al massimo 24 ore nei quattordici giorni precedenti")
    observed = load_station(24 * 15, station_id)
    if not observed.empty:
        observed = observed[observed.time.between(start, end)]
    with get_engine().connect() as con:
        radar = pd.read_sql(
            text(
                "SELECT observed_at,sri_point_mm_h,vmi_point_dbz,lightning_10km,lightning_25km FROM radar_local_snapshots WHERE station_id=:id AND observed_at>=:start AND observed_at<=:end ORDER BY observed_at"
            ),
            con,
            params={
                "id": station_id,
                "start": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "end": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
            },
        )
        stored = (
            con.execute(
                text(
                    "SELECT payload,acquired_at FROM location_model_runs WHERE station_id=:id AND basis='published' AND acquired_at<=:at ORDER BY acquired_at DESC LIMIT 1"
                ),
                {"id": station_id, "at": start.isoformat()},
            )
            .mappings()
            .first()
        )
    forecast = pd.DataFrame(json.loads(stored["payload"])) if stored else pd.DataFrame()
    if not forecast.empty:
        forecast["valid_time"] = pd.to_datetime(forecast.valid_time, utc=True)
        forecast = forecast[forecast.valid_time.between(start, end)]
    return clean_json(
        {
            "station_id": station_id,
            "start": start,
            "end": end,
            "observations": records(
                observed,
                [
                    "time",
                    "temp_c",
                    "rain_mm",
                    "rain_rate_mm_h",
                    "wind_kmh",
                    "windgust_kmh",
                ],
            ),
            "radar": records(radar),
            "forecast": records(forecast),
            "forecast_known_at": stored["acquired_at"] if stored else None,
            "forecast_status": "available" if len(forecast) else "not_archived",
            "radar_kind": "observed",
            "dpc_history_url": "https://radar.protezionecivile.gov.it/",
            "note": "Pioggia Ecowitt per campione, radar in mm/h, previsione per intervallo: unità distinte. Nessuna previsione ricostruita a posteriori.",
        }
    )
