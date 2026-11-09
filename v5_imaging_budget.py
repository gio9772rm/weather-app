"""Estimate complete exposures and overheads inside validated observing blocks."""

from __future__ import annotations

import math
from collections import Counter

import pandas as pd

DEFAULT = {
    "filters": [{"name": "OSC", "seconds": 180, "frames": 1}],
    "readout_seconds": 2,
    "filter_change_seconds": 5,
    "focus_seconds": 90,
    "focus_every_minutes": 60,
    "dither_seconds": 15,
    "dither_every_frames": 3,
    "flip_seconds": 420,
}
BOUNDS = {
    "readout_seconds": (0, 120),
    "filter_change_seconds": (0, 120),
    "focus_seconds": (0, 900),
    "focus_every_minutes": (0, 480),
    "dither_seconds": (0, 300),
    "dither_every_frames": (0, 100),
    "flip_seconds": (0, 1800),
}


def budget_options(value):
    if value is None:
        return None
    if not isinstance(value, dict) or set(value) - ({"filters"} | BOUNDS.keys()):
        raise ValueError("Budget di ripresa non valido")
    raw = value.get("filters")
    if not isinstance(raw, list) or not 1 <= len(raw) <= 8:
        raise ValueError("Inserisci da uno a otto filtri o modalità di ripresa")
    filters, seen = [], set()
    for row in raw:
        name = " ".join(row["name"].split())
        seconds, frames = float(row["seconds"]), float(row["frames"])
        if (
            not 1 <= len(name) <= 24
            or name.casefold() in seen
            or not 1 <= seconds <= 3600
            or not 1 <= frames <= 50
            or frames != int(frames)
        ):
            raise ValueError("Filtri, pose o quantità non valide")
        filters.append(
            {"name": name, "seconds": round(seconds, 1), "frames": int(frames)}
        )
        seen.add(name.casefold())
    result = {"filters": filters}
    for key, (lo, hi) in BOUNDS.items():
        number = float(value.get(key, DEFAULT[key]))
        if not math.isfinite(number) or not lo <= number <= hi or number != int(number):
            raise ValueError("Tempi di preparazione non validi")
        result[key] = int(number)
    return result


def _meridian(start, end, longitude, ra):
    julian = start / 86400 + 2440587.5
    sidereal = (280.46061837 + 360.98564736629 * (julian - 2451545) + longitude) % 360
    crossing = start + ((ra - sidereal) % 360) / 360.98564736629 * 86400
    return crossing if crossing < end else None


def _filters(recipe, counts):
    return [
        {
            **row,
            "count": counts[row["name"]],
            "integration_seconds": round(row["seconds"] * counts[row["name"]], 1),
        }
        for row in recipe
    ]


def imaging_budget(schedule, equipment_site, targets, options):
    """Fit complete exposures; gaps/flip and block end are hard boundaries."""
    target_ra = {target.name: target.ra_deg for target in targets}
    recipe = options["filters"]
    cycle = [(r["name"], r["seconds"]) for r in recipe for _ in range(r["frames"])]
    result, totals, overhead_totals = [], Counter(), Counter()
    for block in schedule["blocks"]:
        left, right = (pd.Timestamp(block[k]).timestamp() for k in ("start", "end"))
        crossing = (
            _meridian(left, right, equipment_site.longitude, target_ra[block["target"]])
            if options["flip_seconds"]
            else None
        )
        phases, overhead = [(left, right)], Counter()
        if crossing is not None:
            after = min(right, crossing + options["flip_seconds"])
            phases = [(left, crossing), (after, right)]
            overhead["flip"] = after - crossing
        counts = Counter()
        cursor, current_filter, since_dither = 0, None, 0
        for start, end in phases:
            moment, last_focus = start, None
            while moment < end:
                name, exposure = cycle[cursor]
                costs = {"readout": options["readout_seconds"]}
                if len(recipe) > 1 and current_filter != name:
                    costs["filter"] = options["filter_change_seconds"]
                focus_period = options["focus_every_minutes"] * 60
                if options["focus_seconds"] and (
                    last_focus is None
                    or (focus_period and moment - last_focus >= focus_period)
                ):
                    costs["focus"] = options["focus_seconds"]
                if (
                    options["dither_every_frames"]
                    and since_dither >= options["dither_every_frames"]
                ):
                    costs["dither"] = options["dither_seconds"]
                duration = exposure + sum(costs.values())
                if moment + duration > end + 1e-6:
                    break
                if "focus" in costs:
                    last_focus = moment
                if "dither" in costs:
                    since_dither = 0
                moment += duration
                overhead.update(costs)
                counts[name] += 1
                current_filter = name
                since_dither += 1
                cursor = (cursor + 1) % len(cycle)
        filtered = _filters(recipe, counts)
        integration = sum(row["integration_seconds"] for row in filtered)
        total_overhead = sum(overhead.values())
        result.append(
            {
                "target": block["target"],
                "start": block["start"],
                "end": block["end"],
                "filters": filtered,
                "integration_seconds": round(integration, 1),
                "overhead": {k: round(v, 1) for k, v in overhead.items()},
                "idle_seconds": round(
                    max(0, right - left - integration - total_overhead), 1
                ),
                "flip_at": pd.to_datetime(crossing, unit="s", utc=True)
                if crossing is not None
                else None,
            }
        )
        totals.update(counts)
        overhead_totals.update(overhead)
    return {
        "filters": _filters(recipe, totals),
        "blocks": result,
        "integration_seconds": round(sum(b["integration_seconds"] for b in result), 1),
        "overhead": {k: round(v, 1) for k, v in overhead_totals.items()},
        "idle_seconds": round(sum(b["idle_seconds"] for b in result), 1),
        "preparation_seconds": 60 * schedule["overhead_minutes"],
    }


def total_budget(nights, options):
    counts, overhead = Counter(), Counter()
    budgets = [n["budget"] for n in nights]
    for budget in budgets:
        counts.update({r["name"]: r["count"] for r in budget["filters"]})
        overhead.update(budget["overhead"])
    return {
        "filters": _filters(options["filters"], counts),
        "integration_seconds": round(sum(b["integration_seconds"] for b in budgets), 1),
        "overhead": dict(overhead),
        "idle_seconds": round(sum(b["idle_seconds"] for b in budgets), 1),
        "preparation_seconds": sum(b["preparation_seconds"] for b in budgets),
    }
