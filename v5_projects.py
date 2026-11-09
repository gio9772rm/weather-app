"""Compare actual integration gaps with the capacity of forecast windows."""

from __future__ import annotations

import math

import pandas as pd

from sky_catalog import resolve_target
from v5_imaging_budget import budget_options, imaging_budget
from v53_schedule import schedule_options


def project_spec(value):
    if not isinstance(value, dict):
        raise TypeError("Progetto non valido")
    target = resolve_target(value.get("target"))
    filters, seen = [], set()
    if (
        not isinstance(value.get("filters"), list)
        or not 1 <= len(value["filters"]) <= 8
    ):
        raise ValueError("Definisci da uno a otto obiettivi per filtro")
    for row in value["filters"]:
        name = " ".join(row["name"].split())
        desired, acquired, exposure = map(
            float,
            (
                row["desired_seconds"],
                row["acquired_seconds"],
                row["exposure_seconds"],
            ),
        )
        if (
            not 1 <= len(name) <= 24
            or ";" in name
            or name.casefold() in seen
            or not 1 <= desired <= 3600000
            or not 0 <= acquired <= 3600000
            or not 1 <= exposure <= 3600
        ):
            raise ValueError("Filtro, ore o durata della posa non validi")
        filters.append(
            {
                "name": name,
                "desired_seconds": round(desired),
                "acquired_seconds": round(acquired),
                "exposure_seconds": round(exposure, 1),
            }
        )
        seen.add(name.casefold())
    return {"target": target.name, "filters": filters}


def saved_projects(value):
    if not isinstance(value, list) or len(value) > 30:
        raise ValueError("Puoi salvare fino a trenta progetti")
    result = [project_spec(item) for item in value]
    if len({item["target"] for item in result}) != len(result):
        raise ValueError("Usa un progetto per soggetto")
    return result


def project_outlook(site, project, windows_by_date, options):
    target = resolve_target(project["target"])
    missing = {
        row["name"]: max(0, row["desired_seconds"] - row["acquired_seconds"])
        for row in project["filters"]
    }
    budget = options.get("budget") or {}
    if not isinstance(budget, dict):
        raise TypeError("Budget non valido")
    prior = budget_options(budget) if budget else None
    filters = [
        {"name": row["name"], "seconds": row["exposure_seconds"], "frames": 1}
        for row in project["filters"]
        if missing[row["name"]] > 0
    ]
    schedule = schedule_options(
        {
            key: options[key]
            for key in ("setup_minutes", "minimum_minutes")
            if key in options
        }
    )
    recipe = budget_options({**(prior or {}), "filters": filters}) if filters else None
    best, nights = None, []
    setup = pd.Timedelta(minutes=schedule["setup_minutes"])
    minimum = pd.Timedelta(minutes=math.ceil(schedule["minimum_minutes"] / 15) * 15)
    for date, windows in windows_by_date.items():
        choice = None
        for window in windows if recipe else ():
            start = (pd.Timestamp(window["start"]) + setup).ceil("15min")
            end = pd.Timestamp(window["end"]).floor("15min")
            if end - start < minimum:
                continue
            budget_for_window = imaging_budget(
                {
                    "blocks": [{"target": target.name, "start": start, "end": end}],
                    "overhead_minutes": schedule["setup_minutes"],
                },
                site,
                [target],
                recipe,
            )
            useful = sum(
                min(missing[f["name"]], f["integration_seconds"])
                for f in budget_for_window["filters"]
            )
            if useful > (choice["useful_seconds"] if choice else 0):
                choice = {
                    "date": date,
                    "window": window,
                    "useful_seconds": useful,
                    "filters": budget_for_window["filters"],
                }
        nights.append(
            choice or {"date": date, "window": None, "useful_seconds": 0, "filters": []}
        )
        if choice and (
            best is None or choice["useful_seconds"] > best["useful_seconds"]
        ):
            best = choice
    return {
        "target": target.name,
        "missing_seconds": sum(missing.values()),
        "best": best,
        "nights": nights,
        "budget": recipe,
        **schedule,
    }
