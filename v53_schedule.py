"""Bounded interval scheduling on validated geometry/weather windows."""

import math
from bisect import bisect_right

import pandas as pd


def schedule_options(values):
    result = {}
    for key, default, low, high in (
        ("setup_minutes", 20, 0, 180),
        ("switch_minutes", 10, 0, 60),
        ("minimum_minutes", 45, 15, 240),
        ("compare_nights", 3, 1, 5),
    ):
        v = float(values.get(key, default))
        if not math.isfinite(v) or not low <= v <= high or v != int(v):
            raise ValueError("Durata o numero di notti non valido")
        result[key] = int(v)
    priorities = values.get("priorities", {})
    if not isinstance(priorities, dict) or len(priorities) > 8:
        raise ValueError("Priorità non valide")
    result["priorities"] = {}
    for target, v in priorities.items():
        v = float(v)
        if (
            target not in values.get("targets", [])
            or not math.isfinite(v)
            or not 1 <= v <= 5
        ):
            raise ValueError("Priorità da 1 a 5 per gli oggetti selezionati")
        result["priorities"][target] = v
    return result


def optimize_schedule(sessions, tracks, start, end, options):
    """Maximise priority-weighted quality minutes, including every change cost.

    Each candidate is inside one uninterrupted validated window. Dynamic
    programming chooses compatible blocks; targets may recur after a gap.
    """
    setup = pd.Timedelta(minutes=options["setup_minutes"])
    switch = pd.Timedelta(minutes=options["switch_minutes"])
    minimum = options["minimum_minutes"]
    candidates = []
    for session in sessions:
        target = session["target"]
        points = (
            tracks[tracks.target.eq(target)].set_index("valid_time")
            if not tracks.empty
            else pd.DataFrame()
        )
        for window in session["windows"]:
            left, right = (
                max(pd.Timestamp(window["start"]), start).ceil("15min"),
                min(pd.Timestamp(window["end"]), end).floor("15min"),
            )
            for a in pd.date_range(left, right, freq="15min"):
                for b in pd.date_range(
                    a + pd.Timedelta(minutes=math.ceil(minimum / 15) * 15),
                    right,
                    freq="15min",
                ):
                    sample = points[(points.index >= a) & (points.index <= b)]
                    quality = float(sample.planner_score.mean()) if len(sample) else 0
                    if not math.isfinite(quality):
                        continue
                    minutes = (b - a).total_seconds() / 60
                    candidates.append(
                        {
                            "target": target,
                            "start": a,
                            "end": b,
                            "minutes": minutes,
                            "quality": quality,
                            "utility": minutes
                            * max(quality, 1)
                            * options["priorities"].get(target, 1),
                        }
                    )
    candidates.sort(key=lambda x: (x["end"], x["start"], x["target"]))
    ends = [c["end"] for c in candidates]
    # best[i] is the best schedule among the first i candidates.
    best, choices = [0.0], [None]
    for i, candidate in enumerate(candidates):
        j = bisect_right(ends, candidate["start"] - switch, hi=i)
        previous = choices[j]
        allowed = previous is not None or candidate["start"] >= start + setup
        value = best[j] + candidate["utility"] if allowed else -1
        if value > best[-1]:
            best.append(value)
            choices.append((i, previous))
        else:
            best.append(best[-1])
            choices.append(choices[-1])
    blocks, node = [], choices[-1]
    while node is not None:
        i, node = node
        block = {k: v for k, v in candidates[i].items() if k != "utility"}
        blocks.append(block)
    blocks.reverse()
    for i, block in enumerate(blocks):
        overhead = options["setup_minutes"] if i == 0 else options["switch_minutes"]
        block["prepare_start"] = block["start"] - pd.Timedelta(minutes=overhead)
        block["overhead_minutes"] = overhead
    included = {b["target"] for b in blocks}
    return {
        "blocks": blocks,
        "net_hours": sum(b["minutes"] for b in blocks) / 60,
        "overhead_minutes": sum(b["overhead_minutes"] for b in blocks),
        "omitted": [
            {
                "target": s["target"],
                "reason": s["limiting_factor"]
                if not s["windows"]
                else "Finestra breve o priorità inferiore nella sequenza ottimizzata",
            }
            for s in sessions
            if s["target"] not in included
        ],
        "method": "Ottimizzazione su griglia di 15 minuti: massimizza minuti × qualità geometrica/meteo × priorità. Preparazione iniziale e riacquisizione tra blocchi escluse dalle ore nette. Nessuna sovrapposizione; il risultato dipende dalle previsioni.",
    }
