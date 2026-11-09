"""Local Messier/NGC/IC lookup, with a small pool for night recommendations."""

from __future__ import annotations

import csv
import gzip
import re
import unicodedata
from dataclasses import asdict
from functools import lru_cache
from pathlib import Path

from astronomy_planner import TARGET_BY_NAME, TARGETS, SkyTarget

DATA = Path(__file__).with_name("assets_data") / "openngc.tsv.gz"
EMISSION = {"PN", "EmN", "HII", "SNR", "Cl+N", "Neb+Cl"}
RECOMMENDED = (
    "M16",
    "M17",
    "M20",
    "M33",
    "M57",
    "M101",
    "M104",
    "NGC 104",
    "NGC 253",
    "NGC 2070",
    "NGC 3372",
    "NGC 4755",
    "NGC 5128",
    "NGC 5139",
    "NGC 7293",
)
EXTRA_NAMES = {
    "M16": "Nebulosa Aquila",
    "M17": "Nebulosa Omega",
    "M20": "Nebulosa Trifida",
    "M33": "Galassia del Triangolo",
    "M57": "Nebulosa Anello",
    "M101": "Galassia Girandola",
    "M104": "Galassia Sombrero",
    "NGC 104": "47 Tucanae",
    "NGC 253": "Galassia dello Scultore",
    "NGC 2070": "Nebulosa Tarantola",
    "NGC 3372": "Nebulosa della Carena",
    "NGC 5139": "Omega Centauri",
    "NGC 7293": "Nebulosa Elica",
}


def _key(value):
    decomposed = unicodedata.normalize("NFKD", value).casefold()
    key = "".join(c for c in decomposed if c.isalnum())
    return re.sub(r"^(ngc|ic|m)0+(?=\d)", r"\1", key)


def _number(value):
    return float(value) if value else None


@lru_cache(maxsize=1)
def _index():
    by_name, by_alias, entries = {}, {}, []
    with gzip.open(DATA, "rt", encoding="utf-8", newline="") as source:
        for name, kind, ra, dec, width, height, mag, title, *aliases in csv.reader(
            source, delimiter="\t"
        ):
            category = (
                "Galassia"
                if kind in {"G", "GPair", "GTrpl", "GGroup"}
                else ("Ammasso" if kind in {"OCl", "GCl"} else "Nebulosa")
            )
            target = TARGET_BY_NAME.get(name) or SkyTarget(
                name,
                EXTRA_NAMES.get(name, title),
                category,
                float(ra),
                float(dec),
                _number(mag),
                _number(width),
                _number(height),
            )
            keys = tuple(
                dict.fromkeys(
                    _key(alias)
                    for alias in [name, target.common_name, *aliases]
                    if alias
                )
            )
            by_name[name] = (target, kind)
            by_alias[_key(name)] = name
            entries.append((name, keys))
            for key in keys:
                by_alias.setdefault(key, name)
    for name, target in TARGET_BY_NAME.items():
        if name not in by_name:
            by_name[name] = (target, "EmN" if target.category == "Nebulosa" else "")
            entries.append((name, (_key(name), _key(target.common_name))))
        by_alias[_key(name)] = name
        by_alias[_key(target.common_name)] = name
    return by_name, by_alias, entries


def resolve_target(identifier):
    if not isinstance(identifier, str) or not 1 <= len(identifier) <= 80:
        raise ValueError("Soggetto non valido")
    if identifier in TARGET_BY_NAME:
        return TARGET_BY_NAME[identifier]
    targets, aliases, _ = _index()
    key = aliases.get(_key(identifier))
    if key is None:
        raise ValueError("Soggetto non trovato nel catalogo")
    return targets[key][0]


def plan_targets(values):
    if not isinstance(values, list) or not 1 <= len(values) <= 8:
        raise ValueError("Scegli da uno a otto oggetti del catalogo")
    return list({t.name: t for t in map(resolve_target, values)}.values())


def recommended_targets():
    return list(TARGETS) + [resolve_target(name) for name in RECOMMENDED]


def target_filters(target):
    kind = _index()[0].get(target.name, (None, ""))[1]
    return ["broadband", "narrowband"] if kind in EMISSION else ["broadband"]


@lru_cache(maxsize=128)
def search_targets(query):
    query = " ".join(query.split())
    if not 2 <= len(query) <= 80 or len(_key(query)) < 2:
        raise ValueError("Scrivi il nome o la sigla del soggetto")
    key = _key(query)
    targets, aliases, entries = _index()
    exact = aliases.get(key)
    matches = []
    for name, keys in entries:
        rank = (
            0
            if name == exact
            else 1
            if any(k.startswith(key) for k in keys)
            else (2 if len(key) >= 4 and any(key in k for k in keys) else 3)
        )
        if rank < 3:
            target = targets[name][0]
            matches.append(
                (rank, target.magnitude if target.magnitude is not None else 99, name)
            )
    return [asdict(targets[name][0]) for _, _, name in sorted(matches)[:12]]
