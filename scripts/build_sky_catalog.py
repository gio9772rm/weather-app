"""Build the small offline search index from pinned OpenNGC CSVs (CC BY-SA 4.0)."""

import csv
import gzip
import io
import re
import sys
from pathlib import Path

TYPES = {
    "G",
    "GPair",
    "GTrpl",
    "GGroup",
    "OCl",
    "GCl",
    "Cl+N",
    "PN",
    "EmN",
    "HII",
    "DrkN",
    "Neb",
    "RefN",
    "SNR",
    "RfN",
    "Neb+Cl",
}


def angle(text, factor):
    sign = -1 if text.startswith("-") else 1
    a, b, c = map(float, text.lstrip("+-").split(":"))
    return f"{sign * factor * (a + b / 60 + c / 3600):.6f}"


def identifier(text):
    match = re.fullmatch(r"(NGC|IC|M)0*(\d+)([A-Z]?)", text.strip().upper())
    if match:
        prefix, number, suffix = match.groups()
        return prefix + ("" if prefix == "M" else " ") + str(int(number)) + suffix
    return " ".join(text.split())[:48]


def main(paths):
    rows = {}
    duplicates = {}
    for path in paths:
        with open(path, encoding="utf-8-sig", newline="") as source:
            for row in csv.DictReader(source, delimiter=";"):
                name = identifier(row["Name"])
                aliases = [
                    identifier(t) for k in ("NGC", "IC") for t in row[k].split(",") if t
                ]
                if row["Type"] == "Dup":
                    for alias in aliases:
                        duplicates.setdefault(alias, []).append(name)
                    continue
                if row["Type"] not in TYPES or not row["RA"] or not row["Dec"]:
                    continue
                primary = identifier("M" + row["M"]) if row["M"] else name
                common = [
                    n.strip() for n in row["Common names"].split(",") if n.strip()
                ]
                other = [
                    n.strip()
                    for n in row["Identifiers"].split(",")
                    if n.strip().startswith(
                        ("C ", "Sh ", "B ", "Mel ", "Ced ", "LBN ", "LDN ")
                    )
                ][:8]
                rows[primary] = (
                    primary,
                    row["Type"],
                    angle(row["RA"], 15),
                    angle(row["Dec"], 1),
                    row["MajAx"],
                    row["MinAx"],
                    row["V-Mag"],
                    common[0][:100] if common else name,
                    list(dict.fromkeys([name, *aliases, *common, *other])),
                )
    output = io.StringIO()
    writer = csv.writer(output, delimiter="\t", lineterminator="\n")
    for key, (*fields, aliases) in sorted(rows.items()):
        synonyms = [a for alias in aliases for a in duplicates.get(alias, [])]
        writer.writerow([*fields, *dict.fromkeys(aliases + synonyms)])
    dest = Path(__file__).resolve().parents[1] / "assets_data" / "openngc.tsv.gz"
    dest.parent.mkdir(exist_ok=True)
    dest.write_bytes(
        gzip.compress(output.getvalue().encode("utf-8"), compresslevel=9, mtime=0)
    )
    print(f"{len(rows)} deep sky objects · {dest.stat().st_size} bytes")


if __name__ == "__main__":
    main(sys.argv[1:])
