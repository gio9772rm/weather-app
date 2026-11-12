"""Build the compact, public GeoNames index from cities15000.zip and countryInfo.txt."""

import argparse
import gzip
import io
import json
from pathlib import Path
from zipfile import ZipFile


def build(cities: Path, countries: Path, output: Path):
    country_names = {}
    for line in countries.read_text(encoding="utf-8").splitlines():
        if line and not line.startswith("#"):
            code, _, _, _, name, *_ = line.split("\t")
            country_names[code] = name
    with ZipFile(cities) as archive, archive.open("cities15000.txt") as source:
        rows = []
        for line in io.TextIOWrapper(source, encoding="utf-8"):
            row = line.rstrip("\n").split("\t")
            if (
                row[6] != "P"
                or row[7] in {"PPLX", "PPLQ", "PPLH", "PPLW", "PPLS"}
                or not row[17]
            ):
                continue
            rows.append(
                [
                    int(row[0]),
                    row[1],
                    row[8],
                    round(float(row[4]), 3),
                    round(float(row[5]), 3),
                    row[17],
                    max(0, int(row[14] or 0)),
                ]
            )
    rows.sort(key=lambda row: row[0])
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(
        gzip.compress(
            json.dumps(
                {"countries": country_names, "cities": rows},
                separators=(",", ":"),
                ensure_ascii=False,
            ).encode(),
            mtime=0,
        )
    )
    print(f"{len(rows)} public city centres · {output.stat().st_size} bytes")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("cities", type=Path)
    parser.add_argument("countries", type=Path)
    parser.add_argument(
        "--output", type=Path, default=Path("assets/geo/public-cities.json.gz")
    )
    args = parser.parse_args()
    build(args.cities, args.countries, args.output)
