"""Import private CSV/XLSX station history without committing it to Git."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from db import ensure_schema, get_engine, set_meta
from weather_ingest_ecowitt_cloud import RAW_COLUMNS, recompute_3h, upsert_raw

load_dotenv()

RENAME = {
    "Time": "time",
    "DateTime": "time",
    "datetime": "time",
    "timestamp": "time",
    "TempOut": "temp_c",
    "Temperature_C": "temp_c",
    "Temperature": "temp_c",
    "Temp_C": "temp_c",
    "HumidityOut": "humidity",
    "Humidity_%": "humidity",
    "Humidity": "humidity",
    "Pressure": "pressure_hpa",
    "Pressure_hPa": "pressure_hpa",
    "Wind": "wind_kmh",
    "Wind_km_h": "wind_kmh",
    "Wind_kph": "wind_kmh",
    "Wind_kmh": "wind_kmh",
    "WindGust": "windgust_kmh",
    "WindGust_kmh": "windgust_kmh",
    "WindDir": "winddir",
    "Rain": "rain_mm",
    "Rain_mm_3h": "rain_mm",
    "Rain_mm": "rain_mm",
}


def _read(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    if path.suffix.lower() in {".xlsx", ".xls"}:
        return pd.read_excel(path)
    raise ValueError(f"Formato non supportato: {path.suffix}")


def load_file(path: Path, timezone_name: str) -> pd.DataFrame:
    frame = _read(path).rename(columns=RENAME)
    if "time" not in frame:
        raise ValueError("Colonna data/ora non trovata")
    parsed = pd.to_datetime(frame["time"], errors="coerce")
    if parsed.dt.tz is None:
        parsed = parsed.dt.tz_localize(
            timezone_name, ambiguous="NaT", nonexistent="shift_forward"
        ).dt.tz_convert("UTC")
    else:
        parsed = parsed.dt.tz_convert("UTC")
    frame["time"] = parsed
    for column in (
        "temp_c",
        "humidity",
        "pressure_hpa",
        "wind_kmh",
        "windgust_kmh",
        "winddir",
        "rain_mm",
    ):
        if column not in frame:
            frame[column] = None
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame["wind_ms"] = frame["wind_kmh"] / 3.6
    frame["source"] = "historical_import"
    frame["data_quality"] = "historical_aggregate"
    for column in RAW_COLUMNS:
        if column not in frame:
            frame[column] = None
    return (
        frame[RAW_COLUMNS]
        .dropna(subset=["time"])
        .drop_duplicates("time", keep="last")
        .sort_values("time")
    )


def discover_files(explicit: list[str]) -> list[Path]:
    if explicit:
        return [Path(value) for value in explicit]
    candidates: list[Path] = []
    historical = Path("historical")
    if historical.exists():
        candidates.extend(
            path
            for path in historical.iterdir()
            if path.suffix.lower() in {".csv", ".xlsx", ".xls"}
        )
    legacy = Path("storico_stazione.xlsx")
    if legacy.exists():
        candidates.append(legacy)
    return sorted(set(candidates))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Importa lo storico privato della stazione"
    )
    parser.add_argument("files", nargs="*", help="File CSV/XLSX da importare")
    parser.add_argument(
        "--timezone",
        default=os.getenv("LOCAL_TZ") or "Europe/Rome",
        help="Fuso dei timestamp privi di offset",
    )
    args = parser.parse_args()
    files = discover_files(args.files)
    if not files:
        print(
            "Nessun file trovato. Passa un percorso oppure usa la cartella historical/."
        )
        return 2
    missing = [str(path) for path in files if not path.exists()]
    if missing:
        print("File non trovati: " + ", ".join(missing))
        return 2

    ensure_schema()
    engine = get_engine()
    total = 0
    earliest = None
    for path in files:
        frame = load_file(path, args.timezone)
        rows = upsert_raw(frame, engine)
        total += rows
        if not frame.empty:
            value = frame["time"].min()
            earliest = value if earliest is None else min(earliest, value)
        print(f"{path.name}: {rows} righe importate")
    buckets = recompute_3h(earliest, lookback_hours=24 * 365 * 10, engine=engine)
    set_meta(
        "last_historical_import",
        pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%dT%H:%M:%SZ"),
    )
    print(f"Completato: {total} osservazioni, {buckets} intervalli da 3 ore")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
