"""Import a private Ecowitt daily XLSX into the station-aware archive."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import pandas as pd

from db import ensure_schema, get_engine
from station_daily import (
    parse_ecowitt_daily_export,
    pressure_review_share,
    upsert_daily_summaries,
)
from station_registry import normalise_station_id, register_station


def _env_float(name: str) -> float | None:
    try:
        return float(os.getenv(name, ""))
    except (TypeError, ValueError):
        return None


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Importa un export giornaliero Ecowitt senza trasformarlo in dati live"
    )
    parser.add_argument("file", type=Path, help="File XLSX Ecowitt")
    parser.add_argument(
        "--station-id",
        default=os.getenv("SECONDARY_STATION_ID") or "secondary-station",
    )
    parser.add_argument(
        "--name",
        default=os.getenv("SECONDARY_STATION_NAME") or "Stazione secondaria",
    )
    parser.add_argument(
        "--latitude", type=float, default=_env_float("SECONDARY_STATION_LAT")
    )
    parser.add_argument(
        "--longitude", type=float, default=_env_float("SECONDARY_STATION_LON")
    )
    parser.add_argument(
        "--elevation",
        type=float,
        default=_env_float("SECONDARY_STATION_ELEVATION_M"),
    )
    parser.add_argument(
        "--timezone",
        default=os.getenv("SECONDARY_STATION_TZ") or "Europe/Rome",
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not args.file.exists():
        parser.error(f"File non trovato: {args.file}")
    if args.latitude is None or args.longitude is None or args.elevation is None:
        parser.error(
            "Servono latitudine, longitudine e quota tramite opzioni o variabili SECONDARY_STATION_*"
        )

    frame = parse_ecowitt_daily_export(args.file)
    if frame.empty:
        parser.error("L'export non contiene giornate valide")
    expected = pd.date_range(frame["local_date"].min(), frame["local_date"].max())
    missing_days = len(expected) - frame["local_date"].nunique()
    review_days = round(pressure_review_share(frame) * len(frame))
    station_id = normalise_station_id(args.station_id)
    print(
        f"{args.file.name}: {len(frame)} giorni dal {frame['local_date'].min()} "
        f"al {frame['local_date'].max()}, {missing_days} mancanti, "
        f"{review_days} con pressione da verificare"
    )
    if args.dry_run:
        print("Controllo completato: nessuna scrittura eseguita")
        return 0

    ensure_schema()
    engine = get_engine()
    register_station(
        station_id=station_id,
        display_name=args.name,
        latitude=args.latitude,
        longitude=args.longitude,
        elevation_m=args.elevation,
        timezone=args.timezone,
        source="ecowitt",
        role="secondary",
        enabled=True,
        engine=engine,
    )
    rows = upsert_daily_summaries(frame, station_id, engine)
    print(f"Importazione completata: {rows} riepiloghi giornalieri")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
