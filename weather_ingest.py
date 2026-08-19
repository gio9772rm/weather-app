"""Backward-compatible launcher for the single Meteo V3 pipeline."""

from ingest_all import main

if __name__ == "__main__":
    raise SystemExit(main())
