"""Backward-compatible seven-day backfill launcher."""

from ingest_all import run_all

if __name__ == "__main__":
    outcome = run_all(backfill_hours=168, force_forecast=True)
    print(outcome)
    raise SystemExit(1 if outcome["errors"] else 0)
