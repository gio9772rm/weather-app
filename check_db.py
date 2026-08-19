"""Privacy-safe database status report (no credentials or sample values)."""

from __future__ import annotations

from sqlalchemy import inspect, text
from sqlalchemy.exc import SQLAlchemyError

from db import ensure_schema, get_engine


def main() -> int:
    try:
        ensure_schema()
        engine = get_engine()
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        print("Database raggiungibile.")
        with engine.connect() as connection:
            for table in sorted(tables):
                count = connection.execute(
                    text(f"SELECT COUNT(*) FROM {table}")
                ).scalar_one()
                print(f"- {table}: {count} righe")
            latest_station = connection.execute(
                text("SELECT MAX(time) FROM station_raw")
            ).scalar_one_or_none()
            latest_forecast = connection.execute(
                text("SELECT MAX(issued_at) FROM forecast_blend")
            ).scalar_one_or_none()
        print(f"Ultima osservazione: {latest_station or 'nessuna'}")
        print(f"Ultima previsione: {latest_forecast or 'nessuna'}")
        return 0
    except SQLAlchemyError as exc:
        print(f"Database non raggiungibile: {type(exc).__name__}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
