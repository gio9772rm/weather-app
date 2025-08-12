import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

def get_db_url() -> str:
    url = os.getenv("DATABASE_URL")
    if url:
        # normalizza eventuale "postgres://" -> "postgresql://"
        if url.startswith("postgres://"):
            url = url.replace("postgres://", "postgresql://", 1)
        return url
    sqlite_path = os.getenv("SQLITE_PATH", "data/weather.sqlite")
    return f"sqlite:///{sqlite_path}"

def get_engine(echo: bool = False) -> Engine:
    db_url = get_db_url()
    connect_args = {}
    if db_url.startswith("sqlite"):
        connect_args["check_same_thread"] = False
    return create_engine(db_url, echo=echo, pool_pre_ping=True, connect_args=connect_args)

def ensure_schema() -> None:
    """Esegue schema.sql in modo idempotente (CREATE TABLE IF NOT EXISTS)."""
    engine = get_engine()
    with engine.begin() as conn:
        with open("schema.sql", "r", encoding="utf-8") as f:
            conn.execute(text(f.read()))  # lo schema definisce: station_3h, forecast_ow, meta
