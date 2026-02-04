import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

_ENGINE_CACHE: dict[tuple[str, bool], Engine] = {}

def get_db_url() -> str:
    url = (os.getenv("DATABASE_URL") or "").strip()
    if url:
        # normalizza eventuale "postgres://" -> "postgresql://"
        if url.startswith("postgres://"):
            url = url.replace("postgres://", "postgresql://", 1)
        # evita placeholder copiati in env
        if "USER:PASS@HOST:PORT/DBNAME" in url:
            url = ""
    if url:
        return url
    sqlite_path = (os.getenv("SQLITE_PATH") or "data/weather.db").strip()
    return f"sqlite:///{sqlite_path}"

def get_engine(echo: bool = False) -> Engine:
    """Ritorna un Engine riusabile (cache per url+echo)."""
    db_url = get_db_url()
    key = (db_url, bool(echo))
    if key in _ENGINE_CACHE:
        return _ENGINE_CACHE[key]

    connect_args = {}
    if db_url.startswith("sqlite"):
        connect_args["check_same_thread"] = False

    eng = create_engine(db_url, echo=echo, pool_pre_ping=True, future=True, connect_args=connect_args)
    _ENGINE_CACHE[key] = eng
    return eng

def ensure_schema() -> None:
    """Esegue schema.sql in modo idempotente (CREATE TABLE IF NOT EXISTS)."""
    engine = get_engine()
    schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
    with open(schema_path, "r", encoding="utf-8") as f:
        ddl = f.read()
    with engine.begin() as conn:
        conn.execute(text(ddl))
