# migrate_station_raw_schema.py
# Allinea lo schema "station_raw" su Postgres (o lo crea se manca)
import os
from sqlalchemy import create_engine, text, inspect

DB_URL = os.getenv("DATABASE_URL")
if not DB_URL:
    raise SystemExit("DATABASE_URL mancante (usa quello del tuo .env)")

eng = create_engine(DB_URL, future=True, pool_pre_ping=True)
insp = inspect(eng)

with eng.begin() as con:
    # Crea tabella se non esiste (schema 'giusto')
    if "station_raw" not in insp.get_table_names():
        con.execute(text("""
            CREATE TABLE station_raw (
                ts_utc TIMESTAMPTZ PRIMARY KEY,
                temp_c DOUBLE PRECISION,
                hum DOUBLE PRECISION,
                press_hpa DOUBLE PRECISION,
                wind_ms DOUBLE PRECISION,
                winddir DOUBLE PRECISION,
                rain_mm DOUBLE PRECISION
            );
        """))
        print("Creata tabella station_raw (schema nuovo).")
    else:
        # Aggiungi colonne mancanti per allineare allo schema nuovo
        cols = {c["name"].lower(): c for c in insp.get_columns("station_raw")}
        def add(coldef):
            print("ADD COLUMN", coldef)
            con.execute(text(f"ALTER TABLE station_raw ADD COLUMN IF NOT EXISTS {coldef};"))
        if "ts_utc" not in cols:
            add("ts_utc TIMESTAMPTZ")
            # se esiste una colonna Time, prova a copiarla
            if "time" in cols:
                con.execute(text("UPDATE station_raw SET ts_utc = CAST(Time AS TIMESTAMPTZ) WHERE ts_utc IS NULL;"))
            # crea indice/PK (ignora errore se già c'è)
            try:
                con.execute(text("ALTER TABLE station_raw ADD PRIMARY KEY (ts_utc);"))
            except Exception:
                pass
        for name, coldef in [
            ("temp_c", "DOUBLE PRECISION"),
            ("hum", "DOUBLE PRECISION"),
            ("press_hpa", "DOUBLE PRECISION"),
            ("wind_ms", "DOUBLE PRECISION"),
            ("winddir", "DOUBLE PRECISION"),
            ("rain_mm", "DOUBLE PRECISION"),
        ]:
            if name not in cols:
                add(f"{name} {coldef}")
print("Schema station_raw allineato.")
