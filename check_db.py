# -*- coding: utf-8 -*-
"""
check_db.py — Ispeziona il database meteo
Stampa tabelle disponibili, colonne e 5 righe di esempio per:
- station_3h (se esiste)
- station_raw (se esiste)
- forecast_ow
- meta

Usa DATABASE_URL (Postgres, ecc.) oppure fallback a SQLite in data/weather.db.
"""

import os
import re
import sys
import pandas as pd
from sqlalchemy import create_engine, inspect
from dotenv import load_dotenv

def mask_url(u: str) -> str:
    if not u:
        return u
    return re.sub(r"://([^:]+):([^@]+)@", r"://\\1:***@", u)

def normalize_db_url(raw: str) -> str:
    u = (raw or "").strip()
    if not u:
        return u
    if u.startswith("postgres://"):
        u = "postgresql+psycopg2://" + u[len("postgres://"):]
    return u

def main():
    load_dotenv()
    db_url = normalize_db_url(os.getenv("DATABASE_URL", ""))
    sqlite_path = os.getenv("SQLITE_PATH", "data/weather.db")
    if not db_url:
        db_url = f"sqlite:///{sqlite_path}"
    print(f"Connessione: {mask_url(db_url)}\n")

    eng = create_engine(db_url, future=True)
    insp = inspect(eng)

    try:
        tables = insp.get_table_names()
    except Exception as e:
        print("Errore nel leggere le tabelle:", e)
        sys.exit(2)

    print("Tabelle trovate:")
    for t in tables:
        print(" -", t)
    print()

    def show(table: str):
        if table not in tables:
            print(f"[i] Tabella '{table}' non trovata.\n")
            return
        with eng.connect() as cx:
            try:
                df = pd.read_sql(f"SELECT * FROM {table} LIMIT 5;", cx)
            except Exception as e:
                print(f"[!] Errore leggendo '{table}':", e, "\n")
                return
        print(f"=== {table} ===")
        print("Colonne:", list(df.columns))
        try:
            print("Dtypes:\n", df.dtypes)
        except Exception:
            pass
        print("Sample (max 5 righe):")
        print(df.head())
        print()

    show("station_3h")
    show("station_raw")
    show("forecast_ow")
    show("meta")

if __name__ == "__main__":
    main()
