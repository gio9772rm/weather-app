from __future__ import annotations

import pandas as pd
from sqlalchemy import text

from measured_pollen import (
    archive_measured_pollen,
    nearest_station,
    parse_concentrations,
    parse_stations,
)


def test_pollnet_uses_nearest_station_and_avoids_family_double_count(sqlite_engine):
    stations = parse_stations(
        {
            "features": [
                {
                    "properties": {
                        "STAT_ID": 195,
                        "STAT_CODE": "RM1",
                        "STAT_NAME_I": "Roma Tor Vergata",
                        "REGI_NAME_I": "Lazio",
                        "LATITUDE": "41.8543",
                        "LONGITUDE": "12.6041",
                    }
                },
                {
                    "properties": {
                        "STAT_ID": 118,
                        "STAT_CODE": "BO1",
                        "STAT_NAME_I": "Bologna",
                        "REGI_NAME_I": "Emilia Romagna",
                        "LATITUDE": "44.4914",
                        "LONGITUDE": "11.3694",
                    }
                },
            ]
        }
    )
    station = nearest_station(stations, 41.9028, 12.4964)
    assert station["station_code"] == "RM1"

    payload = {
        "features": [
            {
                "properties": {
                    "PART_LEVEL": 2,
                    "PART_NAME_L": "Gramineae",
                    "REMA_CONCENTRATION": 32,
                    "REMA_DATE": "2026-08-20Z",
                }
            },
            {
                "properties": {
                    "PART_LEVEL": 3,
                    "PART_NAME_L": "Poa",
                    "REMA_CONCENTRATION": 32,
                    "REMA_DATE": "2026-08-20Z",
                }
            },
        ]
    }
    frame = parse_concentrations(payload, station, pd.Timestamp("2026-08-27T08:00:00Z"))
    assert frame["metric"].tolist() == ["pollen_gramineae"]
    assert frame["is_modelled"].eq(0).all()

    archive_measured_pollen(frame, sqlite_engine)
    with sqlite_engine.connect() as connection:
        row = (
            connection.execute(
                text("SELECT source,metric,value FROM environment_observations")
            )
            .mappings()
            .one()
        )
    assert row == {"source": "pollnet", "metric": "pollen_gramineae", "value": 32.0}
