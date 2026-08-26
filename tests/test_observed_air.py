from __future__ import annotations

import pandas as pd
from sqlalchemy import text

from observed_air import archive_observed_air, normalise_eea_observations


def test_eea_normaliser_selects_nearest_station_for_each_pollutant(sqlite_engine):
    measurements = pd.DataFrame(
        [
            {
                "sampling_point": "SPO.NEAR_5",
                "pollutant_id": 5,
                "time": pd.Timestamp("2026-08-26T08:00:00Z"),
                "value": 21,
                "Unit": "µg/m³",
            },
            {
                "sampling_point": "SPO.FAR_5",
                "pollutant_id": 5,
                "time": pd.Timestamp("2026-08-26T09:00:00Z"),
                "value": 50,
                "Unit": "µg/m³",
            },
            {
                "sampling_point": "SPO.FAR_8",
                "pollutant_id": 8,
                "time": pd.Timestamp("2026-08-26T09:00:00Z"),
                "value": 18,
                "Unit": "µg/m³",
            },
        ]
    )
    metadata = {
        "SPO.NEAR_5": {
            "station_id": "NEAR",
            "station_name": "Roma vicino",
            "latitude": 41.91,
            "longitude": 12.39,
        },
        "SPO.FAR_5": {
            "station_id": "FAR",
            "station_name": "Roma lontano",
            "latitude": 42.2,
            "longitude": 12.8,
        },
        "SPO.FAR_8": {
            "station_id": "FAR",
            "station_name": "Roma lontano",
            "latitude": 42.2,
            "longitude": 12.8,
        },
    }
    frame = normalise_eea_observations(
        measurements,
        metadata,
        latitude=41.9067,
        longitude=12.3899,
        fetched_at=pd.Timestamp("2026-08-26T09:10:00Z"),
    )
    assert set(frame["metric"]) == {"pm10", "nitrogen_dioxide"}
    assert frame.loc[frame["metric"] == "pm10", "station_id"].iloc[0] == "NEAR"
    assert frame["is_modelled"].eq(0).all()
    assert frame["quality_flag"].eq("UTD_preliminare").all()

    archive_observed_air(frame, sqlite_engine)
    with sqlite_engine.connect() as connection:
        assert (
            connection.execute(
                text("SELECT COUNT(*) FROM environment_observations")
            ).scalar_one()
            == 2
        )
        assert (
            connection.execute(text("SELECT COUNT(*) FROM station_raw")).scalar_one()
            == 0
        )
