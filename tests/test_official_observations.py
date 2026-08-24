from __future__ import annotations

from dataclasses import replace

import pandas as pd
from sqlalchemy import text

from config import Settings
from forecast_blend import _score_lookup, score_forecasts_against_references
from official_observations import (
    archive_official_observations,
    parse_metar_payload,
    relative_humidity,
)


def _settings() -> Settings:
    return replace(
        Settings.from_env(),
        latitude=41.9067235,
        longitude=12.3899144,
        metar_station_ids=("LIRF", "LIRA"),
    )


def test_parse_metar_normalises_official_weather_values():
    payload = [
        {
            "icaoId": "LIRF",
            "obsTime": 1787599200,
            "temp": 27,
            "dewp": 21,
            "wdir": 260,
            "wspd": 4,
            "wgst": 8,
            "visib": "6+",
            "altim": 1016,
            "precip": 0.01,
            "wxString": "-RA",
            "lat": 41.8,
            "lon": 12.239,
            "elev": 2,
            "name": "Rome/Fiumicino",
            "cover": "BKN",
            "clouds": [{"cover": "BKN", "base": 2500}],
            "rawOb": "METAR LIRF 241920Z 26004G08KT 9999 -RA BKN025 27/21 Q1016",
            "qcField": 0,
        }
    ]

    frame = parse_metar_payload(
        payload, _settings(), pd.Timestamp("2026-08-24T20:00:00Z")
    )

    assert len(frame) == 1
    row = frame.iloc[0]
    assert row["source"] == "awc_metar"
    assert row["station_id"] == "LIRF"
    assert row["wind_kmh"] == 4 * 1.852
    assert row["wind_gust_kmh"] == 8 * 1.852
    assert row["rain_mm"] == 0.254
    assert row["precip_observed"] == 1
    assert row["clouds"] == 75
    assert row["visibility_m"] == 6 * 1609.344
    assert 69 < row["humidity"] < 71
    assert 10 < row["distance_km"] < 25


def test_relative_humidity_requires_both_temperature_and_dewpoint():
    assert relative_humidity(20, None) is None
    assert relative_humidity(20, 20) == 100.0


def test_official_archive_is_idempotent_and_separate_from_ecowitt(sqlite_engine):
    frame = parse_metar_payload(
        [
            {
                "icaoId": "LIRA",
                "obsTime": 1787601000,
                "temp": 27,
                "dewp": 20,
                "wdir": 230,
                "wspd": 3,
                "altim": 1017,
                "lat": 41.808,
                "lon": 12.585,
                "elev": 101,
                "name": "Rome/Ciampino",
                "cover": "CAVOK",
                "rawOb": "METAR LIRA 241950Z 23003KT CAVOK 27/20 Q1017",
            }
        ],
        _settings(),
        pd.Timestamp("2026-08-24T20:00:00Z"),
    )

    assert archive_official_observations(frame, sqlite_engine) == 1
    assert archive_official_observations(frame, sqlite_engine) == 1
    with sqlite_engine.connect() as connection:
        assert connection.execute(
            text("SELECT COUNT(*) FROM official_observations")
        ).scalar_one() == 1
        assert connection.execute(
            text("SELECT COUNT(*) FROM station_raw")
        ).scalar_one() == 0


def test_ecowitt_score_remains_primary_when_official_score_is_available():
    local = pd.DataFrame(
        [
            {
                "provider": "open_meteo",
                "variable": "temp_c",
                "horizon": "0-24h",
                "n": 100,
                "bias": 2.0,
                "mae": 3.0,
            }
        ]
    )
    official = pd.DataFrame(
        [
            {
                "provider": "open_meteo",
                "variable": "temp_c",
                "horizon": "0-24h",
                "n": 100,
                "bias": -2.0,
                "mae": 1.0,
                "reference_weight": 1.0,
            }
        ]
    )

    bias, mae = _score_lookup(
        local,
        "open_meteo",
        "temp_c",
        "0-24h",
        official,
        official_max_share=0.20,
    )

    assert round(bias, 3) == 1.333
    assert round(mae, 3) == 2.667


def test_official_score_learns_site_offset_before_evaluating_forecast(sqlite_engine):
    now = pd.Timestamp.now(tz="UTC").floor("h")
    issued = now - pd.Timedelta(hours=48)
    station_rows = []
    official_rows = []
    forecast_rows = []
    for offset in range(30, 0, -1):
        moment = now - pd.Timedelta(hours=offset)
        local_temperature = 20.0 + offset / 20.0
        station_rows.append(
            {
                "time": moment.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "temp": local_temperature,
            }
        )
        official_rows.append(
            {
                "time": moment.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "temp": local_temperature + 2.0,
                "fetched": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
        )
        forecast_rows.append(
            {
                "issued": issued.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "valid": moment.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "temp": local_temperature + 1.0,
                "fetched": issued.strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
        )
    with sqlite_engine.begin() as connection:
        connection.execute(
            text("INSERT INTO station_raw (time,temp_c) VALUES (:time,:temp)"),
            station_rows,
        )
        connection.execute(
            text(
                "INSERT INTO official_observations "
                "(source,station_id,time,distance_km,temp_c,fetched_at) "
                "VALUES ('awc_metar','LIRF',:time,17,:temp,:fetched)"
            ),
            official_rows,
        )
        connection.execute(
            text(
                "INSERT INTO forecast_runs "
                "(provider,model,issued_at,valid_time,temp_c,fetched_at) "
                "VALUES ('open_meteo','test',:issued,:valid,:temp,:fetched)"
            ),
            forecast_rows,
        )

    scores = score_forecasts_against_references(
        replace(
            _settings(),
            score_lookback_days=7,
            official_min_overlap_samples=12,
        ),
        sqlite_engine,
    )

    temperature = scores[scores["variable"] == "temp_c"]
    assert not temperature.empty
    assert temperature["transfer_bias"].round(3).eq(2.0).all()
    assert temperature["bias"].round(3).eq(1.0).all()
    assert temperature["reference_weight"].gt(0).all()
