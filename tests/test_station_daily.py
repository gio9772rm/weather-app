from __future__ import annotations

import base64
import gzip

import pandas as pd
from sqlalchemy import text

from config import settings
from data_access import load_station_daily_summaries
from station_daily import (
    aggregate_observations_daily,
    combine_daily_sources,
    import_daily_bootstrap_from_env,
    parse_ecowitt_daily_export,
    upsert_daily_summaries,
)
from station_registry import register_station


def test_ecowitt_daily_export_uses_calendar_day_and_flags_pressure(
    monkeypatch, tmp_path
):
    columns = pd.MultiIndex.from_tuples(
        [
            ("Time", "Unnamed: 0_level_1"),
            ("Outdoor", "Temperature(℃)"),
            ("Outdoor", "Temperature Low(℃)"),
            ("Outdoor", "Temperature High(℃)"),
            ("Outdoor", "Humidity(%)"),
            ("Rainfall", "Daily(mm)"),
            ("Pressure", "Relative(hPa)"),
            ("Pressure", "Relative Low(hPa)"),
            ("Pressure", "Relative High(hPa)"),
            ("Pressure", "Absolute(hPa)"),
        ]
    )
    source = pd.DataFrame(
        [
            ["2026-03-28 01:00", 12.0, 5.0, 18.0, 80, 4.2, 1014, 1010, 1016, 1013],
            ["2026-05-27 02:00", 22.0, 15.0, 29.0, 70, 0.0, 1015, 1012, 1017, 1028],
        ],
        columns=columns,
    )
    monkeypatch.setattr(pd, "read_excel", lambda *args, **kwargs: source)

    parsed = parse_ecowitt_daily_export(tmp_path / "history.xlsx")

    assert parsed["local_date"].tolist() == ["2026-03-28", "2026-05-27"]
    assert parsed["rain_mm"].tolist() == [4.2, 0.0]
    assert "pressure_calibration_review" not in parsed.iloc[0]["data_quality"]
    assert "pressure_calibration_review" in parsed.iloc[1]["data_quality"]


def test_daily_import_is_station_scoped_and_idempotent(sqlite_engine):
    register_station(
        station_id="secondary-one",
        display_name="Secondaria",
        latitude=44.8,
        longitude=12.1,
        elevation_m=-1,
        timezone="Europe/Rome",
        source="ecowitt",
        role="secondary",
        engine=sqlite_engine,
    )
    frame = pd.DataFrame(
        {
            "local_date": ["2026-08-29", "2026-08-30"],
            "temp_mean_c": [26.8, 25.0],
            "rain_mm": [0.0, 0.0],
            "source": ["ecowitt_daily_export"] * 2,
            "data_quality": ["historical_daily_summary"] * 2,
            "imported_at": ["2026-09-01T08:00:00Z"] * 2,
        }
    )

    assert upsert_daily_summaries(frame, "secondary-one", sqlite_engine) == 2
    frame.loc[0, "temp_mean_c"] = 27.1
    assert upsert_daily_summaries(frame, "secondary-one", sqlite_engine) == 2

    with sqlite_engine.connect() as connection:
        rows = (
            connection.execute(
                text(
                    "SELECT station_id,local_date,temp_mean_c "
                    "FROM station_daily_summaries ORDER BY local_date"
                )
            )
            .mappings()
            .all()
        )
    assert len(rows) == 2
    assert rows[0]["station_id"] == "secondary-one"
    assert rows[0]["temp_mean_c"] == 27.1


def test_private_daily_bootstrap_is_imported_only_once(sqlite_engine):
    register_station(
        station_id="secondary-bootstrap",
        display_name="Secondaria bootstrap",
        latitude=44.8,
        longitude=12.1,
        elevation_m=-1,
        timezone="Europe/Rome",
        source="ecowitt",
        role="secondary",
        engine=sqlite_engine,
    )
    frame = pd.DataFrame(
        {
            "local_date": ["2026-08-29", "2026-08-30"],
            "temp_mean_c": [26.8, 25.0],
            "humidity_mean": [68, 72],
            "rain_mm": [0.0, 1.2],
            "source": ["ecowitt_daily_export"] * 2,
            "data_quality": [
                "historical_daily_summary",
                "historical_daily_summary;pressure_calibration_review",
            ],
        }
    )
    payload = base64.b64encode(
        gzip.compress(frame.to_csv(index=False).encode("utf-8"))
    ).decode("ascii")

    first = import_daily_bootstrap_from_env(
        "secondary-bootstrap", sqlite_engine, payload=payload
    )
    second = import_daily_bootstrap_from_env(
        "secondary-bootstrap", sqlite_engine, payload=payload
    )

    assert first is not None
    assert first["rows"] == 2
    assert first["first_date"] == "2026-08-29"
    assert first["last_date"] == "2026-08-30"
    assert second is None
    with sqlite_engine.connect() as connection:
        rows = connection.execute(
            text(
                "SELECT local_date,temp_mean_c,data_quality "
                "FROM station_daily_summaries WHERE station_id='secondary-bootstrap' "
                "ORDER BY local_date"
            )
        ).mappings().all()
    assert len(rows) == 2
    assert rows[1]["temp_mean_c"] == 25.0
    assert "pressure_calibration_review" in rows[1]["data_quality"]


def test_live_daily_aggregate_overrides_import_for_same_day():
    imported = pd.DataFrame(
        {
            "station_id": ["secondary-one"],
            "local_date": ["2026-08-30"],
            "temp_mean_c": [20.0],
            "source": ["ecowitt_daily_export"],
        }
    )
    observations = pd.DataFrame(
        {
            "time": pd.to_datetime(
                ["2026-08-30T10:00:00Z", "2026-08-30T11:00:00Z"], utc=True
            ),
            "temp_c": [24.0, 26.0],
            "humidity": [70, 74],
            "rain_mm": [0.3, 0.2],
            "winddir": [350, 10],
        }
    )
    live = aggregate_observations_daily(observations, "secondary-one", "Europe/Rome")

    combined = combine_daily_sources(imported, live)

    assert len(combined) == 1
    assert combined.iloc[0]["source"] == "station_observations"
    assert combined.iloc[0]["temp_mean_c"] == 25.0
    assert combined.iloc[0]["rain_mm"] == 0.5
    assert combined.iloc[0]["wind_dir_deg"] in {0.0, 360.0}


def test_primary_daily_comparison_falls_back_to_authoritative_raw_archive(
    sqlite_engine,
):
    register_station(
        station_id=settings.station_id,
        display_name="Primaria",
        latitude=41.9,
        longitude=12.5,
        elevation_m=20,
        timezone="Europe/Rome",
        source="ecowitt",
        role="primary",
        engine=sqlite_engine,
    )
    observed_at = pd.Timestamp.now(tz="UTC").floor("10min")
    with sqlite_engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO station_raw (time,temp_c,humidity,rain_mm,source,data_quality) "
                "VALUES (:time,21.5,60,0.4,'ecowitt_cloud','ok')"
            ),
            {"time": observed_at.strftime("%Y-%m-%dT%H:%M:%SZ")},
        )

    daily = load_station_daily_summaries(30)
    primary = daily[daily["station_id"].eq(settings.station_id)]

    assert len(primary) == 1
    assert primary.iloc[0]["source"] == "station_raw"
    assert primary.iloc[0]["temp_mean_c"] == 21.5
    assert primary.iloc[0]["rain_mm"] == 0.4
