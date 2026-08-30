from __future__ import annotations

import pandas as pd
from sqlalchemy import text

from data_access import load_forecast


def test_load_forecast_prepends_two_hour_archived_tail(sqlite_engine):
    now = pd.Timestamp.now(tz="UTC")
    current_hour = now.floor("h")
    current_rows = [
        {
            "valid_time": (current_hour + pd.Timedelta(hours=offset)).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            ),
            "issued_at": current_hour.strftime("%Y-%m-%dT%H:%M:%SZ"),
            # Reproduce a real provider edge case: the new emission retains
            # the previous hour but no longer supplies its temperature.
            "temp": None if offset == -1 else 20 + offset,
        }
        for offset in range(-1, 4)
    ]
    history_rows = []
    for offset in (-2, -1):
        valid_time = current_hour + pd.Timedelta(hours=offset)
        history_rows.append(
            {
                "valid_time": valid_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "issued_at": (valid_time - pd.Timedelta(hours=1)).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                ),
                "temp": 10 + offset,
            }
        )
    with sqlite_engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO forecast_blend (valid_time,issued_at,temp_c,method) "
                "VALUES (:valid_time,:issued_at,:temp,'current')"
            ),
            current_rows,
        )
        connection.execute(
            text(
                "INSERT INTO forecast_blend_history "
                "(valid_time,issued_at,temp_c,method) "
                "VALUES (:valid_time,:issued_at,:temp,'archived')"
            ),
            history_rows,
        )

    forecast = load_forecast(hours=3)

    assert forecast.iloc[0]["valid_time"] == current_hour - pd.Timedelta(hours=2)
    assert forecast.iloc[0]["chart_origin"] == "previsione_archiviata"
    previous_hour = forecast.loc[
        forecast["valid_time"].eq(current_hour - pd.Timedelta(hours=1))
    ].iloc[0]
    assert previous_hour["chart_origin"] == "previsione_archiviata"
    assert previous_hour["temp_c"] == 9
    assert (
        forecast.loc[forecast["valid_time"].eq(current_hour), "chart_origin"].iloc[0]
        == "blend_corrente"
    )
    assert forecast["valid_time"].is_monotonic_increasing
