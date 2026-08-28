from __future__ import annotations

import pandas as pd

from monthly_report import (
    monthly_csv_bytes,
    monthly_pdf_bytes,
    monthly_summary,
    report_filename,
)


def _month_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "time": pd.to_datetime(
                [
                    "2026-08-01T00:00:00Z",
                    "2026-08-01T12:00:00Z",
                    "2026-08-02T00:00:00Z",
                    "2026-08-02T12:00:00Z",
                ],
                utc=True,
            ),
            "temp_c": [20.0, 30.0, 18.0, 28.0],
            "humidity": [70, 40, 75, 45],
            "pressure_hpa": [1012, 1010, 1014, 1011],
            "wind_kmh": [5, 10, 4, 12],
            "windgust_kmh": [8, 18, 7, 22],
            "winddir": [90, 120, 80, 140],
            "rain_mm": [1.0, 2.0, 0.0, 5.0],
            "rain_rate_mm_h": [0, 1, 0, 2],
            "solar_w_m2": [0, 800, 0, 750],
            "uv_index": [0, 7, 0, 6],
            "source": "ecowitt",
            "data_quality": "ok",
        }
    )


def test_monthly_summary_csv_and_pdf_are_consistent():
    frame = _month_frame()
    summary = monthly_summary(
        frame,
        2026,
        8,
        timezone="UTC",
        station_name="Stazione primaria",
    )

    assert summary["samples"] == 4
    assert summary["temp_min_c"] == 18.0
    assert summary["temp_mean_c"] == 24.0
    assert summary["temp_max_c"] == 30.0
    assert summary["rain_total_mm"] == 8.0
    assert summary["rainiest_day_mm"] == 5.0

    csv_payload = monthly_csv_bytes(frame, 2026, 8, timezone="UTC")
    assert csv_payload.startswith(b"time_local,time_utc,temp_c")
    assert b"latitude" not in csv_payload
    assert b"longitude" not in csv_payload

    pdf_payload = monthly_pdf_bytes(
        frame,
        2026,
        8,
        timezone="UTC",
        station_name="Stazione primaria",
    )
    assert pdf_payload.startswith(b"%PDF-")
    assert len(pdf_payload) > 2_000
    assert report_filename("Nord / Futuro", 2026, 8, "PDF") == "meteo-nord-futuro-2026-08.pdf"
