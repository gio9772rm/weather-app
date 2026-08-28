"""Monthly station exports for the dashboard.

The PDF deliberately contains no station coordinates.  Ecowitt remains the
measurement source; regional and model data are only listed as independent
context when a caller supplies their health status.
"""

from __future__ import annotations

from io import BytesIO
from typing import Any
from xml.sax.saxutils import escape

import numpy as np
import pandas as pd

MONTHS_IT = (
    "gennaio",
    "febbraio",
    "marzo",
    "aprile",
    "maggio",
    "giugno",
    "luglio",
    "agosto",
    "settembre",
    "ottobre",
    "novembre",
    "dicembre",
)


def _number(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _local_month(
    frame: pd.DataFrame, year: int, month: int, timezone: str
) -> pd.DataFrame:
    if frame is None or frame.empty or "time" not in frame:
        return pd.DataFrame()
    data = frame.copy()
    data["time"] = pd.to_datetime(data["time"], utc=True, errors="coerce")
    data = data.dropna(subset=["time"])
    data["time_local"] = data["time"].dt.tz_convert(timezone)
    return data[
        data["time_local"].dt.year.eq(int(year))
        & data["time_local"].dt.month.eq(int(month))
    ].sort_values("time_local")


def monthly_summary(
    frame: pd.DataFrame,
    year: int,
    month: int,
    *,
    timezone: str,
    station_name: str,
) -> dict[str, Any]:
    """Return deterministic, unit-aware headline statistics for one month."""
    data = _local_month(frame, year, month, timezone)
    label = f"{MONTHS_IT[int(month) - 1]} {int(year)}"
    summary: dict[str, Any] = {
        "station_name": station_name,
        "period": label,
        "year": int(year),
        "month": int(month),
        "samples": len(data),
        "coverage_pct": 0.0,
        "temp_min_c": np.nan,
        "temp_mean_c": np.nan,
        "temp_max_c": np.nan,
        "rain_total_mm": np.nan,
        "rainiest_day": None,
        "rainiest_day_mm": np.nan,
        "wind_mean_kmh": np.nan,
        "gust_max_kmh": np.nan,
        "solar_max_w_m2": np.nan,
        "uv_max": np.nan,
        "first_sample": None,
        "last_sample": None,
    }
    if data.empty:
        return summary

    start = pd.Timestamp(year=int(year), month=int(month), day=1, tz=timezone)
    end = start + pd.offsets.MonthBegin(1)
    now = pd.Timestamp.now(tz=timezone)
    covered_end = min(end, now) if start <= now < end else end
    expected = max(1, int((covered_end - start).total_seconds() // 300))
    observed = data["time_local"].dt.floor("5min").nunique()
    summary["coverage_pct"] = min(100.0, observed / expected * 100.0)
    summary["first_sample"] = data["time_local"].iloc[0]
    summary["last_sample"] = data["time_local"].iloc[-1]

    if "temp_c" in data:
        temperature = _number(data["temp_c"])
        summary["temp_min_c"] = float(temperature.min())
        summary["temp_mean_c"] = float(temperature.mean())
        summary["temp_max_c"] = float(temperature.max())
    if "rain_mm" in data:
        rain = _number(data["rain_mm"]).clip(lower=0)
        summary["rain_total_mm"] = float(rain.sum(min_count=1))
        daily = rain.groupby(data["time_local"].dt.date).sum(min_count=1).dropna()
        if not daily.empty:
            summary["rainiest_day"] = daily.idxmax()
            summary["rainiest_day_mm"] = float(daily.max())
    if "wind_kmh" in data:
        summary["wind_mean_kmh"] = float(_number(data["wind_kmh"]).mean())
    if "windgust_kmh" in data:
        summary["gust_max_kmh"] = float(_number(data["windgust_kmh"]).max())
    if "solar_w_m2" in data:
        summary["solar_max_w_m2"] = float(_number(data["solar_w_m2"]).max())
    if "uv_index" in data:
        summary["uv_max"] = float(_number(data["uv_index"]).max())
    return summary


def monthly_csv_bytes(
    frame: pd.DataFrame, year: int, month: int, *, timezone: str
) -> bytes:
    """Export only station measurements for the selected local calendar month."""
    data = _local_month(frame, year, month, timezone)
    if data.empty:
        return b"time_local,time_utc\n"
    output = pd.DataFrame(
        {
            "time_local": data["time_local"].dt.strftime("%Y-%m-%d %H:%M:%S%z"),
            "time_utc": data["time"].dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
    )
    for column in (
        "temp_c",
        "humidity",
        "pressure_hpa",
        "wind_kmh",
        "windgust_kmh",
        "winddir",
        "rain_mm",
        "rain_rate_mm_h",
        "rain_total_mm",
        "solar_w_m2",
        "uv_index",
        "source",
        "data_quality",
    ):
        if column in data:
            output[column] = data[column].to_numpy()
    return output.to_csv(index=False, lineterminator="\n").encode("utf-8")


def _fmt(value: Any, digits: int = 1, suffix: str = "") -> str:
    number = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return "-" if pd.isna(number) else f"{float(number):.{digits}f}{suffix}"


def monthly_pdf_bytes(
    frame: pd.DataFrame,
    year: int,
    month: int,
    *,
    timezone: str,
    station_name: str,
    source_health: pd.DataFrame | None = None,
) -> bytes:
    """Build a compact, printable PDF report without exposing coordinates."""
    from reportlab.lib import colors
    from reportlab.lib.enums import TA_LEFT
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
    from reportlab.lib.units import mm
    from reportlab.platypus import (
        Paragraph,
        SimpleDocTemplate,
        Spacer,
        Table,
        TableStyle,
    )

    data = _local_month(frame, year, month, timezone)
    summary = monthly_summary(
        data,
        year,
        month,
        timezone=timezone,
        station_name=station_name,
    )
    buffer = BytesIO()
    document = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        rightMargin=16 * mm,
        leftMargin=16 * mm,
        topMargin=16 * mm,
        bottomMargin=15 * mm,
        title=f"Rapporto meteo {summary['period']}",
        author="Meteo V4",
    )
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "WeatherTitle",
        parent=styles["Title"],
        fontName="Helvetica-Bold",
        fontSize=21,
        leading=25,
        textColor=colors.HexColor("#10243d"),
        alignment=TA_LEFT,
        spaceAfter=4 * mm,
    )
    subtitle_style = ParagraphStyle(
        "WeatherSubtitle",
        parent=styles["Normal"],
        fontSize=9.5,
        leading=13,
        textColor=colors.HexColor("#52657b"),
        spaceAfter=5 * mm,
    )
    heading_style = ParagraphStyle(
        "WeatherHeading",
        parent=styles["Heading2"],
        fontSize=13,
        leading=16,
        textColor=colors.HexColor("#1d4ed8"),
        spaceBefore=4 * mm,
        spaceAfter=2.5 * mm,
    )
    note_style = ParagraphStyle(
        "WeatherNote",
        parent=styles["Normal"],
        fontSize=8.2,
        leading=11,
        textColor=colors.HexColor("#64748b"),
    )

    story: list[Any] = [
        Paragraph("Rapporto meteo mensile", title_style),
        Paragraph(
            f"<b>{escape(station_name)}</b> - {escape(summary['period'].title())}<br/>"
            "Misure locali Ecowitt. Le fonti istituzionali restano riferimenti indipendenti.",
            subtitle_style,
        ),
    ]
    metrics = [
        ["Copertura", f"{summary['coverage_pct']:.1f}%", "Campioni", str(summary["samples"])],
        ["Temperatura min", _fmt(summary["temp_min_c"], 1, " °C"), "Temperatura media", _fmt(summary["temp_mean_c"], 1, " °C")],
        ["Temperatura max", _fmt(summary["temp_max_c"], 1, " °C"), "Pioggia totale", _fmt(summary["rain_total_mm"], 1, " mm")],
        ["Vento medio", _fmt(summary["wind_mean_kmh"], 1, " km/h"), "Raffica massima", _fmt(summary["gust_max_kmh"], 1, " km/h")],
        ["Radiazione max", _fmt(summary["solar_max_w_m2"], 0, " W/m²"), "UV massimo", _fmt(summary["uv_max"], 1)],
    ]
    metric_table = Table(metrics, colWidths=[42 * mm, 40 * mm, 43 * mm, 40 * mm])
    metric_table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#f1f5f9")),
                ("BOX", (0, 0), (-1, -1), 0.6, colors.HexColor("#cbd5e1")),
                ("INNERGRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#dbe4ee")),
                ("FONTNAME", (0, 0), (-1, -1), "Helvetica"),
                ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
                ("FONTNAME", (2, 0), (2, -1), "Helvetica-Bold"),
                ("TEXTCOLOR", (0, 0), (-1, -1), colors.HexColor("#10243d")),
                ("FONTSIZE", (0, 0), (-1, -1), 8.4),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("TOPPADDING", (0, 0), (-1, -1), 6),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
            ]
        )
    )
    story.extend([metric_table, Spacer(1, 3 * mm)])

    if summary["rainiest_day"] is not None:
        rainiest = pd.Timestamp(summary["rainiest_day"]).strftime("%d/%m/%Y")
        story.append(
            Paragraph(
                f"Giorno più piovoso: <b>{rainiest}</b> con "
                f"<b>{_fmt(summary['rainiest_day_mm'], 1, ' mm')}</b>.",
                note_style,
            )
        )
    if data.empty:
        story.append(
            Paragraph(
                "Nessun campione disponibile per il mese selezionato.", note_style
            )
        )
    else:
        story.append(Paragraph("Riepilogo giornaliero", heading_style))
        daily_source = data.assign(date=data["time_local"].dt.date)
        rows: list[list[str]] = [["Data", "T min", "T media", "T max", "Pioggia", "Raffica max"]]
        for day, group in daily_source.groupby("date", sort=True):
            temperature = _number(group.get("temp_c", pd.Series(dtype=float)))
            rain = _number(group.get("rain_mm", pd.Series(dtype=float))).clip(lower=0)
            gust = _number(group.get("windgust_kmh", pd.Series(dtype=float)))
            rows.append(
                [
                    pd.Timestamp(day).strftime("%d/%m"),
                    _fmt(temperature.min(), 1),
                    _fmt(temperature.mean(), 1),
                    _fmt(temperature.max(), 1),
                    _fmt(rain.sum(min_count=1), 1),
                    _fmt(gust.max(), 1),
                ]
            )
        daily_table = Table(
            rows,
            repeatRows=1,
            colWidths=[24 * mm, 25 * mm, 29 * mm, 25 * mm, 28 * mm, 31 * mm],
        )
        daily_table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1d4ed8")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                    ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f8fafc")]),
                    ("GRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#cbd5e1")),
                    ("FONTSIZE", (0, 0), (-1, -1), 8.0),
                    ("ALIGN", (1, 1), (-1, -1), "RIGHT"),
                    ("TOPPADDING", (0, 0), (-1, -1), 4.5),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 4.5),
                ]
            )
        )
        story.append(daily_table)

    if source_health is not None and not source_health.empty:
        story.extend([Spacer(1, 3 * mm), Paragraph("Stato delle fonti", heading_style)])
        health_rows = [["Componente", "Stato", "Ultimo successo"]]
        for item in source_health.head(18).itertuples(index=False):
            succeeded = pd.to_datetime(
                getattr(item, "last_success_at", None), utc=True, errors="coerce"
            )
            health_rows.append(
                [
                    str(getattr(item, "label", getattr(item, "source", "-"))),
                    str(getattr(item, "display_status", "-")),
                    "-" if pd.isna(succeeded) else succeeded.strftime("%d/%m/%Y %H:%M UTC"),
                ]
            )
        health_table = Table(health_rows, repeatRows=1, colWidths=[68 * mm, 42 * mm, 58 * mm])
        health_table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#10243d")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                    ("GRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#cbd5e1")),
                    ("FONTSIZE", (0, 0), (-1, -1), 8.0),
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f8fafc")]),
                    ("TOPPADDING", (0, 0), (-1, -1), 5),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
                ]
            )
        )
        story.append(health_table)

    story.extend(
        [
            Spacer(1, 5 * mm),
            Paragraph(
                "Documento generato da Meteo V4.3. I valori dipendono dalla copertura "
                "dei sensori e non costituiscono un bollettino ufficiale o un'allerta.",
                note_style,
            ),
        ]
    )

    def footer(canvas: Any, doc: Any) -> None:
        canvas.saveState()
        canvas.setStrokeColor(colors.HexColor("#cbd5e1"))
        canvas.line(16 * mm, 12 * mm, A4[0] - 16 * mm, 12 * mm)
        canvas.setFont("Helvetica", 7.5)
        canvas.setFillColor(colors.HexColor("#64748b"))
        canvas.drawString(16 * mm, 7.5 * mm, "Meteo V4.3 - rapporto mensile")
        canvas.drawRightString(
            A4[0] - 16 * mm, 7.5 * mm, f"Pagina {doc.page}"
        )
        canvas.restoreState()

    document.build(story, onFirstPage=footer, onLaterPages=footer)
    return buffer.getvalue()


def report_filename(station_id: str, year: int, month: int, extension: str) -> str:
    safe = "-".join(
        part
        for part in "".join(
            character if character.isalnum() else "-" for character in station_id
        ).lower().split("-")
        if part
    ) or "stazione"
    suffix = extension.lower().lstrip(".")
    return f"meteo-{safe}-{int(year):04d}-{int(month):02d}.{suffix}"
