"""Meteo V3 — station-aware, multi-provider Streamlit dashboard."""

from __future__ import annotations

import html
import time
from typing import Any

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from astro_weather import astronomy_events, best_observing_windows, prepare_astronomy
from config import Settings
from data_access import (
    daily_forecast,
    health_snapshot,
    load_forecast,
    load_provider_scores,
    load_recent_logs,
    load_station,
)
from weather_display import compass_direction, weather_cell_style

st.set_page_config(
    page_title="Meteo V3",
    page_icon="🌦️",
    layout="wide",
    initial_sidebar_state="collapsed",
)
CFG = Settings.from_env()


st.markdown(
    """
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&display=swap');
:root {
  color-scheme:light;
  --page-bg:#f6f8fb; --sidebar-bg:#edf6fc; --surface:#ffffff; --surface-soft:#f8fafc;
  --ink:#10243d; --muted:#64748b; --subtle:#475569; --line:rgba(148,163,184,.28);
  --blue:#2563eb; --card-bg:linear-gradient(155deg,rgba(255,255,255,.98),rgba(241,245,249,.9));
  --control-bg:#ffffff; --shadow:0 7px 22px rgba(15,23,42,.055);
}
html, body, [class*="css"] { font-family:'DM Sans',system-ui,sans-serif; }
.stApp { background:var(--page-bg); color:var(--ink); }
section[data-testid="stSidebar"] { background:var(--sidebar-bg); border-right:1px solid var(--line); }
.stApp h1,.stApp h2,.stApp h3,.stApp h4,.stApp h5,.stApp h6,
.stApp label,[data-testid="stCaptionContainer"] { color:var(--ink); }
.block-container { max-width:1480px; padding-top:1.2rem; padding-bottom:3rem; }
.hero { position:relative; overflow:hidden; color:white; padding:1.7rem 1.9rem; border-radius:24px;
  background:radial-gradient(circle at 85% 20%,rgba(255,255,255,.24),transparent 24%),
  linear-gradient(125deg,#0f3d78 0%,#0b76b7 48%,#10a6a0 100%); box-shadow:0 18px 45px rgba(15,61,120,.20); }
.hero h1 { margin:0; font-size:clamp(1.8rem,4vw,3rem); letter-spacing:-.045em; }
.hero p { margin:.45rem 0 0; opacity:.86; font-size:1rem; }
.hero,.hero * { color:#fff !important; }
.eyebrow { font-size:.72rem; letter-spacing:.13em; text-transform:uppercase; font-weight:700; opacity:.76; }
.health-row { display:flex; flex-wrap:wrap; align-items:center; gap:.5rem; margin:.75rem 0 1.15rem; }
.pill { display:inline-flex; align-items:center; gap:.38rem; padding:.42rem .72rem; border-radius:999px;
  font-size:.78rem; font-weight:700; border:1px solid var(--line); background:var(--surface); color:var(--ink); }
.dot { width:.48rem; height:.48rem; border-radius:99px; display:inline-block; }
.online .dot { background:#16a34a; box-shadow:0 0 0 4px rgba(22,163,74,.12); }
.delayed .dot { background:#f59e0b; box-shadow:0 0 0 4px rgba(245,158,11,.12); }
.offline .dot { background:#ef4444; box-shadow:0 0 0 4px rgba(239,68,68,.12); }
.forecast-grid { display:grid; grid-template-columns:repeat(7,minmax(145px,1fr)); gap:.7rem; margin:.3rem 0 1.2rem; }
.day-card { border:1px solid var(--line); border-radius:17px; padding:.92rem; background:var(--card-bg);
  box-shadow:var(--shadow); min-height:205px; }
.day-name { color:var(--blue); font-size:.76rem; font-weight:700; text-transform:uppercase; letter-spacing:.07em; }
.day-icon { font-size:1.75rem; margin:.32rem 0; }.day-temp { font-size:1.18rem; font-weight:700; color:var(--ink); }
.day-desc { color:var(--muted); font-size:.76rem; min-height:2.1rem; line-height:1.3; }
.day-meta { margin-top:.55rem; color:var(--subtle); font-size:.72rem; line-height:1.55; }
.hour-grid { display:grid; grid-template-columns:repeat(3,minmax(180px,1fr)); gap:.75rem; margin:.5rem 0 1.25rem; }
.hour-card { border:1px solid var(--line); border-radius:16px; padding:1rem; background:var(--surface); box-shadow:var(--shadow); }
.hour-title { color:var(--blue); font-size:.78rem; font-weight:700; text-transform:uppercase; letter-spacing:.06em; }
.hour-weather { color:var(--ink); font-size:1.05rem; font-weight:700; margin:.35rem 0; }
.hour-meta { color:var(--subtle); font-size:.8rem; line-height:1.65; }
.color-legend { display:flex; flex-wrap:wrap; align-items:center; gap:.5rem .85rem; margin:.45rem 0 .8rem;
  padding:.72rem .85rem; border:1px solid var(--line); border-radius:13px; background:var(--surface); }
.legend-item { display:inline-flex; align-items:center; gap:.34rem; color:var(--subtle); font-size:.74rem; font-weight:600; }
.legend-swatch { width:.68rem; height:.68rem; border-radius:4px; box-shadow:inset 0 0 0 1px rgba(15,23,42,.12); }
.legend-green { background:#bbf7d0; }.legend-yellow { background:#fef3c7; }.legend-orange { background:#fed7aa; }
.legend-red { background:#fecaca; }.legend-blue { background:#60a5fa; }.legend-grey { background:#cbd5e1; }
.legend-note { flex-basis:100%; color:var(--muted); font-size:.68rem; margin-top:.05rem; }
.section-kicker { color:#0b76b7; font-size:.72rem; font-weight:700; text-transform:uppercase; letter-spacing:.11em; margin-top:.25rem; }
.empty { border:1px dashed #94a3b8; background:var(--surface-soft); padding:1rem 1.1rem; border-radius:15px; color:var(--subtle); }
[data-testid="stMetric"] { border:1px solid var(--line); border-radius:17px; padding:.8rem 1rem; background:var(--surface); }
[data-testid="stMetricLabel"] { color:var(--muted); } [data-testid="stMetricValue"] { color:var(--ink); }
div[data-baseweb="tab-list"] { gap:.25rem; } button[data-baseweb="tab"] { border-radius:11px; padding:.45rem .8rem; }
[data-baseweb="select"],[data-baseweb="select"] > div,[data-baseweb="select"] input,
[data-baseweb="select"] span { background-color:var(--control-bg) !important; color:var(--ink) !important; }
[data-baseweb="select"] svg { fill:var(--ink) !important; }
[data-baseweb="popover"] > div,ul[role="listbox"],div[role="listbox"] {
  background:var(--control-bg) !important; color:var(--ink) !important; border-color:var(--line) !important; }
li[role="option"],div[role="option"] { background:var(--control-bg) !important; color:var(--ink) !important; }
li[role="option"]:hover,div[role="option"]:hover,
li[role="option"][aria-selected="true"],div[role="option"][aria-selected="true"] {
  background:#1d4ed8 !important; color:#fff !important; }
[data-testid="stExpander"],[data-testid="stExpander"] details,
[data-testid="stExpander"] summary,[data-testid="stExpander"] summary:hover {
  background:var(--surface) !important; color:var(--ink) !important; border-color:var(--line) !important; }
[data-testid="stExpander"] summary * { color:var(--ink) !important; }
.stButton > button { background:var(--control-bg) !important; color:var(--ink) !important; border-color:var(--line) !important; }
[data-testid="stDataFrame"] { background:var(--surface); border:1px solid var(--line); border-radius:12px; overflow:hidden; }
@media(max-width:1050px){.forecast-grid{grid-template-columns:repeat(4,minmax(140px,1fr));}}
@media(max-width:680px){.block-container{padding:.7rem}.hero{padding:1.25rem;border-radius:18px}.forecast-grid{grid-template-columns:repeat(2,minmax(135px,1fr));}.day-card{min-height:190px}.hour-grid{grid-template-columns:1fr}}
</style>
""",
    unsafe_allow_html=True,
)


@st.cache_data(ttl=180, show_spinner=False)
def station_data(hours: int) -> pd.DataFrame:
    return load_station(hours)


@st.cache_data(ttl=180, show_spinner=False)
def forecast_data() -> pd.DataFrame:
    return load_forecast()


@st.cache_data(ttl=180, show_spinner=False)
def health_data() -> dict[str, Any]:
    return health_snapshot(Settings.from_env())


@st.cache_data(ttl=600, show_spinner=False)
def score_data() -> pd.DataFrame:
    return load_provider_scores()


@st.cache_data(ttl=180, show_spinner=False)
def log_data() -> pd.DataFrame:
    return load_recent_logs()


@st.fragment(run_every=300)
def _refresh_controller(enabled: bool) -> None:
    """Refresh the full app every five minutes without injecting browser script."""
    if not enabled:
        return
    now = time.monotonic()
    previous = st.session_state.get("last_full_refresh")
    if previous is None:
        st.session_state["last_full_refresh"] = now
    elif now - previous >= 295:
        st.session_state["last_full_refresh"] = now
        st.cache_data.clear()
        st.rerun()


def _apply_theme(dark_mode: bool) -> None:
    """Apply a complete high-contrast palette after the sidebar choice is known."""
    if not dark_mode:
        return
    st.markdown(
        """
<style>
:root {
  color-scheme:dark;
  --page-bg:#05070b; --sidebar-bg:#0b111b; --surface:#101826; --surface-soft:#0d1522;
  --ink:#f8fafc; --muted:#b6c2d1; --subtle:#d5deea; --line:rgba(226,232,240,.2);
  --blue:#60a5fa; --card-bg:linear-gradient(155deg,#111b2b,#0b1320);
  --control-bg:#111827; --shadow:0 9px 28px rgba(0,0,0,.28);
}
.stApp,[data-testid="stAppViewContainer"],[data-testid="stHeader"] { background:#05070b !important; color:var(--ink) !important; }
section[data-testid="stSidebar"] { background:#0b111b !important; }
.stApp p,.stApp li,.stApp label,.stApp span,.stApp div { border-color:var(--line); }
.stApp h1,.stApp h2,.stApp h3,.stApp h4,.stApp h5,.stApp h6,
.stApp label,.stApp p,[data-testid="stCaptionContainer"],button[data-baseweb="tab"] { color:var(--ink); }
.hero,.hero * { color:#fff !important; }
[data-testid="stMetric"],[data-testid="stExpander"],.stButton > button,
[data-baseweb="select"] > div,[data-testid="stTextInput"] input { background:var(--surface) !important; color:var(--ink) !important; }
[data-testid="stMetricLabel"],[data-testid="stMetricLabel"] p { color:var(--muted) !important; }
[data-testid="stMetricValue"],[data-testid="stMetricValue"] div { color:var(--ink) !important; }
[data-testid="stDataFrame"],[data-testid="stDataFrame"] [role="grid"] { background:var(--surface) !important; color:var(--ink) !important; }
[data-testid="stSidebarCollapseButton"] button,[data-testid="stBaseButton-headerNoPadding"] { color:var(--ink) !important; }
hr { border-color:var(--line) !important; }
</style>
""",
        unsafe_allow_html=True,
    )


def _local_time(value: Any, pattern: str = "%d/%m %H:%M") -> str:
    timestamp = pd.to_datetime(value, utc=True, errors="coerce")
    return (
        "—"
        if pd.isna(timestamp)
        else timestamp.tz_convert(CFG.local_timezone).strftime(pattern)
    )


def _age_text(minutes: Any) -> str:
    if minutes is None or not np.isfinite(minutes):
        return "mai"
    minutes = max(0, int(minutes))
    if minutes < 60:
        return f"{minutes} min fa"
    hours, remainder = divmod(minutes, 60)
    return f"{hours} h {remainder:02d} min fa"


def _number(value: Any, digits: int = 1, suffix: str = "") -> str:
    number = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return "—" if pd.isna(number) else f"{number:.{digits}f}{suffix}"


def _numeric_series(
    frame: pd.DataFrame, column: str, default: float = np.nan
) -> pd.Series:
    if column not in frame:
        return pd.Series(default, index=frame.index, dtype=float)
    return pd.to_numeric(frame[column], errors="coerce")


def _style_plotly(figure: go.Figure, dark_mode: bool) -> go.Figure:
    """Keep every Plotly surface and label aligned with the selected app theme."""
    if dark_mode:
        paper, plot, ink = "#05070b", "#0b111b", "#f8fafc"
        grid, line = "rgba(148,163,184,.18)", "rgba(226,232,240,.28)"
        hover_bg = "#111827"
        template = "plotly_dark"
    else:
        paper, plot, ink = "#f6f8fb", "#ffffff", "#10243d"
        grid, line = "rgba(100,116,139,.16)", "rgba(100,116,139,.3)"
        hover_bg = "#ffffff"
        template = "plotly_white"
    figure.update_layout(
        template=template,
        paper_bgcolor=paper,
        plot_bgcolor=plot,
        font={"color": ink, "family": "DM Sans, system-ui, sans-serif"},
        legend={"bgcolor": "rgba(0,0,0,0)"},
        hoverlabel={"bgcolor": hover_bg, "font_color": ink, "bordercolor": line},
    )
    figure.update_xaxes(
        gridcolor=grid,
        linecolor=line,
        tickfont={"color": ink},
        title_font={"color": ink},
        zerolinecolor=line,
    )
    figure.update_yaxes(
        gridcolor=grid,
        linecolor=line,
        tickfont={"color": ink},
        title_font={"color": ink},
        zerolinecolor=line,
    )
    return figure


def _base_table_style(frame: pd.DataFrame, dark_mode: bool) -> Any:
    background = "#0b111b" if dark_mode else "#ffffff"
    foreground = "#e5edf7" if dark_mode else "#10243d"
    header = "#162235" if dark_mode else "#e8f1f8"
    return frame.style.set_properties(
        **{"background-color": background, "color": foreground}
    ).set_table_styles(
        [
            {
                "selector": "th",
                "props": [
                    ("background-color", header),
                    ("color", foreground),
                    ("font-weight", "700"),
                ],
            }
        ]
    )


def _style_hourly_table(table: pd.DataFrame, dark_mode: bool) -> Any:
    styler = _base_table_style(table, dark_mode)
    metric_columns = {
        "temperature": ["Temp °C", "Percepita °C"],
        "humidity": ["Umidità %"],
        "pressure": ["Pressione hPa"],
        "rain": ["Pioggia mm"],
        "rain_probability": ["Prob. %"],
        "wind": ["Vento km/h"],
        "gust": ["Raffiche km/h"],
        "clouds": ["Nuvole %"],
        "confidence": ["Fiducia %"],
    }
    for metric, columns in metric_columns.items():
        available = [column for column in columns if column in table]
        if available:
            styler = styler.map(
                lambda value, metric=metric: weather_cell_style(value, metric),
                subset=available,
            )
    formats = {
        "Temp °C": "{:.1f}",
        "Percepita °C": "{:.1f}",
        "Pioggia mm": "{:.1f}",
        "Prob. %": "{:.0f}",
        "Umidità %": "{:.0f}",
        "Pressione hPa": "{:.0f}",
        "Vento km/h": "{:.1f}",
        "Raffiche km/h": "{:.1f}",
        "Nuvole %": "{:.0f}",
        "Fiducia %": "{:.0f}",
    }
    return styler.format(
        {key: value for key, value in formats.items() if key in table}, na_rep="—"
    )


def _style_status_table(
    table: pd.DataFrame, dark_mode: bool, status_column: str | None = None
) -> Any:
    styler = _base_table_style(table, dark_mode)
    if status_column and status_column in table:
        styler = styler.map(
            lambda value: (
                weather_cell_style(80, "confidence")
                if str(value).lower() in {"ok", "success", "online"}
                else weather_cell_style(20, "confidence")
                if str(value).lower() in {"error", "failed", "failure", "offline"}
                else weather_cell_style(55, "confidence")
            ),
            subset=[status_column],
        )
    return styler


def _style_score_table(table: pd.DataFrame, dark_mode: bool) -> Any:
    """Colour forecast errors relative to the values currently being compared."""
    styler = _base_table_style(table, dark_mode)
    for column in ("Bias", "MAE", "RMSE", "Brier"):
        if column not in table:
            continue
        values = pd.to_numeric(table[column], errors="coerce").abs().dropna()
        if values.empty:
            continue
        median = float(values.quantile(0.5))
        high = float(values.quantile(0.8))

        def relative_style(value: Any, median: float = median, high: float = high) -> str:
            number = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
            if pd.isna(number):
                return ""
            magnitude = abs(float(number))
            if magnitude <= median:
                return weather_cell_style(80, "confidence")
            if magnitude <= high:
                return weather_cell_style(55, "confidence")
            return weather_cell_style(20, "confidence")

        styler = styler.map(relative_style, subset=[column])
    return styler.format(precision=2, na_rep="—")


def render_color_legend(kind: str = "weather") -> None:
    """Render a compact, theme-aware explanation of semantic table colours."""
    if kind == "scores":
        items = (
            ("green", "Errore minore"),
            ("yellow", "Errore intermedio"),
            ("red", "Errore maggiore"),
        )
        note = "Confronto relativo tra i provider e gli orizzonti presenti nella tabella."
    elif kind == "status":
        items = (
            ("green", "Esecuzione regolare"),
            ("yellow", "Da controllare / dato storico"),
            ("red", "Errore o servizio offline"),
        )
        note = "La colorazione aiuta a individuare rapidamente anomalie nella pipeline."
    else:
        items = (
            ("green", "Regolare"),
            ("yellow", "Da monitorare"),
            ("orange", "Vicino alla soglia"),
            ("red", "Oltre soglia"),
            ("blue", "Pioggia probabile"),
            ("grey", "Molte nuvole"),
        )
        note = "Soglie indicative per la lettura rapida: non sostituiscono allerte ufficiali."
    chips = "".join(
        '<span class="legend-item">'
        f'<span class="legend-swatch legend-{colour}"></span>{html.escape(label)}'
        "</span>"
        for colour, label in items
    )
    st.markdown(
        f'<div class="color-legend">{chips}<div class="legend-note">{html.escape(note)}</div></div>',
        unsafe_allow_html=True,
    )


def _weather_icon(description: Any) -> str:
    value = str(description or "").lower()
    if "tempor" in value:
        return "⛈️"
    if "neve" in value:
        return "🌨️"
    if "piogg" in value or "rovesc" in value or "piovigg" in value:
        return "🌧️"
    if "nebb" in value:
        return "🌫️"
    if "coperto" in value:
        return "☁️"
    if "nuvol" in value:
        return "⛅"
    return "☀️"


def _delta(
    row: pd.Series, previous: pd.Series | None, column: str, digits: int = 1
) -> str | None:
    if previous is None or column not in row or column not in previous:
        return None
    current_value = pd.to_numeric(pd.Series([row.get(column)]), errors="coerce").iloc[0]
    previous_value = pd.to_numeric(
        pd.Series([previous.get(column)]), errors="coerce"
    ).iloc[0]
    if pd.isna(current_value) or pd.isna(previous_value):
        return None
    return f"{current_value - previous_value:+.{digits}f} in 3 h"


def _health_pill(label: str, status: str, detail: str) -> str:
    return (
        f'<span class="pill {status}"><span class="dot"></span>{html.escape(label)}: '
        f'{html.escape(status.upper())}</span><span style="color:var(--muted);font-size:.78rem">{html.escape(detail)}</span>'
    )


def render_daily_cards(daily: pd.DataFrame) -> None:
    if daily.empty:
        st.info(
            "La previsione giornaliera comparirà dopo il primo aggiornamento della pipeline V3."
        )
        return
    names = ["Lun", "Mar", "Mer", "Gio", "Ven", "Sab", "Dom"]
    cards = []
    for _, row in daily.head(7).iterrows():
        day = pd.Timestamp(row["date"])
        description = str(row.get("description") or "Variabile")
        cards.append(
            '<div class="day-card">'
            f'<div class="day-name">{names[day.weekday()]} {day.day}</div>'
            f'<div class="day-icon">{_weather_icon(description)}</div>'
            f'<div class="day-temp">{_number(row.get("temp_min"), 0, "°")} / {_number(row.get("temp_max"), 0, "°")}</div>'
            f'<div class="day-desc">{html.escape(description)}</div>'
            f'<div class="day-meta">☔ {_number(row.get("rain_mm"), 1, " mm")} · rischio {_number(row.get("pop_max"), 0, "%")}'
            f"<br>💧 umidità media {_number(row.get('humidity_mean'), 0, '%')}"
            f"<br>💨 vento {_number(row.get('wind_mean'), 0, ' km/h')} · raffiche {_number(row.get('wind_max'), 0, ' km/h')}"
            f"<br>◎ fiducia {_number(row.get('confidence'), 0, '%')}</div>"
            "</div>"
        )
    st.markdown(
        '<div class="forecast-grid">' + "".join(cards) + "</div>",
        unsafe_allow_html=True,
    )


def render_three_hour_forecast(forecast: pd.DataFrame) -> None:
    """Render the next three hourly forecast points as an explicit local outlook."""
    if forecast.empty:
        st.info("La previsione a 3 ore comparirà al prossimo aggiornamento dei dati.")
        return
    now = pd.Timestamp.now(tz="UTC")
    next_hours = forecast[forecast["valid_time"] >= now].sort_values(
        "valid_time"
    ).head(3)
    if next_hours.empty:
        st.info("Nessun punto di previsione futuro disponibile.")
        return
    cards = []
    for position, (_, row) in enumerate(next_hours.iterrows(), start=1):
        local_time = pd.Timestamp(row["valid_time"]).tz_convert(CFG.local_timezone)
        description = str(row.get("description") or "Variabile")
        cards.append(
            '<div class="hour-card">'
            f'<div class="hour-title">+{position} h · {local_time:%H:%M}</div>'
            f'<div class="hour-weather">{_weather_icon(description)} {html.escape(description)}</div>'
            f'<div class="hour-meta">☁️ nuvole {_number(row.get("clouds"), 0, "%")}'
            f'<br>☔ {_number(row.get("rain_mm"), 1, " mm")} · rischio {_number(row.get("precip_probability"), 0, "%")}'
            f'<br>💨 {_number(row.get("wind_kmh"), 0, " km/h")} · {compass_direction(row.get("wind_dir"))}</div>'
            "</div>"
        )
    st.markdown(
        '<div class="hour-grid">' + "".join(cards) + "</div>",
        unsafe_allow_html=True,
    )


def combined_chart(
    station: pd.DataFrame, forecast: pd.DataFrame, hours: int, theme: str
) -> go.Figure:
    now = pd.Timestamp.now(tz="UTC")
    observations = (
        station[station["time"] >= now - pd.Timedelta(hours=hours)].copy()
        if not station.empty
        else station
    )
    future = (
        forecast[
            (forecast["valid_time"] >= now - pd.Timedelta(hours=1))
            & (forecast["valid_time"] <= now + pd.Timedelta(hours=72))
        ].copy()
        if not forecast.empty
        else forecast
    )
    figure = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        row_heights=[0.68, 0.32],
        vertical_spacing=0.08,
        specs=[[{}], [{"secondary_y": True}]],
    )
    if not observations.empty and "temp_c" in observations:
        figure.add_trace(
            go.Scatter(
                x=observations["time"],
                y=observations["temp_c"],
                name="Stazione",
                mode="lines",
                line={"color": "#0b76b7", "width": 3},
                hovertemplate="%{x|%d/%m %H:%M}<br>%{y:.1f} °C<extra>Stazione</extra>",
            ),
            row=1,
            col=1,
        )
    if not future.empty and "temp_c" in future:
        uncertainty = pd.to_numeric(
            future.get("temp_uncertainty_c"), errors="coerce"
        ).fillna(1.2)
        upper = future["temp_c"] + uncertainty
        lower = future["temp_c"] - uncertainty
        figure.add_trace(
            go.Scatter(
                x=future["valid_time"],
                y=upper,
                mode="lines",
                line={"width": 0},
                showlegend=False,
                hoverinfo="skip",
            ),
            row=1,
            col=1,
        )
        figure.add_trace(
            go.Scatter(
                x=future["valid_time"],
                y=lower,
                mode="lines",
                line={"width": 0},
                fill="tonexty",
                fillcolor="rgba(37,99,235,.12)",
                name="Incertezza",
                hoverinfo="skip",
            ),
            row=1,
            col=1,
        )
        figure.add_trace(
            go.Scatter(
                x=future["valid_time"],
                y=future["temp_c"],
                name="Previsione corretta",
                mode="lines",
                line={"color": "#2563eb", "width": 3, "dash": "dash"},
                customdata=np.stack(
                    [future.get("confidence", pd.Series(np.nan, index=future.index))],
                    axis=-1,
                ),
                hovertemplate="%{x|%d/%m %H:%M}<br>%{y:.1f} °C<br>Fiducia %{customdata[0]:.0f}%<extra>Previsione</extra>",
            ),
            row=1,
            col=1,
        )
        if "rain_mm" in future:
            figure.add_trace(
                go.Bar(
                    x=future["valid_time"],
                    y=future["rain_mm"],
                    name="Pioggia",
                    marker_color="#38bdf8",
                    opacity=0.72,
                ),
                row=2,
                col=1,
                secondary_y=False,
            )
        if "precip_probability" in future:
            figure.add_trace(
                go.Scatter(
                    x=future["valid_time"],
                    y=future["precip_probability"],
                    name="Prob. pioggia",
                    line={"color": "#7c3aed", "width": 2},
                ),
                row=2,
                col=1,
                secondary_y=True,
            )
    figure.add_vline(
        x=now.timestamp() * 1000, line_dash="dot", line_color="#f97316", opacity=0.9
    )
    figure.update_yaxes(title_text="Temperatura °C", row=1, col=1)
    figure.update_yaxes(
        title_text="Pioggia mm/h", rangemode="tozero", row=2, col=1, secondary_y=False
    )
    figure.update_yaxes(
        title_text="Probabilità %", range=[0, 105], row=2, col=1, secondary_y=True
    )
    figure.update_layout(
        height=570,
        margin={"l": 15, "r": 15, "t": 30, "b": 10},
        hovermode="x unified",
        legend={"orientation": "h", "y": 1.08, "x": 0},
        template=theme,
        bargap=0.1,
    )
    return figure


def weather_details_chart(
    station: pd.DataFrame, forecast: pd.DataFrame, hours: int, theme: str
) -> go.Figure:
    """Join observed and forecast humidity, pressure, wind and direction."""
    now = pd.Timestamp.now(tz="UTC")
    observations = (
        station[station["time"] >= now - pd.Timedelta(hours=hours)].copy()
        if not station.empty
        else station
    )
    future = (
        forecast[
            (forecast["valid_time"] >= now - pd.Timedelta(hours=1))
            & (forecast["valid_time"] <= now + pd.Timedelta(hours=72))
        ].copy()
        if not forecast.empty
        else forecast
    )
    figure = make_subplots(
        rows=3,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.08,
        specs=[[{}], [{}], [{"secondary_y": True}]],
    )

    for frame, time_column, suffix, dash in (
        (observations, "time", "stazione", "solid"),
        (future, "valid_time", "previsione", "dash"),
    ):
        if frame.empty:
            continue
        if "humidity" in frame:
            figure.add_trace(
                go.Scatter(
                    x=frame[time_column],
                    y=frame["humidity"],
                    name=f"Umidità · {suffix}",
                    mode="lines",
                    line={"color": "#0ea5e9", "width": 2.5, "dash": dash},
                ),
                row=1,
                col=1,
            )
        if "pressure_hpa" in frame:
            figure.add_trace(
                go.Scatter(
                    x=frame[time_column],
                    y=frame["pressure_hpa"],
                    name=f"Pressione · {suffix}",
                    mode="lines",
                    line={"color": "#8b5cf6", "width": 2.5, "dash": dash},
                ),
                row=2,
                col=1,
            )
        wind_column = "wind_kmh"
        gust_column = "windgust_kmh" if suffix == "stazione" else "wind_gust_kmh"
        direction_column = "winddir" if suffix == "stazione" else "wind_dir"
        if wind_column in frame:
            figure.add_trace(
                go.Scatter(
                    x=frame[time_column],
                    y=frame[wind_column],
                    name=f"Vento · {suffix}",
                    mode="lines",
                    line={"color": "#10b981", "width": 2.5, "dash": dash},
                ),
                row=3,
                col=1,
                secondary_y=False,
            )
        if gust_column in frame:
            figure.add_trace(
                go.Scatter(
                    x=frame[time_column],
                    y=frame[gust_column],
                    name=f"Raffiche · {suffix}",
                    mode="lines",
                    line={"color": "#f59e0b", "width": 1.7, "dash": dash},
                ),
                row=3,
                col=1,
                secondary_y=False,
            )
        if direction_column in frame:
            figure.add_trace(
                go.Scatter(
                    x=frame[time_column],
                    y=frame[direction_column],
                    name=f"Direzione · {suffix}",
                    mode="lines",
                    line={"color": "#ec4899", "width": 1.5, "dash": dash},
                    opacity=0.72,
                    hovertemplate="%{x|%d/%m %H:%M}<br>%{y:.0f}°<extra></extra>",
                ),
                row=3,
                col=1,
                secondary_y=True,
            )

    figure.add_vline(
        x=now.timestamp() * 1000, line_dash="dot", line_color="#f97316", opacity=0.9
    )
    figure.update_yaxes(title_text="Umidità %", range=[0, 105], row=1, col=1)
    figure.update_yaxes(title_text="Pressione hPa", row=2, col=1)
    figure.update_yaxes(
        title_text="Vento km/h", rangemode="tozero", row=3, col=1, secondary_y=False
    )
    figure.update_yaxes(
        title_text="Direzione",
        range=[0, 360],
        tickvals=[0, 45, 90, 135, 180, 225, 270, 315, 360],
        ticktext=["N", "NE", "E", "SE", "S", "SO", "O", "NO", "N"],
        row=3,
        col=1,
        secondary_y=True,
    )
    figure.update_layout(
        height=760,
        margin={"l": 15, "r": 15, "t": 45, "b": 10},
        hovermode="x unified",
        legend={"orientation": "h", "y": 1.09, "x": 0},
        template=theme,
    )
    return figure


def forecast_alerts(forecast: pd.DataFrame) -> None:
    if forecast.empty:
        return
    now = pd.Timestamp.now(tz="UTC")
    next_24 = forecast[
        (forecast["valid_time"] >= now)
        & (forecast["valid_time"] <= now + pd.Timedelta(hours=24))
    ]
    if next_24.empty:
        return
    messages = []
    rain = _numeric_series(next_24, "rain_mm", 0).sum()
    pop = _numeric_series(next_24, "precip_probability", 0).max()
    gust = _numeric_series(next_24, "wind_gust_kmh", 0).max()
    confidence = _numeric_series(next_24, "confidence").mean()
    if rain >= 15 or pop >= 80:
        messages.append(f"pioggia probabile ({rain:.1f} mm, rischio max {pop:.0f}%)")
    if gust >= 50:
        messages.append(f"raffiche fino a {gust:.0f} km/h")
    if pd.notna(confidence) and confidence < 50:
        messages.append("modelli poco concordi: previsione più incerta del normale")
    if messages:
        st.warning("Nelle prossime 24 ore: " + "; ".join(messages) + ".")


with st.sidebar:
    st.header("Vista")
    observation_hours = st.select_slider(
        "Storico nel grafico",
        options=[12, 24, 48, 72, 120],
        value=24,
        format_func=lambda value: f"{value} ore",
    )
    dark_mode = st.toggle("Tema scuro", value=False)
    auto_refresh = st.toggle("Aggiorna la pagina ogni 5 min", value=True)
    st.divider()
    st.caption(
        "I dati vengono aggiornati dal workflow GitHub. Il pulsante ricarica solo la pagina e non espone chiavi API."
    )
    if st.button("Ricarica dati", width="stretch"):
        st.cache_data.clear()
        st.rerun()

_refresh_controller(auto_refresh)
_apply_theme(dark_mode)

station = station_data(max(observation_hours + 24, 240))
forecast = forecast_data()
health = health_data()
theme = "plotly_dark" if dark_mode else "plotly_white"

st.markdown(
    '<div class="hero">'
    '<div class="eyebrow">Stazione locale · previsione multi‑modello</div>'
    f"<h1>{html.escape(CFG.location_name)}</h1>"
    f"<p>{CFG.latitude:.4f}, {CFG.longitude:.4f} · aggiornamento automatico · orari {html.escape(CFG.local_timezone)}</p>"
    "</div>",
    unsafe_allow_html=True,
)

station_detail = f"ultimo dato {_age_text(health.get('station_age_minutes'))}"
forecast_detail = f"emissione {_age_text(health.get('forecast_age_minutes'))}"
st.markdown(
    '<div class="health-row">'
    + _health_pill("Stazione", health["station_status"], station_detail)
    + _health_pill("Previsioni", health["forecast_status"], forecast_detail)
    + f'<span style="color:var(--muted);font-size:.78rem">Dati stazione: {_local_time(health.get("station_time"))} · copertura prevista fino al {_local_time(health.get("forecast_until"), "%d/%m %H:%M")}</span>'
    + "</div>",
    unsafe_allow_html=True,
)

if not station.empty:
    current = station.iloc[-1]
    target = current["time"] - pd.Timedelta(hours=3)
    older = station[station["time"] <= target]
    previous = older.iloc[-1] if not older.empty else None
    rain_24 = (
        station[station["time"] >= current["time"] - pd.Timedelta(hours=24)]
        .get("rain_mm", pd.Series(dtype=float))
        .sum()
    )
    cols = st.columns(6)
    cols[0].metric(
        "Temperatura",
        _number(current.get("temp_c"), 1, " °C"),
        _delta(current, previous, "temp_c"),
    )
    cols[1].metric(
        "Umidità",
        _number(current.get("humidity"), 0, " %"),
        _delta(current, previous, "humidity", 0),
    )
    cols[2].metric(
        "Pressione",
        _number(current.get("pressure_hpa"), 1, " hPa"),
        _delta(current, previous, "pressure_hpa"),
    )
    cols[3].metric(
        "Vento",
        _number(current.get("wind_kmh"), 1, " km/h"),
        f"{compass_direction(current.get('winddir'))} · raffica {_number(current.get('windgust_kmh'), 1, ' km/h')}",
    )
    cols[4].metric(
        "Pioggia 24 h",
        _number(rain_24, 1, " mm"),
        f"ora {_number(current.get('rain_rate_mm_h'), 1, ' mm/h')}",
    )
    cols[5].metric(
        "Solare / UV",
        _number(current.get("solar_w_m2"), 0, " W/m²"),
        f"UV {_number(current.get('uv_index'), 1)}",
    )
else:
    st.markdown(
        '<div class="empty"><b>Stazione non ancora popolata.</b> La dashboard può già mostrare le previsioni; '
        "esegui la pipeline dopo aver configurato le tre credenziali Ecowitt.</div>",
        unsafe_allow_html=True,
    )

forecast_alerts(forecast)

tab_overview, tab_forecast, tab_station, tab_astro, tab_radar = st.tabs(
    ["Panoramica", "7 giorni", "Stazione", "Astronomia", "Radar"]
)

with tab_overview:
    st.markdown(
        '<div class="section-kicker">Passato e futuro, senza interruzioni</div>',
        unsafe_allow_html=True,
    )
    st.subheader("Temperatura e precipitazioni")
    if station.empty and forecast.empty:
        st.info(
            "Nessun dato disponibile. Avvia `python ingest_all.py --force-forecast` dopo la configurazione."
        )
    else:
        st.plotly_chart(
            _style_plotly(
                combined_chart(station, forecast, observation_hours, theme),
                dark_mode,
            ),
            width="stretch",
        )
        st.markdown(
            '<div class="section-kicker">Atmosfera e vento</div>',
            unsafe_allow_html=True,
        )
        st.subheader("Umidità, pressione, forza e direzione del vento")
        st.plotly_chart(
            _style_plotly(
                weather_details_chart(station, forecast, observation_hours, theme),
                dark_mode,
            ),
            width="stretch",
        )
    if not forecast.empty:
        st.markdown('<div class="section-kicker">Sintesi</div>', unsafe_allow_html=True)
        st.subheader("I prossimi 7 giorni")
        render_daily_cards(daily_forecast(forecast, CFG.local_timezone))
        with st.expander("Come leggere fiducia e fascia azzurra"):
            st.write(
                "La linea tratteggiata combina i provider e corregge gradualmente l'errore misurato dalla tua stazione. "
                "La fascia azzurra cresce quando i modelli divergono; la fiducia considera anche numero di provider e distanza temporale."
            )

with tab_forecast:
    st.markdown(
        '<div class="section-kicker">Previsione calibrata localmente</div>',
        unsafe_allow_html=True,
    )
    st.subheader("Dettaglio orario")
    render_daily_cards(daily_forecast(forecast, CFG.local_timezone))
    if forecast.empty:
        st.info("La pipeline non ha ancora prodotto la previsione combinata.")
    else:
        now = pd.Timestamp.now(tz="UTC")
        hourly = forecast[
            (forecast["valid_time"] >= now)
            & (forecast["valid_time"] <= now + pd.Timedelta(hours=72))
        ].copy()
        hourly["Ora"] = (
            hourly["valid_time"]
            .dt.tz_convert(CFG.local_timezone)
            .dt.strftime("%a %d · %H:%M")
        )
        table = pd.DataFrame(
            {
                "Ora": hourly["Ora"],
                "Cielo": hourly.get("description"),
                "Temp °C": _numeric_series(hourly, "temp_c").round(1),
                "Percepita °C": _numeric_series(hourly, "feels_like_c").round(1),
                "Pioggia mm": _numeric_series(hourly, "rain_mm").round(1),
                "Prob. %": _numeric_series(hourly, "precip_probability").round(0),
                "Umidità %": _numeric_series(hourly, "humidity").round(0),
                "Pressione hPa": _numeric_series(hourly, "pressure_hpa").round(0),
                "Vento km/h": _numeric_series(hourly, "wind_kmh").round(1),
                "Raffiche km/h": _numeric_series(hourly, "wind_gust_kmh").round(1),
                "Direzione": hourly.get(
                    "wind_dir", pd.Series(np.nan, index=hourly.index)
                ).map(compass_direction),
                "Nuvole %": _numeric_series(hourly, "clouds").round(0),
                "Fiducia %": _numeric_series(hourly, "confidence").round(0),
            }
        )
        render_color_legend("weather")
        st.dataframe(
            _style_hourly_table(table, dark_mode),
            hide_index=True,
            width="stretch",
            height=520,
        )

        scores = score_data()
        with st.expander("Accuratezza misurata sulla stazione"):
            if scores.empty:
                st.caption(
                    "Il confronto automatico inizierà quando previsioni archiviate e osservazioni avranno orari sovrapposti."
                )
            else:
                display = scores.rename(
                    columns={
                        "provider": "Provider",
                        "variable": "Variabile",
                        "horizon": "Orizzonte",
                        "n": "Campioni",
                        "bias": "Bias",
                        "mae": "MAE",
                        "rmse": "RMSE",
                        "brier": "Brier",
                    }
                )[
                    [
                        "Provider",
                        "Variabile",
                        "Orizzonte",
                        "Campioni",
                        "Bias",
                        "MAE",
                        "RMSE",
                        "Brier",
                    ]
                ]
                render_color_legend("scores")
                st.dataframe(
                    _style_score_table(display.round(2), dark_mode),
                    hide_index=True,
                    width="stretch",
                )

with tab_station:
    st.markdown(
        '<div class="section-kicker">Misure reali</div>', unsafe_allow_html=True
    )
    st.subheader(f"Ultime {observation_hours} ore")
    if station.empty:
        st.info("Nessun dato della stazione disponibile.")
    else:
        cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=observation_hours)
        recent = station[station["time"] >= cutoff].copy()
        figure = make_subplots(rows=4, cols=1, shared_xaxes=True, vertical_spacing=0.055)
        figure.add_trace(
            go.Scatter(
                x=recent["time"],
                y=recent.get("temp_c"),
                name="Temperatura",
                line={"color": "#ef4444", "width": 2.5},
            ),
            row=1,
            col=1,
        )
        figure.add_trace(
            go.Scatter(
                x=recent["time"],
                y=recent.get("humidity"),
                name="Umidità",
                line={"color": "#0ea5e9", "width": 2},
            ),
            row=2,
            col=1,
        )
        figure.add_trace(
            go.Scatter(
                x=recent["time"],
                y=recent.get("pressure_hpa"),
                name="Pressione",
                line={"color": "#8b5cf6", "width": 2.5},
            ),
            row=3,
            col=1,
        )
        figure.add_trace(
            go.Scatter(
                x=recent["time"],
                y=recent.get("wind_kmh"),
                name="Vento",
                line={"color": "#10b981", "width": 2},
            ),
            row=4,
            col=1,
        )
        figure.add_trace(
            go.Scatter(
                x=recent["time"],
                y=recent.get("windgust_kmh"),
                name="Raffiche",
                line={"color": "#f59e0b", "width": 1.5},
            ),
            row=4,
            col=1,
        )
        figure.update_yaxes(title_text="Temperatura °C", row=1, col=1)
        figure.update_yaxes(title_text="Umidità %", range=[0, 105], row=2, col=1)
        figure.update_yaxes(title_text="Pressione hPa", row=3, col=1)
        figure.update_yaxes(title_text="Vento km/h", row=4, col=1)
        figure.update_layout(
            height=850,
            template=theme,
            hovermode="x unified",
            margin={"l": 10, "r": 10, "t": 20, "b": 10},
            legend={"orientation": "h"},
        )
        st.plotly_chart(_style_plotly(figure, dark_mode), width="stretch")

        rain_figure = go.Figure(
            go.Bar(
                x=recent["time"],
                y=recent.get("rain_mm"),
                name="Incremento",
                marker_color="#38bdf8",
            )
        )
        if "rain_rate_mm_h" in recent:
            rain_figure.add_trace(
                go.Scatter(
                    x=recent["time"],
                    y=recent["rain_rate_mm_h"],
                    name="Intensità",
                    line={"color": "#2563eb"},
                )
            )
        rain_figure.update_layout(
            height=330,
            title="Pioggia: quantità per campione e intensità",
            template=theme,
            hovermode="x unified",
        )
        st.plotly_chart(_style_plotly(rain_figure, dark_mode), width="stretch")

        quality = recent.get("data_quality", pd.Series(dtype="object")).value_counts(
            dropna=False
        )
        with st.expander("Qualità dati e ultime esecuzioni"):
            render_color_legend("status")
            left, right = st.columns(2)
            left.write("Campioni per qualità")
            quality_table = quality.rename_axis("Qualità").reset_index(
                name="Campioni"
            )
            left.dataframe(
                _style_status_table(quality_table, dark_mode, "Qualità"),
                hide_index=True,
                width="stretch",
            )
            logs = log_data()
            if not logs.empty:
                safe_logs = logs[
                    [
                        column
                        for column in [
                            "started_at",
                            "component",
                            "status",
                            "rows_written",
                        ]
                        if column in logs
                    ]
                ].copy()
                if "started_at" in safe_logs:
                    safe_logs["started_at"] = (
                        safe_logs["started_at"]
                        .dt.tz_convert(CFG.local_timezone)
                        .dt.strftime("%d/%m %H:%M")
                    )
                right.write("Pipeline recenti")
                right.dataframe(
                    _style_status_table(safe_logs, dark_mode, "status"),
                    hide_index=True,
                    width="stretch",
                )

with tab_astro:
    st.markdown(
        '<div class="section-kicker">Finestre osservative</div>', unsafe_allow_html=True
    )
    st.subheader("Qualità del cielo notturno")
    astro = prepare_astronomy(forecast, CFG)
    if astro.empty:
        st.info("Servono le previsioni V3 per calcolare le condizioni astronomiche.")
    else:
        windows = best_observing_windows(astro)
        if windows.empty:
            st.warning(
                "Nessuna finestra di almeno 2 ore con punteggio ≥ 65 nei prossimi giorni."
            )
        else:
            cols = st.columns(min(3, len(windows)))
            for position, (_, window) in enumerate(windows.head(3).iterrows()):
                cols[position].metric(
                    f"{window['start']:%a %d · %H:%M}–{window['end']:%H:%M}",
                    f"{window['score']:.0f}/100",
                    f"{int(window['hours'])} h · nuvole {window['clouds']:.0f}%",
                )
        night = astro[astro["is_night"]].copy()
        figure = make_subplots(specs=[[{"secondary_y": True}]])
        figure.add_trace(
            go.Scatter(
                x=night["local_time"],
                y=night["astro_score"],
                name="Qualità cielo",
                fill="tozeroy",
                line={"color": "#8b5cf6", "width": 3},
            ),
            secondary_y=False,
        )
        for column, name, color in (
            ("cloud_low", "Nuvole basse", "#0ea5e9"),
            ("cloud_mid", "Nuvole medie", "#64748b"),
            ("cloud_high", "Nuvole alte", "#f59e0b"),
        ):
            if column in night:
                figure.add_trace(
                    go.Scatter(
                        x=night["local_time"],
                        y=night[column],
                        name=name,
                        line={"color": color, "width": 1.5, "dash": "dot"},
                    ),
                    secondary_y=True,
                )
        figure.update_yaxes(
            title_text="Punteggio /100", range=[0, 105], secondary_y=False
        )
        figure.update_yaxes(title_text="Nuvole %", range=[0, 105], secondary_y=True)
        figure.update_layout(
            height=450,
            template=theme,
            hovermode="x unified",
            legend={"orientation": "h"},
            margin={"l": 10, "r": 10, "t": 20, "b": 10},
        )
        st.plotly_chart(_style_plotly(figure, dark_mode), width="stretch")
        st.caption(
            "Il punteggio penalizza nuvole basse/medie/alte, rischio pioggia, vento, visibilità e temperatura vicina al punto di rugiada."
        )

        events = astronomy_events(CFG)
        if not events.empty:
            table = pd.DataFrame(
                {
                    "Data": pd.to_datetime(events["date"]).dt.strftime("%a %d/%m"),
                    "Tramonto": events["sunset"].map(
                        lambda value: value.strftime("%H:%M")
                    ),
                    "Fine crepuscolo": events["dusk"].map(
                        lambda value: value.strftime("%H:%M")
                    ),
                    "Inizio alba": events["dawn"].map(
                        lambda value: value.strftime("%H:%M")
                    ),
                    "Luna sorge": events["moonrise"].map(
                        lambda value: "—" if pd.isna(value) else value.strftime("%H:%M")
                    ),
                    "Luna tramonta": events["moonset"].map(
                        lambda value: "—" if pd.isna(value) else value.strftime("%H:%M")
                    ),
                    "Luna illuminata": events["moon_illumination"].map(
                        lambda value: f"{value:.0f}%"
                    ),
                }
            )
            st.dataframe(
                _base_table_style(table, dark_mode),
                hide_index=True,
                width="stretch",
            )

with tab_radar:
    st.markdown(
        '<div class="section-kicker">Satellite e precipitazioni osservate</div>',
        unsafe_allow_html=True,
    )
    st.subheader("Satellite e radar osservato")
    observed_layer = st.radio(
        "Livello osservativo",
        options=("Satellite", "Radar precipitazioni"),
        horizontal=True,
    )
    observed_overlay = "satellite" if observed_layer == "Satellite" else "radar"
    observed_product = "satellite" if observed_layer == "Satellite" else "radar"
    observed_url = (
        "https://embed.windy.com/embed.html?type=map&location=coordinates"
        "&metricRain=mm&metricTemp=%C2%B0C&metricWind=km%2Fh&zoom=7"
        f"&overlay={observed_overlay}&product={observed_product}&level=surface"
        f"&lat={CFG.latitude:.4f}&lon={CFG.longitude:.4f}"
        "&marker=true&play=1"
    )
    st.iframe(observed_url, height=620)
    st.caption(
        "Immagini osservative Windy: scegli Satellite oppure Radar precipitazioni. "
        "Nel radar l'assenza di aree colorate indica che non sono rilevate precipitazioni."
    )

    st.divider()
    st.markdown(
        '<div class="section-kicker">Previsione fino a +3 ore</div>',
        unsafe_allow_html=True,
    )
    st.subheader("Evoluzione locale")
    render_three_hour_forecast(forecast)

    st.subheader("Nuvole previste sulla mappa")
    windy_url = (
        "https://embed.windy.com/embed.html?type=map&location=coordinates"
        "&metricRain=mm&metricTemp=%C2%B0C&metricWind=km%2Fh&zoom=7"
        f"&overlay=clouds&product=ecmwf&level=surface&lat={CFG.latitude:.4f}"
        f"&lon={CFG.longitude:.4f}"
    )
    st.iframe(windy_url, height=560)
    st.caption(
        "Mappa previsionale Windy/ECMWF: usa la linea temporale interna per spostarti a +1, +2 e +3 ore. "
        "Il radar sopra mostra invece precipitazioni realmente osservate."
    )

st.caption(
    "Meteo V3 · Open‑Meteo + OpenWeather · correzione locale sulla stazione · "
    f"ultimo dato reale {_local_time(health.get('station_time'))}"
)
