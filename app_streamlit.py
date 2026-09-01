"""Meteo V4 — station-aware daily weather experience and expert dashboard."""

from __future__ import annotations

import html
import time
from dataclasses import replace
from typing import Any

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import streamlit.components.v1 as components
from plotly.subplots import make_subplots

from air_quality import AirQualityError, AirQualityForecast, fetch_air_quality
from astro_weather import (
    astronomy_events,
    best_observing_windows,
    daily_astronomy_summary,
    prepare_astronomy,
)
from astronomy_planner import (
    EquipmentProfile,
    SkyTarget,
    custom_target,
    equipment_profile,
    field_of_view,
    framing_assessment,
    framing_geometry,
    horizon_altitudes,
    local_night_window,
    night_plan_csv,
    night_plan_tracks,
    observing_calendar_ics,
    observing_log_csv,
    parse_planner_configuration,
    planner_configuration_json,
    resolve_targets,
    summarize_night_plan,
    target_labels,
)
from chart_data import (
    forecast_chart_window,
    merge_intervals,
    missing_forecast_segments,
    plotly_local_datetime,
    plotly_local_datetimes,
)
from city_weather import (
    CityForecast,
    CityLocation,
    CityWeatherError,
    fetch_city_forecast,
    search_cities,
)
from climatology import anomaly_snapshot
from config import Settings
from data_access import (
    available_station_months,
    daily_forecast,
    data_completeness_snapshot,
    health_snapshot,
    load_climate_normals,
    load_ensemble,
    load_forecast,
    load_forecast_history,
    load_forecast_reliability,
    load_latest_dpc_radar,
    load_measured_pollen,
    load_observed_air,
    load_official_alerts,
    load_official_station_status,
    load_provider_scores,
    load_recent_logs,
    load_reference_climate_normals,
    load_reference_scores,
    load_regime_scores,
    load_source_health,
    load_station,
    load_station_daily_summaries,
    load_station_month,
    load_station_profiles,
)
from dpc_radar import DpcRadarError, fetch_dpc_radar_snapshot
from ecowitt_diagnostics import load_ecowitt_diagnostics, telemetry_sensor_label
from feature_registry import features as feature_registry
from forecast_change import ForecastChangeSummary, summarize_forecast_change
from light_pollution import (
    LightPollutionError,
    LightPollutionEstimate,
    fetch_light_pollution,
)
from monthly_report import (
    MONTHS_IT,
    monthly_csv_bytes,
    monthly_pdf_bytes,
    report_filename,
)
from radar_nowcast import RadarNowcastError, fetch_radar_nowcast
from rain_consistency import reportable_rain_amount, reportable_rain_series
from share_card import ShareCardSummary, render_share_card
from terrain_horizon import (
    TerrainHorizonError,
    TerrainHorizonEstimate,
    combine_horizon_masks,
    fetch_terrain_horizon,
)
from ux_features import best_ventilation_window, daily_city_comparison
from weather_display import compass_direction, forecast_interval, weather_cell_style
from weather_experience import (
    activity_outlooks,
    aqi_category,
    build_daily_briefing,
    nearest_forecast,
    pollen_category,
    weather_insights,
)

st.set_page_config(
    page_title="Meteo V4",
    page_icon="🌦️",
    layout="wide",
    initial_sidebar_state="collapsed",
)
CFG = Settings.from_env()
PUBLIC_MAP_LAT = round(CFG.latitude, 2)
PUBLIC_MAP_LON = round(CFG.longitude, 2)
ITALIAN_WEEKDAYS = ("Lun", "Mar", "Mer", "Gio", "Ven", "Sab", "Dom")
TAB_SLUGS = {
    "Oggi": "today",
    "Panoramica": "overview",
    "7 giorni": "forecast",
    "Stazione": "station",
    "Aria": "air",
    "Astronomia": "astronomy",
    "Radar": "radar",
    "Sistema": "system",
}


st.markdown(
    """
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&display=swap');
:root {
  color-scheme:light !important;
  --page-bg:#f6f8fb; --sidebar-bg:#edf6fc; --surface:#ffffff; --surface-soft:#f8fafc;
  --ink:#10243d; --muted:#5b687b; --subtle:#475569; --line:rgba(148,163,184,.28);
  --blue:#2563eb; --card-bg:linear-gradient(155deg,rgba(255,255,255,.98),rgba(241,245,249,.9));
  --control-bg:#ffffff; --shadow:0 7px 22px rgba(15,23,42,.055);
  --scrollbar-track:#dbe7f1; --scrollbar-thumb:#267fc0; --scrollbar-hover:#155f96;
  --weather-icon-bg:linear-gradient(145deg,#dbeafe,#bfdbfe); --weather-icon-ring:rgba(37,99,235,.25);
}
html,body { color-scheme:light !important; }
html, body, [class*="css"] { font-family:'DM Sans',system-ui,sans-serif; }
.stApp,[data-testid="stAppViewContainer"],[data-testid="stMain"] {
  background:var(--page-bg) !important; color:var(--ink) !important;
}
[data-testid="stHeader"],[data-testid="stToolbar"],[data-testid="stDecoration"] {
  background:var(--page-bg) !important; color:var(--ink) !important;
}
section[data-testid="stSidebar"] { background:var(--sidebar-bg) !important; border-right:1px solid var(--line); }
.stApp h1,.stApp h2,.stApp h3,.stApp h4,.stApp h5,.stApp h6,
.stApp label,[data-testid="stCaptionContainer"] { color:var(--ink); }

/* Streamlit can retain the browser's native dark palette even while the app
   toggle is on light mode. Explicit widget descendants keep both modes
   readable and make the in-app theme the single source of truth. */
.stApp [data-testid="stMarkdownContainer"] p,
.stApp [data-testid="stMarkdownContainer"] li,
.stApp [data-testid="stWidgetLabel"],
.stApp [data-testid="stWidgetLabel"] p,
.stApp [data-testid="stWidgetLabel"] span,
.stApp [data-testid="stRadio"] label,
.stApp [data-testid="stRadio"] label p,
.stApp [data-testid="stRadio"] label span,
.stApp [role="radiogroup"] label,
.stApp [role="radiogroup"] label p,
.stApp [role="radiogroup"] label span,
.stApp [data-testid="stToggle"] label,
.stApp [data-testid="stToggle"] label p,
.stApp [data-testid="stToggle"] label span,
.stApp [data-testid="stSlider"] label,
.stApp [data-testid="stSlider"] label p,
.stApp [data-testid="stSlider"] [data-testid="stTickBar"] *,
.stApp [data-testid="stExpander"] summary,
.stApp [data-testid="stExpander"] summary *,
.stApp [data-testid="stAlert"],
.stApp [data-testid="stAlert"] *,
.stApp .stButton > button,
.stApp .stButton > button *,
.stApp [data-testid="stDownloadButton"] button,
.stApp [data-testid="stDownloadButton"] button * {
  color:var(--ink) !important;
}
.stApp [data-testid="stCaptionContainer"],
.stApp [data-testid="stCaptionContainer"] p,
.stApp [data-testid="stCaptionContainer"] span { color:var(--muted) !important; }
.stApp button[data-baseweb="tab"],
.stApp button[data-baseweb="tab"] *,
.stApp [data-testid="stTab"],
.stApp [data-testid="stTab"] * { color:var(--subtle) !important; opacity:1 !important; }
.stApp button[data-baseweb="tab"][aria-selected="true"],
.stApp button[data-baseweb="tab"][aria-selected="true"] *,
.stApp [data-testid="stTab"][aria-selected="true"],
.stApp [data-testid="stTab"][aria-selected="true"] * { color:var(--blue) !important; }
[data-testid="stHeader"] button,[data-testid="stHeader"] button *,
[data-testid="stSidebarCollapseButton"] button,
[data-testid="stSidebarCollapseButton"] button * { color:var(--ink) !important; }
[data-testid="stHeader"] button svg,
[data-testid="stSidebarCollapseButton"] svg { color:var(--ink) !important; fill:currentColor !important; }
[data-testid="stHeader"] button svg [fill="none"],
[data-testid="stSidebarCollapseButton"] svg [fill="none"] { fill:none !important; }
[data-testid="stMetricDelta"],[data-testid="stMetricDelta"] * { color:#10243d !important; }
.stApp :focus-visible { outline:2px solid var(--blue) !important; outline-offset:2px; }
.block-container { max-width:1480px; padding-top:1.2rem; padding-bottom:3rem; }
.hero { position:relative; overflow:hidden; color:white; padding:1.7rem 1.9rem; border-radius:24px;
  background:radial-gradient(circle at 85% 20%,rgba(255,255,255,.24),transparent 24%),
  linear-gradient(125deg,#0f3d78 0%,#0b76b7 48%,#10a6a0 100%); box-shadow:0 18px 45px rgba(15,61,120,.20); }
.hero h1 { margin:0; font-size:clamp(1.8rem,4vw,3rem); letter-spacing:-.045em; }
.hero p { margin:.45rem 0 0; opacity:.86; font-size:1rem; }
.hero,.hero *,
.stApp [data-testid="stMarkdownContainer"] .hero,
.stApp [data-testid="stMarkdownContainer"] .hero * { color:#fff !important; }
.eyebrow { font-size:.72rem; letter-spacing:.13em; text-transform:uppercase; font-weight:700; opacity:.76; }
.health-row { display:flex; flex-wrap:wrap; align-items:center; gap:.5rem; margin:.75rem 0 1.15rem; }
.pill { display:inline-flex; align-items:center; gap:.38rem; padding:.42rem .72rem; border-radius:999px;
  font-size:.78rem; font-weight:700; border:1px solid var(--line); background:var(--surface); color:var(--ink); }
.freshness-row { display:flex; flex-wrap:wrap; align-items:center; gap:.45rem; margin:-.65rem 0 1rem; }
.freshness-label { color:var(--muted); font-size:.72rem; font-weight:700; text-transform:uppercase; letter-spacing:.06em; }
.freshness-chip { display:inline-flex; align-items:center; gap:.32rem; padding:.3rem .52rem; border-radius:9px;
  border:1px solid var(--line); background:var(--surface-soft); color:var(--subtle); font-size:.71rem; }
.freshness-chip .dot { width:.4rem; height:.4rem; }
.dot { width:.48rem; height:.48rem; border-radius:99px; display:inline-block; }
.online .dot { background:#009e73; box-shadow:0 0 0 4px rgba(0,158,115,.14); }
.delayed .dot { background:#e69f00; box-shadow:0 0 0 4px rgba(230,159,0,.14); }
.offline .dot { background:#d55e00; box-shadow:0 0 0 4px rgba(213,94,0,.14); }
.forecast-grid { display:grid; grid-template-columns:repeat(7,minmax(145px,1fr)); gap:.7rem; margin:.3rem 0 1.2rem; }
.day-card { border:1px solid var(--line); border-radius:17px; padding:.92rem; background:var(--card-bg);
  box-shadow:var(--shadow); min-height:205px; }
.day-name { color:var(--blue); font-size:.76rem; font-weight:700; text-transform:uppercase; letter-spacing:.07em; }
.day-icon { display:flex; align-items:center; justify-content:center; width:2.65rem; height:2.65rem;
  font-size:1.75rem; margin:.32rem 0; border-radius:50%; background:var(--weather-icon-bg);
  border:1px solid var(--weather-icon-ring); filter:saturate(1.12) contrast(1.08);
  text-shadow:0 1px 2px rgba(15,23,42,.22); }
.day-temp { font-size:1.18rem; font-weight:700; color:var(--ink); }
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
.station-active-card { display:flex; align-items:center; gap:.7rem; padding:.72rem .78rem; margin:.15rem 0 .7rem;
  border:1px solid rgba(37,99,235,.38); border-radius:14px;
  background:linear-gradient(135deg,rgba(37,99,235,.14),var(--surface)); box-shadow:var(--shadow); }
.station-active-dot { width:.62rem; height:.62rem; flex:0 0 .62rem; border-radius:50%; background:#009e73;
  box-shadow:0 0 0 4px rgba(0,158,115,.14); }
.station-active-copy { min-width:0; }.station-active-copy small { display:block; color:var(--muted); font-size:.66rem;
  font-weight:700; letter-spacing:.065em; text-transform:uppercase; }
.station-active-copy strong { display:block; color:var(--ink); font-size:.86rem; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
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
.weather-table-wrap { width:100%; overflow:auto; border:1px solid var(--line); border-radius:14px;
  background:var(--surface); box-shadow:var(--shadow); margin:.35rem 0 .8rem; }
.weather-table-wrap table { width:100%; min-width:760px; border-collapse:separate; border-spacing:0;
  font-size:.79rem; line-height:1.35; }
.weather-table-wrap thead th { position:sticky; top:0; z-index:2; padding:.68rem .62rem !important;
  background:#e8f1f8 !important; color:#10243d !important; border:0 !important;
  border-bottom:1px solid var(--line) !important; text-align:left; white-space:nowrap; }
.weather-table-wrap tbody td { padding:.58rem .62rem !important; border:0 !important;
  border-bottom:1px solid var(--line) !important; white-space:nowrap; }
.weather-table-wrap tbody tr:last-child td { border-bottom:0 !important; }
.weather-table-wrap tbody tr:hover td { box-shadow:inset 0 0 0 999px rgba(96,165,250,.08); }
.weather-table-tools { display:flex; justify-content:space-between; align-items:center; gap:.7rem;
  margin:.2rem 0 .35rem; color:var(--muted); font-size:.74rem; }
.city-current { display:flex; align-items:center; gap:.85rem; padding:.9rem 1rem; margin:.7rem 0 1rem;
  border:1px solid var(--line); border-radius:17px; background:var(--surface); box-shadow:var(--shadow); }
.city-current-icon { display:flex; align-items:center; justify-content:center; width:3.35rem; height:3.35rem;
  flex:0 0 3.35rem; border-radius:50%; font-size:2.4rem; line-height:1; background:var(--weather-icon-bg);
  border:1px solid var(--weather-icon-ring); filter:saturate(1.12) contrast(1.08);
  text-shadow:0 1px 2px rgba(15,23,42,.22); }
.city-current-copy strong { color:var(--ink); font-size:1.05rem; }
.city-current-copy div { color:var(--muted); font-size:.78rem; margin-top:.15rem; }
.hero-v4 { position:relative; overflow:hidden; color:#fff; padding:1.4rem 1.55rem; border-radius:26px;
  min-height:245px; display:grid; grid-template-columns:minmax(0,1fr) auto; align-items:end; gap:1rem;
  box-shadow:0 22px 55px rgba(15,61,120,.22); isolation:isolate; }
.hero-v4::before { content:""; position:absolute; width:20rem; height:20rem; border-radius:50%;
  right:-5rem; top:-8rem; background:rgba(255,255,255,.16); filter:blur(1px); z-index:-1; }
.hero-v4::after { content:""; position:absolute; inset:0; z-index:-2;
  background:linear-gradient(125deg,#0b3f7a 0%,#087eb6 52%,#20aca1 100%); }
.hero-v4.hero-clear::after { background:linear-gradient(125deg,#1456a0 0%,#1297db 55%,#58c6dd 100%); }
.hero-v4.hero-cloud::after { background:linear-gradient(125deg,#334155 0%,#54738f 52%,#6fa7b6 100%); }
.hero-v4.hero-rain::after { background:linear-gradient(125deg,#172554 0%,#1d4f7a 52%,#477a91 100%); }
.hero-v4.hero-night::after { background:radial-gradient(circle at 82% 16%,rgba(191,219,254,.22),transparent 16%),
  linear-gradient(125deg,#11152d 0%,#172554 55%,#164e63 100%); }
.hero-v4,.hero-v4 * { color:#fff !important; }
.hero-v4 .eyebrow { margin-bottom:.65rem; }
.hero-place { font-size:clamp(1.5rem,3vw,2.35rem); font-weight:700; line-height:1.05; letter-spacing:-.04em; }
.hero-condition { display:flex; align-items:center; gap:.8rem; margin:.75rem 0 .35rem; }
.hero-weather-icon { font-size:clamp(2.5rem,6vw,4.4rem); line-height:1; filter:drop-shadow(0 6px 13px rgba(0,0,0,.18)); }
.hero-temperature { font-size:clamp(3.1rem,8vw,5.9rem); line-height:.9; font-weight:600; letter-spacing:-.07em; }
.hero-description { font-size:1.05rem; font-weight:700; margin-bottom:.18rem; }
.hero-secondary { opacity:.82; font-size:.82rem; line-height:1.5; }
.hero-brief { max-width:440px; padding:1rem 1.05rem; border-radius:18px; align-self:end;
  background:rgba(8,18,35,.22); border:1px solid rgba(255,255,255,.2); backdrop-filter:blur(12px); }
.hero-brief strong { display:block; font-size:1.02rem; line-height:1.35; margin:.3rem 0; }
.hero-brief small { display:block; opacity:.78; line-height:1.45; }
.v4-section-head { display:flex; justify-content:space-between; align-items:end; gap:1rem; margin:.9rem 0 .65rem; }
.v4-section-head h3 { margin:0; font-size:1.35rem; }.v4-section-head span { color:var(--muted); font-size:.76rem; }
.current-grid { display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:.65rem; margin:.8rem 0 1rem; }
.current-card { min-width:0; min-height:118px; border:1px solid var(--line); border-radius:18px; padding:.82rem .88rem;
  background:var(--surface); box-shadow:var(--shadow); }
.current-head,.air-title-row { display:flex; align-items:center; justify-content:space-between; gap:.45rem; min-width:0; }
.current-label { min-width:0; color:var(--muted); font-size:.7rem; font-weight:700; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
.current-value { color:var(--ink); font-size:clamp(1.28rem,2.2vw,1.85rem); line-height:1.1; font-weight:650;
  letter-spacing:-.045em; margin:.46rem 0 .35rem; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
.current-detail { display:inline-block; max-width:100%; color:var(--subtle); background:var(--surface-soft); border-radius:999px;
  padding:.23rem .46rem; font-size:.66rem; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
.live-badge { flex:0 0 auto; display:inline-flex; align-items:center; gap:.28rem; padding:.2rem .4rem;
  border-radius:999px; font-size:.58rem; font-weight:800; letter-spacing:.055em; line-height:1;
  border:1px solid currentColor; }
.live-badge .live-dot { width:.4rem; height:.4rem; border-radius:50%; background:currentColor; }
.live-badge.is-live { color:#007a5a !important; background:rgba(0,158,115,.12); }
.live-badge.is-stale { color:#b42318 !important; background:rgba(213,94,0,.12); }
.live-badge.is-live .live-dot { animation:live-pulse 1.7s ease-out infinite; }
@keyframes live-pulse { 0%{box-shadow:0 0 0 0 rgba(0,158,115,.55)} 70%{box-shadow:0 0 0 6px rgba(0,158,115,0)} 100%{box-shadow:0 0 0 0 rgba(0,158,115,0)} }
@media(prefers-reduced-motion:reduce){.live-badge.is-live .live-dot{animation:none}.expandable-card{transition:none!important}}
.hourly-strip { display:grid; grid-auto-flow:column; grid-auto-columns:minmax(116px,1fr); gap:.65rem;
  overflow-x:auto; overscroll-behavior-inline:contain; padding:.15rem .05rem .9rem;
  scrollbar-width:auto; scrollbar-color:var(--scrollbar-thumb) var(--scrollbar-track); scrollbar-gutter:stable; }
.hourly-tile { min-height:174px; padding:.8rem .72rem; border-radius:17px; border:1px solid var(--line);
  background:var(--surface); box-shadow:var(--shadow); text-align:center; }
.hourly-tile.is-now { border-color:var(--blue); box-shadow:0 0 0 2px color-mix(in srgb,var(--blue) 20%,transparent); }
.hourly-time { color:var(--muted); font-size:.72rem; font-weight:700; }
.hourly-icon { display:flex; align-items:center; justify-content:center; width:2.55rem; height:2.55rem;
  margin:.32rem auto; border-radius:50%; font-size:1.7rem; background:var(--weather-icon-bg);
  border:1px solid var(--weather-icon-ring); filter:saturate(1.12) contrast(1.08);
  text-shadow:0 1px 2px rgba(15,23,42,.22); }
.hourly-temp { color:var(--ink); font-size:1.2rem; font-weight:700; }.hourly-rain,.hourly-wind { color:var(--subtle); font-size:.69rem; margin-top:.2rem; }
.hourly-cloud { color:var(--subtle); font-size:.69rem; margin-top:.2rem; font-weight:650; }
.hourly-strip::-webkit-scrollbar,.weather-table-wrap::-webkit-scrollbar,
div[data-baseweb="tab-list"]::-webkit-scrollbar { height:11px; width:11px; }
.hourly-strip::-webkit-scrollbar-track,.weather-table-wrap::-webkit-scrollbar-track,
div[data-baseweb="tab-list"]::-webkit-scrollbar-track { background:var(--scrollbar-track); border-radius:999px; }
.hourly-strip::-webkit-scrollbar-thumb,.weather-table-wrap::-webkit-scrollbar-thumb,
div[data-baseweb="tab-list"]::-webkit-scrollbar-thumb { background:var(--scrollbar-thumb); border-radius:999px;
  border:2px solid var(--scrollbar-track); min-width:44px; }
.hourly-strip::-webkit-scrollbar-thumb:hover,.weather-table-wrap::-webkit-scrollbar-thumb:hover,
div[data-baseweb="tab-list"]::-webkit-scrollbar-thumb:hover { background:var(--scrollbar-hover); }
.weather-table-wrap,div[data-baseweb="tab-list"] { scrollbar-color:var(--scrollbar-thumb) var(--scrollbar-track); }
.insight-grid { display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:.7rem; margin:.35rem 0 1.1rem; }
.insight-card,.activity-card,.air-card { border:1px solid var(--line); border-radius:18px; padding:.95rem;
  background:var(--surface); box-shadow:var(--shadow); position:relative; overflow:hidden; }
.insight-card::before,.activity-card::before,.air-card::before { content:""; position:absolute; inset:0 auto 0 0; width:4px; background:#94a3b8; }
.tone-good::before { background:#009e73; }.tone-warning::before { background:#e69f00; }.tone-danger::before { background:#d55e00; }
.insight-title,.activity-title,.air-title { min-width:0; color:var(--muted); font-size:.72rem; font-weight:700; text-transform:uppercase; letter-spacing:.045em; overflow-wrap:anywhere; }
.insight-value,.air-value { color:var(--ink); font-size:1.05rem; font-weight:700; margin:.42rem 0 .22rem; }
.insight-detail,.activity-detail,.air-detail { color:var(--subtle); font-size:.72rem; line-height:1.45; }
.activity-grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(210px,1fr)); gap:.7rem; margin:.35rem 0 1rem; }
.activity-top { display:flex; align-items:center; justify-content:space-between; gap:.5rem; }.activity-icon { font-size:1.6rem; }
.activity-score { color:var(--ink); font-size:1.55rem; font-weight:700; letter-spacing:-.04em; }
.activity-label { display:inline-block; margin:.35rem 0; padding:.18rem .46rem; border-radius:999px;
  background:var(--surface-soft); color:var(--subtle); font-size:.68rem; font-weight:700; }
.activity-time { color:var(--blue); font-size:.76rem; font-weight:700; margin-bottom:.28rem; }
.air-grid { display:grid; grid-template-columns:repeat(6,minmax(0,1fr)); gap:.65rem; margin:.4rem 0 1rem; }
.air-card { min-height:128px; }.air-value { font-size:1.35rem; }.air-source { color:var(--muted); font-size:.7rem; margin-top:.65rem; }
.expandable-card { transition:border-color .18s ease,box-shadow .18s ease,transform .18s ease; }
.expandable-card:hover { border-color:color-mix(in srgb,var(--blue) 48%,var(--line)); transform:translateY(-1px); }
.expandable-card[open] { border-color:color-mix(in srgb,var(--blue) 65%,var(--line));
  box-shadow:0 14px 34px rgba(15,61,120,.13); }
.expandable-card > summary { display:block; list-style:none; cursor:pointer; outline:none; }
.expandable-card > summary::-webkit-details-marker { display:none; }
.expandable-card > summary::marker { content:""; }
.expandable-card > summary:focus-visible { outline:2px solid var(--blue); outline-offset:5px; border-radius:10px; }
.expand-hint { display:flex; align-items:center; gap:.3rem; margin-top:.58rem; color:var(--blue);
  font-size:.65rem; font-weight:750; letter-spacing:.02em; }
.expand-hint::after { content:"↓"; font-size:.78rem; transition:transform .18s ease; }
.expandable-card[open] .expand-hint::after { transform:rotate(180deg); }
.expandable-card[open] .expand-hint .closed-label { display:none; }
.expand-hint .open-label { display:none; }
.expandable-card[open] .expand-hint .open-label { display:inline; }
.card-expanded { margin-top:.72rem; padding-top:.68rem; border-top:1px solid var(--line);
  color:var(--subtle); font-size:.73rem; line-height:1.55; }
.current-card[open],.day-card[open],.hour-card[open],.insight-card[open],.activity-card[open],.air-card[open] {
  grid-column:span 2; }
.hourly-tile[open] { grid-column:span 2; min-width:235px; text-align:left; }
.hourly-tile[open] .hourly-icon { margin-left:0; }
.v4-note { border:1px solid var(--line); border-radius:15px; background:var(--surface-soft); padding:.75rem .9rem;
  color:var(--subtle); font-size:.74rem; line-height:1.5; }
.change-card { display:grid; grid-template-columns:auto minmax(0,1fr); gap:.85rem; align-items:center;
  border:1px solid var(--line); border-radius:18px; background:var(--surface); box-shadow:var(--shadow);
  padding:.9rem 1rem; margin:.35rem 0 1rem; }
.change-score { display:flex; align-items:center; justify-content:center; width:4.3rem; height:4.3rem;
  border-radius:50%; background:var(--surface-soft); color:var(--ink); font-size:1.16rem; font-weight:800; }
.change-card.stable .change-score { box-shadow:inset 0 0 0 3px #009e73; }
.change-card.evolving .change-score { box-shadow:inset 0 0 0 3px #e69f00; }
.change-card.changed .change-score { box-shadow:inset 0 0 0 3px #d55e00; }
.change-title { color:var(--ink); font-size:1rem; font-weight:750; }.change-detail { color:var(--subtle); font-size:.76rem; margin-top:.2rem; line-height:1.45; }
.source-grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(215px,1fr)); gap:.55rem; margin:.4rem 0; }
.source-card { border:1px solid var(--line); border-radius:14px; padding:.72rem .78rem; background:var(--surface-soft); }
.source-card strong { display:block; color:var(--ink); font-size:.78rem; }.source-card span { color:var(--muted); font-size:.7rem; line-height:1.45; }
.accessibility-note { display:flex; gap:.45rem; align-items:flex-start; color:var(--muted); font-size:.7rem; margin:.35rem 0 .8rem; }
[data-testid="stForm"] { border-color:var(--line) !important; background:var(--surface-soft) !important; border-radius:16px; }
[data-testid="stTextInput"] input,[data-baseweb="input"] { background:var(--control-bg) !important;
  color:var(--ink) !important; border-color:var(--line) !important; }
[data-testid="stDownloadButton"] button { background:var(--control-bg) !important; color:var(--ink) !important;
  border-color:var(--line) !important; }

/* Complete widget palette. Streamlit/BaseWeb can otherwise inherit the OS
   colour scheme after hydration, producing dark number fields on the light
   theme and low-contrast form buttons. These selectors intentionally cover
   the stable public test IDs as well as their input descendants. */
.stApp [data-testid="stNumberInputContainer"],
.stApp [data-testid="stTextInputRootElement"],
.stApp [data-testid="stTextInput"] > div > div,
.stApp [data-baseweb="input"],
.stApp [data-baseweb="select"] > div {
  background:var(--control-bg) !important;
  color:var(--ink) !important;
  border-color:var(--line) !important;
  color-scheme:inherit !important;
}
.stApp [data-testid="stNumberInputContainer"] input,
.stApp [data-testid="stNumberInputField"],
.stApp [data-testid="stTextInput"] input,
.stApp [data-baseweb="input"] input {
  background:transparent !important;
  color:var(--ink) !important;
  -webkit-text-fill-color:var(--ink) !important;
  caret-color:var(--blue) !important;
  opacity:1 !important;
}
.stApp [data-testid^="stNumberInputStep"] {
  background:var(--surface-soft) !important;
  color:var(--ink) !important;
  border-color:var(--line) !important;
}
.stApp [data-testid^="stNumberInputStep"] *,
.stApp [data-testid^="stNumberInputStep"] svg {
  color:var(--ink) !important;
  fill:currentColor !important;
  opacity:1 !important;
}
.stApp [data-testid="stFormSubmitButton"] button,
.stApp button[data-testid="stBaseButton-secondaryFormSubmit"],
.stApp [data-testid="stBaseButton-secondaryFormSubmit"] {
  background:var(--control-bg) !important;
  color:var(--ink) !important;
  border:1px solid var(--line) !important;
  opacity:1 !important;
}
.stApp [data-testid="stFormSubmitButton"] button *,
.stApp [data-testid="stBaseButton-secondaryFormSubmit"] * {
  color:var(--ink) !important;
  opacity:1 !important;
}
.stApp [data-testid="stFormSubmitButton"] button:disabled,
.stApp [data-testid="stBaseButton-secondaryFormSubmit"]:disabled {
  background:var(--surface-soft) !important;
  color:var(--muted) !important;
  -webkit-text-fill-color:var(--muted) !important;
  opacity:1 !important;
  cursor:not-allowed !important;
}
.stApp [data-testid="stCaptionContainer"],
.stApp [data-testid="stCaptionContainer"] * {
  color:var(--muted) !important;
  opacity:1 !important;
}
.stApp input::placeholder,
.stApp textarea::placeholder {
  color:var(--muted) !important;
  -webkit-text-fill-color:var(--muted) !important;
  opacity:1 !important;
}
@media(max-width:1050px){.forecast-grid{grid-template-columns:repeat(4,minmax(140px,1fr));}}
@media(max-width:1050px){.current-grid{grid-template-columns:repeat(2,minmax(0,1fr));}}
@media(max-width:680px){
  .block-container{padding:.7rem}.hero{padding:1.25rem;border-radius:18px}
  .forecast-grid{grid-template-columns:repeat(2,minmax(135px,1fr));}.day-card{min-height:190px}
  .hour-grid{grid-template-columns:1fr}.weather-table-wrap table{font-size:.74rem}.city-current{align-items:flex-start}
  .hero-v4{grid-template-columns:1fr;min-height:0;padding:1.15rem;border-radius:19px}.hero-brief{max-width:none}
  .insight-grid,.activity-grid{grid-template-columns:repeat(2,minmax(0,1fr))}.air-grid{grid-template-columns:repeat(2,minmax(0,1fr))}
  .current-grid{grid-template-columns:repeat(2,minmax(0,1fr))}.current-card{min-height:106px}
  .hourly-strip{grid-auto-columns:105px}.v4-section-head{align-items:flex-start;flex-direction:column;gap:.15rem}
  div[data-baseweb="tab-list"]{overflow-x:auto;scrollbar-width:thin;flex-wrap:nowrap}
  button[data-baseweb="tab"],[data-testid="stTab"]{flex:0 0 auto;white-space:nowrap}
  [data-testid="stMetric"]{min-height:102px;padding:.65rem .75rem}
  .expandable-card[open] { grid-column:1 / -1; }
}
</style>
""",
    unsafe_allow_html=True,
)


@st.cache_data(ttl=600, show_spinner=False)
def station_data(hours: int, station_id: str) -> pd.DataFrame:
    return load_station(hours, station_id)


@st.cache_data(ttl=600, show_spinner=False)
def station_profiles_data() -> pd.DataFrame:
    return load_station_profiles()


@st.cache_data(ttl=600, show_spinner=False)
def station_daily_data(days: int = 365) -> pd.DataFrame:
    return load_station_daily_summaries(days)


@st.cache_data(ttl=600, show_spinner=False)
def station_months_data(station_id: str, timezone: str) -> list[tuple[int, int]]:
    return available_station_months(station_id, timezone)


@st.cache_data(ttl=600, show_spinner=False)
def station_month_data(
    station_id: str, year: int, month: int, timezone: str
) -> pd.DataFrame:
    return load_station_month(station_id, year, month, timezone)


@st.cache_data(ttl=600, show_spinner=False)
def forecast_data() -> pd.DataFrame:
    return load_forecast()


@st.cache_data(ttl=600, show_spinner=False)
def forecast_history_data() -> pd.DataFrame:
    return load_forecast_history(hours=48, emissions=2)


@st.cache_data(ttl=600, show_spinner=False)
def ensemble_guidance_data() -> pd.DataFrame:
    return load_ensemble()


@st.cache_data(ttl=600, show_spinner=False)
def observed_air_data() -> pd.DataFrame:
    return load_observed_air()


@st.cache_data(ttl=600, show_spinner=False)
def measured_pollen_data() -> pd.DataFrame:
    return load_measured_pollen()


@st.cache_data(ttl=600, show_spinner=False)
def climate_normals_data() -> pd.DataFrame:
    return load_climate_normals()


@st.cache_data(ttl=3_600, show_spinner=False)
def reference_climate_data(station_id: str) -> pd.DataFrame:
    return load_reference_climate_normals(station_id)


@st.cache_data(ttl=600, show_spinner=False)
def official_alerts_data() -> pd.DataFrame:
    return load_official_alerts()


@st.cache_data(ttl=600, show_spinner=False)
def health_data() -> dict[str, Any]:
    return health_snapshot(Settings.from_env())


@st.cache_data(ttl=600, show_spinner=False)
def score_data() -> pd.DataFrame:
    return load_provider_scores()


@st.cache_data(ttl=600, show_spinner=False)
def regime_score_data() -> pd.DataFrame:
    return load_regime_scores()


@st.cache_data(ttl=600, show_spinner=False)
def reference_score_data() -> pd.DataFrame:
    return load_reference_scores()


@st.cache_data(ttl=600, show_spinner=False)
def reliability_data() -> pd.DataFrame:
    return load_forecast_reliability()


@st.cache_data(ttl=600, show_spinner=False)
def source_status_data() -> pd.DataFrame:
    return load_source_health(Settings.from_env())


@st.cache_data(ttl=600, show_spinner=False)
def completeness_data() -> dict[str, Any]:
    return data_completeness_snapshot(24)


@st.cache_data(ttl=600, show_spinner=False)
def ecowitt_diagnostic_data(station_id: str):
    return load_ecowitt_diagnostics(
        station_id=station_id,
        stale_minutes=min(CFG.station_stale_minutes, 30),
    )


@st.cache_data(ttl=600, show_spinner=False)
def official_station_data() -> pd.DataFrame:
    return load_official_station_status()


@st.cache_data(ttl=600, show_spinner=False)
def log_data() -> pd.DataFrame:
    return load_recent_logs()


@st.cache_data(ttl=86_400, show_spinner=False)
def city_search_data(query: str) -> list[CityLocation]:
    return search_cities(query)


@st.cache_data(ttl=600, show_spinner=False)
def city_forecast_data(location: CityLocation) -> CityForecast:
    return fetch_city_forecast(location)


@st.cache_data(ttl=600, show_spinner=False)
def air_quality_data(
    latitude: float, longitude: float, timezone: str
) -> AirQualityForecast:
    return fetch_air_quality(latitude, longitude, timezone)


@st.cache_data(ttl=21_600, show_spinner=False)
def light_pollution_data() -> tuple[LightPollutionEstimate | None, str | None]:
    try:
        return fetch_light_pollution(CFG.latitude, CFG.longitude), None
    except LightPollutionError as exc:
        return None, str(exc)


@st.cache_data(ttl=600, show_spinner=False)
def radar_nowcast_data(latitude: float, longitude: float):
    return fetch_radar_nowcast(latitude, longitude)


@st.cache_data(ttl=600, show_spinner=False)
def dpc_radar_live_data(cfg: Settings):
    return fetch_dpc_radar_snapshot(cfg)


@st.cache_data(ttl=600, show_spinner=False)
def dpc_radar_archive_data(station_id: str) -> pd.DataFrame:
    return load_latest_dpc_radar(station_id)


@st.fragment(run_every=600)
def _refresh_controller(enabled: bool) -> None:
    """Refresh the full app every ten minutes without injecting browser script."""
    if not enabled:
        return
    now = time.monotonic()
    previous = st.session_state.get("last_full_refresh")
    if previous is None:
        st.session_state["last_full_refresh"] = now
    elif now - previous >= 595:
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
  color-scheme:dark !important;
  --page-bg:#05070b; --sidebar-bg:#0b111b; --surface:#101826; --surface-soft:#0d1522;
  --ink:#f8fafc; --muted:#b6c2d1; --subtle:#d5deea; --line:rgba(226,232,240,.2);
  --blue:#60a5fa; --card-bg:linear-gradient(155deg,#111b2b,#0b1320);
  --control-bg:#111827; --shadow:0 9px 28px rgba(0,0,0,.28);
  --scrollbar-track:#172235; --scrollbar-thumb:#60a5fa; --scrollbar-hover:#93c5fd;
  --weather-icon-bg:linear-gradient(145deg,#1e3a5f,#172554); --weather-icon-ring:rgba(147,197,253,.4);
}
html,body { color-scheme:dark !important; }
.stApp,[data-testid="stAppViewContainer"],[data-testid="stHeader"] { background:#05070b !important; color:var(--ink) !important; }
section[data-testid="stSidebar"] { background:#0b111b !important; }
.stApp p,.stApp li,.stApp label,.stApp span,.stApp div { border-color:var(--line); }
.stApp h1,.stApp h2,.stApp h3,.stApp h4,.stApp h5,.stApp h6,
.stApp label,.stApp p,[data-testid="stCaptionContainer"],button[data-baseweb="tab"],
.stApp [data-testid="stTab"] { color:var(--ink) !important; }

/* Streamlit assegna colori propri ai discendenti dei widget. Li riallineiamo
   esplicitamente al tema, evitando le celle delle tabelle con colori semantici. */
.stApp [data-testid="stMarkdownContainer"] p,
.stApp [data-testid="stMarkdownContainer"] li,
.stApp [data-testid="stWidgetLabel"] p,
.stApp [data-testid="stWidgetLabel"] span,
.stApp [data-testid="stCaptionContainer"] p,
.stApp [data-testid="stCaptionContainer"] span,
.stApp [data-testid="stToggle"] label,
.stApp [data-testid="stToggle"] label p,
.stApp [data-testid="stToggle"] label span,
.stApp [data-testid="stSlider"] label,
.stApp [data-testid="stSlider"] label p,
.stApp [data-testid="stSlider"] [data-testid="stTickBar"] *,
.stApp [role="radiogroup"] label,
.stApp [role="radiogroup"] label p,
.stApp [role="radiogroup"] label span,
.stApp button[data-baseweb="tab"],
.stApp button[data-baseweb="tab"] p,
.stApp button[data-baseweb="tab"] span,
.stApp [data-testid="stTab"],
.stApp [data-testid="stTab"] *,
.stApp [data-testid="stExpander"] summary,
.stApp [data-testid="stExpander"] summary p,
.stApp [data-testid="stExpander"] summary span,
.stApp [data-testid="stExpanderDetails"] p,
.stApp [data-testid="stAlert"] p,
.stApp [data-testid="stAlert"] span,
.stApp .stButton > button,
.stApp .stButton > button p,
.stApp .stButton > button span {
  color:var(--ink) !important;
}
.stApp [data-testid="stCaptionContainer"],
.stApp [data-testid="stCaptionContainer"] p,
.stApp [data-testid="stCaptionContainer"] span { color:var(--muted) !important; }
.stApp a:not(.hero a) { color:#7dd3fc !important; }
.stApp button[data-baseweb="tab"][aria-selected="true"],
.stApp button[data-baseweb="tab"][aria-selected="true"] *,
.stApp [data-testid="stTab"][aria-selected="true"],
.stApp [data-testid="stTab"][aria-selected="true"] * { color:var(--blue) !important; }
.stApp .hero,.stApp .hero * { color:#fff !important; }
[data-testid="stMetric"],[data-testid="stExpander"],.stButton > button,
[data-baseweb="select"] > div,[data-testid="stTextInput"] input { background:var(--surface) !important; color:var(--ink) !important; }
[data-testid="stMetricLabel"],[data-testid="stMetricLabel"] p { color:var(--muted) !important; }
[data-testid="stMetricValue"],[data-testid="stMetricValue"] div { color:var(--ink) !important; }
[data-testid="stMetricDelta"],[data-testid="stMetricDelta"] * { color:#f8fafc !important; }
[data-testid="stAlert"] { background:var(--surface) !important; color:var(--ink) !important; border-color:var(--line) !important; }
[data-testid="stDataFrame"],[data-testid="stDataFrame"] [role="grid"] { background:var(--surface) !important; color:var(--ink) !important; }
[data-testid="stDataFrame"] [role="columnheader"],
[data-testid="stDataFrame"] [role="columnheader"] *,
[data-testid="stDataFrame"] [data-testid="stElementToolbar"] button,
[data-testid="stDataFrame"] [data-testid="stElementToolbar"] button * { color:var(--ink) !important; }
[data-testid="stDataFrame"] button svg,
[data-testid="stElementToolbar"] button svg { color:var(--ink) !important; fill:var(--ink) !important; }
[data-testid="stSidebarCollapseButton"] button,
[data-testid="stSidebarCollapseButton"] button *,
[data-testid="stBaseButton-headerNoPadding"],
[data-testid="stBaseButton-headerNoPadding"] * { color:var(--ink) !important; }
[data-testid="stHeader"] button svg,
[data-testid="stSidebarCollapseButton"] svg { color:var(--ink) !important; fill:var(--ink) !important; }
[data-testid="stPlotlyChart"] text.legendtext,
[data-testid="stPlotlyChart"] text.legendtitletext,
[data-testid="stPlotlyChart"] .gtitle,
[data-testid="stPlotlyChart"] .annotation-text-g text { fill:var(--ink) !important; color:var(--ink) !important; }
[data-testid="stPlotlyChart"] .modebar-btn path { fill:var(--ink) !important; }
.weather-table-wrap thead th { background:#162235 !important; color:#f8fafc !important;
  border-bottom-color:rgba(226,232,240,.2) !important; }
.weather-table-wrap { background:#0b111b !important; }
.weather-table-wrap tbody td { border-bottom-color:rgba(226,232,240,.14) !important; }
[data-testid="stForm"],[data-testid="stTextInput"] > div > div,[data-baseweb="input"] {
  background:var(--surface) !important; color:var(--ink) !important; border-color:var(--line) !important; }
[data-testid="stTextInput"] input::placeholder { color:#94a3b8 !important; opacity:1; }
[data-testid="stDownloadButton"] button,[data-testid="stDownloadButton"] button * { color:var(--ink) !important; }

/* I menu BaseWeb vengono montati fuori da .stApp: per questo richiedono
   selettori globali e sono il caso che in precedenza restava bianco. */
body [data-baseweb="popover"] > div,
body [data-baseweb="menu"],
body ul[role="listbox"],
body div[role="listbox"],
body [data-baseweb="calendar"] {
  background:var(--control-bg) !important;
  color:var(--ink) !important;
  border-color:var(--line) !important;
}
body [data-baseweb="popover"] [role="option"],
body [data-baseweb="popover"] [role="option"] *,
body [data-baseweb="menu"] li,
body [data-baseweb="menu"] li *,
body [data-baseweb="calendar"] * { color:var(--ink) !important; }
body [data-baseweb="popover"] [role="option"] { background:var(--control-bg) !important; }
body [data-baseweb="popover"] [role="option"]:hover,
body [data-baseweb="popover"] [role="option"][aria-selected="true"] { background:#1d4ed8 !important; }
body [data-baseweb="popover"] [role="option"]:hover *,
body [data-baseweb="popover"] [role="option"][aria-selected="true"] * { color:#fff !important; }
body [data-baseweb="tooltip"],body [role="tooltip"] {
  background:#111827 !important; color:var(--ink) !important; border:1px solid var(--line) !important;
}
body [data-baseweb="tooltip"] *,body [role="tooltip"] * { color:var(--ink) !important; }
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


def _valid_timestamp(value: Any) -> bool:
    return bool(pd.notna(pd.to_datetime(value, utc=True, errors="coerce")))


def _hour_label(value: Any) -> str:
    timestamp = pd.to_datetime(value, errors="coerce")
    if pd.isna(timestamp):
        return "—"
    return f"{ITALIAN_WEEKDAYS[timestamp.weekday()]} {timestamp:%d · %H:%M}"


def _day_label(value: Any) -> str:
    timestamp = pd.to_datetime(value, errors="coerce")
    if pd.isna(timestamp):
        return "—"
    return f"{ITALIAN_WEEKDAYS[timestamp.weekday()]} {timestamp:%d/%m}"


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


def _optional_decimal(value: Any) -> float | None:
    rendered = str(value or "").strip().replace(",", ".")
    return None if not rendered else float(rendered)


def _filename_slug(value: str) -> str:
    slug = "".join(character if character.isalnum() else "-" for character in value)
    return "-".join(part for part in slug.casefold().split("-") if part) or "meteo"


def _numeric_series(
    frame: pd.DataFrame, column: str, default: float = np.nan
) -> pd.Series:
    if column not in frame:
        return pd.Series(default, index=frame.index, dtype=float)
    return pd.to_numeric(frame[column], errors="coerce")


def _style_plotly(figure: go.Figure, dark_mode: bool) -> go.Figure:
    """Keep plots readable and let Plotly show the real trace colour in legends."""
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
    legend_entries = sum(
        1
        for trace in figure.data
        if trace.showlegend is not False and bool(getattr(trace, "name", None))
    )
    legend_rows = max(1, (legend_entries + 2) // 3)
    current_top = int(figure.layout.margin.t or 0)
    figure.update_layout(
        template=template,
        paper_bgcolor=paper,
        plot_bgcolor=plot,
        font={"color": ink, "family": "DM Sans, system-ui, sans-serif"},
        title={"font": {"color": ink}},
        legend={
            "orientation": "h",
            "x": 0,
            "xanchor": "left",
            "y": 1.02,
            "yanchor": "bottom",
            "bgcolor": "rgba(11,17,27,.90)" if dark_mode else "rgba(255,255,255,.94)",
            "bordercolor": line,
            "borderwidth": 1,
            "font": {"color": ink, "size": 13},
            "title": {"font": {"color": ink}},
            "tracegroupgap": 6,
            "itemsizing": "constant",
        },
        margin={"t": max(current_top, 58 + legend_rows * 30)},
        modebar={
            "bgcolor": "rgba(0,0,0,0)",
            "color": ink,
            "activecolor": "#60a5fa" if dark_mode else "#2563eb",
        },
        hoverlabel={"bgcolor": hover_bg, "font_color": ink, "bordercolor": line},
    )
    for annotation in figure.layout.annotations or ():
        annotation.font.color = ink
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


def _framing_figure(
    target: SkyTarget,
    profile: EquipmentProfile,
    rotation_deg: float,
    dark_mode: bool,
) -> go.Figure:
    """Draw a geometric camera footprint without pretending to be a sky survey."""
    geometry = framing_geometry(target, profile, rotation_deg=rotation_deg)
    figure = go.Figure()
    figure.add_trace(
        go.Scatter(
            x=geometry["sensor_x"],
            y=geometry["sensor_y"],
            mode="lines",
            fill="toself",
            fillcolor="rgba(14,165,233,.12)",
            line={"color": "#0ea5e9", "width": 3},
            name="Campo del sensore",
            hovertemplate="Sensore · %{x:.1f}′ × %{y:.1f}′<extra></extra>",
        )
    )
    has_dimensions = target.angular_width_arcmin is not None
    figure.add_trace(
        go.Scatter(
            x=geometry["target_x"],
            y=geometry["target_y"],
            mode="lines" if has_dimensions else "markers",
            fill="toself" if has_dimensions else None,
            fillcolor="rgba(245,158,11,.28)" if has_dimensions else None,
            line={"color": "#f59e0b", "width": 3},
            marker={"color": "#f59e0b", "size": 14, "symbol": "cross"},
            name=f"{target.name} · ingombro indicativo",
            hovertemplate=(
                f"{html.escape(target.name)} · offset %{{x:.1f}}′, %{{y:.1f}}′"
                "<extra></extra>"
            ),
        )
    )
    all_x = [*geometry["sensor_x"], *geometry["target_x"]]
    all_y = [*geometry["sensor_y"], *geometry["target_y"]]
    extent = max(max(abs(value) for value in [*all_x, *all_y]), 1) * 1.18
    figure.add_hline(y=0, line={"color": "rgba(148,163,184,.45)", "width": 1})
    figure.add_vline(x=0, line={"color": "rgba(148,163,184,.45)", "width": 1})
    figure.update_xaxes(
        title_text="Offset orizzontale · arcmin",
        range=[-extent, extent],
        scaleanchor="y",
        scaleratio=1,
    )
    figure.update_yaxes(title_text="Offset verticale · arcmin", range=[-extent, extent])
    figure.update_layout(
        height=520,
        hovermode="closest",
        margin={"l": 10, "r": 10, "t": 74, "b": 10},
        title={
            "text": f"Campo inquadrato · {target.name} · rotazione {rotation_deg:.0f}°",
            "x": 0,
        },
    )
    return _style_plotly(figure, dark_mode)


def _horizon_profile_figure(
    manual_mask: dict[float, float],
    terrain: TerrainHorizonEstimate,
    combined_mask: dict[float, float],
    dark_mode: bool,
) -> go.Figure:
    """Compare manual, DEM and effective obstruction masks without coordinates."""
    directions = np.asarray(sorted(terrain.mask), dtype=float)
    closed_directions = np.append(directions, directions[0])
    manual = horizon_altitudes(directions, manual_mask)
    profiles = (
        ("Manuale", manual, "#f59e0b", "dot"),
        (
            "Terreno Copernicus GLO-90",
            np.asarray([terrain.mask[float(value)] for value in directions]),
            "#0ea5e9",
            "dash",
        ),
        (
            "Usato dal planner · massimo dei due",
            np.asarray([combined_mask[float(value)] for value in directions]),
            "#7c3aed",
            "solid",
        ),
    )
    figure = go.Figure()
    for label, values, colour, dash in profiles:
        closed_values = np.append(values, values[0])
        figure.add_trace(
            go.Scatterpolar(
                theta=closed_directions,
                r=closed_values,
                name=label,
                mode="lines",
                line={"color": colour, "width": 3, "dash": dash},
                hovertemplate="Azimut %{theta:.0f}° · ostacolo %{r:.1f}°<extra>%{fullData.name}</extra>",
            )
        )
    maximum = max(
        10.0,
        float(
            np.nanmax(
                [
                    value
                    for _label, values, _colour, _dash in profiles
                    for value in values
                ]
            )
        )
        + 3,
    )
    figure.update_layout(
        height=470,
        margin={"l": 30, "r": 30, "t": 75, "b": 35},
        title={"text": "Profilo indicativo dell’orizzonte", "x": 0},
        polar={
            "angularaxis": {
                "direction": "clockwise",
                "rotation": 90,
                "tickmode": "array",
                "tickvals": list(range(0, 360, 45)),
                "ticktext": ["N", "NE", "E", "SE", "S", "SO", "O", "NO"],
            },
            "radialaxis": {"range": [0, min(60.0, maximum)], "ticksuffix": "°"},
        },
    )
    return _style_plotly(figure, dark_mode)


def _aladin_field_html(
    target: SkyTarget,
    profile: EquipmentProfile,
    rotation_deg: float,
    dark_mode: bool,
) -> str:
    """Build an isolated CDS Aladin view with only celestial coordinates."""
    view = field_of_view(profile)
    geometry = framing_geometry(target, profile, rotation_deg=rotation_deg)
    cosine_declination = max(abs(np.cos(np.deg2rad(target.dec_deg))), 0.01)
    corners = [
        [
            (target.ra_deg + x / (60 * cosine_declination)) % 360,
            float(np.clip(target.dec_deg + y / 60, -90, 90)),
        ]
        for x, y in zip(geometry["sensor_x"][:4], geometry["sensor_y"][:4], strict=True)
    ]
    corners.append(corners[0])
    polyline = ",".join(f"[{ra:.8f},{dec:.8f}]" for ra, dec in corners)
    target_ellipse = ""
    if (
        target.angular_width_arcmin is not None
        and target.angular_height_arcmin is not None
    ):
        target_ellipse = (
            "overlay.add(A.ellipse("
            f"{target.ra_deg:.8f},{target.dec_deg:.8f},"
            f"{target.angular_width_arcmin / 120:.8f},"
            f"{target.angular_height_arcmin / 120:.8f},0,"
            "{color:'#f59e0b',lineWidth:2}));"
        )
    background = "#05070b" if dark_mode else "#eef3f8"
    foreground = "#f8fafc" if dark_mode else "#10243d"
    field = max(view.width_deg, view.height_deg) * 1.35
    return f"""
<!doctype html>
<html lang="it">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,height=device-height,initial-scale=1">
  <style>
    html,body{{margin:0;background:{background};color:{foreground};font-family:system-ui,sans-serif}}
    #aladin-lite-div{{width:100%;height:500px;border-radius:14px;overflow:hidden}}
    .note{{padding:8px 4px 0;font-size:12px;line-height:1.45;color:{foreground}}}
  </style>
</head>
<body>
  <div id="aladin-lite-div" aria-label="Atlante celeste interattivo">
    <div style="padding:24px">Caricamento dell’atlante CDS…</div>
  </div>
  <div class="note">CDS Aladin Lite · DSS2 a colori · rettangolo azzurro: sensore · ellisse arancio: ingombro indicativo.</div>
  <script>
    const showAladinFallback = () => {{
      document.getElementById('aladin-lite-div').innerHTML =
        '<div style="padding:24px">Atlante CDS non disponibile in questo browser o rete. L’anteprima geometrica sopra resta valida.</div>';
    }};
    window.showAladinFallback = showAladinFallback;
  </script>
  <script>
    const initialiseAladin = () => {{
      if (typeof A === 'undefined' || !A.init) {{
        showAladinFallback();
        return;
      }}
      A.init.then(() => {{
        const aladin = A.aladin('#aladin-lite-div', {{
          survey:'P/DSS2/color', target:'{target.ra_deg:.8f} {target.dec_deg:+.8f}',
          fov:{field:.8f}, projection:'TAN', cooFrame:'ICRS', showReticle:true,
          showCooGrid:true, showCooGridControl:true, showGotoControl:false,
          showShareControl:false, showSimbadPointerControl:false, showContextMenu:false
        }});
        const overlay = A.graphicOverlay({{color:'#22d3ee',lineWidth:3}});
        aladin.addOverlay(overlay);
        overlay.add(A.polyline([{polyline}], {{color:'#22d3ee',lineWidth:3}}));
        {target_ellipse}
      }}).catch(showAladinFallback);
    }};
    const webgl2 = document.createElement('canvas').getContext('webgl2');
    if (!webgl2) {{
      showAladinFallback();
    }} else {{
      const aladinScript = document.createElement('script');
      aladinScript.src = 'https://aladin.cds.unistra.fr/AladinLite/api/v3/3.8.1/aladin.js';
      aladinScript.charset = 'utf-8';
      aladinScript.onerror = showAladinFallback;
      aladinScript.onload = initialiseAladin;
      document.head.appendChild(aladinScript);
    }}
  </script>
</body>
</html>
"""


def _night_plan_figure(
    tracks: pd.DataFrame,
    dark_mode: bool,
    minimum_altitude: float,
    secondary_metric: str = "Qualità",
) -> go.Figure:
    """Render ASIAIR-inspired altitude and quality tracks in explicit local time."""
    secondary_options = {
        "Qualità": ("planner_score", "Qualità della finestra", "Qualità /100"),
        "Massa d'aria": ("airmass", "Massa d'aria", "Massa d'aria"),
        "Distanza Luna": (
            "moon_separation",
            "Separazione angolare dalla Luna",
            "Distanza Luna °",
        ),
    }
    secondary_column, secondary_title, secondary_axis = secondary_options.get(
        secondary_metric, secondary_options["Qualità"]
    )
    secondary_format = ".2f" if secondary_column == "airmass" else ".0f"
    figure = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.12,
        row_heights=(0.64, 0.36),
        subplot_titles=("Altezza e orizzonte locale", secondary_title),
    )
    colors = ("#2563eb", "#f59e0b", "#8b5cf6", "#10b981", "#ef4444")

    def hover_value(value: Any, digits: int = 0, suffix: str = "") -> str:
        number = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
        return "n/d" if pd.isna(number) else f"{number:.{digits}f}{suffix}"

    for position, (target_name, group) in enumerate(
        tracks.groupby("target", sort=False)
    ):
        ordered = group.sort_values("valid_time")
        color = colors[position % len(colors)]
        x_values = plotly_local_datetimes(ordered["local_time"], CFG.local_timezone)
        custom_data = [
            [
                hover_value(row["magnitude"], 1),
                hover_value(row["azimuth"], 0, "°"),
                hover_value(row["airmass"], 2),
                hover_value(row["planner_score"], 0, "/100"),
                hover_value(row["clouds"], 0, "%"),
                hover_value(row["moon_separation"], 0, "°"),
                hover_value(row["horizon_altitude"], 0, "°"),
                "sì" if bool(row["weather_available"]) else "no",
            ]
            for _, row in ordered.iterrows()
        ]
        figure.add_trace(
            go.Scatter(
                x=x_values,
                y=ordered["altitude"],
                customdata=custom_data,
                name=str(target_name),
                mode="lines",
                line={"color": color, "width": 3},
                hovertemplate=(
                    "%{x|%d/%m · %H:%M}<br>"
                    "Altezza %{y:.1f}° · Azimut %{customdata[1]}<br>"
                    "Massa d'aria %{customdata[2]} · Magnitudine %{customdata[0]}<br>"
                    "Qualità %{customdata[3]} · Nuvole %{customdata[4]}<br>"
                    "Luna %{customdata[5]} · Orizzonte %{customdata[6]}<br>"
                    "Meteo coperto: %{customdata[7]}<extra>%{fullData.name}</extra>"
                ),
            ),
            row=1,
            col=1,
        )
        figure.add_trace(
            go.Scatter(
                x=x_values,
                y=ordered["horizon_altitude"],
                name=f"Orizzonte · {target_name}",
                mode="lines",
                line={"color": color, "width": 1.3, "dash": "dot"},
                opacity=0.52,
                showlegend=False,
                hovertemplate="Orizzonte locale %{y:.1f}°<extra></extra>",
            ),
            row=1,
            col=1,
        )
        figure.add_trace(
            go.Scatter(
                x=x_values,
                y=ordered[secondary_column],
                name=f"{secondary_metric} · {target_name}",
                mode="lines",
                line={"color": color, "width": 2.4},
                showlegend=False,
                customdata=custom_data,
                hovertemplate=(
                    f"%{{x|%d/%m · %H:%M}}<br>{secondary_metric} "
                    f"%{{y:{secondary_format}}}<br>"
                    "Nuvole %{customdata[4]} · Luna %{customdata[5]}"
                    "<extra>%{fullData.name}</extra>"
                ),
            ),
            row=2,
            col=1,
        )
    figure.add_hline(
        y=float(minimum_altitude),
        line={"color": "#64748b", "width": 1.5, "dash": "dash"},
        row=1,
        col=1,
    )
    now_local = pd.Timestamp.now(tz=CFG.local_timezone)
    start = pd.to_datetime(tracks["local_time"], errors="coerce").min()
    end = pd.to_datetime(tracks["local_time"], errors="coerce").max()
    if pd.notna(start) and start <= now_local <= end:
        marker = plotly_local_datetime(now_local, CFG.local_timezone)
        for row in (1, 2):
            figure.add_vline(
                x=marker,
                line={"color": "#ef4444", "width": 2, "dash": "dash"},
                row=row,
                col=1,
            )
    figure.update_yaxes(title_text="Altezza °", range=[-5, 95], row=1, col=1)
    if secondary_column == "planner_score":
        figure.update_yaxes(title_text=secondary_axis, range=[0, 105], row=2, col=1)
    elif secondary_column == "moon_separation":
        figure.update_yaxes(title_text=secondary_axis, range=[0, 180], row=2, col=1)
    else:
        finite_airmass = pd.to_numeric(tracks["airmass"], errors="coerce").dropna()
        upper_airmass = (
            min(6.0, max(3.0, float(finite_airmass.max())))
            if not finite_airmass.empty
            else 3.0
        )
        figure.update_yaxes(
            title_text=secondary_axis,
            range=[upper_airmass, 1],
            row=2,
            col=1,
        )
    figure.update_xaxes(
        title_text=f"Ora locale · {CFG.local_timezone}",
        tickformat="%H:%M",
        row=2,
        col=1,
    )
    figure.update_layout(
        height=680,
        hovermode="x unified",
        margin={"l": 10, "r": 10, "t": 78, "b": 10},
        title={"text": "Andamento dei soggetti nella notte", "x": 0},
    )
    return _style_plotly(figure, dark_mode)


def _use_local_subplot_keys(figure: go.Figure) -> go.Figure:
    """Keep subplot titles concise and preserve Plotly's colour-accurate legend."""
    for annotation in figure.layout.annotations or ():
        annotation.update(
            x=0,
            xanchor="left",
            align="left",
            font={"size": 12},
        )
    figure.update_layout(showlegend=True)
    return figure


def _station_label(profiles: pd.DataFrame, station_id: str) -> str:
    if profiles.empty:
        return station_id
    selected = profiles[profiles["station_id"].astype(str).eq(str(station_id))]
    return (
        station_id
        if selected.empty
        else str(selected.iloc[0].get("display_name") or station_id)
    )


def _render_daily_station_metric(
    daily: pd.DataFrame,
    station_ids: tuple[str, str],
    profiles: pd.DataFrame,
    metric: str,
    title: str,
    unit: str,
    dark_mode: bool,
    *,
    bars: bool = False,
) -> None:
    colours = ("#2563eb", "#f97316")
    figure = go.Figure()
    for station_id, colour in zip(station_ids, colours):
        selected = daily[daily["station_id"].astype(str).eq(station_id)].sort_values(
            "local_date"
        )
        if selected.empty or metric not in selected:
            continue
        values = pd.to_numeric(selected[metric], errors="coerce")
        label = _station_label(profiles, station_id)
        if bars:
            figure.add_trace(
                go.Bar(
                    x=selected["local_date"],
                    y=values,
                    name=label,
                    marker_color=colour,
                    opacity=0.82,
                    hovertemplate=f"%{{x|%d/%m/%Y}}<br>%{{y:.1f}} {unit}<extra>{html.escape(label)}</extra>",
                )
            )
        else:
            figure.add_trace(
                go.Scatter(
                    x=selected["local_date"],
                    y=values,
                    name=label,
                    mode="lines+markers",
                    line={"color": colour, "width": 2.6},
                    marker={"color": colour, "size": 4},
                    connectgaps=False,
                    hovertemplate=f"%{{x|%d/%m/%Y}}<br>%{{y:.1f}} {unit}<extra>{html.escape(label)}</extra>",
                )
            )
    figure.update_layout(
        height=310,
        title={"text": title, "x": 0},
        hovermode="x unified",
        barmode="group",
        margin={"l": 10, "r": 10, "t": 78, "b": 10},
        legend={
            "orientation": "h",
            "x": 0,
            "y": 1.02,
            "xanchor": "left",
            "yanchor": "bottom",
        },
    )
    figure.update_yaxes(title_text=unit, rangemode="nonnegative" if bars else "normal")
    st.plotly_chart(_style_plotly(figure, dark_mode), width="stretch", theme=None)


def render_station_comparison(
    daily: pd.DataFrame,
    profiles: pd.DataFrame,
    primary_station_id: str,
    dark_mode: bool,
) -> None:
    if daily.empty or profiles.empty:
        st.info("Il confronto comparirà dopo l'importazione dello storico secondario.")
        return
    available = [
        identifier
        for identifier in profiles["station_id"].astype(str).tolist()
        if identifier in set(daily["station_id"].astype(str))
    ]
    if primary_station_id not in available or len(available) < 2:
        st.info("Servono almeno due stazioni con riepiloghi giornalieri confrontabili.")
        return
    secondary_options = [
        identifier for identifier in available if identifier != primary_station_id
    ]
    controls = st.columns([1.2, 1])
    secondary_id = controls[0].selectbox(
        "Confronta Roma con",
        options=secondary_options,
        format_func=lambda value: _station_label(profiles, value),
        key="station_comparison_secondary",
    )
    window_days = controls[1].selectbox(
        "Periodo del confronto",
        options=(30, 90, 180, 365),
        index=1,
        format_func=lambda value: f"Ultimi {value} giorni disponibili",
        key="station_comparison_days",
    )
    selected_ids = (primary_station_id, secondary_id)
    selected = daily[daily["station_id"].astype(str).isin(selected_ids)].copy()
    selected["local_date"] = pd.to_datetime(selected["local_date"], errors="coerce")
    selected = selected.dropna(subset=["local_date"])
    if selected.empty:
        st.info("Nessun giorno confrontabile nel periodo scelto.")
        return
    end = selected["local_date"].max()
    selected = selected[
        selected["local_date"] >= end - pd.Timedelta(days=window_days - 1)
    ]

    def overlap(metric: str) -> pd.DataFrame:
        pivot = selected.pivot_table(
            index="local_date", columns="station_id", values=metric, aggfunc="last"
        )
        return pivot.dropna(subset=list(selected_ids)) if not pivot.empty else pivot

    temperature = overlap("temp_mean_c")
    humidity = overlap("humidity_mean")
    rain = overlap("rain_mm")
    metric_columns = st.columns(4)
    metric_columns[0].metric(
        "Giorni sovrapposti",
        str(len(temperature)),
        "solo date presenti in entrambe",
        delta_color="off",
    )
    temperature_delta = (
        (temperature[secondary_id] - temperature[primary_station_id]).mean()
        if not temperature.empty
        else np.nan
    )
    metric_columns[1].metric(
        "Δ temperatura media",
        _number(temperature_delta, 1, " °C"),
        f"{_station_label(profiles, secondary_id)} − Roma",
        delta_color="off",
    )
    humidity_delta = (
        (humidity[secondary_id] - humidity[primary_station_id]).mean()
        if not humidity.empty
        else np.nan
    )
    metric_columns[2].metric(
        "Δ umidità media",
        _number(humidity_delta, 1, " punti"),
        f"{_station_label(profiles, secondary_id)} − Roma",
        delta_color="off",
    )
    rain_totals = rain[list(selected_ids)].sum() if not rain.empty else pd.Series()
    metric_columns[3].metric(
        "Pioggia su date comuni",
        (
            f"{rain_totals.get(primary_station_id, 0):.1f} / "
            f"{rain_totals.get(secondary_id, 0):.1f} mm"
        ),
        "Roma / seconda stazione",
        delta_color="off",
    )
    st.caption(
        "Confronto descrittivo su giorni omogenei: Comacchio resta una stazione "
        "indipendente e non applica correzioni automatiche alla previsione calibrata di Roma."
    )
    _render_daily_station_metric(
        selected,
        selected_ids,
        profiles,
        "temp_mean_c",
        "Temperatura media giornaliera",
        "°C",
        dark_mode,
    )
    _render_daily_station_metric(
        selected,
        selected_ids,
        profiles,
        "humidity_mean",
        "Umidità media giornaliera",
        "%",
        dark_mode,
    )
    _render_daily_station_metric(
        selected,
        selected_ids,
        profiles,
        "rain_mm",
        "Pioggia giornaliera",
        "mm",
        dark_mode,
        bars=True,
    )
    _render_daily_station_metric(
        selected,
        selected_ids,
        profiles,
        "pressure_mean_hpa",
        "Pressione media giornaliera",
        "hPa",
        dark_mode,
    )
    secondary_quality = selected[
        selected["station_id"].astype(str).eq(secondary_id)
    ].get("data_quality", pd.Series(dtype="object"))
    if (
        secondary_quality.astype(str)
        .str.contains("pressure_calibration_review", regex=False)
        .any()
    ):
        st.warning(
            "Una parte dello storico della seconda stazione presenta uno scarto anomalo "
            "tra pressione relativa e assoluta: il grafico resta consultabile, ma la "
            "pressione non verrà usata per calibrare alcuna previsione finché la console "
            "non sarà verificata."
        )


def _base_table_style(frame: pd.DataFrame, dark_mode: bool) -> Any:
    background = "#0b111b" if dark_mode else "#ffffff"
    foreground = "#e5edf7" if dark_mode else "#10243d"
    header = "#162235" if dark_mode else "#e8f1f8"
    return (
        frame.style.hide(axis="index")
        .set_properties(**{"background-color": background, "color": foreground})
        .set_table_styles(
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
    )


def _style_hourly_table(table: pd.DataFrame, dark_mode: bool) -> Any:
    styler = _base_table_style(table, dark_mode)
    metric_columns = {
        "temperature": ["Temp °C", "Percepita °C", "Rugiada °C"],
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
        "Rugiada °C": "{:.1f}",
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
                if str(value).lower()
                in {
                    "ok",
                    "success",
                    "online",
                    "operativa",
                    "operativo",
                    "riuscita",
                    "attiva",
                    "regolare",
                    "ottima",
                    "favorevole",
                }
                else weather_cell_style(20, "confidence")
                if str(value).lower()
                in {
                    "error",
                    "failed",
                    "failure",
                    "offline",
                    "non disponibile",
                    "non disponibili",
                    "critica",
                    "problema",
                    "troppo basso",
                }
                else weather_cell_style(55, "confidence")
            ),
            subset=[status_column],
        )
    for text_column in ("Continuità / fallback", "Ultimo errore", "Motivo"):
        if text_column in table:
            styler = styler.set_properties(
                subset=[text_column],
                **{
                    "white-space": "normal",
                    "min-width": "220px",
                    "max-width": "380px",
                    "overflow-wrap": "anywhere",
                },
            )
    return styler


def _style_score_table(table: pd.DataFrame, dark_mode: bool) -> Any:
    """Colour forecast errors relative to the values currently being compared."""
    styler = _base_table_style(table, dark_mode)
    for column in (
        "Bias",
        "MAE",
        "RMSE",
        "Brier",
        "MAE validazione",
        "MAE persistenza",
        "Gap affidabilità %",
    ):
        if column not in table:
            continue
        values = pd.to_numeric(table[column], errors="coerce").abs().dropna()
        if values.empty:
            continue
        median = float(values.quantile(0.5))
        high = float(values.quantile(0.8))

        def relative_style(
            value: Any, median: float = median, high: float = high
        ) -> str:
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
    for column in ("Skill vs persistenza %", "Correlazione sito %"):
        if column in table:
            styler = styler.map(
                lambda value: weather_cell_style(value, "confidence"),
                subset=[column],
            )
    return styler.format(precision=2, na_rep="—")


def _style_astronomy_table(table: pd.DataFrame, dark_mode: bool) -> Any:
    styler = _base_table_style(table, dark_mode)
    metrics = {
        "SQM stimato": "sqm",
        "Bortle ≈": "bortle",
        "Qualità media": "confidence",
        "Qualità migliore": "confidence",
        "Nuvole notte %": "clouds",
        "Vento notte km/h": "wind",
    }
    for column, metric in metrics.items():
        if column in table:
            styler = styler.map(
                lambda value, metric=metric: weather_cell_style(value, metric),
                subset=[column],
            )
    formats = {
        "SQM stimato": "{:.2f}",
        "Qualità media": "{:.0f}",
        "Qualità migliore": "{:.0f}",
        "Nuvole notte %": "{:.0f}",
        "Vento notte km/h": "{:.1f}",
        "Luna illuminata %": "{:.0f}",
    }
    return styler.format(
        {column: pattern for column, pattern in formats.items() if column in table},
        na_rep="—",
    )


def _style_city_daily_table(table: pd.DataFrame, dark_mode: bool) -> Any:
    styler = _base_table_style(table, dark_mode)
    metrics = {
        "Min °C": "temperature",
        "Max °C": "temperature",
        "Pioggia mm": "rain",
        "Prob. %": "rain_probability",
        "Vento max km/h": "wind",
        "Raffiche km/h": "gust",
    }
    for column, metric in metrics.items():
        if column in table:
            styler = styler.map(
                lambda value, metric=metric: weather_cell_style(value, metric),
                subset=[column],
            )
    formats = {
        "Min °C": "{:.1f}",
        "Max °C": "{:.1f}",
        "Pioggia mm": "{:.1f}",
        "Prob. %": "{:.0f}",
        "Vento max km/h": "{:.1f}",
        "Raffiche km/h": "{:.1f}",
        "UV max": "{:.1f}",
    }
    return styler.format(
        {column: pattern for column, pattern in formats.items() if column in table},
        na_rep="—",
    )


def _style_air_table(table: pd.DataFrame, dark_mode: bool) -> Any:
    styler = _base_table_style(table, dark_mode)
    if "AQI europeo" in table:
        styler = styler.map(
            lambda value: weather_cell_style(value, "aqi"),
            subset=["AQI europeo"],
        )
    if "Pollini max" in table:
        styler = styler.map(
            lambda value: weather_cell_style(value, "pollen"),
            subset=["Pollini max"],
        )
    formats = {
        "AQI europeo": "{:.0f}",
        "PM2.5 µg/m³": "{:.1f}",
        "PM10 µg/m³": "{:.1f}",
        "NO₂ µg/m³": "{:.1f}",
        "O₃ µg/m³": "{:.1f}",
        "UV": "{:.1f}",
        "Pollini max": "{:.1f}",
    }
    return styler.format(
        {column: pattern for column, pattern in formats.items() if column in table},
        na_rep="—",
    )


def render_styled_table(
    styler: Any,
    *,
    height: int | None = None,
    container: Any = st,
) -> None:
    """Render a responsive semantic table with a theme-aware sticky header."""
    max_height = f"max-height:{max(180, int(height))}px;" if height else ""
    table_html = styler.to_html(table_attributes='class="weather-table"')
    container.markdown(
        f'<div class="weather-table-wrap" style="{max_height}">{table_html}</div>',
        unsafe_allow_html=True,
    )


def interval_selector(label: str, key: str, default: int = 3) -> int:
    return int(
        st.radio(
            label,
            options=(1, 3, 6),
            index=(1, 3, 6).index(default),
            horizontal=True,
            format_func=lambda value: "Ogni ora" if value == 1 else f"Ogni {value} ore",
            key=key,
        )
    )


def render_color_legend(kind: str = "weather") -> None:
    """Render a compact, theme-aware explanation of semantic table colours."""
    if kind == "scores":
        items = (
            ("green", "Errore minore"),
            ("yellow", "Errore intermedio"),
            ("red", "Errore maggiore"),
        )
        note = (
            "Confronto relativo tra i provider e gli orizzonti presenti nella tabella."
        )
    elif kind == "status":
        items = (
            ("green", "Esecuzione regolare"),
            ("yellow", "Da controllare / archivio / fonte esterna"),
            ("red", "Errore della pipeline o servizio primario offline"),
        )
        note = "La colorazione aiuta a individuare rapidamente anomalie nella pipeline."
    elif kind == "astronomy":
        items = (
            ("green", "Cielo più scuro / condizioni buone"),
            ("yellow", "Condizioni intermedie"),
            ("orange", "Inquinamento o meteo sfavorevole"),
            ("red", "Cielo urbano / condizioni critiche"),
        )
        note = (
            "Per SQM un numero più alto indica un cielo più scuro; per Bortle è "
            "l'opposto: 1 è il cielo migliore, 9 il più luminoso."
        )
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


def _live_badge_html(status: str, age_minutes: Any = None) -> str:
    is_live = str(status).lower() == "online"
    css_class = "is-live" if is_live else "is-stale"
    label = "LIVE" if is_live else "NON LIVE"
    age = _age_text(age_minutes)
    title = (
        f"Dato aggiornato: {age}"
        if is_live
        else f"Dato non aggiornato nei tempi attesi: {age}"
    )
    return (
        f'<span class="live-badge {css_class}" title="{html.escape(title)}" '
        f'aria-label="{html.escape(label + ": " + title)}">'
        f'<span class="live-dot"></span>{label}</span>'
    )


def _timestamp_live_state(value: Any, *, max_age_minutes: int) -> tuple[str, float]:
    timestamp = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(timestamp):
        return "offline", float("inf")
    age = max(
        0.0,
        (pd.Timestamp.now(tz="UTC") - timestamp).total_seconds() / 60.0,
    )
    return ("online" if age <= max_age_minutes else "offline"), age


def _measurement_freshness_row(health: dict[str, Any]) -> str:
    labels = {
        "temperature": "Temperatura",
        "humidity": "Umidità",
        "pressure": "Pressione",
        "wind": "Vento",
        "rain": "Pioggia",
        "solar": "Solare/UV",
    }
    freshness = health.get("measurement_freshness") or {}
    chips = []
    for key, label in labels.items():
        details = freshness.get(key) or {}
        status = str(details.get("status") or "offline")
        chips.append(
            f'<span class="freshness-chip {html.escape(status)}">'
            f'<span class="dot"></span>{html.escape(label)}: '
            f"{html.escape(_age_text(details.get('age_minutes')))}</span>"
        )
    return (
        '<div class="freshness-row"><span class="freshness-label">Ultime misure reali</span>'
        + "".join(chips)
        + "</div>"
    )


def _stale_measurement_labels(health: dict[str, Any], minutes: int = 30) -> list[str]:
    labels = {
        "temperature": "temperatura",
        "humidity": "umidità",
        "pressure": "pressione",
        "wind": "vento",
        "rain": "pioggia",
        "solar": "solare/UV",
    }
    freshness = health.get("measurement_freshness") or {}
    stale = []
    for key, label in labels.items():
        value = pd.to_numeric(
            pd.Series([(freshness.get(key) or {}).get("age_minutes")]),
            errors="coerce",
        ).iloc[0]
        if pd.isna(value) or not np.isfinite(value) or value > minutes:
            stale.append(label)
    return stale


def render_daily_cards(daily: pd.DataFrame) -> None:
    if daily.empty:
        st.info(
            "La previsione giornaliera comparirà dopo il primo aggiornamento della pipeline V4."
        )
        return
    cards = []
    for _, row in daily.head(7).iterrows():
        day = pd.Timestamp(row["date"])
        description = str(row.get("description") or "Variabile")
        card_label = f"{ITALIAN_WEEKDAYS[day.weekday()]} {day.day}: {description}"
        cards.append(
            '<details class="day-card expandable-card">'
            f'<summary aria-label="Apri dettagli {html.escape(card_label)}">'
            f'<div class="day-name">{ITALIAN_WEEKDAYS[day.weekday()]} {day.day}</div>'
            f'<div class="day-icon">{_weather_icon(description)}</div>'
            f'<div class="day-temp">{_number(row.get("temp_min"), 0, "°")} / {_number(row.get("temp_max"), 0, "°")}</div>'
            f'<div class="day-desc">{html.escape(description)}</div>'
            '<div class="expand-hint"><span class="closed-label">Altri dettagli</span><span class="open-label">Riduci</span></div>'
            '</summary><div class="card-expanded">'
            f"☔ Pioggia {_number(row.get('rain_mm'), 1, ' mm')} · rischio {_number(row.get('pop_max'), 0, '%')}"
            f"<br>💧 umidità media {_number(row.get('humidity_mean'), 0, '%')}"
            f"<br>💨 vento {_number(row.get('wind_mean'), 0, ' km/h')} · raffiche {_number(row.get('wind_max'), 0, ' km/h')}"
            f"<br>◎ fiducia del blend {_number(row.get('confidence'), 0, '%')}"
            "</div></details>"
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
    next_hours = (
        forecast[forecast["valid_time"] >= now].sort_values("valid_time").head(3)
    )
    if next_hours.empty:
        st.info("Nessun punto di previsione futuro disponibile.")
        return
    cards = []
    for position, (_, row) in enumerate(next_hours.iterrows(), start=1):
        local_time = pd.Timestamp(row["valid_time"]).tz_convert(CFG.local_timezone)
        description = str(row.get("description") or "Variabile")
        cards.append(
            '<details class="hour-card expandable-card">'
            f'<summary aria-label="Apri dettagli previsione delle {local_time:%H:%M}">'
            f'<div class="hour-title">+{position} h · {local_time:%H:%M}</div>'
            f'<div class="hour-weather">{_weather_icon(description)} {html.escape(description)}</div>'
            f'<div class="hour-meta">🌡️ {_number(row.get("temp_c"), 1, " °C")} · percepita {_number(row.get("feels_like_c"), 1, " °C")}</div>'
            '<div class="expand-hint"><span class="closed-label">Altri dettagli</span><span class="open-label">Riduci</span></div>'
            '</summary><div class="card-expanded">'
            f"◉ Punto di rugiada {_number(row.get('dewpoint_c'), 1, ' °C')}"
            f"<br>☁️ Nuvole {_number(row.get('clouds'), 0, '%')}"
            f"<br>☔ {_number(reportable_rain_amount(row.get('rain_mm'), row.get('precip_probability')), 1, ' mm')} · rischio {_number(row.get('precip_probability'), 0, '%')}"
            f"<br>💨 {_number(row.get('wind_kmh'), 0, ' km/h')} · {html.escape(compass_direction(row.get('wind_dir')))}"
            f"<br>◎ Fiducia {_number(row.get('confidence'), 0, '%')} · fonte {html.escape(str(row.get('model') or 'blend calibrato'))}"
            "</div></details>"
        )
    st.markdown(
        '<div class="hour-grid">' + "".join(cards) + "</div>",
        unsafe_allow_html=True,
    )


def _hero_variant(description: str, is_day: Any) -> str:
    text = description.casefold()
    daylight = pd.to_numeric(pd.Series([is_day]), errors="coerce").iloc[0]
    if pd.notna(daylight) and float(daylight) < 0.5:
        return "hero-night"
    if any(word in text for word in ("piogg", "tempor", "rovesc", "neve")):
        return "hero-rain"
    if any(word in text for word in ("nuvol", "coperto", "nebb", "foschia")):
        return "hero-cloud"
    return "hero-clear"


def render_current_grid(
    cards: list[tuple[str, ...]],
) -> None:
    """Render responsive, keyboard-accessible current-condition detail cards."""
    rendered = []
    for card in cards:
        icon, label, value, detail = card[:4]
        expanded = (
            card[4] if len(card) > 4 else "Misura locale con contesto e provenienza."
        )
        value_text = str(value or "—")
        detail_text = str(detail or "Nessun confronto recente")
        expanded_text = str(expanded or "Dettagli non disponibili")
        live_status = card[5] if len(card) > 5 else None
        live_age = card[6] if len(card) > 6 else None
        live_badge = (
            _live_badge_html(str(live_status), live_age)
            if live_status is not None
            else ""
        )
        rendered.append(
            '<details class="current-card expandable-card">'
            f'<summary aria-label="Apri dettagli {html.escape(label)}">'
            '<div class="current-head">'
            f'<div class="current-label">{icon} {html.escape(label)}</div>'
            f"{live_badge}</div>"
            f'<div class="current-value" title="{html.escape(value_text)}">{html.escape(value_text)}</div>'
            f'<div class="current-detail" title="{html.escape(detail_text)}">{html.escape(detail_text)}</div>'
            '<div class="expand-hint"><span class="closed-label">Altri dettagli</span><span class="open-label">Riduci</span></div>'
            f'</summary><div class="card-expanded">{html.escape(expanded_text)}</div></details>'
        )
    st.markdown(
        '<div class="current-grid">' + "".join(rendered) + "</div>",
        unsafe_allow_html=True,
    )


def render_v4_hero(
    station: pd.DataFrame,
    forecast: pd.DataFrame,
    health: dict[str, Any],
) -> None:
    """Render a daily-first, MSN-like summary without hiding data provenance."""
    point = nearest_forecast(forecast)
    forecast_row = point if point is not None else pd.Series(dtype="object")
    station_live = health.get("station_status") in {"online", "delayed"}
    measured = station.iloc[-1] if station_live and not station.empty else None
    current_temp = (
        measured.get("temp_c") if measured is not None else forecast_row.get("temp_c")
    )
    description = str(forecast_row.get("description") or "Condizioni locali")
    icon = _weather_icon(description)
    is_day = forecast_row.get("is_day")
    if pd.isna(pd.to_numeric(pd.Series([is_day]), errors="coerce").iloc[0]):
        local_hour = pd.Timestamp.now(tz="UTC").tz_convert(CFG.local_timezone).hour
        is_day = 1 if 7 <= local_hour < 20 else 0
    briefing = build_daily_briefing(forecast)
    daily = daily_forecast(forecast, CFG.local_timezone)
    today = daily.iloc[0] if not daily.empty else pd.Series(dtype="object")
    source = "misura Ecowitt" if measured is not None else "previsione combinata"
    updated = _local_time(
        health.get("station_time")
        if measured is not None
        else health.get("forecast_issued"),
        "%H:%M",
    )
    confidence = "—" if briefing.confidence is None else f"{briefing.confidence:.0f}%"
    temperature = _number(current_temp, 1, "°")
    perceived_value = (
        measured.get("feels_like_c")
        if measured is not None
        else forecast_row.get("feels_like_c")
    )
    dewpoint_value = (
        measured.get("dewpoint_c")
        if measured is not None
        else forecast_row.get("dewpoint_c")
    )
    perceived = _number(perceived_value, 1, "° percepiti")
    dewpoint = _number(dewpoint_value, 1, "° rugiada")
    minimum = _number(today.get("temp_min"), 0, "°")
    maximum = _number(today.get("temp_max"), 0, "°")
    variant = _hero_variant(description, is_day)
    st.markdown(
        f'<div class="hero-v4 {variant}">'
        '<div class="hero-primary">'
        '<div class="eyebrow">Meteo V4 · adesso e prossime ore</div>'
        f'<div class="hero-place">{html.escape(CFG.location_name)}</div>'
        '<div class="hero-condition">'
        f'<div class="hero-weather-icon">{icon}</div>'
        f'<div class="hero-temperature">{temperature}</div>'
        '<div><div class="hero-description">'
        f'{html.escape(description)}</div><div class="hero-secondary">'
        f"{html.escape(perceived)} · {html.escape(dewpoint)} · min {html.escape(minimum)} · max {html.escape(maximum)}"
        "</div></div></div>"
        f'<div class="hero-secondary">Aggiornato alle {html.escape(updated)} · {html.escape(source)} · orari {html.escape(CFG.local_timezone)}</div>'
        "</div>"
        '<div class="hero-brief"><div class="eyebrow">In breve</div>'
        f"<strong>{html.escape(briefing.headline)}</strong>"
        f"<small>{html.escape(briefing.detail)}</small>"
        f'<small style="margin-top:.55rem">Fiducia media {html.escape(confidence)}</small>'
        "</div></div>",
        unsafe_allow_html=True,
    )


def render_v4_hourly_strip(
    forecast: pd.DataFrame,
    limit: int = 12,
    *,
    timezone: str | None = None,
) -> None:
    if forecast.empty:
        st.info("La timeline oraria comparirà al prossimo aggiornamento.")
        return
    now = pd.Timestamp.now(tz="UTC")
    upcoming = (
        forecast[
            pd.to_datetime(forecast["valid_time"], utc=True, errors="coerce")
            >= now.floor("h")
        ]
        .sort_values("valid_time")
        .head(limit)
    )
    if upcoming.empty:
        st.info("Nessuna ora futura disponibile.")
        return
    tiles = []
    for position, (_, row) in enumerate(upcoming.iterrows()):
        moment = pd.Timestamp(row["valid_time"]).tz_convert(
            timezone or CFG.local_timezone
        )
        description = str(row.get("description") or "Variabile")
        time_label = "Adesso" if position == 0 else moment.strftime("%H:%M")
        accessible_time = "adesso" if position == 0 else f"delle {moment:%H:%M}"
        tiles.append(
            f'<details class="hourly-tile expandable-card{" is-now" if position == 0 else ""}">'
            f'<summary aria-label="Apri dettagli previsione {accessible_time}">'
            f'<div class="hourly-time">{time_label}</div>'
            f'<div class="hourly-icon">{_weather_icon(description)}</div>'
            f'<div class="hourly-temp">{_number(row.get("temp_c"), 0, "°")}</div>'
            f'<div class="hourly-rain">☔ {_number(row.get("precip_probability"), 0, "%")} · {_number(reportable_rain_amount(row.get("rain_mm"), row.get("precip_probability")), 1, " mm")}</div>'
            '<div class="expand-hint"><span class="closed-label">Dettagli</span><span class="open-label">Riduci</span></div>'
            '</summary><div class="card-expanded">'
            f"Condizione: {html.escape(description)}"
            f"<br>🌡️ Percepita {_number(row.get('feels_like_c'), 1, ' °C')} · rugiada {_number(row.get('dewpoint_c'), 1, ' °C')}"
            f"<br>💧 Umidità {_number(row.get('humidity'), 0, '%')}"
            f"<br>☁️ Nuvole {_number(row.get('clouds'), 0, '%')}"
            f"<br>💨 Vento {_number(row.get('wind_kmh'), 0, ' km/h')} · {html.escape(compass_direction(row.get('wind_dir')))}"
            f"<br>◎ Fiducia {_number(row.get('confidence'), 0, '%')} · {html.escape(str(row.get('model') or 'blend calibrato'))}"
            "</div></details>"
        )
    st.markdown(
        '<div class="hourly-strip">' + "".join(tiles) + "</div>",
        unsafe_allow_html=True,
    )


def render_v4_insights(forecast: pd.DataFrame, *, timezone: str | None = None) -> None:
    insights = weather_insights(forecast, timezone=timezone or CFG.local_timezone)
    if not insights:
        return
    cards = []
    for item in insights:
        cards.append(
            f'<details class="insight-card expandable-card tone-{html.escape(item.tone)}">'
            f'<summary aria-label="Apri dettagli {html.escape(item.title)}">'
            f'<div class="insight-title">{item.icon} {html.escape(item.title)}</div>'
            f'<div class="insight-value">{html.escape(item.value)}</div>'
            '<div class="expand-hint"><span class="closed-label">Perché</span><span class="open-label">Riduci</span></div>'
            f'</summary><div class="card-expanded">{html.escape(item.detail)}'
            "<br>Indicazione calcolata dalle prossime ore del blend calibrato.</div></details>"
        )
    st.markdown(
        '<div class="insight-grid">' + "".join(cards) + "</div>",
        unsafe_allow_html=True,
    )


def _planner_card(
    *,
    icon: str,
    title: str,
    value: str,
    label: str,
    timing: str,
    detail: str,
    tone: str,
) -> str:
    return (
        f'<details class="activity-card expandable-card tone-{html.escape(tone)}">'
        f'<summary aria-label="Apri dettagli {html.escape(title)}">'
        '<div class="activity-top">'
        f'<div><div class="activity-icon">{icon}</div><div class="activity-title">{html.escape(title)}</div></div>'
        f'<div class="activity-score">{html.escape(value)}</div></div>'
        f'<div class="activity-label">{html.escape(label)}</div>'
        '<div class="expand-hint"><span class="closed-label">Orario e motivazione</span><span class="open-label">Riduci</span></div>'
        f'</summary><div class="card-expanded"><div class="activity-time">{html.escape(timing)}</div>'
        f'<div class="activity-detail">{html.escape(detail)}</div>'
        '<div class="activity-detail" style="margin-top:.45rem">Valutazione orientativa: apri la scheda dedicata per i dati completi.</div>'
        "</div></details>"
    )


def _pollen_planner_card(air: AirQualityForecast | None) -> str:
    if air is None or air.hourly.empty:
        return _planner_card(
            icon="🌿",
            title="Pollini",
            value="—",
            label="In aggiornamento",
            timing="Previsione temporaneamente non disponibile",
            detail="Il meteo principale continua normalmente.",
            tone="neutral",
        )
    now = pd.Timestamp.now(tz=air.timezone)
    upcoming = air.hourly[
        (air.hourly["time"] >= now.floor("h"))
        & (air.hourly["time"] <= now + pd.Timedelta(hours=24))
    ].copy()
    available = [column for column in POLLEN_LABELS if column in upcoming]
    if upcoming.empty or not available:
        return _pollen_planner_card(None)
    matrix = upcoming[available].apply(pd.to_numeric, errors="coerce")
    stacked = matrix.stack(future_stack=True).dropna()
    if stacked.empty:
        return _pollen_planner_card(None)
    row_index, pollen_column = stacked.idxmax()
    value = float(stacked.loc[(row_index, pollen_column)])
    category, tone = pollen_category(value)
    moment = pd.Timestamp(upcoming.loc[row_index, "time"])
    return _planner_card(
        icon="🌿",
        title="Pollini",
        value=category,
        label=f"{POLLEN_LABELS[pollen_column]} · {value:.1f} grani/m³",
        timing=f"Picco previsto: {_hour_label(moment)}",
        detail="Previsione CAMS orientativa, non sensore locale.",
        tone=tone,
    )


def _ventilation_planner_card(
    forecast: pd.DataFrame,
    air: AirQualityForecast | None,
    *,
    timezone: str,
) -> str:
    window = best_ventilation_window(
        forecast,
        None if air is None else air.hourly,
        timezone=timezone,
    )
    return _planner_card(
        icon="🪟",
        title="Arieggiare casa",
        value="—" if window.score is None else f"{window.score}/100",
        label=window.label,
        timing=window.timing,
        detail=window.detail,
        tone=window.tone,
    )


def _moon_planner_card(settings: Settings) -> str:
    events = astronomy_events(settings, days=2)
    if events.empty:
        return _planner_card(
            icon="🌙",
            title="Luna e cielo",
            value="—",
            label="Effemeridi in aggiornamento",
            timing="Consulta la scheda Astronomia",
            detail="Il punteggio astronomico meteo resta disponibile.",
            tone="neutral",
        )
    today = pd.Timestamp.now(tz=settings.local_timezone).date()
    candidates = events[events["date"] == today]
    event = candidates.iloc[0] if not candidates.empty else events.iloc[0]
    illumination = pd.to_numeric(
        pd.Series([event.get("moon_illumination")]), errors="coerce"
    ).iloc[0]
    if pd.isna(illumination):
        phase_label, tone, value = "Illuminazione non disponibile", "neutral", "—"
    elif illumination < 15:
        phase_label, tone, value = "Cielo più buio", "good", f"{illumination:.0f}%"
    elif illumination < 65:
        phase_label, tone, value = "Fase intermedia", "warning", f"{illumination:.0f}%"
    else:
        phase_label, tone, value = "Luna luminosa", "warning", f"{illumination:.0f}%"

    def event_time(value: Any) -> str:
        return (
            "—"
            if value is None or pd.isna(value)
            else pd.Timestamp(value).strftime("%H:%M")
        )

    return _planner_card(
        icon="🌙",
        title="Luna e cielo",
        value=value,
        label=phase_label,
        timing=f"Sorge {event_time(event.get('moonrise'))} · tramonta {event_time(event.get('moonset'))}",
        detail="Illuminazione e orari locali; dettagli completi in Astronomia.",
        tone=tone,
    )


def render_v4_activities(
    forecast: pd.DataFrame,
    *,
    timezone: str | None = None,
    air: AirQualityForecast | None = None,
    settings: Settings | None = None,
) -> None:
    activities = activity_outlooks(forecast, timezone=timezone or CFG.local_timezone)
    cards = [
        _planner_card(
            icon=item.icon,
            title=item.activity,
            value=str(item.score),
            label=item.label,
            timing=f"Momento migliore: {item.best_time}",
            detail=item.detail,
            tone=item.tone,
        )
        for item in activities
    ]
    cards.insert(1, _pollen_planner_card(air))
    cards.insert(
        2,
        _ventilation_planner_card(
            forecast,
            air,
            timezone=timezone or CFG.local_timezone,
        ),
    )
    cards.append(_moon_planner_card(settings or CFG))
    st.markdown(
        '<div class="activity-grid">' + "".join(cards) + "</div>",
        unsafe_allow_html=True,
    )
    st.markdown(
        '<div class="v4-note">Gli indici sono orientativi: passeggiata e astronomia usano soglie '
        "trasparenti su pioggia, vento, temperatura, umidità e nuvole. Arieggiare combina anche "
        "AQI e pollini CAMS; Luna usa effemeridi. Sono indicazioni orientative, non allerte.</div>",
        unsafe_allow_html=True,
    )


def render_forecast_change(history: pd.DataFrame) -> ForecastChangeSummary:
    """Show stability versus the previous run without hiding missing history."""
    summary = summarize_forecast_change(history)
    status = (
        summary.status
        if summary.status in {"stable", "evolving", "changed"}
        else "collecting"
    )
    symbols = {"stable": "✓", "evolving": "↻", "changed": "△", "collecting": "…"}
    score = "—" if summary.score is None else f"{summary.score:.0f}%"
    issued = (
        ""
        if summary.previous_issued_at is None
        else " · confronto con " + _local_time(summary.previous_issued_at, "%H:%M")
    )
    st.markdown(
        f'<div class="change-card {status}">'
        f'<div class="change-score" aria-label="stabilità {html.escape(score)}">{symbols[status]} {html.escape(score)}</div>'
        "<div>"
        f'<div class="change-title">{html.escape(summary.headline)}</div>'
        f'<div class="change-detail">{html.escape(summary.detail + issued)}</div>'
        "</div></div>",
        unsafe_allow_html=True,
    )
    return summary


def render_ensemble_guidance(
    ensemble: pd.DataFrame,
    dark_mode: bool,
) -> None:
    """Render real ensemble percentiles as guidance, never as extra providers."""
    if ensemble.empty:
        st.info(
            "La guida probabilistica apparirà dopo il prossimo aggiornamento; "
            "la previsione deterministica resta disponibile."
        )
        return
    temperature = ensemble[ensemble["variable"].eq("temp_c")].sort_values("valid_time")
    rain = ensemble[ensemble["variable"].eq("rain_mm")].sort_values("valid_time")
    if temperature.empty:
        st.info("L'ensemble è archiviato ma non contiene la temperatura attesa.")
        return
    now = pd.Timestamp.now(tz="UTC")
    cutoff = now + pd.Timedelta(hours=72)
    display_start = now - pd.Timedelta(hours=2)
    temperature = temperature[temperature["valid_time"].between(display_start, cutoff)]
    rain = rain[rain["valid_time"].between(display_start, cutoff)]
    members = pd.to_numeric(ensemble.get("member_count"), errors="coerce").max()
    spread = (
        pd.to_numeric(temperature["p90"], errors="coerce")
        - pd.to_numeric(temperature["p10"], errors="coerce")
    ).max()
    rain_risk = (
        np.nan
        if rain.empty or "event_probability" not in rain
        else pd.to_numeric(rain["event_probability"], errors="coerce").max()
    )
    metrics = st.columns(3)
    metrics[0].metric(
        "Membri ensemble",
        "—" if pd.isna(members) else f"{members:.0f}",
        "ICON-EPS · non duplicati nel blend",
    )
    metrics[1].metric(
        "Forchetta termica max", _number(spread, 1, " °C"), "intervallo P10–P90 · 72 h"
    )
    metrics[2].metric(
        "Rischio pioggia max", _number(rain_risk, 0, " %"), "quota membri ≥ 0,1 mm/h"
    )

    figure = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.11,
        row_heights=[0.68, 0.32],
        subplot_titles=(
            "Temperatura ensemble",
            "Probabilità di pioggia tra i membri",
        ),
    )
    figure.add_trace(
        go.Scatter(
            x=plotly_local_datetimes(temperature["valid_time"], CFG.local_timezone),
            y=temperature["p90"],
            mode="lines",
            line={"width": 0},
            hoverinfo="skip",
            showlegend=False,
        ),
        row=1,
        col=1,
    )
    figure.add_trace(
        go.Scatter(
            x=plotly_local_datetimes(temperature["valid_time"], CFG.local_timezone),
            y=temperature["p10"],
            mode="lines",
            line={"width": 0},
            fill="tonexty",
            fillcolor="rgba(86,180,233,.24)",
            name="Intervallo P10–P90",
            hovertemplate="%{x|%d/%m %H:%M}<br>P10 %{y:.1f} °C<extra>Forchetta ensemble</extra>",
        ),
        row=1,
        col=1,
    )
    figure.add_trace(
        go.Scatter(
            x=plotly_local_datetimes(temperature["valid_time"], CFG.local_timezone),
            y=temperature["p50"],
            mode="lines",
            name="Mediana P50",
            line={"color": "#0072b2", "width": 3},
        ),
        row=1,
        col=1,
    )
    if not rain.empty:
        figure.add_trace(
            go.Scatter(
                x=plotly_local_datetimes(rain["valid_time"], CFG.local_timezone),
                y=rain["event_probability"],
                mode="lines",
                name="Membri con pioggia",
                line={"color": "#e69f00", "width": 2.5},
                fill="tozeroy",
                fillcolor="rgba(230,159,0,.12)",
            ),
            row=2,
            col=1,
        )
    figure.update_yaxes(title_text="Temperatura °C", row=1, col=1)
    figure.update_yaxes(title_text="Probabilità %", range=[0, 105], row=2, col=1)
    figure.update_layout(
        height=560,
        hovermode="x unified",
        margin={"l": 10, "r": 10, "t": 65, "b": 10},
    )
    st.plotly_chart(
        _style_plotly(_use_local_subplot_keys(figure), dark_mode),
        width="stretch",
        theme=None,
    )
    st.caption(
        "P10–P90 contiene circa l’80% degli scenari ICON-EPS. I membri servono a "
        "stimare l’incertezza e non contano come quaranta provider indipendenti."
    )


def render_data_provenance(
    forecast: pd.DataFrame,
    ensemble: pd.DataFrame,
    observed_air: pd.DataFrame,
    health: dict[str, Any],
) -> None:
    """Keep source, age and quality one tap away from every simplified view."""
    with st.expander("Origine, età e qualità dei dati"):
        first = forecast.iloc[0] if not forecast.empty else pd.Series(dtype="object")
        provider_count = pd.to_numeric(
            pd.Series([first.get("provider_count")]), errors="coerce"
        ).iloc[0]
        method = str(first.get("method") or "—")
        ensemble_time = ensemble["issued_at"].max() if not ensemble.empty else None
        eea_time = observed_air["time"].max() if not observed_air.empty else None
        cards = (
            (
                "Misure locali · Ecowitt",
                f"{_local_time(health.get('station_time'))} · {_age_text(health.get('station_age_minutes'))}",
            ),
            (
                "Previsione combinata",
                f"{_local_time(health.get('forecast_issued'))} · {('—' if pd.isna(provider_count) else f'{provider_count:.0f} provider')} · {method}",
            ),
            (
                "Ensemble ICON-EPS",
                "in attesa"
                if ensemble_time is None
                else f"emissione {_local_time(ensemble_time)} · guida probabilistica",
            ),
            (
                "Aria osservata EEA",
                "in attesa"
                if eea_time is None
                else f"misura preliminare {_local_time(eea_time)} · separata da Ecowitt",
            ),
        )
        st.markdown(
            '<div class="source-grid">'
            + "".join(
                f'<div class="source-card"><strong>{html.escape(title)}</strong><span>{html.escape(detail)}</span></div>'
                for title, detail in cards
            )
            + "</div>",
            unsafe_allow_html=True,
        )
        st.markdown(
            '<div class="accessibility-note">◉ I grafici distinguono le serie anche con tratteggi, simboli e testi: il colore non è l’unico indicatore.</div>',
            unsafe_allow_html=True,
        )


def render_share_summary(
    forecast: pd.DataFrame,
    air: AirQualityForecast | None,
) -> None:
    if forecast.empty:
        return
    daily = daily_forecast(forecast, CFG.local_timezone)
    today = daily.iloc[0] if not daily.empty else pd.Series(dtype="object")
    current_point = nearest_forecast(forecast)
    current = pd.Series(dtype="object") if current_point is None else current_point
    briefing = build_daily_briefing(forecast)
    date_value = today.get("date")
    if date_value is None or pd.isna(date_value):
        date_value = pd.Timestamp.now(tz=CFG.local_timezone)
    aqi = None if air is None else air.current.get("european_aqi")
    aqi_label = aqi_category(aqi)[0] if aqi is not None else "non disponibile"
    payload = render_share_card(
        ShareCardSummary(
            location=CFG.location_name,
            date_label=_day_label(date_value),
            condition=str(current.get("description") or "Condizioni locali"),
            temperature=f"{_number(today.get('temp_min'), 0, '°')} / {_number(today.get('temp_max'), 0, '°')}",
            rain=f"{_number(today.get('rain_mm'), 1, ' mm')} · max {_number(today.get('pop_max'), 0, '%')}",
            wind=f"raffiche {_number(today.get('wind_max'), 0, ' km/h')}",
            confidence=_number(today.get("confidence"), 0, "%"),
            briefing=briefing.headline,
            air=f"{aqi_label} · AQI {_number(aqi, 0)}",
        )
    )
    st.download_button(
        "Scarica il riepilogo del giorno (PNG)",
        data=payload,
        file_name=f"meteo-oggi-{_filename_slug(CFG.location_name)}.png",
        mime="image/png",
        width="stretch",
    )


POLLEN_LABELS = {
    "alder_pollen": "Ontano",
    "birch_pollen": "Betulla",
    "grass_pollen": "Graminacee",
    "mugwort_pollen": "Artemisia",
    "olive_pollen": "Olivo",
    "ragweed_pollen": "Ambrosia",
}


def _pollen_peak(values: Any) -> tuple[str, float | None]:
    candidates = {
        label: pd.to_numeric(pd.Series([values.get(column)]), errors="coerce").iloc[0]
        for column, label in POLLEN_LABELS.items()
    }
    candidates = {
        label: float(value) for label, value in candidates.items() if pd.notna(value)
    }
    if not candidates:
        return "Nessuno", None
    label = max(candidates, key=candidates.get)
    return label, candidates[label]


def _air_quality_figure(air: AirQualityForecast, dark_mode: bool) -> go.Figure:
    now = pd.Timestamp.now(tz="UTC").tz_convert(air.timezone)
    hourly = air.hourly[
        (air.hourly["time"] >= now.floor("h"))
        & (air.hourly["time"] <= now + pd.Timedelta(hours=72))
    ].copy()
    figure = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.11,
        row_heights=[0.58, 0.42],
        subplot_titles=(
            "Qualità dell’aria",
            "Pollini per specie",
        ),
    )
    figure.add_trace(
        go.Scatter(
            x=plotly_local_datetimes(hourly["time"], air.timezone),
            y=hourly["european_aqi"],
            name="AQI europeo",
            line={"color": "#22c55e", "width": 3},
            fill="tozeroy",
            fillcolor="rgba(34,197,94,.10)",
        ),
        row=1,
        col=1,
    )
    figure.add_trace(
        go.Scatter(
            x=plotly_local_datetimes(hourly["time"], air.timezone),
            y=hourly["pm2_5"],
            name="PM2.5",
            line={"color": "#f59e0b", "width": 1.8},
        ),
        row=1,
        col=1,
    )
    for column, label, color in (
        ("grass_pollen", "Graminacee", "#84cc16"),
        ("olive_pollen", "Olivo", "#a3e635"),
        ("ragweed_pollen", "Ambrosia", "#f97316"),
        ("mugwort_pollen", "Artemisia", "#a855f7"),
    ):
        if column in hourly and hourly[column].notna().any():
            figure.add_trace(
                go.Scatter(
                    x=plotly_local_datetimes(hourly["time"], air.timezone),
                    y=hourly[column],
                    name=label,
                    line={"color": color, "width": 2},
                ),
                row=2,
                col=1,
            )
    figure.update_yaxes(title_text="AQI / PM2.5", rangemode="tozero", row=1, col=1)
    figure.update_yaxes(title_text="Pollini grani/m³", rangemode="tozero", row=2, col=1)
    figure.update_layout(
        height=570,
        hovermode="x unified",
        margin={"l": 15, "r": 15, "t": 48, "b": 10},
    )
    return _style_plotly(_use_local_subplot_keys(figure), dark_mode)


def _expandable_air_card(
    title: str,
    value: str,
    detail: str,
    *,
    expanded: str,
    tone: str = "neutral",
    live_status: str | None = None,
    live_age_minutes: Any = None,
) -> str:
    live_badge = (
        _live_badge_html(live_status, live_age_minutes)
        if live_status is not None
        else ""
    )
    return (
        f'<details class="air-card expandable-card tone-{html.escape(tone)}">'
        f'<summary aria-label="Apri dettagli {html.escape(title)}">'
        '<div class="air-title-row">'
        f'<div class="air-title">{html.escape(title)}</div>{live_badge}</div>'
        f'<div class="air-value">{html.escape(value)}</div>'
        f'<div class="air-detail">{html.escape(detail)}</div>'
        '<div class="expand-hint"><span class="closed-label">Altri dettagli</span>'
        '<span class="open-label">Riduci</span></div></summary>'
        f'<div class="card-expanded">{html.escape(expanded)}</div></details>'
    )


def render_air_quality_dashboard(
    air: AirQualityForecast,
    dark_mode: bool,
    *,
    location_label: str,
) -> None:
    current = air.current
    aqi_label, aqi_tone = aqi_category(current.get("european_aqi"))
    pollen_name, pollen_value = _pollen_peak(current)
    pollen_label, pollen_tone = pollen_category(pollen_value)
    model_note = (
        f"Previsione modellistica {air.source} per {location_label}; "
        f"fonte aggiornata {_local_time(air.fetched_at)}. Non è una misura locale."
    )
    cards = [
        (
            "AQI europeo",
            _number(current.get("european_aqi"), 0),
            aqi_label,
            aqi_tone,
            model_note,
        ),
        (
            "PM2.5",
            _number(current.get("pm2_5"), 1, " µg/m³"),
            "particolato fine",
            "neutral",
            model_note,
        ),
        (
            "PM10",
            _number(current.get("pm10"), 1, " µg/m³"),
            "particolato",
            "neutral",
            model_note,
        ),
        (
            "Ozono",
            _number(current.get("ozone"), 1, " µg/m³"),
            "O₃ al suolo",
            "neutral",
            model_note,
        ),
        (
            "Biossido d'azoto",
            _number(current.get("nitrogen_dioxide"), 1, " µg/m³"),
            "NO₂",
            "neutral",
            model_note,
        ),
        (
            "Polline prevalente",
            _number(pollen_value, 1, " grani/m³"),
            f"{pollen_name} · {pollen_label}",
            pollen_tone,
            model_note + " Le fasce polliniche sono orientative.",
        ),
    ]
    st.markdown(
        '<div class="air-grid">'
        + "".join(
            _expandable_air_card(
                title,
                value,
                detail,
                tone=tone,
                expanded=expanded,
            )
            for title, value, detail, tone, expanded in cards
        )
        + "</div>",
        unsafe_allow_html=True,
    )
    st.plotly_chart(_air_quality_figure(air, dark_mode), width="stretch", theme=None)

    now = pd.Timestamp.now(tz="UTC").tz_convert(air.timezone)
    hourly = (
        air.hourly[
            (air.hourly["time"] >= now.floor("h"))
            & (air.hourly["time"] <= now + pd.Timedelta(hours=48))
        ]
        .iloc[::3]
        .copy()
    )
    if not hourly.empty:
        pollen_matrix = hourly[list(POLLEN_LABELS)].apply(
            pd.to_numeric, errors="coerce"
        )
        pollen_values = pollen_matrix.max(axis=1, skipna=True)
        has_pollen_value = pollen_matrix.notna().any(axis=1)
        pollen_species = pollen_matrix.fillna(-np.inf).idxmax(axis=1).map(POLLEN_LABELS)
        pollen_species = pollen_species.where(has_pollen_value, "—")
        table = pd.DataFrame(
            {
                "Ora": hourly["time"].map(_hour_label),
                "AQI europeo": _numeric_series(hourly, "european_aqi"),
                "PM2.5 µg/m³": _numeric_series(hourly, "pm2_5"),
                "PM10 µg/m³": _numeric_series(hourly, "pm10"),
                "NO₂ µg/m³": _numeric_series(hourly, "nitrogen_dioxide"),
                "O₃ µg/m³": _numeric_series(hourly, "ozone"),
                "UV": _numeric_series(hourly, "uv_index"),
                "Pollini max": pollen_values,
                "Specie prevalente": pollen_species,
            }
        )
        render_styled_table(_style_air_table(table, dark_mode), height=480)
    st.caption(
        f"{location_label} · previsione modellistica {air.source}, non sensore locale. "
        "Risoluzione CAMS europea circa 11 km; pollini disponibili in Europa durante la stagione. "
        "Fonte: [Open-Meteo Air Quality / CAMS](https://open-meteo.com/en/docs/air-quality-api)."
    )
    st.markdown(
        '<div class="v4-note">Le fasce AQI seguono l’indice europeo. I livelli dei pollini sono '
        "un orientamento grafico e non sostituiscono bollettini sanitari o indicazioni mediche.</div>",
        unsafe_allow_html=True,
    )


def render_observed_air_comparison(
    observed: pd.DataFrame,
    modelled: AirQualityForecast | None,
    dark_mode: bool,
) -> None:
    st.markdown(
        '<div class="section-kicker">Misure istituzionali indipendenti</div>',
        unsafe_allow_html=True,
    )
    st.subheader("Aria realmente osservata vicino a Roma")
    if observed.empty:
        st.info(
            "Le misure EEA/Italia appariranno dopo una risposta valida del servizio UTD. "
            "CAMS resta disponibile come previsione modellistica."
        )
        return
    metric_labels = {
        "pm2_5": "PM2.5",
        "pm10": "PM10",
        "nitrogen_dioxide": "NO₂",
        "ozone": "O₃",
        "so2": "SO₂",
    }
    cams_fields = {
        "pm2_5": "pm2_5",
        "pm10": "pm10",
        "nitrogen_dioxide": "nitrogen_dioxide",
        "ozone": "ozone",
        "so2": "sulphur_dioxide",
    }
    current_model = {} if modelled is None else modelled.current
    cards = []
    rows = []
    for _, item in observed.sort_values("metric").iterrows():
        metric = str(item.get("metric") or "")
        label = metric_labels.get(metric, metric)
        value = pd.to_numeric(pd.Series([item.get("value")]), errors="coerce").iloc[0]
        model_value = pd.to_numeric(
            pd.Series([current_model.get(cams_fields.get(metric, ""))]),
            errors="coerce",
        ).iloc[0]
        station_name = str(
            item.get("station_name") or item.get("station_id") or "stazione EEA"
        )
        distance = _number(item.get("distance_km"), 1, " km")
        observed_live, observed_age = _timestamp_live_state(
            item.get("time"), max_age_minutes=180
        )
        cards.append(
            _expandable_air_card(
                f"◉ {label} osservato",
                _number(value, 1, " µg/m³"),
                f"{station_name} · {distance} · {_local_time(item.get('time'))}",
                expanded=(
                    "Misura EEA UTD preliminare e indipendente da Ecowitt. "
                    f"CAMS nello stesso momento: {_number(model_value, 1, ' µg/m³')}. "
                    f"Qualità: {(item.get('quality_flag') or 'UTD_preliminare')!s}."
                ),
                live_status=observed_live,
                live_age_minutes=observed_age,
            )
        )
        rows.append(
            {
                "Inquinante": label,
                "EEA osservato µg/m³": value,
                "CAMS modellistico µg/m³": model_value,
                "Scarto osservato − modello": (
                    np.nan
                    if pd.isna(value) or pd.isna(model_value)
                    else value - model_value
                ),
                "Stazione": station_name,
                "Distanza km": pd.to_numeric(
                    pd.Series([item.get("distance_km")]), errors="coerce"
                ).iloc[0],
                "Ora misura": _local_time(item.get("time")),
                "Qualità": str(item.get("quality_flag") or "UTD_preliminare"),
            }
        )
    st.markdown(
        '<div class="air-grid">' + "".join(cards) + "</div>", unsafe_allow_html=True
    )
    render_styled_table(
        _base_table_style(pd.DataFrame(rows).round(1), dark_mode),
    )
    st.caption(
        "EEA UTD: misure orarie preliminari trasmesse dall’Italia, non ancora formalmente "
        "validate. CAMS è un modello e può riferirsi a una cella territoriale diversa. "
        "Il confronto è informativo e non modifica né sostituisce Ecowitt."
    )


def render_measured_pollen(
    measured: pd.DataFrame,
    modelled: AirQualityForecast | None,
    dark_mode: bool,
    *,
    expert_mode: bool,
) -> None:
    st.markdown(
        '<div class="section-kicker">Monitoraggio aerobiologico ufficiale</div>',
        unsafe_allow_html=True,
    )
    st.subheader("Pollini realmente misurati")
    if measured.empty:
        st.info(
            "Le misure POLLnet/ISPRA appariranno dopo il primo aggiornamento valido. "
            "La previsione CAMS resta disponibile e continua a essere indicata come modello."
        )
        return

    measured = measured.sort_values("value", ascending=False).copy()
    measured_at = pd.to_datetime(measured["time"].max(), utc=True, errors="coerce")
    age_days = (
        max(
            0, int((pd.Timestamp.now(tz="UTC") - measured_at).total_seconds() // 86_400)
        )
        if pd.notna(measured_at)
        else None
    )
    positive = measured[pd.to_numeric(measured["value"], errors="coerce") > 0]
    leaders = (positive if not positive.empty else measured).head(4)
    cards = []
    for _, item in leaders.iterrows():
        value = float(item["value"])
        level, tone = pollen_category(value)
        cards.append(
            _expandable_air_card(
                f"◉ {(item.get('family') or item['metric'])!s}",
                f"{value:.0f} granuli/m³",
                f"{level} indicativo · misura giornaliera",
                tone=tone,
                expanded=(
                    f"POLLnet/ISPRA · {(item.get('station_name') or 'stazione ufficiale')!s} · "
                    f"campione del {_local_time(item.get('time'), '%d/%m/%Y')} · "
                    f"distanza {_number(item.get('distance_km'), 1, ' km')}."
                ),
            )
        )
    st.markdown(
        '<div class="air-grid">' + "".join(cards) + "</div>", unsafe_allow_html=True
    )
    station_name = str(measured.iloc[0].get("station_name") or "POLLnet")
    distance = _number(measured.iloc[0].get("distance_km"), 1, " km")
    freshness = (
        "dato recente"
        if age_days is not None and age_days <= 14
        else f"ultimo campione {age_days} giorni fa"
        if age_days is not None
        else "età non disponibile"
    )
    if age_days is not None and age_days > 21:
        st.warning(
            f"POLLnet pubblica misure con cadenza e tempi di validazione propri: {freshness}. "
            "Il valore viene mostrato come archivio osservato, non come livello attuale."
        )
    if expert_mode:
        cams_map = {
            "pollen_gramineae": "grass_pollen",
            "pollen_oleaceae": "olive_pollen",
            "pollen_betulaceae": "birch_pollen",
            "pollen_compositae": "mugwort_pollen",
        }
        model_current = {} if modelled is None else modelled.current
        table = pd.DataFrame(
            {
                "Famiglia": measured["family"],
                "POLLnet osservato granuli/m³": measured["value"].round(1),
                "CAMS modellistico granuli/m³": measured["metric"].map(
                    lambda metric: model_current.get(cams_map.get(str(metric), ""))
                ),
                "Data misura": measured["time"].map(
                    lambda value: _local_time(value, "%d/%m/%Y")
                ),
            }
        )
        render_styled_table(_style_air_table(table, dark_mode), height=430)
    st.caption(
        f"{station_name} · {distance} dalla stazione Ecowitt · {freshness}. "
        "Fonte istituzionale: [POLLnet ISPRA/SNPA](https://pollnet.isprambiente.it/opendata/). "
        "Le fasce basso/medio/alto sono una lettura orientativa dell’app, non una "
        "classificazione sanitaria POLLnet. Una misura giornaliera non è una previsione "
        "e non sostituisce indicazioni mediche."
    )


def render_climate_context(
    station: pd.DataFrame,
    normals: pd.DataFrame,
    dark_mode: bool,
    *,
    expert_mode: bool,
) -> None:
    st.markdown(
        '<div class="section-kicker">Contesto storico della tua stazione</div>',
        unsafe_allow_html=True,
    )
    st.subheader("Com’è rispetto al consueto?")
    if station.empty or normals.empty:
        st.info(
            "La baseline locale si costruirà automaticamente quando l’archivio Ecowitt "
            "conterrà campioni sufficienti per mese e ora."
        )
        return
    snapshot = anomaly_snapshot(station.iloc[-1], normals, timezone=CFG.local_timezone)
    if snapshot.empty:
        st.caption("Nessuna baseline disponibile per questo mese e questa ora.")
        return
    cards = []
    for _, item in snapshot.iterrows():
        decimals = int(item["decimals"])
        unit = str(item["unit"])
        delta = float(item["delta"])
        tone = "neutral" if item["state"] == "Nella fascia consueta" else "warning"
        cards.append(
            _expandable_air_card(
                str(item["label"]),
                f"{float(item['value']):.{decimals}f} {unit}",
                f"{delta:+.{decimals}f} {unit} dalla mediana · {item['state']!s}",
                tone=tone,
                expanded=(
                    f"Fascia locale P10–P90: {float(item['p10']):.{decimals}f}–"
                    f"{float(item['p90']):.{decimals}f} {unit}; "
                    f"campione storico di {int(item['sample_years'])} anni."
                ),
            )
        )
    st.markdown(
        '<div class="air-grid">' + "".join(cards) + "</div>", unsafe_allow_html=True
    )
    years = int(snapshot["sample_years"].max())
    if expert_mode:
        table = pd.DataFrame(
            {
                "Parametro": snapshot["label"],
                "Ora": snapshot["value"].round(1),
                "Mediana locale": snapshot["normal"].round(1),
                "Fascia consueta P10–P90": snapshot.apply(
                    lambda row: f"{row['p10']:.1f}–{row['p90']:.1f} {row['unit']}",
                    axis=1,
                ),
                "Scarto": snapshot["delta"].round(1),
                "Esito": snapshot["state"],
            }
        )
        render_styled_table(_style_status_table(table, dark_mode, "Esito"), height=360)
    st.caption(
        f"Baseline Ecowitt locale per questo mese e questa ora · {years} "
        + ("anno rappresentato" if years == 1 else "anni rappresentati")
        + ". Finché non copre molti anni è un confronto con lo storico disponibile, "
        "non una normale climatica ufficiale 1991–2020."
    )


def render_reference_climate_context(
    station_id: str,
    normals: pd.DataFrame,
    dark_mode: bool,
    *,
    timezone: str,
) -> None:
    st.markdown(
        '<div class="section-kicker">Riferimento climatico 1991–2020</div>',
        unsafe_allow_html=True,
    )
    st.subheader("Confronto con un trentennio omogeneo")
    if normals.empty:
        st.info(
            "Il riferimento Copernicus ERA5-Land 1991–2020 verrà calcolato al "
            "prossimo aggiornamento mensile disponibile."
        )
        return
    available = station_months_data(station_id, timezone)
    now = pd.Timestamp.now(tz=timezone)
    completed = [item for item in available if item < (now.year, now.month)]
    selected = completed[0] if completed else (available[0] if available else None)
    if selected is None:
        st.caption(
            "Il riferimento è pronto; il confronto apparirà quando sarà disponibile "
            "almeno un mese di misure Ecowitt."
        )
        return
    year, month = selected
    measured = station_month_data(station_id, year, month, timezone)
    month_normals = normals[normals["month"].eq(month)].set_index("metric")
    temperature = pd.to_numeric(measured.get("temp_c"), errors="coerce")
    rain = pd.to_numeric(measured.get("rain_mm"), errors="coerce").clip(lower=0)
    observed_temp = float(temperature.mean()) if not temperature.empty else np.nan
    observed_rain = float(rain.sum(min_count=1)) if not rain.empty else np.nan
    normal_temp = (
        float(month_normals.loc["temp_c_mean", "value"])
        if "temp_c_mean" in month_normals.index
        else np.nan
    )
    normal_rain = (
        float(month_normals.loc["rain_mm", "value"])
        if "rain_mm" in month_normals.index
        else np.nan
    )
    cards = [
        (
            "Temperatura media",
            _number(observed_temp, 1, " °C"),
            f"riferimento {_number(normal_temp, 1, ' °C')} · scarto {_number(observed_temp - normal_temp, 1, ' °C')}",
        ),
        (
            "Precipitazione mensile",
            _number(observed_rain, 1, " mm"),
            f"riferimento {_number(normal_rain, 1, ' mm')} · scarto {_number(observed_rain - normal_rain, 1, ' mm')}",
        ),
    ]
    st.markdown(
        '<div class="air-grid">'
        + "".join(
            _expandable_air_card(
                title,
                value,
                detail,
                expanded=(
                    f"Confronto del mese {MONTHS_IT[month - 1]} {year} con la "
                    "rianalisi Copernicus ERA5-Land 1991–2020; non è una normale "
                    "ufficiale ISPRA/SCIA."
                ),
            )
            for title, value, detail in cards
        )
        + "</div>",
        unsafe_allow_html=True,
    )
    table = month_normals.reset_index().rename(
        columns={"metric": "Parametro", "value": "Valore", "unit": "Unità"}
    )
    labels = {
        "temp_c_mean": "Temperatura media",
        "temp_c_min": "Minima media",
        "temp_c_max": "Massima media",
        "rain_mm": "Precipitazione mensile",
    }
    table["Parametro"] = table["Parametro"].map(labels).fillna(table["Parametro"])
    render_styled_table(
        _base_table_style(
            table[["Parametro", "Valore", "Unità", "sample_years"]]
            .rename(columns={"sample_years": "Anni"})
            .round(1),
            dark_mode,
        ),
        height=260,
    )
    st.caption(
        f"Confronto: {MONTHS_IT[month - 1]} {year}. Riferimento numerico "
        "Copernicus ERA5-Land 1991–2020, coerente nel tempo ma basato su rianalisi. "
        "Per il prodotto istituzionale nazionale consulta [ISPRA/SCIA - Normali "
        "climatiche 1991–2020](https://www.isprambiente.gov.it/it/pubblicazioni/"
        "stato-dellambiente/i-normali-climatici-1991-2020-di-temperatura-e-"
        "precipitazione-in-italia). I valori SCIA non vengono copiati o ripubblicati."
    )


def render_monthly_exports(
    station_id: str,
    station_name: str,
    timezone: str,
) -> None:
    st.markdown(
        '<div class="section-kicker">Archivio personale</div>',
        unsafe_allow_html=True,
    )
    st.subheader("Rapporto mensile PDF e CSV")
    months = station_months_data(station_id, timezone)
    if not months:
        st.caption("Nessun mese disponibile per creare il rapporto.")
        return
    selected = st.selectbox(
        "Mese del rapporto",
        options=months,
        format_func=lambda item: f"{MONTHS_IT[item[1] - 1].title()} {item[0]}",
        key=f"report_month_{station_id}",
    )
    year, month = selected
    frame = station_month_data(station_id, year, month, timezone)
    health_frame = source_status_data()
    try:
        pdf = monthly_pdf_bytes(
            frame,
            year,
            month,
            timezone=timezone,
            station_name=station_name,
            source_health=health_frame,
        )
        csv = monthly_csv_bytes(frame, year, month, timezone=timezone)
    except (ImportError, ValueError) as exc:
        st.info(f"Il rapporto non è disponibile in questo momento ({exc}).")
        return
    left, right = st.columns(2)
    left.download_button(
        "Scarica rapporto PDF",
        data=pdf,
        file_name=report_filename(station_id, year, month, "pdf"),
        mime="application/pdf",
        width="stretch",
    )
    right.download_button(
        "Scarica misure CSV",
        data=csv,
        file_name=report_filename(station_id, year, month, "csv"),
        mime="text/csv",
        width="stretch",
    )
    st.caption(
        "Il PDF riassume le sole misure Ecowitt e lo stato delle fonti; non contiene "
        "le coordinate esatte della stazione. Il CSV conserva i campioni live oppure, "
        "quando l'archivio è giornaliero, le medie e gli estremi originali del file."
    )


def render_official_alerts(alerts: pd.DataFrame, *, expert_mode: bool) -> None:
    st.markdown(
        '<div class="section-kicker">Fonti istituzionali</div>',
        unsafe_allow_html=True,
    )
    st.subheader("Bollettini e allerte ufficiali")
    if alerts.empty:
        st.info(
            "I bollettini ufficiali compariranno dopo il primo aggiornamento valido. "
            "Gli avvisi previsionali dell’app restano indicazioni interne e non allerte."
        )
        return
    latest_dpc = alerts[alerts["source"] == "dpc_nazionale"].head(1)
    if not latest_dpc.empty:
        item = latest_dpc.iloc[0]
        message = f"**{item['title']}** — {item['description']} [Apri la fonte]({item['source_url']})"
        severity = str(item.get("severity") or "information")
        if severity == "green":
            st.success(message)
        elif severity in {"yellow", "orange", "official_notice"}:
            st.warning(message)
        elif severity == "red":
            st.error(message)
        else:
            st.info(message)

    regional = alerts[alerts["source"] == "regione_lazio"].copy()
    if not regional.empty:
        regional["age_hours"] = (
            (pd.Timestamp.now(tz="UTC") - regional["issued_at"])
            .dt.total_seconds()
            .div(3600)
        )
        for kind in ("criticita-", "allertamento-"):
            selected = regional[
                regional["alert_id"].astype(str).str.startswith(kind)
            ].head(1)
            if selected.empty:
                continue
            item = selected.iloc[0]
            current_note = (
                "documento recente"
                if float(item["age_hours"]) <= 48
                else "ultimo documento disponibile in archivio"
            )
            st.markdown(
                f"- **{item['title']}** · {current_note} · "
                f"[{item['area']}]({item['source_url']})"
            )
    if expert_mode:
        with st.expander("Archivio e metadati dei bollettini"):
            table = pd.DataFrame(
                {
                    "Fonte": alerts["source"],
                    "Emissione": alerts["issued_at"].map(_local_time),
                    "Livello": alerts["severity"],
                    "Titolo": alerts["title"],
                    "Area": alerts["area"],
                    "URL": alerts["source_url"],
                }
            )
            render_styled_table(_base_table_style(table, st.session_state["dark_mode"]))
    st.caption(
        "Solo DPC e Regione Lazio possono emettere allerte ufficiali. Le soglie meteo "
        "della dashboard restano avvisi contestuali e non sostituiscono i bollettini."
    )


def render_today_dashboard(
    forecast: pd.DataFrame,
    *,
    air: AirQualityForecast | None = None,
    alerts: pd.DataFrame | None = None,
    expert_mode: bool = False,
    history: pd.DataFrame | None = None,
    ensemble: pd.DataFrame | None = None,
    observed_air: pd.DataFrame | None = None,
    health: dict[str, Any] | None = None,
) -> None:
    st.markdown(
        '<div class="v4-section-head"><h3>Cosa è cambiato</h3><span>Confronto automatico con l’emissione precedente</span></div>',
        unsafe_allow_html=True,
    )
    render_forecast_change(pd.DataFrame() if history is None else history)
    st.markdown(
        '<div class="v4-section-head"><h3>Le prossime ore</h3><span>Scorri orizzontalmente · dati calibrati sulla stazione</span></div>',
        unsafe_allow_html=True,
    )
    render_v4_hourly_strip(forecast)
    st.markdown(
        '<div class="v4-section-head"><h3>Da sapere oggi</h3><span>Le informazioni più utili in un colpo d’occhio</span></div>',
        unsafe_allow_html=True,
    )
    render_v4_insights(forecast)
    st.markdown(
        '<div class="v4-section-head"><h3>Pianifica la giornata</h3><span>Indici orientativi · momento meteo migliore</span></div>',
        unsafe_allow_html=True,
    )
    render_v4_activities(forecast, air=air, settings=CFG)
    render_official_alerts(
        pd.DataFrame() if alerts is None else alerts,
        expert_mode=expert_mode,
    )
    st.markdown(
        '<div class="v4-section-head"><h3>Tendenza a 7 giorni</h3><span>Minime, massime, pioggia, vento e fiducia</span></div>',
        unsafe_allow_html=True,
    )
    render_daily_cards(daily_forecast(forecast, CFG.local_timezone))
    render_data_provenance(
        forecast,
        pd.DataFrame() if ensemble is None else ensemble,
        pd.DataFrame() if observed_air is None else observed_air,
        {} if health is None else health,
    )
    render_share_summary(forecast, air)


def _city_future_hours(city: CityForecast, hours: int = 72) -> pd.DataFrame:
    hourly = city.hourly.sort_values("time").copy()
    if hourly.empty:
        return hourly
    reference = pd.to_datetime(city.current.get("time"), errors="coerce")
    if pd.isna(reference):
        reference = hourly.iloc[0]["time"]
    return hourly[
        (hourly["time"] >= pd.Timestamp(reference).floor("h"))
        & (hourly["time"] <= pd.Timestamp(reference) + pd.Timedelta(hours=hours))
    ].copy()


def _render_city_daily_cards(city: CityForecast) -> None:
    if city.daily.empty:
        st.info("La sintesi giornaliera non è disponibile per questa località.")
        return
    cards = []
    for _, row in city.daily.head(7).iterrows():
        day = pd.Timestamp(row["time"])
        description = str(row.get("description") or "Variabile")
        sunrise = pd.to_datetime(row.get("sunrise"), errors="coerce")
        sunset = pd.to_datetime(row.get("sunset"), errors="coerce")
        cards.append(
            '<details class="day-card expandable-card">'
            f'<summary aria-label="Apri dettagli {ITALIAN_WEEKDAYS[day.weekday()]} {day.day}">'
            f'<div class="day-name">{ITALIAN_WEEKDAYS[day.weekday()]} {day.day}</div>'
            f'<div class="day-icon">{_weather_icon(description)}</div>'
            f'<div class="day-temp">{_number(row.get("temp_min_c"), 0, "°")} / {_number(row.get("temp_max_c"), 0, "°")}</div>'
            f'<div class="day-desc">{html.escape(description)}</div>'
            '<div class="expand-hint"><span class="closed-label">Altri dettagli</span><span class="open-label">Riduci</span></div>'
            '</summary><div class="card-expanded">'
            f"☔ {_number(row.get('precipitation_mm'), 1, ' mm')} · rischio {_number(row.get('precip_probability'), 0, '%')}"
            f"<br>💨 max {_number(row.get('wind_max_kmh'), 0, ' km/h')} · raffiche {_number(row.get('wind_gust_max_kmh'), 0, ' km/h')}"
            f"<br>🧭 {html.escape(compass_direction(row.get('wind_dir')))} · UV {_number(row.get('uv_index_max'), 1)}"
            f"<br>☀️ {'—' if pd.isna(sunrise) else sunrise.strftime('%H:%M')}–{'—' if pd.isna(sunset) else sunset.strftime('%H:%M')}"
            "</div></details>"
        )
    st.markdown(
        '<div class="forecast-grid">' + "".join(cards) + "</div>",
        unsafe_allow_html=True,
    )


def _render_city_next_hours(city: CityForecast) -> None:
    upcoming = _city_future_hours(city, 6).head(3)
    if upcoming.empty:
        return
    cards = []
    for _, row in upcoming.iterrows():
        moment = pd.Timestamp(row["time"])
        description = str(row.get("description") or "Variabile")
        cards.append(
            '<details class="hour-card expandable-card">'
            f'<summary aria-label="Apri dettagli previsione delle {moment:%H:%M}">'
            f'<div class="hour-title">{moment:%H:%M}</div>'
            f'<div class="hour-weather">{_weather_icon(description)} {html.escape(description)}</div>'
            f'<div class="hour-meta">🌡️ {_number(row.get("temp_c"), 1, " °C")}</div>'
            '<div class="expand-hint"><span class="closed-label">Altri dettagli</span><span class="open-label">Riduci</span></div>'
            '</summary><div class="card-expanded">'
            f"Percepita {_number(row.get('feels_like_c'), 1, ' °C')}"
            f"<br>☔ {_number(row.get('precipitation_mm'), 1, ' mm')} · rischio {_number(row.get('precip_probability'), 0, '%')}"
            f"<br>💨 {_number(row.get('wind_kmh'), 0, ' km/h')} · {html.escape(compass_direction(row.get('wind_dir')))}"
            f"<br>☁️ Nuvole {_number(row.get('cloud_cover'), 0, '%')} · UV {_number(row.get('uv_index'), 1)}"
            "</div></details>"
        )
    st.markdown(
        '<div class="hour-grid">' + "".join(cards) + "</div>",
        unsafe_allow_html=True,
    )


def _city_hourly_chart(city: CityForecast, dark_mode: bool) -> go.Figure:
    hourly = _city_future_hours(city)
    figure = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.09,
        row_heights=[0.64, 0.36],
        specs=[[{}], [{"secondary_y": True}]],
        subplot_titles=(
            "Temperatura prevista",
            "Pioggia e probabilità",
        ),
    )
    figure.add_trace(
        go.Scatter(
            x=plotly_local_datetimes(hourly["time"], city.timezone),
            y=hourly["temp_c"],
            name="Temperatura",
            line={"color": "#0ea5e9", "width": 3},
        ),
        row=1,
        col=1,
    )
    if "dewpoint_c" in hourly:
        figure.add_trace(
            go.Scatter(
                x=plotly_local_datetimes(hourly["time"], city.timezone),
                y=hourly["dewpoint_c"],
                name="Punto di rugiada",
                line={"color": "#0f9f9a", "width": 2, "dash": "dash"},
            ),
            row=1,
            col=1,
        )
    figure.add_trace(
        go.Scatter(
            x=plotly_local_datetimes(hourly["time"], city.timezone),
            y=hourly["feels_like_c"],
            name="Percepita",
            line={"color": "#f59e0b", "width": 2, "dash": "dot"},
        ),
        row=1,
        col=1,
    )
    figure.add_trace(
        go.Bar(
            x=plotly_local_datetimes(hourly["time"], city.timezone),
            y=pd.to_numeric(hourly["precipitation_mm"], errors="coerce").clip(lower=0),
            name="Precipitazioni",
            marker_color="#38bdf8",
            opacity=0.72,
        ),
        row=2,
        col=1,
        secondary_y=False,
    )
    figure.add_trace(
        go.Scatter(
            x=plotly_local_datetimes(hourly["time"], city.timezone),
            y=pd.to_numeric(hourly["precip_probability"], errors="coerce").clip(0, 100),
            name="Probabilità",
            line={"color": "#8b5cf6", "width": 2.2},
        ),
        row=2,
        col=1,
        secondary_y=True,
    )
    figure.update_yaxes(title_text="Temperatura °C", row=1, col=1)
    figure.update_yaxes(
        title_text="Pioggia mm",
        rangemode="nonnegative",
        row=2,
        col=1,
        secondary_y=False,
    )
    figure.update_yaxes(
        title_text="Probabilità %",
        range=[0, 105],
        row=2,
        col=1,
        secondary_y=True,
    )
    figure.update_layout(
        height=570,
        hovermode="x unified",
        margin={"l": 15, "r": 15, "t": 48, "b": 10},
    )
    return _style_plotly(_use_local_subplot_keys(figure), dark_mode)


def _city_hourly_table(hourly: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Ora": hourly["time"].map(_hour_label),
            "Cielo": hourly.get("description"),
            "Temp °C": _numeric_series(hourly, "temp_c").round(1),
            "Percepita °C": _numeric_series(hourly, "feels_like_c").round(1),
            "Rugiada °C": _numeric_series(hourly, "dewpoint_c").round(1),
            "Pioggia mm": _numeric_series(hourly, "precipitation_mm")
            .clip(lower=0)
            .round(1),
            "Prob. %": _numeric_series(hourly, "precip_probability").round(0),
            "Umidità %": _numeric_series(hourly, "humidity").round(0),
            "Pressione hPa": _numeric_series(hourly, "pressure_hpa").round(0),
            "Vento km/h": _numeric_series(hourly, "wind_kmh").round(1),
            "Raffiche km/h": _numeric_series(hourly, "wind_gust_kmh").round(1),
            "Direzione": hourly.get(
                "wind_dir", pd.Series(np.nan, index=hourly.index)
            ).map(compass_direction),
            "Nuvole %": _numeric_series(hourly, "clouds").round(0),
            "Visibilità km": (_numeric_series(hourly, "visibility_m") / 1000).round(1),
        }
    )


def render_city_dashboard(
    location: CityLocation,
    city: CityForecast,
    dark_mode: bool,
    local_forecast: pd.DataFrame | None = None,
) -> None:
    current = city.current
    current_time = pd.to_datetime(current.get("time"), errors="coerce")
    description = str(current.get("description") or "Variabile")
    city_briefing_frame = city.hourly.rename(
        columns={"time": "valid_time", "precipitation_mm": "rain_mm"}
    )
    city_briefing = build_daily_briefing(city_briefing_frame)
    city_today = (
        city.daily.iloc[0] if not city.daily.empty else pd.Series(dtype="object")
    )
    city_variant = _hero_variant(description, current.get("is_day"))
    st.markdown(
        f'<div class="hero-v4 {city_variant}"><div class="hero-primary">'
        '<div class="eyebrow">Meteo V4 città · dati internet</div>'
        f'<div class="hero-place">{html.escape(location.label)}</div>'
        '<div class="hero-condition">'
        f'<div class="hero-weather-icon">{_weather_icon(description)}</div>'
        f'<div class="hero-temperature">{_number(current.get("temp_c"), 1, "°")}</div>'
        f'<div><div class="hero-description">{html.escape(description)}</div>'
        f'<div class="hero-secondary">percepita {_number(current.get("feels_like_c"), 1, "°")} · '
        f"rugiada {_number(current.get('dewpoint_c'), 1, '°')} · min {_number(city_today.get('temp_min_c'), 0, '°')} · "
        f"max {_number(city_today.get('temp_max_c'), 0, '°')}</div></div></div>"
        f'<div class="hero-secondary">Fonte {html.escape(city.source)} · orari {html.escape(city.timezone)} · nessun dato Ecowitt</div>'
        '</div><div class="hero-brief"><div class="eyebrow">In breve</div>'
        f"<strong>{html.escape(city_briefing.headline)}</strong>"
        f"<small>{html.escape(city_briefing.detail)}</small></div></div>",
        unsafe_allow_html=True,
    )
    favorites = list(st.session_state.get("favorite_cities", []))
    favorite_lookup = {item.casefold() for item in favorites}
    is_favorite = location.name.casefold() in favorite_lookup
    if st.button(
        "★ Rimuovi dai preferiti" if is_favorite else "☆ Salva tra le città preferite",
        key=f"favorite_{location.latitude:.4f}_{location.longitude:.4f}",
    ):
        if is_favorite:
            favorites = [
                item
                for item in favorites
                if item.casefold() != location.name.casefold()
            ]
        else:
            favorites.append(location.name)
        st.session_state["favorite_cities"] = list(dict.fromkeys(favorites))[:8]
        _set_query_value("fav", "|".join(st.session_state["favorite_cities"]))
        st.rerun()
    st.markdown(
        '<div class="city-current">'
        f'<div class="city-current-icon">{_weather_icon(description)}</div>'
        '<div class="city-current-copy">'
        f"<strong>{html.escape(description)}</strong>"
        f"<div>Aggiornato {'—' if pd.isna(current_time) else current_time.strftime('%d/%m alle %H:%M')} · fonte {html.escape(city.source)}</div>"
        "</div></div>",
        unsafe_allow_html=True,
    )

    render_current_grid(
        [
            (
                "🌡️",
                "Temperatura",
                _number(current.get("temp_c"), 1, " °C"),
                "adesso",
                f"Dato modellistico aggiornato {'—' if pd.isna(current_time) else current_time.strftime('%d/%m alle %H:%M')} · fonte {city.source}.",
            ),
            (
                "🧍",
                "Percepita",
                _number(current.get("feels_like_c"), 1, " °C"),
                "sensazione termica",
                f"Combina temperatura, umidità e vento; temperatura dell’aria {_number(current.get('temp_c'), 1, ' °C')}.",
            ),
            (
                "◉",
                "Punto di rugiada",
                _number(current.get("dewpoint_c"), 1, " °C"),
                "condensa",
                f"Punto di rugiada previsto da {city.source}; temperatura dell’aria {_number(current.get('temp_c'), 1, ' °C')}.",
            ),
            (
                "💧",
                "Umidità",
                _number(current.get("humidity"), 0, " %"),
                "relativa",
                f"Umidità relativa prevista per {location.name}; nuvolosità {_number(current.get('clouds'), 0, '%')}.",
            ),
            (
                "⏱️",
                "Pressione",
                _number(current.get("pressure_hpa"), 0, " hPa"),
                "livello del mare",
                f"Pressione ricondotta al livello medio del mare · fonte {city.source}.",
            ),
            (
                "💨",
                "Vento",
                _number(current.get("wind_kmh"), 1, " km/h"),
                f"{compass_direction(current.get('wind_dir'))} · raffica {_number(current.get('wind_gust_kmh'), 1, ' km/h')}",
                f"Vento da {compass_direction(current.get('wind_dir'))}; raffiche fino a {_number(current.get('wind_gust_kmh'), 1, ' km/h')}.",
            ),
            (
                "☔",
                "Pioggia / nuvole",
                _number(current.get("precipitation_mm"), 1, " mm"),
                f"nuvole {_number(current.get('clouds'), 0, '%')}",
                f"Precipitazione dell’ora; probabilità {_number(current.get('precip_probability'), 0, '%')} e copertura nuvolosa {_number(current.get('clouds'), 0, '%')}.",
            ),
        ]
    )

    next_24 = _city_future_hours(city, 24)
    if not next_24.empty:
        rain = _numeric_series(next_24, "precipitation_mm", 0).clip(lower=0).sum()
        risk = _numeric_series(next_24, "precip_probability", 0).max()
        gust = _numeric_series(next_24, "wind_gust_kmh", 0).max()
        messages = []
        if rain >= 10 or risk >= 75:
            messages.append(f"pioggia {rain:.1f} mm, probabilità fino al {risk:.0f}%")
        if gust >= 45:
            messages.append(f"raffiche fino a {gust:.0f} km/h")
        if messages:
            st.warning("Nelle prossime 24 ore: " + "; ".join(messages) + ".")
        else:
            st.success(
                "Nessuna condizione rilevante individuata nelle prossime 24 ore."
            )

    city_overview, city_daily, city_compare, city_air, city_map = st.tabs(
        ["Oggi", "7 giorni", "Confronta con Roma", "Aria", "Mappa"],
        key="city_tab",
    )
    with city_overview:
        st.markdown(
            '<div class="section-kicker">Prossime ore</div>', unsafe_allow_html=True
        )
        st.subheader("Oggi a colpo d’occhio")
        render_v4_hourly_strip(
            city_briefing_frame,
            timezone=city.timezone,
        )
        render_v4_insights(
            city_briefing_frame,
            timezone=city.timezone,
        )
        city_planner_air = None
        if st.session_state.get("city_tab") == "Oggi":
            try:
                city_planner_air = air_quality_data(
                    location.latitude, location.longitude, city.timezone
                )
            except AirQualityError:
                pass
        city_settings = replace(
            CFG,
            latitude=location.latitude,
            longitude=location.longitude,
            elevation_m=location.elevation_m or 0.0,
            local_timezone=city.timezone,
            location_name=location.label,
        )
        render_v4_activities(
            city_briefing_frame,
            timezone=city.timezone,
            air=city_planner_air,
            settings=city_settings,
        )
        st.subheader("Temperatura e precipitazioni")
        st.plotly_chart(
            _city_hourly_chart(city, dark_mode), width="stretch", theme=None
        )
        interval = interval_selector(
            "Risoluzione della tabella", "city_forecast_interval", default=3
        )
        hourly = forecast_interval(_city_future_hours(city, 168), interval)
        table = _city_hourly_table(hourly)
        st.caption(
            f"{len(table)} righe · previsione internet campionata ogni {interval} "
            + ("ora" if interval == 1 else "ore")
        )
        render_color_legend("weather")
        render_styled_table(_style_hourly_table(table, dark_mode), height=520)
        st.download_button(
            "Scarica la previsione CSV",
            data=table.to_csv(index=False).encode("utf-8-sig"),
            file_name=f"meteo-{_filename_slug(location.name)}.csv",
            mime="text/csv",
            width="stretch",
        )

    with city_daily:
        st.markdown(
            '<div class="section-kicker">Tendenza internet</div>',
            unsafe_allow_html=True,
        )
        st.subheader("I prossimi 7 giorni")
        _render_city_daily_cards(city)
        daily = city.daily.copy()
        daily_table = pd.DataFrame(
            {
                "Data": daily["time"].map(_day_label),
                "Cielo": daily.get("description"),
                "Min °C": _numeric_series(daily, "temp_min_c"),
                "Max °C": _numeric_series(daily, "temp_max_c"),
                "Pioggia mm": _numeric_series(daily, "precipitation_mm").clip(lower=0),
                "Prob. %": _numeric_series(daily, "precip_probability"),
                "Vento max km/h": _numeric_series(daily, "wind_max_kmh"),
                "Raffiche km/h": _numeric_series(daily, "wind_gust_max_kmh"),
                "Direzione": daily.get(
                    "wind_dir", pd.Series(np.nan, index=daily.index)
                ).map(compass_direction),
                "UV max": _numeric_series(daily, "uv_index_max"),
                "Alba": daily["sunrise"].dt.strftime("%H:%M"),
                "Tramonto": daily["sunset"].dt.strftime("%H:%M"),
            }
        )
        render_color_legend("weather")
        render_styled_table(_style_city_daily_table(daily_table, dark_mode))

    with city_compare:
        st.markdown(
            '<div class="section-kicker">Due località, stessa scala</div>',
            unsafe_allow_html=True,
        )
        st.subheader(f"Roma e {location.name}")
        comparison = daily_city_comparison(
            pd.DataFrame()
            if local_forecast is None
            else daily_forecast(local_forecast, CFG.local_timezone),
            city.daily,
            city_label=location.name,
        )
        if comparison.empty:
            st.info(
                "Il confronto sarà disponibile appena la previsione locale e quella "
                "della città avranno giorni comuni."
            )
        else:
            render_styled_table(_base_table_style(comparison.round(1), dark_mode))
            st.caption(
                "Roma usa il blend calibrato sulla stazione Ecowitt; la città selezionata "
                "usa soltanto dati internet Open-Meteo. Le due origini restano separate."
            )

    with city_air:
        st.markdown(
            '<div class="section-kicker">Qualità dell’aria e pollini</div>',
            unsafe_allow_html=True,
        )
        st.subheader("Aria nelle prossime ore")
        if st.session_state.get("city_tab") != "Aria":
            st.caption("Apri questa scheda per caricare la previsione ambientale.")
        else:
            try:
                city_air_quality = air_quality_data(
                    location.latitude, location.longitude, city.timezone
                )
            except AirQualityError as exc:
                st.info(
                    f"La previsione dell’aria non è disponibile ora ({exc}). "
                    "Il resto del meteo città continua normalmente."
                )
            else:
                render_air_quality_dashboard(
                    city_air_quality,
                    dark_mode,
                    location_label=location.label,
                )

    with city_map:
        st.markdown(
            '<div class="section-kicker">Posizione e modello</div>',
            unsafe_allow_html=True,
        )
        st.subheader(location.label)
        st.map(
            pd.DataFrame({"lat": [location.latitude], "lon": [location.longitude]}),
            zoom=8,
        )
        elevation = (
            "—" if location.elevation_m is None else f"{location.elevation_m:.0f} m"
        )
        st.caption(
            f"Coordinate {location.latitude:.4f}, {location.longitude:.4f} · quota {elevation}. "
            "La previsione usa il modello più adatto disponibile per queste coordinate."
        )

    st.caption(
        f"Meteo città fornito da {city.source} · i valori non includono misure, correzioni "
        "o dati della tua stazione Ecowitt."
    )


def combined_chart(
    station: pd.DataFrame, forecast: pd.DataFrame, hours: int, theme: str
) -> go.Figure:
    now = pd.Timestamp.now(tz="UTC")
    history_start = now - pd.Timedelta(hours=hours)
    observations = (
        station[station["time"] >= history_start].copy()
        if not station.empty
        else station
    )
    future = forecast_chart_window(forecast, now, future_hours=72, lookback_hours=2)
    figure = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        row_heights=[0.68, 0.32],
        vertical_spacing=0.08,
        specs=[[{}], [{"secondary_y": True}]],
        subplot_titles=(
            "Temperatura: misure e previsioni",
            "Pioggia: misure, previsione e probabilità",
        ),
    )
    if not observations.empty and "temp_c" in observations:
        figure.add_trace(
            go.Scatter(
                x=plotly_local_datetimes(observations["time"], CFG.local_timezone),
                y=observations["temp_c"],
                name="Temperatura misurata",
                mode="lines",
                line={"color": "#0b76b7", "width": 3},
                connectgaps=False,
                hovertemplate="%{x|%d/%m %H:%M}<br>%{y:.1f} °C<extra>Misurata dalla stazione</extra>",
            ),
            row=1,
            col=1,
        )
        for column, label, colour in (
            ("feels_like_c", "Percepita calcolata", "#f59e0b"),
            ("dewpoint_c", "Punto di rugiada calcolato", "#0f9f9a"),
        ):
            if column in observations:
                figure.add_trace(
                    go.Scatter(
                        x=plotly_local_datetimes(
                            observations["time"], CFG.local_timezone
                        ),
                        y=observations[column],
                        name=label,
                        mode="lines",
                        line={"color": colour, "width": 2.1},
                        connectgaps=False,
                        hovertemplate=(
                            f"%{{x|%d/%m %H:%M}}<br>%{{y:.1f}} °C<extra>{label}</extra>"
                        ),
                    ),
                    row=1,
                    col=1,
                )

    missing_temperature = missing_forecast_segments(
        observations,
        forecast,
        "temp_c",
        "temp_c",
        now,
        lookback_hours=min(hours, 3),
    )
    for position, gap in enumerate(missing_temperature):
        figure.add_vrect(
            x0=plotly_local_datetime(gap.start, CFG.local_timezone),
            x1=plotly_local_datetime(gap.end, CFG.local_timezone),
            fillcolor="rgba(244,63,94,.13)",
            line_width=0,
            layer="below",
            row=1,
            col=1,
        )
        figure.add_trace(
            go.Scatter(
                x=plotly_local_datetimes(gap.points["valid_time"], CFG.local_timezone),
                y=gap.points["temp_c"],
                name="Stima per perdita dati · non misurata",
                legendgroup="missing-data",
                showlegend=position == 0,
                mode="lines",
                line={"color": "#f43f5e", "width": 3, "dash": "dashdot"},
                hovertemplate=(
                    "%{x|%d/%m %H:%M}<br>%{y:.1f} °C"
                    "<br><b>DATO MANCANTE: valore stimato</b>"
                    "<extra>Non misurato</extra>"
                ),
            ),
            row=1,
            col=1,
        )
    if not observations.empty and "rain_rate_mm_h" in observations:
        measured_rain = pd.to_numeric(
            observations["rain_rate_mm_h"], errors="coerce"
        ).clip(lower=0)
        figure.add_trace(
            go.Bar(
                x=plotly_local_datetimes(observations["time"], CFG.local_timezone),
                y=measured_rain,
                name="Pioggia misurata",
                marker_color="#0284c7",
                opacity=0.78,
                hovertemplate=(
                    "%{x|%d/%m %H:%M}<br>%{y:.1f} mm/h"
                    "<extra>Misurata dalla stazione</extra>"
                ),
            ),
            row=2,
            col=1,
            secondary_y=False,
        )
    if not future.empty and "temp_c" in future:
        uncertainty = pd.to_numeric(
            future.get("temp_uncertainty_c"), errors="coerce"
        ).fillna(1.2)
        upper = future["temp_c"] + uncertainty
        lower = future["temp_c"] - uncertainty
        figure.add_trace(
            go.Scatter(
                x=plotly_local_datetimes(future["valid_time"], CFG.local_timezone),
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
                x=plotly_local_datetimes(future["valid_time"], CFG.local_timezone),
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
        for column, label, colour, dash in (
            ("feels_like_c", "Percepita prevista", "#f59e0b", "dash"),
            ("dewpoint_c", "Punto di rugiada previsto", "#0f9f9a", "dot"),
        ):
            if column in future:
                figure.add_trace(
                    go.Scatter(
                        x=plotly_local_datetimes(
                            future["valid_time"], CFG.local_timezone
                        ),
                        y=future[column],
                        name=label,
                        mode="lines",
                        line={"color": colour, "width": 2.0, "dash": dash},
                        opacity=0.9,
                        hovertemplate=(
                            f"%{{x|%d/%m %H:%M}}<br>%{{y:.1f}} °C<extra>{label}</extra>"
                        ),
                    ),
                    row=1,
                    col=1,
                )
        figure.add_trace(
            go.Scatter(
                x=plotly_local_datetimes(future["valid_time"], CFG.local_timezone),
                y=future["temp_c"],
                name="Previsione · da −2 h archiviata",
                mode="lines",
                line={"color": "#2563eb", "width": 3, "dash": "dash"},
                text=future.get("confidence", pd.Series(np.nan, index=future.index)),
                customdata=future.get(
                    "chart_origin",
                    pd.Series("blend_corrente", index=future.index),
                ).map(
                    {
                        "previsione_archiviata": "emissione archiviata",
                        "blend_corrente": "blend corrente",
                    }
                ),
                hovertemplate=(
                    "%{x|%d/%m %H:%M}<br>%{y:.1f} °C"
                    "<br>Fiducia %{text:.0f}%<br>%{customdata}"
                    "<extra>Previsione</extra>"
                ),
            ),
            row=1,
            col=1,
        )
        if "rain_mm" in future:
            figure.add_trace(
                go.Bar(
                    x=plotly_local_datetimes(future["valid_time"], CFG.local_timezone),
                    y=pd.to_numeric(future["rain_mm"], errors="coerce").clip(lower=0),
                    name="Pioggia prevista",
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
                    x=plotly_local_datetimes(future["valid_time"], CFG.local_timezone),
                    y=pd.to_numeric(future["precip_probability"], errors="coerce").clip(
                        0, 100
                    ),
                    name="Prob. pioggia futura",
                    line={"color": "#7c3aed", "width": 2},
                ),
                row=2,
                col=1,
                secondary_y=True,
            )
    figure.add_vline(
        x=plotly_local_datetime(now, CFG.local_timezone),
        line_dash="dot",
        line_color="#f97316",
        opacity=0.9,
    )
    figure.update_yaxes(title_text="Temperatura °C", row=1, col=1)
    figure.update_yaxes(
        title_text="Pioggia mm/h",
        rangemode="nonnegative",
        row=2,
        col=1,
        secondary_y=False,
    )
    figure.update_yaxes(
        title_text="Probabilità %", range=[0, 105], row=2, col=1, secondary_y=True
    )
    figure.update_xaxes(
        title_text=f"Ora locale · {CFG.local_timezone}",
        tickformat="%d/%m %H:%M",
        row=2,
        col=1,
    )
    figure.update_layout(
        height=570,
        margin={"l": 15, "r": 15, "t": 48, "b": 10},
        hovermode="x unified",
        template=theme,
        bargap=0.1,
    )
    return _use_local_subplot_keys(figure)


def weather_details_chart(
    station: pd.DataFrame, forecast: pd.DataFrame, hours: int, theme: str
) -> go.Figure:
    """Join observations and future forecasts, highlighting sensor data loss."""
    now = pd.Timestamp.now(tz="UTC")
    observations = (
        station[station["time"] >= now - pd.Timedelta(hours=hours)].copy()
        if not station.empty
        else station
    )
    future = forecast_chart_window(forecast, now, future_hours=72, lookback_hours=2)
    figure = make_subplots(
        rows=3,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.08,
        specs=[[{}], [{}], [{"secondary_y": True}]],
        subplot_titles=(
            "Umidità: misure e previsioni",
            "Pressione: misure e previsioni",
            "Vento, raffiche e direzione",
        ),
    )

    metrics = (
        ("Umidità", "humidity", "humidity", 1, False, "#0ea5e9", "%"),
        ("Pressione", "pressure_hpa", "pressure_hpa", 2, False, "#8b5cf6", "hPa"),
        ("Vento", "wind_kmh", "wind_kmh", 3, False, "#10b981", "km/h"),
        (
            "Raffiche",
            "windgust_kmh",
            "wind_gust_kmh",
            3,
            False,
            "#f59e0b",
            "km/h",
        ),
        ("Direzione", "winddir", "wind_dir", 3, True, "#ec4899", "°"),
    )
    fallback_intervals: dict[int, list[tuple[pd.Timestamp, pd.Timestamp]]] = {
        1: [],
        2: [],
        3: [],
    }
    fallback_legend_shown = False
    for (
        label,
        station_column,
        forecast_column,
        row,
        secondary_y,
        colour,
        unit,
    ) in metrics:
        if not observations.empty and station_column in observations:
            figure.add_trace(
                go.Scatter(
                    x=plotly_local_datetimes(observations["time"], CFG.local_timezone),
                    y=observations[station_column],
                    name=f"{label} · misurata",
                    mode="lines",
                    connectgaps=False,
                    line={"color": colour, "width": 2.5},
                    hovertemplate=(
                        f"%{{x|%d/%m %H:%M}}<br>%{{y:.1f}} {unit}"
                        f"<extra>{label} misurata</extra>"
                    ),
                ),
                row=row,
                col=1,
                secondary_y=secondary_y,
            )
        if not future.empty and forecast_column in future:
            figure.add_trace(
                go.Scatter(
                    x=plotly_local_datetimes(future["valid_time"], CFG.local_timezone),
                    y=future[forecast_column],
                    name=f"{label} · previsione da −2 h",
                    mode="lines",
                    line={"color": colour, "width": 2.2, "dash": "dash"},
                    opacity=0.86,
                    hovertemplate=(
                        f"%{{x|%d/%m %H:%M}}<br>%{{y:.1f}} {unit}"
                        f"<extra>{label} prevista</extra>"
                    ),
                ),
                row=row,
                col=1,
                secondary_y=secondary_y,
            )

        gaps = missing_forecast_segments(
            observations,
            forecast,
            station_column,
            forecast_column,
            now,
            lookback_hours=min(hours, 3),
        )
        for gap in gaps:
            fallback_intervals[row].append((gap.start, gap.end))
            figure.add_trace(
                go.Scatter(
                    x=plotly_local_datetimes(
                        gap.points["valid_time"], CFG.local_timezone
                    ),
                    y=gap.points[forecast_column],
                    name="Stima per perdita dati · non misurata",
                    legendgroup="missing-data",
                    showlegend=not fallback_legend_shown,
                    mode="lines",
                    line={"color": "#f43f5e", "width": 2.8, "dash": "dashdot"},
                    hovertemplate=(
                        f"%{{x|%d/%m %H:%M}}<br>%{{y:.1f}} {unit}"
                        f"<br><b>{label.upper()} NON MISURATA: valore stimato</b>"
                        "<extra>Perdita dati</extra>"
                    ),
                ),
                row=row,
                col=1,
                secondary_y=secondary_y,
            )
            fallback_legend_shown = True

    for row, intervals in fallback_intervals.items():
        for start, end in merge_intervals(intervals):
            figure.add_vrect(
                x0=plotly_local_datetime(start, CFG.local_timezone),
                x1=plotly_local_datetime(end, CFG.local_timezone),
                fillcolor="rgba(244,63,94,.12)",
                line_width=0,
                layer="below",
                row=row,
                col=1,
            )

    figure.add_vline(
        x=plotly_local_datetime(now, CFG.local_timezone),
        line_dash="dot",
        line_color="#f97316",
        opacity=0.9,
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
    figure.update_xaxes(
        title_text=f"Ora locale · {CFG.local_timezone}",
        tickformat="%d/%m %H:%M",
        row=3,
        col=1,
    )
    figure.update_layout(
        height=760,
        margin={"l": 15, "r": 15, "t": 48, "b": 10},
        hovermode="x unified",
        template=theme,
    )
    return _use_local_subplot_keys(figure)


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
    rain = reportable_rain_series(next_24).sum()
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


def _query_value(name: str, default: str = "") -> str:
    value = st.query_params.get(name, default)
    if isinstance(value, list):
        value = value[-1] if value else default
    return str(value)


def _set_query_value(name: str, value: Any) -> None:
    rendered = str(value)
    if _query_value(name) != rendered:
        st.query_params[name] = rendered


def _remember_selected_tab() -> None:
    label = str(st.session_state.get("main_tab", "Oggi"))
    _set_query_value("tab", TAB_SLUGS.get(label, "today"))


query_mode = _query_value("mode", "local")
if "app_section" not in st.session_state:
    st.session_state["app_section"] = (
        "Meteo città" if query_mode == "city" else "Stazione locale"
    )
if "dark_mode" not in st.session_state:
    st.session_state["dark_mode"] = _query_value("theme", "light") == "dark"
if "experience_mode" not in st.session_state:
    st.session_state["experience_mode"] = (
        "Esperta" if _query_value("detail", "simple") == "expert" else "Semplice"
    )
if "observation_hours" not in st.session_state:
    try:
        requested_hours = int(_query_value("hours", "24"))
    except ValueError:
        requested_hours = 24
    st.session_state["observation_hours"] = (
        requested_hours if requested_hours in {12, 24, 48, 72, 120} else 24
    )
if "city_query" not in st.session_state:
    st.session_state["city_query"] = _query_value("city", "").strip()
if "favorite_cities" not in st.session_state:
    st.session_state["favorite_cities"] = [
        item.strip()
        for item in _query_value("fav", "").split("|")
        if 1 < len(item.strip()) <= 80
    ][:8]


selected_city: CityLocation | None = None
city_lookup_error: str | None = None
observation_hours = 24
station_profiles = station_profiles_data()
active_station_id = CFG.station_id
active_station_name = CFG.location_name
active_station_timezone = CFG.local_timezone

with st.sidebar:
    st.markdown("### 🌦️ Meteo V4")
    st.caption("La tua stazione, spiegata in modo semplice.")
    st.header("Esplora")
    app_section = st.radio(
        "Sezione",
        options=("Stazione locale", "Meteo città"),
        label_visibility="collapsed",
        key="app_section",
    )

    if app_section == "Meteo città":
        st.subheader("Cerca una città")
        favorite_cities = list(st.session_state.get("favorite_cities", []))
        if favorite_cities:
            st.caption("Preferite")
            for position, favorite in enumerate(favorite_cities):
                if st.button(
                    f"★ {favorite}",
                    key=f"favorite_shortcut_{position}_{favorite.casefold()}",
                    width="stretch",
                ):
                    st.session_state["city_query"] = favorite
                    st.rerun()
        with st.form("city_search_form", border=False):
            typed_city = st.text_input(
                "Nome o CAP",
                value=st.session_state.get("city_query", ""),
                placeholder="Es. Milano, Londra, 00100",
            )
            city_submitted = st.form_submit_button(
                "Cerca",
                type="primary",
                width="stretch",
            )
        if city_submitted:
            cleaned_city = " ".join(typed_city.split())
            if len(cleaned_city) < 2:
                city_lookup_error = "Inserisci almeno due caratteri."
            else:
                st.session_state["city_query"] = cleaned_city

        city_query = str(st.session_state.get("city_query", "")).strip()
        if city_query:
            try:
                city_matches = city_search_data(city_query)
            except CityWeatherError as exc:
                city_matches = []
                city_lookup_error = str(exc)
            if city_matches:
                selected_city = st.selectbox(
                    "Località trovata",
                    options=city_matches,
                    format_func=lambda location: location.label,
                    key=f"city_choice_{city_query.casefold()}",
                )
            elif city_lookup_error is None:
                city_lookup_error = (
                    "Nessuna località trovata. Prova ad aggiungere provincia o nazione."
                )
        if city_lookup_error:
            st.warning(city_lookup_error)
        st.caption(
            "La ricerca usa esclusivamente dati meteo online; la stazione locale non viene mescolata."
        )
    elif not station_profiles.empty:
        profile_lookup = station_profiles.set_index("station_id")
        identifiers = station_profiles["station_id"].astype(str).tolist()
        primary_profiles = station_profiles[
            station_profiles["role"].astype(str).eq("primary")
        ]
        active_station_id = (
            CFG.station_id
            if CFG.station_id in identifiers
            else str(primary_profiles.iloc[0]["station_id"])
            if not primary_profiles.empty
            else identifiers[0]
        )
        single_name = str(profile_lookup.loc[active_station_id, "display_name"])
        st.markdown(
            '<div class="station-active-card" aria-label="Stazione principale">'
            '<span class="station-active-dot"></span>'
            '<span class="station-active-copy"><small>Stazione principale</small>'
            f"<strong>{html.escape(single_name)}</strong></span></div>",
            unsafe_allow_html=True,
        )
        if len(identifiers) > 1:
            st.caption(
                f"{len(identifiers)} stazioni disponibili · consultazione e confronto "
                "nella scheda Stazione. Roma resta il riferimento delle previsioni."
            )
        profile = profile_lookup.loc[active_station_id]
        active_station_name = str(profile["display_name"])
        active_station_timezone = str(profile["timezone"])

    st.divider()
    st.subheader("Vista")
    if CFG.feature_experience_mode_enabled:
        experience_mode = st.radio(
            "Livello di dettaglio",
            options=("Semplice", "Esperta"),
            horizontal=True,
            key="experience_mode",
            help=(
                "Semplice privilegia sintesi e decisioni quotidiane; Esperta aggiunge "
                "confronti grezzi, fasce statistiche e metadati delle fonti."
            ),
        )
    else:
        experience_mode = "Esperta"
    if app_section == "Stazione locale":
        observation_hours = st.select_slider(
            "Storico nel grafico",
            options=[12, 24, 48, 72, 120],
            format_func=lambda value: f"{value} ore",
            key="observation_hours",
        )
    dark_mode = st.toggle("Tema scuro", key="dark_mode")
    auto_refresh = st.toggle(
        "Aggiorna la pagina ogni 10 min", value=True, key="auto_refresh"
    )
    st.divider()
    if app_section == "Stazione locale":
        st.caption(
            "I dati vengono acquisiti dal Cron Job Render ogni 10 minuti e riconciliati "
            "ogni giorno da GitHub. Ricarica dati forza il ciclo e fa ripartire i 10 minuti."
        )
    else:
        st.caption(
            "Le città visibili partecipano allo stesso aggiornamento automatico ogni 10 minuti."
        )
    if st.button("Ricarica dati", width="stretch"):
        st.session_state["last_full_refresh"] = time.monotonic()
        st.cache_data.clear()
        st.rerun()

_set_query_value("mode", "city" if app_section == "Meteo città" else "local")
_set_query_value("theme", "dark" if dark_mode else "light")
_set_query_value("detail", "expert" if experience_mode == "Esperta" else "simple")
if app_section == "Stazione locale":
    _set_query_value("hours", observation_hours)
elif st.session_state.get("city_query"):
    _set_query_value("city", st.session_state["city_query"])
_set_query_value("fav", "|".join(st.session_state.get("favorite_cities", [])))

_refresh_controller(auto_refresh)
_apply_theme(dark_mode)
expert_mode = experience_mode == "Esperta"

if app_section == "Meteo città":
    if selected_city is None:
        st.markdown(
            '<div class="hero">'
            '<div class="eyebrow">Meteo V4 città · ricerca mondiale</div>'
            "<h1>Che tempo fa altrove?</h1>"
            "<p>Cerca una città dal menu laterale per ottenere condizioni attuali, "
            "previsione oraria, sette giorni e mappa.</p>"
            "</div>",
            unsafe_allow_html=True,
        )
        st.info(
            "Apri il menu laterale, seleziona **Meteo città** e cerca per nome o CAP. "
            "Se esistono più località omonime potrai scegliere provincia e nazione."
        )
        st.stop()
    try:
        with st.spinner(f"Carico il meteo per {selected_city.label}…"):
            selected_city_forecast = city_forecast_data(selected_city)
    except CityWeatherError as exc:
        st.error(
            f"Non riesco a caricare questa località: {exc}. Riprova tra qualche minuto."
        )
        st.stop()
    render_city_dashboard(
        selected_city,
        selected_city_forecast,
        dark_mode,
        local_forecast=forecast_data(),
    )
    st.stop()

station = station_data(max(observation_hours + 24, 240), active_station_id)
forecast = forecast_data()
forecast_history = forecast_history_data()
ensemble_guidance = ensemble_guidance_data()
official_air_observed = observed_air_data()
official_pollen_observed = measured_pollen_data()
climate_normals = climate_normals_data()
reference_climate_normals = reference_climate_data(active_station_id)
official_alerts = official_alerts_data()
health = health_data()
theme = "plotly_dark" if dark_mode else "plotly_white"

render_v4_hero(station, forecast, health)

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
st.markdown(_measurement_freshness_row(health), unsafe_allow_html=True)

stale_measurements = _stale_measurement_labels(health)
if stale_measurements:
    st.warning(
        "Misure reali mancanti o ferme per: "
        + ", ".join(stale_measurements)
        + ". Nel tratto precedente alla linea arancione, la fascia rosa e la linea "
        "corallo tratteggiata indicano una stima di emergenza: non sono dati misurati."
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
    current_time_label = _local_time(current.get("time"))
    current_freshness = health.get("measurement_freshness") or {}

    def live_state(metric: str) -> tuple[str, Any]:
        details = current_freshness.get(metric) or {}
        return str(details.get("status") or "offline"), details.get("age_minutes")

    def derived_live_state(*metrics: str) -> tuple[str, Any]:
        states = [live_state(metric) for metric in metrics]
        statuses = [state[0] for state in states]
        status = (
            "offline"
            if "offline" in statuses
            else "delayed"
            if "delayed" in statuses
            else "online"
        )
        age_values = pd.to_numeric(
            pd.Series([state[1] for state in states]), errors="coerce"
        ).dropna()
        ages = [float(value) for value in age_values if np.isfinite(value)]
        return status, max(ages) if ages else None

    temperature_live = live_state("temperature")
    humidity_live = live_state("humidity")
    pressure_live = live_state("pressure")
    wind_live = live_state("wind")
    rain_live = live_state("rain")
    solar_live = live_state("solar")
    perceived_live = derived_live_state("temperature", "humidity", "wind")
    dewpoint_live = derived_live_state("temperature", "humidity")
    dew_spread_values = pd.to_numeric(
        pd.Series([current.get("temp_c"), current.get("dewpoint_c")]),
        errors="coerce",
    )
    dew_spread = (
        float(dew_spread_values.iloc[0] - dew_spread_values.iloc[1])
        if dew_spread_values.notna().all()
        else np.nan
    )
    render_current_grid(
        [
            (
                "🌡️",
                "Temperatura",
                _number(current.get("temp_c"), 1, " °C"),
                _delta(current, previous, "temp_c"),
                f"Misura Ecowitt delle {current_time_label}. Il badge confronta il valore con circa tre ore prima.",
                *temperature_live,
            ),
            (
                "🧍",
                "Percepita",
                _number(current.get("feels_like_c"), 1, " °C"),
                "stima all’ombra",
                f"Calcolata da temperatura, umidità e vento Ecowitt delle {current_time_label}; non include il sole diretto o l’abbigliamento.",
                *perceived_live,
            ),
            (
                "◉",
                "Punto di rugiada",
                _number(current.get("dewpoint_c"), 1, " °C"),
                f"scarto {_number(dew_spread, 1, ' °C')}",
                f"Calcolo Magnus da temperatura e umidità Ecowitt delle {current_time_label}; più si avvicina alla temperatura, maggiore è il rischio di condensa.",
                *dewpoint_live,
            ),
            (
                "💧",
                "Umidità",
                _number(current.get("humidity"), 0, " %"),
                _delta(current, previous, "humidity", 0),
                f"Umidità relativa misurata alle {current_time_label}; temperatura contestuale {_number(current.get('temp_c'), 1, ' °C')}.",
                *humidity_live,
            ),
            (
                "⏱️",
                "Pressione",
                _number(current.get("pressure_hpa"), 1, " hPa"),
                _delta(current, previous, "pressure_hpa"),
                f"Pressione atmosferica della stazione alle {current_time_label}; variazione riferita a circa tre ore.",
                *pressure_live,
            ),
            (
                "💨",
                "Vento",
                _number(current.get("wind_kmh"), 1, " km/h"),
                f"{compass_direction(current.get('winddir'))} · raffica {_number(current.get('windgust_kmh'), 1, ' km/h')}",
                f"Direzione {compass_direction(current.get('winddir'))}; raffica massima istantanea {_number(current.get('windgust_kmh'), 1, ' km/h')} alle {current_time_label}.",
                *wind_live,
            ),
            (
                "☔",
                "Pioggia 24 h",
                _number(rain_24, 1, " mm"),
                f"ora {_number(current.get('rain_rate_mm_h'), 1, ' mm/h')}",
                f"Accumulo calcolato sui campioni reali delle ultime 24 ore; intensità corrente {_number(current.get('rain_rate_mm_h'), 1, ' mm/h')}.",
                *rain_live,
            ),
            (
                "☀️",
                "Solare / UV",
                _number(current.get("solar_w_m2"), 0, " W/m²"),
                f"UV {_number(current.get('uv_index'), 1)}",
                f"Radiazione solare globale e indice UV misurati dalla stazione alle {current_time_label}.",
                *solar_live,
            ),
        ]
    )
else:
    st.markdown(
        '<div class="empty"><b>Stazione non ancora popolata.</b> La dashboard può già mostrare le previsioni; '
        "esegui la pipeline dopo aver configurato le tre credenziali Ecowitt.</div>",
        unsafe_allow_html=True,
    )

forecast_alerts(forecast)

tab_labels = list(TAB_SLUGS)
requested_tab = next(
    (
        label
        for label, slug in TAB_SLUGS.items()
        if slug == _query_value("tab", "today")
    ),
    "Oggi",
)
(
    tab_today,
    tab_overview,
    tab_forecast,
    tab_station,
    tab_air,
    tab_astro,
    tab_radar,
    tab_system,
) = st.tabs(
    tab_labels,
    default=requested_tab,
    key="main_tab",
    on_change=_remember_selected_tab,
)

with tab_today:
    today_air_quality = None
    if st.session_state.get("main_tab") == "Oggi" and not forecast.empty:
        try:
            today_air_quality = air_quality_data(
                CFG.latitude, CFG.longitude, CFG.local_timezone
            )
        except AirQualityError:
            pass
    render_today_dashboard(
        forecast,
        air=today_air_quality,
        alerts=official_alerts,
        expert_mode=expert_mode,
        history=forecast_history,
        ensemble=ensemble_guidance,
        observed_air=official_air_observed,
        health=health,
    )

with tab_overview:
    st.markdown(
        '<div class="section-kicker">Passato misurato e futuro previsto</div>',
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
            theme=None,
        )
        st.caption(
            f"Orari {CFG.local_timezone}: la linea arancione indica l'istante reale adesso. "
            "La previsione blu tratteggiata recupera dall'archivio anche le due ore "
            "precedenti per confrontarla con le misure, senza spostare artificialmente "
            "i timestamp. "
            "Fascia rosa e corallo tratteggiato: perdita dati, valore stimato."
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
            theme=None,
        )
    if not forecast.empty:
        st.markdown('<div class="section-kicker">Sintesi</div>', unsafe_allow_html=True)
        st.subheader("I prossimi 7 giorni")
        render_daily_cards(daily_forecast(forecast, CFG.local_timezone))
        with st.expander("Come leggere misure, stime, fiducia e fascia azzurra"):
            st.write(
                "A sinistra della linea arancione la linea continua proviene dalla stazione. "
                "La linea blu tratteggiata parte due ore prima di adesso usando le emissioni "
                "archiviate, rende confrontabile lo scorrimento della previsione e prosegue "
                "nel futuro combinando i provider. "
                "Se una misura reale manca per oltre 30 minuti, "
                "solo quel buco può essere coperto dalla linea corallo tratteggiata e dalla fascia "
                "rosa: è una stima di emergenza, non una misura. La fascia azzurra rappresenta "
                "l'incertezza e cresce quando i modelli divergono; la fiducia considera anche "
                "numero di provider e distanza temporale."
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
        forecast_step = interval_selector(
            "Risoluzione della tabella", "local_forecast_interval", default=3
        )
        now = pd.Timestamp.now(tz="UTC")
        hourly = forecast[
            (forecast["valid_time"] >= now)
            & (forecast["valid_time"] <= now + pd.Timedelta(days=7))
        ].copy()
        hourly = forecast_interval(hourly, forecast_step)
        hourly["Ora"] = (
            hourly["valid_time"].dt.tz_convert(CFG.local_timezone).map(_hour_label)
        )
        table = pd.DataFrame(
            {
                "Ora": hourly["Ora"],
                "Cielo": hourly.get("description"),
                "Temp °C": _numeric_series(hourly, "temp_c").round(1),
                "Percepita °C": _numeric_series(hourly, "feels_like_c").round(1),
                "Rugiada °C": _numeric_series(hourly, "dewpoint_c").round(1),
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
        st.caption(
            f"{len(table)} righe · previsione calibrata campionata ogni {forecast_step} "
            + ("ora" if forecast_step == 1 else "ore")
        )
        render_color_legend("weather")
        render_styled_table(
            _style_hourly_table(table, dark_mode),
            height=520,
        )
        st.download_button(
            "Scarica la previsione CSV",
            data=table.to_csv(index=False).encode("utf-8-sig"),
            file_name=f"previsione-{_filename_slug(CFG.location_name)}.csv",
            mime="text/csv",
            width="stretch",
        )

        st.markdown(
            '<div class="section-kicker">Scenari probabilistici</div>',
            unsafe_allow_html=True,
        )
        st.subheader("Quanto possono divergere gli scenari")
        render_ensemble_guidance(ensemble_guidance, dark_mode)

        scores = score_data()
        regime_scores = regime_score_data()
        reliability = reliability_data()
        reference_scores = reference_score_data()
        official_stations = official_station_data()
        enabled_reference_sources = {"awc_metar"}
        if CFG.arsial_polling_enabled:
            enabled_reference_sources.add("arsial_siarl")
        if CFG.cfr_observations_enabled:
            enabled_reference_sources.add("cfr_lazio")
        if "source" in official_stations:
            official_stations = official_stations[
                official_stations["source"].isin(enabled_reference_sources)
            ].copy()
        if "source" in reference_scores:
            reference_scores = reference_scores[
                reference_scores["source"].isin(enabled_reference_sources)
            ].copy()
        source_labels = {
            "awc_metar": "METAR aeroportuale",
            "arsial_siarl": "ARSIAL / SIARL",
            "cfr_lazio": "CFR Lazio · MeteoHub",
        }
        active_reference_names = ["Fiumicino e Ciampino"]
        if CFG.arsial_polling_enabled:
            active_reference_names.append(
                "ARSIAL Roma-Lanciani"
                + (" · verifica automatica" if CFG.arsial_auto_probe else "")
            )
        if CFG.cfr_observations_enabled:
            active_reference_names.append("CFR Roma Monte Mario via MeteoHub")
        cfr_status_caption = (
            " Il CFR usa la raccolta pubblica anonima DPCN-Lazio di MeteoHub (CC BY 4.0)."
            if CFG.cfr_observations_enabled
            else ""
        )
        with st.expander("Calibrazione 2.0: Ecowitt, regimi e rete ufficiale"):
            st.markdown("#### Verifica locale Ecowitt")
            if scores.empty:
                st.caption(
                    "Il confronto automatico inizierà quando previsioni archiviate e osservazioni avranno orari sovrapposti."
                )
            else:
                display = scores.copy()
                display["skill_vs_persistence"] = (
                    pd.to_numeric(display.get("skill_vs_persistence"), errors="coerce")
                    * 100.0
                )
                display["reliability_gap"] = (
                    pd.to_numeric(display.get("reliability_gap"), errors="coerce")
                    * 100.0
                )
                display = display.rename(
                    columns={
                        "provider": "Provider",
                        "variable": "Variabile",
                        "horizon": "Orizzonte",
                        "n": "Campioni",
                        "bias": "Bias",
                        "mae": "MAE",
                        "rmse": "RMSE",
                        "brier": "Brier",
                        "holdout_n": "Campioni validazione",
                        "holdout_mae": "MAE validazione",
                        "persistence_mae": "MAE persistenza",
                        "skill_vs_persistence": "Skill vs persistenza %",
                        "reliability_gap": "Gap affidabilità %",
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
                        "Campioni validazione",
                        "MAE validazione",
                        "MAE persistenza",
                        "Skill vs persistenza %",
                        "Gap affidabilità %",
                    ]
                ]
                render_color_legend("scores")
                render_styled_table(
                    _style_score_table(display.round(2), dark_mode),
                )
                st.caption(
                    "La validazione usa la parte temporale più recente non impiegata "
                    "come riepilogo storico. Skill positiva significa che il provider "
                    "batte la semplice persistenza dell'ultima misura disponibile."
                )
                if expert_mode and not regime_scores.empty:
                    regime_labels = {
                        "cold": "freddo",
                        "hot": "caldo",
                        "mild": "mite",
                        "dry": "asciutto",
                        "wet": "piovoso",
                        "humid": "umido",
                        "moderate": "intermedio",
                        "low": "bassa pressione",
                        "high": "alta pressione",
                        "normal": "pressione normale",
                        "windy": "ventoso",
                        "calm": "calmo",
                        "all": "generale",
                    }
                    regime_display = regime_scores.rename(
                        columns={
                            "provider": "Provider",
                            "variable": "Variabile",
                            "horizon": "Orizzonte",
                            "regime": "Regime",
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
                            "Regime",
                            "Campioni",
                            "Bias",
                            "MAE",
                            "RMSE",
                            "Brier",
                        ]
                    ]
                    regime_display["Regime"] = (
                        regime_display["Regime"]
                        .map(regime_labels)
                        .fillna(regime_display["Regime"])
                    )
                    st.markdown("#### Errore per situazione meteo")
                    render_styled_table(
                        _style_score_table(regime_display.round(2), dark_mode),
                        height=380,
                    )
                    st.caption(
                        "Quando esistono almeno sei confronti, il blend usa anche "
                        "l'errore del regime previsto; il suo contributo è limitato "
                        "e cresce gradualmente con i campioni."
                    )

            if not reliability.empty:
                reliability_figure = go.Figure()
                for (provider, horizon), group in reliability.groupby(
                    ["provider", "horizon"]
                ):
                    reliability_figure.add_trace(
                        go.Scatter(
                            x=group["mean_probability"] * 100.0,
                            y=group["observed_frequency"] * 100.0,
                            mode="lines+markers",
                            name=f"{provider} · {horizon}",
                            marker={
                                "size": 7
                                + np.sqrt(pd.to_numeric(group["n"], errors="coerce")),
                            },
                            customdata=group[["n", "brier"]],
                            hovertemplate=(
                                "Prevista %{x:.0f}%<br>Osservata %{y:.0f}%"
                                "<br>Campioni %{customdata[0]:.0f}"
                                "<br>Brier %{customdata[1]:.3f}<extra></extra>"
                            ),
                        )
                    )
                reliability_figure.add_trace(
                    go.Scatter(
                        x=[0, 100],
                        y=[0, 100],
                        mode="lines",
                        name="··· Calibrazione ideale",
                        line={"color": "#94a3b8", "dash": "dot"},
                    )
                )
                reliability_figure.update_layout(
                    height=410,
                    title="Affidabilità della probabilità di pioggia",
                    xaxis_title="Probabilità prevista %",
                    yaxis_title="Frequenza osservata %",
                    legend={"orientation": "h"},
                    margin={"l": 10, "r": 10, "t": 55, "b": 10},
                )
                reliability_figure.update_xaxes(range=[0, 100])
                reliability_figure.update_yaxes(range=[0, 100])
                st.plotly_chart(
                    _style_plotly(reliability_figure, dark_mode),
                    width="stretch",
                    theme=None,
                )

            st.markdown("#### Riferimenti ufficiali indipendenti")
            st.caption(
                "Ecowitt resta il riferimento principale. "
                + ", ".join(active_reference_names)
                + " "
                f"regolarizzano al massimo il {CFG.official_score_max_share:.0%} "
                f"della statistica dopo almeno {CFG.official_min_overlap_samples} "
                "campioni sovrapposti; non sostituiscono mai una misura locale."
                + cfr_status_caption
            )
            if official_stations.empty:
                st.caption(
                    "Le osservazioni ufficiali verranno mostrate dopo la prima "
                    "esecuzione della pipeline."
                )
            else:
                station_display = pd.DataFrame(
                    {
                        "Fonte": official_stations["source"].map(source_labels),
                        "Stazione": official_stations["station_id"],
                        "Nome": official_stations["station_name"],
                        "Ultimo dato": official_stations["time"]
                        .dt.tz_convert(CFG.local_timezone)
                        .dt.strftime("%d/%m %H:%M"),
                        "Distanza km": official_stations["distance_km"].round(1),
                        "Qualità": official_stations["quality_flag"],
                    }
                )
                render_styled_table(
                    _style_score_table(station_display, dark_mode),
                )

            if not reference_scores.empty:
                reference_scores = reference_scores.copy()
                reference_scores["site_correlation"] = (
                    pd.to_numeric(
                        reference_scores.get("site_correlation"), errors="coerce"
                    )
                    * 100.0
                )
                reference_display = reference_scores.rename(
                    columns={
                        "provider": "Provider",
                        "source": "Fonte",
                        "station_id": "Stazione",
                        "variable": "Variabile",
                        "horizon": "Orizzonte",
                        "n": "Campioni",
                        "bias": "Bias",
                        "mae": "MAE",
                        "rmse": "RMSE",
                        "brier": "Brier",
                        "site_correlation": "Correlazione sito %",
                        "reference_weight": "Peso qualità",
                    }
                )[
                    [
                        "Provider",
                        "Fonte",
                        "Stazione",
                        "Variabile",
                        "Orizzonte",
                        "Campioni",
                        "Bias",
                        "MAE",
                        "RMSE",
                        "Brier",
                        "Correlazione sito %",
                        "Peso qualità",
                    ]
                ]
                reference_display["Fonte"] = reference_display["Fonte"].map(
                    source_labels
                )
                render_color_legend("scores")
                render_styled_table(
                    _style_score_table(reference_display.round(2), dark_mode),
                    height=420,
                )

with tab_air:
    st.markdown(
        '<div class="section-kicker">Salute ambientale</div>',
        unsafe_allow_html=True,
    )
    st.subheader("Qualità dell’aria e pollini")
    local_air_quality = None
    if st.session_state.get("main_tab") != "Aria":
        st.caption("Apri questa scheda per caricare la previsione ambientale.")
    else:
        try:
            local_air_quality = air_quality_data(
                CFG.latitude, CFG.longitude, CFG.local_timezone
            )
        except AirQualityError as exc:
            st.info(
                f"La previsione dell’aria non è disponibile ora ({exc}). "
                "Stazione e previsioni meteo continuano normalmente."
            )
        else:
            render_air_quality_dashboard(
                local_air_quality,
                dark_mode,
                location_label=CFG.location_name,
            )
        render_observed_air_comparison(
            official_air_observed,
            local_air_quality,
            dark_mode,
        )
        render_measured_pollen(
            official_pollen_observed,
            local_air_quality,
            dark_mode,
            expert_mode=expert_mode,
        )

with tab_station:
    st.markdown(
        '<div class="section-kicker">Misure reali</div>', unsafe_allow_html=True
    )
    station_view_id = active_station_id
    station_view_name = active_station_name
    station_view_timezone = active_station_timezone
    if not station_profiles.empty:
        station_profile_lookup = station_profiles.set_index("station_id")
        station_view_options = station_profiles["station_id"].astype(str).tolist()
        if len(station_view_options) > 1:
            station_view_id = st.selectbox(
                "Stazione da consultare",
                options=station_view_options,
                index=(
                    station_view_options.index(active_station_id)
                    if active_station_id in station_view_options
                    else 0
                ),
                format_func=lambda value: str(
                    station_profile_lookup.loc[value, "display_name"]
                ),
                help=(
                    "La scelta riguarda misure, storico e diagnostica della scheda "
                    "Stazione; le previsioni generali restano calibrate su Roma."
                ),
                key="station_detail_selector",
            )
        selected_profile = station_profile_lookup.loc[station_view_id]
        station_view_name = str(selected_profile["display_name"])
        station_view_timezone = str(selected_profile["timezone"])
    station_view = (
        station
        if station_view_id == active_station_id
        else station_data(max(observation_hours + 24, 240), station_view_id)
    )
    daily_station_history = station_daily_data(365)
    if len(station_profiles) > 1:
        st.markdown(
            '<div class="section-kicker">Confronto omogeneo</div>',
            unsafe_allow_html=True,
        )
        st.subheader("Roma e seconda stazione")
        render_station_comparison(
            daily_station_history,
            station_profiles,
            active_station_id,
            dark_mode,
        )
        st.divider()

    st.subheader(f"{station_view_name} · ultime {observation_hours} ore")
    if station_view.empty:
        st.info(
            "Nessun campione live disponibile per questa stazione. Lo storico "
            "giornaliero resta consultabile nel confronto qui sopra."
        )
    else:
        cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=observation_hours)
        recent = station_view[station_view["time"] >= cutoff].copy()
        figure = make_subplots(
            rows=4,
            cols=1,
            shared_xaxes=True,
            vertical_spacing=0.065,
            subplot_titles=(
                "Temperatura, percepita e punto di rugiada",
                "Umidità misurata",
                "Pressione misurata",
                "Vento e raffiche misurati",
            ),
        )
        figure.add_trace(
            go.Scatter(
                x=plotly_local_datetimes(recent["time"], station_view_timezone),
                y=recent.get("temp_c"),
                name="Temperatura",
                line={"color": "#ef4444", "width": 2.5},
            ),
            row=1,
            col=1,
        )
        for column, label, colour, dash in (
            ("feels_like_c", "Percepita calcolata", "#f59e0b", "dash"),
            ("dewpoint_c", "Punto di rugiada calcolato", "#0f9f9a", "dot"),
        ):
            if column in recent:
                figure.add_trace(
                    go.Scatter(
                        x=plotly_local_datetimes(recent["time"], station_view_timezone),
                        y=recent[column],
                        name=label,
                        line={"color": colour, "width": 2.0, "dash": dash},
                        connectgaps=False,
                    ),
                    row=1,
                    col=1,
                )
        figure.add_trace(
            go.Scatter(
                x=plotly_local_datetimes(recent["time"], station_view_timezone),
                y=recent.get("humidity"),
                name="Umidità",
                line={"color": "#0ea5e9", "width": 2},
            ),
            row=2,
            col=1,
        )
        figure.add_trace(
            go.Scatter(
                x=plotly_local_datetimes(recent["time"], station_view_timezone),
                y=recent.get("pressure_hpa"),
                name="Pressione",
                line={"color": "#8b5cf6", "width": 2.5},
            ),
            row=3,
            col=1,
        )
        figure.add_trace(
            go.Scatter(
                x=plotly_local_datetimes(recent["time"], station_view_timezone),
                y=recent.get("wind_kmh"),
                name="Vento",
                line={"color": "#10b981", "width": 2},
            ),
            row=4,
            col=1,
        )
        figure.add_trace(
            go.Scatter(
                x=plotly_local_datetimes(recent["time"], station_view_timezone),
                y=recent.get("windgust_kmh"),
                name="Raffiche",
                line={"color": "#f59e0b", "width": 1.8, "dash": "dash"},
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
            margin={"l": 10, "r": 10, "t": 45, "b": 10},
        )
        st.plotly_chart(
            _style_plotly(_use_local_subplot_keys(figure), dark_mode),
            width="stretch",
            theme=None,
        )

        rain_figure = go.Figure(
            go.Bar(
                x=plotly_local_datetimes(recent["time"], station_view_timezone),
                y=_numeric_series(recent, "rain_mm", 0).clip(lower=0),
                name="Quantità misurata per campione",
                marker_color="#38bdf8",
            )
        )
        if "rain_rate_mm_h" in recent:
            rain_figure.add_trace(
                go.Scatter(
                    x=plotly_local_datetimes(recent["time"], station_view_timezone),
                    y=pd.to_numeric(recent["rain_rate_mm_h"], errors="coerce").clip(
                        lower=0
                    ),
                    name="Intensità misurata",
                    line={"color": "#2563eb", "width": 2.2},
                )
            )
        rain_figure.update_layout(
            height=330,
            title="Pioggia: quantità per campione e intensità",
            template=theme,
            hovermode="x unified",
            margin={"l": 10, "r": 10, "t": 72, "b": 10},
        )
        rain_figure.update_yaxes(rangemode="nonnegative", title_text="Pioggia mm")
        st.plotly_chart(
            _style_plotly(rain_figure, dark_mode), width="stretch", theme=None
        )

        render_climate_context(
            station_view,
            climate_normals,
            dark_mode,
            expert_mode=expert_mode,
        )
        render_reference_climate_context(
            station_view_id,
            (
                reference_climate_normals
                if station_view_id == active_station_id
                else reference_climate_data(station_view_id)
            ),
            dark_mode,
            timezone=station_view_timezone,
        )
        render_monthly_exports(
            station_view_id,
            station_view_name,
            station_view_timezone,
        )

        quality = recent.get("data_quality", pd.Series(dtype="object")).value_counts(
            dropna=False
        )
        with st.expander("Qualità dati e ultime esecuzioni"):
            render_color_legend("status")
            left, right = st.columns(2)
            left.write("Campioni per qualità")
            quality_table = quality.rename_axis("Qualità").reset_index(name="Campioni")
            render_styled_table(
                _style_status_table(quality_table, dark_mode, "Qualità"),
                container=left,
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
                render_styled_table(
                    _style_status_table(safe_logs, dark_mode, "status"),
                    container=right,
                )

with tab_astro:
    st.markdown(
        '<div class="section-kicker">Finestre osservative</div>', unsafe_allow_html=True
    )
    st.subheader("Qualità del cielo notturno")
    astro = prepare_astronomy(forecast, CFG)
    if astro.empty:
        st.info("Servono le previsioni V4 per calcolare le condizioni astronomiche.")
    else:
        events = astronomy_events(CFG)
        light_pollution, light_pollution_error = light_pollution_data()
        if light_pollution is not None:
            sqm_cols = st.columns(3)
            sqm_cols[0].metric(
                f"SQM stimato · atlante {light_pollution.year}",
                f"{light_pollution.sqm:.2f} mag/arcsec²",
                "zenit sereno e senza Luna · non misurato",
                delta_color="off",
            )
            sqm_cols[1].metric(
                "Bortle indicativa",
                f"≈ {light_pollution.approximate_bortle}",
                "classe visuale approssimata",
                delta_color="off",
            )
            sqm_cols[2].metric(
                "Zona inquinamento luminoso",
                light_pollution.lp_zone,
                f"indice LP {light_pollution.lp_index:.1f}× il cielo naturale",
                delta_color="off",
            )

            daily_astro = daily_astronomy_summary(astro, events).head(7)
            if not daily_astro.empty:
                daily_astro["sqm"] = light_pollution.sqm
                daily_astro["bortle"] = light_pollution.approximate_bortle
                daily_astro["lp_zone"] = light_pollution.lp_zone
                daily_table = pd.DataFrame(
                    {
                        "Data": pd.to_datetime(daily_astro["date"]).map(_day_label),
                        "SQM stimato": daily_astro["sqm"],
                        "Bortle ≈": daily_astro["bortle"],
                        "Zona LP": daily_astro["lp_zone"],
                        "Qualità media": daily_astro["weather_score_mean"],
                        "Qualità migliore": daily_astro["weather_score_best"],
                        "Ore buone": daily_astro["good_hours"],
                        "Nuvole notte %": daily_astro["clouds_mean"],
                        "Vento notte km/h": daily_astro["wind_mean"],
                        "Trasparenza proxy": _numeric_series(
                            daily_astro, "transparency_proxy"
                        ),
                        "Stabilità proxy": _numeric_series(
                            daily_astro, "stability_proxy"
                        ),
                        "Rischio condensa %": _numeric_series(daily_astro, "dew_risk"),
                        "Luna illuminata %": _numeric_series(
                            daily_astro, "moon_illumination"
                        ),
                    }
                )
                st.markdown(
                    '<div class="section-kicker">Riepilogo giornaliero</div>',
                    unsafe_allow_html=True,
                )
                st.subheader("SQM, inquinamento luminoso e meteo astronomico")
                render_color_legend("astronomy")
                render_styled_table(
                    _style_astronomy_table(daily_table, dark_mode),
                )
            st.caption(
                f"SQM e zona LP sono stime zenitali geolocalizzate dell'Atlante {light_pollution.year} "
                "per l'area della stazione; restano uguali nei giorni perché "
                "descrivono il sito. Meteo e Luna cambiano ogni notte. Un valore SQM reale richiede "
                f"un fotometro calibrato. [Apri la fonte]({light_pollution.source_url})."
            )
        else:
            st.info(
                "La stima geolocalizzata SQM non è disponibile in questo momento: "
                + (light_pollution_error or "fonte non raggiungibile")
                + ". Le altre previsioni astronomiche restano utilizzabili."
            )

        st.divider()
        st.markdown(
            '<div class="section-kicker">Pianificazione personale</div>',
            unsafe_allow_html=True,
        )
        st.subheader("Pianificatore Astronomia Pro")
        if "astro_custom_targets" not in st.session_state:
            st.session_state["astro_custom_targets"] = {}
        if "astro_equipment_profiles" not in st.session_state:
            default_profile = equipment_profile(
                name="Setup Tripletto",
                telescope="Tripletto 80/480",
                camera="571MC-Pro",
                aperture_mm=80,
                focal_length_mm=480,
                sensor_width_mm=23.5,
                sensor_height_mm=15.7,
                pixel_size_um=3.76,
                focal_multiplier=0.8,
            )
            st.session_state["astro_equipment_profiles"] = {
                default_profile.name: default_profile
            }
            st.session_state["astro_active_profile"] = default_profile.name
            st.session_state["astro_active_profile_select"] = default_profile.name
        if "astro_horizon_mask" not in st.session_state:
            st.session_state["astro_horizon_mask"] = {
                direction: 0.0 for direction in range(0, 360, 45)
            }
        if "astro_session_log" not in st.session_state:
            st.session_state["astro_session_log"] = []
        pending_configuration = st.session_state.pop(
            "astro_pending_configuration", None
        )
        if pending_configuration is not None:
            restored_profiles, restored_targets, restored_horizon = (
                pending_configuration
            )
            st.session_state["astro_equipment_profiles"] = restored_profiles
            st.session_state["astro_custom_targets"] = restored_targets
            st.session_state.pop("astronomy_targets", None)
            st.session_state["astro_horizon_mask"] = {
                float(direction): float(restored_horizon.get(direction, 0.0))
                for direction in range(0, 360, 45)
            }
            for direction in range(0, 360, 45):
                st.session_state[f"astro_horizon_{direction}"] = float(
                    restored_horizon.get(float(direction), 0.0)
                )
            if restored_profiles:
                first_profile = next(iter(restored_profiles))
                st.session_state["astro_active_profile"] = first_profile
                st.session_state["astro_active_profile_select"] = first_profile
            st.session_state["astro_configuration_imported"] = True
        if st.session_state.pop("astro_configuration_imported", False):
            st.success("Configurazione verificata e ripristinata.")

        with st.expander("Oggetto personalizzato · RA/Dec"):
            st.caption(
                "Le coordinate celesti restano nella sessione browser e non vengono "
                "inviate a cataloghi esterni. RA è espressa in ore decimali."
            )
            with st.form("astronomy_custom_target_form", border=False):
                custom_columns = st.columns(3)
                custom_name = custom_columns[0].text_input(
                    "Nome oggetto", placeholder="Es. IC 434"
                )
                custom_ra = custom_columns[1].number_input(
                    "RA ore",
                    min_value=0.0,
                    max_value=23.999999,
                    value=5.683,
                    step=0.001,
                )
                custom_dec = custom_columns[2].number_input(
                    "Dec gradi", min_value=-90.0, max_value=90.0, value=-2.45, step=0.01
                )
                optional_columns = st.columns(3)
                custom_magnitude = optional_columns[0].text_input(
                    "Magnitudine · facoltativa", placeholder="es. 7,3"
                )
                custom_width = optional_columns[1].text_input(
                    "Larghezza arcmin · facoltativa", placeholder="es. 60"
                )
                custom_height = optional_columns[2].text_input(
                    "Altezza arcmin · facoltativa", placeholder="es. 20"
                )
                add_custom_target = st.form_submit_button("Aggiungi al planner")
            if add_custom_target:
                try:
                    personal_target = custom_target(
                        custom_name,
                        custom_ra,
                        custom_dec,
                        magnitude=_optional_decimal(custom_magnitude),
                        angular_width_arcmin=_optional_decimal(custom_width),
                        angular_height_arcmin=_optional_decimal(custom_height),
                    )
                except ValueError as exc:
                    st.error(str(exc))
                else:
                    st.session_state["astro_custom_targets"][personal_target.name] = (
                        personal_target
                    )
                    st.success(f"{personal_target.name} aggiunto alla sessione.")
            custom_targets: list[SkyTarget] = list(
                st.session_state["astro_custom_targets"].values()
            )
            if custom_targets:
                st.caption(
                    "Target personali attivi: "
                    + ", ".join(target.name for target in custom_targets)
                )
        available_targets = target_labels(custom_targets)

        active_equipment: EquipmentProfile | None = None
        framing_rotation = 0
        with st.expander("Attrezzatura e campo inquadrato"):
            st.caption(
                "Il calcolo usa dimensioni fisiche del sensore e focale effettiva. "
                "È una verifica geometrica, non una garanzia di inseguimento o qualità ottica."
            )
            with st.form("astronomy_equipment_form", border=False):
                profile_columns = st.columns(3)
                profile_name = profile_columns[0].text_input(
                    "Nome profilo", value="Setup Tripletto"
                )
                telescope_name = profile_columns[1].text_input(
                    "Telescopio", value="Tripletto 80/480"
                )
                camera_name = profile_columns[2].text_input("Camera", value="571MC-Pro")
                optical_columns = st.columns(4)
                aperture_mm = optical_columns[0].number_input(
                    "Apertura mm",
                    min_value=10.0,
                    max_value=2000.0,
                    value=80.0,
                    step=5.0,
                )
                focal_length_mm = optical_columns[1].number_input(
                    "Focale mm",
                    min_value=20.0,
                    max_value=20000.0,
                    value=480.0,
                    step=10.0,
                )
                focal_multiplier = optical_columns[2].number_input(
                    "Riduttore/Barlow ×",
                    min_value=0.1,
                    max_value=10.0,
                    value=0.8,
                    step=0.05,
                )
                pixel_size_um = optical_columns[3].number_input(
                    "Pixel µm", min_value=0.5, max_value=30.0, value=3.76, step=0.01
                )
                sensor_columns = st.columns(2)
                sensor_width_mm = sensor_columns[0].number_input(
                    "Sensore larghezza mm",
                    min_value=1.0,
                    max_value=80.0,
                    value=23.5,
                    step=0.1,
                )
                sensor_height_mm = sensor_columns[1].number_input(
                    "Sensore altezza mm",
                    min_value=1.0,
                    max_value=80.0,
                    value=15.7,
                    step=0.1,
                )
                save_equipment = st.form_submit_button("Salva profilo nella sessione")
            if save_equipment:
                try:
                    saved_profile = equipment_profile(
                        name=profile_name,
                        telescope=telescope_name,
                        camera=camera_name,
                        aperture_mm=aperture_mm,
                        focal_length_mm=focal_length_mm,
                        sensor_width_mm=sensor_width_mm,
                        sensor_height_mm=sensor_height_mm,
                        pixel_size_um=pixel_size_um,
                        focal_multiplier=focal_multiplier,
                    )
                except ValueError as exc:
                    st.error(str(exc))
                else:
                    st.session_state["astro_equipment_profiles"][saved_profile.name] = (
                        saved_profile
                    )
                    st.session_state["astro_active_profile"] = saved_profile.name
                    st.session_state["astro_active_profile_select"] = saved_profile.name
                    st.success(f"Profilo {saved_profile.name} attivo.")
            profiles: dict[str, EquipmentProfile] = st.session_state[
                "astro_equipment_profiles"
            ]
            if profiles:
                profile_names = list(profiles)
                if (
                    st.session_state.get("astro_active_profile_select")
                    not in profile_names
                ):
                    st.session_state["astro_active_profile_select"] = profile_names[0]
                selected_profile = st.selectbox(
                    "Profilo attivo",
                    options=profile_names,
                    key="astro_active_profile_select",
                )
                st.session_state["astro_active_profile"] = selected_profile
                active_equipment = profiles[selected_profile]
                view = field_of_view(active_equipment)
                field_metrics = st.columns(4)
                field_metrics[0].metric(
                    "Focale effettiva", f"{view.effective_focal_length_mm:.0f} mm"
                )
                field_metrics[1].metric(
                    "Campo", f"{view.width_deg:.2f}° × {view.height_deg:.2f}°"
                )
                field_metrics[2].metric("Diagonale", f"{view.diagonal_deg:.2f}°")
                field_metrics[3].metric(
                    "Campionamento", f"{view.image_scale_arcsec_px:.2f}″/px"
                )
                default_framing_target = next(
                    (
                        label
                        for label in available_targets
                        if label.split(" · ", 1)[0] == "M42"
                    ),
                    available_targets[0],
                )
                if (
                    st.session_state.get("astro_framing_target")
                    not in available_targets
                ):
                    st.session_state["astro_framing_target"] = default_framing_target
                framing_controls = st.columns((3, 2))
                framing_target_label = framing_controls[0].selectbox(
                    "Soggetto da inquadrare",
                    options=available_targets,
                    key="astro_framing_target",
                )
                framing_rotation = framing_controls[1].slider(
                    "Rotazione camera",
                    min_value=0,
                    max_value=175,
                    value=0,
                    step=5,
                    format="%d°",
                    key="astro_framing_rotation",
                )
                framing_target = resolve_targets(
                    [framing_target_label], custom_targets
                )[0]
                assessment = framing_assessment(
                    framing_target,
                    active_equipment,
                    rotation_deg=framing_rotation,
                )
                framing_metrics = st.columns(3)
                framing_metrics[0].metric(
                    "Soggetto",
                    (
                        "dimensioni n/d"
                        if assessment.target_width_arcmin is None
                        else (
                            f"{assessment.target_width_arcmin:.0f}′ × "
                            f"{assessment.target_height_arcmin:.0f}′"
                        )
                    ),
                )
                framing_metrics[1].metric(
                    "Riempimento massimo",
                    (
                        "n/d"
                        if assessment.width_fill_percent is None
                        else (
                            f"{max(assessment.width_fill_percent, assessment.height_fill_percent):.0f}%"
                        )
                    ),
                )
                framing_metrics[2].metric(
                    "Margine minimo",
                    (
                        "n/d"
                        if assessment.minimum_margin_arcmin is None
                        else f"{assessment.minimum_margin_arcmin:.1f}′"
                    ),
                )
                if assessment.fits is False:
                    st.warning(assessment.status)
                elif assessment.fits is True:
                    st.success(assessment.status)
                else:
                    st.info(assessment.status)
                st.plotly_chart(
                    _framing_figure(
                        framing_target,
                        active_equipment,
                        framing_rotation,
                        dark_mode,
                    ),
                    width="stretch",
                    theme=None,
                    key="astronomy_framing_preview",
                )
                st.caption(
                    "Anteprima geometrica centrata sul soggetto: il rettangolo è il "
                    "sensore, l'ellisse usa le dimensioni apparenti di catalogo. Non "
                    "è un'immagine del cielo e non simula vignettatura o distorsione."
                )
                show_sky_atlas = st.toggle(
                    "Apri atlante fotografico CDS",
                    value=False,
                    key="astro_show_aladin",
                    help=(
                        "Carica su richiesta Aladin Lite e il rilievo DSS2. Sono "
                        "trasmesse al CDS soltanto le coordinate celesti del soggetto."
                    ),
                )
                if show_sky_atlas:
                    components.html(
                        _aladin_field_html(
                            framing_target,
                            active_equipment,
                            framing_rotation,
                            dark_mode,
                        ),
                        height=550,
                        scrolling=False,
                    )
                    st.caption(
                        "Atlante interattivo fornito da CDS Aladin Lite. Nessuna "
                        "coordinata terrestre, profilo o nota viene inviato al servizio."
                    )
            else:
                st.info("Salva almeno un profilo per calcolare campo e campionamento.")

        with st.expander("Orizzonte locale · ostacoli"):
            st.caption(
                "Indica l'altezza apparente di tetti, alberi o rilievi nelle otto "
                "direzioni. La maschera manuale resta il riferimento per edifici, "
                "alberi e antenne vicini."
            )
            direction_labels = {
                0: "N",
                45: "NE",
                90: "E",
                135: "SE",
                180: "S",
                225: "SO",
                270: "O",
                315: "NO",
            }
            horizon_columns = st.columns(4)
            horizon_mask: dict[float, float] = {}
            for position, (direction, label) in enumerate(direction_labels.items()):
                value = horizon_columns[position % 4].number_input(
                    f"{label} · ostacolo °",
                    min_value=0.0,
                    max_value=60.0,
                    value=float(
                        st.session_state["astro_horizon_mask"].get(direction, 0.0)
                    ),
                    step=1.0,
                    key=f"astro_horizon_{direction}",
                )
                horizon_mask[float(direction)] = float(value)
            st.session_state["astro_horizon_mask"] = horizon_mask
            st.markdown("##### Stima indicativa da terreno satellitare")
            st.caption(
                "Su richiesta vengono inviati a Open-Meteo 97 punti attorno alla "
                "stazione per leggere Copernicus GLO-90. Il risultato resta nella "
                "sessione e non contiene coordinate; la risoluzione di circa 90 m "
                "non distingue in modo affidabile il singolo tetto o albero."
            )
            terrain_controls = st.columns((2, 3))
            sensor_height_m = terrain_controls[0].number_input(
                "Sensore sopra il suolo · m",
                min_value=0.0,
                max_value=100.0,
                value=float(st.session_state.get("astro_sensor_height_m", 2.0)),
                step=0.5,
                key="astro_sensor_height_m",
                help=(
                    "Altezza del sensore o telescopio sopra la quota del terreno. "
                    "Serve a non sovrastimare l'orizzonte vicino."
                ),
            )
            calculate_terrain = terrain_controls[1].button(
                "Stima l’orizzonte dal DEM",
                type="primary",
                width="stretch",
                key="astro_calculate_terrain_horizon",
            )
            if calculate_terrain:
                try:
                    with st.spinner("Campiono il terreno nelle 16 direzioni…"):
                        estimate = fetch_terrain_horizon(
                            CFG.latitude,
                            CFG.longitude,
                            CFG.elevation_m,
                            sensor_height_m=float(sensor_height_m),
                        )
                except TerrainHorizonError as exc:
                    st.warning(
                        f"Stima DEM non disponibile: {exc}. La maschera manuale "
                        "continua a funzionare."
                    )
                else:
                    st.session_state["astro_terrain_horizon"] = estimate
                    st.success(
                        "Profilo DEM calcolato: il planner può combinarlo con gli "
                        "ostacoli manuali."
                    )
            terrain_estimate = st.session_state.get("astro_terrain_horizon")
            if isinstance(terrain_estimate, TerrainHorizonEstimate):
                use_combined_horizon = st.toggle(
                    "Usa il massimo fra DEM e ostacoli manuali",
                    value=True,
                    key="astro_use_terrain_horizon",
                )
                combined_horizon = combine_horizon_masks(
                    horizon_mask, terrain_estimate.mask
                )
                st.plotly_chart(
                    _horizon_profile_figure(
                        horizon_mask,
                        terrain_estimate,
                        combined_horizon,
                        dark_mode,
                    ),
                    width="stretch",
                    theme=None,
                    key="astronomy_terrain_horizon",
                )
                peak_direction = max(
                    terrain_estimate.mask, key=terrain_estimate.mask.get
                )
                peak_angle = terrain_estimate.mask[peak_direction]
                peak_distance = terrain_estimate.peak_distances_km[peak_direction]
                generated = pd.Timestamp(terrain_estimate.generated_at).tz_convert(
                    CFG.local_timezone
                )
                st.caption(
                    f"Copernicus GLO-90 · risoluzione {terrain_estimate.resolution_m} m · "
                    f"massimo DEM {peak_angle:.1f}° verso azimut {peak_direction:.0f}° "
                    f"a circa {peak_distance:g} km · calcolato {generated:%d/%m %H:%M}. "
                    f"[Fonte]({terrain_estimate.source_url})"
                )
                if abs(terrain_estimate.elevation_difference_m) > 35:
                    st.info(
                        "La quota configurata e la quota media del tassello DEM "
                        "differiscono sensibilmente. Il calcolo usa il profilo "
                        "relativo del terreno, ma conviene verificare la quota della stazione."
                    )
                if use_combined_horizon:
                    horizon_mask = combined_horizon

        default_target_names = {"M31", "M42", "M45", "M13"}
        selected_targets = st.multiselect(
            "Oggetti da osservare o fotografare",
            options=available_targets,
            default=[
                label
                for label in available_targets
                if label.split(" · ", 1)[0] in default_target_names
            ],
            key="astronomy_targets",
        )
        if len(selected_targets) > 5:
            st.warning(
                "Il grafico confronta al massimo cinque soggetti per restare leggibile; "
                "sono usati i primi cinque selezionati."
            )
        plotted_targets = selected_targets[:5]
        planner_controls = st.columns(2)
        minimum_altitude = planner_controls[0].slider(
            "Altezza minima",
            min_value=10,
            max_value=60,
            value=25,
            step=5,
            format="%d°",
            key="astronomy_min_altitude",
        )
        minimum_moon_separation = planner_controls[1].slider(
            "Distanza minima dalla Luna",
            min_value=10,
            max_value=90,
            value=30,
            step=5,
            format="%d°",
            key="astronomy_moon_separation",
        )
        st.markdown("#### Piano della notte")
        now_local = pd.Timestamp.now(tz=CFG.local_timezone)
        default_night_date = (
            (now_local - pd.Timedelta(days=1)).date()
            if now_local.hour < 8
            else now_local.date()
        )
        time_controls = st.columns((2, 1, 1, 1))
        observing_date = time_controls[0].date_input(
            "Notte che inizia il",
            value=default_night_date,
            key="astronomy_plan_date",
        )
        plan_start_clock = time_controls[1].time_input(
            "Dalle",
            value=pd.Timestamp("20:00").time(),
            step=900,
            key="astronomy_plan_start",
        )
        plan_end_clock = time_controls[2].time_input(
            "Alle",
            value=pd.Timestamp("06:00").time(),
            step=900,
            key="astronomy_plan_end",
        )
        sample_minutes = time_controls[3].selectbox(
            "Dettaglio",
            options=(15, 30, 60),
            format_func=lambda value: f"{value} min",
            key="astronomy_plan_sample",
        )
        secondary_metric = st.selectbox(
            "Pannello inferiore del grafico",
            options=("Qualità", "Massa d'aria", "Distanza Luna"),
            key="astronomy_plan_secondary_metric",
            help=(
                "La magnitudine non varia durante la notte: resta nei tooltip e "
                "nella tabella, invece di essere disegnata come una curva fuorviante."
            ),
        )
        try:
            plan_start, plan_end = local_night_window(
                observing_date,
                plan_start_clock,
                plan_end_clock,
                CFG.local_timezone,
            )
            detailed_plan = night_plan_tracks(
                astro,
                CFG,
                plotted_targets,
                start=plan_start,
                end=plan_end,
                minimum_altitude=minimum_altitude,
                minimum_moon_separation=minimum_moon_separation,
                custom_targets=custom_targets,
                horizon_mask=horizon_mask,
                sample_minutes=sample_minutes,
            )
        except ValueError as exc:
            st.error(str(exc))
            detailed_plan = pd.DataFrame()
        target_plan = summarize_night_plan(
            detailed_plan,
            equipment=active_equipment,
            custom_targets=custom_targets,
            rotation_deg=framing_rotation,
        )
        if target_plan.empty:
            st.info("Seleziona almeno un oggetto per costruire il piano della notte.")
        else:
            st.plotly_chart(
                _night_plan_figure(
                    detailed_plan,
                    dark_mode,
                    minimum_altitude,
                    secondary_metric,
                ),
                width="stretch",
                theme=None,
                key="astronomy_night_plan_chart",
            )
            coverage = float(detailed_plan["weather_available"].mean() * 100)
            if coverage == 0:
                st.warning(
                    "La finestra non è coperta dalle previsioni disponibili: altezza, "
                    "azimut, massa d'aria e Luna restano calcolati; la qualità è solo geometrica."
                )
            elif coverage < 100:
                st.info(
                    f"Copertura meteo del piano: {coverage:.0f}%. Nei tratti mancanti "
                    "restano disponibili i calcoli astronomici."
                )
            else:
                st.caption(
                    "Copertura meteo completa. La linea rossa tratteggiata, quando "
                    f"presente, indica l'ora attuale in {CFG.local_timezone}."
                )
            st.download_button(
                "Scarica il piano dettagliato (CSV)",
                data=night_plan_csv(detailed_plan),
                file_name=(
                    f"piano-astronomico-{pd.Timestamp(observing_date):%Y-%m-%d}.csv"
                ),
                mime="text/csv",
                width="stretch",
            )
            planner_table = pd.DataFrame(
                {
                    "Oggetto": target_plan["target"] + " · " + target_plan["name"],
                    "Tipo": target_plan["category"],
                    "Mag.": target_plan["magnitude"],
                    "Finestra migliore": target_plan["best_time"].map(_hour_label),
                    "Altezza max °": target_plan["max_altitude"],
                    "Azimut al meglio °": target_plan["best_azimuth"],
                    "Massa d'aria min": target_plan["minimum_airmass"],
                    "Score": target_plan["planner_score"],
                    "Distanza Luna °": target_plan["moon_separation"],
                    "Ore utili": target_plan["visible_hours"],
                    "Copertura meteo %": target_plan["weather_coverage"],
                    "Inquadratura": target_plan["framing"],
                    "Esito": target_plan["status"],
                }
            )
            planner_styler = _style_status_table(
                planner_table, dark_mode, "Esito"
            ).format(
                {
                    "Mag.": "{:.1f}",
                    "Massa d'aria min": "{:.2f}",
                    "Ore utili": "{:.1f}",
                    **{
                        column: "{:.0f}"
                        for column in (
                            "Altezza max °",
                            "Azimut al meglio °",
                            "Score",
                            "Distanza Luna °",
                            "Copertura meteo %",
                        )
                    },
                },
                na_rep="—",
            )
            render_styled_table(
                planner_styler,
                height=430,
            )
            schedulable = target_plan[target_plan["best_time"].notna()].copy()
            if not schedulable.empty:
                with st.expander("Calendario e diario della sessione"):
                    target_options = schedulable["target"].astype(str).tolist()
                    calendar_target = st.selectbox(
                        "Finestra da usare",
                        options=target_options,
                        format_func=lambda name: (
                            f"{name} · "
                            + str(
                                schedulable.loc[
                                    schedulable["target"].eq(name), "name"
                                ].iloc[0]
                            )
                        ),
                        key="astro_calendar_target",
                    )
                    selected_plan = schedulable[
                        schedulable["target"].eq(calendar_target)
                    ].iloc[0]
                    session_duration = st.slider(
                        "Durata sessione",
                        min_value=30,
                        max_value=360,
                        value=120,
                        step=30,
                        format="%d min",
                        key="astro_session_duration",
                    )
                    calendar_description = (
                        f"Score {selected_plan['planner_score']:.0f}/100; "
                        f"altezza massima {selected_plan['max_altitude']:.0f}°; "
                        f"Luna {selected_plan['moon_separation']:.0f}°."
                    )
                    calendar_payload = observing_calendar_ics(
                        calendar_target,
                        selected_plan["best_time"],
                        duration_minutes=session_duration,
                        timezone_name=CFG.local_timezone,
                        description=calendar_description,
                    )
                    st.download_button(
                        "Aggiungi la finestra al calendario (.ics)",
                        data=calendar_payload,
                        file_name=f"osservazione-{_filename_slug(calendar_target)}.ics",
                        mime="text/calendar",
                        width="stretch",
                    )
                    with st.form("astronomy_session_log_form", border=False):
                        log_status = st.selectbox(
                            "Stato diario",
                            options=("Pianificata", "Completata", "Annullata"),
                        )
                        log_notes = st.text_input(
                            "Nota facoltativa",
                            max_chars=240,
                            placeholder="Filtri, esposizioni, esito",
                        )
                        add_to_log = st.form_submit_button("Aggiungi al diario")
                    if add_to_log:
                        st.session_state["astro_session_log"].append(
                            {
                                "target": calendar_target,
                                "planned_start": selected_plan["best_time"].isoformat(),
                                "duration_minutes": session_duration,
                                "status": log_status,
                                "score": round(
                                    float(selected_plan["planner_score"]), 1
                                ),
                                "equipment": (
                                    active_equipment.name
                                    if active_equipment is not None
                                    else ""
                                ),
                                "notes": log_notes.strip(),
                            }
                        )
                        st.success("Voce aggiunta al diario della sessione browser.")
                    diary = st.session_state["astro_session_log"]
                    if diary:
                        diary_table = pd.DataFrame(diary).rename(
                            columns={
                                "target": "Oggetto",
                                "planned_start": "Inizio",
                                "duration_minutes": "Durata min",
                                "status": "Stato",
                                "score": "Score",
                                "equipment": "Attrezzatura",
                                "notes": "Note",
                            }
                        )
                        render_styled_table(_base_table_style(diary_table, dark_mode))
                        st.download_button(
                            "Scarica diario CSV",
                            data=observing_log_csv(diary),
                            file_name="diario-osservazioni.csv",
                            mime="text/csv",
                        )
                    st.caption(
                        "Il diario resta privato nella sessione browser; scarica il CSV "
                        "prima di chiudere. Il file ICS usa UTC standard e si apre "
                        f"all'ora corretta {CFG.local_timezone}."
                    )

        with st.expander("Esporta o ripristina la configurazione"):
            configuration_payload = planner_configuration_json(
                st.session_state["astro_equipment_profiles"].values(),
                custom_targets,
                st.session_state["astro_horizon_mask"],
            )
            st.download_button(
                "Esporta configurazione planner (JSON)",
                data=configuration_payload,
                file_name="configurazione-astronomia-pro.json",
                mime="application/json",
                width="stretch",
            )
            imported_configuration = st.file_uploader(
                "Ripristina un JSON esportato dal planner",
                type=("json",),
                accept_multiple_files=False,
                key="astro_configuration_upload",
            )
            if imported_configuration is not None and st.button(
                "Importa configurazione verificata",
                key="astro_configuration_import",
                width="stretch",
            ):
                try:
                    restored_profiles, restored_targets, restored_horizon = (
                        parse_planner_configuration(imported_configuration.getvalue())
                    )
                except (TypeError, ValueError) as exc:
                    st.error(str(exc))
                else:
                    st.session_state["astro_pending_configuration"] = (
                        restored_profiles,
                        restored_targets,
                        restored_horizon,
                    )
                    st.rerun()
        st.caption(
            "Posizioni e separazione dalla Luna sono calcoli astronomici orari; lo "
            "score combina altezza, meteo, nuvole, luminosità lunare e maschera locale. "
            "Dimensioni catalogo e inquadratura sono indicative: verifica sempre "
            "limiti della montatura e campo reale prima di iniziare. Riferimenti: "
            "[Hubble Messier Catalog](https://science.nasa.gov/mission/hubble/science/explore-the-night-sky/hubble-messier-catalog/) "
            "e [SIMBAD/CDS](https://simbad.cds.unistra.fr/simbad/)."
        )

        windows = best_observing_windows(astro)
        if windows.empty:
            st.warning(
                "Nessuna finestra di almeno 2 ore con punteggio ≥ 65 nei prossimi giorni."
            )
        else:
            cols = st.columns(min(3, len(windows)))
            for position, (_, window) in enumerate(windows.head(3).iterrows()):
                end_time = pd.to_datetime(window["end"], errors="coerce")
                end_label = "—" if pd.isna(end_time) else f"{end_time:%H:%M}"
                cols[position].metric(
                    f"{_hour_label(window['start'])}–{end_label}",
                    f"{window['score']:.0f}/100",
                    f"{int(window['hours'])} h · nuvole {window['clouds']:.0f}%",
                )
        night = astro[astro["is_night"]].copy()
        figure = make_subplots(specs=[[{"secondary_y": True}]])
        figure.add_trace(
            go.Scatter(
                x=plotly_local_datetimes(night["local_time"], CFG.local_timezone),
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
                        x=plotly_local_datetimes(
                            night["local_time"], CFG.local_timezone
                        ),
                        y=night[column],
                        name=f"··· {name}",
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
            margin={"l": 10, "r": 10, "t": 62, "b": 10},
        )
        st.plotly_chart(_style_plotly(figure, dark_mode), width="stretch", theme=None)
        st.caption(
            "Astronomia Pro combina nuvole, pioggia, vento, visibilità e rischio "
            "condensa con proxy di trasparenza/stabilità derivati da CAPE, umidità "
            "a 700 hPa e vento a 300 hPa. Non è una misura strumentale del seeing."
        )

        if expert_mode and not night.empty:
            pro_table = pd.DataFrame(
                {
                    "Ora": night["local_time"].map(_hour_label),
                    "Score": _numeric_series(night, "astro_score"),
                    "Trasparenza": _numeric_series(night, "transparency_proxy"),
                    "Stabilità": _numeric_series(night, "stability_proxy"),
                    "Condensa %": _numeric_series(night, "dew_risk"),
                    "CAPE J/kg": _numeric_series(night, "cape_j_kg"),
                    "Jet 300 hPa km/h": _numeric_series(night, "wind_300hpa_kmh"),
                    "UR 700 hPa %": _numeric_series(night, "humidity_700hpa"),
                    "Zero termico m": _numeric_series(night, "freezing_level_m"),
                }
            ).head(36)
            st.markdown("#### Diagnostica atmosferica · prossime ore notturne")
            render_styled_table(
                _style_astronomy_table(pro_table.round(0), dark_mode), height=430
            )

        if not events.empty:
            table = pd.DataFrame(
                {
                    "Data": pd.to_datetime(events["date"]).map(_day_label),
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
            render_styled_table(
                _base_table_style(table, dark_mode),
            )

with tab_radar:
    st.markdown(
        '<div class="section-kicker">Osservazione ufficiale locale</div>',
        unsafe_allow_html=True,
    )
    st.subheader("Radar DPC e fulmini vicino alla stazione")
    dpc_live = None
    dpc_archive = dpc_radar_archive_data(active_station_id)
    if not CFG.dpc_radar_enabled:
        st.caption("Il radar puntuale DPC è disattivato; le mappe restano disponibili.")
    elif (
        st.session_state.get("main_tab") == "Radar"
        and active_station_id == CFG.station_id
    ):
        try:
            dpc_live = dpc_radar_live_data(CFG)
        except DpcRadarError as exc:
            if dpc_archive.empty:
                st.info(f"Radar DPC temporaneamente non disponibile ({exc}).")
            else:
                st.info(
                    f"Uso l'ultimo riassunto DPC archiviato ({exc}); nessuna immagine "
                    "radar nazionale viene conservata."
                )
        snapshot = (
            dpc_live
            if dpc_live is not None
            else None
            if dpc_archive.empty
            else dpc_archive.iloc[0]
        )
        if snapshot is not None:

            def snapshot_value(name: str, default: Any = None) -> Any:
                return (
                    getattr(snapshot, name, default)
                    if dpc_live is not None
                    else snapshot.get(name, default)
                )

            radar_live, radar_age = _timestamp_live_state(
                snapshot_value("observed_at"),
                max_age_minutes=max(15, CFG.dpc_radar_refresh_minutes * 3),
            )
            st.markdown(
                '<div class="freshness-row"><span class="freshness-label">Radar DPC</span>'
                + _live_badge_html(radar_live, radar_age)
                + "</div>",
                unsafe_allow_html=True,
            )
            metrics = st.columns(4)
            metrics[0].metric(
                "Pioggia sul punto",
                _number(snapshot_value("sri_point_mm_h"), 1, " mm/h"),
                f"max ritaglio {_number(snapshot_value('sri_max_mm_h'), 1, ' mm/h')}",
            )
            metrics[1].metric(
                "Riflettività sul punto",
                _number(snapshot_value("vmi_point_dbz"), 1, " dBZ"),
                f"max ritaglio {_number(snapshot_value('vmi_max_dbz'), 1, ' dBZ')}",
            )
            lightning_observed = snapshot_value("lightning_observed_at")
            if _valid_timestamp(lightning_observed):
                metrics[2].metric(
                    "Fulmini entro 25 km",
                    _number(snapshot_value("lightning_25km"), 0),
                    f"entro 10 km {_number(snapshot_value('lightning_10km'), 0)}",
                )
                metrics[3].metric(
                    "Fulmine più vicino",
                    _number(snapshot_value("nearest_lightning_km"), 1, " km"),
                    f"entro 50 km {_number(snapshot_value('lightning_50km'), 0)}",
                )
            else:
                metrics[2].metric("Fulmini entro 25 km", "n/d", "frame non pubblicato")
                metrics[3].metric("Fulmine più vicino", "n/d", "nessun dato associato")
            observed = snapshot_value("observed_at")
            sri_observed = snapshot_value("sri_observed_at")
            vmi_observed = snapshot_value("vmi_observed_at")
            lightning_label = (
                _local_time(lightning_observed)
                if _valid_timestamp(lightning_observed)
                else "non disponibile per questo frame"
            )
            st.caption(
                f"Dipartimento della Protezione Civile · SRI {_local_time(sri_observed)} · "
                f"VMI {_local_time(vmi_observed)} · fulmini {lightning_label}. "
                "L'app scarica soltanto il tassello che contiene la stazione e il piccolo "
                "ritaglio adiacente; nel database salva solo questi valori riassuntivi. "
                "[Apri la mappa radar ufficiale](https://mappe.protezionecivile.gov.it/it/mappe-e-dashboard-rischi/piattaforma-radar/)."
            )
            if dpc_live is not None and dpc_live.sri_window is not None:
                window_figure = go.Figure(
                    go.Heatmap(
                        z=dpc_live.sri_window,
                        colorscale=[
                            [0.0, "#f8fafc"],
                            [0.02, "#bae6fd"],
                            [0.12, "#38bdf8"],
                            [0.35, "#22c55e"],
                            [0.60, "#facc15"],
                            [0.80, "#f97316"],
                            [1.0, "#dc2626"],
                        ],
                        zmin=0,
                        zmax=max(5.0, float(np.nanmax(dpc_live.sri_window))),
                        colorbar={"title": "mm/h"},
                        hovertemplate="Intensità %{z:.1f} mm/h<extra></extra>",
                    )
                )
                center = CFG.dpc_radar_crop_radius
                window_figure.add_trace(
                    go.Scatter(
                        x=[center],
                        y=[center],
                        mode="markers",
                        marker={"size": 11, "color": "#111827", "symbol": "x"},
                        name="Stazione",
                        hoverinfo="skip",
                    )
                )
                window_figure.update_layout(
                    title="Ritaglio locale SRI · precipitazione osservata",
                    height=390,
                    margin={"l": 10, "r": 10, "t": 55, "b": 10},
                    showlegend=False,
                )
                window_figure.update_xaxes(visible=False)
                window_figure.update_yaxes(visible=False, autorange="reversed")
                st.plotly_chart(
                    _style_plotly(window_figure, dark_mode),
                    width="stretch",
                    theme=None,
                )
    else:
        st.caption("Apri la scheda Radar per leggere il prodotto DPC puntuale.")

    st.markdown("#### Tendenza di arrivo delle eco · fonte secondaria")
    if not CFG.radar_nowcast_enabled:
        st.caption("Il nowcast RainViewer è disattivato.")
    elif st.session_state.get("main_tab") == "Radar":
        try:
            local_nowcast = radar_nowcast_data(CFG.latitude, CFG.longitude)
        except RadarNowcastError as exc:
            st.info(f"Nowcast temporaneamente non disponibile ({exc}).")
        else:
            if local_nowcast.status == "rain":
                st.warning(
                    "☔ "
                    + local_nowcast.message
                    + (
                        " Probabilità eco orientativa "
                        f"{local_nowcast.echo_probability:.0f}%."
                        if local_nowcast.echo_probability is not None
                        else ""
                    )
                )
            elif local_nowcast.status == "dry":
                st.success("◎ " + local_nowcast.message)
            else:
                st.info("ⓘ " + local_nowcast.message)
            st.caption(
                f"RainViewer · generato {_local_time(local_nowcast.generated_at)} · "
                f"{local_nowcast.frame_count} fotogrammi · affidabilità {local_nowcast.confidence}. "
                "È un’indicazione di movimento delle eco, non un’allerta."
            )
    else:
        st.caption("Apri la scheda Radar per calcolare la tendenza delle eco.")

    st.divider()
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
        f"&lat={PUBLIC_MAP_LAT:.2f}&lon={PUBLIC_MAP_LON:.2f}"
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
        f"&overlay=clouds&product=ecmwf&level=surface&lat={PUBLIC_MAP_LAT:.2f}"
        f"&lon={PUBLIC_MAP_LON:.2f}"
    )
    st.iframe(windy_url, height=560)
    st.caption(
        "Mappa previsionale Windy/ECMWF: usa la linea temporale interna per spostarti a +1, +2 e +3 ore. "
        "Il radar sopra mostra invece precipitazioni realmente osservate."
    )

with tab_system:
    st.markdown(
        '<div class="section-kicker">Affidabilità e continuità</div>',
        unsafe_allow_html=True,
    )
    st.subheader("Stato del sistema")
    completeness = completeness_data()
    sources = source_status_data()
    if not CFG.cfr_observations_enabled and "source" in sources:
        sources = sources[sources["source"] != "cfr_lazio"].copy()
    core_statuses = {health.get("station_status"), health.get("forecast_status")}
    overall_label = (
        "Operativo"
        if core_statuses == {"online"}
        else "Da controllare"
        if "offline" not in core_statuses
        else "Problema rilevato"
    )
    system_columns = st.columns(4)
    system_columns[0].metric(
        "Copertura stazione · 24 h",
        f"{completeness['coverage']:.1f}%",
        f"{completeness['observed']} / {completeness['expected']} intervalli",
    )
    largest_gap = completeness.get("largest_gap_minutes")
    system_columns[1].metric(
        "Buco massimo · 24 h",
        "—" if largest_gap is None else f"{largest_gap:.0f} min",
        "obiettivo ≤ 10 min",
        delta_color="off",
    )
    system_columns[2].metric(
        "Campioni segnalati · 24 h",
        str(completeness["anomalies"]),
        "controlli fisici e temporali",
        delta_color="inverse",
    )
    system_columns[3].metric(
        "Salute essenziale",
        overall_label,
        "Ecowitt + previsione combinata",
        delta_color="off",
    )
    st.caption(
        "Schema dati v8 · migrazioni additive · controllo Render continuo + verifica "
        "indipendente GitHub ogni 30 minuti."
    )
    status_labels = {
        "online": "Operativa",
        "delayed": "In ritardo",
        "cached": "Archivio disponibile",
        "external_unavailable": "Fonte esterna indisponibile",
        "offline": "Non disponibile",
        "waiting": "In attesa",
        "manual": "Manuale · mai eseguito",
        "scheduled": "Pianificata",
        "disabled": "Disattivata",
    }
    if not sources.empty:
        protection_labels = {
            "online": "Operativo",
            "delayed": "Ultima verifica valida",
            "cached": "Ultima verifica valida",
            "scheduled": "Pianificato",
            "waiting": "In attesa",
            "offline": "Non disponibile",
            "external_unavailable": "Non disponibile",
            "disabled": "Disattivato",
        }

        def _protection_summary(source_name: str) -> tuple[str, str]:
            selected = sources[sources["source"].eq(source_name)]
            if selected.empty:
                return "In attesa", "nessuna telemetria"
            row = selected.iloc[0]
            state = str(row.get("display_status") or "waiting")
            success = _local_time(row.get("last_success_at"))
            detail = (
                "prima esecuzione programmata"
                if success == "—" and state == "scheduled"
                else f"ultimo successo {success}"
                if success != "—"
                else "nessun esito valido"
            )
            return protection_labels.get(state, "Da controllare"), detail

        health_label, health_detail = _protection_summary("system_health")
        backup_label, backup_detail = _protection_summary("github_backup")
        data_sources = sources[
            sources["enabled"].fillna(False) & sources["category"].ne("protezione")
        ]
        usable_sources = data_sources[
            data_sources["display_status"].isin({"online", "cached", "delayed"})
        ]
        protection_columns = st.columns(3)
        protection_columns[0].metric(
            "Controllo automatico", health_label, health_detail, delta_color="off"
        )
        protection_columns[1].metric(
            "Backup cloud cifrato", backup_label, backup_detail, delta_color="off"
        )
        protection_columns[2].metric(
            "Fonti utilizzabili",
            f"{len(usable_sources)} / {len(data_sources)}",
            "fallback inclusi; dettagli sotto",
            delta_color="off",
        )
    st.markdown("#### Fonti e processi indipendenti")
    st.caption(
        "Un errore di una fonte non interrompe le altre. Lo stato considera sia "
        "l'ultimo tentativo sia la frequenza attesa di aggiornamento."
    )
    if sources.empty:
        st.info("Lo stato dettagliato comparirà dopo la prossima acquisizione.")
    else:
        category_labels = {
            "misure": "Misure",
            "previsioni": "Previsioni",
            "probabilistica": "Probabilistica",
            "riferimenti": "Riferimenti",
            "ambiente": "Ambiente",
            "elaborazione": "Elaborazione",
            "protezione": "Protezione",
            "sicurezza": "Sicurezza",
        }
        readable_status = sources["display_status"].map(status_labels)
        scheduled_backup = sources["source"].isin(
            {"database_backup", "github_backup"}
        ) & sources["display_status"].eq("scheduled")
        readable_status.loc[scheduled_backup] = "Pianificata · ore 22:00"
        scheduled_arsial = sources["source"].eq("arsial_siarl") & sources[
            "display_status"
        ].eq("scheduled")
        readable_status.loc[scheduled_arsial] = (
            f"Verifica automatica · ogni {CFG.arsial_probe_hours} h"
        )
        cached_health = sources["source"].eq("system_health") & sources[
            "display_status"
        ].eq("cached")
        readable_status.loc[cached_health] = "Ultima verifica valida · Render attivo"
        source_table = pd.DataFrame(
            {
                "Componente": sources["label"],
                "Categoria": sources["category"].map(category_labels),
                "Stato": readable_status,
                "Ultimo tentativo": sources["last_attempt_at"].map(_local_time),
                "Ultimo successo": sources["last_success_at"].map(_local_time),
                "Ultimo dato/copertura": sources["last_observation_at"].map(
                    _local_time
                ),
                "Età successo": sources["age_minutes"].map(_age_text),
                "Latenza": pd.to_numeric(sources["latency_ms"], errors="coerce").map(
                    lambda value: (
                        "—"
                        if pd.isna(value) or value <= 0
                        else f"{value / 1000:.1f} s"
                        if value >= 1000
                        else f"{value:.0f} ms"
                    )
                ),
                "Righe": pd.to_numeric(sources["rows_received"], errors="coerce")
                .fillna(0)
                .astype("Int64"),
                "Errori consecutivi": pd.to_numeric(
                    sources["consecutive_failures"], errors="coerce"
                )
                .fillna(0)
                .astype("Int64"),
                "Ultimo errore": sources["last_error"].replace("", "—"),
                "Continuità / fallback": sources["continuity"],
            }
        )
        if not expert_mode:
            source_table = source_table[
                [
                    "Componente",
                    "Categoria",
                    "Stato",
                    "Ultimo successo",
                    "Ultimo dato/copertura",
                    "Continuità / fallback",
                ]
            ]
        render_color_legend("status")
        render_styled_table(
            _style_status_table(source_table, dark_mode, "Stato"),
            height=470,
        )
        if not expert_mode:
            st.caption(
                "Passa alla modalità Esperta per latenza, righe, tentativi ed errori tecnici."
            )
        arsial_state = sources.loc[
            sources["source"].eq("arsial_siarl"), "display_status"
        ]
        if not arsial_state.empty and arsial_state.iloc[0] == "external_unavailable":
            st.warning(
                "Il portale pubblico ARSIAL/SIARL al momento non restituisce un "
                "export orario leggibile. Il connettore verifica automaticamente ogni "
                f"{CFG.arsial_probe_hours} ore e si riattiva al primo campione valido; "
                "Ecowitt, METAR "
                "e previsioni continuano normalmente. Dopo il primo campione valido, "
                f"l'archivio resta utilizzabile fino a {CFG.arsial_cache_hours} ore "
                "durante eventuali nuovi disservizi."
            )
        elif not arsial_state.empty and arsial_state.iloc[0] == "scheduled":
            st.info(
                "La verifica automatica ARSIAL/SIARL è attiva: il primo sondaggio "
                f"utile verrà eseguito entro {CFG.arsial_probe_hours} ore. CFR Lazio "
                "via MeteoHub resta il riferimento regionale operativo."
            )
        elif not CFG.arsial_polling_enabled:
            st.info(
                "ARSIAL/SIARL è disattivata dalla configurazione. CFR Lazio via "
                "MeteoHub resta il riferimento regionale operativo."
            )

    st.markdown("#### Percorso V4 modulare")
    registry = feature_registry(CFG)
    registry_table = pd.DataFrame(
        {
            "Funzione": [item.title for item in registry],
            "Fase": [item.phase for item in registry],
            "Stato": [
                "Attiva" if item.enabled else "Predisposta · non attiva"
                for item in registry
            ],
            "Archivio / integrazione": [item.storage for item in registry],
        }
    )
    render_styled_table(
        _style_status_table(registry_table, dark_mode, "Stato"),
    )
    st.caption(
        "Climatologia locale, pollini misurati POLLnet, bollettini DPC/Regione Lazio "
        "e modalità semplice/esperta sono moduli V4.3 indipendenti. Le fonti esterne "
        "restano non bloccanti; gli avvisi GitHub riguardano solo guasti operativi, "
        "non notifiche meteo personali."
    )

    st.markdown("#### Diagnostica Ecowitt")
    diagnostic_station_id = active_station_id
    if len(station_profiles) > 1:
        diagnostic_station_id = st.selectbox(
            "Stazione da diagnosticare",
            options=station_profiles["station_id"].astype(str).tolist(),
            index=(
                station_profiles["station_id"]
                .astype(str)
                .tolist()
                .index(active_station_id)
                if active_station_id
                in station_profiles["station_id"].astype(str).tolist()
                else 0
            ),
            format_func=lambda value: _station_label(station_profiles, value),
            key="system_diagnostic_station",
        )
    sensor_diagnostics, device_telemetry, diagnostic_summary = ecowitt_diagnostic_data(
        diagnostic_station_id
    )
    diagnostic_columns = st.columns(4)
    diagnostic_columns[0].metric(
        "Stato sensori",
        {
            "online": "Operativi",
            "warning": "Da controllare",
            "delayed": "In ritardo",
            "offline": "Non disponibili",
        }.get(diagnostic_summary.status, "Da controllare"),
    )
    diagnostic_columns[1].metric("Sensori online", str(diagnostic_summary.online))
    diagnostic_columns[2].metric(
        "Avvisi / ritardi", str(diagnostic_summary.warning), delta_color="inverse"
    )
    diagnostic_columns[3].metric(
        "Copertura media · 24 h", f"{diagnostic_summary.average_coverage:.1f}%"
    )
    diagnostic_labels = {
        "online": "Operativo",
        "warning": "Dati sospetti",
        "delayed": "In ritardo",
        "offline": "Non disponibile",
    }
    diagnostic_table = pd.DataFrame(
        {
            "Sensore": sensor_diagnostics["sensor"],
            "Stato": sensor_diagnostics["status"].map(diagnostic_labels),
            "Ultimo dato": sensor_diagnostics["last_time"].map(_local_time),
            "Età": sensor_diagnostics["age_minutes"].map(_age_text),
            "Valore": sensor_diagnostics.apply(
                lambda row: (
                    "—"
                    if pd.isna(row["last_value"])
                    else f"{row['last_value']:.1f} {row['unit']}"
                ),
                axis=1,
            ),
            "Copertura 24 h": sensor_diagnostics["coverage"].map(
                lambda value: f"{value:.1f}%"
            ),
            "Buco massimo": sensor_diagnostics["largest_gap_minutes"].map(
                lambda value: "—" if pd.isna(value) else f"{value:.0f} min"
            ),
            "Campioni segnalati": sensor_diagnostics["quality_flags"],
            "Motivo": sensor_diagnostics["quality_note"],
        }
    )
    if not expert_mode:
        diagnostic_table = diagnostic_table[
            [
                "Sensore",
                "Stato",
                "Ultimo dato",
                "Valore",
                "Copertura 24 h",
                "Motivo",
            ]
        ]
    render_styled_table(
        _style_status_table(diagnostic_table, dark_mode, "Stato"), height=390
    )
    if device_telemetry.empty:
        st.caption(
            "Batterie e segnale compariranno quando l'API cloud Ecowitt li espone. "
            "La diagnostica non salva MAC, chiavi né risposte API complete."
        )
    else:
        telemetry_labels = {
            "ok": "Regolare",
            "warning": "Da controllare",
            "critical": "Problema",
            "unknown": "Da interpretare",
        }

        def telemetry_value(row: pd.Series) -> str:
            if pd.notna(row.get("value")):
                unit = row.get("unit")
                rendered_unit = "" if pd.isna(unit) else str(unit)
                return f"{float(row['value']):.2f} {rendered_unit}".strip()
            if row.get("metric") == "battery" and row.get("status") == "ok":
                return "Normale · carica"
            if row.get("metric") == "battery":
                return "Stato anomalo"
            return "—"

        telemetry_table = pd.DataFrame(
            {
                "Dispositivo": device_telemetry["sensor"].map(telemetry_sensor_label),
                "Parametro": device_telemetry["metric"].map(
                    {"battery": "Batteria", "signal": "Segnale"}
                ),
                "Valore": device_telemetry.apply(telemetry_value, axis=1),
                "Stato": device_telemetry["status"].map(telemetry_labels),
                "Aggiornato": device_telemetry["observed_at"].map(_local_time),
            }
        )
        render_styled_table(_style_status_table(telemetry_table, dark_mode, "Stato"))
        st.caption(
            "Per il Sensor Array la risposta Ecowitt “Normal/Normale” significa "
            "batteria carica ed è verde; qualsiasi altro stato testuale viene "
            "evidenziato come problema. Le tensioni numeriche usano le soglie del sensore."
        )
        if not device_telemetry["metric"].eq("signal").any():
            st.caption("Il segnale radio non è esposto dalla risposta cloud corrente.")

    st.markdown("#### Qualità delle osservazioni")
    recent_quality = (
        station[station["time"] >= pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=24)]
        if not station.empty
        else pd.DataFrame()
    )
    if recent_quality.empty:
        st.caption("Nessuna osservazione nelle ultime 24 ore.")
    else:
        quality_counts = (
            recent_quality.get(
                "data_quality", pd.Series("ok", index=recent_quality.index)
            )
            .fillna("ok")
            .value_counts()
            .rename_axis("Esito")
            .reset_index(name="Campioni")
        )
        render_styled_table(
            _style_status_table(quality_counts, dark_mode, "Esito"),
        )
        st.caption(
            "range_filtered rimuove solo il valore fisicamente impossibile; spike e "
            "stuck conservano la misura ma la marcano come sospetta; estimated_rain "
            "indica pioggia ricavata da intensità × intervallo."
        )

    st.markdown("#### Backup e ultime esecuzioni")
    st.caption(
        "Ogni giorno alle 22:00 Europe/Rome GitHub Actions crea lo ZIP portatile, "
        "verifica conteggi e checksum, lo cifra prima dell'upload e lo conserva per "
        "30 giorni. Il database non viene mai pubblicato nel repository "
        "né caricato in chiaro; la procedura funziona anche con il PC locale spento. "
        "I backup/PITR eventualmente inclusi nel piano PostgreSQL Render restano una "
        "protezione indipendente aggiuntiva."
    )
    logs = log_data()
    if not logs.empty:
        safe_logs = logs[
            [
                column
                for column in (
                    "started_at",
                    "finished_at",
                    "component",
                    "status",
                    "rows_written",
                )
                if column in logs
            ]
        ].copy()
        for column in ("started_at", "finished_at"):
            if column in safe_logs:
                safe_logs[column] = safe_logs[column].map(_local_time)
        safe_logs = safe_logs.rename(
            columns={
                "started_at": "Avvio",
                "finished_at": "Fine",
                "component": "Componente",
                "status": "Stato",
                "rows_written": "Righe",
            }
        )
        render_styled_table(
            _style_status_table(safe_logs, dark_mode, "Stato"),
        )

reference_attribution = " · riferimenti: Aviation Weather"
if CFG.arsial_polling_enabled:
    reference_attribution += " + [ARSIAL/SIARL](https://siarl.arsial.it/) · auto"
if CFG.cfr_observations_enabled:
    reference_attribution += " + CFR Lazio"
st.caption(
    "Meteo V4 · Open‑Meteo + OpenWeather · correzione locale sulla stazione"
    + reference_attribution
    + f" · ultimo dato reale {_local_time(health.get('station_time'))}"
)
