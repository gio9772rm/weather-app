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
from plotly.subplots import make_subplots

from air_quality import AirQualityError, AirQualityForecast, fetch_air_quality
from astro_weather import (
    astronomy_events,
    best_observing_windows,
    daily_astronomy_summary,
    prepare_astronomy,
)
from chart_data import (
    clip_forecast,
    merge_intervals,
    missing_forecast_segments,
    plotly_utc_datetime,
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
    daily_forecast,
    data_completeness_snapshot,
    health_snapshot,
    load_climate_normals,
    load_ensemble,
    load_forecast,
    load_forecast_history,
    load_forecast_reliability,
    load_measured_pollen,
    load_observed_air,
    load_official_alerts,
    load_official_station_status,
    load_provider_scores,
    load_recent_logs,
    load_reference_scores,
    load_source_health,
    load_station,
)
from feature_registry import features as feature_registry
from forecast_change import ForecastChangeSummary, summarize_forecast_change
from light_pollution import (
    LightPollutionError,
    LightPollutionEstimate,
    fetch_light_pollution,
)
from radar_nowcast import RadarNowcastError, fetch_radar_nowcast
from rain_consistency import reportable_rain_amount, reportable_rain_series
from share_card import ShareCardSummary, render_share_card
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
  --ink:#10243d; --muted:#64748b; --subtle:#475569; --line:rgba(148,163,184,.28);
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
.current-grid { display:grid; grid-template-columns:repeat(6,minmax(0,1fr)); gap:.65rem; margin:.8rem 0 1rem; }
.current-card { min-width:0; min-height:118px; border:1px solid var(--line); border-radius:18px; padding:.82rem .88rem;
  background:var(--surface); box-shadow:var(--shadow); }
.current-label { color:var(--muted); font-size:.7rem; font-weight:700; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
.current-value { color:var(--ink); font-size:clamp(1.28rem,2.2vw,1.85rem); line-height:1.1; font-weight:650;
  letter-spacing:-.045em; margin:.46rem 0 .35rem; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
.current-detail { display:inline-block; max-width:100%; color:var(--subtle); background:var(--surface-soft); border-radius:999px;
  padding:.23rem .46rem; font-size:.66rem; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
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
.insight-title,.activity-title,.air-title { color:var(--muted); font-size:.72rem; font-weight:700; text-transform:uppercase; letter-spacing:.045em; }
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
@media(max-width:1050px){.forecast-grid{grid-template-columns:repeat(4,minmax(140px,1fr));}}
@media(max-width:1050px){.current-grid{grid-template-columns:repeat(3,minmax(0,1fr));}}
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
}
</style>
""",
    unsafe_allow_html=True,
)


@st.cache_data(ttl=60, show_spinner=False)
def station_data(hours: int) -> pd.DataFrame:
    return load_station(hours)


@st.cache_data(ttl=180, show_spinner=False)
def forecast_data() -> pd.DataFrame:
    return load_forecast()


@st.cache_data(ttl=180, show_spinner=False)
def forecast_history_data() -> pd.DataFrame:
    return load_forecast_history(hours=48, emissions=2)


@st.cache_data(ttl=180, show_spinner=False)
def ensemble_guidance_data() -> pd.DataFrame:
    return load_ensemble()


@st.cache_data(ttl=180, show_spinner=False)
def observed_air_data() -> pd.DataFrame:
    return load_observed_air()


@st.cache_data(ttl=600, show_spinner=False)
def measured_pollen_data() -> pd.DataFrame:
    return load_measured_pollen()


@st.cache_data(ttl=600, show_spinner=False)
def climate_normals_data() -> pd.DataFrame:
    return load_climate_normals()


@st.cache_data(ttl=300, show_spinner=False)
def official_alerts_data() -> pd.DataFrame:
    return load_official_alerts()


@st.cache_data(ttl=60, show_spinner=False)
def health_data() -> dict[str, Any]:
    return health_snapshot(Settings.from_env())


@st.cache_data(ttl=600, show_spinner=False)
def score_data() -> pd.DataFrame:
    return load_provider_scores()


@st.cache_data(ttl=600, show_spinner=False)
def reference_score_data() -> pd.DataFrame:
    return load_reference_scores()


@st.cache_data(ttl=600, show_spinner=False)
def reliability_data() -> pd.DataFrame:
    return load_forecast_reliability()


@st.cache_data(ttl=60, show_spinner=False)
def source_status_data() -> pd.DataFrame:
    return load_source_health(Settings.from_env())


@st.cache_data(ttl=60, show_spinner=False)
def completeness_data() -> dict[str, Any]:
    return data_completeness_snapshot(24)


@st.cache_data(ttl=300, show_spinner=False)
def official_station_data() -> pd.DataFrame:
    return load_official_station_status()


@st.cache_data(ttl=60, show_spinner=False)
def log_data() -> pd.DataFrame:
    return load_recent_logs()


@st.cache_data(ttl=86_400, show_spinner=False)
def city_search_data(query: str) -> list[CityLocation]:
    return search_cities(query)


@st.cache_data(ttl=900, show_spinner=False)
def city_forecast_data(location: CityLocation) -> CityForecast:
    return fetch_city_forecast(location)


@st.cache_data(ttl=1_800, show_spinner=False)
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


@st.cache_data(ttl=300, show_spinner=False)
def radar_nowcast_data(latitude: float, longitude: float):
    return fetch_radar_nowcast(latitude, longitude)


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
        title={"font": {"color": ink}},
        legend={
            "orientation": "h",
            "x": 0,
            "xanchor": "left",
            "y": 1.02,
            "yanchor": "bottom",
            "bgcolor": "rgba(0,0,0,0)",
            "font": {"color": ink},
            "title": {"font": {"color": ink}},
            "tracegroupgap": 6,
        },
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


def _use_local_subplot_keys(figure: go.Figure) -> go.Figure:
    """Keep each visual key beside its subplot instead of in a distant legend."""
    for annotation in figure.layout.annotations or ():
        annotation.update(
            x=0,
            xanchor="left",
            align="left",
            font={"size": 12},
        )
    figure.update_layout(showlegend=False)
    return figure


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
                if str(value).lower()
                in {"ok", "success", "online", "operativa", "riuscita", "attiva"}
                else weather_cell_style(20, "confidence")
                if str(value).lower()
                in {
                    "error",
                    "failed",
                    "failure",
                    "offline",
                    "non disponibile",
                }
                else weather_cell_style(55, "confidence")
            ),
            subset=[status_column],
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


def _measurement_freshness_row(health: dict[str, Any]) -> str:
    labels = {
        "temperature": "Temperatura",
        "humidity": "Umidità",
        "pressure": "Pressione",
        "wind": "Vento",
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
        cards.append(
            '<div class="day-card">'
            f'<div class="day-name">{ITALIAN_WEEKDAYS[day.weekday()]} {day.day}</div>'
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
            '<div class="hour-card">'
            f'<div class="hour-title">+{position} h · {local_time:%H:%M}</div>'
            f'<div class="hour-weather">{_weather_icon(description)} {html.escape(description)}</div>'
            f'<div class="hour-meta">☁️ nuvole {_number(row.get("clouds"), 0, "%")}'
            f"<br>☔ {_number(reportable_rain_amount(row.get('rain_mm'), row.get('precip_probability')), 1, ' mm')} · rischio {_number(row.get('precip_probability'), 0, '%')}"
            f"<br>💨 {_number(row.get('wind_kmh'), 0, ' km/h')} · {compass_direction(row.get('wind_dir'))}</div>"
            "</div>"
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
    cards: list[tuple[str, str, str, str]],
) -> None:
    """Render responsive current-condition cards that never clip on mobile."""
    rendered = []
    for icon, label, value, detail in cards:
        rendered.append(
            '<div class="current-card">'
            f'<div class="current-label">{icon} {html.escape(label)}</div>'
            f'<div class="current-value" title="{html.escape(value)}">{html.escape(value)}</div>'
            f'<div class="current-detail" title="{html.escape(detail)}">{html.escape(detail)}</div>'
            "</div>"
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
    perceived = _number(forecast_row.get("feels_like_c"), 1, "° percepiti")
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
        f"{html.escape(perceived)} · min {html.escape(minimum)} · max {html.escape(maximum)}"
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
        tiles.append(
            f'<div class="hourly-tile{" is-now" if position == 0 else ""}">'
            f'<div class="hourly-time">{time_label}</div>'
            f'<div class="hourly-icon">{_weather_icon(description)}</div>'
            f'<div class="hourly-temp">{_number(row.get("temp_c"), 0, "°")}</div>'
            f'<div class="hourly-cloud">☁️ {_number(row.get("clouds"), 0, "%")} nuvole</div>'
            f'<div class="hourly-rain">☔ {_number(row.get("precip_probability"), 0, "%")} · {_number(reportable_rain_amount(row.get("rain_mm"), row.get("precip_probability")), 1, " mm")}</div>'
            f'<div class="hourly-wind">💨 {_number(row.get("wind_kmh"), 0, " km/h")}</div>'
            "</div>"
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
            f'<div class="insight-card tone-{item.tone}">'
            f'<div class="insight-title">{item.icon} {html.escape(item.title)}</div>'
            f'<div class="insight-value">{html.escape(item.value)}</div>'
            f'<div class="insight-detail">{html.escape(item.detail)}</div>'
            "</div>"
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
        f'<div class="activity-card tone-{html.escape(tone)}">'
        '<div class="activity-top">'
        f'<div><div class="activity-icon">{icon}</div><div class="activity-title">{html.escape(title)}</div></div>'
        f'<div class="activity-score">{html.escape(value)}</div></div>'
        f'<div class="activity-label">{html.escape(label)}</div>'
        f'<div class="activity-time">{html.escape(timing)}</div>'
        f'<div class="activity-detail">{html.escape(detail)}</div>'
        "</div>"
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
    temperature = temperature[temperature["valid_time"].between(now.floor("h"), cutoff)]
    rain = rain[rain["valid_time"].between(now.floor("h"), cutoff)]
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
            "Temperatura ensemble · fascia P10–P90 · ━ mediana P50",
            "Pioggia ensemble · ━ probabilità tra i membri",
        ),
    )
    figure.add_trace(
        go.Scatter(
            x=temperature["valid_time"],
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
            x=temperature["valid_time"],
            y=temperature["p10"],
            mode="lines",
            line={"width": 0},
            fill="tonexty",
            fillcolor="rgba(86,180,233,.24)",
            name="▰ P10–P90",
            hovertemplate="%{x|%d/%m %H:%M}<br>P10 %{y:.1f} °C<extra>Forchetta ensemble</extra>",
        ),
        row=1,
        col=1,
    )
    figure.add_trace(
        go.Scatter(
            x=temperature["valid_time"],
            y=temperature["p50"],
            mode="lines",
            name="━ Mediana P50",
            line={"color": "#0072b2", "width": 3},
        ),
        row=1,
        col=1,
    )
    if not rain.empty:
        figure.add_trace(
            go.Scatter(
                x=rain["valid_time"],
                y=rain["event_probability"],
                mode="lines",
                name="━ Membri con pioggia",
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
            "Qualità dell’aria · ━ AQI europeo · ┄ PM2.5",
            "Pollini · linee colorate per specie",
        ),
    )
    figure.add_trace(
        go.Scatter(
            x=hourly["time"],
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
            x=hourly["time"],
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
                    x=hourly["time"],
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
    cards = [
        ("AQI europeo", _number(current.get("european_aqi"), 0), aqi_label, aqi_tone),
        (
            "PM2.5",
            _number(current.get("pm2_5"), 1, " µg/m³"),
            "particolato fine",
            "neutral",
        ),
        ("PM10", _number(current.get("pm10"), 1, " µg/m³"), "particolato", "neutral"),
        ("Ozono", _number(current.get("ozone"), 1, " µg/m³"), "O₃ al suolo", "neutral"),
        (
            "Biossido d'azoto",
            _number(current.get("nitrogen_dioxide"), 1, " µg/m³"),
            "NO₂",
            "neutral",
        ),
        (
            "Polline prevalente",
            _number(pollen_value, 1, " grani/m³"),
            f"{pollen_name} · {pollen_label}",
            pollen_tone,
        ),
    ]
    st.markdown(
        '<div class="air-grid">'
        + "".join(
            f'<div class="air-card tone-{tone}"><div class="air-title">{html.escape(title)}</div>'
            f'<div class="air-value">{html.escape(value)}</div><div class="air-detail">{html.escape(detail)}</div></div>'
            for title, value, detail, tone in cards
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
        cards.append(
            '<div class="air-card tone-neutral">'
            f'<div class="air-title">◉ {html.escape(label)} osservato</div>'
            f'<div class="air-value">{_number(value, 1, " µg/m³")}</div>'
            f'<div class="air-detail">{html.escape(station_name)} · {html.escape(distance)} · {_local_time(item.get("time"))}</div>'
            "</div>"
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
            f'<div class="air-card tone-{tone}">'
            f'<div class="air-title">◉ {html.escape(str(item.get("family") or item["metric"]))}</div>'
            f'<div class="air-value">{value:.0f} granuli/m³</div>'
            f'<div class="air-detail">{html.escape(level)} indicativo · misura giornaliera</div></div>'
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
            f'<div class="air-card tone-{tone}"><div class="air-title">{html.escape(str(item["label"]))}</div>'
            f'<div class="air-value">{float(item["value"]):.{decimals}f} {html.escape(unit)}</div>'
            f'<div class="air-detail">{delta:+.{decimals}f} {html.escape(unit)} dalla mediana · {html.escape(str(item["state"]))}</div></div>'
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
            '<div class="day-card">'
            f'<div class="day-name">{ITALIAN_WEEKDAYS[day.weekday()]} {day.day}</div>'
            f'<div class="day-icon">{_weather_icon(description)}</div>'
            f'<div class="day-temp">{_number(row.get("temp_min_c"), 0, "°")} / {_number(row.get("temp_max_c"), 0, "°")}</div>'
            f'<div class="day-desc">{html.escape(description)}</div>'
            f'<div class="day-meta">☔ {_number(row.get("precipitation_mm"), 1, " mm")} · rischio {_number(row.get("precip_probability"), 0, "%")}'
            f"<br>💨 max {_number(row.get('wind_max_kmh'), 0, ' km/h')} · raffiche {_number(row.get('wind_gust_max_kmh'), 0, ' km/h')}"
            f"<br>🧭 {html.escape(compass_direction(row.get('wind_dir')))} · UV {_number(row.get('uv_index_max'), 1)}"
            f"<br>☀️ {'—' if pd.isna(sunrise) else sunrise.strftime('%H:%M')}–{'—' if pd.isna(sunset) else sunset.strftime('%H:%M')}</div>"
            "</div>"
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
            '<div class="hour-card">'
            f'<div class="hour-title">{moment:%H:%M}</div>'
            f'<div class="hour-weather">{_weather_icon(description)} {html.escape(description)}</div>'
            f'<div class="hour-meta">🌡️ {_number(row.get("temp_c"), 1, " °C")} · percepita {_number(row.get("feels_like_c"), 1, " °C")}'
            f"<br>☔ {_number(row.get('precipitation_mm'), 1, ' mm')} · rischio {_number(row.get('precip_probability'), 0, '%')}"
            f"<br>💨 {_number(row.get('wind_kmh'), 0, ' km/h')} · {html.escape(compass_direction(row.get('wind_dir')))}</div>"
            "</div>"
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
            "Temperatura · ━ temperatura · ··· percepita",
            "Pioggia · ▮ quantità · ━ probabilità",
        ),
    )
    figure.add_trace(
        go.Scatter(
            x=hourly["time"],
            y=hourly["temp_c"],
            name="Temperatura",
            line={"color": "#0ea5e9", "width": 3},
        ),
        row=1,
        col=1,
    )
    figure.add_trace(
        go.Scatter(
            x=hourly["time"],
            y=hourly["feels_like_c"],
            name="Percepita",
            line={"color": "#f59e0b", "width": 2, "dash": "dot"},
        ),
        row=1,
        col=1,
    )
    figure.add_trace(
        go.Bar(
            x=hourly["time"],
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
            x=hourly["time"],
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
        f"min {_number(city_today.get('temp_min_c'), 0, '°')} · max {_number(city_today.get('temp_max_c'), 0, '°')}</div></div></div>"
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
            ("🌡️", "Temperatura", _number(current.get("temp_c"), 1, " °C"), "adesso"),
            (
                "🧍",
                "Percepita",
                _number(current.get("feels_like_c"), 1, " °C"),
                "sensazione termica",
            ),
            ("💧", "Umidità", _number(current.get("humidity"), 0, " %"), "relativa"),
            (
                "⏱️",
                "Pressione",
                _number(current.get("pressure_hpa"), 0, " hPa"),
                "livello del mare",
            ),
            (
                "💨",
                "Vento",
                _number(current.get("wind_kmh"), 1, " km/h"),
                f"{compass_direction(current.get('wind_dir'))} · raffica {_number(current.get('wind_gust_kmh'), 1, ' km/h')}",
            ),
            (
                "☔",
                "Pioggia / nuvole",
                _number(current.get("precipitation_mm"), 1, " mm"),
                f"nuvole {_number(current.get('clouds'), 0, '%')}",
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
    future = clip_forecast(forecast, now, now + pd.Timedelta(hours=72))
    figure = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        row_heights=[0.68, 0.32],
        vertical_spacing=0.08,
        specs=[[{}], [{"secondary_y": True}]],
        subplot_titles=(
            "Temperatura · ━ misurata · ┄ previsione · ┄· stima buco · fascia = incertezza",
            "Pioggia · ▮ misurata/prevista · ━ probabilità · linea arancione = adesso",
        ),
    )
    if not observations.empty and "temp_c" in observations:
        figure.add_trace(
            go.Scatter(
                x=observations["time"],
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
            x0=gap.start.to_pydatetime(),
            x1=gap.end.to_pydatetime(),
            fillcolor="rgba(244,63,94,.13)",
            line_width=0,
            layer="below",
            row=1,
            col=1,
        )
        figure.add_trace(
            go.Scatter(
                x=gap.points["valid_time"],
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
                x=observations["time"],
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
                name="Previsione futura",
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
                    x=future["valid_time"],
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
        x=plotly_utc_datetime(now),
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
    future = clip_forecast(forecast, now, now + pd.Timedelta(hours=72))
    figure = make_subplots(
        rows=3,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.08,
        specs=[[{}], [{}], [{"secondary_y": True}]],
        subplot_titles=(
            "Umidità · ━ misurata · ┄ previsione · ┄· stima buco",
            "Pressione · ━ misurata · ┄ previsione · ┄· stima buco",
            "Vento · verde = vento · arancio = raffiche · rosa = direzione · ━ misura · ┄ previsione",
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
                    x=observations["time"],
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
                    x=future["valid_time"],
                    y=future[forecast_column],
                    name=f"{label} · previsione futura",
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
                    x=gap.points["valid_time"],
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
                x0=start.to_pydatetime(),
                x1=end.to_pydatetime(),
                fillcolor="rgba(244,63,94,.12)",
                line_width=0,
                layer="below",
                row=row,
                col=1,
            )

    figure.add_vline(
        x=plotly_utc_datetime(now),
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
        "Aggiorna la pagina ogni 5 min", value=True, key="auto_refresh"
    )
    st.divider()
    if app_section == "Stazione locale":
        st.caption(
            "I dati vengono acquisiti dal Cron Job Render ogni 5 minuti e riconciliati "
            "ogni giorno da GitHub. Il pulsante ricarica soltanto la pagina."
        )
    else:
        st.caption(
            "Le città vengono aggiornate da internet e conservate in cache per 15 minuti."
        )
    if st.button("Ricarica dati", width="stretch"):
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

station = station_data(max(observation_hours + 24, 240))
forecast = forecast_data()
forecast_history = forecast_history_data()
ensemble_guidance = ensemble_guidance_data()
official_air_observed = observed_air_data()
official_pollen_observed = measured_pollen_data()
climate_normals = climate_normals_data()
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
    render_current_grid(
        [
            (
                "🌡️",
                "Temperatura",
                _number(current.get("temp_c"), 1, " °C"),
                _delta(current, previous, "temp_c"),
            ),
            (
                "💧",
                "Umidità",
                _number(current.get("humidity"), 0, " %"),
                _delta(current, previous, "humidity", 0),
            ),
            (
                "⏱️",
                "Pressione",
                _number(current.get("pressure_hpa"), 1, " hPa"),
                _delta(current, previous, "pressure_hpa"),
            ),
            (
                "💨",
                "Vento",
                _number(current.get("wind_kmh"), 1, " km/h"),
                f"{compass_direction(current.get('winddir'))} · raffica {_number(current.get('windgust_kmh'), 1, ' km/h')}",
            ),
            (
                "☔",
                "Pioggia 24 h",
                _number(rain_24, 1, " mm"),
                f"ora {_number(current.get('rain_rate_mm_h'), 1, ' mm/h')}",
            ),
            (
                "☀️",
                "Solare / UV",
                _number(current.get("solar_w_m2"), 0, " W/m²"),
                f"UV {_number(current.get('uv_index'), 1)}",
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
    render_official_alerts(official_alerts, expert_mode=expert_mode)
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
            "Linea continua: misura reale · linea arancione: adesso · blu tratteggiato: "
            "previsione futura · fascia rosa e corallo tratteggiato: perdita dati, valore stimato."
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
                "A destra, la linea blu tratteggiata combina i provider e corregge gradualmente "
                "l'errore misurato localmente. Se una misura reale manca per oltre 30 minuti, "
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
        reliability = reliability_data()
        reference_scores = reference_score_data()
        official_stations = official_station_data()
        enabled_reference_sources = {"awc_metar"}
        if CFG.arsial_observations_enabled:
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
            "cfr_lazio": "CFR Lazio",
        }
        active_reference_names = ["Fiumicino e Ciampino"]
        if CFG.arsial_observations_enabled:
            active_reference_names.append("ARSIAL Roma-Lanciani")
        if CFG.cfr_observations_enabled:
            active_reference_names.append("CFR Lazio")
        cfr_status_caption = (
            " Il CFR è abilitato mediante endpoint ufficiale."
            if CFG.cfr_observations_enabled
            else ""
        )
        with st.expander("Accuratezza: Ecowitt principale e rete ufficiale"):
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
                            name=f"━ {provider} · {horizon}",
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
    st.subheader(f"Ultime {observation_hours} ore")
    if station.empty:
        st.info("Nessun dato della stazione disponibile.")
    else:
        cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=observation_hours)
        recent = station[station["time"] >= cutoff].copy()
        figure = make_subplots(
            rows=4,
            cols=1,
            shared_xaxes=True,
            vertical_spacing=0.065,
            subplot_titles=(
                "Temperatura · ━ misura Ecowitt",
                "Umidità · ━ misura Ecowitt",
                "Pressione · ━ misura Ecowitt",
                "Vento · ━ velocità · ┄ raffiche · misure Ecowitt",
            ),
        )
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
                x=recent["time"],
                y=_numeric_series(recent, "rain_mm", 0).clip(lower=0),
                name="▮ Quantità misurata per campione",
                marker_color="#38bdf8",
            )
        )
        if "rain_rate_mm_h" in recent:
            rain_figure.add_trace(
                go.Scatter(
                    x=recent["time"],
                    y=pd.to_numeric(recent["rain_rate_mm_h"], errors="coerce").clip(
                        lower=0
                    ),
                    name="━ Intensità misurata",
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
            station,
            climate_normals,
            dark_mode,
            expert_mode=expert_mode,
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
                f"per {CFG.latitude:.4f}, {CFG.longitude:.4f}; restano uguali nei giorni perché "
                "descrivono il sito. Meteo e Luna cambiano ogni notte. Un valore SQM reale richiede "
                f"un fotometro calibrato. [Apri la fonte]({light_pollution.source_url})."
            )
        else:
            st.info(
                "La stima geolocalizzata SQM non è disponibile in questo momento: "
                + (light_pollution_error or "fonte non raggiungibile")
                + ". Le altre previsioni astronomiche restano utilizzabili."
            )

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
                name="▰ Qualità cielo",
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
            "Il punteggio penalizza nuvole basse/medie/alte, rischio pioggia, vento, visibilità e temperatura vicina al punto di rugiada."
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
        '<div class="section-kicker">Nowcast locale</div>',
        unsafe_allow_html=True,
    )
    st.subheader("Arrivo della pioggia vicino alla stazione")
    if not CFG.radar_nowcast_enabled:
        st.caption("Il nowcast puntuale è disattivato; le mappe restano disponibili.")
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
        st.caption("Apri la scheda Radar per calcolare il nowcast puntuale.")

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

with tab_system:
    st.markdown(
        '<div class="section-kicker">Affidabilità e continuità</div>',
        unsafe_allow_html=True,
    )
    st.subheader("Stato del sistema")
    completeness = completeness_data()
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
        "Schema dati",
        "v5",
        "migrazione additiva",
        delta_color="off",
    )

    sources = source_status_data()
    if not CFG.cfr_observations_enabled and "source" in sources:
        sources = sources[sources["source"] != "cfr_lazio"].copy()
    st.markdown("#### Fonti e processi indipendenti")
    st.caption(
        "Un errore di una fonte non interrompe le altre. Lo stato considera sia "
        "l'ultimo tentativo sia la frequenza attesa di aggiornamento."
    )
    if sources.empty:
        st.info("Lo stato dettagliato comparirà dopo la prossima acquisizione.")
    else:
        status_labels = {
            "online": "Operativa",
            "delayed": "In ritardo",
            "cached": "Archivio disponibile",
            "external_unavailable": "Fonte esterna indisponibile",
            "offline": "Non disponibile",
            "waiting": "In attesa",
            "disabled": "Disattivata",
        }
        category_labels = {
            "misure": "Misure",
            "previsioni": "Previsioni",
            "probabilistica": "Probabilistica",
            "riferimenti": "Riferimenti",
            "ambiente": "Ambiente",
            "elaborazione": "Elaborazione",
            "protezione": "Protezione",
        }
        source_table = pd.DataFrame(
            {
                "Componente": sources["label"],
                "Categoria": sources["category"].map(category_labels),
                "Stato": sources["display_status"].map(status_labels),
                "Ultimo tentativo": sources["last_attempt_at"].map(_local_time),
                "Ultimo successo": sources["last_success_at"].map(_local_time),
                "Ultimo dato/copertura": sources["last_observation_at"].map(
                    _local_time
                ),
                "Età successo": sources["age_minutes"].map(_age_text),
                "Latenza ms": pd.to_numeric(
                    sources["latency_ms"], errors="coerce"
                ).round(0),
                "Righe": pd.to_numeric(sources["rows_received"], errors="coerce").round(
                    0
                ),
                "Errori consecutivi": pd.to_numeric(
                    sources["consecutive_failures"], errors="coerce"
                ).round(0),
                "Ultimo errore": sources["last_error"].replace("", "—"),
            }
        )
        render_color_legend("status")
        render_styled_table(
            _style_status_table(source_table, dark_mode, "Stato"),
            height=470,
        )
        arsial_state = sources.loc[
            sources["source"].eq("arsial_siarl"), "display_status"
        ]
        if not arsial_state.empty and arsial_state.iloc[0] == "external_unavailable":
            st.warning(
                "Il portale pubblico ARSIAL/SIARL al momento non restituisce un "
                "export leggibile. Il connettore ritenta automaticamente ogni "
                f"{CFG.official_observation_refresh_minutes} minuti; Ecowitt, METAR "
                "e previsioni continuano normalmente. Dopo il primo campione valido, "
                f"l'archivio resta utilizzabile fino a {CFG.arsial_cache_hours} ore "
                "durante eventuali nuovi disservizi."
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
        "e modalità semplice/esperta sono moduli V4.2 indipendenti. Le fonti esterne "
        "restano non bloccanti e nessuna notifica automatica è attiva."
    )

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
        "backup_database.py crea sul computer un archivio ZIP portatile e lo "
        "verifica con conteggi e checksum; la data dell'ultima esportazione riuscita "
        "compare tra i componenti. Per il recupero online usa inoltre backup e PITR "
        "gestiti direttamente dal piano PostgreSQL Render, senza trasferire il "
        "database verso GitHub."
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
if CFG.arsial_observations_enabled:
    reference_attribution += " + [ARSIAL/SIARL](https://siarl.arsial.it/)"
if CFG.cfr_observations_enabled:
    reference_attribution += " + CFR Lazio"
st.caption(
    "Meteo V4 · Open‑Meteo + OpenWeather · correzione locale sulla stazione"
    + reference_attribution
    + f" · ultimo dato reale {_local_time(health.get('station_time'))}"
)
