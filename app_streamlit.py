# -*- coding: utf-8 -*-
"""
app_streamlit.py — Meteo Dashboard
- Canonizzazione colonne case-insensitive (Temp_C, Humidity, Pressure_hPa, Wind_kmh, WindGust_kmh, WindDir, Rain_mm, Time)
- Parsing numerico locale-aware + normalizzazione unità (Temp: K/°F→°C; Pressione: Pa/kPa/inHg e hPa*10→hPa)
- Fix grafico vento (usa station_raw o station_3h, conversioni e cast)
- Loader robusto per il timestamp (accetta Time/ts_utc/timestamp/…)
- Health widget + update manuale ingest locale/cloud
- Auto-refresh opzionale (5 min) + cache TTL 5 min
- Radar RainViewer + Nuvole (OpenWeather/NASA)
- Salva impostazioni + Ricerca città (Nominatim)
- Fallback dati quando finestra vuota + warning vento=0
- Diagnostica dati + padding asse vento
"""

import os, json, time, subprocess
from pathlib import Path
import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import requests
import pydeck as pdk

try:
    import folium
    from streamlit_folium import st_folium
    HAS_FOLIUM = True
except Exception:
    HAS_FOLIUM = False

st.set_page_config(page_title="Meteo • Dashboard", layout="wide", page_icon="🌦️")
load_dotenv()

DB_PATH = os.getenv("SQLITE_PATH", "./data/weather.db")
DB_URL = (os.getenv("DATABASE_URL", "") or "").strip()
if DB_URL and "USER:PASS@HOST:PORT/DBNAME" in DB_URL:
    DB_URL = ""

OW_API_KEY = (os.getenv("OW_API_KEY") or "").strip()
ENV_LAT = (os.getenv("LAT") or "").strip()
ENV_LON = (os.getenv("LON") or "").strip()
LOCAL_TZ = "Europe/Rome"

def get_engine():
    if DB_URL:
        return create_engine(DB_URL, future=True)
    p = Path(DB_PATH); p.parent.mkdir(parents=True, exist_ok=True)
    return create_engine(f"sqlite:///{p}", future=True)

# -------------------- Prefs --------------------
def ensure_prefs():
    try:
        with get_engine().begin() as cx:
            cx.execute(text("CREATE TABLE IF NOT EXISTS user_prefs (k TEXT PRIMARY KEY, v TEXT)"))
    except Exception:
        pass

def load_prefs():
    ensure_prefs()
    try:
        with get_engine().begin() as cx:
            rows = cx.execute(text("SELECT k, v FROM user_prefs")).fetchall()
        out = {}
        for k, v in rows:
            try: out[k] = json.loads(v)
            except Exception: out[k] = v
        return out
    except Exception:
        return {}

def save_prefs(d: dict):
    ensure_prefs()
    with get_engine().begin() as cx:
        for k, v in d.items():
            cx.execute(text("""
                INSERT INTO user_prefs (k, v) VALUES (:k, :v)
                ON CONFLICT (k) DO UPDATE SET v=excluded.v
            """), {"k": k, "v": json.dumps(v)})

PREFS = load_prefs()
pref = lambda k, dv: PREFS.get(k, dv)

# -------------------- UI helpers --------------------
def _keep_scroll():
    st.components.v1.html(
        '<script>\n'
        'const KEY="scrollY_weather_app";\n'
        'addEventListener("load",()=>{const y=sessionStorage.getItem(KEY);if(y!==null)scrollTo(0,parseFloat(y));});\n'
        'addEventListener("scroll",()=>{sessionStorage.setItem(KEY, String(window.scrollY||pageYOffset||0));});\n'
        '</script>', height=0)

st.markdown("""
<style>
:root { --radius: 16px; }
.block-container { padding-top: .5rem; }
.header { background: linear-gradient(135deg,#3b82f6 0%,#06b6d4 100%); border-radius:16px; padding:18px 20px; color:#fff; margin-bottom:.5rem; }
.smallcaps { font-variant: all-small-caps; opacity:.8; }
</style>
""", unsafe_allow_html=True)
_keep_scroll()

def autorefresh(minutes: int):
    ms = int(minutes * 60 * 1000)
    st.components.v1.html(f"<script>setTimeout(()=>window.parent.location.reload(), {ms});</script>", height=0)

# -------------------- Data helpers --------------------
CANON_MAP = {
    "temp_c": "Temp_C",
    "humidity": "Humidity",
    "hum": "Humidity",
    "pressure_hpa": "Pressure_hPa",
    "press_hpa": "Pressure_hPa",
    "wind_kmh": "Wind_kmh",
    "windgust_kmh": "WindGust_kmh",
    "wind_gust_kmh": "WindGust_kmh",
    "winddir": "WindDir",
    "wind_dir": "WindDir",
    "rain_mm": "Rain_mm",
    "ts_utc": "Time",
    "timestamp": "Time",
    "datetime": "Time",
    "date_time": "Time",
    "time": "Time",
    "timestamp_utc": "Time",
}
NUMERIC_COLS = ["Temp_C","Humidity","Pressure_hPa","Wind_kmh","WindGust_kmh","WindDir","Rain_mm"]

def _fix_num_str(x):
    """Normalizza stringhe numeriche in vari formati locali."""
    if isinstance(x, (int, float)) or x is None:
        return x
    s = str(x).strip()
    if not s:
        return None
    s = s.replace(" ", "")
    if "." in s and "," in s and s.rfind(",") > s.rfind("."):
        s = s.replace(".", "").replace(",", ".")
    else:
        if "," in s and "." not in s:
            s = s.replace(",", ".")
        if s.count(",") > 1 and "." in s:
            s = s.replace(",", "")
        if s.count(".") > 1 and "," not in s:
            s = s.replace(".", "")
    return s

def canonicalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.copy()
    lower_map = {c: c.lower() for c in df.columns}
    rename = {}
    for orig, low in lower_map.items():
        if low in CANON_MAP:
            rename[orig] = CANON_MAP[low]
    if rename:
        df.rename(columns=rename, inplace=True)
    if "Time" not in df.columns:
        for c in df.columns:
            if "time" in str(c).lower():
                df.rename(columns={c: "Time"}, inplace=True)
                break
    return df

def ensure_time_and_numeric(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.copy()
    if "Time" not in df.columns:
        return pd.DataFrame()
    df["Time"] = pd.to_datetime(df["Time"], utc=True, errors="coerce")
    df = df.dropna(subset=["Time"])
    if df.empty:
        return pd.DataFrame()
    for c in NUMERIC_COLS:
        if c in df.columns:
            if df[c].dtype == object:
                df[c] = df[c].map(_fix_num_str)
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def normalize_units(df: pd.DataFrame) -> pd.DataFrame:
    """Porta Pressione in hPa (~800–1100) e Temperatura in °C in modo deterministico."""
    if df is None or df.empty:
        return df
    df = df.copy()

    # ---- PRESSIONE → hPa
    if "Pressure_hPa" in df.columns:
        p = pd.to_numeric(df["Pressure_hPa"], errors="coerce")

        if p.notna().any():
            med = float(p.median())

            # mapping deterministico per i casi comuni
            if   800.0 <= med <= 1100.0:
                pass                             # già in hPa
            elif 8000.0 <= med <= 11000.0:
                p = p / 10.0                     # hPa*10 → hPa
            elif 80000.0 <= med <= 110000.0:
                p = p / 100.0                    # Pa → hPa
            elif 50.0 <= med <= 200.0:
                p = p * 10.0                     # kPa → hPa
            elif 20.0 <= med <= 40.0:
                p = p * 33.8638866667            # inHg → hPa
            else:
                # fallback: scala per potenze di 10 verso il range 800–1100
                # (max 3 iterazioni per sicurezza)
                for _ in range(3):
                    med = float(pd.to_numeric(p, errors="coerce").dropna().median())
                    if 800.0 <= med <= 1100.0:
                        break
                    if med > 1100.0:
                        p = p / 10.0
                    elif med < 800.0:
                        p = p * 10.0

        df["Pressure_hPa"] = p

    # ---- TEMPERATURA → °C
    if "Temp_C" in df.columns:
        t = pd.to_numeric(df["Temp_C"], errors="coerce")
        if t.notna().any():
            medt = float(t.median())
            if medt > 200.0:         # Kelvin → °C
                t = t - 273.15
            elif medt > 60.0:        # °F → °C
                t = (t - 32.0) * 5.0/9.0
        df["Temp_C"] = t

    return df


def read_table(table):
    try:
        with get_engine().connect() as cx:
            raw = pd.read_sql(f"SELECT * FROM {table}", cx)
    except Exception:
        return pd.DataFrame()
    raw = canonicalize_columns(raw)
    raw = ensure_time_and_numeric(raw)
    raw = normalize_units(raw)
    return raw

# -------------------- Station loaders --------------------
@st.cache_data(ttl=300)
def load_station():
    """Sceglie la tabella più recente tra station_3h e station_raw e normalizza vento/raffiche."""
    df3h = read_table("station_3h")
    dfr  = read_table("station_raw")

    # Se station_raw ha wind_ms, deriviamo Wind_kmh
    try:
        with get_engine().connect() as cx:
            raw = pd.read_sql("SELECT * FROM station_raw", cx)
    except Exception:
        raw = pd.DataFrame()
    if not raw.empty:
        tmp = canonicalize_columns(raw)
        tmp = ensure_time_and_numeric(tmp)
        if "wind_ms" in raw.columns and "Time" in tmp.columns:
            m = tmp[["Time"]].copy()
            m["wind_ms"] = pd.to_numeric(raw["wind_ms"], errors="coerce")
            dfr = dfr.merge(m, on="Time", how="left")
            if "Wind_kmh" not in dfr.columns:
                dfr["Wind_kmh"] = m["wind_ms"] * 3.6
            else:
                dfr["Wind_kmh"] = dfr["Wind_kmh"].fillna(m["wind_ms"] * 3.6)
            if "WindGust_kmh" not in dfr.columns:
                dfr["WindGust_kmh"] = dfr["Wind_kmh"]
            dfr.drop(columns=[c for c in ["wind_ms"] if c in dfr.columns], inplace=True, errors="ignore")

    # Scegli dataset più recente
    if df3h.empty and dfr.empty:
        chosen = pd.DataFrame()
    elif df3h.empty:
        chosen = dfr.sort_values("Time")
    elif dfr.empty:
        chosen = df3h.sort_values("Time")
    else:
        chosen = dfr if dfr["Time"].max() >= df3h["Time"].max() else df3h
        chosen = chosen.sort_values("Time")

    # Cast finali + normalizzazione unità
    for c in ["Wind_kmh","WindGust_kmh","Rain_mm","Temp_C","Humidity","Pressure_hPa","WindDir"]:
        if c in chosen.columns:
            chosen[c] = pd.to_numeric(chosen[c], errors="coerce")
    if "WindGust_kmh" in chosen.columns and "Wind_kmh" in chosen.columns:
        chosen["WindGust_kmh"] = chosen["WindGust_kmh"].fillna(chosen["Wind_kmh"])
    # chosen = normalize_units(chosen)

    return chosen

@st.cache_data(ttl=300)
def load_forecast():
    df = read_table("forecast_ow")
    if df.empty: return df
    if "Wind_mps" in df.columns and "Wind_kmh" not in df.columns:
        df["Wind_kmh"] = pd.to_numeric(df["Wind_mps"], errors="coerce") * 3.6
    df = normalize_units(df)
    return df.sort_values("Time")

def get_last_ingest():
    try:
        with get_engine().connect() as cx:
            df = pd.read_sql("SELECT * FROM meta", cx)
    except Exception:
        return None
    if df.empty:
        return None
    if "k" in df.columns and "v" in df.columns:
        v = df.loc[df["k"]=="last_ingest","v"]
        if v.empty: return None
        return pd.to_datetime(v.iloc[0], utc=True, errors="coerce")
    df = canonicalize_columns(df)
    df = ensure_time_and_numeric(df)
    if df.empty: return None
    return df["Time"].max()

def run_ingest(script_name:str):
    venv_py = Path(".venv") / "Scripts" / "python.exe"
    cmd = f'"{venv_py}" "{script_name}"' if venv_py.exists() else f'python "{script_name}"'
    try:
        res = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=600)
        ok = (res.returncode == 0)
        out = (res.stdout or "") + ("\n" + (res.stderr or ""))
        return ok, out
    except Exception as e:
        return False, str(e)

# -------------------- Health & Force Update --------------------
def _humanize_delta(minutes:int) -> str:
    try:
        m = int(minutes)
        if m < 60: return f"{m} min"
        h = m // 60; r = m % 60
        return f"{h}h {r}m" if r else f"{h}h"
    except Exception:
        return "—"

def health_widget():
    st.markdown(
        """
        <style>
        .pill { display:inline-block; padding:6px 10px; border-radius:999px; font-weight:600; font-size:0.9rem; }
        .ok { background:#16a34a; color:white; }
        .warn { background:#f59e0b; color:black; }
        .crit { background:#ef4444; color:white; }
        .muted { color:#6b7280; font-size:0.9rem; }
        </style>
        """, unsafe_allow_html=True
    )
    ts = get_last_ingest()
    if ts is None or pd.isna(ts):
        st.markdown('<span class="pill crit">HEALTH: sconosciuto</span> <span class="muted">Nessun last_ingest</span>', unsafe_allow_html=True)
    else:
        age = pd.Timestamp.utcnow() - ts.tz_convert("UTC")
        age_min = int(age.total_seconds() // 60)
        css, label = ("ok","OK") if age_min <= 40 else (("warn","RITARDO") if age_min <= 120 else ("crit","FUORI SERVIZIO"))
        local = ts.tz_convert(LOCAL_TZ).strftime("%Y-%m-%d %H:%M")
        st.markdown(f'<span class="pill {css}">HEALTH: {label}</span> <span class="muted">ultimo ingest: {_humanize_delta(age_min)} fa • {local} {LOCAL_TZ}</span>', unsafe_allow_html=True)

    c1, c2, c3 = st.columns([1,1,2])
    with c1:
        if st.button("⚡ Forza aggiornamento (locale)"):
            with st.spinner("Eseguo ingest locale…"):
                ok, out = run_ingest("weather_ingest.py")
            st.expander("Log ingest locale", expanded=True).code(out or "(nessun output)")
            if ok:
                load_station.clear(); load_forecast.clear()
                st.success("Ingest locale completato e cache svuotata.")
                st.rerun()
            else:
                st.error("Ingest locale fallito. Vedi log.")
    with c2:
        if st.button("🌐 Forza ingest (Ecowitt Cloud)"):
            with st.spinner("Eseguo ingest Ecowitt Cloud…"):
                ok, out = run_ingest("weather_ingest_ecowitt_cloud.py")
            st.expander("Log ingest cloud", expanded=True).code(out or "(nessun output)")
            if ok:
                load_station.clear(); load_forecast.clear()
                st.success("Ingest cloud completato e cache svuotata.")
                st.rerun()
            else:
                st.error("Ingest cloud fallito. Vedi log.")
    with c3:
        st.caption("Verde ≤ 40 min; Giallo 40–120; Rosso > 120 o assente.")

# -------------------- Radar & Nuvole --------------------
RADAR_PALETTES = {"Classic":0, "Dark":3, "Blue":5, "Tropical":9, "Original":1}

@st.cache_data(ttl=120)
def rv_frames():
    j = requests.get("https://api.rainviewer.com/public/weather-maps.json", timeout=10).json()
    radar = j.get("radar") or {}
    past = radar.get("past") or []
    nowcast = radar.get("nowcast") or radar.get("future") or []
    frames = past + nowcast
    if not frames: return [], -1
    out = []
    for f in frames:
        ts = int(f.get("time"))
        local = pd.to_datetime(ts, unit="s", utc=True).tz_convert(LOCAL_TZ).strftime("%Y-%m-%d %H:%M")
        out.append({"ts": ts, "label_local": local})
    now_idx = len(past)-1 if past else len(out)-1
    if now_idx < 0: now_idx = len(out)-1
    return out, now_idx

def rv_tile(ts, palette_idx=5, smooth=True, snow=True):
    return f"https://tilecache.rainviewer.com/v2/radar/{ts}/256/{{z}}/{{x}}/{{y}}/2/1_1.png?color={palette_idx}&smooth={1 if smooth else 0}&snow={1 if snow else 0}"

def ow_clouds_tile(api_key: str):
    if not api_key: return None
    return f"https://tile.openweathermap.org/map/clouds_new/{{z}}/{{x}}/{{y}}.png?appid={api_key}"

def gibs_truecolor_tile(dt_utc: pd.Timestamp):
    d = dt_utc.tz_convert("UTC").strftime("%Y-%m-%d")
    return f"https://gibs.earthdata.nasa.gov/wmts/epsg3857/best/MODIS_Terra_CorrectedReflectance_TrueColor/default/{d}/GoogleMapsCompatible_Level{{z}}/{{y}}/{{x}}.jpg"

# -------------------- Sidebar --------------------
with st.sidebar:
    st.title("Impostazioni")
    st.markdown("#### Visuale")
    theme = st.radio("Tema grafici", ["Chiaro","Scuro"], horizontal=True,
                     index=0 if pref("theme","Chiaro")=="Chiaro" else 1, key="ui_theme")
    template = "plotly_white" if theme=="Chiaro" else "plotly_dark"
    hours = st.slider("Ore osservazioni", 6, 240, int(pref("hours",72)), step=6, key="hours")
    auto_ref = st.checkbox("Auto refresh ogni 5 minuti", value=bool(pref("auto_refresh", True)), key="auto_refresh")

    st.markdown("#### Grafici")
    charts_default = ["Temperatura","Umidità","Pressione","Vento","Pioggia"]
    charts = st.multiselect("Mostra", charts_default, default=pref("charts",charts_default), key="charts")

    st.markdown("#### Radar & Nuvole")
    renderer = st.radio("Renderer", ["WebGL (veloce)","Compatibilità (massima)"],
                        index=0 if pref("renderer","WebGL (veloce)")=="WebGL (veloce)" else 1, key="renderer")
    basemap = st.selectbox("Basemap (compatibilità)", ["Carto Positron","OpenStreetMap"],
                           index=0 if pref("basemap","Carto Positron")=="Carto Positron" else 1, key="basemap")
    show_radar = st.checkbox("Mostra radar pioggia", value=bool(pref("show_radar",True)), key="show_radar")
    show_clouds = st.checkbox("Mostra nuvole", value=bool(pref("show_clouds",bool(OW_API_KEY))), key="show_clouds")
    radar_palette = st.selectbox("Palette radar", list(RADAR_PALETTES.keys()),
                                 index=list(RADAR_PALETTES.keys()).index(pref("radar_palette","Blue")), key="radar_palette")
    radar_smooth = st.checkbox("Radar smoothing", value=bool(pref("radar_smooth",True)), key="radar_smooth")
    radar_snow = st.checkbox("Mostra neve", value=bool(pref("radar_snow",True)), key="radar_snow")
    clouds_provider = st.selectbox("Provider nuvole", ["OpenWeather (corrente)","NASA GIBS (TrueColor)"],
                                   index=0 if pref("clouds_provider","OpenWeather (corrente)")=="OpenWeather (corrente)" else 1, key="clouds_provider")
    radar_opacity = st.slider("Opacità radar", 0.0, 1.0, float(pref("radar_opacity",0.7)), 0.05, key="radar_opacity")
    clouds_opacity = st.slider("Opacità nuvole", 0.0, 1.0, float(pref("clouds_opacity",0.55)), 0.05, key="clouds_opacity")

    st.markdown("#### Playback")
    speed_ms = st.slider("Velocità (ms/frame)", 150, 1200, int(pref("speed_ms",400)), 50, key="speed_ms")
    play = st.toggle("▶️ Play", value=bool(pref("play",False)), key="play_toggle")
    if st.button("⏮️ Torna adesso"): st.session_state["rv_force_now"] = True

    st.markdown("#### Mappa")
    try:
        lat_default = float(pref("lat", ENV_LAT or 41.9)); lon_default = float(pref("lon", ENV_LON or 12.5))
    except Exception:
        lat_default, lon_default = 41.9, 12.5
    lat = st.number_input("Latitudine", value=lat_default, format="%.6f", key="lat")
    lon = st.number_input("Longitudine", value=lon_default, format="%.6f", key="lon")
    zoom = st.slider("Zoom iniziale", 3, 12, int(pref("zoom",7)), 1, key="zoom")
    show_marker = st.checkbox("Mostra marker posizione", value=bool(pref("show_marker",True)), key="show_marker")

    # 🔎 Ricerca città (Nominatim)
    st.markdown("#### Ricerca città")
    q = st.text_input("Cerca città/indirizzo", value=pref("city_query",""), key="city_query")
    if st.button("🔎 Cerca città"):
        if q.strip():
            try:
                headers = {"User-Agent": "meteo-dashboard/1.0 (streamlit)"}
                r = requests.get("https://nominatim.openstreetmap.org/search",
                                 params={"q": q, "format": "json", "limit": 1},
                                 headers=headers, timeout=12)
                r.raise_for_status()
                arr = r.json()
                if arr:
                    lat_found = float(arr[0]["lat"]); lon_found = float(arr[0]["lon"])
                    st.session_state["lat"] = lat_found
                    st.session_state["lon"] = lon_found
                    st.success(f"Trovato: {arr[0].get('display_name','')} → lat={lat_found:.5f}, lon={lon_found:.5f}")
                else:
                    st.warning("Nessun risultato.")
            except Exception as e:
                st.error(f"Errore ricerca città: {e}")
        else:
            st.info("Inserisci un nome città prima di cercare.")

    st.markdown("#### Azioni")
    colA, colB = st.columns(2)
    if colA.button("💾 Salva impostazioni"):
        save_prefs({
            "theme": theme, "hours": hours, "charts": charts,
            "renderer": renderer, "basemap": basemap,
            "show_radar": show_radar, "show_clouds": show_clouds,
            "radar_palette": radar_palette, "radar_smooth": radar_smooth, "radar_snow": radar_snow,
            "clouds_provider": clouds_provider,
            "radar_opacity": radar_opacity, "clouds_opacity": clouds_opacity,
            "speed_ms": speed_ms, "lat": st.session_state["lat"], "lon": st.session_state["lon"],
            "zoom": zoom, "show_marker": show_marker,
            "play": st.session_state.get("play_toggle", False),
            "auto_refresh": st.session_state.get("auto_refresh", True),
            "city_query": st.session_state.get("city_query",""),
        })
        st.success("Preferenze salvate.")
    if colB.button("🔄 Aggiorna dati ora"):
        ok, out = run_ingest("weather_ingest.py")
        with st.expander("Log aggiornamento", expanded=True):
            st.code(out or "(nessun output)")
        if ok:
            load_station.clear(); load_forecast.clear()
            st.success("Aggiornato e cache svuotata.")

# -------------------- Header --------------------
with st.container():
    st.markdown('<div class="header"><h2>🌦️ Meteo Dashboard</h2><p class="smallcaps">Stazione locale + OpenWeather</p></div>', unsafe_allow_html=True)
    if st.session_state.get("auto_refresh", True):
        autorefresh(5)
    health_widget()

# -------------------- Tabs --------------------
tab1, tab2, tab3 = st.tabs(["📈 Osservazioni", "🛰️ Previsioni", "🗺️ Radar & Nuvole"])

# -------- Osservazioni --------
with tab1:
    df_station = load_station()
    if df_station.empty:
        st.warning("Nessun dato stazione. (Attendi il prossimo ingest o aggiorna manualmente)")
    else:
        now_utc = pd.Timestamp.utcnow()
        tmin = now_utc - pd.Timedelta(hours=st.session_state["hours"])
        recent = df_station[df_station["Time"] >= tmin].copy()
        if recent.empty:
            recent = df_station.copy()
            st.info("Nessun dato nelle ultime ore selezionate: mostro tutti i dati disponibili. Aumenta 'Ore osservazioni' nella sidebar per filtrare meglio.")
        recent["TimeLocal"] = recent["Time"].dt.tz_convert(LOCAL_TZ)

        # Warning se vento tutto zero
        if "Wind_kmh" in recent.columns and len(recent) > 0:
            if (recent["Wind_kmh"].fillna(0).abs() < 0.001).all():
                st.warning("I valori del vento risultano tutti 0. Verifica la sorgente (riempi 'station_raw' via ingest cloud/backfill).")

        # KPI
        last_row = recent.tail(1)
        if not last_row.empty:
            row = last_row.iloc[0]
            def _fmt(v, fmt, fallback="—"):
                return (fmt.format(v) if pd.notna(v) else fallback)
            c1,c2,c3,c4,c5 = st.columns(5)
            c1.metric("🌡️ Temp", _fmt(row.get("Temp_C"), "{:.1f} °C"))
            c2.metric("💧 UR", _fmt(row.get("Humidity"), "{:.0f} %"))
            c3.metric("⏱️ Press", _fmt(row.get("Pressure_hPa"), "{:.1f} hPa"))
            c4.metric("🍃 Vento", _fmt(row.get("Wind_kmh"), "{:.1f} km/h"))
            rain_val = row.get("Rain_mm")
            rain_disp = "{:.1f} mm".format(rain_val) if pd.notna(rain_val) else "0.0 mm"
            c5.metric("🌧️ Pioggia 3h", rain_disp)

        # 🔎 Diagnostica
        with st.expander("🔎 Diagnostica dati (ultima finestra)", expanded=False):
            def _rng(s):
                if s not in recent.columns or recent[s].dropna().empty:
                    return "—"
                v = pd.to_numeric(recent[s], errors="coerce").dropna()
                if v.empty: return "—"
                return f"{v.min():.2f} → {v.max():.2f}"
            def _nnz(s):
                if s not in recent.columns: return 0
                v = pd.to_numeric(recent[s], errors="coerce").fillna(0)
                return int((v.abs() > 1e-6).sum())
            st.write(
                f"- Temp_C: {_rng('Temp_C')}\n"
                f"- Pressure_hPa: {_rng('Pressure_hPa')}\n"
                f"- Wind_kmh: {_rng('Wind_kmh')}\n"
                f"- WindGust_kmh: {_rng('WindGust_kmh')}"
            )

        # funzione smoothing
        def smooth(df, cols):
            if df.empty: return df
            n = len(df); eff = max(3, min(15, max(3, n//3)))
            out = df.copy()
            for c in cols:
                if c in out.columns:
                    out[c] = out[c].rolling(eff, min_periods=1, center=True).mean()
            return out

        # Temperatura (range dinamico)
        if "Temperatura" in st.session_state["charts"] and "Temp_C" in recent.columns:
            d = smooth(recent, ["Temp_C"])
            fig = px.line(d, x="TimeLocal", y="Temp_C",
                          template=("plotly_dark" if st.session_state["ui_theme"]=="Scuro" else "plotly_white"),
                          title="Temperatura (°C)", markers=True)
            try:
                y = pd.to_numeric(d["Temp_C"], errors="coerce").dropna()
                if not y.empty:
                    ymin, ymax = float(y.min()), float(y.max())
                    pad = max(0.5, (ymax - ymin) * 0.1)
                    fig.update_yaxes(range=[ymin - pad, ymax + pad])
            except Exception:
                pass
            st.plotly_chart(fig, use_container_width=True)

        if "Umidità" in st.session_state["charts"] and "Humidity" in recent.columns:
            d = smooth(recent, ["Humidity"])
            st.plotly_chart(px.line(d, x="TimeLocal", y="Humidity", template=template, title="Umidità (%)", markers=True), use_container_width=True)

        if "Pressione" in st.session_state["charts"] and "Pressure_hPa" in recent.columns:
            d = smooth(recent, ["Pressure_hPa"])
            st.plotly_chart(px.line(d, x="TimeLocal", y="Pressure_hPa", template=template, title="Pressione (hPa)", markers=True), use_container_width=True)

        if "Vento" in st.session_state["charts"]:
            cols = [c for c in ["Wind_kmh","WindGust_kmh"] if c in recent.columns]
            if cols:
                d = smooth(recent, cols)
                fig_w = px.line(d, x="TimeLocal", y=cols, template=template, title="Vento (km/h)", markers=True)
                try:
                    y = pd.concat([pd.to_numeric(d[c], errors="coerce") for c in cols], axis=0).dropna()
                    if not y.empty:
                        ymin, ymax = float(y.min()), float(y.max())
                        if abs(ymax - ymin) < 0.5:
                            fig_w.update_yaxes(range=[ymin - 0.5, ymax + 0.5])
                except Exception:
                    pass
                st.plotly_chart(fig_w, use_container_width=True)
            else:
                st.info("Nessuna colonna vento trovata (cerco Wind_kmh / WindGust_kmh). Colonne presenti: " + ", ".join(list(recent.columns)))

        if "Pioggia" in st.session_state["charts"] and "Rain_mm" in recent.columns:
            d = recent.copy(); d["Rain_mm"] = pd.to_numeric(d["Rain_mm"], errors="coerce").fillna(0)
            st.plotly_chart(px.bar(d, x="TimeLocal", y="Rain_mm", template=template, title="Pioggia aggregata (mm / 3h)"), use_container_width=True)

# -------- Previsioni --------
with tab2:
    df_fc = load_forecast()
    if df_fc.empty:
        st.info("Nessun dato previsione disponibile.")
    else:
        fc = df_fc.copy(); fc["TimeLocal"] = fc["Time"].dt.tz_convert(LOCAL_TZ)
        if "Wind_kmh" not in fc.columns and "Wind_mps" in fc.columns:
            fc["Wind_kmh"] = pd.to_numeric(fc["Wind_mps"], errors="coerce") * 3.6
        if "Temp_C" in fc.columns:
            st.plotly_chart(px.line(fc, x="TimeLocal", y="Temp_C", title="Temperatura prevista (°C)", template=template, markers=True), use_container_width=True)
        if "Pressure_hPa" in fc.columns:
            st.plotly_chart(px.line(fc, x="TimeLocal", y="Pressure_hPa", title="Pressione prevista (hPa)", template=template, markers=True), use_container_width=True)
        if "Wind_kmh" in fc.columns:
            st.plotly_chart(px.line(fc, x="TimeLocal", y="Wind_kmh", title="Vento previsto (km/h)", template=template, markers=True), use_container_width=True)
        if "Clouds" in fc.columns:
            st.plotly_chart(px.line(fc, x="TimeLocal", y="Clouds", title="Copertura nuvolosa (%)", template=template, markers=True), use_container_width=True)
        if "Rain_mm" in fc.columns:
            st.plotly_chart(px.bar(fc, x="TimeLocal", y="Rain_mm", title="Pioggia prevista (mm / 3h)", template=template), use_container_width=True)

# -------- Radar & Nuvole --------
with tab3:
    st.markdown("**Timeline locale**: ~2h passato + ~1h nowcast (RainViewer).")
    frames, now_idx = rv_frames()
    if not frames:
        st.info("Radar non disponibile al momento.")
    else:
        if "rv_idx" not in st.session_state: st.session_state["rv_idx"] = now_idx
        if st.session_state.get("rv_force_now"): st.session_state["rv_idx"] = now_idx; st.session_state["rv_force_now"] = False

        rv_idx = st.slider("Orario frame (locale)", 0, len(frames)-1, value=st.session_state["rv_idx"], key="rv_slider")
        st.session_state["rv_idx"] = rv_idx
        current = frames[rv_idx]
        ts = current["ts"]
        dt = pd.to_datetime(ts, unit="s", utc=True)

        if st.session_state.get("play_toggle", False) and len(frames) > 1:
            last_tick = st.session_state.get("rv_last_tick", 0.0); now = time.time()
            if now - last_tick >= (st.session_state["speed_ms"]/1000.0):
                st.session_state["rv_last_tick"] = now
                st.session_state["rv_idx"] = (st.session_state["rv_idx"] + 1) % len(frames)
                st.rerun()

        palette_idx = RADAR_PALETTES.get(st.session_state["radar_palette"], 5)
        radar_url = rv_tile(ts, palette_idx, st.session_state["radar_smooth"], st.session_state["radar_snow"])

        clouds_url = None
        if st.session_state["show_clouds"]:
            clouds_url = ow_clouds_tile(OW_API_KEY) if st.session_state["clouds_provider"].startswith("OpenWeather") else gibs_truecolor_tile(dt)

        lat = float(st.session_state["lat"]); lon = float(st.session_state["lon"]); zoom = int(st.session_state["zoom"])

        if st.session_state["renderer"].startswith("WebGL"):
            layers = []
            if st.session_state["show_radar"]:
                layers.append(pdk.Layer("TileLayer", data=radar_url, min_zoom=0, max_zoom=18, tile_size=256, opacity=st.session_state["radar_opacity"]))
            if clouds_url:
                layers.append(pdk.Layer("TileLayer", data=clouds_url, min_zoom=0, max_zoom=18, tile_size=256, opacity=st.session_state["clouds_opacity"]))
            if st.session_state["show_marker"]:
                layers.append(pdk.Layer("ScatterplotLayer", data=[{"lat":lat,"lon":lon}], get_position="[lon, lat]", get_radius=10000, pickable=False))
            deck = pdk.Deck(layers=layers, initial_view_state=pdk.ViewState(latitude=lat, longitude=lon, zoom=zoom), map_style=None, tooltip={"text": f"Radar: {current['label_local']}"})
            st.pydeck_chart(deck, use_container_width=True)
        else:
            if not HAS_FOLIUM:
                st.error("Renderer di compatibilità non disponibile: installa 'folium' e 'streamlit-folium'.")
            else:
                base = "CartoDB positron" if st.session_state["basemap"]=="Carto Positron" else "OpenStreetMap"
                m = folium.Map(location=[lat, lon], zoom_start=zoom, tiles=base, control_scale=True)
                if st.session_state["show_radar"]:
                    folium.raster_layers.TileLayer(tiles=radar_url, name=f"Radar {current['label_local']}", attr="RainViewer", overlay=True, control=True, opacity=st.session_state["radar_opacity"]).add_to(m)
                if clouds_url:
                    src = "OpenWeatherMap" if "openweathermap" in (clouds_url or "") else "NASA GIBS"
                    folium.raster_layers.TileLayer(tiles=clouds_url, name=f"Nuvole ({src})", attr=src, overlay=True, control=True, opacity=st.session_state["clouds_opacity"]).add_to(m)
                if st.session_state["show_marker"]:
                    folium.CircleMarker(location=[lat,lon], radius=6, color="#0ea5e9", fill=True).add_to(m)
                folium.LayerControl(collapsed=False).add_to(m)
                st_folium(m, height=660, use_container_width=True, key="compat_map")

        st.caption(f"Frame: {current['label_local']} • {rv_idx+1}/{len(frames)}  | Radar: {'ON' if st.session_state['show_radar'] else 'OFF'}  • Nuvole: {'ON' if st.session_state['show_clouds'] else 'OFF'} ({st.session_state['clouds_provider']})")

# -------------------- Footer --------------------
st.caption("© Meteo Dashboard • Radar RainViewer • Nuvole OWM/NASA • Cache 5 minuti")
