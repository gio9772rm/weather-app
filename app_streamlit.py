
# app_streamlit.py — Meteo Dashboard (WebGL + Compatibilità, timeline radar, nuvole, preferenze)
import os, json, time
from pathlib import Path
from datetime import datetime, timedelta, timezone

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import requests

# Renderer WebGL
import pydeck as pdk
# Renderer compatibilità (Leaflet)
try:
    import folium
    from streamlit_folium import st_folium
    HAS_FOLIUM = True
except Exception:
    HAS_FOLIUM = False

# =========================
# UI helpers
# =========================
def _keep_scroll():
    st.components.v1.html(
        """<script>
        const KEY='scrollY_weather_app';
        window.addEventListener('load',()=>{
            const y=sessionStorage.getItem(KEY);
            if(y!==null){ window.scrollTo(0, parseFloat(y)); }
        });
        window.addEventListener('scroll',()=>{
            sessionStorage.setItem(KEY, String(window.scrollY||window.pageYOffset||0));
        });
        </script>""", height=0)

# =========================
# Setup & ENV
# =========================
st.set_page_config(page_title="Meteo • Dashboard", layout="wide", page_icon="🌦️")
load_dotenv()

DB_PATH = os.getenv("SQLITE_PATH", "./data/weather.db")
DB_URL = (os.getenv("DATABASE_URL", "") or "").strip()
if DB_URL and "USER:PASS@HOST:PORT/DBNAME" in DB_URL:
    DB_URL = ""  # placeholder → usa SQLite in locale

OW_API_KEY = (os.getenv("OW_API_KEY") or "").strip()  # tiles/nuvole
OWM_ONECALL_KEY = (os.getenv("OWM_ONECALL_KEY") or "").strip()  # allerte 3.0
LAT = (os.getenv("LAT") or "").strip()
LON = (os.getenv("LON") or "").strip()

LOCAL_TZ = "Europe/Rome"

# =========================
# Helpers: DB & Prefs
# =========================
def get_engine():
    if DB_URL:
        return create_engine(DB_URL, future=True)
    p = Path(DB_PATH); p.parent.mkdir(parents=True, exist_ok=True)
    return create_engine(f"sqlite:///{p}", future=True)

def ensure_prefs_table():
    try:
        with get_engine().begin() as cx:
            cx.execute(text("""
                CREATE TABLE IF NOT EXISTS user_prefs (
                  k TEXT PRIMARY KEY,
                  v TEXT
                )
            """))
    except Exception:
        pass

def load_prefs():
    ensure_prefs_table()
    try:
        with get_engine().begin() as cx:
            rows = cx.execute(text("SELECT k, v FROM user_prefs")).fetchall()
        out = {}
        for k, v in rows:
            try:
                out[k] = json.loads(v)
            except Exception:
                out[k] = v
        return out
    except Exception:
        return {}

def save_prefs(d: dict):
    ensure_prefs_table()
    with get_engine().begin() as cx:
        for k, v in d.items():
            cx.execute(text("""
                INSERT INTO user_prefs (k, v) VALUES (:k, :v)
                ON CONFLICT (k) DO UPDATE SET v=excluded.v
            """), {"k": k, "v": json.dumps(v)})

PREFS = load_prefs()

def pref(key, default):
    return PREFS.get(key, default)


st.markdown("""
<style>
:root { --radius: 16px; }
.block-container { padding-top: 0.5rem; }
.header {
  background: linear-gradient(135deg, #3b82f6 0%, #06b6d4 100%);
  border-radius: var(--radius);
  padding: 18px 20px; color: white; margin-bottom: 0.5rem;
}
.smallcaps { font-variant: all-small-caps; opacity: .8; }
</style>
""", unsafe_allow_html=True)
_keep_scroll()

# =========================
# Data access
# =========================
def read_table(table):
    try:
        return pd.read_sql_query(text(f"SELECT * FROM {table}"), get_engine())
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_station():
    df = read_table("station_3h")
    if df.empty:
        df = read_table("station_30m")
    if df.empty:
        df = read_table("station_raw")  # fallback
    if df.empty:
        return df
    df["Time"] = pd.to_datetime(df["Time"], utc=True, errors="coerce")
    num = ["Temp_C","Humidity","Pressure_hPa","Wind_kmh","WindGust_kmh","Rain_mm"]
    for c in num:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    if "WindGust_kmh" in df.columns and "Wind_kmh" in df.columns:
        df["WindGust_kmh"] = df["WindGust_kmh"].fillna(df["Wind_kmh"])
    return df.sort_values("Time")

@st.cache_data(ttl=300)
def load_forecast():
    df = read_table("forecast_ow")
    if df.empty:
        return df
    df["Time"] = pd.to_datetime(df["Time"], utc=True, errors="coerce")
    for c in ["Temp_C","Humidity","Pressure_hPa","Clouds","Wind_mps","WindDir","Rain_mm","Snow_mm"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df.sort_values("Time")

# =========================
# Radar & Clouds
# =========================
@st.cache_data(ttl=120)
def rv_frames():
    j = requests.get("https://api.rainviewer.com/public/weather-maps.json", timeout=10).json()
    radar = j.get("radar") or {}
    past = radar.get("past") or []
    nowcast = radar.get("nowcast") or radar.get("future") or []
    frames = past + nowcast
    if not frames:
        return [], -1
    out = []
    for f in frames:
        ts = int(f.get("time"))
        local = pd.to_datetime(ts, unit="s", utc=True).tz_convert(LOCAL_TZ).strftime("%H:%M")
        url = f"https://tilecache.rainviewer.com/v2/radar/{ts}/256/{{z}}/{{x}}/{{y}}/2/1_1.png?color=5&smooth=1&snow=1"
        out.append({"ts": ts, "url": url, "label_local": local})
    now_idx = len(past) - 1 if past else len(out) - 1
    if now_idx < 0: now_idx = len(out) - 1
    return out, now_idx

def ow_clouds_tile(api_key: str):
    if not api_key:
        return None
    return f"https://tile.openweathermap.org/map/clouds_new/{{z}}/{{x}}/{{y}}.png?appid={api_key}"

# =========================
# Sidebar
# =========================
with st.sidebar:
    st.title("Impostazioni")

    # Sezione: Visuale generale
    st.markdown("#### Visuale")
    theme = st.radio("Tema grafici", ["Chiaro", "Scuro"], horizontal=True, index=0 if pref("theme","Chiaro")=="Chiaro" else 1, key="ui_theme")
    template = "plotly_white" if theme=="Chiaro" else "plotly_dark"

    hours = st.slider("Ore osservazioni", 6, 96, int(pref("hours", 72)), step=6, key="hours")

    st.markdown("#### Grafici")
    charts_default = ["Temperatura","Umidità","Pressione","Vento","Pioggia"]
    charts = st.multiselect("Mostra", charts_default, default=pref("charts", charts_default), key="charts")

    st.markdown("#### Radar & Nuvole")
    renderer = st.radio("Renderer", ["WebGL (veloce)", "Compatibilità (massima)"], index=0 if pref("renderer","WebGL (veloce)")=="WebGL (veloce)" else 1, key="renderer")
    basemap = st.selectbox("Basemap (compatibilità)", ["Carto Positron","OpenStreetMap"], index=0 if pref("basemap","Carto Positron")=="Carto Positron" else 1, key="basemap")
    show_radar = st.checkbox("Mostra radar pioggia", value=bool(pref("show_radar", True)), key="show_radar")
    show_clouds = st.checkbox("Mostra nuvole (OWM)", value=bool(pref("show_clouds", bool(OW_API_KEY))), key="show_clouds")
    radar_opacity = st.slider("Opacità radar", 0.0, 1.0, float(pref("radar_opacity", 0.7)), 0.05, key="radar_opacity")
    clouds_opacity = st.slider("Opacità nuvole", 0.0, 1.0, float(pref("clouds_opacity", 0.55)), 0.05, key="clouds_opacity")

    st.markdown("#### Playback")
    speed_ms = st.slider("Velocità (ms/frame)", 150, 1200, int(pref("speed_ms", 400)), 50, key="speed_ms")
    play = st.toggle("▶️ Play", value=bool(pref("play", False)), key="play_toggle")
    if st.button("⏮️ Torna adesso"):
        st.session_state["rv_force_now"] = True

    st.markdown("#### Mappa")
    try:
        lat_default = float(pref("lat", LAT or 41.9))
        lon_default = float(pref("lon", LON or 12.5))
    except Exception:
        lat_default, lon_default = 41.9, 12.5
    lat = st.number_input("Latitudine", value=lat_default, format="%.6f", key="lat")
    lon = st.number_input("Longitudine", value=lon_default, format="%.6f", key="lon")
    zoom = st.slider("Zoom iniziale", 3, 12, int(pref("zoom", 7)), 1, key="zoom")

    st.markdown("#### Azioni")
    colA, colB = st.columns(2)
    if colA.button("💾 Salva impostazioni"):
        save_prefs({
            "theme": theme, "hours": hours, "charts": charts,
            "renderer": renderer, "basemap": basemap,
            "show_radar": show_radar, "show_clouds": show_clouds,
            "radar_opacity": radar_opacity, "clouds_opacity": clouds_opacity,
            "speed_ms": speed_ms, "lat": lat, "lon": lon, "zoom": zoom,
            "play": st.session_state.get("play_toggle", False)
        })
        st.success("Preferenze salvate")

    if colB.button("🔄 Aggiorna dati ora"):
        st.toast("Avvia ingest manuale... (implementazione opzionale)")

# =========================
# Header
# =========================
with st.container():
    st.markdown('<div class="header"><h2>🌦️ Meteo Dashboard</h2><p class="smallcaps">Stazione locale + OpenWeather</p></div>', unsafe_allow_html=True)

# Badge "ultima ingest"
try:
    meta = pd.read_sql_query(text("SELECT v FROM meta WHERE k='last_ingest'"), get_engine())
    if not meta.empty:
        ts = pd.to_datetime(meta.iloc[0,0], utc=True, errors="coerce")
        if pd.notna(ts):
            delta = pd.Timestamp.utcnow() - ts.tz_convert("UTC")
            mins = int(delta.total_seconds() // 60)
            st.caption(f"⏱️ Ultimo ingest Ecowitt: {mins} min fa • {ts.strftime('%Y-%m-%d %H:%M UTC')}")
except Exception:
    pass

# =========================
# Tabs
# =========================
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
        recent["TimeLocal"] = recent["Time"].dt.tz_convert(LOCAL_TZ)

        # KPI ultimo punto
        last = recent.tail(1)
        if not last.empty:
            last = last.iloc[0]
            c1,c2,c3,c4,c5 = st.columns(5)
            c1.metric("🌡️ Temp", f"{last['Temp_C']:.1f} °C" if pd.notna(last["Temp_C"]) else "—")
            c2.metric("💧 UR", f"{last['Humidity']:.0f} %" if pd.notna(last["Humidity"]) else "—")
            c3.metric("⏱️ Press", f"{last['Pressure_hPa']:.1f} hPa" if pd.notna(last["Pressure_hPa"]) else "—")
            c4.metric("🍃 Vento", f"{last['Wind_kmh']:.1f} km/h" if pd.notna(last["Wind_kmh"]) else "—")
            c5.metric("🌧️ Pioggia 3h", f"{(last['Rain_mm'] or 0):.1f} mm" if pd.notna(last["Rain_mm"]) else "—")

        def smooth(df, cols):
            if df.empty: return df
            n = len(df)
            eff = max(3, min(15, max(3, n//3)))
            out = df.copy()
            for c in cols:
                if c in out.columns:
                    out[c] = out[c].rolling(eff, min_periods=1, center=True).mean()
            return out

        # grafici
        if "Temperatura" in st.session_state["charts"] and "Temp_C" in recent.columns:
            d = smooth(recent, ["Temp_C"])
            fig = px.area(d, x="TimeLocal", y="Temp_C", template=template, title="Temperatura (°C)")
            fig.update_traces(mode="lines+markers")
            st.plotly_chart(fig, use_container_width=True)

        if "Umidità" in st.session_state["charts"] and "Humidity" in recent.columns:
            d = smooth(recent, ["Humidity"])
            fig = px.line(d, x="TimeLocal", y="Humidity", template=template, title="Umidità (%)", markers=True)
            st.plotly_chart(fig, use_container_width=True)

        if "Pressione" in st.session_state["charts"] and "Pressure_hPa" in recent.columns:
            d = smooth(recent, ["Pressure_hPa"])
            fig = px.line(d, x="TimeLocal", y="Pressure_hPa", template=template, title="Pressione (hPa)", markers=True)
            st.plotly_chart(fig, use_container_width=True)

        if "Vento" in st.session_state["charts"]:
            cols = [c for c in ["Wind_kmh","WindGust_kmh"] if c in recent.columns]
            if cols:
                d = smooth(recent, cols)
                fig = px.line(d, x="TimeLocal", y=cols, template=template, title="Vento (km/h)", markers=True)
                st.plotly_chart(fig, use_container_width=True)

        if "Pioggia" in st.session_state["charts"] and "Rain_mm" in recent.columns:
            d = recent.copy()
            d["Rain_mm"] = pd.to_numeric(d["Rain_mm"], errors="coerce").fillna(0)
            fig = px.bar(d, x="TimeLocal", y="Rain_mm", template=template, title="Pioggia aggregata (mm / 3h)")
            st.plotly_chart(fig, use_container_width=True)

# -------- Previsioni --------
with tab2:
    df_fc = load_forecast()
    if df_fc.empty:
        st.info("Nessun dato previsione disponibile.")
    else:
        fc = df_fc.copy()
        fc["TimeLocal"] = fc["Time"].dt.tz_convert(LOCAL_TZ)
        if "Wind_mps" in fc.columns:
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
    frames, now_idx = rv_frames()
    if not frames:
        st.info("Radar non disponibile al momento.")
    else:
        # Stato timeline
        if "rv_idx" not in st.session_state:
            st.session_state["rv_idx"] = now_idx
        if st.session_state.get("rv_force_now"):
            st.session_state["rv_idx"] = now_idx
            st.session_state["rv_force_now"] = False

        rv_idx = st.slider("Orario frame (locale)", 0, len(frames)-1, value=st.session_state["rv_idx"], key="rv_slider")
        st.session_state["rv_idx"] = rv_idx
        current = frames[rv_idx]

        # Playback
        if st.session_state.get("play_toggle", False) and len(frames) > 1:
            last_tick = st.session_state.get("rv_last_tick", 0.0)
            now = time.time()
            if now - last_tick >= (st.session_state["speed_ms"]/1000.0):
                st.session_state["rv_last_tick"] = now
                st.session_state["rv_idx"] = (st.session_state["rv_idx"] + 1) % len(frames)
                st.experimental_rerun()

        lat = float(st.session_state["lat"]); lon = float(st.session_state["lon"]); zoom = int(st.session_state["zoom"])

        if st.session_state["renderer"].startswith("WebGL"):
            layers = []
            if st.session_state["show_radar"]:
                layers.append(pdk.Layer("TileLayer", data=current["url"], min_zoom=0, max_zoom=18, tile_size=256, opacity=st.session_state["radar_opacity"]))
            if st.session_state["show_clouds"] and OW_API_KEY:
                layers.append(pdk.Layer("TileLayer", data=ow_clouds_tile(OW_API_KEY), min_zoom=0, max_zoom=18, tile_size=256, opacity=st.session_state["clouds_opacity"]))
            view_state = pdk.ViewState(latitude=lat, longitude=lon, zoom=zoom)
            deck = pdk.Deck(layers=layers, initial_view_state=view_state, map_style=None, tooltip={"text": f"Radar: {current['label_local']}"})
            st.pydeck_chart(deck, use_container_width=True)
        else:
            if not HAS_FOLIUM:
                st.error("Renderer di compatibilità non disponibile: installa 'folium' e 'streamlit-folium'.")
            else:
                base = "CartoDB positron" if st.session_state["basemap"]=="Carto Positron" else "OpenStreetMap"
                m = folium.Map(location=[lat, lon], zoom_start=zoom, tiles=base, control_scale=True)
                if st.session_state["show_radar"]:
                    folium.raster_layers.TileLayer(tiles=current["url"], name=f"Radar {current['label_local']}",
                                                   attr="RainViewer", overlay=True, control=True, opacity=st.session_state["radar_opacity"]).add_to(m)
                if st.session_state["show_clouds"] and OW_API_KEY:
                    folium.raster_layers.TileLayer(tiles=ow_clouds_tile(OW_API_KEY), name="Nuvole (OWM)",
                                                   attr="OpenWeatherMap", overlay=True, control=True, opacity=st.session_state["clouds_opacity"]).add_to(m)
                folium.LayerControl(collapsed=False).add_to(m)
                st_folium(m, height=650, use_container_width=True, key="compat_map")

        st.caption(f"Frame: {current['label_local']} • {rv_idx+1}/{len(frames)}  |  Radar: {'ON' if st.session_state['show_radar'] else 'OFF'}  • Nuvole: {'ON' if (st.session_state['show_clouds'] and OW_API_KEY) else 'OFF'}")

        st.markdown("---")
        st.subheader("🚨 Allerte meteo")
        if not OWM_ONECALL_KEY:
            st.info("Per le allerte abilita `OWM_ONECALL_KEY` nel file .env (One Call 3.0).")
        else:
            try:
                r = requests.get("https://api.openweathermap.org/data/3.0/onecall",
                                 params={"lat": lat, "lon": lon, "appid": OWM_ONECALL_KEY, "units": "metric",
                                         "lang": "it", "exclude": "minutely,hourly,daily"}, timeout=15)
                r.raise_for_status()
                alerts = r.json().get("alerts", []) or []
                if alerts:
                    rows = []
                    for a in alerts:
                        rows.append({
                            "Fonte": a.get("sender_name") or "OpenWeather",
                            "Evento": a.get("event"),
                            "Inizio": pd.to_datetime(a.get("start"), unit="s", utc=True, errors="coerce").tz_convert(LOCAL_TZ).strftime("%d %b %H:%M"),
                            "Fine": pd.to_datetime(a.get("end"), unit="s", utc=True, errors="coerce").tz_convert(LOCAL_TZ).strftime("%d %b %H:%M"),
                            "Descrizione": a.get("description")
                        })
                    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
                else:
                    st.success("Nessuna allerta attiva per l'area selezionata.")
            except requests.HTTPError as e:
                if e.response is not None and e.response.status_code == 401:
                    st.error("Allerte non disponibili: chiave One Call 3.0 non valida o piano non abilitato (401).")
                    st.caption("Imposta una chiave valida in .env come OWM_ONECALL_KEY oppure lascia vuoto per nascondere questa sezione.")
                else:
                    st.warning(f"Errore allerte: {e}")

# Footer
st.caption("© Meteo Dashboard • Radar RainViewer • Nuvole OpenWeather • Preferenze persistenti su DB")
