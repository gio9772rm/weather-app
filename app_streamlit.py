import os
from pathlib import Path
from datetime import datetime, timedelta, timezone
import time

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import subprocess
import requests  # per Radar & Allerte
import pydeck as pdk  # per mappa radar/satellite

# =========================
# Config
# =========================
st.set_page_config(page_title="Meteo • Dashboard", layout="wide", page_icon="🌦️")
load_dotenv()

DB_PATH = os.getenv("SQLITE_PATH", "./data/weather.db")
DB_URL = os.getenv("DATABASE_URL", "").strip()  # se presente (Render/Postgres) lo usiamo
LAT = os.getenv("LAT", "")
LON = os.getenv("LON", "")
OW_API_KEY = os.getenv("OW_API_KEY", "").strip()  # per tile nuvole OpenWeather (opzionale)

# ----------------- Style: header + CSS -----------------
st.markdown("""
<style>
:root { --radius: 16px; }
.block-container { padding-top: 0.5rem; }
.header {
  background: linear-gradient(135deg, #3b82f6 0%, #06b6d4 100%);
  border-radius: var(--radius);
  padding: 18px 20px; color: white; margin-bottom: 0.5rem;
}
.kpi { display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:10px;margin:8px 0 0; }
.kpi .card {
  background: #ffffff10; border: 1px solid #ffffff30; border-radius: var(--radius);
  padding: 12px 14px; color:#fff;
}
.small { opacity:.85; font-size:12px; margin-top:2px; }
</style>
""", unsafe_allow_html=True)

# =========================
# Data access
# =========================
def get_engine():
    if DB_URL:
        return create_engine(DB_URL, future=True)
    db = Path(DB_PATH)
    db.parent.mkdir(parents=True, exist_ok=True)
    return create_engine(f"sqlite:///{db}", future=True)

def read_table(table):
    try:
        eng = get_engine()
        return pd.read_sql_query(text(f"SELECT * FROM {table}"), eng)
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_station():
    df = read_table("station_3h")
    if df.empty:
        return df
    df["Time"] = pd.to_datetime(df["Time"], utc=True, errors="coerce")
    num = ["Temp_C","Humidity","Pressure_hPa","Wind_kmh","WindGust_kmh","Rain_mm"]
    for c in num:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["WindGust_kmh"] = df["WindGust_kmh"].fillna(df["Wind_kmh"])
    return df.sort_values("Time")

@st.cache_data(ttl=300)
def load_forecast():
    df = read_table("forecast_ow")
    if df.empty:
        return df
    df["Time"] = pd.to_datetime(df["Time"], utc=True, errors="coerce")
    num = ["Temp_C","Humidity","Pressure_hPa","Clouds","Wind_mps","WindDir","Rain_mm","Snow_mm"]
    for c in num:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df.sort_values("Time")

# =========================
# Ingest helpers
# =========================
def ensure_venv_and_run_ingest():
    """Crea .venv se manca, installa dipendenze minime e lancia weather_ingest.py."""
    venv_dir = Path(".venv")
    py_exe = venv_dir / "Scripts" / "python.exe"

    if not py_exe.exists():
        res = subprocess.run(["py", "-m", "venv", ".venv"], capture_output=True, text=True)
        if res.returncode != 0:
            return False, f"Errore creazione .venv:\n{res.stdout}\n{res.stderr}"

    cmds = [
        [str(py_exe), "-m", "pip", "install", "--upgrade", "pip"],
        [str(py_exe), "-m", "pip", "install", "streamlit", "python-dotenv", "pandas", "requests", "SQLAlchemy", "plotly", "pydeck"]
    ]
    for cmd in cmds:
        res = subprocess.run(cmd, capture_output=True, text=True)
        if res.returncode != 0:
            return False, f"Errore installazione pacchetti:\n{res.stdout}\n{res.stderr}"

    res = subprocess.run([str(py_exe), "weather_ingest.py"], capture_output=True, text=True, timeout=300)
    ok = (res.returncode == 0)
    out = (res.stdout or "") + "\n" + (res.stderr or "")
    return ok, out

def run_ingest_now():
    """Usa .venv se c'è, altrimenti lo crea."""
    venv_py = Path(".venv") / "Scripts" / "python.exe"
    if venv_py.exists():
        try:
            p = subprocess.run([str(venv_py), "weather_ingest.py"], capture_output=True, text=True, timeout=300)
            return (p.returncode == 0), (p.stdout or "") + "\n" + (p.stderr or "")
        except Exception as e:
            return False, str(e)
    else:
        return ensure_venv_and_run_ingest()

# =========================
# Radar helpers (RainViewer + OpenWeather)
# =========================
@st.cache_data(ttl=300)
def rainviewer_catalog():
    """Restituisce lista timestamps (epoch) disponibili per radar e satellite IR da RainViewer."""
    j = requests.get("https://api.rainviewer.com/public/weather-maps.json", timeout=15).json()
    radar_past = [f.get("time") for f in j.get("radar", {}).get("past", []) if "time" in f]
    radar_now = [f.get("time") for f in j.get("radar", {}).get("nowcast", []) if "time" in f]
    radar_times = sorted(set(radar_past + radar_now))
    sat_past = [f.get("time") for f in j.get("satellite", {}).get("infrared", {}).get("past", []) if "time" in f]
    sat_times = sorted(set(sat_past))
    return {"radar": radar_times, "sat": sat_times}

def rv_url(layer: str, ts: int, color: int = 3, smooth: int = 1, snow: int = 1) -> str:
    """Costruisce la URL template per tile RainViewer (radar/satellite)."""
    base = f"https://tilecache.rainviewer.com/v2/{layer}/{ts}/256/{{z}}/{{x}}/{{y}}/2/1_1.png"
    if layer == "radar":
        return f"{base}?color={color}&smooth={smooth}&snow={snow}"
    return base  # satellite ignora i parametri extra

def ow_clouds_url(appid: str) -> str:
    """Tile nuvole OpenWeather (opzionale)."""
    return f"https://tile.openweathermap.org/map/clouds_new/{{z}}/{{x}}/{{y}}.png?appid={appid}"

def ts_to_str(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

# =========================
# Allerte
# =========================
@st.cache_data(ttl=600)
def fetch_alerts(lat, lon):
    """
    1) Se OWM_ONECALL_KEY è presente → alerts da One Call 3.0
    2) Altrimenti, se METEOALARM_FEED è settato → tenta un feed alternativo
    """
    out = []
    onecall = os.getenv("OWM_ONECALL_KEY", "").strip()
    feed = os.getenv("METEOALARM_FEED", "").strip()

    if onecall and lat and lon:
        try:
            r = requests.get(
                "https://api.openweathermap.org/data/3.0/onecall",
                params={"lat": lat, "lon": lon, "appid": onecall, "units": "metric", "lang": "it", "exclude": "minutely,hourly,daily"},
                timeout=15
            )
            r.raise_for_status()
            alerts = r.json().get("alerts", []) or []
            for a in alerts:
                out.append({
                    "Fonte": a.get("sender_name") or "OpenWeather",
                    "Evento": a.get("event"),
                    "Inizio": pd.to_datetime(a.get("start"), unit="s", utc=True, errors="coerce"),
                    "Fine": pd.to_datetime(a.get("end"), unit="s", utc=True, errors="coerce"),
                    "Descrizione": a.get("description")
                })
            if out:
                df = pd.DataFrame(out)
                df["Inizio"] = df["Inizio"].dt.tz_convert("UTC").dt.strftime("%Y-%m-%d %H:%M UTC")
                df["Fine"] = df["Fine"].dt.tz_convert("UTC").dt.strftime("%Y-%m-%d %H:%M UTC")
                return df
        except Exception:
            pass

    if feed:
        try:
            resp = requests.get(feed, timeout=15)
            txt = resp.text
            # Tentativo semplice per estrarre titoli/descrizioni
            try:
                data = resp.json()
                items = data if isinstance(data, list) else data.get("items") or data.get("entries") or []
                for it in items:
                    out.append({
                        "Fonte": "Feed",
                        "Evento": it.get("title") or it.get("event") or "Allerta",
                        "Inizio": it.get("onset") or it.get("effective") or "",
                        "Fine": it.get("expires") or it.get("ends") or "",
                        "Descrizione": it.get("description") or it.get("summary") or ""
                    })
                if out:
                    return pd.DataFrame(out)
            except Exception:
                import re
                titles = re.findall(r"<title>(.*?)</title>", txt, flags=re.I|re.S)
                sums = re.findall(r"<summary>(.*?)</summary>|<description>(.*?)</description>", txt, flags=re.I|re.S)
                for i, t in enumerate(titles[:10]):
                    desc = ""
                    if i < len(sums):
                        pair = sums[i]
                        desc = pair[0] or pair[1] or ""
                    out.append({"Fonte":"Feed","Evento":t.strip(),"Inizio":"","Fine":"","Descrizione":re.sub("<.*?>","",desc).strip()})
                if out:
                    return pd.DataFrame(out)
        except Exception:
            pass

    return pd.DataFrame(columns=["Fonte","Evento","Inizio","Fine","Descrizione"])

# =========================
# Sidebar
# =========================
with st.sidebar:
    st.title("Meteo • Controls")

    # Tema grafici
    theme = st.radio("Tema grafici", ["Chiaro", "Scuro"], horizontal=True, index=1)
    template = "plotly_dark" if theme == "Scuro" else "plotly_white"

    # Intervallo osservazioni -> default 72h
    hours = st.slider("Ore da mostrare (osservazioni)", 6, 72, 72, step=6)

    # Selettori grafici
    charts = st.multiselect(
        "Mostra grafici",
        ["Temperatura", "Umidità", "Pressione", "Vento", "Pioggia"],
        default=["Temperatura", "Umidità", "Pressione", "Vento", "Pioggia"]
    )

    # Smooth: default attivo e al massimo
    smooth_on = st.checkbox("Smussa curve (media mobile)", value=True)
    window_requested = st.slider("Finestra smoothing (punti)", 3, 15, 15, step=2, disabled=not smooth_on)

    # Anti-piatto: se pochi punti, disattiva smoothing
    anti_flat = st.checkbox("Anti-piatto (niente smoothing se pochi punti)", value=True)
    min_points_for_smoothing = st.slider("Soglia punti per smoothing", 10, 120, 24, step=2, disabled=not anti_flat)

    # Stile step per temp/pressione
    step_style = st.checkbox("Stile linee 'step' per Temperatura e Pressione", value=False)

    # Aggiorna dati
    if st.button("🔄 Aggiorna dati ora"):
        ok, out = run_ingest_now()
        with st.expander("Log aggiornamento", expanded=True):
            st.code(out or "(nessun output)")
        if ok:
            st.success("Aggiornato! Ricarico i dati…")
            load_station.clear(); load_forecast.clear()
        else:
            st.error("Aggiornamento fallito (vedi log).")

    # Ultimo ingest (caption anche in header sotto)
    try:
        meta = pd.read_sql_query(text("SELECT v FROM meta WHERE k='last_ingest'"), get_engine())
        if not meta.empty:
            st.caption(f"Ultimo ingest: {meta.iloc[0,0]}")
    except Exception:
        pass

# =========================
# Header + badge aggiornamento
# =========================
with st.container():
    st.markdown('<div class="header"><h2>🌦️ Meteo Dashboard</h2><p style="margin:0">Stazione locale + OpenWeather • Roma</p></div>', unsafe_allow_html=True)

# Badge "Aggiornato X min fa"
try:
    meta = pd.read_sql_query(text("SELECT v FROM meta WHERE k='last_ingest'"), get_engine())
    if not meta.empty:
        ts = pd.to_datetime(meta.iloc[0,0], utc=True, errors="coerce")
        if pd.notna(ts):
            delta = pd.Timestamp.utcnow() - ts.tz_convert("UTC")
            mins = int(delta.total_seconds() // 60)
            st.markdown(f"**Aggiornato {mins} min fa** • {ts.strftime('%Y-%m-%d %H:%M UTC')}")
except Exception:
    pass

# =========================
# Tabs
# =========================
tab1, tab2, tab3 = st.tabs(["📈 Osservazioni", "🛰️ Previsioni", "🛰️ Radar & Allerte"])

# -------- Osservazioni (default 72h + smooth max) --------
with tab1:
    df_station = load_station()
    if df_station.empty:
        st.warning("Nessun dato stazione. Premi **Aggiorna dati ora** dalla sidebar.")
    else:
        now_utc = pd.Timestamp.utcnow()
        tmin = now_utc - pd.Timedelta(hours=hours)
        recent = df_station[df_station["Time"] >= tmin].copy()

        # KPI ultimo punto
        if not recent.empty:
            last = recent.iloc[-1]
            c1,c2,c3,c4,c5 = st.columns(5)
            c1.metric("🌡️ Temperatura", f"{last['Temp_C']:.1f} °C" if pd.notna(last["Temp_C"]) else "—")
            c2.metric("💧 Umidità", f"{last['Humidity']:.0f} %" if pd.notna(last["Humidity"]) else "—")
            c3.metric("⏱️ Pressione", f"{last['Pressure_hPa']:.1f} hPa" if pd.notna(last["Pressure_hPa"]) else "—")
            c4.metric("🍃 Vento", f"{last['Wind_kmh']:.1f} km/h" if pd.notna(last["Wind_kmh"]) else "—")
            c5.metric("🌧️ Pioggia 3h", f"{(last['Rain_mm'] or 0):.1f} mm" if pd.notna(last["Rain_mm"]) else "—")

        # Smoothing “intelligente” + Anti-piatto
        def smart_smooth(df, cols):
            if not smooth_on or df.empty:
                return df
            n = len(df)
            if anti_flat and n < min_points_for_smoothing:
                return df
            eff_window = max(3, min(window_requested, max(3, n // 3)))
            sdf = df.copy()
            for c in cols:
                if c in sdf.columns:
                    sdf[c] = sdf[c].rolling(window=eff_window, min_periods=1, center=True).mean()
            return sdf

        # Temperatura
        if "Temperatura" in charts:
            data = smart_smooth(recent, ["Temp_C"])
            fig = px.area(data, x="Time", y="Temp_C", title=f"Temperatura (°C) • ultime {hours} ore", template=template)
            if step_style:
                fig.update_traces(mode="lines+markers", line_shape="hv")
            else:
                fig.update_traces(mode="lines+markers")
            st.plotly_chart(fig, use_container_width=True)

        # Umidità
        if "Umidità" in charts:
            data = smart_smooth(recent, ["Humidity"])
            fig = px.line(data, x="Time", y="Humidity", title=f"Umidità (%) • ultime {hours} ore", template=template, markers=True)
            st.plotly_chart(fig, use_container_width=True)

        # Pressione
        if "Pressione" in charts:
            data = smart_smooth(recent, ["Pressure_hPa"])
            fig = px.line(data, x="Time", y="Pressure_hPa", title=f"Pressione (hPa) • ultime {hours} ore", template=template, markers=True)
            if step_style:
                fig.update_traces(line_shape="hv")
            st.plotly_chart(fig, use_container_width=True)

        # Vento
        if "Vento" in charts:
            data = recent.copy()
            data["WindGust_kmh"] = data["WindGust_kmh"].fillna(data["Wind_kmh"])
            data = smart_smooth(data, ["Wind_kmh","WindGust_kmh"])
            fig = px.line(data, x="Time", y=["Wind_kmh","WindGust_kmh"], title=f"Vento (km/h) • ultime {hours} ore", template=template, markers=True)
            st.plotly_chart(fig, use_container_width=True)

        # Pioggia
        if "Pioggia" in charts:
            data = recent.copy()
            data["Rain_mm"] = pd.to_numeric(data["Rain_mm"], errors="coerce").fillna(0)
            data = smart_smooth(data, ["Rain_mm"])
            fig = px.bar(data, x="Time", y="Rain_mm", title=f"Pioggia aggregata (mm / 3h) • ultime {hours} ore", template=template)
            st.plotly_chart(fig, use_container_width=True)

        with st.expander("🔎 Diagnostica dati (ultimi 20 punti)"):
            st.dataframe(recent.tail(20).reset_index(drop=True))

# -------- Previsioni --------
with tab2:
    df_fc = load_forecast()
    if df_fc.empty:
        st.info("Nessun dato previsione disponibile. Premi **Aggiorna dati ora**.")
    else:
        fc = df_fc.copy()
        fc["Wind_kmh"] = pd.to_numeric(fc["Wind_mps"], errors="coerce") * 3.6

        fig = px.line(fc, x="Time", y="Temp_C", title="Temperatura prevista (°C)", template=template, markers=True)
        st.plotly_chart(fig, use_container_width=True)

        fig = px.line(fc, x="Time", y="Pressure_hPa", title="Pressione prevista (hPa)", template=template, markers=True)
        st.plotly_chart(fig, use_container_width=True)

        fig = px.line(fc, x="Time", y="Wind_kmh", title="Vento previsto (km/h)", template=template, markers=True)
        st.plotly_chart(fig, use_container_width=True)

        fig = px.line(fc, x="Time", y="Clouds", title="Copertura nuvolosa (%)", template=template, markers=True)
        st.plotly_chart(fig, use_container_width=True)

        fig = px.bar(fc, x="Time", y="Rain_mm", title="Pioggia prevista (mm / 3h)", template=template)
        st.plotly_chart(fig, use_container_width=True)

# -------- Radar & Allerte --------
with tab3:
    st.subheader("🛰️ Radar pioggia + Nuvole")

    # Centro mappa
    lat = float(LAT) if LAT else 41.89
    lon = float(LON) if LON else 12.49

    # Catalogo RainViewer (timestamps)
    try:
        cat = rainviewer_catalog()
        radar_times_all = cat.get("radar", [])
        sat_times_all = cat.get("sat", [])
    except Exception as e:
        radar_times_all, sat_times_all = [], []
        st.warning(f"RainViewer non disponibile: {e}")

    # Selettori layer + opacità
    c0, c1, c2, c3 = st.columns([1,1,1,1])
    with c0:
        animate = st.checkbox("▶️ Anima", value=False, help="Scorre automaticamente gli ultimi frame")
    with c1:
        show_radar = st.checkbox("Mostra Radar (RainViewer)", value=True, help="Riflettività/precipitazioni")
        radar_opacity = st.slider("Opacità Radar", 0.0, 1.0, 0.9, 0.05, disabled=not show_radar)
    with c2:
        show_sat = st.checkbox("Mostra Satellite IR (RainViewer)", value=True, help="Copertura nuvolosa/IR")
        sat_opacity = st.slider("Opacità Satellite", 0.0, 1.0, 0.7, 0.05, disabled=not show_sat)
    with c3:
        show_ow = st.checkbox("Nuvole (OpenWeather)", value=bool(OW_API_KEY), help="Richiede OW_API_KEY")
        ow_opacity = st.slider("Opacità OW Clouds", 0.0, 1.0, 0.6, 0.05, disabled=not show_ow)

    # Finestra e velocità animazione
    c4, c5 = st.columns([2,1])
    with c4:
        last_n = st.slider("Numero di frame (ultimi N) da usare", 6, 24, 12, step=2)
    with c5:
        fps = st.slider("Velocità (frame/sec)", 1, 12, 4)

    # Sottoinsiemi di frame (ultimi N)
    radar_frames = radar_times_all[-last_n:] if radar_times_all else []
    sat_frames = sat_times_all[-last_n:] if sat_times_all else []

    # Inizializza e clamp indici in sessione
    if "radar_idx" not in st.session_state:
        st.session_state.radar_idx = max(0, len(radar_frames) - 1)
    st.session_state.radar_idx = min(st.session_state.radar_idx, max(0, len(radar_frames) - 1))

    if "sat_idx" not in st.session_state:
        st.session_state.sat_idx = max(0, len(sat_frames) - 1)
    st.session_state.sat_idx = min(st.session_state.sat_idx, max(0, len(sat_frames) - 1))

    # Slider manuali
    col_r, col_s = st.columns(2)
    if show_radar and radar_frames:
        st.session_state.radar_idx = col_r.slider(
            "Frame Radar (più a destra = più recente)",
            0, len(radar_frames)-1, st.session_state.radar_idx, key="radar_slider"
        )
        radar_ts = radar_frames[st.session_state.radar_idx]
        col_r.caption(f"Radar: {ts_to_str(radar_ts)}")
    else:
        radar_ts = None
        col_r.caption("Radar: nessun frame disponibile")

    if show_sat and sat_frames:
        st.session_state.sat_idx = col_s.slider(
            "Frame Satellite IR",
            0, len(sat_frames)-1, st.session_state.sat_idx, key="sat_slider"
        )
        sat_ts = sat_frames[st.session_state.sat_idx]
        col_s.caption(f"Satellite: {ts_to_str(sat_ts)}")
    else:
        sat_ts = None
        col_s.caption("Satellite: nessun frame disponibile")

    # Costruzione layer mappa
    layers = []

    # Basemap OSM (semplice)
    layers.append(pdk.Layer(
        "TileLayer",
        data="https://tile.openstreetmap.org/{z}/{x}/{y}.png",
        min_zoom=0, max_zoom=19, tile_size=256, opacity=1.0
    ))

    if show_radar and radar_ts:
        layers.append(pdk.Layer(
            "TileLayer",
            data=rv_url("radar", radar_ts),
            min_zoom=0, max_zoom=18, tile_size=256, opacity=radar_opacity
        ))

    if show_sat and sat_ts:
        layers.append(pdk.Layer(
            "TileLayer",
            data=rv_url("satellite", sat_ts),
            min_zoom=0, max_zoom=18, tile_size=256, opacity=sat_opacity
        ))

    if show_ow and OW_API_KEY:
        layers.append(pdk.Layer(
            "TileLayer",
            data=ow_clouds_url(OW_API_KEY),
            min_zoom=0, max_zoom=18, tile_size=256, opacity=ow_opacity
        ))

    view_state = pdk.ViewState(latitude=lat, longitude=lon, zoom=6)
    deck = pdk.Deck(layers=layers, initial_view_state=view_state, map_style=None,
                    tooltip={"text": "Radar/Satellite"})
    st.pydeck_chart(deck, use_container_width=True)

    # Animazione: incrementa indici e ricarica
    if animate and ((show_radar and len(radar_frames) > 1) or (show_sat and len(sat_frames) > 1)):
        # attesa in secondi per il prossimo frame
        time.sleep(1.0 / max(1, fps))
        if show_radar and len(radar_frames) > 1:
            st.session_state.radar_idx = (st.session_state.radar_idx + 1) % len(radar_frames)
        if show_sat and len(sat_frames) > 1:
            st.session_state.sat_idx = (st.session_state.sat_idx + 1) % len(sat_frames)
        st.rerun()

    st.markdown("---")
    st.subheader("🚨 Allerte meteo")
    try:
        df_alerts = fetch_alerts(os.getenv("LAT"), os.getenv("LON"))
        if df_alerts is not None and not df_alerts.empty:
            st.dataframe(df_alerts, use_container_width=True, hide_index=True)
        else:
            st.success("Nessuna allerta attiva per l'area configurata.")
            st.caption("Suggerimento: imposta OWM_ONECALL_KEY oppure METEOALARM_FEED nel file .env")
    except Exception as e:
        st.warning(f"Errore lettura allerte: {e}")

# -------- Mappa semplice posizione (facoltativa) --------
try:
    if LAT and LON:
        lat = float(LAT); lon = float(LON)
        st.subheader("📍 Posizione stazione")
        st.map(pd.DataFrame({"lat":[lat], "lon":[lon]}), zoom=11)
except Exception:
    pass

st.caption("© La tua stazione + OpenWeather • aggiornamento automatico via Cron/Render")
