# app_streamlit.py
import os
from pathlib import Path
from datetime import datetime, timedelta, timezone

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import subprocess

# =========================
# Config
# =========================
st.set_page_config(page_title="Meteo • Dashboard", layout="wide", page_icon="🌦️")
load_dotenv()

DB_PATH = os.getenv("SQLITE_PATH", "./data/weather.db")
LAT = os.getenv("LAT", "")
LON = os.getenv("LON", "")

# ----------------- Style: header + CSS -----------------
st.markdown("""
<style>
:root { --radius: 16px; }
.block-container { padding-top: 1rem; }
.header {
  background: linear-gradient(135deg, #4f46e5 0%, #06b6d4 100%);
  border-radius: var(--radius);
  padding: 18px 20px; color: white; margin-bottom: 1rem;
}
.kpi-card {
  background: #ffffff10;
  border: 1px solid #ffffff30;
  border-radius: var(--radius);
  padding: 14px 16px;
}
</style>
""", unsafe_allow_html=True)

# =========================
# Data access
# =========================
def get_engine():
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
        [str(py_exe), "-m", "pip", "install", "streamlit", "python-dotenv", "pandas", "requests", "SQLAlchemy", "plotly"]
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

    # Ultimo ingest
    try:
        meta = pd.read_sql_query(text("SELECT v FROM meta WHERE k='last_ingest'"), get_engine())
        if not meta.empty:
            st.caption(f"Ultimo ingest: {meta.iloc[0,0]}")
    except Exception:
        pass

# =========================
# Header
# =========================
with st.container():
    st.markdown('<div class="header"><h2>🌦️ Meteo Dashboard</h2><p style="margin:0">Stazione locale + OpenWeather • Roma</p></div>', unsafe_allow_html=True)

# =========================
# Tabs
# =========================
tab1, tab2 = st.tabs(["📈 Osservazioni", "🛰️ Previsioni"])

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
                # troppi pochi punti: niente smoothing
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
        # vento in km/h per grafico
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

# -------- Mappa --------
try:
    if LAT and LON:
        lat = float(LAT); lon = float(LON)
        st.subheader("📍 Posizione stazione")
        st.map(pd.DataFrame({"lat":[lat], "lon":[lon]}), zoom=11)
except Exception:
    pass

st.caption("© La tua stazione + OpenWeather • aggiornamento consigliato ogni ora")
