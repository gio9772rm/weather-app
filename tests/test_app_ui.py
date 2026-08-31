from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st
from sqlalchemy import text
from streamlit.testing.v1 import AppTest

from db import ensure_schema, get_engine, reset_engine_cache


def test_theme_css_covers_streamlit_native_widget_text() -> None:
    source = (Path(__file__).parents[1] / "app_streamlit.py").read_text(
        encoding="utf-8"
    )

    for selector in (
        '[data-testid="stHeader"]',
        '[data-testid="stRadio"] label p',
        'button[data-baseweb="tab"] *',
        '[data-testid="stTab"] *',
        '[data-testid="stMetricDelta"] *',
        '[data-testid="stSidebarCollapseButton"] svg [fill="none"]',
        '[data-testid="stMarkdownContainer"] .hero *',
        ".hero-v4",
        ".current-grid",
        ".hourly-strip",
        ".hourly-cloud",
        ".hourly-strip::-webkit-scrollbar-thumb",
        "--weather-icon-bg",
        ".insight-grid",
        ".activity-grid",
        ".expandable-card",
        ".card-expanded",
        ".air-grid",
        ".live-badge",
        "@keyframes live-pulse",
        '[data-testid="stNumberInputContainer"]',
        '[data-testid="stNumberInputField"]',
        '[data-testid="stBaseButton-secondaryFormSubmit"]',
        '[data-testid="stCaptionContainer"] *',
        "_use_local_subplot_keys",
    ):
        assert selector in source
    assert '<details class="current-card expandable-card">' in source
    assert '<details class="activity-card expandable-card' in source
    assert '<details class="air-card expandable-card' in source
    assert 'content:"↓"' in source
    assert 'content:"\\2193"' not in source
    assert "Controllo automatico" in source
    assert "Verifica automatica · ogni" in source
    assert "/AladinLite/api/v3/3.8.1/aladin.js" in source
    assert "Atlante CDS non disponibile in questo browser o rete" in source
    assert "/AladinLite/api/v3/latest/aladin.js" not in source
    today = source[
        source.index("def render_today_dashboard") : source.index(
            "def _city_future_hours"
        )
    ]
    assert today.index("render_v4_activities") < today.index("render_official_alerts")
    assert "lat={CFG.latitude:.4f}" not in source


def test_app_opens_local_dashboard_and_city_search(
    tmp_path, monkeypatch, request
) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("SQLITE_PATH", str(tmp_path / "app-ui.db"))
    reset_engine_cache()
    request.addfinalizer(reset_engine_cache)

    app_path = Path(__file__).parents[1] / "app_streamlit.py"
    app = AppTest.from_file(app_path, default_timeout=30).run()

    assert not app.exception
    assert [tab.label for tab in app.tabs] == [
        "Oggi",
        "Panoramica",
        "7 giorni",
        "Stazione",
        "Aria",
        "Astronomia",
        "Radar",
        "Sistema",
    ]
    assert any("hero-v4" in item.value for item in app.markdown)
    assert any("Pianifica la giornata" in item.value for item in app.markdown)
    assert any("Pollini" in item.value for item in app.markdown)
    assert any("Luna e cielo" in item.value for item in app.markdown)
    assert not any("Bicicletta" in item.value for item in app.markdown)
    assert not any("Bucato" in item.value for item in app.markdown)
    assert any(
        "Apri questa scheda per caricare la previsione ambientale" in item.value
        for item in app.caption
    )

    app.sidebar.radio[0].set_value("Meteo città").run()

    assert not app.exception
    assert app.sidebar.text_input[0].label == "Nome o CAP"
    assert any("Che tempo fa altrove?" in item.value for item in app.markdown)


def test_simple_and_expert_modes_are_available(tmp_path, monkeypatch, request) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("SQLITE_PATH", str(tmp_path / "app-mode.db"))
    reset_engine_cache()
    request.addfinalizer(reset_engine_cache)

    app_path = Path(__file__).parents[1] / "app_streamlit.py"
    app = AppTest.from_file(app_path, default_timeout=30).run()

    assert not app.exception
    assert app.sidebar.radio[1].label == "Livello di dettaglio"
    assert app.sidebar.radio[1].value == "Semplice"
    app.sidebar.radio[1].set_value("Esperta").run()
    assert not app.exception
    assert app.sidebar.radio[1].value == "Esperta"


def test_recent_station_cards_show_real_live_badges(
    tmp_path, monkeypatch, request
) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("SQLITE_PATH", str(tmp_path / "app-live.db"))
    reset_engine_cache()
    st.cache_data.clear()
    request.addfinalizer(st.cache_data.clear)
    request.addfinalizer(reset_engine_cache)
    ensure_schema()
    now = pd.Timestamp.now(tz="UTC")
    older = (now - pd.Timedelta(hours=4)).strftime("%Y-%m-%dT%H:%M:%SZ")
    current = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    with get_engine().begin() as connection:
        connection.execute(
            text(
                "INSERT INTO station_raw ("
                "time,temp_c,humidity,pressure_hpa,wind_kmh,windgust_kmh,"
                "winddir,rain_mm,rain_rate_mm_h,solar_w_m2,uv_index,source,data_quality"
                ") VALUES ("
                ":time,23.8,57,1013,4,7,170,0,0,150,1,'test','ok')"
            ),
            {"time": older},
        )
        connection.execute(
            text(
                "INSERT INTO station_raw ("
                "time,temp_c,humidity,pressure_hpa,wind_kmh,windgust_kmh,"
                "winddir,rain_mm,rain_rate_mm_h,solar_w_m2,uv_index,source,data_quality"
                ") VALUES ("
                ":time,24.2,55,1014,5,8,180,NULL,NULL,NULL,NULL,'test','ok')"
            ),
            {"time": current},
        )

    app = AppTest.from_file(
        Path(__file__).parents[1] / "app_streamlit.py", default_timeout=30
    ).run()

    assert not app.exception
    rendered = "\n".join(item.value for item in app.markdown)
    assert rendered.count('<details class="current-card expandable-card">') == 6
    assert "live-badge is-live" in rendered
    assert "live-badge is-stale" in rendered
    assert "NON LIVE" in rendered
    assert "�" not in rendered


def test_astronomy_pro_planner_renders_with_forecast_data(
    tmp_path, monkeypatch, request
) -> None:
    from light_pollution import LightPollutionError

    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("SQLITE_PATH", str(tmp_path / "app-astronomy.db"))
    monkeypatch.setattr(
        "light_pollution.fetch_light_pollution",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            LightPollutionError("fixture offline")
        ),
    )
    reset_engine_cache()
    st.cache_data.clear()
    request.addfinalizer(st.cache_data.clear)
    request.addfinalizer(reset_engine_cache)
    ensure_schema()
    now = pd.Timestamp.now(tz="UTC").floor("h")
    rows = []
    for offset in range(-3, 49):
        moment = now + pd.Timedelta(hours=offset)
        rows.append(
            {
                "valid_time": moment.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "issued_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "is_day": int(7 <= moment.tz_convert("Europe/Rome").hour < 20),
            }
        )
    with get_engine().begin() as connection:
        connection.execute(
            text(
                "INSERT INTO forecast_blend (valid_time,issued_at,temp_c,humidity,"
                "dewpoint_c,pressure_hpa,wind_kmh,wind_gust_kmh,rain_mm,"
                "precip_probability,clouds,cloud_low,cloud_mid,cloud_high,visibility_m,"
                "is_day,confidence,provider_count,method) VALUES ("
                ":valid_time,:issued_at,18,65,11,1013,6,10,0,10,20,10,15,20,18000,"
                ":is_day,85,2,'ui_fixture')"
            ),
            rows,
        )

    app = AppTest.from_file(
        Path(__file__).parents[1] / "app_streamlit.py", default_timeout=30
    )
    app.query_params["tab"] = "astronomy"
    app.run()

    assert not app.exception
    assert any(item.value == "Pianificatore Astronomia Pro" for item in app.subheader)
    assert {
        "Oggetto personalizzato · RA/Dec",
        "Attrezzatura e campo inquadrato",
        "Orizzonte locale · ostacoli",
        "Esporta o ripristina la configurazione",
    } <= {item.label for item in app.expander}
    planner_markup = "\n".join(item.value for item in app.markdown)
    assert "Piano della notte" in planner_markup
    assert any(item.label == "Notte che inizia il" for item in app.date_input)
    assert {"Dalle", "Alle"} <= {item.label for item in app.time_input}
    active_profile = next(
        item for item in app.selectbox if item.label == "Profilo attivo"
    )
    assert active_profile.value == "Setup Tripletto"
    assert any(item.label == "Pannello inferiore del grafico" for item in app.selectbox)
    assert any(
        item.label == "Scarica il piano dettagliato (CSV)"
        for item in app.download_button
    )
    assert ">nan<" not in planner_markup.lower()
    for english_day in ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"):
        assert f">{english_day} " not in planner_markup
