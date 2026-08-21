from __future__ import annotations

from pathlib import Path

from streamlit.testing.v1 import AppTest

from db import reset_engine_cache


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
        "Panoramica",
        "7 giorni",
        "Stazione",
        "Astronomia",
        "Radar",
    ]

    app.sidebar.radio[0].set_value("Meteo città").run()

    assert not app.exception
    assert app.sidebar.text_input[0].label == "Nome o CAP"
    assert any("Che tempo fa altrove?" in item.value for item in app.markdown)
