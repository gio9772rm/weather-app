"""Browser-level visual contracts for the responsive Streamlit interface."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from urllib.request import urlopen

import numpy as np
import pandas as pd

CASES = (
    ("today-light-desktop", "today", "light", 1440, 1000),
    ("system-dark-desktop", "system", "dark", 1440, 1000),
    ("overview-light-desktop", "overview", "light", 1440, 1000),
    ("astronomy-light-desktop", "astronomy", "light", 1440, 1000),
    ("today-light-mobile", "today", "light", 390, 844),
    ("system-dark-mobile", "system", "dark", 390, 844),
    ("overview-dark-mobile", "overview", "dark", 390, 844),
    ("astronomy-dark-mobile", "astronomy", "dark", 390, 844),
)


def _available_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def _seed_database(path: Path) -> None:
    os.environ.pop("DATABASE_URL", None)
    os.environ["SQLITE_PATH"] = str(path)
    from sqlalchemy import text

    from db import ensure_schema, get_engine, reset_engine_cache

    reset_engine_cache()
    ensure_schema()
    now = pd.Timestamp.now(tz="UTC").floor("5min")
    rows = []
    for offset in range(24 * 12 + 1):
        moment = now - pd.Timedelta(minutes=offset * 5)
        rows.append(
            {
                "time": moment.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "temp": 22.0 + (offset % 12) / 10,
                "humidity": 58 + offset % 4,
                "pressure": 1013 + (offset % 3) / 10,
                "wind": 5 + offset % 3,
                "gust": 9 + offset % 4,
                "direction": 170 + offset % 20,
                "solar": max(0, 350 - offset),
                "uv": max(0, 3 - offset / 100),
            }
        )
    statement = text(
        "INSERT INTO station_raw (time,temp_c,humidity,pressure_hpa,wind_kmh,"
        "windgust_kmh,winddir,rain_mm,rain_rate_mm_h,rain_total_mm,solar_w_m2,"
        "uv_index,source,data_quality) VALUES (:time,:temp,:humidity,:pressure,"
        ":wind,:gust,:direction,0,0,0,:solar,:uv,'visual_fixture','ok')"
    )
    with get_engine().begin() as connection:
        connection.execute(statement, rows)
        issued_at = now.floor("h").strftime("%Y-%m-%dT%H:%M:%SZ")
        forecast_rows = []
        for offset in range(73):
            moment = now.floor("h") + pd.Timedelta(hours=offset)
            local_hour = moment.tz_convert("Europe/Rome").hour
            forecast_rows.append(
                {
                    "valid_time": moment.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "issued_at": issued_at,
                    "temp": 18 + np.sin(offset / 6) * 5,
                    "humidity": 62 + offset % 5,
                    "pressure": 1012 + np.cos(offset / 8),
                    "wind": 7 + offset % 3,
                    "gust": 12 + offset % 4,
                    "direction": 180 + offset % 30,
                    "rain": max(0, np.sin(offset / 9)) * 0.4,
                    "probability": 20 + offset % 40,
                    "clouds": 25 + offset % 35,
                    "low": 10 + offset % 20,
                    "mid": 15 + offset % 25,
                    "high": 20 + offset % 30,
                    "is_day": int(7 <= local_hour < 20),
                }
            )
        connection.execute(
            text(
                "INSERT INTO forecast_blend (valid_time,issued_at,temp_c,feels_like_c,"
                "humidity,dewpoint_c,pressure_hpa,wind_kmh,wind_gust_kmh,wind_dir,"
                "rain_mm,snow_mm,precip_probability,clouds,cloud_low,cloud_mid,cloud_high,"
                "visibility_m,weather_code,description,is_day,temp_uncertainty_c,confidence,"
                "provider_count,provider_weights,method) VALUES ("
                ":valid_time,:issued_at,:temp,:temp,:humidity,12,:pressure,:wind,:gust,"
                ":direction,:rain,0,:probability,:clouds,:low,:mid,:high,18000,'1',"
                "'Sereno',:is_day,1.2,82,2,'{}','visual_fixture')"
            ),
            forecast_rows,
        )
        archived_rows = []
        for offset in (-3, -2, -1):
            moment = now.floor("h") + pd.Timedelta(hours=offset)
            archived_rows.append(
                {
                    "valid_time": moment.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "issued_at": (moment - pd.Timedelta(hours=1)).strftime(
                        "%Y-%m-%dT%H:%M:%SZ"
                    ),
                    "temp": 18 + np.sin(offset / 6) * 5,
                    "humidity": 62 + offset % 5,
                    "pressure": 1012 + np.cos(offset / 8),
                    "wind": 7 + offset % 3,
                    "gust": 12 + offset % 4,
                    "direction": 180 + offset % 30,
                    "rain": max(0, np.sin(offset / 9)) * 0.4,
                    "probability": 20 + offset % 40,
                    "clouds": 25 + offset % 35,
                    "low": 10 + offset % 20,
                    "mid": 15 + offset % 25,
                    "high": 20 + offset % 30,
                    "is_day": int(7 <= moment.tz_convert("Europe/Rome").hour < 20),
                }
            )
        connection.execute(
            text(
                "INSERT INTO forecast_blend_history (valid_time,issued_at,temp_c,"
                "feels_like_c,humidity,dewpoint_c,pressure_hpa,wind_kmh,wind_gust_kmh,"
                "wind_dir,rain_mm,snow_mm,precip_probability,clouds,cloud_low,cloud_mid,"
                "cloud_high,visibility_m,weather_code,description,is_day,"
                "temp_uncertainty_c,confidence,provider_count,provider_weights,method) "
                "VALUES (:valid_time,:issued_at,:temp,:temp,:humidity,12,:pressure,"
                ":wind,:gust,:direction,:rain,0,:probability,:clouds,:low,:mid,:high,"
                "18000,'1','Sereno',:is_day,1.2,82,2,'{}','visual_archive')"
            ),
            archived_rows,
        )
    reset_engine_cache()


def _wait_for_app(url: str, process: subprocess.Popen[str]) -> None:
    deadline = time.monotonic() + 45
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError("Streamlit terminated before visual checks")
        try:
            with urlopen(f"{url}/_stcore/health", timeout=2) as response:
                if response.status == 200:
                    return
        except OSError:
            time.sleep(0.4)
    raise TimeoutError("Streamlit health endpoint did not become ready")


def run_visual_checks(output: str | Path) -> dict[str, str]:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise RuntimeError("Installare Playwright per il controllo visuale") from exc
    output_path = Path(output)
    output_path.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="meteo-visual-") as temporary:
        database_path = Path(temporary) / "visual.sqlite"
        _seed_database(database_path)
        port = _available_port()
        base_url = f"http://127.0.0.1:{port}"
        environment = os.environ.copy()
        environment.pop("DATABASE_URL", None)
        environment.update(
            {
                "SQLITE_PATH": str(database_path),
                "LOCAL_TZ": "Europe/Rome",
                "LAT": "41.90",
                "LON": "12.50",
                "STATION_ID": "visual-primary",
                "LOCATION_NAME": "Stazione visuale",
                "STREAMLIT_BROWSER_GATHER_USAGE_STATS": "false",
            }
        )
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "streamlit",
                "run",
                str(Path(__file__).with_name("app_streamlit.py")),
                "--server.headless=true",
                "--server.address=127.0.0.1",
                f"--server.port={port}",
            ],
            env=environment,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
            text=True,
        )
        try:
            _wait_for_app(base_url, process)
            digests: dict[str, str] = {}
            with sync_playwright() as playwright:
                browser = playwright.chromium.launch(headless=True)
                for name, tab, theme, width, height in CASES:
                    page = browser.new_page(viewport={"width": width, "height": height})
                    screenshot = output_path / f"{name}.png"
                    try:
                        page.goto(
                            f"{base_url}/?tab={tab}&theme={theme}",
                            wait_until="domcontentloaded",
                            timeout=45_000,
                        )
                        page.wait_for_selector(
                            '[data-testid="stAppViewContainer"]', timeout=45_000
                        )
                        # Streamlit mounts the app container before the selected tab has
                        # finished rendering.  Wait for a tab-specific sentinel so the
                        # visual contract never inspects a partially loaded page.
                        if tab == "today":
                            page.wait_for_selector(
                                "details.expandable-card", timeout=45_000
                            )
                        elif tab == "system":
                            page.get_by_text(
                                "Diagnostica Ecowitt", exact=True
                            ).wait_for(state="attached", timeout=45_000)
                        elif tab == "overview":
                            page.wait_for_selector(".js-plotly-plot", timeout=45_000)
                        elif tab == "astronomy":
                            page.get_by_text(
                                "Pianificatore Astronomia Pro", exact=True
                            ).wait_for(state="attached", timeout=45_000)
                        page.wait_for_timeout(500)
                        body = page.locator("body").inner_text()
                        if any(marker in body for marker in ("�", "Ã", "Â")):
                            raise AssertionError(f"{name}: testo con encoding corrotto")
                        page_background = page.evaluate(
                            "getComputedStyle(document.documentElement)"
                            ".getPropertyValue('--page-bg').trim()"
                        )
                        expected_background = (
                            "#05070b" if theme == "dark" else "#f6f8fb"
                        )
                        if page_background.lower() != expected_background:
                            raise AssertionError(f"{name}: tema {theme} non applicato")
                        overflow = page.evaluate(
                            "document.documentElement.scrollWidth - window.innerWidth"
                        )
                        if overflow > 3:
                            raise AssertionError(
                                f"{name}: overflow orizzontale pagina di {overflow}px"
                            )
                        if tab == "today":
                            cards = page.locator("details.expandable-card")
                            if cards.count() < 6:
                                raise AssertionError(
                                    f"{name}: card espandibili mancanti"
                                )
                            first = cards.first
                            first.locator("summary").click()
                            if not first.evaluate("element => element.open"):
                                raise AssertionError(
                                    f"{name}: apertura card non funzionante"
                                )
                            clipped = page.locator(
                                ".current-card summary,.activity-card summary,.air-card summary"
                            ).evaluate_all(
                                "els => els.filter(el => el.scrollWidth > el.clientWidth + 3).length"
                            )
                            if clipped:
                                raise AssertionError(
                                    f"{name}: {clipped} card con testo tagliato"
                                )
                        if tab == "system" and "Diagnostica Ecowitt" not in body:
                            raise AssertionError(
                                f"{name}: diagnostica Ecowitt non visibile"
                            )
                        if tab == "overview":
                            page.wait_for_selector(".js-plotly-plot", timeout=30_000)
                            alignment = page.evaluate(
                                """() => {
                                  const graphs = [...document.querySelectorAll('.js-plotly-plot')];
                                  const graph = graphs.find(item =>
                                    (item.data || []).some(trace =>
                                      String(trace.name || '').includes('Previsione · da −2 h')));
                                  if (!graph) return null;
                                  const trace = graph.data.find(item =>
                                    String(item.name || '').includes('Previsione · da −2 h'));
                                  const shape = (graph.layout.shapes || []).find(item =>
                                    item.type === 'line' && item.line && item.line.dash === 'dot');
                                  if (!trace || !shape) return null;
                                  const points = [...trace.x].map((value, index) => ({
                                    raw: String(value),
                                    time: Date.parse(value),
                                    value: trace.y[index],
                                  })).filter(point =>
                                    Number.isFinite(point.time) &&
                                    point.value !== null &&
                                    point.value !== undefined &&
                                    point.value !== '' &&
                                    Number.isFinite(Number(point.value)));
                                  if (!points.length) return null;
                                  const firstPoint = points.reduce((earliest, point) =>
                                    point.time < earliest.time ? point : earliest);
                                  return {
                                    first: firstPoint.time,
                                    last: Math.max(...points.map(point => point.time)),
                                    marker: Date.parse(shape.x0),
                                    markerRaw: String(shape.x0),
                                    firstRaw: firstPoint.raw,
                                    now: Date.now(),
                                  };
                                }"""
                            )
                            if alignment is None:
                                raise AssertionError(
                                    f"{name}: grafico previsione o linea adesso mancanti"
                                )
                            marker_delta = abs(alignment["marker"] - alignment["now"])
                            lookback = alignment["now"] - alignment["first"]
                            if marker_delta > 5 * 60_000:
                                raise AssertionError(
                                    f"{name}: linea adesso fuori posto di {marker_delta / 60_000:.1f} min"
                                )
                            if not 105 * 60_000 <= lookback <= 135 * 60_000:
                                raise AssertionError(
                                    f"{name}: coda previsionale non pari a 2 h ({lookback / 60_000:.1f} min)"
                                )
                            if not any(
                                offset in alignment["markerRaw"]
                                and offset in alignment["firstRaw"]
                                for offset in ("+01:00", "+02:00")
                            ):
                                raise AssertionError(
                                    f"{name}: offset Europe/Rome non conservato"
                                )
                        if tab == "astronomy":
                            for label in (
                                "Pianificatore Astronomia Pro",
                                "Oggetto personalizzato · RA/Dec",
                                "Attrezzatura e campo inquadrato",
                                "Orizzonte locale · ostacoli",
                            ):
                                if label not in body:
                                    raise AssertionError(
                                        f"{name}: controllo astronomico mancante: {label}"
                                    )
                    finally:
                        page.screenshot(path=str(screenshot), full_page=True)
                        page.close()
                    digests[name] = hashlib.sha256(screenshot.read_bytes()).hexdigest()
                browser.close()
            manifest = output_path / "manifest.json"
            manifest.write_text(
                json.dumps(digests, indent=2, sort_keys=True), encoding="utf-8"
            )
            return digests
        finally:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)


def main() -> int:
    parser = argparse.ArgumentParser(description="Contratti visuali Meteo V4.5")
    parser.add_argument("--output", default="visual-artifacts")
    args = parser.parse_args()
    results = run_visual_checks(args.output)
    print(f"Controllo visuale completato: {len(results)} viewport")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
