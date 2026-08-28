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

import pandas as pd

CASES = (
    ("today-light-desktop", "today", "light", 1440, 1000),
    ("system-dark-desktop", "system", "dark", 1440, 1000),
    ("today-light-mobile", "today", "light", 390, 844),
    ("system-dark-mobile", "system", "dark", 390, 844),
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
                    page.goto(
                        f"{base_url}/?tab={tab}&theme={theme}",
                        wait_until="domcontentloaded",
                        timeout=45_000,
                    )
                    page.wait_for_selector(
                        '[data-testid="stAppViewContainer"]', timeout=45_000
                    )
                    page.wait_for_timeout(2_000)
                    body = page.locator("body").inner_text()
                    if any(marker in body for marker in ("�", "Ã", "Â")):
                        raise AssertionError(f"{name}: testo con encoding corrotto")
                    page_background = page.evaluate(
                        "getComputedStyle(document.documentElement)"
                        ".getPropertyValue('--page-bg').trim()"
                    )
                    expected_background = "#05070b" if theme == "dark" else "#f6f8fb"
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
                            raise AssertionError(f"{name}: card espandibili mancanti")
                        first = cards.first
                        first.locator("summary").click()
                        if not first.get_attribute("open"):
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
                    screenshot = output_path / f"{name}.png"
                    page.screenshot(path=str(screenshot), full_page=True)
                    digests[name] = hashlib.sha256(screenshot.read_bytes()).hexdigest()
                    page.close()
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
    parser = argparse.ArgumentParser(description="Contratti visuali Meteo V4.4")
    parser.add_argument("--output", default="visual-artifacts")
    args = parser.parse_args()
    results = run_visual_checks(args.output)
    print(f"Controllo visuale completato: {len(results)} viewport")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
