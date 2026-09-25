"""Browser-level visual contracts for the responsive Streamlit interface."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from urllib.request import urlopen

import numpy as np
import pandas as pd

CASES = tuple(
    (f"{tab}-{theme}-{viewport}", tab, theme, width, height)
    for tab in ("today", "system", "overview", "astronomy")
    for theme in ("light", "dark")
    for viewport, width, height in (
        ("desktop", 1440, 1000),
        ("mobile", 390, 844),
    )
)


ASTRONOMY_CONTRAST_AUDIT = """() => {
  const parse = value => {
    const parts = String(value || '').match(/[\\d.]+/g) || [];
    return {
      r: Number(parts[0] || 0), g: Number(parts[1] || 0),
      b: Number(parts[2] || 0), a: Number(parts[3] || 1),
    };
  };
  const blend = (foreground, background, alpha) => ({
    r: foreground.r * alpha + background.r * (1 - alpha),
    g: foreground.g * alpha + background.g * (1 - alpha),
    b: foreground.b * alpha + background.b * (1 - alpha),
  });
  const luminance = color => {
    const channels = [color.r, color.g, color.b].map(value => {
      const normalised = value / 255;
      return normalised <= 0.04045
        ? normalised / 12.92
        : Math.pow((normalised + 0.055) / 1.055, 2.4);
    });
    return 0.2126 * channels[0] + 0.7152 * channels[1] + 0.0722 * channels[2];
  };
  const ratio = (foreground, background) => {
    const first = luminance(foreground);
    const second = luminance(background);
    return (Math.max(first, second) + 0.05) / (Math.min(first, second) + 0.05);
  };
  const backgroundFor = element => {
    let node = element;
    while (node) {
      const colour = parse(getComputedStyle(node).backgroundColor);
      if (colour.a >= 0.98) return colour;
      node = node.parentElement;
    }
    return {r: 255, g: 255, b: 255, a: 1};
  };
  const opacityFor = element => {
    let opacity = 1;
    let node = element;
    while (node && node.matches && !node.matches('.stApp')) {
      opacity *= Number(getComputedStyle(node).opacity || 1);
      node = node.parentElement;
    }
    return opacity;
  };
  const visible = element => {
    const style = getComputedStyle(element);
    return style.display !== 'none' && style.visibility !== 'hidden'
      && element.getClientRects().length > 0;
  };
  const selectors = [
    '[data-testid="stNumberInputField"]',
    '[data-testid="stTextInput"] input',
    '[data-testid="stBaseButton-secondaryFormSubmit"]',
    '[data-testid="stCaptionContainer"] p',
  ];
  const samples = [];
  for (const selector of selectors) {
    for (const element of [...document.querySelectorAll(selector)].filter(visible)) {
      const style = getComputedStyle(element);
      const background = backgroundFor(element);
      const opacity = opacityFor(element);
      const foreground = blend(parse(style.color), background, opacity);
      samples.push({
        selector,
        contrast: ratio(foreground, background),
        opacity,
        backgroundLuminance: luminance(background),
      });
    }
  }
  const numberContainers = [...document.querySelectorAll(
    '[data-testid="stNumberInputContainer"]'
  )].filter(visible).map(element => ({
    backgroundLuminance: luminance(backgroundFor(element)),
  }));
  return {samples, numberContainers};
}"""


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
        timestamp = now.strftime("%Y-%m-%dT%H:%M:%SZ")
        connection.execute(
            text(
                "INSERT INTO station_profiles (station_id,display_name,timezone,"
                "source,role,enabled,privacy_level,created_at,updated_at) VALUES ("
                "'visual-primary','Stazione meteo Roma','Europe/Rome','visual_fixture',"
                "'primary',1,'private_location',:timestamp,:timestamp)"
            ),
            {"timestamp": timestamp},
        )
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


def _seed_second_station() -> None:
    from sqlalchemy import text

    from db import get_engine

    with get_engine().begin() as con:
        con.execute(
            text(
                "INSERT INTO station_profiles(station_id,display_name,latitude,longitude,elevation_m,timezone,source,role,enabled,privacy_level,created_at,updated_at) VALUES('visual-secondary','Comacchio · test',44.8,12.1,-1,'Europe/Rome','visual_fixture','secondary',1,'private_location','2026-01-01','2026-01-01')"
            )
        )
        con.execute(
            text(
                "INSERT INTO station_observations(station_id,time,temp_c,humidity,pressure_hpa,wind_kmh,windgust_kmh,winddir,rain_mm,rain_rate_mm_h,rain_total_mm,source,data_quality) SELECT 'visual-secondary',time,temp_c+3,humidity,pressure_hpa,wind_kmh,windgust_kmh,winddir,rain_mm,rain_rate_mm_h,rain_total_mm,source,data_quality FROM station_raw"
            )
        )
        forecast = pd.read_sql(
            text("SELECT * FROM forecast_blend ORDER BY valid_time"), con
        )
        forecast["temp_c"] += 3
        forecast["clouds"] = 69
        forecast["cloud_high"] = 69
        con.execute(
            text(
                "INSERT INTO location_forecasts VALUES('visual-secondary',:issued,:payload)"
            ),
            {
                "issued": str(forecast.issued_at.max()),
                "payload": forecast.to_json(orient="records"),
            },
        )


def _v5_checks(browser, base_url: str, output: Path) -> dict:
    """Real root API, responsive UI, offline snapshot and a single 600s timer."""
    from playwright.sync_api import expect

    digests = {}
    for theme in ("light", "dark"):
        for width, height in ((1440, 1000), (390, 844)):
            context = browser.new_context(viewport={"width": width, "height": height})
            page = context.new_page()
            errors = []
            page.on("pageerror", lambda error, errors=errors: errors.append(str(error)))
            for view in (
                "today",
                "forecast",
                "stations",
                "astronomy",
                "maps",
                "cities",
                "planner",
                "journal",
                "inbox",
                "activities",
            ):
                name = f"v5-{view}-{theme}-{width}"
                try:
                    page.goto(f"{base_url}/?page={view}&theme={theme}")
                    page.wait_for_function(
                        "document.querySelector('#snapshot-time').textContent.includes('Fotografia')"
                    )
                    assert page.locator("#station option").count() == 2
                    assert (
                        page.evaluate(
                            "document.documentElement.scrollWidth-window.innerWidth"
                        )
                        <= 3
                    ), name + ": overflow"
                    assert page.locator("#view .card").count() > 0
                    assert not errors, errors
                    # Legend and line use the same actual computed color.
                    if page.locator(".chart-legend").count():
                        colors = page.evaluate("""() => {
                          const legend = document.querySelector('.chart-legend');
                          const svg = legend.nextElementSibling;
                          return [...legend.querySelectorAll('.swatch')].map((swatch, i) => {
                            const paths = [...svg.querySelectorAll('path[stroke-width="2.8"]')];
                            return paths[i] && getComputedStyle(swatch).borderTopColor === getComputedStyle(paths[i]).stroke;
                          });
                        }""")
                        assert all(colors), name + ": legend mismatch"
                    if view == "astronomy":
                        page.select_option("#station", "visual-secondary")
                        expect(page.locator("#station")).to_have_value(
                            "visual-secondary"
                        )
                        expect(page.locator("#view")).to_contain_text("0,0")
                finally:
                    screenshot = output / f"{name}.png"
                    page.screenshot(path=str(screenshot), full_page=True)
                digests[name] = hashlib.sha256(screenshot.read_bytes()).hexdigest()
            assert page.evaluate(
                "['constructor','toString','__proto__'].every(p => window.MeteoExtra.view(p) === undefined)"
            ), "Unknown tools must never dispatch inherited object methods"
            # Exercise real planner calculation and private journal persistence.
            page.goto(f"{base_url}/?page=planner&theme={theme}")
            page.wait_for_selector('[name="target"]')
            page.get_by_role("button", name="Calcola il piano", exact=True).click()
            expect(page.locator("#plan-result .fov").first).to_be_visible(timeout=15000)
            assert not errors, errors
            page.goto(f"{base_url}/?page=journal&theme={theme}")
            page.locator("#journal-target").fill("M31 · prova sessione")
            page.locator("#journal-notes").fill("Nota da conservare al refresh")
            page.get_by_role("button", name="Aggiorna ora", exact=True).click()
            expect(page.locator("#refresh")).to_be_enabled()
            expect(page.locator("#journal-notes")).to_have_value(
                "Nota da conservare al refresh"
            )
            page.get_by_role("button", name="Registra sessione", exact=True).click()
            page.reload()
            expect(page.locator(".journal-text")).to_contain_text(
                "Nota da conservare al refresh"
            )
            # Missing weather splits activity windows instead of becoming dry.
            result = page.evaluate("""() => {
              const now=Date.parse('2026-09-24T08:00Z');
              const rows=[0,1,2,3].map(h=>({valid_time:new Date(now+h*3600000).toISOString(),temp_c:20,wind_kmh:5,wind_gust_kmh:8,precip_probability:10,rain_mm:0,is_day:1}));
              rows[1].rain_mm=null;
              return window.MeteoExtra.activityWindows(rows,{min:5,max:30,wind:20,gust:35,pop:30,rain:0.1,hours:2,daylight:true},now);
            }""")
            assert result["incomplete"] == 1 and len(result["windows"]) == 1
            assert not errors, errors
            context.close()

    context = browser.new_context(viewport={"width": 390, "height": 844})
    page = context.new_page()
    errors = []
    page.on("pageerror", lambda error: errors.append(str(error)))
    # Record fetch invocation as well as network calls: the HTTP cache must not
    # conceal a second automatic timer from the cadence check.
    page.add_init_script("""(() => {
      window.weatherFetches=0;
      const original=window.fetch;
      window.fetch=(...args)=>{if(String(args[0]).includes('/api/v5/snapshot/'))window.weatherFetches++;return original(...args);};
    })();""")
    page.clock.install()
    page.goto(base_url)
    page.wait_for_function(
        "document.querySelector('#snapshot-time').textContent.includes('Fotografia')"
    )
    page.evaluate("navigator.serviceWorker.ready.then(()=>true)")
    initial = page.evaluate("window.weatherFetches")
    assert initial == 2
    page.clock.fast_forward(599_000)
    page.get_by_role("button", name="Cielo", exact=True).click()
    page.select_option("#station", "visual-secondary")
    assert page.evaluate("window.weatherFetches") == initial
    page.clock.fast_forward(2_000)
    page.wait_for_function("window.weatherFetches===4")
    expect(page.locator("#refresh")).to_be_enabled()
    page.get_by_role("button", name="Aggiorna ora", exact=True).click()
    page.wait_for_function("window.weatherFetches===6")
    expect(page.locator("#refresh")).to_be_enabled()
    page.clock.fast_forward(599_000)
    assert page.evaluate("window.weatherFetches") == 6
    page.get_by_role("button", name="Personalizza", exact=True).click()
    page.locator('[data-card="air"]').uncheck()
    page.get_by_role("button", name="Salva preferenze", exact=True).click()
    assert page.evaluate(
        "JSON.parse(localStorage.getItem('meteo.v5.preferences')).hidden.includes('air')"
    )
    page.clock.resume()
    context.set_offline(True)
    page.reload(wait_until="domcontentloaded")
    try:
        expect(page.locator("#connection")).to_contain_text("Offline", timeout=15000)
        expect(page.locator("#notice")).to_contain_text(
            "Non sono un aggiornamento live"
        )
        expect(page.locator("#view .card").first).to_be_visible()
        assert not errors, errors
    except Exception:
        print(
            "Offline diagnostic:",
            page.evaluate("""() => ({
          online: navigator.onLine,
          controlled: !!navigator.serviceWorker.controller,
          connection: document.querySelector('#connection')?.textContent,
          notice: document.querySelector('#notice')?.textContent,
          saved: Object.keys(localStorage).filter(key => key.startsWith('meteo.v5.snapshot.')),
          cards: document.querySelectorAll('#view .card').length
        })"""),
            "errors:",
            errors,
            flush=True,
        )
        raise
    finally:
        page.screenshot(path=str(output / "v5-offline-mobile.png"), full_page=True)
    context.close()
    return digests


def _android_update_checks(browser, base_url: str, output: Path) -> dict:
    """The standalone download screen stays usable without a weather snapshot."""
    from playwright.sync_api import expect

    config = json.loads(Path("android/release.json").read_text())
    valid = {"schema": 1, **config, "sizeBytes": 3500000, "sha256": "a" * 64,
             "apkUrl": f"https://weather-app-v3-w2jd.onrender.com/app/static/android/MeteoPro-{config['versionName']}.apk"}
    results = {}
    for theme in ("light", "dark"):
        for width in (390, 1440):
            page = browser.new_page(viewport={"width": width, "height": 900}, color_scheme=theme)
            page.route("**/app/static/android/manifest.json", lambda route: route.fulfill(json=valid))
            page.goto(f"{base_url}/app/static/android/index.html")
            expect(page.locator("#download")).to_be_visible()
            expect(page.locator("#download")).to_have_attribute("href", valid["apkUrl"])
            assert page.evaluate("document.documentElement.scrollWidth <= innerWidth + 3")
            name = f"android-updates-{theme}-{width}"
            image = output / f"{name}.png"
            page.screenshot(path=str(image), full_page=True)
            results[name] = hashlib.sha256(image.read_bytes()).hexdigest()
            page.unroute("**/app/static/android/manifest.json")
            page.route("**/app/static/android/manifest.json", lambda route: route.fulfill(json={**valid, "apkUrl": "https://untrusted.example/app.apk"}))
            page.reload()
            expect(page.locator("#status")).to_contain_text("Non è possibile verificare")
            expect(page.locator("#download")).to_be_hidden()
            page.close()
    return results


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
        _seed_second_station()
        port = _available_port()
        base_url = f"http://127.0.0.1:{port}"
        visual_admin_token = "visual-regression-admin"
        environment = os.environ.copy()
        environment.pop("DATABASE_URL", None)
        environment.update(
            {
                "SQLITE_PATH": str(database_path),
                "LOCAL_TZ": "Europe/Rome",
                "LAT": "41.90",
                "LON": "12.50",
                "STATION_ID": "visual-primary",
                "SECONDARY_STATION_ENABLED": "true",
                "SECONDARY_STATION_ID": "visual-secondary",
                "LOCATION_NAME": "Stazione visuale",
                "ADMIN_ACCESS_TOKEN": visual_admin_token,
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
                digests.update(_v5_checks(browser, base_url, output_path))
                digests.update(_android_update_checks(browser, base_url, output_path))
                for name, tab, theme, width, height in CASES:
                    page = browser.new_page(viewport={"width": width, "height": height})
                    screenshot = output_path / f"{name}.png"
                    try:
                        admin_query = (
                            f"&admin={visual_admin_token}" if tab == "system" else ""
                        )
                        page.goto(
                            f"{base_url}/pro/?tab={tab}&theme={theme}{admin_query}",
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
                            page.get_by_text("Piano della notte", exact=True).wait_for(
                                state="attached", timeout=45_000
                            )
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
                            current_cards = page.locator("details.current-card")
                            if current_cards.count() != 8 or cards.count() < 8:
                                raise AssertionError(
                                    f"{name}: card espandibili mancanti"
                                )
                            station_card = page.locator(".station-active-card")
                            if station_card.count() != 1:
                                raise AssertionError(
                                    f"{name}: riquadro stazione attiva mancante"
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
                                  const traceIndex = graph.data.indexOf(trace);
                                  const calculated = (graph.calcdata || [])[traceIndex] || [];
                                  const points = [...trace.x].map((value, index) => ({
                                    raw: String(value),
                                    time: Date.parse(value),
                                    // Plotly 6 may keep the source y values in a binary
                                    // object. calcdata is the rendered numeric series.
                                    value: calculated[index] && calculated[index].y,
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
                                "Piano della notte",
                                "Oggetto personalizzato · RA/Dec",
                                "Attrezzatura e campo inquadrato",
                                "Orizzonte locale · ostacoli",
                            ):
                                if label not in body:
                                    raise AssertionError(
                                        f"{name}: controllo astronomico mancante: {label}"
                                    )
                            for label in (
                                "Oggetto personalizzato · RA/Dec",
                                "Attrezzatura e campo inquadrato",
                            ):
                                summary = (
                                    page.locator('[data-testid="stExpander"] summary')
                                    .filter(has_text=label)
                                    .first
                                )
                                if summary.count() != 1:
                                    raise AssertionError(
                                        f"{name}: expander non individuato: {label}"
                                    )
                                details = summary.locator("xpath=..")
                                if details.get_attribute("open") is None:
                                    summary.click()
                            page.wait_for_selector(
                                '[data-testid="stNumberInputField"]', timeout=15_000
                            )
                            save_profile = page.get_by_role(
                                "button", name="Salva profilo nella sessione"
                            )
                            if save_profile.count() != 1:
                                raise AssertionError(
                                    f"{name}: salvataggio profilo ottico non disponibile"
                                )
                            save_profile.click()
                            # The old plots remain mounted during the form rerun.
                            # Wait for its result, then for Streamlit's stale fade
                            # to finish before auditing actual settled contrast.
                            page.get_by_text(re.compile(r"^Profilo .+ attivo\.$")).wait_for(
                                state="visible", timeout=30_000
                            )
                            page.wait_for_function(
                                """() => [...document.querySelectorAll('[data-testid="stCaptionContainer"] p')].every(el => {
                                  for (let node=el; node && !node.matches('.stApp'); node=node.parentElement) {
                                    if (Number(getComputedStyle(node).opacity) < 0.99) return false;
                                  }
                                  return true;
                                })""",
                                timeout=15_000,
                            )
                            page.wait_for_function(
                                "document.querySelectorAll('.js-plotly-plot').length >= 3",
                                timeout=30_000,
                            )
                            chart_overflow = page.evaluate(
                                "Math.max(...[...document.querySelectorAll('.js-plotly-plot')]"
                                ".map(el => el.scrollWidth - el.clientWidth), 0)"
                            )
                            if chart_overflow > 3:
                                raise AssertionError(
                                    f"{name}: grafico astronomico eccede di "
                                    f"{chart_overflow}px"
                                )
                            audit = page.evaluate(ASTRONOMY_CONTRAST_AUDIT)
                            if not audit["samples"] or not audit["numberContainers"]:
                                raise AssertionError(
                                    f"{name}: controlli astronomici non verificabili"
                                )
                            low_contrast = [
                                sample
                                for sample in audit["samples"]
                                if sample["contrast"] < 4.5
                            ]
                            if low_contrast:
                                raise AssertionError(
                                    f"{name}: contrasto controlli sotto WCAG AA: "
                                    f"{low_contrast[:3]}"
                                )
                            faded = [
                                sample
                                for sample in audit["samples"]
                                if "stCaptionContainer" in sample["selector"]
                                and sample["opacity"] < 0.99
                            ]
                            if faded:
                                raise AssertionError(
                                    f"{name}: didascalie attenuate dal tema: {faded[:3]}"
                                )
                            if theme == "light" and any(
                                item["backgroundLuminance"] < 0.75
                                for item in audit["numberContainers"]
                            ):
                                raise AssertionError(
                                    f"{name}: input numerici ancora scuri nel tema chiaro"
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
    parser = argparse.ArgumentParser(
        description="Contratti visuali Meteo Pro V5 e strumenti Pro"
    )
    parser.add_argument("--output", default="visual-artifacts")
    args = parser.parse_args()
    results = run_visual_checks(args.output)
    print(f"Controllo visuale completato: {len(results)} viewport")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
