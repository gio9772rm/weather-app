/* Meteo Pro V5: dependency-free UI, one automatic weather cycle per 600 s. */
"use strict";
const REFRESH_MS = 600000;
const $ = (selector) => document.querySelector(selector);
const esc = (value) =>
  String(value ?? "").replace(
    /[&<>"']/g,
    (c) =>
      ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[
        c
      ],
  );
const finite = (v) =>
  v !== null && v !== undefined && v !== "" && Number.isFinite(Number(v));
const num = (v, digits = 1) =>
  finite(v)
    ? Number(v).toLocaleString("it-IT", {
        minimumFractionDigits: digits,
        maximumFractionDigits: digits,
      })
    : "—";
const storage = {
  get(key, fallback = null) {
    try {
      return JSON.parse(localStorage.getItem(key)) ?? fallback;
    } catch {
      return fallback;
    }
  },
  set(key, value) {
    try {
      localStorage.setItem(key, JSON.stringify(value));
    } catch {}
  },
};
const defaultCards = [
  "hours",
  "chart",
  "astronomy",
  "change",
  "history",
  "air",
];
const cardNames = {
  hours: "Le prossime ore",
  chart: "Passato e futuro",
  astronomy: "Il cielo di stanotte",
  change: "Cosa è cambiato",
  history: "Il tuo archivio",
  air: "Aria e pollini",
};
const prefs = {
  ...{
    cards: defaultCards,
    hidden: [],
    profile: "deep_sky",
    offline: true,
    expert: false,
    theme: "light",
  },
  ...storage.get("meteo.v5.preferences", {}),
};
prefs.cards = [
  ...new Set([
    ...(Array.isArray(prefs.cards) ? prefs.cards : []),
    ...defaultCards,
  ]),
].filter((k) => defaultCards.includes(k));
prefs.hidden = Array.isArray(prefs.hidden)
  ? prefs.hidden.filter((k) => defaultCards.includes(k))
  : [];
let stations = [],
  snapshots = new Map(),
  cachedOffline = new Map(),
  activeId = "",
  data = null,
  offline = false,
  timer = null,
  refreshing = false,
  installPrompt = null;
let page = new URL(location.href).searchParams.get("page") || "today";
if (
  ![
    "today",
    "forecast",
    "stations",
    "astronomy",
    "more",
    "air",
    "maps",
    "notifications",
    "import",
  ].includes(page)
)
  page = "today";
let forecastStep = 3,
  historyDays = 90,
  historyMetric = "temp_mean_c",
  selectedNight = "",
  importDigest = null;
const tz = () => data?.station?.timezone || "Europe/Rome";
const clock = (value) =>
  value
    ? new Intl.DateTimeFormat("it-IT", {
        hour: "2-digit",
        minute: "2-digit",
        timeZone: tz(),
      }).format(new Date(value))
    : "—";
const dayLabel = (value) =>
  value
    ? new Intl.DateTimeFormat("it-IT", {
        weekday: "short",
        day: "numeric",
        month: "short",
        timeZone: tz(),
      }).format(new Date(value.length === 10 ? value + "T12:00:00Z" : value))
    : "—";
const dateTime = (value) =>
  value ? dayLabel(value) + " · " + clock(value) : "non disponibile";
const currentAge = () =>
  data?.observed_at
    ? Math.max(0, (Date.now() - Date.parse(data.observed_at)) / 60000)
    : null;
function ageText(minutes) {
  return !finite(minutes)
    ? "nessuna misura"
    : minutes < 60
      ? num(minutes, 0) + " min fa"
      : minutes < 1440
        ? num(minutes / 60, 1) + " ore fa"
        : num(minutes / 1440, 1) + " giorni fa";
}
function weatherIcon(description = "", night = false, cls = "mini-weather") {
  const text = String(description).toLowerCase(),
    cloudy = /nuvol|copert|piogg|rovesc|tempor|nebb/.test(text),
    rain = /piogg|rovesc|tempor/.test(text);
  return `<svg class="${cls}" viewBox="0 0 100 78" aria-hidden="true">${night ? '<path d="M57 7a24 24 0 1 0 28 32A28 28 0 0 1 57 7" fill="#c6def6"/>' : '<g stroke="#efb850" stroke-width="3" stroke-linecap="round"><path d="M63 2v5m0 43v5M37 29h5m43 0h5M45 11l4 4m28 28 4 4m0-36-4 4M49 43l-4 4"/><circle cx="63" cy="29" r="17" fill="#ffcf77"/></g>'}${cloudy ? '<path d="M24 59a13 13 0 1 1 2-26 20 20 0 0 1 37 6 11 11 0 1 1 2 22H24" fill="#dbeaf4" stroke="#96b6cc" stroke-width="1.3"/>' : ""}${rain ? '<path d="m31 67-3 6m18-6-3 6m18-6-3 6" stroke="#6baaf0" stroke-width="4" stroke-linecap="round"/>' : ""}</svg>`;
}
const proLink = (tab = "today") =>
  "/pro/?" +
  new URLSearchParams({
    station: activeId,
    tab,
    theme: prefs.theme,
    detail: prefs.expert ? "expert" : "simple",
  });
function proAnchor(tab, label) {
  return `<a href="${esc(proLink(tab))}">${esc(label)} ↗</a>`;
}
function setTheme(theme) {
  prefs.theme = theme === "dark" ? "dark" : "light";
  document.documentElement.dataset.theme = prefs.theme;
  $("#theme").setAttribute(
    "aria-label",
    prefs.theme === "dark" ? "Passa al tema chiaro" : "Passa al tema scuro",
  );
  storage.set("meteo.v5.preferences", prefs);
}
setTheme(new URL(location.href).searchParams.get("theme") || prefs.theme);
function notice(message) {
  $("#notice").hidden = !message;
  $("#notice").textContent = message || "";
}
function updateMeta() {
  if (!data) return;
  const fresh = finite(currentAge()) && currentAge() <= 20;
  $("#connection").innerHTML =
    `<i class="live-dot ${!fresh || offline ? "old" : ""}"></i>${offline ? "Offline · dati salvati" : fresh ? "Stazione live" : "Misure " + ageText(currentAge())}`;
  $("#snapshot-time").textContent =
    "Fotografia " + dateTime(data.generated_at) + " · ciclo automatico 10 min";
  const snapshotAge = (Date.now() - Date.parse(data.generated_at)) / 60000;
  notice(
    offline
      ? "Sei offline. Stai consultando i dati salvati del " +
          dateTime(data.generated_at) +
          ". Non sono un aggiornamento live."
      : snapshotAge > 30
        ? "Fotografia non aggiornata da " +
          ageText(snapshotAge) +
          ". Le fonti potrebbero essere temporaneamente indisponibili."
        : "",
  );
}
async function loadStations() {
  try {
    const r = await fetch("/api/v5/stations");
    if (!r.ok) throw Error();
    stations = (await r.json()).stations;
    storage.set("meteo.v5.stations", stations);
  } catch {
    stations = storage.get("meteo.v5.stations", []);
  }
  if (!stations.length)
    throw Error(
      "Non riesco a caricare le località. Riprova quando la connessione è disponibile.",
    );
  const requested =
    new URL(location.href).searchParams.get("station") || prefs.station;
  if (!stations.some((s) => s.id === activeId))
    activeId = stations.some((s) => s.id === requested)
      ? requested
      : stations[0].id;
  $("#station").innerHTML = stations
    .map((s) => `<option value="${esc(s.id)}">${esc(s.name)}</option>`)
    .join("");
  $("#station").value = activeId;
}
async function loadSnapshot(id) {
  try {
    const r = await fetch("/api/v5/snapshot/" + encodeURIComponent(id));
    if (!r.ok) throw Error();
    const payload = await r.json();
    if (payload?.station?.id !== id) throw Error();
    const fromCache = r.headers.get("X-Meteo-Offline") === "1" || !navigator.onLine;
    snapshots.set(id, payload);
    cachedOffline.set(id, fromCache);
    if (prefs.offline) storage.set("meteo.v5.snapshot." + id, payload);
    return { payload, offline: fromCache };
  } catch {
    const saved = prefs.offline ? storage.get("meteo.v5.snapshot." + id) : null;
    if (saved?.station?.id === id) {
      snapshots.set(id, saved);
      cachedOffline.set(id, true);
      return { payload: saved, offline: true };
    }
    throw Error(
      "Nessuna fotografia salvata per questa località. Serve una prima apertura con connessione.",
    );
  }
}
function scheduleRefresh() {
  clearTimeout(timer);
  timer = setTimeout(() => refreshCycle(), REFRESH_MS);
}
async function refreshCycle() {
  if (refreshing) return;
  refreshing = true;
  $("#refresh").disabled = true;
  try {
    await loadStations();
    const results = await Promise.allSettled(
      stations.map((s) => loadSnapshot(s.id)),
    );
    const own = results[stations.findIndex((s) => s.id === activeId)];
    if (own?.status !== "fulfilled")
      throw Error(own?.reason?.message || "Dati non disponibili");
    data = own.value.payload;
    offline = own.value.offline;
    render();
  } catch (error) {
    notice(error.message);
    if (!data)
      $("#view").innerHTML =
        `<div class="empty"><h1>Dati temporaneamente non disponibili</h1><p>${esc(error.message)}</p><a href="/pro/">Apri gli strumenti Pro</a></div>`;
  } finally {
    refreshing = false;
    $("#refresh").disabled = false;
    scheduleRefresh();
  }
}
function navigate(next) {
  page = next;
  const url = new URL(location.href);
  url.searchParams.set("page", page);
  url.searchParams.set("station", activeId);
  history.pushState(null, "", url);
  render();
  window.scrollTo({ top: 0, behavior: "instant" });
}
window.addEventListener("popstate", () => {
  page = new URL(location.href).searchParams.get("page") || "today";
  const id = new URL(location.href).searchParams.get("station");
  if (id && snapshots.has(id)) {
    activeId = id;
    data = snapshots.get(id);
    offline = !navigator.onLine || cachedOffline.get(id) === true;
    $("#station").value = id;
  }
  render();
});
$("#navigation").addEventListener("click", (event) => {
  const button = event.target.closest("[data-page]");
  if (button) navigate(button.dataset.page);
});
$("#station").addEventListener("change", async (event) => {
  activeId = event.target.value;
  selectedNight = "";
  if (snapshots.has(activeId)) {
    data = snapshots.get(activeId);
    offline = !navigator.onLine || cachedOffline.get(activeId) === true;
  } else {
    try {
      const found = await loadSnapshot(activeId);
      data = found.payload;
      offline = found.offline;
    } catch (error) {
      notice(error.message);
      return;
    }
  }
  const url = new URL(location.href);
  url.searchParams.set("station", activeId);
  history.replaceState(null, "", url);
  render();
});
$("#refresh").addEventListener("click", () => {
  clearTimeout(timer);
  refreshCycle();
});
$("#theme").addEventListener("click", () => {
  setTheme(prefs.theme === "light" ? "dark" : "light");
  render();
});
function chart(
  series,
  { unit = "°C", height = 235, reference = true, gapMs = 90 * 60000 } = {},
) {
  const clean = series.map((s) => ({
    ...s,
    points: s.points
      .map((p) => ({ t: Date.parse(p.t), v: finite(p.v) ? Number(p.v) : null }))
      .filter((p) => Number.isFinite(p.t)),
  }));
  const all = clean.flatMap((s) => s.points).filter((p) => p.v !== null);
  if (all.length < 2)
    return '<div class="chart-empty">Servono più dati per disegnare questo grafico.</div>';
  const w = 900,
    h = height,
    left = 48,
    right = 12,
    top = 15,
    bottom = 35;
  let min = Math.min(...all.map((p) => p.v)),
    max = Math.max(...all.map((p) => p.v));
  const pad = Math.max((max - min) * 0.18, unit === "%" ? 2 : 1);
  min -= pad;
  max += pad;
  const t0 = Math.min(...all.map((p) => p.t)),
    t1 = Math.max(...all.map((p) => p.t)) + 1;
  const x = (t) => left + ((t - t0) / (t1 - t0)) * (w - left - right),
    y = (v) => h - bottom - ((v - min) / (max - min)) * (h - top - bottom);
  let svg = `<svg class="chart" viewBox="0 0 ${w} ${h}" role="img" aria-label="Grafico ${esc(unit)}; le interruzioni indicano dati mancanti"><title>${esc(series.map((s) => s.name).join(", "))}</title>`;
  for (let i = 0; i < 4; i++) {
    const v = min + ((max - min) * i) / 3;
    svg += `<path class="axis" d="M${left} ${y(v)}H${w - right}"/><text x="${left - 9}" y="${y(v) + 4}" text-anchor="end">${num(v, unit === "%" ? 0 : 1)}</text>`;
  }
  for (let i = 0; i < 5; i++) {
    const t = t0 + ((t1 - t0) * i) / 4;
    svg += `<text x="${x(t)}" y="${h - 9}" text-anchor="${i === 0 ? "start" : i === 4 ? "end" : "middle"}">${esc(t1 - t0 > 3 * 86400000 ? dayLabel(new Date(t).toISOString()) : clock(new Date(t).toISOString()))}</text>`;
  }
  for (const s of clean) {
    let path = "",
      previous = null;
    for (const p of s.points) {
      if (p.v === null) {
        previous = null;
        continue;
      }
      const connected = previous && p.t - previous.t <= (s.gapMs || gapMs);
      path += `${connected ? "L" : "M"}${x(p.t).toFixed(2)},${y(p.v).toFixed(2)} `;
      previous = p;
    }
    svg += `<path d="${path}" fill="none" stroke="${s.color}" stroke-width="2.8" stroke-linejoin="round" stroke-linecap="round" ${s.dash ? 'stroke-dasharray="7 5"' : ""}/>`;
  }
  if (reference && Date.now() > t0 && Date.now() < t1) {
    svg += `<path d="M${x(Date.now())} ${top}V${h - bottom}" stroke="var(--orange)" stroke-width="1.5" stroke-dasharray="3 4"/><text x="${x(Date.now()) + 6}" y="${top + 9}">Adesso</text>`;
  }
  svg += "</svg>";
  return `<div class="chart-legend">${series.map((s) => `<span><i class="swatch ${s.dash ? "forecast-swatch" : ""}" style="border-color:${s.color}"></i>${esc(s.name)}</span>`).join("")}<span>${esc(unit)}</span></div>${svg}`;
}
function title(heading, extra = "") {
  return `<div class="section-heading"><h2>${esc(heading)}</h2>${extra}</div>`;
}
function currentHero() {
  const c = data.current || {},
    f = data.forecast?.[0] || {},
    day = data.daily?.[0] || {},
    fresh = finite(currentAge()) && currentAge() <= 20 && !offline;
  const measured = finite(c.temp_c),
    temp = measured ? c.temp_c : f.temp_c;
  return `<div class="grid"><article class="card hero span-8"><div class="hero-main"><div><span class="eyebrow">${measured ? "MISURA ECOWITT" : "PREVISIONE · NON MISURA"}</span><h1>${esc(f.description || "Il tempo nella tua stazione")}</h1><div class="temperature">${num(temp)}<sup>°C</sup></div><p class="status-line">${measured ? "Rilevata alle " + clock(c.time) : "Valida alle " + clock(f.valid_time)} · ${fresh ? "live" : measured ? ageText(currentAge()) : "fonte modellistica"}</p></div>${weatherIcon(f.description, f.is_day === 0, "weather-art")}</div><div class="hero-bottom"><span>Previsione da ora <b>${num(day.temp_min)}° / ${num(day.temp_max)}°</b></span><span>Percepita <b>${num(measured ? c.feels_like_c : f.feels_like_c)}°</b></span><span>${esc(offline ? "DATI SALVATI" : data.station.role === "primary" ? "ROMA · STAZIONE" : "COMACCHIO · STAZIONE")}</span></div></article><div class="side-metrics">${[
    ["Umidità", c.humidity, "%", `Rugiada ${num(c.dewpoint_c)}°`],
    ["Vento", c.wind_kmh, "km/h", `Raffica ${num(c.windgust_kmh)}`],
    ["Pioggia 24 h", data.rain_24h_mm, "mm", "Somma campioni disponibili"],
    ["Pressione", c.pressure_hpa, "hPa", "Misura della stazione"],
  ]
    .map(
      ([name, value, unit, note]) =>
        `<article class="card"><h3>${name}</h3><div class="metric">${num(value, name === "Umidità" ? 0 : 1)}<small>${unit}</small></div><div class="metric-caption">${note}</div></article>`,
    )
    .join("")}</div></div>`;
}
function hourStrip() {
  return `<article class="card span-12"><div class="card-header"><h2>Le prossime ore</h2><button class="quiet" data-go="forecast">7 giorni →</button></div><div class="hour-strip">${
    (data.forecast || [])
      .slice(0, 24)
      .map(
        (f, i) =>
          `<div class="hour-cell"><span>${i === 0 && Math.abs(Date.parse(f.valid_time) - Date.now()) < 3600000 ? "Ora" : clock(f.valid_time)}</span>${weatherIcon(f.description, f.is_day === 0)}<b>${num(f.temp_c)}°</b><div class="rain">${num(f.precip_probability, 0)}%</div><div class="wind">${num(f.wind_kmh, 0)} km/h</div><div class="wind">☁ ${num(f.clouds, 0)}%</div></div>`,
      )
      .join("") || "<p>Previsione non disponibile.</p>"
  }</div><div class="metric-caption">Percentuale blu: probabilità di precipitazione · copertura nuvolosa indicata separatamente.</div></article>`;
}
function weatherChart() {
  return `<article class="card span-12"><div class="card-header"><h2>Dal tempo misurato a quello previsto</h2>${proAnchor("overview", "Apri Pro")}</div>${chart(
    [
      {
        name: "Stazione · misurato",
        color: "var(--blue)",
        gapMs: 20 * 60000,
        points: (data.observations || [])
          .filter((r) => Date.parse(r.time) > Date.now() - 24 * 3600000)
          .map((r) => ({ t: r.time, v: r.temp_c })),
      },
      {
        name: "Previsione",
        color: "var(--teal)",
        dash: true,
        points: (data.forecast || [])
          .slice(0, 36)
          .map((r) => ({ t: r.valid_time, v: r.temp_c })),
      },
    ],
  )}<p class="metric-caption">Le interruzioni rappresentano dati mancanti. La previsione non riempie lo storico misurato.</p></article>`;
}
function astroCard() {
  const a = data.astronomy?.[prefs.profile] || {},
    n = a.nights?.[0];
  return `<article class="card night-card span-6"><div class="card-header"><h2>Il cielo di stanotte</h2><span class="tag">${esc(a.label || "Astronomia")}</span></div><div class="metric">${num(n?.remaining_good_hours, 1)}<small>ore favorevoli residue</small></div><div class="night-window"><span>Finestra continua migliore</span><strong>${n?.best_start ? clock(n.best_start) + " — " + clock(n.best_end) : "Nessuna finestra individuata"}</strong></div><p>${n ? "Limite prevalente: " + esc(n.limiting_factor) + "." : "In attesa di previsioni complete."} ${n?.incomplete_hours > 0 ? num(n.incomplete_hours) + " ore con valutazione incompleta." : ""}</p><a href="?page=astronomy" data-go="astronomy">Confronta le notti e leggi i criteri →</a></article>`;
}
function changeCard() {
  const c = data.forecast_change || {};
  return `<article class="card span-6"><div class="card-header"><h2>Cosa è cambiato</h2><span class="tag">PREVISIONI</span></div><h3>${esc(c.headline || "Confronto in preparazione")}</h3><p>${esc(c.detail || "Servono due emissioni archiviate.")}</p><ul class="fact-list"><li><span>Variazione temperatura media assoluta</span><b>${num(c.temperature_change_c)} °C</b></li><li><span>Differenza pioggia prevista</span><b>${num(c.rain_change_mm)} mm</b></li><li><span>Differenza raffica massima</span><b>${num(c.gust_change_kmh)} km/h</b></li></ul><p class="metric-caption">Confronto fra ore comuni. ${esc(data.calibration || "")}</p></article>`;
}
function historyCard() {
  const h = data.history || {};
  return `<article class="card span-6"><div class="card-header"><h2>Il tuo archivio</h2><span class="tag">MISURE</span></div><div class="metric">${num(h.days, 0)}<small>giornate disponibili</small></div><p class="metric-caption">Dal ${dayLabel(h.first)} al ${dayLabel(h.last)}</p><ul class="fact-list"><li><span>Buco massimo nel periodo</span><b>${num(h.max_gap_days, 0)} giorni</b></li></ul><button class="quiet" data-go="stations">Esplora copertura e confronti →</button></article>`;
}
function airCard() {
  const a = data.air?.current || {};
  return `<article class="card span-6"><div class="card-header"><h2>Aria e pollini</h2><span class="tag">CAMS · PREVISIONE</span></div><div class="metric">${num(a.european_aqi, 0)}<small>AQI europeo</small></div><p class="metric-caption">${data.air ? "Dati modellistici del " + dateTime(a.time) + ". Non sono misure Ecowitt." : "Le misure ambientali della stazione non includono AQI o pollini."}</p><ul class="fact-list"><li><span>PM2.5 / PM10</span><b>${num(a.pm2_5)} / ${num(a.pm10)} µg/m³</b></li><li><span>Graminacee / ambrosia</span><b>${num(a.grass_pollen, 0)} / ${num(a.ragweed_pollen, 0)} grani/m³</b></li></ul><button class="quiet" data-go="air">Apri qualità dell’aria →</button></article>`;
}
function todayView() {
  const next = (data.forecast || []).slice(0, 24),
    rain = next.find(
      (f) => Number(f.precip_probability) >= 40 && Number(f.rain_mm) > 0,
    );
  const intro = rain
    ? `Possibile pioggia dalle ${clock(rain.valid_time)}: probabilità ${num(rain.precip_probability, 0)}%.`
    : "Nelle ore disponibili non emerge una fase piovosa con probabilità almeno del 40%.";
  const renderers = {
    hours: hourStrip,
    chart: weatherChart,
    astronomy: astroCard,
    change: changeCard,
    history: historyCard,
    air: airCard,
  };
  return (
    currentHero() +
    `<p class="briefing">${next.length ? intro : "Previsione temporaneamente non disponibile."} <span>${esc(data.calibration || "")}</span></p>` +
    title(
      "La tua giornata, a colpo d’occhio",
      `<button class="quiet" data-settings>Personalizza</button>`,
    ) +
    `<div class="grid">${prefs.cards
      .filter((k) => !prefs.hidden.includes(k))
      .map((k) => renderers[k]())
      .join("")}</div>`
  );
}
function forecastView() {
  const rows = data.forecast || [],
    groups = [],
    blocks = new Map();
  for (const row of rows) {
    const key = Math.floor(
      Date.parse(row.valid_time) / (forecastStep * 3600000),
    );
    if (!blocks.has(key)) blocks.set(key, []);
    blocks.get(key).push(row);
  }
  for (const part of blocks.values()) {
    const values = (key) =>
      part.filter((row) => finite(row[key])).map((row) => Number(row[key]));
    const average = (key) => {
      const list = values(key);
      return list.length === part.length
        ? list.reduce((a, b) => a + b, 0) / list.length
        : null;
    };
    const maximum = (key) => {
      const list = values(key);
      return list.length ? Math.max(...list) : null;
    };
    const sums = values("rain_mm");
    groups.push({
      ...part[0],
      temp_c: average("temp_c"),
      feels_like_c: average("feels_like_c"),
      humidity: average("humidity"),
      wind_kmh: average("wind_kmh"),
      clouds: average("clouds"),
      confidence: average("confidence"),
      wind_gust_kmh: maximum("wind_gust_kmh"),
      rain_mm:
        sums.length === forecastStep ? sums.reduce((a, b) => a + b, 0) : null,
      precip_probability: maximum("precip_probability"),
    });
  }
  return `<h1>Le prossime giornate</h1><p>${esc(data.calibration)} · emissione ${dateTime(rows[0]?.issued_at)}</p><div class="grid">${(
    data.daily || []
  )
    .slice(0, 7)
    .map(
      (d) =>
        `<article class="card span-3"><span class="eyebrow">${dayLabel(d.date)}</span>${weatherIcon(d.description)}<h3>${esc(d.description || "Previsione")}</h3><div class="metric">${num(d.temp_max)}°<small>min ${num(d.temp_min)}°</small></div><p class="metric-caption">${num(d.rain_mm)} mm · probabilità ${num(d.pop_max, 0)}%<br>Vento ${num(d.wind_mean, 0)} km/h · raffica ${num(d.wind_max, 0)}</p></article>`,
    )
    .join(
      "",
    )}${weatherChart()}</div>${title("Dettaglio orario", proAnchor("forecast", "Confronto modelli Pro"))}<div class="controls"><label for="forecast-step">Passo della tabella</label><select id="forecast-step">${[1, 3, 6].map((n) => `<option value="${n}" ${n === forecastStep ? "selected" : ""}>${n} ${n === 1 ? "ora" : "ore"}</option>`).join("")}</select><small>Valori medi del blocco · raffica e probabilità: massimo · pioggia: somma solo con tutte le ore disponibili.</small></div><article class="card"><div class="table-wrap"><table><thead><tr><th>Ora locale</th><th>Temperatura</th><th>Percepita</th><th>Umidità</th><th>Vento / raffica</th><th>Pioggia</th><th>Probabilità</th><th>Nuvole</th><th>Fiducia</th></tr></thead><tbody>${groups.map((f) => `<tr><td>${dayLabel(f.valid_time)} ${clock(f.valid_time)}</td><td><b>${num(f.temp_c)} °C</b></td><td>${num(f.feels_like_c)} °C</td><td>${num(f.humidity, 0)}%</td><td>${num(f.wind_kmh, 0)} / ${num(f.wind_gust_kmh, 0)} km/h</td><td>${num(f.rain_mm)} mm</td><td>${num(f.precip_probability, 0)}%</td><td>${num(f.clouds, 0)}%</td><td>${finite(f.confidence) ? num(f.confidence, 0) + "%" : "In raccolta"}</td></tr>`).join("")}</tbody></table></div></article>${title("Verifica delle previsioni")}<div class="grid">${changeCard()}<article class="card span-6"><h2>Errori misurati, per orizzonte</h2>${qualityTable()}<p class="metric-caption">MAE: errore assoluto medio. Il confronto di validazione è su dati tenuti fuori dalla calibrazione; i campioni scarsi non dimostrano un miglioramento.</p></article></div>`;
}
function qualityTable() {
  const scores = (data.scores || []).filter((s) =>
    ["temp_c", "humidity", "wind_kmh"].includes(s.variable),
  );
  return scores.length
    ? `<div class="table-wrap"><table><thead><tr><th>Variabile / modello</th><th>Orizzonte</th><th>MAE</th><th>Validazione</th><th>N</th></tr></thead><tbody>${scores
        .slice(0, 18)
        .map(
          (s) =>
            `<tr><td>${esc(s.variable)} · ${esc(s.model)}</td><td>${esc(s.horizon)}</td><td>${num(s.mae)}</td><td>${num(s.holdout_mae)}</td><td>${num(s.n, 0)}</td></tr>`,
        )
        .join("")}</tbody></table></div>`
    : '<div class="empty">Verifica locale in raccolta. Non viene attribuita un’affidabilità non ancora misurata.</div>';
}
function stationsView() {
  const h = data.history || {},
    calendar = (h.calendar || []).slice(-historyDays),
    labels = {
      complete: "Campioni live ≥90%",
      partial: "Giorno parziale",
      imported: "Riepilogo importato",
      missing: "Nessun dato",
    },
    colors = {
      complete: "#38886b",
      partial: "#c7903e",
      imported: "#467acc",
      missing: "var(--soft)",
    };
  const unit =
    historyMetric === "rain_mm"
      ? "mm"
      : historyMetric === "humidity_mean"
        ? "%"
        : "°C";
  const series = stations.map((s, i) => ({
    name: s.name,
    color: i === 0 ? "var(--blue)" : "var(--orange)",
    gapMs: 1.1 * 86400000,
    points: (snapshots.get(s.id)?.history?.calendar || [])
      .slice(-historyDays)
      .map((r) => ({ t: r.date + "T12:00:00Z", v: r[historyMetric] })),
  }));
  return `<h1>La storia delle tue stazioni</h1><p>Calendario, misure e confronto. I riepiloghi giornalieri importati non diventano campioni orari.</p><div class="grid">${historyCard()}<article class="card span-6"><h2>Stato della stazione</h2><ul class="fact-list"><li><span>Ultimo campione</span><b>${dateTime(data.observed_at)}</b></li><li><span>Età della misura</span><b>${ageText(currentAge())}</b></li><li><span>Archivio più antico nel periodo</span><b>${dayLabel(h.first)}</b></li></ul>${proAnchor("station", "Diagnostica sensori e rapporti PDF")}</article></div>${title("Copertura giorno per giorno")}<div class="controls"><label for="history-days">Periodo</label><select id="history-days">${[30, 90, 180, 365].map((n) => `<option value="${n}" ${n === historyDays ? "selected" : ""}>${n} giorni</option>`).join("")}</select><label for="history-metric">Confronta</label><select id="history-metric">${[
    ["temp_mean_c", "Temperatura media"],
    ["humidity_mean", "Umidità media"],
    ["rain_mm", "Pioggia giornaliera"],
  ]
    .map(
      ([v, l]) =>
        `<option value="${v}" ${v === historyMetric ? "selected" : ""}>${l}</option>`,
    )
    .join(
      "",
    )}</select><a href="/api/v5/history/${encodeURIComponent(activeId)}.csv">Scarica CSV</a></div><article class="card"><div class="coverage-legend">${Object.entries(
    labels,
  )
    .map(
      ([k, label]) =>
        `<span><i style="background:${colors[k]}"></i>${label}</span>`,
    )
    .join(
      "",
    )}</div><div class="coverage-grid">${calendar.map((d) => `<button class="day-cell ${d.status}" data-day="${d.date}" aria-label="${esc(dayLabel(d.date) + " · " + labels[d.status])}" title="${esc(dayLabel(d.date) + " · " + labels[d.status])}"></button>`).join("")}</div><p id="day-detail" class="metric-caption">Tocca un giorno per conoscere origine e numero dei campioni. Il giorno corrente rimane parziale.</p>${chart(series, { unit, reference: false, gapMs: 1.1 * 86400000 })}<p class="metric-caption">Le linee si interrompono nei giorni mancanti. Confronto descrittivo tra microclimi diversi: nessuna correzione automatica Roma–Comacchio.</p></article><div class="controls"><button class="quiet" data-go="import">Importa uno storico · amministratore</button>${proAnchor("station", "Archivio completo e rapporti mensili")}</div>`;
}
function astronomyView() {
  const a = data.astronomy?.[prefs.profile] || {},
    nights = a.nights || [];
  if (!nights.some((n) => n.date === selectedNight))
    selectedNight = nights[0]?.date || "";
  const rows = (a.hours || []).filter((h) => h.date === selectedNight);
  return `<h1>Trova la tua prossima notte</h1><p>Buio effettivo, finestre continue e ragioni del punteggio. Il seeing e la trasparenza restano proxy previsionali.</p><div class="controls"><label for="astro-profile">Tipo di osservazione</label><select id="astro-profile">${[
    ["deep_sky", "Fotografia cielo profondo"],
    ["visual", "Osservazione visuale"],
    ["planetary", "Luna e pianeti"],
  ]
    .map(
      ([v, l]) =>
        `<option value="${v}" ${v === prefs.profile ? "selected" : ""}>${l}</option>`,
    )
    .join(
      "",
    )}</select>${proAnchor("astronomy", "Target, strumenti e piano notturno Pro")}</div><div class="grid">${astroCard()}<article class="card span-6"><h2>Come leggere la qualità</h2><p>Ore favorevoli: punteggio ≥65, dati essenziali completi e intervallo ancora futuro. I buchi non uniscono due finestre.</p><ul class="fact-list"><li><span>Profilo</span><b>${esc(a.label || "—")}</b></li><li><span>Sole sotto l’orizzonte</span><b>${prefs.profile === "deep_sky" ? "18°" : prefs.profile === "visual" ? "12°" : "6°"}</b></li><li><span>Nuvole</span><b>Limite conservativo</b></li></ul><details><summary>Limiti delle stime</summary><p>Il punteggio meteo non certifica la sicurezza della strumentazione. SQM/Bortle sono stime di atlante, non misure. Per il target usa anche altezza, Luna e ostacoli nel pianificatore Pro.</p></details></article></div>${title("Confronto tra le notti")}<article class="card"><div class="table-wrap"><table><thead><tr><th>Notte</th><th>Qualità media residua</th><th>Nuvole medie</th><th>Ore favorevoli residue</th><th>Finestra continua</th><th>Ore incomplete</th></tr></thead><tbody>${nights.map((n) => `<tr><td><button class="quiet" data-night="${n.date}">${dayLabel(n.date)}</button></td><td>${num(n.score, 0)}/100</td><td>${num(n.clouds_mean, 0)}%</td><td><b>${num(n.remaining_good_hours)} h</b></td><td>${n.best_start ? clock(n.best_start) + "–" + clock(n.best_end) + " · " + num(n.continuous_hours) + " h" : "—"}</td><td>${num(n.incomplete_hours)} h</td></tr>`).join("")}</tbody></table></div>${!nights.length ? '<p class="empty">Nessuna finestra calcolabile con i dati disponibili.</p>' : ""}</article>${title("Perché questo punteggio · " + dayLabel(selectedNight))}<article class="card"><div class="table-wrap"><table><thead><tr><th>Intervallo futuro</th><th>Qualità</th><th>Nuvole</th><th>Vento</th><th>Limite principale</th><th>Dettaglio</th></tr></thead><tbody>${rows
    .map(
      (h) =>
        `<tr><td>${clock(h.start)}–${clock(h.end)}</td><td>${h.complete ? num(h.astro_score, 0) + "/100" : "Incompleta"}</td><td>${num(h.clouds, 0)}%</td><td>${num(h.wind_kmh)} km/h</td><td>${esc(h.limiting_factor)}</td><td><details><summary>Fattori</summary><p>${Object.entries(
          h.penalties || {},
        )
          .map(([k, v]) => esc(k) + ": " + num(v) + " punti")
          .join(
            "<br>",
          )}${h.missing_fields?.length ? "<br>Mancano: " + esc(h.missing_fields.join(", ")) : ""}</p></details></td></tr>`,
    )
    .join("")}</tbody></table></div></article>`;
}
function airView() {
  const air = data.air;
  return `<h1>Aria e pollini</h1><p>Previsione CAMS per la località selezionata. Non è una misura della tua Ecowitt.</p><div class="grid">${airCard()}<article class="card span-6"><h2>Origine e aggiornamento</h2><p>${air ? esc(air.source) + " · acquisizione " + dateTime(air.fetched_at) : "Previsione ambientale non ancora disponibile nella fotografia."}</p>${proAnchor("air", "Apri grafici, misure ufficiali e dettagli Pro")}</article></div>${
    air
      ? title("Tendenza PM2.5 e PM10") +
        `<article class="card">${chart(
          [
            {
              name: "PM2.5 · CAMS",
              color: "var(--blue)",
              points: (air.hourly || [])
                .slice(0, 72)
                .map((r) => ({ t: r.time, v: r.pm2_5 })),
            },
            {
              name: "PM10 · CAMS",
              color: "var(--orange)",
              points: (air.hourly || [])
                .slice(0, 72)
                .map((r) => ({ t: r.time, v: r.pm10 })),
            },
          ],
          { unit: "µg/m³" },
        )}</article>`
      : ""
  }`;
}
function moreView() {
  const tools = [
    [
      "forecast",
      "≋",
      "Previsioni verificate",
      "Emissioni, cambiamenti e affidabilità per orizzonte.",
    ],
    [
      "air",
      "◌",
      "Aria e pollini",
      "CAMS e approfondimenti con fonti ufficiali.",
    ],
    [
      "maps",
      "◎",
      "Radar e mappe",
      "Osservazione separata dalla tendenza prevista.",
    ],
    [
      "stations",
      "⌁",
      "Archivio e rapporti",
      "Copertura, confronti, CSV e rapporti mensili.",
    ],
    [
      "astronomy",
      "☾",
      "Osservatorio astronomico",
      "Finestre, target, campo inquadrato e piano della notte.",
    ],
    [
      "notifications",
      "♧",
      "Avvisi personali",
      "Condizioni scelte da te, senza notifiche ripetute.",
    ],
  ];
  return `<h1>Il tuo osservatorio, completo</h1><p>Strumenti quotidiani e approfondimenti scientifici, nello stesso posto.</p><div class="grid">${tools.map(([p, i, h, t]) => `<a class="card tool-card span-4" href="?page=${p}" data-go="${p}"><span class="tool-icon">${i}</span><h2>${h}</h2><p>${t}</p></a>`).join("")}<a class="card tool-card span-12" href="/pro/?mode=city"><h2>Cerca una città o un CAP</h2><p>Previsioni mondiali e confronto con la stazione selezionata.</p></a><a class="card tool-card span-6" href="${esc(proLink("system"))}"><h2>Sistema e diagnostica</h2><p>Controllo salute, sorgenti e backup. I dettagli riservati richiedono accesso amministratore.</p></a><a class="card tool-card span-6" href="${esc(proLink("info"))}"><h2>Fonti, metodo e limiti</h2><p>Come sono ottenuti i dati, cosa è misurato e cosa è stimato.</p></a></div>`;
}
function mapsView() {
  return `<h1>Radar e mappe</h1><p>Ogni prodotto mantiene la propria origine e il proprio orario.</p><div class="grid"><article class="card span-6"><span class="tag">OSSERVAZIONE</span><h2>Radar e fulmini DPC</h2><p>Eco radar e fulminazioni osservate. Consulta nel pannello Pro l’ultimo fotogramma disponibile e il suo orario.</p>${proAnchor("radar", "Apri radar della località")}</article><article class="card span-6"><span class="tag">TENDENZA PREVISTA</span><h2>Movimento delle precipitazioni</h2><p>Il nowcast è una tendenza a breve termine, non una nuova osservazione. Se non sono disponibili fotogrammi futuri viene dichiarato nel pannello.</p>${proAnchor("radar", "Apri animazione e mappe")}</article></div>`;
}
function notificationsView() {
  return `<h1>Avvisi personali</h1><p>Facoltativi e disattivabili. Non sostituiscono i bollettini ufficiali né i sistemi di sicurezza dell’osservatorio.</p><article class="card"><div id="push-status" role="status">Verifica disponibilità…</div><div class="controls"><label><input type="checkbox" id="alert-rain" checked> Pioggia probabile</label><label><input type="checkbox" id="alert-wind"> Raffiche forti</label><label><input type="checkbox" id="alert-astro"> Finestra astronomica</label><label><input type="checkbox" id="alert-station"> Stazione non aggiornata</label></div><div class="controls"><label>Probabilità pioggia ≥ <input id="alert-pop" type="number" min="30" max="100" value="60" style="width:80px"> %</label><label>Raffica ≥ <input id="alert-gust" type="number" min="20" max="150" value="40" style="width:80px"> km/h</label></div><div class="controls"><label>Silenzioso dalle <input id="quiet-start" type="time" value="23:00"></label><label>alle <input id="quiet-end" type="time" value="08:00"></label></div><button id="enable-push" class="primary">Attiva su questo dispositivo</button> <button id="disable-push" class="quiet">Disattiva</button><p class="privacy-note">Vale per la località selezionata. Il browser chiederà il consenso; il recapito push resta riservato. I controlli seguono il cron di 10 minuti, senza risvegliare continuamente l’app.</p></article>`;
}
function importView() {
  return `<h1>Importa lo storico giornaliero</h1><p>Operazione riservata. Prima controlli l’anteprima, poi confermi lo stesso file. Non vengono ricostruite osservazioni orarie.</p><article class="card import-form"><label>Token amministratore<input id="import-token" type="password" autocomplete="off"></label><label>Export Ecowitt giornaliero (.xlsx, massimo 8 MB)<input id="import-file" type="file" accept=".xlsx"></label><p>Destinazione: <b>${esc(data.station.name)}</b>. Il token non viene salvato nel dispositivo.</p><button id="preview-import" class="primary">Controlla anteprima</button><button id="confirm-import" disabled>Conferma importazione</button><div id="import-result" role="status"></div></article>`;
}
function render() {
  if (!data) return;
  updateMeta();
  const active = ["today", "forecast", "stations", "astronomy"].includes(page)
    ? page
    : "more";
  document.querySelectorAll("#navigation button").forEach((b) => {
    if (b.dataset.page === active) b.setAttribute("aria-current", "page");
    else b.removeAttribute("aria-current");
  });
  const views = {
    today: todayView,
    forecast: forecastView,
    stations: stationsView,
    astronomy: astronomyView,
    air: airView,
    maps: mapsView,
    more: moreView,
    notifications: notificationsView,
    import: importView,
  };
  $("#view").innerHTML = (views[page] || todayView)();
  bindView();
  if (page === "notifications") checkPush();
}
function bindView() {
  $("#view")
    .querySelectorAll("[data-go]")
    .forEach((el) =>
      el.addEventListener("click", (ev) => {
        ev.preventDefault();
        navigate(el.dataset.go);
      }),
    );
  $("#view")
    .querySelectorAll("[data-settings]")
    .forEach((el) => el.addEventListener("click", openPreferences));
  $("#forecast-step")?.addEventListener("change", (ev) => {
    forecastStep = Number(ev.target.value);
    render();
  });
  $("#history-days")?.addEventListener("change", (ev) => {
    historyDays = Number(ev.target.value);
    render();
  });
  $("#history-metric")?.addEventListener("change", (ev) => {
    historyMetric = ev.target.value;
    render();
  });
  $("#astro-profile")?.addEventListener("change", (ev) => {
    prefs.profile = ev.target.value;
    storage.set("meteo.v5.preferences", prefs);
    render();
  });
  $("#view")
    .querySelectorAll("[data-night]")
    .forEach((el) =>
      el.addEventListener("click", () => {
        selectedNight = el.dataset.night;
        render();
      }),
    );
  $("#view")
    .querySelectorAll("[data-day]")
    .forEach((el) =>
      el.addEventListener("click", () => {
        const d = data.history.calendar.find((r) => r.date === el.dataset.day);
        $("#day-detail").textContent =
          dayLabel(d.date) +
          " · " +
          {
            complete: "Giornata live completa",
            partial: "Giornata live parziale",
            imported: "Riepilogo giornaliero importato, nessun campione orario",
            missing: "Nessun dato disponibile",
          }[d.status] +
          (finite(d.samples)
            ? " · " +
              num(d.samples, 0) +
              " / " +
              num(d.expected, 0) +
              " campioni attesi"
            : "") +
          " · T media " +
          num(d.temp_mean_c) +
          " °C · Pioggia " +
          num(d.rain_mm) +
          " mm";
      }),
    );
  $("#preview-import")?.addEventListener("click", () => sendImport(false));
  $("#confirm-import")?.addEventListener("click", () => sendImport(true));
  $("#import-file")?.addEventListener("change", () => {
    importDigest = null;
    $("#confirm-import").disabled = true;
  });
  $("#enable-push")?.addEventListener("click", enablePush);
  $("#disable-push")?.addEventListener("click", disablePush);
}
function preferenceRows() {
  return prefs.cards
    .map(
      (k, i) =>
        `<div class="preference-row"><label><input type="checkbox" data-card="${k}" ${prefs.hidden.includes(k) ? "" : "checked"}> ${cardNames[k]}</label><div><button type="button" data-up="${k}" aria-label="Sposta ${cardNames[k]} prima" ${i === 0 ? "disabled" : ""}>↑</button><button type="button" data-down="${k}" aria-label="Sposta ${cardNames[k]} dopo" ${i === prefs.cards.length - 1 ? "disabled" : ""}>↓</button></div></div>`,
    )
    .join("");
}
function openPreferences() {
  const dialog = $("#preferences");
  $("#default-station").innerHTML = stations
    .map(
      (s) =>
        `<option value="${esc(s.id)}" ${s.id === (prefs.station || activeId) ? "selected" : ""}>${esc(s.name)}</option>`,
    )
    .join("");
  $("#astro-preference").value = prefs.profile;
  $("#expert").checked = prefs.expert;
  $("#offline-storage").checked = prefs.offline;
  $("#card-settings").innerHTML = preferenceRows();
  dialog.showModal();
}
$("#settings").addEventListener("click", openPreferences);
$("#card-settings").addEventListener("click", (ev) => {
  const b = ev.target.closest("[data-up],[data-down]");
  if (!b) return;
  const key = b.dataset.up || b.dataset.down,
    from = prefs.cards.indexOf(key),
    to = from + (b.dataset.up ? -1 : 1);
  if (to < 0 || to >= prefs.cards.length) return;
  prefs.hidden = [...$("#card-settings").querySelectorAll("[data-card]")]
    .filter((e) => !e.checked)
    .map((e) => e.dataset.card);
  [prefs.cards[from], prefs.cards[to]] = [prefs.cards[to], prefs.cards[from]];
  $("#card-settings").innerHTML = preferenceRows();
});
$("#save-preferences").addEventListener("click", async () => {
  prefs.station = $("#default-station").value;
  prefs.profile = $("#astro-preference").value;
  prefs.expert = $("#expert").checked;
  prefs.offline = $("#offline-storage").checked;
  prefs.hidden = [...$("#card-settings").querySelectorAll("[data-card]")]
    .filter((el) => !el.checked)
    .map((el) => el.dataset.card);
  storage.set("meteo.v5.preferences", prefs);
  if (!prefs.offline) await forgetWeather();
  else
    for (const [id, snapshot] of snapshots)
      storage.set("meteo.v5.snapshot." + id, snapshot);
  navigator.serviceWorker?.controller?.postMessage({
    type: "OFFLINE_SETTING",
    enabled: prefs.offline,
  });
  $("#preferences").close();
  render();
});
async function forgetWeather() {
  try {
    Object.keys(localStorage)
      .filter((k) => k.startsWith("meteo.v5.snapshot."))
      .forEach((k) => localStorage.removeItem(k));
    if ("caches" in window) {
      await caches.delete("meteo-v5-data");
    }
  } catch {}
}
$("#forget").addEventListener("click", async () => {
  await forgetWeather();
  prefs.offline = false;
  $("#offline-storage").checked = false;
  storage.set("meteo.v5.preferences", prefs);
  navigator.serviceWorker?.controller?.postMessage({
    type: "OFFLINE_SETTING",
    enabled: false,
  });
  notice("Dati meteo offline eliminati da questo dispositivo.");
});
async function sendImport(confirm) {
  const file = $("#import-file").files[0],
    token = $("#import-token").value;
  if (!file || !token) {
    $("#import-result").textContent =
      "Seleziona un file e inserisci il token amministratore.";
    return;
  }
  if (file.size > 8000000) {
    $("#import-result").textContent = "Il file supera 8 MB.";
    return;
  }
  if (confirm && !importDigest) return;
  $("#preview-import").disabled = true;
  $("#confirm-import").disabled = true;
  try {
    const r = await fetch(
      "/api/v5/import/" +
        encodeURIComponent(activeId) +
        (confirm ? "?confirm=" + importDigest : ""),
      {
        method: "POST",
        headers: {
          Authorization: "Bearer " + token,
          "Content-Type":
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        },
        body: file,
      },
    );
    const out = await r.json();
    if (!r.ok) throw Error(out.error);
    importDigest = out.sha256;
    $("#import-result").innerHTML =
      `<p>${esc(out.message)} ${out.days} giorni, dal ${esc(out.first)} al ${esc(out.last)}.</p><p class="privacy-note">Le date coincidenti saranno aggiornate in modo idempotente. Verifica la località prima di confermare.</p>`;
    $("#confirm-import").disabled = confirm;
    if (confirm) {
      $("#import-token").value = "";
      importDigest = null;
    }
  } catch (e) {
    $("#import-result").textContent = e.message;
  } finally {
    $("#preview-import").disabled = false;
  }
}
async function checkPush() {
  const saved = storage.get("meteo.v5.push", {});
  if (saved.rules) {
    for (const kind of ["rain", "wind", "astro", "station"])
      $("#alert-" + kind).checked = Boolean(
        saved.rules[kind === "astro" ? "astronomy" : kind],
      );
    $("#alert-pop").value = saved.rules.pop;
    $("#alert-gust").value = saved.rules.gust;
    $("#quiet-start").value = saved.rules.quiet_start;
    $("#quiet-end").value = saved.rules.quiet_end;
  }
  if (
    !("serviceWorker" in navigator) ||
    !("PushManager" in window) ||
    !("Notification" in window)
  ) {
    $("#push-status").textContent =
      "Notifiche push non disponibili in questo browser. Su alcuni telefoni serve installare prima la PWA.";
    $("#enable-push").disabled = true;
    return;
  }
  try {
    const r = await fetch("/api/v5/push/key");
    const info = await r.json();
    if (!r.ok || !info.public_key) throw Error();
    $("#push-status").textContent =
      Notification.permission === "denied"
        ? "Notifiche bloccate nelle impostazioni del browser."
        : saved.id
          ? "Avvisi già attivi su questo dispositivo. Attiva di nuovo per applicare queste preferenze alla località selezionata."
          : "Disponibili · attivazione solo con il tuo consenso.";
  } catch {
    $("#push-status").textContent =
      "Il canale push è in attesa di inizializzazione sul server.";
    $("#enable-push").disabled = true;
  }
}
function decodeKey(value) {
  const raw = atob(
    value.replace(/-/g, "+").replace(/_/g, "/") +
      "=".repeat((4 - (value.length % 4)) % 4),
  );
  return Uint8Array.from(raw, (c) => c.charCodeAt(0));
}
async function enablePush() {
  const status = $("#push-status");
  try {
    const permission = await Notification.requestPermission();
    if (permission !== "granted") {
      status.textContent = "Consenso non concesso. Nessun avviso attivato.";
      return;
    }
    const registration = await navigator.serviceWorker.ready;
    const info = await (await fetch("/api/v5/push/key")).json();
    let subscription = await registration.pushManager.getSubscription();
    if (!subscription)
      subscription = await registration.pushManager.subscribe({
        userVisibleOnly: true,
        applicationServerKey: decodeKey(info.public_key),
      });
    const saved = storage.get("meteo.v5.push", {});
    const rules = {
      rain: $("#alert-rain").checked,
      wind: $("#alert-wind").checked,
      astronomy: $("#alert-astro").checked,
      station: $("#alert-station").checked,
      pop: Number($("#alert-pop").value),
      gust: Number($("#alert-gust").value),
      quiet_start: $("#quiet-start").value,
      quiet_end: $("#quiet-end").value,
      profile: prefs.profile,
    };
    const response = await fetch("/api/v5/push/subscription", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...(saved.token ? { Authorization: "Bearer " + saved.token } : {}),
      },
      body: JSON.stringify({
        subscription: subscription.toJSON(),
        station_id: activeId,
        rules,
      }),
    });
    const out = await response.json();
    if (!response.ok) throw Error(out.error || "Attivazione non riuscita");
    storage.set("meteo.v5.push", {
      id: out.id,
      token: out.token || saved.token,
      station: activeId,
      rules,
    });
    status.textContent =
      "Avvisi attivi per " +
      data.station.name +
      ". Fascia silenziosa " +
      rules.quiet_start +
      "–" +
      rules.quiet_end +
      ".";
  } catch (e) {
    status.textContent = "Avvisi non attivati: " + e.message;
  }
}
async function disablePush() {
  try {
    const saved = storage.get("meteo.v5.push", {});
    if (saved.id) {
      const r = await fetch("/api/v5/push/subscription", {
        method: "DELETE",
        headers: {
          "Content-Type": "application/json",
          Authorization: "Bearer " + saved.token,
        },
        body: JSON.stringify({ id: saved.id }),
      });
      if (!r.ok) throw Error("Server non raggiungibile");
    }
    const registration = await navigator.serviceWorker.ready;
    const sub = await registration.pushManager.getSubscription();
    if (sub) await sub.unsubscribe();
    storage.set("meteo.v5.push", {});
    $("#push-status").textContent = "Avvisi disattivati su questo dispositivo.";
  } catch (e) {
    $("#push-status").textContent =
      "Disattivazione non confermata: " + e.message;
  }
}
async function checkAndroidShell() {
  const params = new URLSearchParams(location.search),
    installed = Number(params.get("shell"));
  if (
    (params.get("src") !== "twa" &&
      !document.referrer.startsWith("android-app://")) ||
    !installed
  )
    return;
  try {
    const info = await (
      await fetch("/app/static/android-version.json", { cache: "no-store" })
    ).json();
    if (info.shellVersion <= installed) return;
    const bar = document.createElement("div");
    bar.className = "notice";
    bar.innerHTML =
      'È disponibile un aggiornamento dell’app Android. <a href="/app/static/MeteoV4.apk">Scarica APK</a>';
    $("#content").prepend(bar);
  } catch {}
}
checkAndroidShell();
window.addEventListener("beforeinstallprompt", (event) => {
  event.preventDefault();
  installPrompt = event;
  $("#install").hidden = false;
});
$("#install").addEventListener("click", async () => {
  if (installPrompt) {
    await installPrompt.prompt();
    installPrompt = null;
    $("#install").hidden = true;
  }
});
window.addEventListener("offline", () => {
  offline = true;
  updateMeta();
});
window.addEventListener("online", () => {
  notice(
    "Connessione ristabilita. I dati si aggiorneranno al prossimo ciclo; puoi usare Aggiorna ora.",
  );
});
if ("serviceWorker" in navigator) {
  navigator.serviceWorker
    .register("/sw.js", { scope: "/" })
    .then((reg) => {
      (reg.active || navigator.serviceWorker.controller)?.postMessage({
        type: "OFFLINE_SETTING",
        enabled: prefs.offline,
      });
    })
    .catch(() => {});
}
navigator.serviceWorker?.addEventListener("controllerchange", () =>
  navigator.serviceWorker.controller?.postMessage({
    type: "OFFLINE_SETTING",
    enabled: prefs.offline,
  }),
);
refreshCycle();
