/* A night, a forecast revision and a shared cursor for all the sky tracks. */
"use strict";
(() => {
  const minute = 60000;
  const profiles = [
    ["deep_sky", "Fotografia cielo profondo"],
    ["visual", "Osservazione visuale"],
    ["planetary", "Luna e pianeti"],
  ];
  const states = new Map(), reports = new Map();
  let request = null;
  const getState = () => {
    const key = activeId + ":" + prefs.profile;
    if (!states.has(key)) states.set(key, { night: "", filter: "", category: "", framing: "", showAll: false, target: "", cursor: NaN });
    return states.get(key);
  };
  const dateLabel = day => new Intl.DateTimeFormat("it-IT", {
    weekday: "long", day: "numeric", month: "short", timeZone: "UTC",
  }).format(new Date(day + "T12:00:00Z"));
  const stamp = at => new Intl.DateTimeFormat("it-IT", {
    weekday: "short", hour: "2-digit", minute: "2-digit", timeZoneName: "shortOffset", timeZone: tz(),
  }).format(at);
  const duration = hours => {
    const minutes = Math.round(hours * 60);
    const h = Math.floor(minutes / 60), m = minutes % 60;
    return h ? h + " h" + (m ? " " + m + " min" : "") : m + " min";
  };
  const interval = window => window ? clock(window.start) + "–" + clock(window.end) : "—";
  const option = (id, label, selected) => `<option value="${esc(id)}"${id === selected ? " selected" : ""}>${esc(label)}</option>`;
  const quantity = (value, unit, digits = 0) => finite(value) ? num(value, digits) + unit : "—";
  const rowAt = (rows, at) => rows.find(row => Date.parse(row.start) <= at && at < Date.parse(row.end));
  const inWindow = (at, windows) => !!rowAt(windows || [], at);

  function setup() {
    const saved = storage.get("meteo.v5.planner", {});
    const profiles = storage.get("meteo.v5.equipment", []);
    const rigs = Array.isArray(profiles) ? profiles.filter(r => r?.name).map(r => ["saved:" + r.name, r]) : [];
    if (saved.equipment) rigs.unshift(["plan", saved.equipment]);
    const choice = storage.get("meteo.v5.cielo.rig", rigs[0]?.[0] || "");
    const rig = rigs.find(([id]) => id === choice);
    const sameSite = saved.station_id === activeId;
    return {
      saved, rigs, choice: rig?.[0] || "", equipment: rig?.[1] || null,
      constraints: {
        altitude: saved.altitude ?? 25, moon: saved.moon ?? 30, rotation: saved.rotation ?? 0,
        horizon: sameSite ? (saved.horizon || {}) : {},
        limits: saved.profile === prefs.profile ? (saved.limits || {}) : {},
      },
    };
  }

  function view() {
    const { rigs, choice } = setup();
    return `<section class="cielo" aria-labelledby="cielo-title">
      <header class="cielo-heading"><div><span class="eyebrow">OSSERVAZIONE E ASTROFOTOGRAFIA</span>
        <h1 id="cielo-title">Cielo · scegli la tua notte</h1><p>Confronta le prossime tre notti e trova quando il meteo e il soggetto sono dalla tua parte.</p></div>
        <button type="button" class="quiet" data-go="planner">Apri il pianificatore →</button></header>
      <section class="card cielo-context" aria-label="Contesto di osservazione">
        <div><span class="eyebrow">POSTAZIONE</span><strong>${esc(data.station.name)}</strong><small>${esc(tz())}</small></div>
        <label for="astro-profile"><span class="eyebrow">ATTIVITÀ</span><select id="astro-profile">${profiles.map(([id, name]) => option(id, name, prefs.profile)).join("")}</select></label>
        <div><label for="cielo-equipment" class="eyebrow">STRUMENTO</label><select id="cielo-equipment">
          ${option("", "Non impostato", choice)}${rigs.map(([id, rig]) => option(id, rig.name + (id === "plan" ? " · ultimo piano" : ""), choice)).join("")}
        </select><button type="button" class="cielo-link" data-go="personal">Gestisci strumenti →</button></div>
      </section>
      <div id="cielo-panel"></div>
      <details class="card cielo-more" id="cielo-atmosphere"><summary>Atmosfera e trasparenza stimate · CAMS</summary>
        <div id="cielo-atmosphere-content"></div></details>
    </section>`;
  }

  function fallback(state, message, retry) {
    const a = data.astronomy?.[prefs.profile];
    const rows = (a?.hours || []).filter(r => Date.parse(r.end) > Date.now());
    const nights = (a?.nights || []).filter(n => rows.some(r => r.date === n.date)).slice(0, 3);
    if (offline && !nights.some(n => n.date === state.night)) state.night = nights[0]?.date || "";
    return `<div class="card cielo-feedback"><p role="status">${esc(message)}</p>
      ${retry ? '<button type="button" id="cielo-retry" class="quiet">Riprova</button>' : ""}
      ${offline && nights.length ? `<p class="metric-caption">Copia del ${esc(dateTime(data.generated_at))}. Puoi consultare il meteo orario delle notti salvate.</p>
      <div class="cielo-nights">${nights.map(n => `<button type="button" class="cielo-night" data-cielo-night="${esc(n.date)}" aria-pressed="${n.date === state.night}">${esc(dateLabel(n.date))}</button>`).join("")}</div>
      ${hourly(rows.filter(r => r.date === state.night), true)}` : ""}</div>`;
  }

  function hourly(rows, open = false) {
    if (!rows.length) return "";
    return `<details class="card cielo-more"${open ? " open" : ""}><summary>Previsione oraria e fattori del punteggio</summary>
      <div class="table-wrap"><table><thead><tr><th>Ora locale</th><th>Meteo cielo</th><th>Nuvole</th><th>Vento</th><th>Fattori</th></tr></thead><tbody>${rows.map(h => `
        <tr><td>${interval(h)}</td><td>${h.complete ? quantity(h.astro_score, "/100") : "Incompleta"}</td><td>${quantity(h.clouds, "%")}</td><td>${quantity(h.wind_kmh, " km/h", 1)}</td>
          <td><details><summary>${esc(h.limiting_factor)}</summary><p>${Object.entries(h.penalties || {}).map(([key, value]) => esc(key) + ": " + quantity(value, " punti", 1)).join("<br>")}
          ${h.probability_source ? "<br>Probabilità pioggia: " + esc(h.probability_source) : ""}
          ${h.missing_fields?.length ? "<br>Mancano: " + esc(h.missing_fields.join(", ")) : ""}</p></details></td></tr>`).join("")}</tbody></table></div></details>`;
  }

  function nightCards(report) {
    const best = report.nights.reduce((a, b) => a.usable_hours >= b.usable_hours ? a : b);
    return `<nav class="cielo-nights" aria-label="Prossime tre notti">${report.nights.map(n => {
      const good = n.best_window, status = !n.darkness ? "Buio non calcolabile" : !n.assessed_hours ? "Dati meteo insufficienti" : good ? duration(n.usable_hours) + " favorevoli" : "Nessuna finestra favorevole";
      return `<button type="button" class="cielo-night" data-cielo-night="${esc(n.date)}" aria-pressed="${n.date === report.selected}">
        <span class="cielo-night-date">${esc(dateLabel(n.date))}${n.ongoing ? ' <span class="cielo-live">In corso</span>' : ""}</span>
        <strong>${esc(status)}</strong>
        <small>${good ? interval(good) + (best === n ? " · più favorevole" : "") : esc(n.limiting_factor || "Per il profilo scelto")}</small>
      </button>`;
    }).join("")}</nav>`;
  }

  function visibleTargets(report, state) {
    return report.targets.filter(t =>
      (!state.filter || t.filters.includes(state.filter)) &&
      (!state.category || t.category === state.category) &&
      (!state.framing || t.framing?.fits === (state.framing === "single"))
    );
  }

  function spotlight(report, night, target) {
    const window = target?.best_window;
    const forecastOld = report.timeline?.weather.some(row => !row.issued_at || Date.parse(report.as_of) - Date.parse(row.issued_at) > 12 * 3600000);
    const windowText = window ? interval(window) : !target && night.best_window ? interval(night.best_window) : "Nessuna finestra";
    return `<article class="card cielo-spotlight">
      <div><span class="eyebrow">${window ? "FINESTRA CONTINUA · " + esc(target.id) : target ? "SOGGETTO · " + esc(target.id) : "MIGLIORE FINESTRA METEO"}</span>
        <h2>${windowText}</h2>
        <p>${window ? esc(target.name + " · " + duration(window.hours) + " utilizzabili") :
          target ? esc(target.limiting_factor || "Finestra inferiore a 15 minuti") :
          night.best_window ? esc(duration(night.best_window.hours) + " favorevoli con le soglie scelte") :
          esc(night.limiting_factor || "Buio non disponibile per il profilo scelto")}</p>
        ${window && target.framing?.fits === false ? '<p class="metric-caption">Il soggetto non entra tutto nel campo: verifica rotazione o mosaico nel piano.</p>' : ""}
        ${window ? '<button type="button" id="cielo-plan" data-cielo-plan class="primary">Usa questa finestra →</button>' : ""}
        <span class="cielo-plan-status metric-caption" role="status"></span>
      </div><dl class="cielo-facts">
        <div><dt>Buio · Sole sotto −${report.depression}°</dt><dd>${night.darkness ? interval({start: night.darkness[0], end: night.darkness[1]}) : "—"}</dd></div>
        <div><dt>Finestra meteo migliore</dt><dd>${interval(night.best_window)}</dd></div>
        <div><dt>Limite meteo prevalente</dt><dd>${esc(night.limiting_factor || (night.assessed_hours ? "Nessuno nelle ore valutate" : "—"))}</dd></div>
        <div><dt>Copertura meteo utile</dt><dd>${duration(night.assessed_hours)} valutate${night.unknown_hours ? " · " + duration(night.unknown_hours) + " senza dati completi" : ""}</dd></div>
      </dl>
      <p class="metric-caption cielo-source">Fotografia meteo ${esc(dateTime(report.snapshot_at))} · ${esc(report.site.timezone)}.${forecastOld ? " Alcune emissioni sono assenti o più vecchie di 12 ore." : ""}</p>
    </article>`;
  }

  function targetsMarkup(report, state, candidates, selected) {
    if (report.profile === "planetary") return '<article class="card"><h2>Luna e pianeti</h2><p>Il grafico mostra altezza e illuminazione della Luna. Scegli un profilo di cielo profondo o visuale per consultare i soggetti del catalogo.</p></article>';
    if (!report.targets.length) return "";
    const categories = [...new Set(report.targets.map(t => t.category))].sort();
    const field = report.field;
    return `<article class="card cielo-targets" aria-labelledby="cielo-target-title">
      <header class="cielo-heading"><div><span class="eyebrow">CATALOGO CIELO PROFONDO</span><h2 id="cielo-target-title">Soggetti per questa notte</h2>
        <p>${field ? `Campo ${quantity(field.width_deg, "°", 2)} × ${quantity(field.height_deg, "°", 2)} · ${quantity(field.image_scale_arcsec_px, "″/px", 2)}` : "Scegli uno strumento per verificare l’inquadratura."}</p></div></header>
      <div class="cielo-filters">
        <label>Tipo<select id="cielo-category">${option("", "Tutti i tipi", state.category)}${categories.map(c => option(c, c, state.category)).join("")}</select></label>
        <label>Filtro<select id="cielo-filter">${[["", "Qualsiasi filtro"], ["broadband", "Banda larga / RGB"], ["narrowband", "Banda stretta / dual band"]].map(([id, name]) => option(id, name, state.filter)).join("")}</select></label>
        <label>Inquadratura<select id="cielo-framing"${field ? "" : " disabled"}>${[["", "Qualsiasi campo"], ["single", "Entra nel campo"], ["mosaic", "Da ampliare o ruotare"]].map(([id, name]) => option(id, name, state.framing)).join("")}</select></label>
      </div>
      <p class="metric-caption">Prima i soggetti con una finestra pianificabile e che entrano nel campo, poi la durata continua. Dimensioni e indicazioni sui filtri sono orientative.</p>
      ${candidates.length ? `<ol class="cielo-target-list">${candidates.slice(0, state.showAll ? undefined : 5).map((target, i) => {
        const w = target.best_window;
        return `<li><button type="button" class="cielo-target" data-cielo-target="${esc(target.id)}" aria-pressed="${target.id === selected?.id}">
          <span class="cielo-rank">${i + 1}</span><span><strong>${esc(target.id)} <span>· ${esc(target.name)}</span></strong>
          <small>${esc(target.category)} · picco ${quantity(target.peak_altitude, "°")}${target.framing ? " · " + esc(target.framing.status) : ""}</small></span>
          <span class="cielo-target-time">${w ? '<b>' + interval(w) + '</b><small>' + duration(w.hours) + ' continui</small>' : '<b>Non pianificabile</b><small>' + esc(target.limiting_factor) + '</small>'}</span>
        </button></li>`;
      }).join("")}</ol>` : '<p class="empty">Nessun soggetto con questi filtri.</p>'}
      ${candidates.length > 5 ? `<button type="button" class="quiet" id="cielo-all">${state.showAll ? "Mostra i primi cinque" : "Mostra tutti i " + candidates.length + " soggetti"}</button>` : ""}
      ${selected ? `<div class="controls"><button type="button" class="quiet" id="cielo-go-plot">Vedi grafico · ${esc(selected.id)} ↑</button>
        ${selected.best_window ? '<button type="button" data-cielo-plan class="primary">Usa questa finestra →</button>' : ""}
        <span class="cielo-plan-status metric-caption" role="status"></span></div>` : ""}
      <details class="cielo-rules"><summary>Soglie usate e qualità dei dati</summary><p>Altezza minima ${num(report.constraints.altitude, 0)}° · separazione lunare ${num(report.constraints.moon, 0)}° con Luna sopra l’orizzonte · rotazione ${num(report.constraints.rotation, 0)}° · ${report.constraints.horizon_configured ? "ostacoli del piano salvato" : "orizzonte senza ostacoli configurati"}.</p>
        <p>Nuvole ≤${num(report.constraints.limits.clouds, 0)}%, vento ≤${num(report.constraints.limits.wind_kmh, 0)} km/h, raffiche ≤${num(report.constraints.limits.gust_kmh, 0)} km/h, margine rugiada ≥${num(report.constraints.limits.dew_margin_c, 1)} °C, probabilità pioggia ≤${num(report.constraints.limits.rain_probability, 0)}%, pioggia prevista assente e indice meteo cielo ≥65/100. Gli intervalli mancanti interrompono la finestra.</p>
        <p>Puoi cambiare queste soglie nel pianificatore. I valori meteo sono previsioni; seeing, trasparenza e inquadratura sono stime.</p></details>
    </article>`;
  }

  function graph(timeline, night, target, limits) {
    const t0 = Date.parse(timeline.start), t1 = Date.parse(timeline.end);
    const times = timeline.times.map(Date.parse);
    const x = at => 1000 * (Math.max(t0, Math.min(t1, at)) - t0) / (t1 - t0);
    const band = (row, style, y = 12, height = 24) => {
      const start = x(Date.parse(row.start)), end = x(Date.parse(row.end));
      return `<rect x="${start}" width="${end - start}" y="${y}" height="${height}" rx="2" class="cielo-${style}"/>`;
    };
    const axes = [0, 0.25, 0.5, 0.75, 1].map(p => clock(t0 + (t1 - t0) * p));
    const curve = (values, low, high, style) => {
      const y = v => 42 - 36 * (v - low) / (high - low);
      const d = values.map((value, i) => (i ? "L" : "M") + x(times[i]).toFixed(1) + " " + y(value).toFixed(1)).join(" ");
      return `<path d="${d}" class="cielo-${style}" vector-effect="non-scaling-stroke"/>`;
    };
    const steps = (field, low, high, style) => {
      const d = timeline.weather.filter(row => finite(row[field])).map(row => {
        const y = (42 - 36 * (row[field] - low) / (high - low)).toFixed(1);
        return `M${x(Date.parse(row.start)).toFixed(1)} ${y}H${x(Date.parse(row.end)).toFixed(1)}`;
      }).join(" ");
      return `<path d="${d}" class="cielo-${style}" vector-effect="non-scaling-stroke"/>`;
    };
    const rule = (value, low, high) => {
      const y = 42 - 36 * (value - low) / (high - low);
      return `<path d="M0 ${y}H1000" class="cielo-threshold" vector-effect="non-scaling-stroke"/>`;
    };
    const track = (id, title, hint, inner) => `<div class="cielo-track"><div class="cielo-track-label"><b>${title}</b><span id="cielo-value-${id}">—</span></div>
      <svg class="cielo-plot" viewBox="0 0 1000 48" preserveAspectRatio="none" aria-hidden="true">
        <path d="M250 0V48M500 0V48M750 0V48" class="cielo-gridline" vector-effect="non-scaling-stroke"/>${inner}
        <line x1="0" y1="0" x2="0" y2="48" class="cielo-cursor" vector-effect="non-scaling-stroke"/></svg><small>${hint}</small></div>`;
    const dark = night.darkness ? { start: night.darkness[0], end: night.darkness[1] } : null;
    const topWind = Math.ceil(Math.max(15, limits.gust_kmh, ...timeline.weather.map(r => Math.max(r.wind_kmh || 0, r.wind_gust_kmh || 0))) / 10) * 10;
    const dew = timeline.weather.map(r => r.dew_margin_c).filter(finite);
    const minDew = Math.floor(Math.min(0, ...dew)), maxDew = Math.ceil(Math.max(5, limits.dew_margin_c, ...dew));
    return `<article class="card cielo-timeline" aria-labelledby="cielo-timeline-title">
      <header class="cielo-heading"><div><span class="eyebrow">UN SOLO CURSORE</span><h2 id="cielo-timeline-title" tabindex="-1">Come cambia il cielo</h2></div><output id="cielo-cursor-time" for="cielo-cursor"></output></header>
      <label class="cielo-slider-label" for="cielo-cursor">Esplora l’ora locale
        <input type="range" id="cielo-cursor" min="0" max="${Math.ceil((t1 - t0) / minute)}" step="1"></label>
      <div class="cielo-axis" aria-hidden="true">${axes.map(at => `<span>${at}</span>`).join("")}</div>
      ${track("dark", "Buio e finestre meteo", '<i class="cielo-key dark"></i> Buio del profilo <i class="cielo-key good"></i> Meteo favorevole',
        (dark ? band(dark, "dark") : "") + timeline.weather_windows.map(w => band(w, "good", 18, 12)).join(""))}
      ${track("moon", "Luna", "Altezza −90° / 90° · tratteggio: orizzonte", rule(0, -90, 90) + curve(timeline.moon_altitude, -90, 90, "lunar"))}
      ${track("cloud", "Nuvole", '<i class="cielo-key total"></i> Totali <i class="cielo-key low"></i> Basse <i class="cielo-key mid"></i> Medie <i class="cielo-key high"></i> Alte · 0–100%',
        [["clouds", "total"], ["cloud_low", "low"], ["cloud_mid", "mid"], ["cloud_high", "high"]].map(([f, s]) => steps(f, 0, 100, s)).join(""))}
      ${track("wind", "Vento e raffiche", '<i class="cielo-key low"></i> Vento <i class="cielo-key high"></i> Raffiche · 0–' + topWind + " km/h",
        rule(limits.wind_kmh, 0, topWind) + steps("wind_kmh", 0, topWind, "low") + steps("wind_gust_kmh", 0, topWind, "high"))}
      ${track("dew", "Margine dal punto di rugiada", minDew + "–" + maxDew + " °C · tratteggio: soglia del piano",
        rule(limits.dew_margin_c, minDew, maxDew) + steps("dew_margin_c", minDew, maxDew, "mid"))}
      ${target ? track("target", "Altezza · " + esc(target.id), '<i class="cielo-key low"></i> Soggetto <i class="cielo-key high"></i> Soglia e ostacoli <i class="cielo-key good"></i> Finestra del soggetto',
        rule(0, -90, 90) + target.windows.map(w => band(w, "good", 38, 6)).join("") +
        curve(target.minimum_altitude, -90, 90, "high") + curve(target.altitude, -90, 90, "low")) : ""}
      <p class="metric-caption">Le interruzioni del grafico meteo indicano dati mancanti o ore trascorse. Posizioni della Luna e dei soggetti stimate ogni 15 minuti.</p>
    </article>`;
  }

  function bindGraph(report, target, state) {
    const slider = $("#cielo-cursor");
    if (!slider) return;
    const t = report.timeline, t0 = Date.parse(t.start), t1 = Date.parse(t.end);
    const times = t.times.map(Date.parse);
    const at = (series, instant) => {
      const i = times.findIndex(time => time >= instant);
      if (i < 1) return series[i < 0 ? series.length - 1 : 0];
      return series[i - 1] + (series[i] - series[i - 1]) * (instant - times[i - 1]) / (times[i] - times[i - 1]);
    };
    const value = (id, text) => { const node = $("#cielo-value-" + id); if (node) node.textContent = text; };
    const update = offset => {
      state.cursor = Math.min(t1, Math.max(t0, t0 + offset * minute));
      slider.value = String(offset);
      const date = stamp(state.cursor), x = 1000 * (state.cursor - t0) / (t1 - t0);
      slider.setAttribute("aria-valuetext", date);
      $("#cielo-cursor-time").textContent = date;
      document.querySelectorAll(".cielo-cursor").forEach(line => {
        line.setAttribute("x1", x);
        line.setAttribute("x2", x);
      });
      const dark = t.darkness && Date.parse(t.darkness[0]) <= state.cursor && state.cursor < Date.parse(t.darkness[1]);
      const row = rowAt(t.weather, state.cursor);
      const moon = at(t.moon_altitude, state.cursor);
      value("dark", inWindow(state.cursor, t.weather_windows) ? "Buio · meteo favorevole" : dark ? (row ? "Buio" : "Buio · meteo non disponibile") : "Fuori dal buio del profilo");
      value("moon", quantity(moon, "°") + (moon < 0 ? " · sotto l’orizzonte" : " · sopra l’orizzonte") + " · " + quantity(at(t.moon_illumination, state.cursor), "% illuminata"));
      value("cloud", row ? [quantity(row.clouds, "% totali"), ...["low", "mid", "high"].map(key => quantity(row["cloud_" + key], "%"))].join(" · ") : "Meteo non disponibile");
      value("wind", row ? quantity(row.wind_kmh, " km/h") + " · " + quantity(row.wind_gust_kmh, " km/h raffiche") : "Meteo non disponibile");
      value("dew", row ? quantity(row.dew_margin_c, " °C", 1) : "Meteo non disponibile");
      if (target) value("target", quantity(at(target.altitude, state.cursor), "°") + " · " + quantity(at(target.moon_separation, state.cursor), "° dalla Luna") + (inWindow(state.cursor, target.windows) ? " · in finestra" : ""));
    };
    const night = report.nights.find(n => n.date === report.selected);
    const window = target?.best_window || night.best_window;
    const initial = state.cursor >= t0 && state.cursor <= t1 ? state.cursor :
      window ? (Date.parse(window.start) + Date.parse(window.end)) / 2 : Math.max(t0, Math.min(t1, Date.now()));
    update(Math.round((initial - t0) / minute));
    slider.oninput = () => update(Number(slider.value));
    document.querySelectorAll(".cielo-plot").forEach(plot => {
      plot.onclick = event => {
        const box = plot.getBoundingClientRect();
        update(Math.round((event.clientX - box.left) / box.width * (t1 - t0) / minute));
      };
    });
  }

  function show(report, state, focusId = "") {
    if (report.selected !== state.night) state.cursor = NaN;
    state.night = report.selected;
    if (!report.field) state.framing = "";
    const current = report.nights.find(n => n.date === state.night);
    const candidates = visibleTargets(report, state);
    const target = candidates.find(t => t.id === state.target) || candidates[0];
    state.target = target?.id || "";
    // Preserve a selected object when shortening the list after filtering.
    if (candidates.indexOf(target) >= 5) state.showAll = true;
    $("#cielo-panel").innerHTML = nightCards(report) + spotlight(report, current, target) +
      (report.timeline ? graph(report.timeline, current, target, report.constraints.limits) : "") +
      targetsMarkup(report, state, candidates, target) +
      hourly(report.timeline?.hourly || []);
    $("#cielo-panel").setAttribute("aria-busy", "false");
    bindGraph(report, target, state);
    $("#cielo-panel").querySelectorAll("[data-cielo-night]").forEach(b => b.onclick = () => {
      if (b.dataset.cieloNight === state.night) return;
      state.night = b.dataset.cieloNight; state.target = ""; state.cursor = NaN; state.showAll = false;
      load(state, false, state.night);
    });
    for (const key of ["filter", "category", "framing"]) {
      const el = $("#cielo-" + key);
      if (el) el.onchange = () => { state[key] = el.value; state.showAll = false; show(report, state, el.id); };
    }
    document.querySelectorAll("[data-cielo-target]").forEach(b => b.onclick = () => {
      state.target = b.dataset.cieloTarget;
      show(report, state);
      document.querySelector('[data-cielo-target][aria-pressed="true"]')?.focus({ preventScroll: true });
    });
    const more = $("#cielo-all");
    if (more) more.onclick = () => {
      state.showAll = !state.showAll;
      if (!state.showAll && candidates.indexOf(target) >= 5) state.target = candidates[0]?.id || "";
      show(report, state, more.id);
    };
    const goPlot = $("#cielo-go-plot");
    if (goPlot) goPlot.onclick = () => $("#cielo-timeline-title")?.focus();
    document.querySelectorAll("[data-cielo-plan]").forEach(plan => plan.onclick = () => {
      const { saved, equipment, constraints } = setup();
      const start = Math.max(Date.parse(target.best_window.start), Math.ceil(Date.now() / minute) * minute);
      const end = Date.parse(target.best_window.end);
      if (end - start < 15 * minute) {
        plan.nextElementSibling.textContent = "Questa finestra è passata. Aggiorna la notte per cercarne un’altra.";
        plan.disabled = true;
        return;
      }
      window.MeteoExtra.loadPlan({
        ...saved, ...constraints, station_id: activeId, profile: prefs.profile, equipment,
        start: new Date(start).toISOString(), end: new Date(end).toISOString(), targets: [target.id],
      });
      navigate("planner");
    });
    if (focusId) document.getElementById(focusId)?.focus({ preventScroll: true });
  }

  function load(state, force = false, focusNight = "") {
    const panel = $("#cielo-panel");
    if (!panel) return;
    const { equipment, constraints } = setup();
    // Only optical dimensions are needed by the server. Keep personal names local.
    const optical = equipment ? { name: "Strumento", camera: "Camera", telescope: "Ottica",
      ...Object.fromEntries(["aperture_mm", "focal_length_mm", "sensor_width_mm", "sensor_height_mm", "pixel_size_um", "focal_multiplier"].map(key => [key, equipment[key]])),
    } : null;
    const body = { ...constraints, station_id: activeId, profile: prefs.profile, date: state.night, equipment: optical };
    const cacheKey = JSON.stringify([data.generated_at, body]);
    const finish = report => {
      show(report, state);
      if (focusNight) document.querySelector('.cielo-night[aria-pressed="true"]')?.focus({ preventScroll: true });
    };
    const feedback = (message, retry) => {
      panel.innerHTML = fallback(state, message, retry);
      panel.setAttribute("aria-busy", "false");
      panel.querySelectorAll("[data-cielo-night]").forEach(b => b.onclick = () => {
        state.night = b.dataset.cieloNight;
        load(state);
      });
      const again = $("#cielo-retry");
      if (again) again.onclick = () => load(state, true);
    };
    if (offline || !navigator.onLine) {
      request?.controller.abort();
      request = null;
      feedback("Dati salvati · grafico astronomico e soggetti saranno disponibili quando torna il collegamento al servizio.", false);
      return;
    }
    const cached = reports.get(cacheKey);
    if (!force && cached && Date.now() - Date.parse(cached.as_of) < 5 * minute) {
      request?.controller.abort();
      request = null;
      finish(cached);
      return;
    }
    if (request?.key !== cacheKey || force) {
      request?.controller.abort();
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), 12000);
      const promise = fetch("/api/v5/tools/cielo", {
        method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body), signal: controller.signal,
      }).then(async response => {
        if (!response.ok) throw Error("Previsione non disponibile");
        const report = await response.json();
        if (report.schema !== 1 || report.site?.id !== body.station_id || report.profile !== body.profile) throw Error("Contesto non valido");
        return report;
      }).finally(() => clearTimeout(timer));
      request = { controller, promise, key: cacheKey };
    }
    const pending = request;
    if (panel.querySelector(".cielo-nights")) {
      // Keep the night controls in place while changing selection.
      panel.querySelectorAll("[data-cielo-night]").forEach(b => b.setAttribute("aria-pressed", String(b.dataset.cieloNight === state.night)));
      panel.querySelectorAll(":scope > :not(.cielo-nights)").forEach(el => el.remove());
      panel.querySelector(".cielo-nights").insertAdjacentHTML("afterend", '<p class="card cielo-loading" role="status">Carico la notte selezionata…</p>');
    } else {
      panel.innerHTML = '<p class="card" role="status">Calcolo le tre prossime notti…</p>';
    }
    panel.setAttribute("aria-busy", "true");
    pending.promise.then(report => {
      if (pending !== request || !panel.isConnected) return;
      request = null;
      reports.set(cacheKey, report);
      reports.set(JSON.stringify([data.generated_at, { ...body, date: report.selected }]), report);
      while (reports.size > 8) reports.delete(reports.keys().next().value);
      finish(report);
    }, () => {
      if (pending !== request || !panel.isConnected) return;
      request = null;
      feedback("Il grafico della notte è temporaneamente non disponibile. Riprova tra poco.", true);
    });
  }

  function bind(activePage) {
    if (activePage !== "astronomy") {
      request?.controller.abort();
      request = null;
      return;
    }
    $("#cielo-equipment").onchange = event => {
      storage.set("meteo.v5.cielo.rig", event.target.value);
      const state = getState();
      state.framing = "";
      load(state);
    };
    const atmosphere = $("#cielo-atmosphere");
    atmosphere.ontoggle = () => {
      const content = $("#cielo-atmosphere-content");
      if (atmosphere.open && !content.innerHTML) content.innerHTML = window.MeteoV53?.skyCard() || "";
    };
    load(getState());
  }

  window.MeteoCielo = { view, bind };
})();
