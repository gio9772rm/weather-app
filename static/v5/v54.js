/* V5.4: all summaries use the existing snapshot and its single refresh cycle. */
"use strict";
(() => {
  const HOUR = 3600000;
  const present = v => v !== null && v !== undefined && v !== "" && Number.isFinite(Number(v));
  function nextHours(snapshot, now = Date.now(), count = 6) {
    const start = Math.floor(now / HOUR) * HOUR, byHour = new Map();
    for (const row of snapshot.forecast || []) {
      const t = Date.parse(row.valid_time);
      if (t >= start && t < start + count * HOUR && t % HOUR === 0) byHour.set(t, row);
    }
    return Array.from({ length: count }, (_, i) => byHour.get(start + i * HOUR) || null);
  }
  function summarize(snapshot, now = Date.now()) {
    const hours = nextHours(snapshot, now), rows = hours.filter(Boolean);
    const count = keys => hours.filter(r => r && keys.every(k => present(r[k]))).length;
    const rain = rows.find(r => (present(r.rain_mm) && r.rain_mm >= .1) || (present(r.precip_probability) && r.precip_probability >= 40));
    const peak = key => rows.filter(r => present(r[key])).sort((a, b) => b[key] - a[key])[0];
    let clearing = null;
    for (let i = 1; i < hours.length - 1; i++) {
      if ([hours[i - 1]?.clouds, hours[i]?.clouds, hours[i + 1]?.clouds].every(present) && hours[i - 1].clouds >= 60 && hours[i].clouds <= 25 && hours[i + 1].clouds <= 25) {
        clearing = hours[i]; break;
      }
    }
    return { rows, rain, wind: peak("wind_kmh"), gust: peak("wind_gust_kmh"), clearing,
      rainHours: count(["rain_mm", "precip_probability"]), windHours: count(["wind_kmh", "wind_gust_kmh"]), skyHours: count(["clouds"]) };
  }
  function reliability(snapshot, now = Date.now(), fromCache = false) {
    const hours = nextHours(snapshot, now, 12), rows = hours.filter(Boolean);
    const age = t => (now - Date.parse(t)) / 60000;
    const issued = rows.map(r => age(r.issued_at));
    const missing = hours.filter(r => !r || !["temp_c", "rain_mm", "precip_probability", "wind_kmh", "wind_gust_kmh", "clouds"].every(k => present(r[k]))).length;
    const forecastStale = issued.some(a => !Number.isFinite(a) || a < -5 || a > 720);
    const snapshotAge = age(snapshot.generated_at), observedAge = age(snapshot.observed_at);
    const oldSnapshot = !Number.isFinite(snapshotAge) || snapshotAge < -5 || snapshotAge > 20;
    const oldObservation = !Number.isFinite(observedAge) || observedAge < -5 || observedAge > 20;
    const spread = snapshot.model_spread || {};
    const spreadAge = age(spread.evaluated_at);
    const common = Number.isFinite(spreadAge) && spreadAge >= -5 && spreadAge <= 20 && !fromCache
      ? (spread.rows || []).filter(r => present(r.spread_c) && r.model_count >= 2 && rows.some(f => Date.parse(f.valid_time) === Date.parse(r.valid_time))) : [];
    const max = common.length ? Math.max(...common.map(r => r.spread_c)) : null;
    return { missing, forecastStale, oldSnapshot, oldObservation, fromCache, common: common.length, spread: max,
      sample: present(snapshot.window_validation?.n) ? Number(snapshot.window_validation.n) : 0 };
  }
  function reliabilityMarkup(compact = false) {
    const r = reliability(data, Date.now(), offline);
    const freshness = r.fromCache ? "Copia offline" : r.oldSnapshot ? "Fotografia da aggiornare" : r.oldObservation ? "Misure da aggiornare" : "Misure recenti";
    const coverage = r.missing ? `${12 - r.missing}/12 ore complete` : "12 ore complete";
    return `<${compact ? "details" : "article"} class="card reliability"><${compact ? "summary" : "h2"}>Come leggere l’affidabilità · ${esc(freshness)}</${compact ? "summary" : "h2"}><div class="reliability-grid"><section><h3>Dati e aggiornamento</h3><p>${esc(freshness)} · ${coverage}.${r.forecastStale ? " Alcune emissioni sono assenti o più vecchie di 12 ore." : ""}</p><small>Misura ${dateTime(data.observed_at)} · fotografia ${dateTime(data.generated_at)}. Un dato mancante resta sconosciuto.</small></section><section><h3>Accordo fra modelli</h3><p>${r.spread === null ? "Confronto recente non disponibile." : `${r.spread >= 3 ? "Modelli discordi" : "Scarto termico contenuto"}: fino a ${num(r.spread)} °C su ${r.common} ore confrontabili.`}</p><small>Massimo scarto di temperatura tra almeno due modelli nelle stesse ore. La soglia descrittiva è 3 °C; l’accordo non garantisce la previsione.</small></section><section><h3>Riscontri delle probabilità</h3><p>${num(r.sample, 0)} finestre asciutte verificate · campione in raccolta.</p><small>Probabilità ensemble ancora non ricalibrate. Servono osservazioni successive e una validazione separata; i riscontri dei modelli deterministici sono consultabili in «Quanto ci prende?».</small></section></div><button class="quiet" data-go="models">Apri fonti e riscontri →</button></${compact ? "details" : "article"}>`;
  }
  function briefing() {
    const s = summarize(data), r = reliability(data, Date.now(), offline);
    const when = row => `${dayLabel(row.valid_time)} ${clock(row.valid_time)}`;
    const rain = s.rain ? `Segnale di pioggia · ${when(s.rain)}` : s.rainHours === 6 ? "Nessun segnale marcato di pioggia" : "Pioggia: dati incompleti";
    const rainReason = s.rain ? `${num(s.rain.rain_mm)} mm · probabilità ${num(s.rain.precip_probability, 0)}%.` : s.rainHours === 6 ? "In tutte le sei ore: accumulo inferiore a 0,1 mm e probabilità inferiore al 40%." : `Accumulo e probabilità disponibili per ${s.rainHours} ore su 6.`;
    const sky = s.clearing ? `Schiarite · ${when(s.clearing)}` : s.skyHours === 6 ? `Nuvolosità ${num(Math.min(...s.rows.map(r => r.clouds)), 0)}–${num(Math.max(...s.rows.map(r => r.clouds)), 0)}%` : "Cielo: copertura parziale";
    return `<article class="card next-hours"><div class="card-header"><div><span class="eyebrow">DALLE ${clock(new Date(Math.floor(Date.now() / HOUR) * HOUR).toISOString())} · 6 ORE</span><h2>Le prossime ore</h2></div><button class="quiet" data-go="forecast">Dettaglio orario →</button></div>${r.fromCache || r.oldSnapshot || r.forecastStale ? '<p class="quality-warning">Riepilogo da una copia offline o da dati da aggiornare. Controlla gli orari delle fonti.</p>' : ""}<div class="briefing-grid"><section><h3>${rain}</h3><p>${rainReason}</p>${s.rain && s.rainHours < 6 ? `<small>Dati completi per ${s.rainHours}/6 ore.</small>` : ""}</section><section><h3>${s.gust ? `Raffica fino a ${num(s.gust.wind_gust_kmh, 0)} km/h` : "Raffiche non disponibili"}</h3><p>${s.gust ? `${when(s.gust)}. ` : ""}${s.wind ? `Vento massimo ${num(s.wind.wind_kmh, 0)} km/h alle ${clock(s.wind.valid_time)}.` : "Vento non disponibile."}</p><small>Vento e raffiche completi per ${s.windHours}/6 ore.</small></section><section><h3>${sky}</h3><p>${s.clearing ? "Da almeno 60% di nuvole a non oltre 25%, per due ore consecutive." : `Nuvolosità disponibile per ${s.skyHours}/6 ore. Le percentuali descrivono la copertura del cielo.`}</p></section></div></article>${reliabilityMarkup(true)}`;
  }
  const input = (id, label, value, min, max, step = 1) => `<label>${esc(label)}<input id="${id}" type="number" min="${min}" max="${max}" step="${step}" value="${esc(value)}" required></label>`;
  function plannerView({ config, date, tomorrow, eq, limits, catalog, result }) {
    const localTime = (value, fallback) => {
      if (!value) return fallback;
      if (!/Z$|[+-]\d\d:\d\d$/.test(value)) return value;
      const at = new Date(value);
      return Number.isFinite(at.getTime()) ? new Intl.DateTimeFormat("sv-SE", { timeZone: data.station.timezone, year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", hourCycle: "h23" }).format(at).replace(" ", "T") : fallback;
    };
    const start = localTime(config.start, date + "T21:00"), end = localTime(config.end, tomorrow + "T06:00");
    return `<h1>Prepara la tua notte</h1><p>Tre passaggi per ${esc(data.station.name)}: scegli quando, verifica lo strumento, poi gli oggetti.</p><form id="planner-form" class="card guided-planner" novalidate><input id="plan-step" type="hidden" value="0"><nav class="planner-steps" aria-label="Passaggi del piano">${["La notte", "Lo strumento", "Gli oggetti"].map((s, i) => `<button type="button" data-plan-step="${i}" aria-controls="planner-step-${i}"><b>${i + 1}</b> ${s}</button>`).join("")}</nav>
      <section id="planner-step-0" data-plan-panel="0" aria-labelledby="night-heading"><h2 id="night-heading" tabindex="-1">1. Scegli la notte</h2><div class="form-grid"><label>Inizio · ${esc(data.station.timezone)}<input id="plan-start" type="datetime-local" value="${esc(start)}" required></label><label>Fine · stesso fuso<input id="plan-end" type="datetime-local" value="${esc(end)}" required></label><label>Tipo di osservazione<select id="plan-profile">${[["deep_sky", "Fotografia cielo profondo"], ["visual", "Osservazione visuale"], ["planetary", "Luna e pianeti"]].map(([id, label]) => `<option value="${id}" ${id === (config.profile || prefs.profile) ? "selected" : ""}>${label}</option>`).join("")}</select></label></div><p>Il piano considera buio, Luna e meteo. Le notti successive vengono confrontate negli stessi orari locali.</p><details><summary>Soglie meteo e geometria</summary><div class="form-grid">${input("plan-alt", "Altezza minima °", config.altitude ?? 25, 0, 85)}${input("plan-moon", "Distanza minima Luna °", config.moon ?? 30, 0, 180)}${input("plan-clouds", "Nuvole massime %", limits.clouds, 0, 100)}${input("plan-wind", "Vento massimo km/h", limits.wind_kmh, 0, 100)}${input("plan-gust", "Raffiche massime km/h", limits.gust_kmh, 0, 150)}${input("plan-dew", "Margine rugiada minimo °C", limits.dew_margin_c, 0, 15, .5)}${input("plan-pop", "Probabilità massima pioggia %", limits.rain_probability, 0, 100)}</div><p class="metric-caption">Soglie personali di pianificazione. Il margine di rugiada non misura la temperatura dell’ottica.</p></details></section>
      <section id="planner-step-1" data-plan-panel="1" aria-labelledby="equipment-heading" hidden><h2 id="equipment-heading" tabindex="-1">2. Verifica lo strumento</h2><p>Controlla il preset per ottenere campo inquadrato e campionamento corretti.</p><div class="form-grid"><label>Nome strumento<input id="eq-name" maxlength="80" value="${esc(eq.name)}" required></label><label>Ottica<input id="eq-telescope" maxlength="80" value="${esc(eq.telescope)}" required></label><label>Camera<input id="eq-camera" maxlength="80" value="${esc(eq.camera)}" required></label>${input("eq-focal", "Focale mm", eq.focal_length_mm, 20, 20000)}${input("eq-width", "Sensore larghezza mm", eq.sensor_width_mm, 1, 80, .01)}${input("eq-height", "Sensore altezza mm", eq.sensor_height_mm, 1, 80, .01)}</div><details><summary>Campionamento, rotazione e ostacoli</summary><div class="form-grid">${input("eq-aperture", "Apertura mm", eq.aperture_mm, 10, 2000)}${input("eq-pixel", "Pixel µm", eq.pixel_size_um, .5, 30, .01)}${input("eq-rotation", "Rotazione °", config.rotation ?? 0, 0, 360)}</div><label>Orizzonte misurato · azimut:altezza<input id="plan-horizon" value="${esc(config.horizonText || "")}" placeholder="0:10, 90:25, 180:5, 270:20"></label><p class="metric-caption">0° nord, 90° est. Usa gli ostacoli rilevati dalla postazione.</p></details><button id="remember-equipment" type="button" class="quiet">Salva questo strumento nel profilo</button></section>
      <section id="planner-step-2" data-plan-panel="2" aria-labelledby="targets-heading" hidden><h2 id="targets-heading" tabindex="-1">3. Scegli gli oggetti</h2><p>Seleziona da uno a otto oggetti. Il piano proporrà i blocchi utilizzabili e spiegherà le esclusioni.</p><fieldset><legend>Catalogo · <span id="target-count"></span></legend><div id="plan-targets" class="target-grid">${catalog.length ? catalog.map(t => `<label><input type="checkbox" name="target" value="${esc(t.name)}" ${(config.targets || catalog.slice(0, 3).map(x => x.name)).includes(t.name) ? "checked" : ""}>${esc(t.name)} <small>${esc(t.common_name)}</small></label>`).join("") : "Carico il catalogo…"}</div></fieldset><details><summary>Durata dei blocchi, preparazione e priorità</summary>${window.MeteoV53?.scheduleControls(config) || ""}</details><div id="planner-review" class="planner-review"></div></section>
      <div class="planner-actions"><button type="button" id="plan-back" class="quiet">Indietro</button><button type="button" id="plan-next" class="primary">Continua</button><button class="primary" id="calculate-plan" hidden>Calcola il piano</button></div><p id="plan-status" role="status"></p></form><section id="plan-result">${result}</section>`;
  }
  function bindWizard() {
    const form = $("#planner-form");
    if (!form) return;
    const review = () => {
      const targets = [...form.querySelectorAll('[name="target"]:checked')].map(e => e.value);
      $("#target-count").textContent = `${targets.length}/8 selezionati`;
      $("#planner-review").textContent = `${$("#plan-start").value.replace("T", " ")} → ${$("#plan-end").value.replace("T", " ")} · ${data.station.timezone}. ${$("#eq-name").value}. ${targets.join(", ") || "Scegli almeno un oggetto"}.`;
    };
    const show = (step, focus = true) => {
      step = Math.max(0, Math.min(2, Number(step) || 0));
      $("#plan-step").value = step;
      form.querySelectorAll("[data-plan-panel]").forEach(el => el.hidden = Number(el.dataset.planPanel) !== step);
      form.querySelectorAll("[data-plan-step]").forEach(el => { if (Number(el.dataset.planStep) === step) el.setAttribute("aria-current", "step"); else el.removeAttribute("aria-current"); });
      $("#plan-back").hidden = step === 0; $("#plan-next").hidden = step === 2; $("#calculate-plan").hidden = step !== 2;
      review(); if (focus) $(`#planner-step-${step} h2`).focus();
    };
    const validThrough = last => {
      for (const el of form.querySelectorAll("input,select")) {
        const panel = el.closest("[data-plan-panel]");
        if (panel && Number(panel.dataset.planPanel) <= last && !el.checkValidity()) {
          show(panel.dataset.planPanel, false); for (let p = el.parentElement; p && p !== form; p = p.parentElement) if (p.tagName === "DETAILS") p.open = true;
          el.reportValidity(); el.focus(); return false;
        }
      }
      if (last >= 0 && $("#plan-end").value <= $("#plan-start").value) { show(0); $("#plan-status").textContent = "La fine deve seguire l’inizio, anche quando passa la mezzanotte."; return false; }
      if (last === 2) {
        const n = form.querySelectorAll('[name="target"]:checked').length;
        if (n < 1 || n > 8) { show(2); $("#plan-status").textContent = "Seleziona da uno a otto oggetti."; return false; }
      }
      $("#plan-status").textContent = ""; return true;
    };
    form.querySelectorAll("[data-plan-step]").forEach(b => b.onclick = () => {
      const target = Number(b.dataset.planStep), current = Number($("#plan-step").value);
      if (target <= current || validThrough(target - 1)) show(target);
    });
    $("#plan-next").onclick = () => { const step = Number($("#plan-step").value); if (validThrough(step)) show(step + 1); };
    $("#plan-back").onclick = () => show(Number($("#plan-step").value) - 1);
    form.addEventListener("change", review);
    form.addEventListener("submit", e => {
      if (Number($("#plan-step").value) < 2) { e.preventDefault(); e.stopImmediatePropagation(); $("#plan-next").click(); }
      else if (!validThrough(2)) { e.preventDefault(); e.stopImmediatePropagation(); }
    }, true);
    show($("#plan-step").value, false);
  }
  globalThis.MeteoV54 = { nextHours, summarize, reliability, reliabilityMarkup, briefing, plannerView, bindWizard };
})();
