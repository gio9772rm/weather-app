/* Site insights share the existing 600-second weather cycle. */
"use strict";
(() => {
  const visits = new Map(), revisions = new Map(), replayCache = new Map();
  let modelVariable = "temp_c", modelHorizon = "0–6 h", selectedMonth = "", replay = null;
  const labels = { temp_c: "Temperatura · °C", humidity: "Umidità · %", wind_kmh: "Vento · km/h", rain_mm: "Pioggia · mm" };
  const status = { current: "Attuale", previous_sample: "Campione precedente", stale: "Dato datato", suspect: "Da verificare", missing: "Non disponibile" };
  const table = (head, rows) => `<div class="table-wrap"><table><thead><tr>${head.map(h => `<th>${esc(h)}</th>`).join("")}</tr></thead><tbody>${rows.map(row => `<tr>${row.map(v => `<td>${v}</td>`).join("")}</tr>`).join("")}</tbody></table></div>`;
  const empty = message => `<p class="empty">${esc(message)}</p>`;
  const insights = () => data.insights || {};
  const download = (filename, content, type) => {
    const url = URL.createObjectURL(new Blob([content], { type }));
    const a = document.createElement("a"); a.href = url; a.download = filename; a.click();
    URL.revokeObjectURL(url);
  };
  function observe(payload, fromCache) {
    if (fromCache) return;
    const id = payload.station.id, key = "meteo.v5.last-visit." + id;
    if (!visits.has(id)) visits.set(id, prefs.offline ? storage.get(key) : null);
    const previous = visits.get(id);
    const common = new Map((previous?.forecast || []).map(r => [r.valid_time, r]));
    const changes = [];
    for (const [variable, threshold, label, unit] of [["temp_c", 2, "Temperatura", "°C"], ["wind_gust_kmh", 10, "Raffiche", "km/h"], ["precip_probability", 20, "Probabilità pioggia", "punti percentuali"]]) {
      const rows = (payload.forecast || []).filter(r => Date.parse(r.valid_time) >= Date.now() && Date.parse(r.valid_time) <= Date.now() + 86400000 && common.has(r.valid_time) && finite(r[variable]) && finite(common.get(r.valid_time)[variable]));
      const best = rows.map(r => ({ time: r.valid_time, delta: r[variable] - common.get(r.valid_time)[variable] })).sort((a, b) => Math.abs(b.delta) - Math.abs(a.delta))[0];
      if (best && Math.abs(best.delta) >= threshold) changes.push({ variable, label, unit, ...best });
    }
    revisions.set(id, { previous_at: previous?.generated_at, changes });
    if (prefs.offline) storage.set(key, { generated_at: payload.generated_at, forecast: payload.forecast });
  }
  function changeList(changes) {
    return `<ul class="fact-list">${changes.map(c => `<li><span>${esc(c.label)} · ${dayLabel(c.time)} ${clock(c.time)}</span><b>${c.delta > 0 ? "+" : ""}${num(c.delta)} ${esc(c.unit)}</b></li>`).join("")}</ul>`;
  }
  function homeFocus() {
    const rows = (data.forecast || []).filter(r => Date.parse(r.valid_time) >= Date.now() - 3600000).slice(0, 24);
    const limits = { min: 5, max: 32, wind: 20, gust: 35, pop: 30, rain: 0, hours: 1, daylight: true };
    const window = globalThis.MeteoExtra?.activityWindows(rows, limits)?.windows?.[0];
    const recent = revisions.get(activeId), server = insights().revisions || {};
    const changes = recent?.previous_at ? recent.changes : server.changes || [];
    return `<div class="grid focus-grid"><article class="card span-6"><span class="tag">FINESTRA DIURNA</span><h2>${window ? clock(new Date(window.start).toISOString()) + "–" + clock(new Date(window.end).toISOString()) : "Nessuna finestra completa"}</h2><p>Almeno un’ora asciutta, vento ≤20 km/h, raffiche ≤35 km/h, 5–32 °C e probabilità pioggia ≤30%.</p><button class="quiet" data-go="activities">Imposta le tue soglie →</button></article><article class="card span-6"><span class="tag">${recent?.previous_at ? "DALL’ULTIMA CONSULTAZIONE" : "ULTIME EMISSIONI"}</span><h2>${changes.length ? "Previsione cambiata" : "Nessuna variazione significativa"}</h2>${changes.length ? changeList(changes.slice(0, 2)) : `<p>${recent?.previous_at || server.available ? "Nelle ore confrontabili, le variazioni restano sotto le soglie del riepilogo." : "Il confronto sarà disponibile dopo due fotografie utili."}</p>`}<button class="quiet" data-go="models">Verifica fonti e precisione →</button></article></div><div class="controls"><button class="quiet" data-go="quality">Origine, orari e qualità di ogni misura${insights().quality?.suspect_count ? " · " + insights().quality.suspect_count + " da verificare" : ""} →</button></div>`;
  }
  function qualityView() {
    const report = insights().quality || {}, rows = report.measurements || [];
    const preview = (data.forecast || []).slice(0, 24).flatMap(r => Object.entries(r.local_corrections || {}).map(([key, c]) => [dayLabel(r.valid_time) + " " + clock(r.valid_time), esc(labels[key] || key), num(c.original), num(r[key]), num(c.holdout_mae), num(c.holdout_n, 0)]));
    return `<h1>Da dove arriva ogni valore</h1><p>Orario effettivo, origine e controlli per ${esc(data.station.name)}. I segnali anomali richiedono verifica: le misure originali sono conservate.</p><article class="card"><h2>Ultime misure disponibili</h2>${(report.issues || []).filter(r => r.variable === "time").map(r => `<p class="quality-warning">${esc(r.message)}</p>`).join("")}${rows.length ? table(["Parametro", "Valore", "Origine", "Ora effettiva", "Stato", "Controlli"], rows.map(r => [esc(r.label), `${num(r.value)} ${esc(r.unit)}`, esc(r.source), dateTime(r.time), `<span class="quality-state ${r.status === "suspect" ? "quality-warning" : ""}">${esc(offline && r.status !== "missing" ? "Copia salvata · " + status[r.status] : status[r.status])}</span>`, (r.flags || []).map(esc).join("; ") || "Nessun segnale rilevato"])) : empty("Controlli disponibili dalla prossima pubblicazione.")}</article><article class="card"><h2>Previsioni e correzioni locali</h2><p>${esc(data.calibration)}. Le previsioni sono modellistiche; punto di rugiada e percepita sono grandezze derivate. Una correzione scaduta torna automaticamente alla previsione di base.</p>${preview.length ? table(["Ora prevista", "Parametro", "Originale", "Corretto", "Errore in validazione", "Riscontri"], preview) : empty("Nessuna correzione locale validata applicata nelle prossime 24 ore.")}<button class="quiet" data-go="models">Leggi la verifica completa →</button></article>`;
  }
  function modelsView() {
    const product = insights().verification, sources = insights().sources || {};
    const scores = (product?.scores || []).filter(r => r.variable === modelVariable && r.horizon === modelHorizon).sort((a, b) => a.holdout_mae - b.holdout_mae);
    const newModel = sources.weathernext;
    return `<h1>Quanto ci prende?</h1><p>Verifica locale per ${esc(data.station.name)}. Confronta errore, periodo e numero di riscontri; modelli valutati su periodi diversi non costituiscono una classifica omogenea.</p>${window.MeteoV53?.comparisonCard() || ""}<div class="controls"><label>Parametro<select id="model-variable">${Object.entries(labels).map(([key, label]) => `<option value="${key}" ${key === modelVariable ? "selected" : ""}>${esc(label)}</option>`).join("")}</select></label><label>Orizzonte<select id="model-horizon">${["0–6 h", "6–24 h", "24–72 h"].map(h => `<option ${h === modelHorizon ? "selected" : ""}>${h}</option>`).join("")}</select></label></div><article class="card"><h2>Risultati su osservazioni Ecowitt</h2>${scores.length ? table(["Modello / provenienza", "Periodo", "Riscontri / giorni", "Errore su dati successivi", "Correzione in prova", "Persistenza / riscontri", "Stato"], scores.map(r => [`${esc(r.provider === "canonical" ? "Previsione del sito" : r.model)}<br><small>${r.basis === "previous_run" ? "Archivio a scadenza fissa" : "Acquisizioni operative"}</small>`, `${dayLabel(r.start)}–${dayLabel(r.end)}`, `${num(r.n, 0)} / ${num(r.days, 0)}<br><small>${num(r.holdout_n, 0)} di validazione</small>`, num(r.holdout_mae, 2), num(r.corrected_mae, 2), `${num(r.persistence_mae, 2)} / ${num(r.persistence_n, 0)}`, r.applied && Date.now() - Date.parse(product.evaluated_at) <= 26 * 3600000 ? "Correzione attiva" : r.eligible ? "Candidato verificato" : "In raccolta / vantaggio insufficiente"])) : empty("Servono altri riscontri confrontabili per questo parametro e orizzonte.")}<p class="metric-caption">${esc(product?.method || "Verifica in preparazione")}. Valori più bassi indicano errori minori. La persistenza usa la misura già nota all’emissione. Valutazione: ${dateTime(product?.evaluated_at)}.</p></article><div class="grid">${[["WeatherNext 2", newModel, 13], ["Archivio ICON", sources.previous_icon, 27], ["Archivio ECMWF", sources.previous_ecmwf, 27]].map(([label, source, hours]) => `<article class="card span-4"><span class="tag">${source ? Date.now() - Date.parse(source.fetched_at) > hours * 3600000 ? "ULTIMO DATO DATATO" : "ACQUISITO" : "IN ATTESA"}</span><h2>${label}</h2><p>${source ? dateTime(source.fetched_at) : "Prima acquisizione non ancora disponibile."}</p><p>${esc(source?.note || "Il meteo principale continua a usare le fonti disponibili.")}</p></article>`).join("")}</div>${newModel?.rows?.length ? `<article class="card"><h2>WeatherNext · intervalli nativi di sei ore</h2>${chart([{ name: "Media ensemble WeatherNext", color: "var(--blue)", points: newModel.rows.filter(r => Date.parse(r.valid_time) >= Date.now()).map(r => ({ t: r.valid_time, v: r.temp_c })) }], { unit: "°C", gapMs: 7 * 3600000 })}<p class="metric-caption">La linea collega campioni ogni sei ore. Nuvole e pioggia sono derivate dal modello; non è una previsione radar a breve termine.</p></article>` : ""}`;
  }
  function archiveView() {
    const archive = insights().archive || {}, months = archive.months || [];
    if (!months.some(m => m.month === selectedMonth)) selectedMonth = months.at(-1)?.month || "";
    const chosen = months.find(m => m.month === selectedMonth), comparisons = [...snapshots.values()].map(s => ({ name: s.station.name, month: s.insights?.archive?.months?.find(m => m.month === selectedMonth) }));
    return `<h1>Il tuo archivio, mese per mese</h1><p>${esc(archive.note || "Riepiloghi automatici alla pubblicazione dei dati.")}</p><div class="controls"><label>Mese<select id="archive-month">${months.slice().reverse().map(m => `<option ${m.month === selectedMonth ? "selected" : ""}>${m.month}</option>`).join("")}</select></label><button id="archive-csv" class="quiet" ${months.length ? "" : "disabled"}>Esporta rapporto CSV</button></div>${chosen ? `<div class="grid"><article class="card span-4"><h2>Pioggia del mese</h2><div class="metric">${num(chosen.rain_sum_mm)}<small>mm nei giorni utilizzabili</small></div><p>${chosen.rain_days_available}/${chosen.expected_days} giorni con pioggia verificabile${chosen.is_partial ? " · totale parziale" : ""}</p></article><article class="card span-4"><h2>Giorni piovosi</h2><div class="metric">${num(chosen.rainy_days, 0)}<small>almeno 0,1 mm</small></div><p>Periodo asciutto più lungo: ${num(chosen.longest_dry_spell_days, 0)} giorni. Le lacune interrompono il conteggio.</p></article><article class="card span-4"><h2>Copertura del mese</h2><div class="metric">${num(chosen.coverage_percent, 0)}<small>% giorni completi o importati</small></div><p>${chosen.complete_days} completi · ${chosen.imported_days} riepiloghi importati · ${chosen.partial_days} parziali. Giorno corrente escluso.</p></article></div>` : empty("Nessun mese disponibile.")}<article class="card"><h2>Confronto nello stesso mese</h2>${table(["Stazione", "Minima", "Massima", "Media", "Pioggia disponibile", "Copertura"], comparisons.map(c => [esc(c.name), num(c.month?.temp_min_c) + " °C", num(c.month?.temp_max_c) + " °C", num(c.month?.temp_mean_c) + " °C", num(c.month?.rain_sum_mm) + " mm", num(c.month?.coverage_percent, 0) + "%"]))}<p class="metric-caption">Microclimi e coperture diversi: il confronto è descrittivo.</p></article><article class="card"><h2>Estremi del periodo disponibile</h2><p>${dayLabel(archive.period?.start)}–${dayLabel(archive.period?.end)}</p>${table(["Record", "Valore", "Giorno", "Origine"], Object.entries(archive.records || {}).map(([key, record]) => [esc({ temp_min_c: "Temperatura minima", temp_max_c: "Temperatura massima", rain_mm: "Pioggia giornaliera massima" }[key]), num(record.value) + (key === "rain_mm" ? " mm" : " °C"), dayLabel(record.date), record.status === "imported" ? "Riepilogo importato" : "Giornata completa"]))}</article><button class="quiet" data-go="events">Rivedi un evento delle ultime due settimane →</button>`;
  }
  function eventView() {
    const localInput = value => new Intl.DateTimeFormat("sv-SE", { timeZone: data.station.timezone, year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", hourCycle: "h23" }).format(value).replace(" ", "T");
    return `<h1>Rivedi un evento meteo</h1><p>Misure, radar osservato e previsione pubblicata prima dell’evento, sulla stessa finestra temporale.</p><form id="event-form" class="card controls"><label>Inizio · ${esc(data.station.timezone)}<input id="event-start" type="datetime-local" value="${localInput(new Date(Date.now() - 6 * 3600000))}" required></label><label>Fine · stesso fuso<input id="event-end" type="datetime-local" value="${localInput(new Date(Date.now() - 600000))}" required></label><button class="primary">Ricostruisci evento</button><span id="event-status" role="status"></span></form><p class="metric-caption">Massimo 24 ore negli ultimi 14 giorni. Le ore ambigue al cambio d’ora richiedono un intervallo non ambiguo.</p><section id="event-result">${replay?.station_id === activeId ? replayMarkup() : ""}</section>`;
  }
  function replayMarkup() {
    const r = replay, obs = r.observations || [], fc = r.forecast || [], radar = r.radar || [];
    const frames = [];
    for (let t = Date.parse(r.start); t <= Date.parse(r.end); t += 3600000) frames.push(new Date(Math.floor(t / 300000) * 300000).toISOString());
    r.frames = frames;
    const specs = [["Temperatura · °C", "temp_c", "temp_c", "°C"], ["Raffiche · km/h", "windgust_kmh", "wind_gust_kmh", "km/h"]];
    return `<article class="card"><h2>Previsione conosciuta prima dell’evento</h2><p>${r.forecast_known_at ? "Archiviata il " + dateTime(r.forecast_known_at) : "Non disponibile: il sito non aveva ancora archiviato una previsione utilizzabile. Nessuna viene ricostruita a posteriori."}</p></article><div class="grid">${specs.map(([label, observed, predicted, unit]) => `<article class="card span-6"><h2>${label}</h2>${chart([{ name: "Ecowitt osservata", color: "var(--teal)", points: obs.map(row => ({ t: row.time, v: row[observed] })) }, { name: "Previsione archiviata", color: "var(--orange)", dash: true, points: fc.map(row => ({ t: row.valid_time, v: row[predicted] })) }], { unit })}</article>`).join("")}</div><article class="card"><h2>Intensità pioggia osservata</h2>${chart([{ name: "Ecowitt · intensità", color: "var(--teal)", points: obs.map(row => ({ t: row.time, v: row.rain_rate_mm_h })) }, { name: "DPC · stima radar", color: "var(--blue)", points: radar.map(row => ({ t: row.observed_at, v: row.sri_point_mm_h })) }], { unit: "mm/h", gapMs: 20 * 60000 })}<p>${esc(r.note)}</p></article><article class="card"><h2>Radar storico DPC · osservazione</h2><div class="controls"><label>Fotogramma<select id="event-frame">${frames.map((at, index) => `<option value="${index}">${dateTime(at)}</option>`).join("")}</select></label><button id="load-history-radar" class="quiet">Mostra fotogramma</button><button id="download-dpc" class="quiet">Scarica GeoTIFF ufficiale</button></div><div id="event-radar" class="historical-radar"></div><p id="event-radar-status" role="status"></p><div class="chart-legend">${[["#50b4eb", "0,1–1"], ["#2873d2", "1–5"], ["#46b97d", "5–10"], ["#ebc332", "10–30"], ["#f06e32", "30–50"], ["#c83c64", "≥50"]].map(([color, text]) => `<span><i class="swatch" style="border-color:${color}"></i>${text} mm/h</span>`).join("")}</div><p class="metric-caption">Grigio: dato mancante. Colori rielaborati da DPC · CC BY-SA 4.0. Inquadratura sul centro abitato pubblico. Un file mancante resta non disponibile.</p><a href="https://radar.protezionecivile.gov.it/" target="_blank" rel="noopener">Apri la piattaforma ufficiale DPC</a></article>`;
  }
  async function api(action, params) {
    const response = await fetch("/api/v5/tools/" + action + "?" + new URLSearchParams(params), { cache: "no-store" });
    const value = await response.json(); if (!response.ok) throw Error(value.error || "Fonte non disponibile"); return value;
  }
  function bindReplay() {
    if (!$("#load-history-radar")) return;
    const frame = () => replay.frames[Number($("#event-frame").value)];
    $("#load-history-radar").onclick = () => {
      const at = frame(), [lat, lon] = data.station.role === "primary" ? [41.90, 12.50] : [44.69, 12.18];
      const z = 7, x = Math.floor((lon + 180) / 360 * 2 ** z), y = Math.floor((1 - Math.asinh(Math.tan(lat * Math.PI / 180)) / Math.PI) / 2 * 2 ** z);
      $("#event-radar").innerHTML = `<img alt="" referrerpolicy="origin" src="https://tile.openstreetmap.org/${z}/${x}/${y}.png"><img id="event-radar-overlay" alt="Radar storico DPC"><a class="map-attribution" href="https://www.openstreetmap.org/copyright" target="_blank" rel="noopener">© OpenStreetMap contributors · DPC</a>`;
      const overlay = $("#event-radar-overlay");
      $("#event-radar-status").textContent = "Caricamento osservazione " + dateTime(at) + "…";
      overlay.onload = () => { if (!overlay.isConnected) return; $("#event-radar-status").textContent = "Osservazione DPC del " + dateTime(at); };
      overlay.onerror = () => { if (!overlay.isConnected) return; overlay.remove(); $("#event-radar-status").textContent = "Fotogramma non disponibile. La mappa di base non indica assenza di pioggia."; };
      overlay.src = "/api/v5/tools/dpc-frame?" + new URLSearchParams({ station: activeId, at });
    };
    $("#download-dpc").onclick = async () => { try { const value = await api("dpc-download", { at: frame(), product: "SRI" }); const link = document.createElement("a"); link.href = value.url; link.rel = "noopener"; link.target = "_blank"; link.click(); } catch (error) { notice(error.message); } };
  }
  function bind(p) {
    if (p === "models") {
      $("#model-variable").onchange = e => { modelVariable = e.target.value; render(); };
      $("#model-horizon").onchange = e => { modelHorizon = e.target.value; render(); };
    }
    if (p === "archive") {
      $("#archive-month").onchange = e => { selectedMonth = e.target.value; render(); };
      $("#archive-csv").onclick = () => { const rows = insights().archive?.months || [], columns = ["month", "expected_days", "complete_days", "imported_days", "partial_days", "coverage_percent", "rain_days_available", "rain_sum_mm", "rainy_days", "longest_dry_spell_days", "temp_min_c", "temp_max_c", "temp_mean_c", "is_partial"]; download("meteo-rapporti-mensili.csv", "\ufeff" + columns.join(",") + "\n" + rows.map(r => columns.map(c => JSON.stringify(r[c] ?? "")).join(",")).join("\n"), "text/csv"); };
    }
    if (p === "events") {
      $("#event-form").onsubmit = async e => {
        e.preventDefault(); const station = activeId, values = { station, start: $("#event-start").value, end: $("#event-end").value }, key = JSON.stringify(values);
        $("#event-status").textContent = "Ricostruzione…";
        try { const result = replayCache.get(key) || await api("event", values); replayCache.set(key, result); if (replayCache.size > 12) replayCache.delete(replayCache.keys().next().value); replay = result; if (page === "events" && activeId === station) { $("#event-result").innerHTML = replayMarkup(); $("#event-status").textContent = "Evento ricostruito"; bindReplay(); } } catch (error) { notice(error.message); if ($("#event-status")) $("#event-status").textContent = "Intervallo non disponibile"; }
      }; bindReplay();
    }
  }
  window.MeteoInsights = { observe, homeFocus, qualityView, modelsView, archiveView, eventView, bind,
    view: p => { switch(p) { case "quality": return qualityView(); case "models": return modelsView(); case "archive": return archiveView(); case "events": return eventView(); default: return undefined; } },
    forget: () => { visits.clear(); revisions.clear(); for (const key of Object.keys(localStorage)) if (key.startsWith("meteo.v5.last-visit.")) localStorage.removeItem(key); }
  };
})();
