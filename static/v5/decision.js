/* Observed radar, issued forecasts and the reasons behind a planning hour. */
"use strict";
(() => {
  const hour = 3600000, minute = 60000;
  const rowAt = (rows, at) => (rows || []).find(r => Date.parse(r.start) <= at && at < Date.parse(r.end));
  const stamp = (at, zone) => MeteoSky.dateTime(at, zone);
  const range = (a, b, zone) => MeteoSky.clock(a, zone) + "–" + MeteoSky.clock(b, zone);
  const value = (v, unit, digits = 0) => finite(v) ? num(v, digits) + unit : "—";
  const layers = { clouds: "nuvole totali", cloud_low: "nuvole basse", cloud_mid: "nuvole medie", cloud_high: "nuvole alte" };
  const table = (headers, rows) => `<div class="table-wrap"><table><thead><tr>${headers.map(h => `<th>${esc(h)}</th>`).join("")}</tr></thead>
    <tbody>${rows.map(r => `<tr>${r.map(c => `<td>${c}</td>`).join("")}</tr>`).join("")}</tbody></table></div>`;

  function quick(check, zone, remote = false) {
    if (!check) return "";
    const now = Date.now(), radar = check.radar, next = (check.upcoming || []).filter(r => Date.parse(r.end) > now);
    const age = now - Date.parse(radar?.at), hasRadar = Number.isFinite(age) && age >= 0;
    const radarLabel = hasRadar ? stamp(radar.at, zone) + (age > 20 * minute ? " · datato" : "") : "Non disponibile";
    const forecastLabel = next[0]?.issued_at ? "Emissione " + stamp(next[0].issued_at, zone)
      : next.length && check.snapshot_at ? "Dati pubblicati " + stamp(check.snapshot_at, zone)
      : next.length ? "Orario di emissione non disponibile" : "Aggiorna per ricontrollare";
    return `<article class="card decision-card" aria-label="Ultimo controllo">
      <div class="card-header"><h2>Ultimo controllo</h2><span class="tag">${offline ? "COPIA SALVATA" : "METEO LOCALE"}</span></div>
      <dl class="decision-pair"><div><dt>Radar osservato</dt><dd>${esc(radarLabel)}</dd><small>${esc(hasRadar ? radar.source : remote ? "Nessuna misura radar associata a questa postazione" : "Nessun fotogramma con orario verificabile")}</small></div>
      <div><dt>Prossime ore · previsione</dt><dd>${next.length ? esc(range(next[0].start, next.at(-1).end, zone)) : "Non disponibile"}</dd><small>${esc(forecastLabel)}</small></div></dl>
      <details class="detail-row"><summary>Nuvole, pioggia e raffiche</summary>
        ${check.observed_at ? '<p>Ultima misura della stazione: ' + esc(stamp(check.observed_at, zone)) + '.</p>' : ""}
        ${hasRadar && finite(radar.rate_mm_h) ? '<p>Stima del radar sul punto a quell’ora: ' + value(radar.rate_mm_h, " mm/h", 1) + '.</p>' : ""}
        ${next.length ? table(["Ora locale", "Nuvole", "Pioggia", "Raffiche"], next.map(r => [
          esc(range(r.start, r.end, zone)), value(r.clouds, "%"), value(r.rain_mm, " mm", 2), value(r.wind_gust_kmh, " km/h"),
        ])) : ""}
        ${remote ? "" : '<div class="controls"><button type="button" class="quiet" data-decision-map="radar">Vedi radar →</button><button type="button" class="quiet" data-decision-map="satellite">Vedi satellite e orario →</button></div>'}
      </details></article>`;
  }

  function slots(report, target) {
    const t = report.timeline, end = Date.parse(t.end);
    const start = Math.max(Date.parse(t.start), Date.parse(report.as_of));
    const windows = target?.windows || t.weather_windows || [];
    const checks = t.weather_checks || [];
    const result = [];
    for (let at = Math.floor(start / hour) * hour; at < end && result.length < 26; at += hour) {
      const a = Math.max(at, start), b = Math.min(at + hour, end);
      if (b <= a) continue;
      const overlap = windows.reduce((sum, w) => sum + Math.max(0, Math.min(b, Date.parse(w.end)) - Math.max(a, Date.parse(w.start))), 0);
      const known = checks.some(r => Date.parse(r.start) < b && Date.parse(r.end) > a && !r.reasons.includes("Dati meteo incompleti"));
      const dark = t.darkness && Date.parse(t.darkness[0]) < b && Date.parse(t.darkness[1]) > a;
      const tone = overlap >= b - a ? "good" : overlap > 0 ? "mixed" : !dark ? "day" : known ? "bad" : "unknown";
      result.push({ start: a, end: b, at: Math.floor((a + b) / 2), tone });
    }
    return result;
  }

  const labels = { good: "Utile", mixed: "Parziale", bad: "Esclusa", unknown: "Dati mancanti", day: "Fuori buio" };
  function hourStrip(report, target) {
    if (!report.timeline) return "";
    const zone = report.site.timezone, hours = slots(report, target);
    const spreads = report.timeline.cloud_disagreement || [];
    const best = target?.best_window || report.nights.find(n => n.date === report.selected)?.best_window;
    const near = best && spreads.filter(r => Date.parse(r.valid_time) < Date.parse(best.end) && Date.parse(r.valid_time) + hour > Date.parse(best.start));
    const spread = [...(near?.length ? near : spreads)].sort((a, b) => b.spread - a.spread)[0];
    if (!hours.length) return "";
    return `<section class="card decision-hours" aria-labelledby="cielo-hour-title"><div class="card-header"><h2 id="cielo-hour-title">La notte ora per ora</h2><span class="tag">${esc(target?.id || "METEO")}</span></div>
      <div class="decision-hour-strip">${hours.map(r => `<button type="button" class="decision-hour ${r.tone}" data-night-hour="${r.at}" data-from="${r.start}" data-to="${r.end}" aria-pressed="false" aria-label="${esc(range(r.start, r.end, zone) + ' · ' + labels[r.tone])}"><b>${esc(MeteoSky.clock(r.start, zone))}</b><small>${labels[r.tone]}</small></button>`).join("")}</div>
      <p id="cielo-hour-info" class="metric-caption" aria-live="polite"></p>
      ${spread?.spread >= 25 ? `<details class="detail-row"><summary>Modelli discordi sulle ${esc(layers[spread.field])} · ${esc(MeteoSky.clock(spread.valid_time, zone))}</summary><p>${value(spread.low, "%")} (${esc(spread.low_model)}) · ${value(spread.high, "%")} (${esc(spread.high_model)}). Confronto della stessa ora e dello stesso strato di nuvole.</p></details>` : ""}
    </section>`;
  }

  function cursor(report, target, at, interpolate) {
    const out = $("#cielo-hour-info");
    if (!out) return;
    document.querySelectorAll("[data-night-hour]").forEach(button => {
      button.setAttribute("aria-pressed", String(Number(button.dataset.from) <= at && at < Number(button.dataset.to)));
    });
    const t = report.timeline, row = rowAt(t.weather, at), check = rowAt(t.weather_checks, at), reasons = [];
    if (!t.darkness || at < Date.parse(t.darkness[0]) || at >= Date.parse(t.darkness[1])) reasons.push("fuori dal buio del profilo");
    else {
      reasons.push(...(check?.reasons || ["dati meteo incompleti"]));
      if (target && interpolate(target.altitude, at) < interpolate(target.minimum_altitude, at)) reasons.push("soggetto sotto l’altezza minima o dietro un ostacolo");
      if (target && interpolate(t.moon_altitude, at) > 0 && interpolate(target.moon_separation, at) < report.constraints.moon) reasons.push("Luna troppo vicina");
    }
    const usable = rowAt(target?.windows || t.weather_windows, at);
    const conditions = row ? ` · Nuvole basse / medie / alte: ${["low", "mid", "high"].map(k => value(row["cloud_" + k], "%")).join(" / ")} · Raffiche ${value(row.wind_gust_kmh, " km/h")}` : "";
    out.textContent = MeteoSky.clock(at, report.site.timezone) + " · " + (usable ? "Dentro la finestra utile" : reasons.join(" · ") || "Fuori dalla finestra continua") + conditions;
  }

  function bind() {
    document.querySelectorAll("[data-decision-map]").forEach(button => {
      button.onclick = () => window.MeteoRadar.open(button.dataset.decisionMap);
    });
  }
  window.MeteoDecision = { quick, hourStrip, cursor, bind, table };
})();
