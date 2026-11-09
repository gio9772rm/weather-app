/* Exposure planning inputs and a compact night-by-night budget. */
"use strict";
(() => {
  const fields = [
    ["readout_seconds", "Scaricamento per posa · s", 2, 120],
    ["filter_change_seconds", "Cambio filtro · s", 5, 120],
    ["focus_seconds", "Durata autofocus · s", 90, 900],
    ["focus_every_minutes", "Ripeti autofocus dopo · min", 60, 480],
    ["dither_seconds", "Dither e assestamento · s", 15, 300],
    ["dither_every_frames", "Dither ogni N pose", 3, 100],
    ["flip_seconds", "Riserva flip al meridiano · s", 420, 1800],
  ];
  const duration = seconds => {
    const minutes = Math.floor(seconds / 60);
    return minutes >= 60 ? Math.floor(minutes / 60) + " h " + minutes % 60 + " min" :
      minutes ? minutes + " min" : Math.round(seconds) + " s";
  };
  const frameCount = filters => filters.reduce((sum, row) => sum + row.count, 0);
  function controls(config) {
    const recipe = config.budget?.filters || [{ name: "OSC", seconds: 180, frames: 1 }];
    return `<details class="detail-row" id="budget-controls"><summary>Budget di ripresa · filtri e tempi tecnici</summary>
      <label class="budget-toggle"><input id="budget-enabled" type="checkbox" ${config.budget === null ? "" : "checked"}> Stima pose complete e integrazione per filtro</label>
      <label>Ricetta · filtro; secondi; pose per giro<textarea id="budget-recipe" rows="3" maxlength="700" spellcheck="false">${esc(recipe.map(r => [r.name, r.seconds, r.frames].join("; ")).join("\n"))}</textarea></label>
      <p class="metric-caption">Una riga per filtro. Per una camera a colori: <code>OSC; 180; 1</code>. Per più filtri il giro si ripete. Verifica i valori con il tuo setup.</p>
      <div class="form-grid">${fields.map(([key, label, defaultValue, max]) =>
        `<label>${label}<input id="budget-${key}" type="number" min="0" max="${max}" value="${esc(config.budget?.[key] ?? defaultValue)}" required></label>`
      ).join("")}</div>
      <p class="metric-caption">Usa 0 per escludere una durata. Un intervallo autofocus di 0 mantiene solo il fuoco iniziale; dither ogni 0 pose lo esclude. Il flip è stimato al passaggio geometrico al meridiano.</p></details>`;
  }
  function read(profile) {
    if (profile !== "deep_sky" || !$("#budget-enabled")?.checked) return null;
    const filters = $("#budget-recipe").value.split("\n").filter(line => line.trim()).map(line => {
      const parts = line.split(";").map(x => x.trim());
      const [name, duration, count] = parts, seconds = Number(duration?.replace(",", ".")), frames = Number(count);
      if (parts.length !== 3 || !name || name.length > 24 || !finite(seconds) || seconds < 1 || seconds > 3600 || !Number.isInteger(frames) || frames < 1 || frames > 50) {
        $("#budget-controls").open = true;
        throw Error("Budget: usa una riga per filtro, per esempio OSC; 180; 1.");
      }
      return { name, seconds, frames };
    });
    if (!filters.length || filters.length > 8 || new Set(filters.map(f => f.name.toLowerCase())).size !== filters.length) {
      $("#budget-controls").open = true;
      throw Error("Inserisci da uno a otto filtri con nomi distinti.");
    }
    return { filters, ...Object.fromEntries(fields.map(([key]) => [key, Number($("#budget-" + key).value)])) };
  }
  const overheadNames = { readout: "Scaricamento", filter: "Cambi filtro", focus: "Autofocus", dither: "Dither", flip: "Flip" };
  const overheadList = row => Object.entries(row.overhead).filter(([, seconds]) => seconds > 0).map(([key, seconds]) => overheadNames[key] + " " + duration(seconds)).join(" · ");
  function result(plan) {
    if (!plan?.budget) return "";
    const budget = plan.budget;
    return `<article class="card budget-result"><span class="eyebrow">INTEGRAZIONE STIMATA</span>
      <h2>${duration(budget.integration_seconds)} in ${plan.nights.length} ${plan.nights.length === 1 ? "notte" : "notti"}</h2>
      <p>${frameCount(budget.filters)} pose complete stimate · ${esc(plan.site.name)} · ${esc(plan.timezone)}</p>
      <div class="budget-filters">${budget.filters.map(f => `<span><b>${esc(f.name)}</b> ${f.count} × ${num(f.seconds, 1)} s · ${duration(f.integration_seconds)}</span>`).join("")}</div>
      <p class="metric-caption">Stima entro i blocchi meteo e geometria. Preparazione e cambio soggetto sono già fuori dai blocchi.</p>
      <details class="detail-row"><summary>Bilancio per notte e tempi tecnici</summary>
        <div class="table-wrap"><table><thead><tr><th>Notte · ora locale</th><th>Pose per filtro</th><th>Integrazione</th><th>Tempi tecnici</th></tr></thead><tbody>
          ${plan.nights.map(n => `<tr><td>${esc(MeteoSky.dateTime(n.start, plan.timezone))}</td><td>${n.budget.filters.map(f => esc(f.name) + ": " + f.count).join(" · ")}</td><td>${duration(n.budget.integration_seconds)}</td><td>${overheadList(n.budget) || "—"}</td></tr>`).join("")}
        </tbody></table></div><p class="metric-caption">Preparazione e cambi soggetto: ${duration(budget.preparation_seconds)}. Tempo residuo troppo breve per una posa completa: ${duration(budget.idle_seconds)}.</p>
      </details><button id="budget-download" type="button" class="quiet">Scarica budget per blocco · CSV</button></article>`;
  }
  function bind(plan) {
    const button = $("#budget-download");
    if (!button) return;
    const field = value => {
      const string = String(value ?? "");
      return '"' + (/^[=+@\-\t\r]/.test(string) ? "'" : "") + string.replaceAll('"', '""') + '"';
    };
    button.onclick = () => {
      const rows = [["Notte", "Fuso", "Soggetto", "Inizio UTC", "Fine UTC", "Filtro", "Pose", "Secondi per posa", "Integrazione secondi"]];
      for (const night of plan.nights) for (const block of night.budget.blocks) for (const f of block.filters) {
        rows.push([MeteoSky.dateTime(night.start, plan.timezone), plan.timezone, block.target, block.start, block.end, f.name, f.count, f.seconds, f.integration_seconds]);
      }
      const url = URL.createObjectURL(new Blob([rows.map(row => row.map(field).join(",")).join("\r\n")], { type: "text/csv;charset=utf-8" }));
      const link = document.createElement("a"); link.href = url; link.download = "meteo-budget-ripresa.csv"; link.click(); URL.revokeObjectURL(url);
    };
  }
  window.MeteoBudget = { controls, read, result, bind };
})();
