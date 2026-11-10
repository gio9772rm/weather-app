/* User-entered integration goals and forecast contributions for a target. */
"use strict";
(() => {
  const list = () => MeteoSky.settings().projects;
  const find = id => list().find(project => project.target === id);
  const hours = seconds => num(seconds / 3600, 1) + " h";
  const missing = project => project.filters.reduce((sum, f) => sum + Math.max(0, f.desired_seconds - f.acquired_seconds), 0);
  function markup(report, target) {
    if (!target || report.profile !== "deep_sky") return "";
    const raw = find(target.id), project = raw && MeteoIntegration.totals(raw);
    const outlook = project && report.project?.target === target.id ? report.project : null;
    const best = outlook?.best, zone = report.site.timezone;
    return `<section class="cielo-project" aria-labelledby="cielo-project-title">
      <div class="card-header"><h3 id="cielo-project-title">Obiettivi per filtro · ${esc(target.id)}</h3><button type="button" id="project-edit" class="quiet">${project ? "Modifica" : "Imposta obiettivi"}</button></div>
      ${!project ? '<p class="metric-caption">Indica le ore che vuoi raggiungere e quelle già raccolte per confrontare le prossime tre notti.</p>' :
        `<div class="project-progress">${project.filters.map(f => `<div><b>${esc(f.name)}</b><progress aria-label="Integrazione ${esc(f.name)}" max="${f.desired_seconds}" value="${Math.min(f.acquired_seconds, f.desired_seconds)}"></progress><span>${hours(f.acquired_seconds)} / ${hours(f.desired_seconds)}</span></div>`).join("")}</div>
        <p>${missing(project) ? "Mancano " + hours(missing(project)) : "Obiettivi raggiunti"}</p>
        <div class="controls"><button id="project-import" type="button" class="quiet">Chiudi serata</button>
        ${missing(project) ? '<button id="project-compare" type="button" class="quiet">Confronta le tre notti</button><button id="project-compare-sites" type="button" class="quiet">Confronta due postazioni</button>' : ""}</div>`}
      ${best ? `<div class="project-best"><b>Notte consigliata · ${esc(MeteoSky.dateTime(best.window.start, zone))}</b>
        <p>${hours(best.useful_seconds)} stimate verso gli obiettivi · ${best.filters.map(f => esc(f.name) + " " + hours(f.integration_seconds)).join(" · ")}</p>
        <button id="project-use-window" type="button" class="primary">Usa la notte consigliata →</button>
        <details class="detail-row"><summary>Confronto con le altre notti</summary><ul>${outlook.nights.map(n => `<li>${esc(n.date)} · ${n.useful_seconds ? hours(n.useful_seconds) + " stimate utili" : "Nessuna finestra utile confermata"}</li>`).join("")}</ul></details></div>`
        : outlook && missing(project) ? '<p class="metric-caption">Nessuna finestra utile confermata per il soggetto e la ricetta nelle tre notti.</p>' : ""}
      </section>`;
  }
  function edit(target, recipe, changed) {
    $("#project-dialog")?.remove();
    const dialog = document.createElement("dialog"), old = find(target.id);
    dialog.id = "project-dialog";
    const row = f => `<fieldset data-project-filter class="form-grid"><legend>Filtro</legend>
      <label>Nome<input data-name maxlength="24" value="${esc(f.name || "")}" ${old?.imports?.some(entry => entry.filters.some(part => part.name === f.name)) ? "readonly" : ""} required></label>
      <label>Obiettivo · ore<input data-desired type="number" min=".01" max="1000" step=".01" value="${f.desired_seconds ? +(f.desired_seconds / 3600).toFixed(2) : ""}" required></label>
      <label>Raccolte a mano · ore<input data-acquired type="number" min="0" max="1000" step=".01" value="${+((f.acquired_seconds || 0) / 3600).toFixed(2)}" required></label>
      <label>Posa · secondi<input data-exposure type="number" min="1" max="3600" step=".1" value="${f.exposure_seconds || 180}" required></label>
      <button type="button" data-remove class="quiet">Togli filtro</button></fieldset>`;
    const initial = old?.filters || (recipe?.filters || [{ name: "OSC", seconds: 180 }]).map(f => ({ name: f.name, exposure_seconds: f.seconds }));
    dialog.innerHTML = `<form><div class="dialog-title"><h2>Obiettivi · ${esc(target.id)}</h2><button id="project-close" type="button" class="quiet" aria-label="Chiudi">×</button></div>
      <p>Inserisci le ore manuali. I totali caricati con «Chiudi serata» si aggiungono qui solo nel riepilogo.</p>
      <div id="project-filter-rows">${initial.map(row).join("")}</div>
      <div class="controls"><button type="button" id="project-add" class="quiet">Aggiungi filtro</button><button class="primary">Salva obiettivi</button>
      ${old ? '<button type="button" id="project-remove" class="quiet">Rimuovi obiettivi</button>' : ""}</div><p id="project-status" role="status"></p></form>`;
    document.body.append(dialog); dialog.showModal();
    dialog.onclose = () => dialog.remove();
    const rows = $("#project-filter-rows");
    $("#project-close").onclick = () => dialog.close();
    $("#project-add").onclick = () => { if (rows.children.length < 8) rows.insertAdjacentHTML("beforeend", row({})); };
    rows.onclick = event => {
      if (event.target.matches("[data-remove]") && rows.children.length > 1) event.target.closest("fieldset").remove();
    };
    const commit = project => {
      const other = list().filter(item => item.target !== target.id);
      if (project && other.length >= 30) { $("#project-status").textContent = "Puoi salvare fino a trenta progetti."; return; }
      MeteoSky.update({ projects: project ? [...other, project] : other }); dialog.close(); changed();
    };
    const remove = $("#project-remove");
    if (remove) remove.onclick = () => commit(null);
    dialog.querySelector("form").onsubmit = event => {
      event.preventDefault();
      const filters = [...rows.children].map(node => ({
        name: node.querySelector("[data-name]").value.trim().replace(/\s+/g, " "),
        desired_seconds: Math.round(Number(node.querySelector("[data-desired]").value) * 3600),
        acquired_seconds: Math.round(Number(node.querySelector("[data-acquired]").value) * 3600),
        exposure_seconds: Number(node.querySelector("[data-exposure]").value),
      }));
      if (filters.some(f => !f.name || f.name.includes(";")) || new Set(filters.map(f => f.name.toLowerCase())).size !== filters.length) {
        $("#project-status").textContent = "Usa nomi distinti per i filtri, senza punto e virgola."; return;
      }
      const updated = { target: target.id, filters, ...(old?.imports ? { imports: old.imports } : {}) };
      if (MeteoIntegration.totals(updated).filters.some(f => f.acquired_seconds > 3600000)) {
        $("#project-status").textContent = "Ore manuali e importate superano il limite."; return;
      }
      commit(updated);
    };
  }
  window.MeteoProjects = { list, find, markup, edit };
})();
