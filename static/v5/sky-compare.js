/* Compare one remaining filter at two sites, without changing the active site. */
"use strict";
(() => {
  function places(site, stationId) {
    const result = stations.map(s => ({ stationId: s.id, site: null, name: s.name }));
    const globals = new Map(MeteoSky.settings().sites.map(s => [MeteoSky.placeKey(s), s]));
    if (site) globals.set(MeteoSky.placeKey(site), site);
    for (const s of globals.values()) result.push({ stationId, site: s, name: s.label });
    return result;
  }

  function inputs(place, target, filter, setup, profile) {
    const { site, stationId } = place, { saved, equipment, constraints } = setup;
    const matching = site ? MeteoSky.placeKey(site) === MeteoSky.placeKey(saved.site) : !saved.site && saved.station_id === stationId;
    const optics = ["aperture_mm", "focal_length_mm", "sensor_width_mm", "sensor_height_mm", "pixel_size_um", "focal_multiplier"];
    return {
      ...constraints, station_id: stationId, site, profile, compare_only: true,
      horizon: MeteoHorizon.saved(site, stationId)?.mask ?? (matching ? saved.horizon || {} : {}),
      equipment: equipment ? {name:"Strumento", camera:"Camera", telescope:"Ottica",
        ...Object.fromEntries(optics.map(k => [k, equipment[k]]))} : null,
      project: {target: target.id, filters: [filter]}, target: target.id,
      budget: saved.budget, setup_minutes: saved.setup_minutes, minimum_minutes: saved.minimum_minutes,
    };
  }

  async function get(body, signal) {
    const response = await fetch("/api/v5/tools/cielo", {
      method: "POST", headers: {"Content-Type":"application/json"}, body: JSON.stringify(body), signal,
    });
    if (!response.ok) throw Error("Previsione non disponibile");
    const report = await response.json();
    if (report.schema !== 1 || report.project?.target !== body.target) throw Error("Confronto non disponibile");
    return report;
  }

  function resultCard(place, outcome) {
    if (outcome.status !== "fulfilled") return `<article class="decision-result"><h3>${esc(place.name)}</h3><p>Dati non disponibili per il confronto.</p></article>`;
    const {site, project} = outcome.value, best = project.best;
    const zone = site.timezone, relevant = project.nights.find(n => n.date === best?.date) || project.nights[0];
    return `<article class="decision-result"><h3>${esc(site.name)}</h3><p class="metric-caption">${esc(zone)}</p>
      ${best ? `<p><b>${num(best.useful_seconds / 3600, 2)} h</b> stimate verso l’obiettivo</p><p>${esc(MeteoSky.dateTime(best.window.start, zone))}–${esc(MeteoSky.clock(best.window.end, zone))}</p>` : "<p>Nessuna finestra utile confermata.</p>"}
      ${relevant?.limiting_factor ? `<p class="metric-caption">Limite nella notte: ${esc(relevant.limiting_factor)}.</p>` : ""}
      <details class="detail-row"><summary>Dettaglio delle tre notti</summary><ul>${project.nights.map(n => `<li>${esc(n.date)} · ${n.useful_seconds ? num(n.useful_seconds / 3600, 2) + " h utili" : esc(n.limiting_factor || "Nessuna finestra")}</li>`).join("")}</ul></details>
    </article>`;
  }

  function open(report, target, project, setup, stationId) {
    $("#site-compare-dialog")?.remove();
    const options = places(setup.site, stationId), pending = project.filters.filter(f => f.acquired_seconds < f.desired_seconds);
    if (!pending.length) return;
    const dialog = document.createElement("dialog");
    dialog.id = "site-compare-dialog";
    const current = options.findIndex(x => setup.site ? MeteoSky.placeKey(x.site) === MeteoSky.placeKey(setup.site) : !x.site && x.stationId === stationId);
    const selection = (index, label, selected) => `<label>${label}<select id="site-compare-${index}">${options.map((place, i) => `<option value="${i}"${selected === i ? " selected" : ""}>${esc(place.name)}</option>`).join("")}</select></label>`;
    dialog.innerHTML = `<form><div class="dialog-title"><h2>Confronta due postazioni · ${esc(target.id)}</h2><button type="button" id="site-compare-close" class="quiet" aria-label="Chiudi">×</button></div>
      <p>Stesso soggetto, filtro e soglie; orizzonte e fuso della singola postazione. Confronto su richiesta delle prossime tre notti locali.</p>
      <div class="form-grid">${selection(0, "Prima postazione", current)}${selection(1, "Seconda postazione", current === 0 ? 1 : 0)}
        <label>Filtro ancora da completare<select id="site-compare-filter">${pending.map((f, i) => `<option value="${i}">${esc(f.name)}</option>`).join("")}</select></label></div>
      <button class="primary" ${options.length < 2 ? "disabled" : ""}>Confronta</button>
      <p id="site-compare-status" role="status">${options.length < 2 ? "Salva un’altra postazione in Cielo." : ""}</p><div id="site-compare-result" class="decision-results"></div></form>`;
    document.body.append(dialog); dialog.showModal();
    const $d = selector => dialog.querySelector(selector);
    let controller;
    dialog.onclose = () => { controller?.abort(); dialog.remove(); };
    $d("#site-compare-close").onclick = () => dialog.close();
    dialog.querySelector("form").onsubmit = async event => {
      event.preventDefault();
      const a = Number($d("#site-compare-0").value), b = Number($d("#site-compare-1").value);
      const info = $d("#site-compare-status"), out = $d("#site-compare-result"), button = $d("button.primary");
      if (a === b) { info.textContent = "Scegli due postazioni diverse."; return; }
      controller?.abort();
      const request = controller = new AbortController(), timeout = setTimeout(() => request.abort(), 35000);
      const filter = pending[Number($d("#site-compare-filter").value)];
      const choices = [options[a], options[b]];
      button.disabled = true; info.textContent = "Confronto in corso…"; out.replaceChildren();
      try {
        const outcomes = await Promise.allSettled(choices.map(place => get(inputs(place, target, filter, setup, report.profile), request.signal)));
        if (!dialog.open || request !== controller) return;
        info.textContent = filter.name + " · " + num((filter.desired_seconds - filter.acquired_seconds) / 3600, 2) + " h mancanti";
        out.innerHTML = outcomes.map((value, i) => resultCard(choices[i], value)).join("");
      } finally { clearTimeout(timeout); if (request === controller) button.disabled = false; }
    };
  }
  window.MeteoSiteCompare = { open };
})();
