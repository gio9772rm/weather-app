/* Small shared controls for the observing console and the guided planner. */
"use strict";
(() => {
  const prefKey = "meteo.v5.observing";
  let placesOpen = false;
  const settings = () => ({ site: null, sites: [], targets: [], horizons: {}, projects: [], ...storage.get(prefKey, {}) });
  const update = value => storage.set(prefKey, { ...settings(), ...value });
  const placeKey = site => !site ? "" : site.kind === "city" ? "city:" + site.geo_id :
    [site.latitude, site.longitude, site.timezone, site.elevation_m || 0].join(":");
  const format = (value, zone, options) => value == null ? "—" :
    new Intl.DateTimeFormat("it-IT", { ...options, timeZone: zone }).format(new Date(value));
  const clock = (value, zone) => format(value, zone, { hour: "2-digit", minute: "2-digit", hourCycle: "h23" });
  const dateTime = (value, zone) => format(value, zone, { weekday: "short", day: "numeric", month: "short", hour: "2-digit", minute: "2-digit", hourCycle: "h23" });

  async function lookup(path, query) {
    const response = await fetch("/api/v5/tools/" + path + "?q=" + encodeURIComponent(query), { cache: "no-store" });
    const value = await response.json();
    if (!response.ok) throw Error(value.error || "Ricerca non disponibile");
    return value;
  }

  function lookupMarkup(id) {
    return `<div id="${esc(id)}" class="sky-lookup">
      <label>Cerca un soggetto<input type="search" maxlength="80" autocomplete="off" placeholder="Es. NGC 2070, M42, Tarantola"></label>
      <button type="button" class="quiet" data-lookup> Cerca </button>
      <div class="sky-search-results" data-results aria-live="polite"></div></div>`;
  }

  function bindLookup(id, pick) {
    const root = document.getElementById(id);
    if (!root) return;
    const input = root.querySelector("input"), output = root.querySelector("[data-results]");
    let generation = 0;
    async function search() {
      const query = input.value.trim(), token = ++generation;
      if (query.length < 2) {
        output.textContent = "Scrivi almeno due caratteri.";
        return;
      }
      output.textContent = "Cerco nel catalogo…";
      try {
        const { targets } = await lookup("targets", query);
        if (token !== generation || !root.isConnected) return;
        output.innerHTML = targets.length ? targets.map((target, index) =>
          `<button type="button" class="sky-match" data-match="${index}"><b>${esc(target.name)}</b><span>${esc(target.common_name === target.name ? target.category : target.common_name)}</span></button>`
        ).join("") : "<p>Nessun risultato. Prova il nome comune o la sigla del catalogo.</p>";
        output.querySelectorAll("[data-match]").forEach(button => {
          button.onclick = () => {
            const target = targets[Number(button.dataset.match)];
            if (pick(target) === false) {
              output.textContent = "Puoi scegliere fino a otto soggetti. Deselezionane uno per aggiungerne un altro.";
              return;
            }
            input.value = target.name;
            output.textContent = target.name + " selezionato.";
          };
        });
      } catch {
        if (token === generation && root.isConnected) output.textContent = "Ricerca non disponibile. Riprova tra poco.";
      }
    }
    root.querySelector("[data-lookup]").onclick = search;
    input.onkeydown = event => { if (event.key === "Enter") { event.preventDefault(); search(); } };
  }

  function placesMarkup() {
    const { site, sites } = settings();
    const saved = sites.find(place => placeKey(place) === placeKey(site));
    return `<details class="card detail-row" id="cielo-places"${placesOpen ? " open" : ""}>
      <summary>Cambia postazione o apri un preferito</summary>
      <div class="sky-place-actions"><button type="button" class="quiet" id="sky-local"${site ? "" : " disabled"}>Usa la stazione meteo</button>
        <label>Postazioni salvate<select id="sky-favorites"><option value="">Scegli…</option>${sites.map((place, index) => `<option value="${index}">${esc(place.label)}</option>`).join("")}</select></label>
        ${site ? `<button type="button" class="quiet" id="sky-save-place">${saved ? "Rimuovi dai preferiti" : "Salva nei preferiti"}</button>` : ""}
      </div>
      <form id="sky-city" class="sky-lookup"><label>Cerca una località<input type="search" id="sky-city-name" maxlength="80" placeholder="Es. San Pedro de Atacama" required></label><button class="quiet">Cerca</button>
        <div class="sky-search-results" id="sky-city-results" aria-live="polite"></div></form>
      <details><summary>Inserisci coordinate e fuso della postazione</summary>
        <form id="sky-point" class="form-grid">
          <label>Nome<input id="sky-point-label" type="text" maxlength="100" placeholder="Es. Osservatorio" required></label>
          <label>Latitudine °<input id="sky-point-lat" type="number" min="-90" max="90" step="any" required></label>
          <label>Longitudine °<input id="sky-point-lon" type="number" min="-180" max="180" step="any" required></label>
          <label>Quota m<input id="sky-point-height" type="number" min="-500" max="9000" value="0" step="any" required></label>
          <label>Fuso IANA<input id="sky-point-zone" type="text" maxlength="64" value="${esc(Intl.DateTimeFormat().resolvedOptions().timeZone)}" placeholder="America/Santiago" required></label>
          <button class="quiet">Usa queste coordinate</button>
          <p id="sky-point-status" role="status"></p>
        </form></details>
      <p class="metric-caption">Per le altre postazioni Cielo usa modelli globali. Il meteo della stazione in home resta separato. Le coordinate inserite vengono usate per calcolare cielo e previsione.</p>
    </details>`;
  }

  function bindPlaces(change) {
    const root = $("#cielo-places");
    if (!root) return;
    root.ontoggle = () => { placesOpen = root.open; };
    const choose = site => { placesOpen = false; update({ site }); change(); };
    $("#sky-local").onclick = () => choose(null);
    $("#sky-favorites").onchange = event => {
      if (event.target.value !== "") choose(settings().sites[Number(event.target.value)]);
    };
    const save = $("#sky-save-place");
    if (save) save.onclick = () => {
      const { site, sites } = settings(), existing = sites.some(place => placeKey(place) === placeKey(site));
      if (!existing && sites.length >= 12) { notice("Puoi salvare al massimo dodici postazioni."); return; }
      update({ sites: existing ? sites.filter(place => placeKey(place) !== placeKey(site)) : [...sites, site] });
      placesOpen = true; change();
    };
    $("#sky-city").onsubmit = async event => {
      event.preventDefault();
      const query = $("#sky-city-name").value.trim(), output = $("#sky-city-results");
      if (query.length < 2) { output.textContent = "Scrivi almeno due caratteri."; return; }
      output.textContent = "Cerco la località…";
      try {
        const { results } = await lookup("cities", query);
        if (!output.isConnected) return;
        const cities = results.filter(city => Number.isSafeInteger(city.geo_id));
        output.innerHTML = cities.length ? cities.map((city, index) =>
          `<button type="button" class="sky-match" data-place="${index}"><b>${esc(city.label)}</b><span>${esc(city.timezone)}</span></button>`
        ).join("") : "<p>Nessuna località corrispondente.</p>";
        output.querySelectorAll("[data-place]").forEach(button => button.onclick = () => {
          const city = cities[Number(button.dataset.place)];
          choose({ kind: "city", label: city.label, geo_id: city.geo_id });
        });
      } catch { if (output.isConnected) output.textContent = "Ricerca non disponibile."; }
    };
    $("#sky-point").onsubmit = event => {
      event.preventDefault();
      const timezone = $("#sky-point-zone").value.trim();
      const latitude = Number(Number($("#sky-point-lat").value).toFixed(6));
      if (Math.abs(latitude) >= 90) { $("#sky-point-status").textContent = "Usa una latitudine fra −90° e 90°, esclusi i poli."; return; }
      try { new Intl.DateTimeFormat("it-IT", { timeZone: timezone }); }
      catch { $("#sky-point-status").textContent = "Verifica il fuso, per esempio America/Santiago."; return; }
      choose({
        kind: "point", label: $("#sky-point-label").value.trim(), timezone,
        latitude, longitude: Number(Number($("#sky-point-lon").value).toFixed(6)),
        elevation_m: Number(Number($("#sky-point-height").value).toFixed(1)),
      });
    };
  }

  window.MeteoSky = { settings, update, placeKey, lookupMarkup, bindLookup, placesMarkup, bindPlaces, clock, dateTime, format };
})();
