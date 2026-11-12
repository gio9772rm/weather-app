/* City weather uses its own selection, forecast and offline copies. */
"use strict";
(() => {
  const DEFAULT = { geo_id: 3169070, name: "Roma", label: "Roma, Lazio, Italia" };
  const FAVORITES = "meteo.v5.cities", CACHE = "meteo.v5.city-weather";
  const HOUR = 3600000;
  let place, report, copy = false, error = "", flight = null, epoch = 0, checked = 0;
  let startId = null, fix = null, byPosition = false, day = "", query = "", matches = [], searchStatus = "";
  let searchEpoch = 0, looking = false, locating = false, migrate = null;

  const validId = id => Number.isInteger(id) && id > 0 && id < 2 ** 31;
  const saved = v => validId(v?.geo_id) && typeof v.name === "string" && v.name &&
    typeof v.label === "string" && v.label;
  const descriptor = ({geo_id, name, label}) => ({geo_id, name, label});
  const favorites = () => {
    const value = storage.get(FAVORITES, []);
    return Array.isArray(value) ? value.filter(v => saved(v) ||
      (typeof v?.label === "string" && typeof v?.query === "string" && v.query.trim().length >= 2)).slice(0, 12) : [];
  };
  const initial = () => saved(prefs.home_city) ? prefs.home_city : DEFAULT;
  const active = () => ["today", "forecast", "cities"].includes(page);
  const mode = () => page === "forecast" ? "forecast" : "today";
  const date = v => new Date(v);
  const stamp = (v, options) => v != null && Number.isFinite(Date.parse(v)) ? new Intl.DateTimeFormat("it-IT",
    {timeZone: report.timezone, ...options}).format(date(v)) : "—";
  const hour = v => stamp(v, {hour: "2-digit", minute: "2-digit"});
  const dayName = v => stamp(v, {weekday: "short", day: "numeric", month: "short"});
  const dateTime = v => dayName(v) + " · " + hour(v);
  const localDay = v => typeof v === "string" ? v.slice(0, 10) : "";
  const unit = (v, suffix, digits = 0) => finite(v) ? num(v, digits) + suffix : "—";
  const compass = v => finite(v) ? ["N","NE","E","SE","S","SO","O","NO"][Math.round(((Number(v) % 360 + 360) % 360) / 45) % 8] : "";
  const canonical = value => typeof value?.timezone === "string" && Number.isFinite(Date.parse(value.fetched_at)) &&
    finite(value.current?.temp_c) && Array.isArray(value.hourly) && value.hourly.length > 0 && Array.isArray(value.daily);
  const readCache = () => {
    const values = storage.get(CACHE, []);
    return Array.isArray(values) ? values.filter(v => saved(v) && canonical(v)).slice(0, 4) : [];
  };
  const getCopy = id => prefs.offline && validId(id) ? readCache().find(c => c.geo_id === id) : null;
  const keepCopy = value => {
    if (prefs.offline && saved(value) && !value.approximate)
      storage.set(CACHE, [value, ...readCache().filter(c => c.geo_id !== value.geo_id)].slice(0, 4));
  };
  function forget() {
    try { localStorage.removeItem(CACHE); } catch {}
    if (copy) report = null;
    copy = false;
  }
  function ensure() {
    if (place) return;
    place = descriptor(initial());
    startId = place.geo_id;
    report = getCopy(place.geo_id);
    copy = !!report;
  }
  function reset(next, point = null) {
    epoch++;
    place = next;
    fix = point;
    byPosition = !!point;
    report = getCopy(next.geo_id);
    copy = !!report;
    error = "";
    flight = null;
    checked = 0;
    day = "";
  }
  function profileChanged(accountChanged) {
    const next = initial();
    if (!place || accountChanged || startId !== next.geo_id) {
      startId = next.geo_id;
      reset(descriptor(next));
      searchEpoch++;
      query = ""; matches = []; searchStatus = ""; looking = false; locating = false; migrate = null;
      if (active()) refresh();
    }
  }
  function startupOptions() {
    ensure();
    const candidates = new Map();
    for (const v of [prefs.home_city, place, ...favorites()]) if (saved(v)) candidates.set(v.geo_id, v);
    return '<option value="">Roma · predefinita</option>' + [...candidates.values()].map(v =>
      '<option value="' + v.geo_id + '"' + (prefs.home_city?.geo_id === v.geo_id ? " selected" : "") +
      ">" + esc(v.label) + "</option>").join("");
  }
  function setStartup(id) {
    ensure();
    const city = [prefs.home_city, place, ...favorites()].find(v => saved(v) && v.geo_id === Number(id));
    prefs.home_city = city ? descriptor(city) : null;
    startId = initial().geo_id;
  }
  function removeFavorite(index) {
    const list = favorites();
    if (Number.isInteger(index) && index >= 0 && index < list.length) {
      storage.set(FAVORITES, list.filter((_, i) => i !== index));
      paint();
    }
  }
  async function request(path, body) {
    const controller = new AbortController(), timer = setTimeout(() => controller.abort(), 32000);
    try {
      const response = await fetch("/api/v5/tools/" + path, {
        cache: "no-store", signal: controller.signal,
        ...(body ? {method: "POST", headers: {"Content-Type": "application/json"}, body: JSON.stringify(body)} : {}),
      });
      if (!response.headers.get("Content-Type")?.includes("application/json")) throw Error();
      const payload = await response.json();
      if (!response.ok) throw Error(payload.error || "Servizio meteo non disponibile");
      return payload;
    } finally { clearTimeout(timer); }
  }
  function refresh(force = false) {
    ensure();
    if (flight) return flight;
    if (!force && !copy && report && Date.now() - checked < REFRESH_MS) return Promise.resolve(report);
    if (!navigator.onLine) {
      copy = !!report;
      error = "Connessione assente.";
      paint();
      return Promise.resolve();
    }
    const id = epoch, want = place.geo_id;
    error = "";
    flight = (async () => {
      try {
        const value = fix ? await request("nearby-city", fix) : await request("city?geo_id=" + want);
        if (id !== epoch) return;
        if (!canonical(value) || (validId(want) && !fix && value.geo_id !== want)) throw Error();
        report = value;
        place = descriptor(value);
        if (saved(value)) fix = null;
        copy = false;
        checked = Date.now();
        keepCopy(value);
      } catch {
        if (id === epoch) {
          error = "La previsione non è ancora disponibile.";
          copy = !!report;
        }
      } finally {
        if (id === epoch) {
          flight = null;
          paint();
          scheduleRecovery();
        }
      }
    })();
    paint();
    return flight;
  }
  function choose(value) {
    if (!saved(value)) return;
    if (migrate) {
      const old = migrate;
      const next = favorites().filter(v => v.label !== old.label || v.query !== old.query)
        .filter(v => v.geo_id !== value.geo_id);
      storage.set(FAVORITES, [...next.slice(0, 11), descriptor(value)]);
    }
    searchEpoch++;
    query = ""; matches = []; searchStatus = ""; looking = false; locating = false; migrate = null;
    reset(descriptor(value));
    restartRecovery();
    if (active()) render();
    refresh();
  }
  async function search(value, old = null) {
    query = value.trim();
    if (query.length < 2 || query.length > 80) return;
    const id = ++searchEpoch;
    migrate = old;
    looking = true; matches = []; searchStatus = "Cerco le città…";
    paintSearch();
    try {
      const response = await request("cities?q=" + encodeURIComponent(query));
      if (id !== searchEpoch) return;
      matches = Array.isArray(response.results) ? response.results.filter(saved).slice(0, 12) : [];
      if (old) {
        const identical = matches.find(v => v.label.toLocaleLowerCase("it") === old.label.toLocaleLowerCase("it"));
        if (identical) { choose(identical); return; }
      }
      searchStatus = matches.length ? "Scegli il risultato corretto." : "Nessuna città trovata. Prova il nome con il Paese o il CAP.";
    } catch {
      if (id === searchEpoch) searchStatus = "Ricerca non disponibile. Riprova tra poco.";
    } finally {
      if (id === searchEpoch) { looking = false; paintSearch(); }
    }
  }
  function locate() {
    if (locating) return;
    if (!navigator.geolocation) {
      searchStatus = "La posizione non è disponibile. Puoi cercare una città.";
      paintSearch();
      return;
    }
    const id = ++searchEpoch;
    locating = true; looking = false; migrate = null; matches = []; searchStatus = "Attendo la posizione del dispositivo…";
    paintSearch();
    navigator.geolocation.getCurrentPosition(({coords}) => {
      if (id !== searchEpoch || !active()) return;
      locating = false; query = ""; searchStatus = "";
      const round = n => Math.round(n * 100) / 100;
      reset({name: "Vicino a te", label: "Posizione approssimata"}, {
        latitude: round(coords.latitude), longitude: round(coords.longitude),
        timezone: Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC",
      });
      restartRecovery();
      render();
      refresh();
    }, () => {
      if (id !== searchEpoch) return;
      locating = false; searchStatus = "Posizione non ottenuta. Cerca la città per nome o CAP."; paintSearch();
    }, {enableHighAccuracy: false, timeout: 10000, maximumAge: 120000});
  }
  function leave() {
    if (locating || looking) {
      searchEpoch++;
      locating = false; looking = false; searchStatus = ""; matches = []; migrate = null;
    }
  }
  function citySearch() {
    return `<div class="city-toolbar"><form id="city-search" role="search">
      <label class="sr-only" for="city-query">Città o CAP</label>
      <span class="city-search-icon" aria-hidden="true">⌕</span><input id="city-query" type="search" name="q" value="${esc(query)}" placeholder="Città o CAP nel mondo" minlength="2" maxlength="80" required autocomplete="off">
      <button class="primary" type="submit">Cerca</button></form>
      <button id="city-locate" type="button" class="quiet"><span aria-hidden="true">⌖</span> Usa la mia posizione</button></div>
      <div id="city-choices"></div><p id="city-search-status" class="city-status" role="status"></p><div id="city-results"></div>`;
  }
  function paintSearch() {
    if (!active() || !$("#city-results")) return;
    $("#city-search-status").textContent = searchStatus;
    $("#city-search button").disabled = looking;
    $("#city-locate").disabled = locating;
    $("#city-results").innerHTML = matches.length ? '<ul class="city-results" aria-label="Risultati di ricerca">' +
      matches.map((item, i) => `<li><button class="city-choice" type="button" data-city-result="${i}"><strong>${esc(item.name)}</strong><small>${esc(item.label === item.name ? item.timezone : item.label)}</small><span aria-hidden="true">→</span></button></li>`).join("") + "</ul>" : "";
    $("#city-results").querySelectorAll("[data-city-result]").forEach(b => b.onclick = () => choose(matches[Number(b.dataset.cityResult)]));
  }
  function paintChoices() {
    if (!active() || !$("#city-choices")) return;
    const list = favorites();
    const quick = [...new Map([initial(), ...list].map(v => [v.geo_id || v.label, v])).values()];
    $("#city-choices").innerHTML = `<nav class="city-favorites" aria-label="Città rapide"><span>SCELTE RAPIDE</span>
      ${quick.map((v, i) => `<button type="button" class="city-chip" data-quick-city="${i}" ${validId(place.geo_id) && place.geo_id === v.geo_id ? 'aria-current="true"' : ""} title="${esc(v.label)}">${v.geo_id === initial().geo_id ? '<span aria-hidden="true">⌂</span> ' : ""}${esc(v.name || v.label)}</button>`).join("")}
      </nav>`;
    $("#city-choices").querySelectorAll("[data-quick-city]").forEach(b => b.onclick = () => {
      const value = quick[Number(b.dataset.quickCity)];
      if (saved(value)) choose(value);
      else search(value.query, value);
    });
  }
  const night = r => {
    if (finite(r.is_day)) return Number(r.is_day) === 0;
    const d = report.daily.find(d => localDay(d.time) === localDay(r.time)), at = Date.parse(r.time);
    return Number.isFinite(Date.parse(d?.sunrise)) && Number.isFinite(Date.parse(d?.sunset)) &&
      (at < Date.parse(d.sunrise) || at >= Date.parse(d.sunset));
  };
  function hero() {
    const c = report?.current || {}, name = place.name, named = saved(place);
    const local = report?.daily.find(d => localDay(d.time) === localDay(c.time)) || report?.daily[0] || {};
    const subtitle = place.label === name ? "" : place.label.startsWith(name + ", ") ? place.label.slice(name.length + 2) : place.label;
    const fav = named && favorites().some(v => v.geo_id === place.geo_id);
    const home = named && prefs.home_city?.geo_id === place.geo_id;
    return `<article class="card city-hero ${report && night(c) ? "city-night" : ""}">
      <div class="city-hero-heading"><div><span class="city-kicker">${copy ? "PREVISIONE SALVATA" : byPosition && named ? "CITTÀ VICINA ALLA POSIZIONE" : byPosition ? "POSIZIONE APPROSSIMATA" : "METEO LOCALE"}</span>
      <h1>${esc(name)}</h1>${subtitle ? '<p class="city-place">' + esc(subtitle) + '</p>' : ""}</div>
      ${named ? `<div class="city-pin-actions"><button type="button" id="city-favorite" aria-pressed="${fav}" title="${fav ? "Rimuovi dai preferiti" : "Aggiungi ai preferiti"}"><span aria-hidden="true">${fav ? "★" : "☆"}</span> Preferito</button>
      <button type="button" id="city-pin" aria-pressed="${home}"><span aria-hidden="true">⌂</span> ${home ? "Città di avvio" : "Imposta come avvio"}</button></div>` : ""}
      </div>
      ${report ? `<div class="city-now"><div><div class="city-temperature">${unit(c.temp_c, "°")}</div>
        <p><strong>${esc(c.description || "Condizioni previste")}</strong></p>
        <p class="city-range">Percepita ${unit(c.feels_like_c, "°")} <span>↓ ${unit(local.temp_min_c,"°")} · ↑ ${unit(local.temp_max_c,"°")}</span></p></div>
        ${weatherIcon(c.description, night(c), "city-now-icon")}</div>
        <p class="city-time">${finite(c.temp_c) ? "Stima delle " + esc(hour(c.time)) + " · " : ""}${esc(dayName(c.time))} · ora locale</p>`
      : `<p class="city-loading">${flight || !error ? "Carico la previsione per questa località…" : "Previsione momentaneamente non disponibile."}</p>`}
      </article>`;
  }
  function details() {
    if (!report) return "";
    const c = report.current, d = report.daily.find(v => localDay(v.time) === localDay(c.time)) || report.daily[0] || {};
    const rows = [
      ["Pioggia oggi", unit(d.precip_probability,"%"), "probabilità massima"],
      ["Vento", unit(c.wind_kmh," km/h"), [compass(c.wind_dir), "Raffica " + unit(c.wind_gust_kmh, " km/h")].filter(Boolean).join(" · ")],
      ["Umidità", unit(c.humidity,"%"), "umidità relativa"],
      ["Luce", hour(d.sunset), "Tramonto · alba " + hour(d.sunrise)],
    ];
    return `<section class="city-facts" aria-label="Condizioni del giorno">${rows.map(([name, value, hint]) =>
      `<article class="card city-fact"><h2>${name}</h2><b>${esc(value)}</b><small>${esc(hint)}</small></article>`).join("")}</section>`;
  }
  function hours() {
    if (!report) return "";
    const at = copy ? Date.parse(report.fetched_at) : Date.now();
    const selected = mode() === "forecast" ? day : "";
    const rows = report.hourly.filter(r => selected ? localDay(r.time) === selected : Date.parse(r.time) + HOUR > at).slice(0, 24);
    return `<article class="card city-hours" id="city-hour-detail"><div class="card-header"><div><h2 id="city-hour-heading" tabindex="-1">${selected && rows.length ? "Ore di " + esc(dayName(rows[0].time)) : "Prossime ore"}</h2><p>Ora locale · probabilità di precipitazione</p></div>
      ${mode() === "today" ? '<button class="quiet" type="button" data-go="forecast">Tutte le previsioni →</button>' : ""}</div>
      ${rows.length ? `<ul class="city-hour-list" tabindex="0" aria-label="Previsioni ora per ora">${rows.map((r, i) => `<li>
        <time datetime="${esc(r.time)}">${!selected && i === 0 && !copy && Date.parse(r.time) <= at ? "Ora" : esc(hour(r.time))}</time>
        ${weatherIcon(r.description, night(r))}<span class="sr-only">${esc(r.description)}</span>
        <strong>${unit(r.temp_c,"°")}</strong><small class="city-rain">${unit(r.precip_probability,"%")}</small>
        ${selected ? '<small>' + unit(r.wind_kmh," km/h") + '</small>' : ""}</li>`).join("")}</ul>`
      : '<p class="empty">Ore non disponibili per questa data.</p>'}</article>`;
  }
  function week() {
    const rows = report?.daily.slice(0, 7) || [];
    if (!rows.length) return "";
    const values = rows.flatMap(d => [d.temp_min_c, d.temp_max_c]).filter(finite).map(Number);
    const low = Math.min(...values), span = Math.max(1, Math.max(...values) - low);
    const today = new Intl.DateTimeFormat("sv-SE", {timeZone: report.timezone, year:"numeric", month:"2-digit", day:"2-digit"}).format(new Date());
    const tomorrow = new Date(Date.parse(today + "T00:00:00Z") + 24 * HOUR).toISOString().slice(0, 10);
    return `<article class="card city-week"><div class="card-header"><div><h2>Prossimi 7 giorni</h2><p>Seleziona un giorno per vedere le ore.</p></div></div>
      <ul class="city-week-list">${rows.map(d => {
        const showRange = finite(d.temp_min_c) && finite(d.temp_max_c) && Number(d.temp_max_c) >= Number(d.temp_min_c);
        const left = showRange ? (Number(d.temp_min_c) - low) / span * 100 : 0;
        const width = showRange ? Math.max(1, (Number(d.temp_max_c) - Number(d.temp_min_c)) / span * 100) : 0;
        const key = localDay(d.time), label = key === today && !copy ? "Oggi" : key === tomorrow && !copy ? "Domani" : stamp(d.time, {weekday: "long"});
        return `<li><button type="button" data-city-day="${esc(key)}" aria-pressed="${mode() === "forecast" && day === key}">
          <span class="city-week-date"><b>${esc(label)}</b><small>${esc(stamp(d.time, {day:"numeric", month:"short"}))}</small></span>
          <span class="city-week-condition">${weatherIcon(d.description)}<small>${esc(d.description || "—")}</small></span>
          <span class="city-rain" title="Probabilità di precipitazione">${unit(d.precip_probability,"%")}</span>
          <span class="city-week-range"><span>${unit(d.temp_min_c,"°")}</span><i aria-hidden="true"><span style="left:${left.toFixed(1)}%;width:${width.toFixed(1)}%"></span></i><b>${unit(d.temp_max_c,"°")}</b></span>
          </button></li>`;
      }).join("")}</ul></article>`;
  }
  function forecastBody() {
    if (mode() === "forecast" && !day && report?.daily.length) day = localDay(report.daily[0].time);
    const footer = report ? `<div class="city-source"><p>Previsioni: <b>${esc(report.source)}</b> · aggiornate ${esc(dateTime(report.fetched_at))} · ${esc(report.timezone)}. Località: <a href="https://www.geonames.org/" target="_blank" rel="noopener noreferrer">GeoNames</a>.</p>
      ${saved(place) ? '<button id="city-sky" type="button" class="quiet">Pianifica il cielo per questa città →</button>' : ""}</div>` : "";
    return hero() + details() + (mode() === "forecast" ? week() + hours() : hours() + week()) + footer;
  }
  function paint() {
    if (!active() || !$("#city-feed")) return;
    const previous = document.activeElement, id = previous?.id, date = previous?.dataset.cityDay;
    $("#city-feed").innerHTML = forecastBody();
    $("#city-refresh").disabled = !!flight;
    $("#city-state").textContent = error ? ((copy ? "Copia salvata · " : "") + error + (needsRecovery() ? " Riprovo automaticamente." : "")) :
      copy ? "Copia salvata · cerco un aggiornamento…" : flight ? "Aggiornamento…" : report ? "Previsione disponibile" : "";
    $("#snapshot-time").textContent = report ? "Previsione città · " + dateTime(report.fetched_at) : "Previsioni per la città selezionata";
    paintChoices();
    bindFeed();
    if (id && ["city-pin","city-favorite"].includes(id)) $("#" + id)?.focus({preventScroll:true});
    if (date) [...$("#city-feed").querySelectorAll("[data-city-day]")].find(el => el.dataset.cityDay === date)?.focus({preventScroll:true});
  }
  function bindFeed() {
    if ($("#city-favorite")) $("#city-favorite").onclick = () => {
      const list = favorites(), next = list.filter(v => v.geo_id !== place.geo_id);
      if (list.length === next.length) {
        if (list.length >= 12) { notice("Hai 12 preferiti. Rimuovine uno per aggiungere questa città."); return; }
        next.push(descriptor(place));
      }
      storage.set(FAVORITES, next);
      notice(next.length > list.length ? "Aggiunta alle scelte rapide." : "Rimossa dai preferiti.");
      paint();
    };
    if ($("#city-pin")) $("#city-pin").onclick = () => {
      setStartup(prefs.home_city?.geo_id === place.geo_id ? "" : place.geo_id);
      storage.set("meteo.v5.preferences", prefs);
      notice(prefs.home_city ? place.name + " si aprirà all’avvio." : "Ripristinata Roma come città di avvio.");
      paint();
    };
    if ($("#city-sky")) $("#city-sky").onclick = () => {
      MeteoSky.update({site: {kind:"city", geo_id: place.geo_id, label: place.label}});
      navigate("astronomy");
    };
    $("#city-feed").querySelectorAll("[data-go]").forEach(b => b.onclick = () => navigate(b.dataset.go));
    $("#city-feed").querySelectorAll("[data-city-day]").forEach(b => b.onclick = () => {
      day = b.dataset.cityDay;
      if (mode() === "today") navigate("forecast");
      else paint();
      $("#city-hour-heading")?.focus({preventScroll:true});
      $("#city-hour-detail")?.scrollIntoView({block:"start"});
    });
  }
  function view() {
    ensure();
    return `<section class="city-view" aria-label="Previsioni della città">
      ${citySearch()}<div class="city-update"><span id="city-state" role="status"></span><button id="city-refresh" type="button" class="quiet">Aggiorna ora</button></div>
      <div id="city-feed"></div></section>`;
  }
  function bind() {
    $("#city-search").onsubmit = e => {e.preventDefault(); search($("#city-query").value);};
    $("#city-locate").onclick = locate;
    $("#city-refresh").onclick = () => { restartRecovery(); refreshCycle({forceCity: true}); };
    paintSearch();
    paint();
  }
  window.MeteoCity = { view, bind, refresh, profileChanged, startupOptions, setStartup, initial, favorites, removeFavorite,
    leave, forget, offline: () => {if (active()) refresh(true);}, needsRecovery: () => !!error && !flight };
})();
