/* City weather uses its own selection, forecast and offline copies. */
"use strict";
(() => {
  const DEFAULT = { geo_id: 3169070, name: "Roma", label: "Roma, Lazio, Italia" };
  const FAVORITES = "meteo.v5.cities", CACHE = "meteo.v5.city-weather";
  const HOUR = 3600000;
  let place, report, copy = false, error = "", flight = null, epoch = 0, checked = 0;
  let startId = null, fix = null, byPosition = false, day = "", query = "", matches = [], searchStatus = "";
  let searchEpoch = 0, looking = false, locating = false, migrate = null;
  let comparison = null, crossFlight = null, crossError = false, crossChecked = 0, metric = "temp_c";

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
  const calendarDay = () => report ? new Intl.DateTimeFormat("sv-SE", {timeZone:report.timezone,
    year:"numeric",month:"2-digit",day:"2-digit"}).format(new Date(copy ? Date.parse(report.fetched_at) : Date.now())) : "";
  const unit = (v, suffix, digits = 0) => finite(v) ? num(v, digits) + suffix : "—";
  const compass = v => finite(v) ? ["N","NE","E","SE","S","SO","O","NO"][Math.round(((Number(v) % 360 + 360) % 360) / 45) % 8] : "";
  function precipitation(row, daily = false) {
    if (!row.rainSources && finite(row.precip_probability)) {
      const span = !daily && row.probability_hours > 1 ? " nelle prossime " + num(row.probability_hours, 0) + " h" : "";
      return [unit(row.precip_probability, "%"), daily ? "Probabilità massima" : "Probabilità di precipitazione" + span];
    }
    const amount = finite(row.precipitation_mm) ? row.precipitation_mm : row.rain_mm;
    if (!finite(amount)) return ["—", "Dato non disponibile"];
    const span = daily && row.precipitation_partial ? "nelle " + num(row.precipitation_hours, 0) + " h coperte"
      : !daily && row.precipitation_hours > 1 ? "nelle prossime " + num(row.precipitation_hours, 0) + " h"
      : daily ? "nella giornata" : "per l’ora indicata";
    const source = row.rainSources ? " · mediana di " + row.rainSources + " fonti" : "";
    return [unit(amount, " mm", Number.isInteger(Number(amount)) ? 0 : 1), "Precipitazioni previste " + span + source];
  }
  const canonical = value => typeof value?.timezone === "string" && Number.isFinite(Date.parse(value.fetched_at)) &&
    finite(value.current?.temp_c) && Array.isArray(value.hourly) && value.hourly.length > 0 && Array.isArray(value.daily);
  const readCache = () => {
    const values = storage.get(CACHE, []);
    return Array.isArray(values) ? values.filter(v => saved(v) && canonical(v)).slice(0, 4) : [];
  };
  const getCopy = id => prefs.offline && validId(id) ? readCache().find(c => c.geo_id === id) : null;
  const keepCopy = value => {
    if (prefs.offline && saved(value) && !value.approximate) {
      const cached = {...value};
      delete cached.station_observation;
      storage.set(CACHE, [cached, ...readCache().filter(c => c.geo_id !== value.geo_id)].slice(0, 4));
    }
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
    metric = "temp_c";
    comparison = null; crossFlight = null; crossError = false; crossChecked = 0;
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
  function refreshComparison(force = false) {
    if (!report || copy || !navigator.onLine || crossFlight ||
        (!force && crossChecked && Date.now() - crossChecked < 10 * 60000)) return;
    const generation = epoch, id = place.geo_id ?? null;
    if (!validId(id) && !fix) return;
    crossError = false;
    crossFlight = (validId(id) ? request("city-comparison?geo_id=" + id) : request("city-comparison", {...fix, timezone: report.timezone}))
      .then(value => {
        if (generation !== epoch) return;
        if (value.geo_id !== id || !Array.isArray(value.hourly) || !Array.isArray(value.daily) || !Array.isArray(value.sources))
          throw Error();
        comparison = {...value, byTime: new Map(value.hourly.map(r => [Date.parse(r.time), r])),
          byDay: new Map(value.daily.map(r => [r.date, r]))};
        crossChecked = Date.now();
      })
      .catch(() => { if (generation === epoch) crossError = true; })
      .finally(() => {
        if (generation !== epoch) return;
        crossFlight = null;
        paint();
      });
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
          refreshComparison(force);
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
  const fields = {
    temp_c: ["Temperatura", "°C", 1], feels_like_c: ["Percepita", "°C", 1],
    dewpoint_c: ["Punto di rugiada", "°C", 1], humidity: ["Umidità", "%", 0],
    precipitation_mm: ["Precipitazioni", "mm", 1], precip_probability: ["Probabilità di pioggia", "%", 0],
    wind_kmh: ["Vento", "km/h", 0], wind_gust_kmh: ["Raffiche", "km/h", 0],
    wind_dir: ["Direzione", "", 0], clouds: ["Nuvole", "%", 0],
    cloud_low: ["Nuvole basse", "%", 0], cloud_mid: ["Nuvole medie", "%", 0], cloud_high: ["Nuvole alte", "%", 0],
    pressure_hpa: ["Pressione", "hPa", 0], visibility_m: ["Visibilità", "km", 1], uv_index: ["Indice UV", "", 1],
  };
  const groups = [
    ["temp_c","feels_like_c","dewpoint_c","humidity","pressure_hpa"],
    ["precipitation_mm","precip_probability","humidity","clouds"],
    ["wind_kmh","wind_gust_kmh","wind_dir"],
    ["clouds","cloud_low","cloud_mid","cloud_high","humidity"],
    ["visibility_m","uv_index","clouds"],
  ];
  const datum = (v, key) => key === "visibility_m" && finite(v) ? Number(v) / 1000 : v;
  const display = (v, key) => key === "wind_dir" ? compass(v) || "—" :
    unit(datum(v, key), fields[key][1] ? " " + fields[key][1] : "", fields[key][2]);
  const modelName = id => comparison?.sources.find(s => s.id === id)?.name || "";
  const crossDay = key => comparison?.byDay.get(key);
  const band = (r, key) => {
    const a = r?.values?.[key];
    return Array.isArray(a) && a.length === 4 && a.every(finite) && a[3] >= 1 ? a : null;
  };
  const blended = (r, key) => { const a = band(r, key); return a?.[3] >= 2 ? a : null; };
  const sourceCount = () => comparison?.sources.length || 0;
  function crossStatus() {
    const n = sourceCount();
    if (n > 1) return `Confronto tra ${n} fonti · i valori disponibili sono incrociati${copy || crossError ? " · ultimo confronto salvato in questa scheda" : ""}`;
    if (crossFlight || (!comparison && flight)) return "Confronto delle fonti in corso…";
    if (copy) return navigator.onLine ? "Confronto in attesa di una previsione aggiornata" : "Previsione salvata · confronto non disponibile offline";
    return n === 1 ? "Una sola fonte di confronto disponibile" : "Confronto non disponibile · previsione di riferimento";
  }
  function observation() {
    const o = report?.station_observation, age = Date.now() - Date.parse(o?.time);
    return !copy && place.geo_id === DEFAULT.geo_id && o?.values && age >= -2 * 60000 && age <= 20 * 60000 ? o : null;
  }
  function stationMatch() {
    const o = observation();
    if (!o) return null;
    const at = Date.parse(o.time), near = comparison?.hourly
      .map(r => [Math.abs(Date.parse(r.time) - at), r]).filter(([d]) => d <= HOUR / 2).sort((a,b) => a[0]-b[0])[0]?.[1];
    const base = [report.current, ...report.hourly].map(r => [Math.abs(Date.parse(r.time) - at), r])
      .filter(([d]) => d <= HOUR / 2).sort((a,b) => a[0]-b[0])[0]?.[1];
    const best = near && blended(near, "temp_c") ? near : null;
    const values = {};
    for (const k of ["temp_c","humidity","wind_kmh","pressure_hpa"]) {
      const a = blended(best, k);
      if (a) values[k] = a[0];
      else if (!best && finite(base?.[k])) values[k] = base[k];
    }
    return {observation:o, values, at:best?.time || base?.time, models:blended(best,"temp_c")?.[3] || 0};
  }
  function pinActions() {
    if (!saved(place)) return "";
    const fav = favorites().some(v => v.geo_id === place.geo_id), home = prefs.home_city?.geo_id === place.geo_id;
    return `<div class="city-pin-actions"><button type="button" id="city-favorite" aria-pressed="${fav}" title="${fav ? "Rimuovi dai preferiti" : "Aggiungi ai preferiti"}"><span aria-hidden="true">${fav ? "★" : "☆"}</span> Preferito</button>
      <button type="button" id="city-pin" aria-pressed="${home}"><span aria-hidden="true">⌂</span> ${home ? "Città di avvio" : "Imposta come avvio"}</button></div>`;
  }
  function enrich(r, compare) {
    if (!compare) return r;
    const result = {...r, compare};
    for (const key of Object.keys(compare.values || {})) {
      const a = band(compare, key);
      if (!a || (a[3] < 2 && finite(r[key]))) continue;
      if (key === "precipitation_mm") {
        result.precipitation_hours = 1;
        result.rainSources = a[3] >= 2 ? a[3] : 0;
      }
      result[key] = a[0];
    }
    return result;
  }
  function dayForecast(d) {
    const c = crossDay(localDay(d.time));
    if (!c) return d;
    const v = {...d, compare:c};
    for (const k of ["temp_min_c","temp_max_c"])
      if (c.temp_hours >= 18 && blended(c,k)) v[k] = blended(c,k)[0];
    if (blended(c,"precipitation_mm") && c.rain_hours) {
      const next = report.daily.find(next => Date.parse(next.time) > Date.parse(d.time));
      const hours = next ? (Date.parse(next.time) - Date.parse(d.time)) / HOUR : 24;
      v.precipitation_mm = blended(c,"precipitation_mm")[0];
      v.precipitation_hours = c.rain_hours;
      v.precipitation_partial = c.rain_hours < hours;
      v.rainSources = blended(c,"precipitation_mm")[3];
    }
    return v;
  }
  function weatherRows(key = "") {
    const base = new Map(report.hourly.map(r => [Date.parse(r.time), r]));
    const at = copy ? Date.parse(report.fetched_at) : Date.now();
    const ref = (row, t) => {
      if (report.source !== "Open-Meteo") return row;
      const end = base.get(t + HOUR);
      return {...row, precipitation_mm:end?.precipitation_mm, rain_mm:end?.rain_mm,
        precip_probability:end?.precip_probability, precipitation_hours:1, probability_hours:1};
    };
    const times = new Set([...base.keys()].filter(Number.isFinite));
    const alternate = comparison?.hourly || [];
    const localCross = alternate.filter(r => blended(r,"temp_c") && (key ? localDay(r.time) === key :
      Date.parse(r.time) + HOUR > at && Date.parse(r.time) < at + 24 * HOUR));
    // Do not interleave :00 and :30/:45 time grids on cities with fractional UTC offsets.
    if (localCross.length && localCross.every(r => !base.has(Date.parse(r.time)))) {
      return localCross.filter(r => Date.parse(r.time) + HOUR > at).map(r => {
        const t = Date.parse(r.time), sameHalfHour = [...base.entries()].filter(([when]) => Math.abs(when-t)<=HOUR/2)
          .sort((a,b)=>Math.abs(a[0]-t)-Math.abs(b[0]-t))[0]?.[1];
        return enrich({time:r.time, description:sameHalfHour?.description, is_day:sameHalfHour?.is_day},r);
      });
    }
    if (key) for (const r of localCross) times.add(Date.parse(r.time));
    return [...times].sort((a,b) => a-b).filter(t => t + HOUR > at).map(t => {
      const cross = comparison?.byTime.get(t), row = base.get(t);
      return enrich(row ? ref(row, t) : {time:cross.time}, cross);
    }).filter(r => !key || localDay(r.time) === key);
  }
  function compactCross() {
    const match = stationMatch();
    const roma = match && finite(match.observation.values.temp_c) && finite(match.values.temp_c) ?
      `<span>Roma · stazione ${display(match.observation.values.temp_c,"temp_c")} alle ${esc(hour(match.observation.time))} · stima ${display(match.values.temp_c,"temp_c")} alle ${esc(hour(match.at))}</span>` : "";
    return `<div class="city-cross-strip"><b>${esc(crossStatus())}</b>${roma}</div>`;
  }
  function hero() {
    const c = report?.current || {}, name = place.name, named = saved(place), today = calendarDay();
    const local = dayForecast(report?.daily.find(d => localDay(d.time) === today) || report?.daily[0] || {});
    const o = observation(), hasMeasure = finite(o?.values.temp_c);
    const subtitle = place.label === name ? "" : place.label.startsWith(name + ", ") ? place.label.slice(name.length + 2) : place.label;
    return `<article class="card city-hero ${report && night(c) ? "city-night" : ""}">
      <div class="city-hero-heading"><div><span class="city-kicker">${copy ? "PREVISIONE SALVATA" : byPosition && named ? "CITTÀ VICINA ALLA POSIZIONE" : byPosition ? "POSIZIONE APPROSSIMATA" : "METEO LOCALE"}</span>
      <h1>${esc(name)}</h1>${subtitle ? '<p class="city-place">' + esc(subtitle) + '</p>' : ""}</div>
      ${pinActions()}
      </div>
      ${report ? `<div class="city-now"><div><div class="city-temperature">${unit(hasMeasure ? o.values.temp_c : c.temp_c, "°")}</div>
        <p><strong>${esc(c.description || "Condizioni previste")}</strong></p>
        <p class="city-range">${!hasMeasure && finite(c.feels_like_c) ? "Percepita " + unit(c.feels_like_c, "°") : ""}<span>↓ ${unit(local.temp_min_c,"°")} · ↑ ${unit(local.temp_max_c,"°")}</span></p></div>
        ${weatherIcon(c.description, night(c), "city-now-icon")}</div>
        <p class="city-time">${hasMeasure ? "Stazione di Roma · misura delle " + esc(hour(o.time)) : "Stima delle " + esc(hour(c.time))} · ${esc(dayName(hasMeasure ? o.time : c.time))} · ora locale</p>`
      : `<p class="city-loading">${flight || !error ? "Carico la previsione per questa località…" : "Previsione momentaneamente non disponibile."}</p>`}
      </article>`;
  }
  function details() {
    if (!report) return "";
    const c = report.current, today = calendarDay();
    const d = dayForecast(report.daily.find(v => localDay(v.time) === today) || report.daily[0] || {});
    const o = observation()?.values || {};
    const rain = precipitation(d, true);
    const rows = [
      [copy ? "Precipitazioni" : "Precipitazioni oggi", ...rain],
      ["Vento", unit(o.wind_kmh ?? c.wind_kmh," km/h"), finite(o.wind_kmh) ? "Misurato dalla stazione" : [compass(c.wind_dir), finite(c.wind_gust_kmh) ? "Raffica " + unit(c.wind_gust_kmh, " km/h") : ""].filter(Boolean).join(" · ")],
      ["Umidità", unit(o.humidity ?? c.humidity,"%"), finite(o.humidity) ? "Misurata dalla stazione" : "umidità relativa"],
      ["Luce", hour(d.sunset), "Tramonto · alba " + hour(d.sunrise)],
    ];
    return `<section class="city-facts" aria-label="Condizioni del giorno">${rows.map(([name, value, hint]) =>
      `<article class="card city-fact"><h2>${name}</h2><b>${esc(value)}</b><small>${esc(hint)}</small></article>`).join("")}</section>`;
  }
  function hours() {
    if (!report) return "";
    const at = copy ? Date.parse(report.fetched_at) : Date.now();
    const rows = weatherRows().slice(0, 24);
    return `<article class="card city-hours"><div class="card-header"><div><h2>Prossime ore</h2><p>Ora locale · precipitazioni in % o mm</p></div>
      <button class="quiet" type="button" data-go="forecast">Tutte le previsioni →</button></div>
      ${compactCross()}
      ${rows.length ? `<ul class="city-hour-list" tabindex="0" aria-label="Previsioni ora per ora">${rows.map((r, i) => {
        const [rain, hint] = precipitation(r);
        return `<li><time datetime="${esc(r.time)}">${i === 0 && !copy && Date.parse(r.time) <= at ? (observation() ? "Ora · stima" : "Ora") : esc(hour(r.time))}</time>
        ${weatherIcon(r.description, night(r))}<span class="sr-only">${esc(r.description || "Condizioni previste")}</span>
        <strong>${unit(r.temp_c,"°")}</strong><small class="city-rain" title="${esc(hint)}">${esc(rain)}</small></li>`;
      }).join("")}</ul>` : '<p class="empty">Ore non disponibili per questa data.</p>'}</article>`;
  }
  function week() {
    const rows = report?.daily.slice(0, 7).map(dayForecast) || [];
    if (!rows.length) return "";
    const values = rows.flatMap(d => [d.temp_min_c, d.temp_max_c]).filter(finite).map(Number);
    const low = Math.min(...values), span = Math.max(1, Math.max(...values) - low);
    const today = calendarDay();
    const tomorrow = new Date(Date.parse(today + "T00:00:00Z") + 24 * HOUR).toISOString().slice(0, 10);
    return `<article class="card city-week"><div class="card-header"><div><h2>Prossimi 7 giorni</h2><p>Seleziona un giorno per vedere le ore.</p></div></div>
      <ul class="city-week-list">${rows.map(d => {
        const showRange = finite(d.temp_min_c) && finite(d.temp_max_c) && Number(d.temp_max_c) >= Number(d.temp_min_c);
        const left = showRange ? (Number(d.temp_min_c) - low) / span * 100 : 0;
        const width = showRange ? Math.max(1, (Number(d.temp_max_c) - Number(d.temp_min_c)) / span * 100) : 0;
        const key = localDay(d.time), label = key === today && !copy ? "Oggi" : key === tomorrow && !copy ? "Domani" : stamp(d.time, {weekday: "long"});
        const [rain, hint] = precipitation(d, true);
        return `<li><button type="button" data-city-day="${esc(key)}" aria-pressed="${mode() === "forecast" && day === key}">
          <span class="city-week-date"><b>${esc(label)}</b><small>${esc(stamp(d.time, {day:"numeric", month:"short"}))}${d.compare?.temp_hours>=18 && blended(d.compare,"temp_min_c") ? " · " + blended(d.compare,"temp_min_c")[3] + " fonti" : ""}</small></span>
          <span class="city-week-condition">${weatherIcon(d.description)}<small>${esc(d.description || "—")}</small></span>
          <span class="city-rain" title="${esc(hint)}">${esc(rain)}</span>
          <span class="city-week-range"><span>${unit(d.temp_min_c,"°")}</span><i aria-hidden="true"><span style="left:${left.toFixed(1)}%;width:${width.toFixed(1)}%"></span></i><b>${unit(d.temp_max_c,"°")}</b></span>
          </button></li>`;
      }).join("")}</ul></article>`;
  }
  function forecastDays() {
    const dates = report.daily.slice(0, 7);
    return `<div class="city-day-tabs" role="group" aria-label="Giorno della previsione">${dates.map(d => {
      const key = localDay(d.time), v = dayForecast(d);
      return `<button type="button" data-city-day="${esc(key)}" aria-pressed="${day === key}">
        <b>${esc(stamp(d.time,{weekday:"short"}))}</b><small>${esc(stamp(d.time,{day:"numeric",month:"short"}))}</small>
        <span>${unit(v.temp_min_c,"°")} / ${unit(v.temp_max_c,"°")}</span></button>`;
    }).join("")}</div>`;
  }
  function forecastLead(d, rows) {
    const c = d?.compare, temperatures = [blended(c,"temp_min_c"), blended(c,"temp_max_c")];
    const low = temperatures[0]?.[0] ?? d?.temp_min_c, high = temperatures[1]?.[0] ?? d?.temp_max_c;
    const peak = key => rows.reduce((max,r) => finite(r[key]) ? Math.max(max,Number(r[key])) : max, -Infinity);
    const gust = peak("wind_gust_kmh"), wind = peak("wind_kmh");
    const rain = precipitation(d || {}, true);
    const tiles = [
      ["Minima e massima", unit(low,"°") + " / " + unit(high,"°"), temperatures[0] ? c.temp_hours + " ore confrontabili" : "Fascia prevista"],
      ["Precipitazioni", ...rain],
      ["Vento massimo", unit(wind," km/h"), finite(gust) ? "Raffiche fino a " + unit(gust," km/h") : "Velocità prevista"],
      ["Alba e tramonto", hour(d?.sunrise) + " / " + hour(d?.sunset), "Ora locale della città"],
    ];
    return `<article class="card city-forecast-lead">
      <div class="city-forecast-heading"><div><span class="city-forecast-kicker">QUADRO PREVISIONALE</span>
      <h1>Previsioni per ${esc(place.name)}</h1><p>${esc(place.label)}${d ? " · " + esc(dayName(d.time)) : ""}</p></div>${pinActions()}</div>
      ${report ? forecastDays() + `<div class="city-forecast-key">${tiles.map(([name,value,note]) =>
        `<div><span>${esc(name)}</span><strong>${esc(value)}</strong><small>${esc(note)}</small></div>`).join("")}</div>
        <p class="city-lead-cross">${esc(crossStatus())}</p>` : `<p>${error ? "Previsione momentaneamente non disponibile." : "Carico i dati della località…"} </p>`}
      </article>`;
  }
  function seriesValue(r, key) {
    if ((key === "precipitation_mm" && r.precipitation_hours > 1) ||
        (key === "precip_probability" && r.probability_hours > 1)) return null;
    return finite(r[key]) ? Number(r[key]) : null;
  }
  function forecastPlot(rows) {
    const points = rows.map(r => ({t:Date.parse(r.time), time:r.time, v:seriesValue(r,metric),
      spread:blended(r.compare,metric)})).filter(r => Number.isFinite(r.t) && r.v !== null);
    if (!points.length) return '<p class="empty">Valori orari non disponibili per questo parametro. Gli eventuali totali su più ore sono indicati in tabella.</p>';
    const rainfall = metric === "precipitation_mm", xs = points.map(p=>p.t);
    const minX = Math.min(...xs), maxX = Math.max(...xs);
    const values = points.flatMap(p=>p.spread ? [p.v,p.spread[1],p.spread[2]] : [p.v]);
    let lo = rainfall ? 0 : Math.min(...values), hi = Math.max(...values);
    if (lo === hi) { if (!rainfall) lo -= 1; hi += 1; }
    const x = t => minX === maxX ? 324 : 64 + 533 * (t-minX)/(maxX-minX), y = n => 143 - 113 * (n-lo)/(hi-lo);
    const q = n => n.toFixed(1);
    const ticks = Array.from({length:4},(_,i) => {
      const v=lo+(hi-lo)*i/3, pos=q(y(v));
      return `<path d="M56 ${pos}H607"/><text x="50" y="${pos}" text-anchor="end" dominant-baseline="middle">${esc(display(v,metric))}</text>`;
    }).join("");
    const marks = points.map((p,i)=>{
      const range = p.spread && p.spread[1] !== p.spread[2] ? `<path class="model-range" d="M${q(x(p.t))} ${q(y(p.spread[1]))}V${q(y(p.spread[2]))}"/>` : "";
      const previous = points[i-1];
      if (rainfall) return range + `<rect class="value-bar" x="${q(x(p.t)-4)}" y="${q(y(p.v))}" width="8" height="${q(Math.max(1,143-y(p.v)))}"/>`;
      const segment = previous && p.t-previous.t <= 1.01 * HOUR ?
        `<path class="value-line" d="M${q(x(previous.t))} ${q(y(previous.v))}L${q(x(p.t))} ${q(y(p.v))}"/>` : "";
      return range + segment + `<circle class="value-dot" cx="${q(x(p.t))}" cy="${q(y(p.v))}" r="2.8"/>`;
    }).join("");
    const labelCount = Math.min(5,points.length);
    const labels = Array.from({length:labelCount},(_,i)=>Math.round(i*(points.length-1)/Math.max(1,labelCount-1)));
    return `<svg class="city-forecast-plot" viewBox="0 0 630 174" role="img" aria-label="Andamento orario: ${esc(fields[metric][0])}">
      ${ticks}${marks}${labels.map(i=>`<text x="${q(x(points[i].t))}" y="166" text-anchor="middle">${esc(hour(points[i].time))}</text>`).join("")}</svg>
      <p class="city-plot-legend"><span>${points.some(p=>p.spread) ? "Stima centrale dove confrontabile" : "Valori disponibili"}</span>
      ${points.some(p=>p.spread) ? "<span>Estremi delle fonti per la stessa ora</span>" : ""}</p>`;
  }
  function hourTable(rows) {
    if (!rows.length) return '<p class="empty">Ore non disponibili per questa data.</p>';
    const columns = groups.find(g => g.includes(metric)) || groups[0];
    return `<div class="city-table-wrap" tabindex="0" role="region" aria-label="Tabella oraria scorrevole"><table>
      <thead><tr><th scope="col">Ora locale</th>${columns.map(k => `<th scope="col">${esc(fields[k][0])}</th>`).join("")}</tr></thead>
      <tbody>${rows.map(r => `<tr><th scope="row"><time datetime="${esc(r.time)}">${esc(hour(r.time))}</time></th>${columns.map(k => {
        const a = band(r.compare,k), multiple = a?.[3] >= 2, native = k === "precipitation_mm" ? r.precipitation_hours : k === "precip_probability" ? r.probability_hours : null;
        const comparable = multiple ? ` · ${a[3]} fonti (${display(a[1],k)}–${display(a[2],k)})` : "";
        const single = !multiple && a?.[3] === 1 && Number(r[k]) === a[0] && r.compare?.single?.[k] ? " · " + modelName(r.compare.single[k]) : "";
        return `<td title="${esc(fields[k][0] + comparable + single)}">${esc(display(r[k],k))}${finite(r[k]) && native > 1 ? ` <small>/ ${num(native,0)} h</small>` : ""}${multiple && k === metric ? `<small class="city-row-band">${esc(display(a[1],k))}–${esc(display(a[2],k))}</small>` : ""}</td>`;
      }).join("")}</tr>`).join("")}</tbody></table></div>`;
  }
  function hourlyDetail(d, rows) {
    return `<article class="card city-detail-panel" id="city-hour-detail"><div class="card-header city-detail-header"><div>
      <span class="city-forecast-kicker">ORA PER ORA</span><h2 id="city-hour-heading" tabindex="-1">Ore di ${esc(dayName(d.time))}</h2>
      <p>Grafico e valori per la giornata selezionata.</p></div>
      <label>Parametro<select id="city-metric">${Object.keys(fields).filter(k=>k!=="wind_dir").map(k=>
        `<option value="${k}"${metric===k ? " selected" : ""}>${esc(fields[k][0])}</option>`).join("")}</select></label></div>
      ${forecastPlot(rows)}${hourTable(rows)}
      <p class="metric-caption">I millimetri misurano la quantità prevista; l’eventuale percentuale proviene dalla previsione di riferimento. «—» indica che manca il dato.</p></article>`;
  }
  const CREDITS = {
    "Open-Meteo":"https://open-meteo.com/", "Locationforecast":"https://www.met.no/en",
    "7Timer!":"https://www.7timer.info/", "Bright Sky":"https://brightsky.dev/",
  };
  function modelsDetail(d) {
    const info = d?.compare, list = comparison?.sources || [];
    const count = info?.models?.filter(r => finite(r.temp_min_c) || finite(r.precipitation_mm)).length || 0;
    const tempCoverage = blended(info,"temp_min_c") ? info.temp_hours + " h" + (info.temp_start ? " da " + hour(info.temp_start) : "") : "—";
    const rainCoverage = blended(info,"precipitation_mm") ? info.rain_hours + " h" : "—";
    return `<article class="card city-detail-panel"><div class="card-header"><div><span class="city-forecast-kicker">CONFRONTO DELLE FONTI</span>
      <h2>Come cambiano le stime</h2><p>${esc(crossStatus())}</p></div><span class="tag">${count}/3 sul giorno</span></div>
      ${list.length ? `<div class="city-table-wrap" tabindex="0" role="region" aria-label="Confronto delle fonti"><table>
        <thead><tr><th scope="col">Fonte</th><th scope="col">Minima / massima</th><th scope="col">Precipitazioni</th></tr></thead><tbody>${list.map(s=>{
          const r = info?.models?.find(m=>m.id===s.id) || {};
          const via = CREDITS[s.via] ? `<a href="${CREDITS[s.via]}" target="_blank" rel="noopener noreferrer">${esc(s.via)}</a>` : esc(s.via);
          return `<tr><th scope="row"><b>${esc(s.name)}</b><small>${via}</small></th>
            <td>${display(r.temp_min_c,"temp_c")} / ${display(r.temp_max_c,"temp_c")}</td><td>${display(r.precipitation_mm,"precipitation_mm")}</td></tr>`;
        }).join("")}</tbody></table></div>` : ""}
      ${info ? `<p class="metric-caption">Ore con più fonti: temperatura ${esc(tempCoverage)}, precipitazioni ${esc(rainCoverage)}. Gli estremi descrivono lo scarto tra le fonti.</p>` : ""}
      ${list.length ? `<details class="city-method"><summary>Come si ottiene la stima</summary><p>Il valore centrale è la mediana dei dati disponibili per lo stesso orario o intervallo. Il numero di fonti può cambiare per parametro e ora. Il loro accordo non è una probabilità calibrata.</p>
      ${comparison.shared_model ? "<p>Le fonti di riserva possono condividere ECMWF: il confronto non equivale a tre modelli indipendenti.</p>" : ""}
      ${list.filter(s=>s.note).map(s=>`<p><b>${esc(s.name)}:</b> ${esc(s.note)}</p>`).join("")}</details>` : ""}</article>`;
  }
  function stationDetail() {
    if (place.geo_id !== DEFAULT.geo_id) return "";
    const m = stationMatch(), keys = ["temp_c","humidity","wind_kmh","pressure_hpa"];
    return `<article class="card city-detail-panel city-local-compare"><div class="card-header"><div>
      <span class="city-forecast-kicker">RISCONTRO LOCALE</span><h2>Stazione di Roma</h2>
      <p>${m ? "Misura delle " + esc(hour(m.observation.time)) + (m.at ? " · previsione delle " + esc(hour(m.at)) + (m.models ? " · " + m.models + " fonti" : " · fonte di riferimento") : "") : "Misure recenti e utilizzabili non disponibili."}</p></div>
      <a href="/?page=stations&amp;station=roma-primary">Apri la stazione →</a></div>
      ${m ? `<div class="city-table-wrap" tabindex="0" role="region" aria-label="Misure di Roma a confronto"><table>
        <thead><tr><th scope="col">Parametro</th><th scope="col">Misurato</th><th scope="col">Previsto</th><th scope="col">Scarto</th></tr></thead>
        <tbody>${keys.filter(k=>finite(m.observation.values[k])).map(k=>{
          const v=m.observation.values[k], f=m.values[k], delta=finite(f)? Number(v)-Number(f):null;
          return `<tr><th scope="row">${esc(fields[k][0])}</th><td>${display(v,k)}</td><td>${display(f,k)}</td><td>${finite(delta) && delta>0 ? "+" : ""}${display(delta,k)}</td></tr>`;
        }).join("")}</tbody></table></div>` : ""}
      <p class="metric-caption">La stazione descrive il meteo locale adesso; le previsioni dei giorni successivi restano basate sulle fonti previsionali.</p></article>`;
  }
  function expandedForecast() {
    if (!report?.daily.length) return forecastLead(null, []);
    const dates = report.daily;
    if (!dates.some(d=>localDay(d.time)===day)) {
      const first = localDay(dates[0].time);
      day = !copy && dates.length>1 && weatherRows(first).length<4 ? localDay(dates[1].time) : first;
    }
    const d = dayForecast(dates.find(d=>localDay(d.time)===day));
    const rows = weatherRows(day);
    return forecastLead(d,rows) + hourlyDetail(d,rows) + modelsDetail(d) + stationDetail();
  }
  function forecastBody() {
    const source = report?.source === "MET Norway" ? '<a href="https://www.met.no/en" target="_blank" rel="noopener noreferrer">MET Norway</a>'
      : report?.source === "Open-Meteo" ? '<a href="https://open-meteo.com/" target="_blank" rel="noopener noreferrer">Open-Meteo</a>' : esc(report?.source);
    const footer = report ? `<div class="city-source"><p>Previsioni: <b>${source}</b> · aggiornate ${esc(dateTime(report.fetched_at))} · ${esc(report.timezone)}. Località: <a href="https://www.geonames.org/" target="_blank" rel="noopener noreferrer">GeoNames</a>. Dati rielaborati · <a href="https://creativecommons.org/licenses/by/4.0/" target="_blank" rel="noopener noreferrer">CC BY 4.0</a>.</p>
      ${saved(place) ? '<button id="city-sky" type="button" class="quiet">Pianifica il cielo per questa città →</button>' : ""}</div>` : "";
    return (mode() === "forecast" ? expandedForecast() : hero() + details() + hours() + week()) + footer;
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
    if (id && ["city-pin","city-favorite","city-metric"].includes(id)) $("#" + id)?.focus({preventScroll:true});
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
    if ($("#city-metric")) $("#city-metric").onchange = e => {
      if (Object.hasOwn(fields,e.target.value)) { metric = e.target.value; paint(); }
    };
    $("#city-feed").querySelectorAll("[data-go]").forEach(b => b.onclick = () => navigate(b.dataset.go));
    $("#city-feed").querySelectorAll("[data-city-day]").forEach(b => b.onclick = () => {
      day = b.dataset.cityDay;
      if (mode() === "today") {
        navigate("forecast");
        $("#city-hour-heading")?.focus({preventScroll:true});
        $("#city-hour-detail")?.scrollIntoView({block:"start"});
      } else paint();
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
