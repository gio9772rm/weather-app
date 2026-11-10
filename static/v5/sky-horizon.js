/* N.I.N.A. horizon pairs, normalized at the north seam. */
"use strict";
(() => {
  const MAX = 721;
  let open = false;
  const round = v => Math.round(v * 1000) / 1000;
  const ordered = mask => Object.entries(mask).map(([az, alt]) => [Number(az), Number(alt)]).sort((a, b) => a[0] - b[0]);
  function seam(points) {
    if (points[0][0] === 0) return points[0][1];
    const [first, low] = points[0], [last, high] = points.at(-1);
    return round(high + (low - high) * (360 - last) / (first + 360 - last));
  }
  function parse(text) {
    const mask = {};
    for (const [index, line] of text.replace(/^\uFEFF/, "").split(/\r?\n/).entries()) {
      const pair = line.split("#", 1)[0].trim();
      if (!pair) continue;
      const tokens = pair.split(/\s+/);
      if (tokens.length !== 2 || !tokens.every(t => /^[+-]?(?:\d+(?:[.,]\d*)?|[.,]\d+)$/.test(t))) {
        throw Error("Riga " + (index + 1) + ": usa due numeri, azimut e altezza.");
      }
      const [az, alt] = tokens.map(t => Number(t.replace(",", ".")));
      if (!(az >= 0 && az <= 360 && alt >= -90 && alt <= 90)) throw Error("Riga " + (index + 1) + ": valori fuori intervallo.");
      const key = round(az) % 360;
      mask[key] = Math.max(mask[key] ?? -90, round(alt));
    }
    const points = ordered(mask);
    if (points.length < 2) throw Error("Il file deve contenere almeno due azimut distinti.");
    mask[0] = seam(points);
    if (Object.keys(mask).length > MAX) throw Error("Usa un file con al massimo 721 punti distinti.");
    return mask;
  }
  function exportText(mask) {
    const points = ordered(mask);
    if (points.length < 2) throw Error("Servono almeno due punti per esportare un orizzonte.");
    const north = seam(points);
    return "# Az Alt\n" + [[0, north], ...points.filter(p => p[0] > 0), [360, north]].map(p => p.join(" ")).join("\n") + "\n";
  }
  const key = (site, station) => !site ? "station:" + station :
    site.kind === "city" ? "city:" + site.geo_id : "point:" + [
      Number(site.latitude).toFixed(6), Number(site.longitude).toFixed(6), site.timezone, Number(site.elevation_m || 0).toFixed(1),
    ].join(":");
  const saved = (site, station) => window.MeteoSky.settings().horizons[key(site, station)];
  function preview(mask) {
    const points = ordered(mask);
    if (points.length < 2) return "";
    const north = seam(points), values = [[0, north], ...points.filter(p => p[0] > 0), [360, north]];
    const min = Math.min(0, ...points.map(p => p[1])), max = Math.max(10, ...points.map(p => p[1]));
    const coords = values.map(([az, h]) => `${az},${62 - 54 * (h - min) / (max - min)}`).join(" ");
    return `<div class="horizon-preview"><svg viewBox="0 0 360 66" preserveAspectRatio="none" role="img" aria-label="Altezza degli ostacoli in funzione dell’azimut">
      <polygon points="0,66 ${coords} 360,66"/><polyline points="${coords}" vector-effect="non-scaling-stroke"/></svg>
      <div aria-hidden="true"><span>N 0°</span><span>E 90°</span><span>S 180°</span><span>O 270°</span><span>N 360°</span></div></div>`;
  }
  function markup(site, station, legacy) {
    const record = saved(site, station), mask = record?.mask ?? legacy ?? {}, count = Object.keys(mask).length;
    return `<details class="card detail-row" id="cielo-horizon"${open ? " open" : ""}>
      <summary>Orizzonte della postazione · ${count ? count + " punti" : "non impostato"}</summary>
      ${count ? `<p>${esc(record?.name || "Orizzonte del piano")}. Gli ostacoli alzano la soglia per i soggetti e le finestre.</p>${preview(mask)}` : ""}
      <form id="horizon-import" class="sky-place-actions"><label>File orizzonte<input type="file" id="horizon-file" accept=".hrz,.txt,text/plain" required></label>
        <button class="quiet">Salva per questa postazione</button></form>
      <div class="controls">${count >= 2 ? '<button id="horizon-export" type="button" class="quiet">Scarica .hrz</button>' : ""}
        <button id="horizon-photo" type="button" class="quiet">Crea da foto panoramica</button>
        ${count ? '<button id="horizon-clear" type="button" class="quiet">Rimuovi orizzonte</button>' : ""}</div>
      <p class="metric-caption">Una coppia azimut altezza per riga, in gradi. Nord 0°, est 90°. Il file viene letto qui; nel profilo si salvano i punti, senza il file originale.</p>
      <p id="horizon-status" role="status"></p></details>`;
  }
  function bind(site, station, legacy, change) {
    const root = $("#cielo-horizon"), id = key(site, station);
    if (!root) return;
    const report = message => { if (root.isConnected) $("#horizon-status").textContent = message; };
    root.ontoggle = () => { if (root.isConnected) open = root.open; };
    const set = (value, message) => {
      const horizons = { ...MeteoSky.settings().horizons };
      if (value || Object.keys(legacy || {}).length) {
        if (!horizons[id] && Object.keys(horizons).length >= 16) {
          report("Puoi conservare fino a sedici orizzonti. Rimuovine uno prima di continuare."); return false;
        }
        horizons[id] = value || { name: "", mask: {} };
      } else delete horizons[id];
      MeteoSky.update({ horizons }); open = true; change();
      $("#horizon-status").textContent = message;
      return true;
    };
    $("#horizon-import").onsubmit = async event => {
      event.preventDefault();
      const file = $("#horizon-file").files[0];
      if (!file || file.size > 65536) { report("Scegli un file di testo fino a 64 KB."); return; }
      try {
        const mask = parse(await file.text());
        if (root.isConnected) set({ name: file.name.slice(0, 100), mask }, "Orizzonte applicato alla postazione.");
      } catch (error) { report(error.message); }
    };
    const clear = $("#horizon-clear"), download = $("#horizon-export");
    $("#horizon-photo").onclick = () => MeteoPanorama.open(mask => set(
      { name: "Panoramica tracciata", mask }, "Orizzonte da foto applicato alla postazione."
    ), Boolean(Object.keys(saved(site, station)?.mask ?? legacy ?? {}).length));
    if (clear) clear.onclick = () => set(null, "Orizzonte rimosso.");
    if (download) download.onclick = () => {
      const mask = saved(site, station)?.mask ?? legacy;
      const url = URL.createObjectURL(new Blob([exportText(mask)], { type: "text/plain" }));
      const a = document.createElement("a"); a.href = url; a.download = "meteo-orizzonte.hrz"; a.click(); URL.revokeObjectURL(url);
    };
  }
  globalThis.MeteoHorizon = { parse, exportText, key, saved, markup, bind };
})();
