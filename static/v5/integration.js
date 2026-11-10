/* Read acquisition summaries on device; keep only reviewed nightly totals. */
"use strict";
(() => {
  const normalize = s => String(s).normalize("NFKD").replace(/\p{M}/gu, "").toLowerCase().replace(/[^a-z0-9]/g, "");
  const clean = s => String(s).trim().replace(/\s+/g, " ");
  const filterKey = s => { const key = clean(s).toLowerCase(); return key === "sii" ? "s2" : key; };
  const accepted = new Set(["ok", "yes", "si", "true", "1", "valid", "valido", "valida", "accepted", "accettato", "accettata", "perfect", "perfetto", "perfetta", "perfette", "perfetti", "good", "buona", "buono", "keep"]);
  const aliases = {
    filter: ["filter", "filtro", "filtername", "nomefiltro", "band"],
    frames: ["posevalide", "scattivalidi", "acceptedframes", "acceptedcount", "perfectframes", "perfetti", "number", "frames", "framecount", "pose", "scatti", "numero", "count"],
    seconds: ["secondiperposa", "duration", "durationseconds", "exposureseconds", "exposuretime", "exposure", "exptime", "durata", "duratas", "duratasecondi", "posasecondi", "esposizione", "esposiziones"],
    status: ["status", "stato", "esito", "decision", "quality", "qualita", "accepted", "valido"],
    target: ["target", "object", "objectname", "soggetto", "oggetto", "progetto"],
    day: ["dataserata", "sessiondate", "nightdate", "date", "data"],
  };

  function csv(text, delimiter) {
    const rows = [];
    let row = [], field = "", quoted = false, closed = false;
    const cell = () => { row.push(field.trim()); field = ""; closed = false; };
    const line = () => {
      cell();
      if (row.some(Boolean)) rows.push(row);
      if (rows.length > 5001) throw Error("Dividi il file: massimo 5.000 righe per importazione.");
      row = [];
    };
    text = text.replace(/^\uFEFF/, "").replace(/\r\n?/g, "\n");
    for (let i = 0; i < text.length; i++) {
      const c = text[i];
      if (quoted) {
        if (c === '"' && text[i + 1] === '"') { field += '"'; i++; }
        else if (c === '"') { quoted = false; closed = true; }
        else field += c;
      } else if (c === delimiter) cell();
      else if (c === "\n") line();
      else if (c === '"' && !field.trim() && !closed) { quoted = true; field = ""; }
      else if (!closed || /\s/.test(c)) field += c;
      else throw Error("Controlla le virgolette del CSV.");
    }
    if (quoted) throw Error("Il CSV contiene una virgoletta non chiusa.");
    line();
    return rows;
  }

  function parse(text) {
    const choices = [",", ";", "\t"].map(d => {
      try {
        const [headers, ...rows] = csv(text, d);
        if (headers?.length >= 2 && headers.length <= 40 && rows.length && rows.every(r => r.length === headers.length)) return {headers, rows};
      } catch { /* another delimiter can still be valid */ }
      return null;
    }).filter(Boolean).sort((a, b) => b.headers.length - a.headers.length);
    if (!choices.length) throw Error("Serve un CSV o TSV con intestazioni e righe della stessa lunghezza (massimo 5.000).");
    return choices[0];
  }

  function suggest(headers) {
    const keys = headers.map(normalize);
    return Object.fromEntries(Object.entries(aliases).map(([key, names]) => [key, names.map(n => keys.indexOf(n)).find(i => i >= 0) ?? -1]));
  }

  function counts(parsed, columns, target, day = "") {
    const result = new Map(), subject = new Set([normalize(target.id), normalize(target.name)]);
    let skipped = 0;
    const number = s => /^[+]?(?:\d+(?:[.,]\d*)?|[.,]\d+)$/.test(s) ? Number(s.replace(",", ".")) : NaN;
    if (columns.filter < 0 || columns.seconds < 0) throw Error("Scegli le colonne Filtro e Secondi per posa.");
    if (columns.frames === -2) throw Error("Scegli il conteggio o indica «Una riga per posa».");
    const isoDay = s => {
      const local = /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/.exec(s);
      return local ? [local[3], local[2].padStart(2, "0"), local[1].padStart(2, "0")].join("-") : s.slice(0, 10);
    };
    for (const [index, row] of parsed.rows.entries()) {
      if ((columns.target >= 0 && !subject.has(normalize(row[columns.target]))) ||
          (columns.day >= 0 && isoDay(row[columns.day]) !== day) ||
          (columns.status >= 0 && !accepted.has(normalize(row[columns.status])))) { skipped++; continue; }
      const n = columns.frames >= 0 ? number(row[columns.frames]) : 1;
      if (n === 0) { skipped++; continue; }
      const name = clean(row[columns.filter]), seconds = number(row[columns.seconds]);
      if (!name || name.length > 80 || !Number.isInteger(n) || n < 1 || n > 100000 || !(seconds > 0 && seconds <= 3600)) {
        throw Error("Riga dati " + (index + 1) + ": controlla filtro, numero di pose e durata in secondi.");
      }
      const entry = result.get(name) || { frames: 0, seconds: 0 };
      entry.frames += n; entry.seconds += n * seconds; result.set(name, entry);
    }
    if (!result.size || result.size > 32) throw Error("Il file deve contenere da 1 a 32 filtri con pose valide per il soggetto.");
    return { rows: [...result], skipped };
  }

  function match(name, filters) {
    const exact = filters.find(f => clean(f.name).toLowerCase() === clean(name).toLowerCase());
    const alias = filters.filter(f => filterKey(f.name) === filterKey(name));
    return exact?.name || (alias.length === 1 ? alias[0].name : "");
  }

  function totals(project) {
    const seconds = new Map();
    for (const entry of project.imports || []) for (const part of entry.filters) {
      const key = part.name.toLowerCase();
      seconds.set(key, (seconds.get(key) || 0) + part.seconds);
    }
    return { target: project.target, filters: project.filters.map(f => ({
      ...f, acquired_seconds: f.acquired_seconds + (seconds.get(f.name.toLowerCase()) || 0),
    })) };
  }

  const same = (a, b) => a.day === b.day && a.batch.toLowerCase() === b.batch.toLowerCase();
  function replace(project, receipt) {
    const other = (project.imports || []).filter(row => !same(row, receipt));
    if (other.some(row => row.digest === receipt.digest)) throw Error("Questo file risulta già importato con un’altra data o lotto.");
    if (other.length >= 60) throw Error("Limite di sessanta importazioni per progetto.");
    const result = { ...project, imports: [...other, receipt] };
    const all = new Map(project.filters.map(f => [f.name.toLowerCase(), f.acquired_seconds]));
    for (const entry of result.imports) for (const row of entry.filters) {
      const k = row.name.toLowerCase();
      all.set(k, (all.get(k) || 0) + row.seconds);
      if (all.get(k) > 3600000) throw Error("Ore raccolte oltre il limite del progetto.");
    }
    return result;
  }

  function download(text, filename) {
    const url = URL.createObjectURL(new Blob([text], { type: "text/csv;charset=utf-8" }));
    const a = document.createElement("a"); a.href = url; a.download = filename; a.click();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  }

  function open(target, zone, onSave) {
    $("#integration-dialog")?.remove();
    const project = MeteoProjects.find(target.id);
    if (!project) return;
    const dialog = document.createElement("dialog");
    dialog.id = "integration-dialog";
    const day = new Intl.DateTimeFormat("en-CA", { timeZone: zone, year: "numeric", month: "2-digit", day: "2-digit" }).format(new Date(Date.now() - 12 * 3600000));
    dialog.innerHTML = `<form><div class="dialog-title"><h2>Chiudi serata · ${esc(target.id)}</h2><button type="button" id="integration-close" class="quiet" aria-label="Chiudi">×</button></div>
      <p>Usa un riepilogo delle pose valide per questo soggetto. Il file resta sul dispositivo; si salvano data, lotto e totali per filtro.</p>
      <div class="form-grid"><label>Inizio serata<input id="integration-day" type="date" value="${esc(day)}" required></label><label>Lotto<input id="integration-batch" value="Principale" maxlength="24" required></label></div>
      <label>Riepilogo CSV o TSV<input id="integration-file" type="file" accept=".csv,.tsv,.txt,text/csv" required></label>
      <button type="button" class="quiet" id="integration-template">Scarica modello CSV</button>
      <div id="integration-columns" class="form-grid"></div><div id="integration-mapping"></div><div id="integration-preview"></div>
      <p id="integration-status" role="status"></p><label class="integration-confirm"><input type="checkbox" id="integration-confirm" required> Ho controllato che il riepilogo includa solo le pose valide della serata.</label>
      <button type="submit" class="primary" id="integration-commit" disabled>Aggiorna le ore raccolte</button>
      ${project.imports?.length ? `<details class="detail-row"><summary>Serate importate (${project.imports.length})</summary><ul>${project.imports.map((entry, i) => `<li>${esc(entry.day + " · " + entry.batch)} <button type="button" class="quiet" data-integration-remove="${i}">Rimuovi</button></li>`).join("")}</ul></details>` : ""}</form>`;
    document.body.append(dialog); dialog.showModal(); dialog.onclose = () => dialog.remove();
    let parsed, digest, receipt, generation = 0;
    const $d = selector => dialog.querySelector(selector);
    const info = $d("#integration-status"), submit = $d("#integration-commit");
    const pick = {};
    const save = result => {
      MeteoSky.update({ projects: MeteoProjects.list().map(p => p.target === result.target ? result : p) });
      dialog.close(); onSave();
    };
    $d("#integration-close").onclick = () => dialog.close();
    $d("#integration-template").onclick = () => download("filtro;pose_valide;secondi_per_posa\n", "meteo-pose-valide.csv");
    function preview() {
      submit.disabled = true; receipt = null; info.textContent = "";
      $d("#integration-preview").replaceChildren();
      if (!parsed || !digest) return;
      try {
        const day = $d("#integration-day").value, batch = clean($d("#integration-batch").value);
        if (!day || !batch) throw Error("Indica la data di inizio e il lotto.");
        const {rows, skipped} = counts(parsed, Object.fromEntries(Object.entries(pick).map(([key, node]) => [key, Number(node.value)])), target, day);
        const selected = [...$d("#integration-mapping").querySelectorAll("select")].map(node => node.value);
        const previous = [...$d("#integration-mapping").querySelectorAll("select")].map(node => node.dataset.raw);
        const unknown = rows.filter(([name]) => !match(name, project.filters));
        const unresolved = unknown.map(([name]) => name);
        if (JSON.stringify(previous) !== JSON.stringify(unresolved)) {
          $d("#integration-mapping").innerHTML = unknown.map(([name]) => `<label>Associa «${esc(name)}»<select data-raw="${esc(name)}"><option value="">Scegli filtro</option><option value="-">Ignora</option>${project.filters.map((f, i) => `<option value="${i}">${esc(f.name)}</option>`).join("")}</select></label>`).join("");
          for (const node of $d("#integration-mapping").querySelectorAll("select")) node.onchange = preview;
          if (unknown.length) return;
        }
        if (selected.some(v => !v)) return;
        const grouped = new Map();
        for (const [name, amount] of rows) {
          const mapped = match(name, project.filters) || project.filters[Number(selected[unresolved.indexOf(name)])]?.name;
          if (!mapped) continue;
          const entry = grouped.get(mapped) || {name: mapped, frames: 0, seconds: 0};
          entry.frames += amount.frames; entry.seconds += amount.seconds; grouped.set(mapped, entry);
        }
        const filters = [...grouped.values()].map(row => ({...row, seconds: Math.round(row.seconds)}));
        if (!filters.length || filters.some(r => r.frames > 100000 || r.seconds < 1)) throw Error("Controlla i filtri associati e la durata totale.");
        const pending = {day, batch, digest, filters};
        const latest = MeteoProjects.find(target.id);
        if (!latest) throw Error("Il progetto è cambiato. Riapri Chiudi serata.");
        const result = replace(latest, pending), prior = (latest.imports || []).find(r => same(r, pending));
        if (prior?.digest === digest && JSON.stringify(prior.filters) === JSON.stringify(filters)) {
          info.textContent = "Questo riepilogo è già stato registrato."; return;
        }
        $d("#integration-preview").innerHTML = MeteoDecision.table(["Filtro", "Pose valide", "Integrazione", "Totale dopo il salvataggio"], filters.map(f => [
          esc(f.name), num(f.frames, 0), num(f.seconds / 3600, 2) + " h",
          num(totals(result).filters.find(row => row.name === f.name)?.acquired_seconds / 3600, 2) + " h",
        ]));
        info.textContent = (prior ? "Sostituisce il riepilogo precedente per questa data e lotto. " : "") + (skipped ? skipped + " righe escluse per esito, data, soggetto o zero pose. " : "") + "Verifica il riepilogo prima di salvare.";
        receipt = pending; submit.disabled = false;
      } catch (error) { info.textContent = error.message; }
    }
    $d("#integration-file").onchange = async event => {
      const token = ++generation, file = event.target.files[0]; parsed = digest = null;
      $d("#integration-columns").replaceChildren(); $d("#integration-mapping").replaceChildren(); preview();
      if (!file) return;
      if (file.size > 262144) { info.textContent = "Usa un file fino a 256 KB."; return; }
      try {
        const bytes = await file.arrayBuffer();
        const input = parse(new TextDecoder().decode(bytes));
        const hash = [...new Uint8Array(await crypto.subtle.digest("SHA-256", bytes))].map(v => v.toString(16).padStart(2, "0")).join("");
        if (!dialog.isConnected || token !== generation) return;
        parsed = input; digest = hash;
        const defaults = suggest(parsed.headers), labels = {filter:"Filtro",frames:"Pose valide",seconds:"Secondi per posa",status:"Esito · facoltativo",target:"Soggetto · facoltativo",day:"Data serata · facoltativa"};
        $d("#integration-columns").innerHTML = Object.entries(labels).map(([key, label]) => `<label>${label}<select data-column="${key}">${key === "frames" ? '<option value="-2">Seleziona il conteggio</option>' : ""}<option value="-1">${key === "frames" ? "Una riga per posa" : "Nessuna colonna"}</option>${parsed.headers.map((h, i) => `<option value="${i}">${esc(h || "Colonna " + (i + 1))}</option>`).join("")}</select></label>`).join("");
        for (const node of $d("#integration-columns").querySelectorAll("select")) {
          const key = node.dataset.column; pick[key] = node; node.value = String(key === "frames" && defaults[key] < 0 ? -2 : defaults[key]);
          node.onchange = () => { $d("#integration-mapping").replaceChildren(); preview(); };
        }
        preview();
      } catch (error) { if (dialog.isConnected && token === generation) info.textContent = error.message; }
    };
    $d("#integration-day").onchange = preview; $d("#integration-batch").oninput = preview;
    dialog.querySelector("form").onsubmit = event => {
      event.preventDefault();
      if (!receipt) return;
      try { save(replace(MeteoProjects.find(target.id), receipt)); }
      catch (error) { info.textContent = error.message; }
    };
    dialog.querySelectorAll("[data-integration-remove]").forEach(button => button.onclick = () => {
      const prior = project.imports[Number(button.dataset.integrationRemove)], current = MeteoProjects.find(target.id);
      if (current && confirm("Rimuovere i totali importati per questa serata?")) {
        save({...current, imports: (current.imports || []).filter(row => !same(row, prior))});
      }
    });
  }
  globalThis.MeteoIntegration = { parse, suggest, counts, totals, replace, open, download };
})();
