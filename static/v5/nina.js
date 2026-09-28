/* Native advanced-sequencer JSON contract: N.I.N.A. 3.2, built-in entities only.
 * See docs/NINA_EXPORT.md for the upstream reference and runtime limitations. */
"use strict";
(() => {
  const numeric = v => v !== null && v !== undefined && v !== "" && Number.isFinite(Number(v));
  function wallTime(value, timezone) {
    const date = new Date(value);
    if (!Number.isFinite(date.getTime())) throw Error("Orario del piano non valido.");
    const parts = Object.fromEntries(new Intl.DateTimeFormat("en-GB", { timeZone: timezone, year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", second: "2-digit", hourCycle: "h23" }).formatToParts(date).map(p => [p.type, p.value]));
    const local = Date.UTC(+parts.year, +parts.month - 1, +parts.day, +parts.hour, +parts.minute, +parts.second);
    return { hours: +parts.hour, minutes: +parts.minute, seconds: +parts.second, date: `${parts.year}-${parts.month}-${parts.day}`, offset: local - Math.floor(date.getTime() / 1000) * 1000, night: Math.floor((local - 12 * 3600000) / 86400000) };
  }
  function sequence(plan, config, catalog, settings, now = Date.now()) {
    const blocks = plan?.schedule?.blocks || [];
    if (!blocks.length) throw Error("Nessun blocco utilizzabile: modifica notte, oggetti o soglie e ricalcola.");
    const age = now - Date.parse(plan.generated_at);
    if (!Number.isFinite(age) || age < -300000 || age > 20 * 60000) throw Error("Ricalcola il piano: il risultato deve avere meno di 20 minuti.");
    if (config.profile !== "deep_sky" || plan.profile !== "deep_sky") throw Error("L’esportazione N.I.N.A. richiede il profilo Fotografia cielo profondo.");
    const timezone = plan.timezone;
    if (typeof timezone !== "string" || !timezone) throw Error("Fuso del piano assente: ricalcola la sessione.");
    for (const [key, min, max, integer] of [["exposure", .1, 3600, false], ["gain", -1, 10000, true], ["offset", -1, 10000, true], ["binning", 1, 4, true]]) {
      const value = Number(settings[key]);
      if (!numeric(settings[key]) || value < min || value > max || (integer && !Number.isInteger(value))) throw Error("Parametri di ripresa non validi.");
    }
    if (!numeric(config.rotation) || config.rotation < 0 || config.rotation > 360) throw Error("Rotazione non valida.");
    const allTimes = blocks.flatMap(b => [b.prepare_start, b.start, b.end]).map(t => wallTime(t, timezone));
    if (new Set(allTimes.map(t => t.night)).size !== 1) throw Error("Esporta una sola notte, compresa tra un mezzogiorno e il successivo.");
    if (new Set(allTimes.map(t => t.offset)).size !== 1) throw Error("La notte attraversa un cambio di ora legale: imposta gli orari direttamente in N.I.N.A.");
    let previousEnd = Date.parse(plan.start);
    for (const b of blocks) {
      const prepare = Date.parse(b.prepare_start), start = Date.parse(b.start), end = Date.parse(b.end);
      if (![prepare, start, end, previousEnd].every(Number.isFinite) || prepare < previousEnd || start < prepare || start < now || end <= start || end > Date.parse(plan.end) || Number(settings.exposure) * 1000 > end - start) throw Error("Blocchi scaduti, sovrapposti o troppo brevi: ricalcola il piano.");
      previousEnd = end;
    }
    let nextId = 0;
    const object = (type, fields = {}, assembly = "NINA.Sequencer") => ({ $id: String(++nextId), $type: `${type}, ${assembly}`, ...fields });
    const ref = obj => ({ $ref: obj.$id });
    const collection = kind => object(`System.Collections.ObjectModel.ObservableCollection\`1[[NINA.Sequencer.${kind}, NINA.Sequencer]]`, { $values: [] }, "System.ObjectModel");
    const container = (type, name, parent = null) => object(`NINA.Sequencer.Container.${type}`, {
      Strategy: { $type: "NINA.Sequencer.Container.ExecutionStrategy.SequentialStrategy, NINA.Sequencer" }, Name: name,
      Conditions: collection("Conditions.ISequenceCondition"), IsExpanded: true, Items: collection("SequenceItem.ISequenceItem"), Triggers: collection("Trigger.ISequenceTrigger"), Parent: parent ? ref(parent) : null, ErrorBehavior: 1, Attempts: 1
    });
    const item = (parent, type, fields = {}) => object(`NINA.Sequencer.SequenceItem.${type}`, { ...fields, Parent: ref(parent), ErrorBehavior: 1, Attempts: 1 });
    const clockFields = stamp => {
      const t = wallTime(stamp, timezone);
      return { Hours: t.hours, Minutes: t.minutes, MinutesOffset: 0, Seconds: t.seconds, SelectedProvider: object("NINA.Sequencer.Utility.DateTimeProvider.TimeProvider") };
    };
    const coordinates = target => {
      if (!numeric(target?.ra_deg) || !numeric(target?.dec_deg) || target.ra_deg < 0 || target.ra_deg >= 360 || Math.abs(target.dec_deg) > 90) throw Error("Coordinate J2000 non disponibili nel catalogo.");
      const split = value => { const total = Math.round(value * 3600 * 1e5) / 1e5; return [Math.floor(total / 3600), Math.floor(total % 3600 / 60), +(total % 60).toFixed(5)]; };
      const ra = split(target.ra_deg / 15), dec = split(Math.abs(target.dec_deg));
      return object("NINA.Astrometry.InputCoordinates", { RAHours: ra[0] % 24, RAMinutes: ra[1], RASeconds: ra[2], NegativeDec: target.dec_deg < 0, DecDegrees: target.dec_deg < 0 ? -dec[0] : dec[0], DecMinutes: dec[1], DecSeconds: dec[2] }, "NINA.Astrometry");
    };
    const root = container("SequenceRootContainer", `Meteo Pro · ${allTimes[0].date} · ${timezone}`);
    const startArea = container("StartAreaContainer", "Start", root), targetArea = container("TargetAreaContainer", "Targets", root), endArea = container("EndAreaContainer", "End", root);
    root.Items.$values.push(startArea, targetArea, endArea);
    for (const block of blocks) {
      const target = catalog.find(t => t.name === block.target), coords = coordinates(target);
      const dso = container("DeepSkyObjectContainer", block.target, targetArea);
      dso.Target = object("NINA.Astrometry.InputTarget", { Expanded: true, TargetName: block.target, PositionAngle: Number(config.rotation), InputCoordinates: coords }, "NINA.Astrometry");
      dso.ExposureInfoListExpanded = false;
      dso.ExposureInfoList = object("NINA.Core.Utility.AsyncObservableCollection`1[[NINA.Sequencer.Utility.ExposureInfo, NINA.Sequencer]]", { $values: [] }, "NINA.Core");
      dso.Conditions.$values.push(object("NINA.Sequencer.Conditions.LoopCondition", { CompletedIterations: 0, Iterations: 1, Parent: ref(dso) }));
      dso.Conditions.$values.push(object("NINA.Sequencer.Conditions.TimeCondition", { ...clockFields(block.end), Parent: ref(dso) }));
      dso.Items.$values.push(item(dso, "Utility.WaitForTime", clockFields(block.prepare_start)));
      dso.Items.$values.push(item(dso, "Platesolving.Center", { Inherited: true, Coordinates: coordinates(target) }));
      dso.Items.$values.push(item(dso, "Utility.WaitForTime", clockFields(block.start)));
      const exposures = container("SequentialContainer", `LIGHT · ${block.target}`, dso);
      exposures.Conditions.$values.push(object("NINA.Sequencer.Conditions.TimeCondition", { ...clockFields(block.end), Parent: ref(exposures) }));
      exposures.Items.$values.push(item(exposures, "Imaging.TakeExposure", { ExposureTime: Number(settings.exposure), Gain: Number(settings.gain), Offset: Number(settings.offset), Binning: object("NINA.Core.Model.Equipment.BinningMode", { X: Number(settings.binning), Y: Number(settings.binning) }, "NINA.Core"), ImageType: "LIGHT", ExposureCount: 0 }));
      if (settings.meridianFlip) exposures.Triggers.$values.push(object("NINA.Sequencer.Trigger.MeridianFlip.MeridianFlipTrigger", { Parent: ref(exposures), TriggerRunner: container("SequentialContainer", null) }));
      dso.Items.$values.push(exposures); targetArea.Items.$values.push(dso);
    }
    return root;
  }
  function markup(plan) {
    return `<article class="card nina-export"><h2>Porta il piano in N.I.N.A.</h2><p>Sequenza avanzata con coordinate J2000, centratura e pose LIGHT negli intervalli del piano. Imposta esposizione e parametri camera prima di scaricarla.</p><button type="button" id="export-nina" class="quiet" ${!plan?.schedule?.blocks?.length || plan.profile !== "deep_sky" ? "disabled" : ""}>Esporta sequenza N.I.N.A.</button>${!plan?.schedule?.blocks?.length ? '<p class="metric-caption">Disponibile quando il piano contiene almeno un blocco utilizzabile.</p>' : plan.profile !== "deep_sky" ? '<p class="metric-caption">Scegli Fotografia cielo profondo per creare una sequenza di ripresa.</p>' : ""}</article>`;
  }
  function bind(plan, config, catalog) {
    const button = document.querySelector("#export-nina"); if (!button) return;
    button.onclick = () => {
      document.querySelector("#nina-dialog")?.remove();
      const dialog = document.createElement("dialog"); dialog.id = "nina-dialog";
      dialog.innerHTML = `<form id="nina-form"><div class="dialog-title"><h2>Esporta per N.I.N.A. 3.2</h2><button type="button" id="nina-close" class="quiet" aria-label="Chiudi esportazione">×</button></div><p>${esc(config.equipment?.name || "Strumento del piano")} · ${dateTime(plan.start)} – ${dateTime(plan.end)} · ${esc(plan.timezone)}.</p><div class="form-grid"><label>Esposizione · secondi<input id="nina-exposure" type="number" min="0.1" max="3600" step="0.1" value="180" required></label><label>Gain · −1 predefinito camera<input id="nina-gain" type="number" min="-1" max="10000" value="-1" required></label><label>Offset · −1 predefinito camera<input id="nina-offset" type="number" min="-1" max="10000" value="-1" required></label><label>Binning<select id="nina-binning">${[1, 2, 3, 4].map(n => `<option value="${n}">${n}×${n}</option>`).join("")}</select></label></div><label class="nina-confirm"><input id="nina-flip" type="checkbox" checked> Includi il flip al meridiano con le impostazioni del profilo N.I.N.A.</label><p>Apri il JSON nel sequenziatore avanzato per la notte indicata, con il PC impostato su ${esc(plan.timezone)}. Gli orari di N.I.N.A. sono locali e non memorizzano la data.</p><p>Prepara connessione, raffreddamento, filtro, fuoco e guida nel tuo profilo; completa le sezioni Start ed End. La rotazione ${num(config.rotation, 0)}° è annotata nel target e va impostata sull’attrezzatura. Il controllo meteo resta quello configurato in N.I.N.A.</p><label class="nina-confirm"><input id="nina-reviewed" type="checkbox" required> Ho verificato notte, fuso del PC e parametri di ripresa. Rivedrò la sequenza in N.I.N.A. prima dell’avvio.</label><button class="primary">Scarica JSON N.I.N.A.</button><p id="nina-status" role="status"></p></form>`;
      document.body.append(dialog); dialog.showModal(); document.querySelector("#nina-close").onclick = () => dialog.close();
      dialog.querySelector("form").onsubmit = e => {
        e.preventDefault();
        try {
          const settings = Object.fromEntries(["exposure", "gain", "offset", "binning"].map(k => [k, document.querySelector("#nina-" + k).value]));
          settings.meridianFlip = document.querySelector("#nina-flip").checked;
          const result = sequence(plan, config, catalog, settings);
          const url = URL.createObjectURL(new Blob([JSON.stringify(result, null, 2)], { type: "application/json" }));
          const a = document.createElement("a"); a.href = url; a.download = `MeteoPro_NINA_${wallTime(plan.start, plan.timezone).date}.json`; a.click(); URL.revokeObjectURL(url);
          document.querySelector("#nina-status").textContent = "Sequenza scaricata. Aprila nel sequenziatore avanzato e verifica il profilo dell’attrezzatura.";
        } catch (error) { document.querySelector("#nina-status").textContent = error.message; }
      };
    };
  }
  globalThis.MeteoNina = { sequence, wallTime, markup, bind };
})();
