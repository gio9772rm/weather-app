/* Expert maps in the simple UI. City centres only; no extra weather timer. */
"use strict";
(() => {
  const centres = {
    primary: {name: "Roma", lat: "41.90", lon: "12.50"},
    secondary: {name: "Comacchio", lat: "44.69", lon: "12.18"},
  };
  const layers = {
    radar: {label: "Radar precipitazioni", product: "radar", kind: "OSSERVAZIONE", note: "Precipitazioni osservate. Usa la linea temporale della mappa per rivederne il movimento. L’assenza di colore può dipendere dalla copertura: non garantisce assenza di pioggia."},
    satellite: {label: "Satellite", product: "satellite", kind: "OSSERVAZIONE", note: "Immagini satellitari delle nubi. Orario e animazione sono indicati all’interno della mappa; la copertura nuvolosa non misura la pioggia al suolo."},
    clouds: {label: "Nuvole previste", product: "ecmwf", kind: "PREVISIONE · ECMWF", note: "Nuvolosità prevista dal modello ECMWF. Sposta la linea temporale per consultare le ore successive; questi valori non sono immagini osservate."},
  };
  let layer = "radar";
  function embedUrl(role, selected) {
    if (!Object.hasOwn(centres, role) || !Object.hasOwn(layers, selected)) return null;
    const centre = centres[role], info = layers[selected];
    if (!centre || !info) return null;
    return "https://embed.windy.com/embed.html?" + new URLSearchParams({
      type: "map", location: "coordinates", metricRain: "mm", metricTemp: "°C",
      metricWind: "km/h", zoom: "7", overlay: selected, product: info.product,
      level: "surface", lat: centre.lat, lon: centre.lon,
    });
  }
  function status(value, now = Date.now()) {
    const stamp = Date.parse(value), age = (now - stamp) / 60000;
    if (!Number.isFinite(stamp) || age < -5) return "Orario non disponibile";
    return age > 30 ? "Dato datato" : "Dato recente";
  }
  function frame() {
    const url = embedUrl(data.station.role, layer);
    if (!url) return '<p class="empty">Mappa non disponibile per questa località.</p>';
    if (offline || !navigator.onLine) return '<p class="empty">Le mappe interattive richiedono la connessione. I riepiloghi salvati restano consultabili con i loro orari.</p>';
    return `<iframe id="expert-radar" class="expert-radar" src="${esc(url)}" title="${esc(layers[layer].label)} · ${esc(centres[data.station.role].name)} · Windy" loading="lazy" referrerpolicy="origin" allowfullscreen></iframe>`;
  }
  function view() {
    return `<article class="card expert-radar-card">
      <div class="card-header"><h2>Le mappe della vista esperto</h2><span id="radar-kind" class="tag">${layers[layer].kind}</span></div>
      <div class="controls"><label for="radar-layer">Mappa<select id="radar-layer">${Object.entries(layers).map(([key, info]) => `<option value="${key}" ${key === layer ? "selected" : ""}>${info.label}</option>`).join("")}</select></label><button id="radar-fullscreen" class="quiet" type="button">Schermo intero</button></div>
      <p id="radar-description">${layers[layer].note}</p>
      <div id="expert-radar-holder" class="expert-radar-holder">${frame()}</div>
      <p id="radar-map-status" role="status"></p>
      <p class="metric-caption">Mappe interattive Windy centrate sul comune, con zoom e linea temporale propri. Consulta l’orario nella mappa; i riepiloghi dell’app seguono il ciclo di 10 minuti.</p>
      <div class="controls"><a id="radar-external" href="${esc(embedUrl(data.station.role, layer) || "https://www.windy.com/")}" target="_blank" rel="noopener noreferrer">Apri la mappa in una nuova scheda ↗</a>${proAnchor("radar", "Scheda esperto completa")}</div>
    </article>`;
  }
  function bind() {
    if (!$("#radar-layer")) return;
    const attach = () => {
      const iframe = $("#expert-radar"), message = $("#radar-map-status");
      // Cross-origin load events cannot establish whether map tiles are valid.
      message.textContent = iframe ? "Se la mappa non compare, aprila in una nuova scheda." : "";
      $("#radar-fullscreen").disabled = !iframe || !document.fullscreenEnabled;
      if (iframe) iframe.onerror = () => {message.textContent = "Mappa non caricata. Puoi aprirla in una nuova scheda.";};
    };
    $("#radar-layer").onchange = event => {
      if (!Object.hasOwn(layers, event.target.value)) return;
      layer = event.target.value;
      $("#radar-kind").textContent = layers[layer].kind;
      $("#radar-description").textContent = layers[layer].note;
      $("#expert-radar-holder").innerHTML = frame();
      $("#radar-external").href = embedUrl(data.station.role, layer) || "https://www.windy.com/";
      attach();
    };
    $("#radar-fullscreen").onclick = async () => {
      try { await $("#expert-radar-holder").requestFullscreen(); }
      catch { $("#radar-map-status").textContent = "Schermo intero non disponibile: usa il collegamento alla mappa."; }
    };
    attach();
  }
  function dpcCard(d = {}) {
    const sriTime = d.sri_observed_at || d.observed_at || d.time;
    const validLightning = Number.isFinite(Date.parse(d.lightning_observed_at));
    const lightning = key => validLightning ? num(d[key], key === "nearest_lightning_km" ? 1 : 0) : "—";
    return `<article class="card span-4"><span class="tag">MISURA RADAR · DPC</span><h2>Pioggia e fulmini locali</h2>
      <p>${esc(status(sriTime))} · ${dateTime(sriTime)}</p><div class="metric">${num(d.sri_point_mm_h)}<small>mm/h · stima radar puntuale</small></div>
      <ul class="fact-list"><li><span>Massimo nel ritaglio</span><b>${num(d.sri_max_mm_h)} mm/h</b></li><li><span>Riflettività sul punto</span><b>${num(d.vmi_point_dbz)} dBZ</b></li>${[10,25,50].map(k => `<li><span>Fulmini entro ${k} km</span><b>${lightning("lightning_" + k + "km")}</b></li>`).join("")}<li><span>Fulmine più vicino</span><b>${lightning("nearest_lightning_km")} km</b></li></ul>
      <p class="metric-caption">Riflettività: ${esc(status(d.vmi_observed_at))} · ${dateTime(d.vmi_observed_at)}.<br>Fulmini: ${validLightning ? esc(status(d.lightning_observed_at)) + " · " + dateTime(d.lightning_observed_at) : "fotogramma non disponibile"}. Conteggi riferiti al prodotto osservato, non a fulmini futuri.</p>
      <a href="https://radar.protezionecivile.gov.it/" target="_blank" rel="noopener noreferrer">Mappa ufficiale DPC ↗</a></article>`;
  }
  window.MeteoRadar = {view, bind, dpcCard, embedUrl, status};
})();
