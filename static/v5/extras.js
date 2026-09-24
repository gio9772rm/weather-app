/* V5 tools share the page's single weather refresh. Personal notes stay local. */
"use strict";
(() => {
  let animation = null, playing = false, frameIndex = 0, lastFrameAt = 0;
  let catalog = [], plan = null, city = null, cityId = "", cityQuery = "", cityBusy = false;
  let cityResults = [], editingEntry = null;
  const drafts = new Map();
  function captureDrafts(){
    document.querySelectorAll("#view form[data-draft-key]").forEach(form=>{
      const values=[...form.querySelectorAll("input,textarea,select")].filter(el=>el.type!=="file").map(el=>({id:el.id,name:el.name,value:el.value,checked:el.checked}));
      drafts.set(form.dataset.draftKey,values);
    });
  }
  function restoreDrafts(){
    document.querySelectorAll("#view form").forEach(form=>{
      const suffix=page==="journal"?(editingEntry||"new"):page==="activities"?activity:activeId;
      const id=page+":"+suffix;form.dataset.draftKey=id;
      for(const v of drafts.get(id)||[]){const el=[...form.querySelectorAll("input,textarea,select")].find(e=>v.id?e.id===v.id:e.name===v.name&&e.value===v.value);if(el){el.value=v.value;if(el.type==="checkbox")el.checked=v.checked;}}
    });
  }
  const key = name => "meteo.v5." + name;
  const local = (name, fallback) => storage.get(key(name), fallback);
  const save = (name, value) => {
    try { localStorage.setItem(key(name), JSON.stringify(value)); return true; }
    catch { notice("Spazio locale non disponibile: esporta una copia prima di continuare."); return false; }
  };
  const download = (name, text, type = "application/json") => {
    const url = URL.createObjectURL(new Blob([text], { type }));
    const a = document.createElement("a"); a.href = url; a.download = name; a.click();
    URL.revokeObjectURL(url);
  };
  async function api(action, values) {
    const r = await fetch("/api/v5/tools/" + action, values ? {
      method: "POST", headers: {"Content-Type":"application/json"}, body: JSON.stringify(values)
    } : {cache:"no-store"});
    const out = await r.json(); if (!r.ok) throw Error(out.error || "Strumento non disponibile"); return out;
  }
  const input = (id, label, value, min, max, step = 1) => `<label>${esc(label)}<input id="${id}" type="number" min="${min}" max="${max}" step="${step}" value="${esc(value)}" required></label>`;
  const table = (headers, rows) => `<div class="table-wrap"><table><thead><tr>${headers.map(h=>`<th>${h}</th>`).join("")}</tr></thead><tbody>${rows.map(r=>`<tr>${r.map(c=>`<td>${c}</td>`).join("")}</tr>`).join("")}</tbody></table></div>`;
  const empty = text => `<p class="empty">${esc(text)}</p>`;

  function uncertaintyCard() {
    const source = data.uncertainty || {}, now = Date.now();
    const rows = (source.rows || []).filter(r => r.variable === "temp_c" && r.member_count >= 5 &&
      now-Date.parse(r.issued_at) <= 12*3600000 && Date.parse(r.valid_time)>=now-3600000 &&
      finite(r.p10) && finite(r.p50) && finite(r.p90)).slice(0,72);
    if (!rows.length) return `<article class="card"><h2>Variabilità prevista</h2>${empty("Ensemble recente non disponibile per questa località. Nessuna fascia viene ricostruita dalle misure.")}</article>`;
    const width=900,height=240,left=48,right=16,top=15,bottom=35;
    const low=Math.min(...rows.map(r=>r.p10))-1, high=Math.max(...rows.map(r=>r.p90))+1;
    const first=Date.parse(rows[0].valid_time), last=Date.parse(rows.at(-1).valid_time)+1;
    const x=r=>left+(Date.parse(r.valid_time)-first)/(last-first)*(width-left-right);
    const y=v=>height-bottom-(v-low)/(high-low)*(height-top-bottom);
    const segments=[]; let part=[];
    for(const r of rows){if(part.length && Date.parse(r.valid_time)-Date.parse(part.at(-1).valid_time)>90*60000){segments.push(part);part=[];}part.push(r);}if(part.length)segments.push(part);
    let svg=`<svg class="chart uncertainty-chart" viewBox="0 0 ${width} ${height}" role="img" aria-label="Temperatura: mediana e intervallo dal decimo al novantesimo percentile ensemble">`;
    for(let i=0;i<4;i++){const v=low+(high-low)*i/3;svg+=`<path class="axis" d="M${left} ${y(v)}H${width-right}"/><text x="${left-7}" y="${y(v)+4}" text-anchor="end">${num(v)}°</text>`;}
    for(const segment of segments){const outline=[...segment.map(r=>`${x(r)},${y(r.p90)}`),...[...segment].reverse().map(r=>`${x(r)},${y(r.p10)}`)];svg+=`<polygon points="${outline.join(" ")}" fill="var(--teal)" opacity="0.18"/><polyline points="${segment.map(r=>`${x(r)},${y(r.p50)}`).join(" ")}" fill="none" stroke="var(--teal)" stroke-width="3"/>`;}
    for(const r of [rows[0],rows[Math.floor(rows.length/2)],rows.at(-1)])svg+=`<text x="${x(r)}" y="${height-9}" text-anchor="${r===rows[0]?"start":r===rows.at(-1)?"end":"middle"}">${dayLabel(r.valid_time)} ${clock(r.valid_time)}</text>`;
    svg+="</svg>";
    return `<article class="card"><h2>Quanto può variare la temperatura?</h2><div class="band-legend"><span class="band-key"></span>Fascia P10–P90 · <b>linea: mediana ensemble</b></div>${svg}<p>${esc(source.source)} · ${num(rows[0].member_count,0)} membri · acquisizione ${dateTime(rows[0].issued_at)}.</p><p class="metric-caption">La fascia racchiude l’80% centrale dei membri del modello. La sua copertura reale deve essere verificata: non è una probabilità di accuratezza né la previsione calibrata.</p></article>`;
  }
  function verificationTable() {
    const scores=(data.scores||[]).filter(s=>["temp_c","humidity","wind_kmh","pressure_hpa"].includes(s.variable));
    if(!scores.length)return empty("Verifica locale in raccolta.");
    const labels={temp_c:"Temperatura °C",humidity:"Umidità %",wind_kmh:"Vento km/h",pressure_hpa:"Pressione hPa"};
    return table(["Variabile / modello","Orizzonte","Errore medio","Validazione","Riferimento semplice","Correzione in prova","Campioni"],scores.slice(0,24).map(s=>[
      `${labels[s.variable]}<br><small>${esc(s.model)}</small>`,esc(s.horizon),num(s.mae),num(s.holdout_mae),num(s.persistence_mae),num(s.candidate_holdout_mae),num(s.n,0)
    ]))+`<p class="metric-caption">Valori più bassi indicano errori minori. Il riferimento mantiene la misura nota all’emissione. La correzione in prova usa solo il periodo di addestramento; a Comacchio non viene applicata automaticamente. Servono almeno 30 giornate e un vantaggio stabile sui dati di validazione.</p>`;
  }

  // Public city centres are deliberate map anchors, never the private station.
  function radarView() {
    const product=data.radar_animation||{}, frames=product.frames||[], d=data.radar?.[0]||{};
    const old=!product.fetched_at || Date.now()-Date.parse(product.fetched_at)>30*60000;
    return `<h1>Il movimento della pioggia</h1><p>Animazione osservata delle ultime due ore. Inquadratura regionale sul centro abitato; la posizione privata della stazione non è mostrata.</p><div class="grid"><article class="card span-8"><div class="card-header"><h2>Radar osservato</h2><span class="tag">RAINVIEWER${old?" · DATI DATATI":""}</span></div>${frames.length?`<div class="radar-map" id="radar-map" aria-label="Mappa radar regionale"><div id="radar-base"></div><img id="radar-image" alt="Eco di precipitazione del fotogramma selezionato"><img id="radar-coverage" alt="Maschera delle zone senza copertura radar"><span class="map-label">${data.station.role==="primary"?"Roma":"Comacchio"} · area regionale</span></div><div class="controls"><button id="radar-play" class="primary">Avvia animazione</button><label class="radar-slider">Fotogramma<input id="radar-frame" type="range" min="0" max="${frames.length-1}" value="${frames.length-1}" step="1"></label><output id="radar-time"></output></div><p id="radar-load" role="status"></p><p class="metric-caption">Orario di composizione: le scansioni radar possono avere orari diversi. Scuro: assenza di copertura. Trasparente con copertura: nessuna eco visibile, non garanzia di assenza di pioggia.</p>`:empty("Fotogrammi non disponibili. I valori DPC restano separati qui accanto.")}<p class="map-credit">Mappe © <a href="https://www.openstreetmap.org/copyright" target="_blank" rel="noopener">OpenStreetMap</a> · radar <a href="https://www.rainviewer.com" target="_blank" rel="noopener">RainViewer</a> · acquisizione ${dateTime(product.fetched_at)}</p></article><article class="card span-4"><span class="tag">MISURA RADAR · DPC</span><h2>Pioggia e fulmini locali</h2><p>${dateTime(d.observed_at||d.time)}</p><div class="metric">${num(d.sri_point_mm_h)}<small>mm/h · stima radar puntuale</small></div><ul class="fact-list">${[10,25,50].map(k=>`<li><span>Fulmini entro ${k} km</span><b>${num(d["lightning_"+k+"km"],0)}</b></li>`).join("")}</ul><p class="metric-caption">Il conteggio riguarda il prodotto disponibile, non i fulmini futuri. I valori mancanti restano non disponibili.</p></article></div><article class="card"><span class="tag">PREVISIONE MODELLISTICA</span><h2>Pioggia nelle prossime tre ore</h2>${table(["Ora","Quantità","Probabilità"],(data.forecast||[]).filter(f=>Date.parse(f.valid_time)+3600000>Date.now()).slice(0,3).map(f=>[clock(f.valid_time),num(f.rain_mm)+" mm",num(f.precip_probability,0)+"%"]))}<p>RainViewer non distribuisce più i fotogrammi futuri tramite questa API. Queste ore provengono dai modelli e non sono un’animazione radar prevista.</p></article>`;
  }
  function bindRadar(){
    const frames=data.radar_animation?.frames||[];if(!frames.length||!$("#radar-map"))return;
    const lat=data.station.role==="primary"?41.90:44.69,lon=data.station.role==="primary"?12.50:12.18,z=7;
    // Identical z/x/y bounds for OSM (256 px) and radar (512 px HiDPI).
    const tx=Math.floor((lon+180)/360*2**z),ty=Math.floor((1-Math.asinh(Math.tan(lat*Math.PI/180))/Math.PI)/2*2**z);
    $("#radar-base").innerHTML=`<img alt="" src="https://tile.openstreetmap.org/${z}/${tx}/${ty}.png" style="left:0;top:0;width:100%;height:100%">`;
    const tile=path=>`https://tilecache.rainviewer.com${path}/512/${z}/${tx}/${ty}`;
    let coverageReady=false;
    const coverageNotice=()=>coverageReady?"":"Copertura radar non verificabile: l’assenza di colore non prova assenza di pioggia.";
    const show=index=>{$("#radar-load").textContent="Caricamento fotogramma…";frameIndex=index;const f=frames[index];$("#radar-image").src=tile(f.path)+"/2/1_1.png";$("#radar-time").textContent=dateTime(f.time);$("#radar-frame").value=index;};
    $("#radar-image").onerror=()=>{$("#radar-load").textContent="Fotogramma non caricato. La mappa di base non indica cielo sereno.";};
    $("#radar-image").onload=()=>{$("#radar-load").textContent=coverageNotice();};
    $("#radar-coverage").onerror=()=>{coverageReady=false;$("#radar-load").textContent=coverageNotice();};
    $("#radar-coverage").onload=()=>{coverageReady=true;if($("#radar-image").complete&&$("#radar-image").naturalWidth)$("#radar-load").textContent="";};
    $("#radar-coverage").src=tile("/v2/coverage/0")+"/0/0_0.png";
    show(frames.length-1);
    $("#radar-frame").oninput=e=>show(Number(e.target.value));
    const tick=now=>{if(!playing||page!=="maps"||!$("#radar-image"))return;if(now-lastFrameAt>900){show((frameIndex+1)%frames.length);lastFrameAt=now;}animation=requestAnimationFrame(tick);};
    $("#radar-play").onclick=()=>{playing=!playing;$("#radar-play").textContent=playing?"Pausa":"Avvia animazione";if(playing)animation=requestAnimationFrame(tick);else cancelAnimationFrame(animation);};
  }

  function citiesView(){
    const favorites=local("cities",[]);
    return `<h1>Il meteo, dove vuoi</h1><p>Previsioni da internet per la città scelta. Le misure Ecowitt restano nelle sezioni delle due stazioni.</p><form id="city-search" class="card controls"><label>Città o CAP<input id="city-query" minlength="2" maxlength="80" value="${esc(cityQuery)}" placeholder="Es. Firenze o 50122" required></label><button class="primary">Cerca</button><span id="city-status" role="status"></span></form><div class="controls">${favorites.map((f,i)=>`<button class="quiet" data-city-favorite="${i}">${esc(f.label)}</button><button class="quiet" data-remove-city="${i}" aria-label="Rimuovi ${esc(f.label)} dai preferiti">×</button>`).join("")}</div><div id="city-results">${cityResults.map(r=>`<button class="city-choice" data-city="${r.id}">${esc(r.label)}</button>`).join("")}</div><section id="city-weather">${cityMarkup()}</section>`;
  }
  function cityMarkup(){
    if(!city)return empty("Cerca una città e seleziona il risultato corretto.");
    const fmt=v=>new Intl.DateTimeFormat("it-IT",{timeZone:city.timezone,day:"2-digit",month:"short",hour:"2-digit",minute:"2-digit"}).format(new Date(v));
    const rows=(city.hourly||[]).filter(f=>Date.parse(f.time||f.valid_time)>=Date.now()-3600000).slice(0,72);
    return `<article class="card"><div class="card-header"><h2>${esc(city.label)}</h2><button id="save-city" class="quiet">Salva nei preferiti</button></div><p>${esc(city.source)} · acquisizione ${fmt(city.fetched_at)} · orari ${esc(city.timezone)}</p>${table(["Ora locale","Temperatura","Umidità","Vento / raffica","Pioggia","Probabilità"],rows.map(r=>[fmt(r.time||r.valid_time),num(r.temp_c)+" °C",num(r.humidity,0)+"%",num(r.wind_kmh)+" / "+num(r.wind_gust_kmh)+" km/h",num(r.rain_mm)+" mm",num(r.precip_probability,0)+"%"]))}</article>`;
  }
  async function search(query){cityQuery=query;if($("#city-query"))$("#city-query").value=query;try{cityResults=(await api("cities?q="+encodeURIComponent(query))).results;render();if(!cityResults.length)$("#city-results").innerHTML=empty("Nessuna città trovata. Prova il nome del comune.");}catch(e){notice(e.message);}}
  async function loadCity(id){if(cityBusy)return;cityBusy=true;try{const value=await api("city?id="+encodeURIComponent(id));city=value;cityId=id;if(page==="cities"){const holder=$("#city-weather");if(holder){holder.innerHTML=cityMarkup();bindCitySave();}}}catch(e){notice(e.message);}finally{cityBusy=false;}}
  function bindCitySave(){if($("#save-city"))$("#save-city").onclick=()=>{const list=local("cities",[]).filter(f=>f.label!==city.label);list.push({label:city.label,query:cityQuery});save("cities",list.slice(-12));render();};}
  function bindCities(){
    $("#city-search").onsubmit=e=>{e.preventDefault();$("#city-status").textContent="Ricerca…";search($("#city-query").value);};
    document.querySelectorAll("[data-city]").forEach(b=>b.onclick=()=>loadCity(b.dataset.city));
    document.querySelectorAll("[data-city-favorite]").forEach(b=>b.onclick=()=>search(local("cities",[])[Number(b.dataset.cityFavorite)].query));
    document.querySelectorAll("[data-remove-city]").forEach(b=>b.onclick=()=>{save("cities",local("cities",[]).filter((_,i)=>i!==Number(b.dataset.removeCity)));render();});bindCitySave();
  }

  function plannerView(){
    const config=local("planner",{}), night=data.astronomy?.[prefs.profile]?.nights?.[0];
    const date=night?.date||new Date().toISOString().slice(0,10), tomorrow=new Date(Date.parse(date+"T12:00Z")+86400000).toISOString().slice(0,10);
    const eq={name:"Il mio strumento",telescope:"Rifrattore",camera:"Camera",aperture_mm:80,focal_length_mm:480,sensor_width_mm:23.5,sensor_height_mm:15.7,pixel_size_um:3.76,...config.equipment};
    return `<h1>Prepara la tua notte</h1><p>Target, ostacoli e campo inquadrato per ${esc(data.station.name)}. Le coordinate della stazione rimangono sul server.</p><form id="planner-form" class="card"><div class="form-grid"><label>Inizio · ${esc(data.station.timezone)}<input id="plan-start" type="datetime-local" value="${date}T21:00" required></label><label>Fine · stesso fuso<input id="plan-end" type="datetime-local" value="${tomorrow}T06:00" required></label>${input("plan-alt","Altezza minima °",config.altitude??25,0,85)}${input("plan-moon","Distanza minima Luna °",config.moon??30,0,180)}</div><fieldset><legend>Oggetti · massimo otto</legend><div id="plan-targets" class="target-grid">${catalog.length?catalog.map(t=>`<label><input type="checkbox" name="target" value="${esc(t.name)}" ${(config.targets||catalog.slice(0,3).map(x=>x.name)).includes(t.name)?"checked":""}>${esc(t.name)} <small>${esc(t.common_name)}</small></label>`).join(""):"Carico il catalogo…"}</div></fieldset><details><summary>Strumento e ostacoli locali</summary><div class="form-grid">${input("eq-aperture","Apertura mm",eq.aperture_mm,10,2000)}${input("eq-focal","Focale mm",eq.focal_length_mm,20,20000)}${input("eq-width","Sensore larghezza mm",eq.sensor_width_mm,1,80,0.01)}${input("eq-height","Sensore altezza mm",eq.sensor_height_mm,1,80,0.01)}${input("eq-pixel","Pixel µm",eq.pixel_size_um,0.5,30,0.01)}${input("eq-rotation","Rotazione °",config.rotation??0,0,360)}</div><label>Orizzonte misurato · azimut:altezza, separati da virgola<input id="plan-horizon" value="${esc(config.horizonText||"")}" placeholder="0:10, 90:25, 180:5, 270:20"></label><p class="metric-caption">0° nord, 90° est. Inserisci ostacoli rilevati dalla tua postazione; una mappa satellitare non ne certifica l’altezza.</p></details><button class="primary" id="calculate-plan">Calcola il piano</button><span id="plan-status" role="status"></span></form><section id="plan-result">${plan?.station_id===activeId?planMarkup():""}</section>`;
  }
  function planMarkup(){
    const rows=plan.summary||[];const targetNames=[...new Set(plan.tracks.map(r=>r.target))];const colors=["var(--teal)","var(--orange)","var(--blue)","#b17bd1","#d78c9c","#71974e","#837adb","#d59445"];
    return `<article class="card"><h2>La finestra utile per ogni oggetto</h2>${table(["Oggetto","Magnitudine","Ora migliore","Altezza massima","Distanza Luna","Ore visibili","Copertura meteo","Campo"],rows.map(r=>[esc(r.target)+" · "+esc(r.name),num(r.magnitude),r.best_time?clock(r.best_time):esc(r.status),num(r.max_altitude,0)+"°",num(r.moon_separation,0)+"°",num(r.visible_hours)+" h",num(r.weather_coverage,0)+"%",esc(r.framing)]))}<p class="metric-caption">Ore visibili: sopra gli ostacoli e la soglia geometrica. Non equivalgono alle ore di bel tempo; consulta anche qualità meteo e copertura.</p>${chart(targetNames.map((name,i)=>({name,color:colors[i],points:plan.tracks.filter(r=>r.target===name).map(r=>({t:r.valid_time,v:r.altitude}))})),{unit:"°",gapMs:20*60000})}<button id="export-plan" class="quiet">Esporta piano CSV</button><button id="plan-to-journal" class="quiet">Apri il diario della sessione</button></article>${plan.field?`<article class="card"><h2>Il campo inquadrato</h2><p>${num(plan.field.width_deg,2)}° × ${num(plan.field.height_deg,2)}° · campionamento ${num(plan.field.image_scale_arcsec_px,2)}″/pixel</p><div class="grid">${Object.entries(plan.geometry).map(([name,g])=>`<div class="span-4"><h3>${esc(name)}</h3>${geometrySvg(g)}</div>`).join("")}</div><p class="metric-caption">Sagoma indicativa degli oggetti del catalogo. Non è una fotografia né include la distorsione dell’ottica.</p></article>`:""}`;
  }
  function geometrySvg(g){const all=[...g.sensor_x,...g.sensor_y,...g.target_x,...g.target_y],m=Math.max(1,...all.map(Math.abs))*1.15;const xy=(x,y)=>`${150+x/m*135},${150-y/m*135}`;return `<svg class="fov" viewBox="0 0 300 300" role="img" aria-label="Campo sensore blu e ingombro oggetto arancione"><polyline points="${g.sensor_x.map((x,i)=>xy(x,g.sensor_y[i])).join(" ")}" fill="none" stroke="var(--blue)" stroke-width="2"/><polyline points="${g.target_x.map((x,i)=>xy(x,g.target_y[i])).join(" ")}" fill="none" stroke="var(--orange)" stroke-width="2"/></svg><p class="metric-caption">Blu: sensore · arancione: oggetto</p>`;}
  async function bindPlanner(){
    if(!catalog.length){try{catalog=(await api("catalog")).targets;if(page==="planner")render();}catch(e){notice(e.message);}return;}
    $("#planner-form").onsubmit=async e=>{e.preventDefault();const b=$("#calculate-plan");b.disabled=true;try{
      const horizonText=$("#plan-horizon").value;const horizon={};for(const pair of horizonText.split(",").filter(x=>x.trim())){const parts=pair.trim().split(":");if(parts.length!==2||!parts.every(finite))throw Error("Orizzonte: usa azimut:altezza, per esempio 90:25.");horizon[Number(parts[0])]=Number(parts[1]);}
      const values={station_id:activeId,profile:prefs.profile,start:$("#plan-start").value,end:$("#plan-end").value,targets:[...document.querySelectorAll('[name="target"]:checked')].map(x=>x.value),altitude:Number($("#plan-alt").value),moon:Number($("#plan-moon").value),rotation:Number($("#eq-rotation").value),horizon,equipment:{name:"Il mio strumento",telescope:"Ottica personale",camera:"Camera personale",aperture_mm:Number($("#eq-aperture").value),focal_length_mm:Number($("#eq-focal").value),sensor_width_mm:Number($("#eq-width").value),sensor_height_mm:Number($("#eq-height").value),pixel_size_um:Number($("#eq-pixel").value)}};
      if(!values.targets.length||values.targets.length>8)throw Error("Seleziona da uno a otto oggetti.");
      save("planner",{...values,horizonText});$("#plan-status").textContent="Calcolo…";const result=await api("planner",values);plan=result;if(page==="planner"&&activeId===values.station_id){$("#plan-result").innerHTML=planMarkup();bindPlanResult();$("#plan-status").textContent="Piano aggiornato";}
    }catch(error){notice(error.message);}finally{b.disabled=false;}};bindPlanResult();
  }
  function bindPlanResult(){if($("#export-plan"))$("#export-plan").onclick=()=>{const cols=["target","valid_time","altitude","azimuth","moon_separation","planner_score","weather_available"];download("meteo-piano-notte.csv",cols.join(",")+"\n"+plan.tracks.map(r=>cols.map(c=>JSON.stringify(r[c]??"")).join(",")).join("\n"),"text/csv");};if($("#plan-to-journal"))$("#plan-to-journal").onclick=()=>navigate("journal");}

  function journalView(){
    const list=local("journal",[]), entry=list.find(r=>r.id===editingEntry)||{};
    return `<h1>Il diario delle tue notti</h1><p>Note private su questo dispositivo. Esporta una copia per conservarle o trasferirle; non vengono pubblicate sul sito.</p><form id="journal-form" class="card"><div class="form-grid"><label>Data della sessione<input id="journal-date" type="date" value="${esc(entry.date||data.astronomy?.[prefs.profile]?.nights?.[0]?.date||new Date().toISOString().slice(0,10))}" required></label><label>Oggetto / sessione<input id="journal-target" maxlength="120" value="${esc(entry.target||plan?.summary?.[0]?.target||"")}" required></label><label>Qualità osservata<select id="journal-quality">${[["","Da registrare"],["1","1 · Scarsa"],["2","2 · Limitata"],["3","3 · Discreta"],["4","4 · Buona"],["5","5 · Ottima"]].map(([v,l])=>`<option value="${v}" ${String(entry.quality??"")===v?"selected":""}>${l}</option>`).join("")}</select></label></div><label>Note<textarea id="journal-notes" maxlength="4000" rows="4" placeholder="Nuvole reali, condensa, trasparenza, sessione riuscita…">${esc(entry.notes||"")}</textarea></label><button class="primary">${editingEntry?"Salva modifiche":"Registra sessione"}</button>${editingEntry?'<button type="button" id="journal-cancel" class="quiet">Annulla modifica</button>':""}<p class="metric-caption">La previsione viene associata solo a una sessione ancora futura nella stessa notte. Le note osservate non modificano automaticamente il punteggio meteo.</p></form><div class="controls"><button id="journal-export" class="quiet">Esporta diario JSON</button><label class="file-label">Importa una copia<input id="journal-import" type="file" accept=".json,application/json"></label></div>${list.length?[...list].reverse().map(r=>`<article class="card"><div class="card-header"><h2>${esc(r.target)}</h2><span class="tag">${esc(r.station_name||r.station_id)}</span></div><p>${esc(r.date)} · osservata: ${finite(r.quality)?num(r.quality,0)+"/5":"da registrare"} · prevista: ${finite(r.forecast?.score)?num(r.forecast.score,0)+"/100":"non archiviata"}</p>${r.forecast?`<p class="metric-caption">Previsione salvata il ${dateTime(r.forecast.captured_at)} · nuvole ${num(r.forecast.clouds,0)}% · ore favorevoli residue ${num(r.forecast.good_hours)}.</p>`:""}<p class="journal-text">${esc(r.notes)}</p><button class="quiet" data-edit-entry="${esc(r.id)}">Modifica</button><button class="quiet" data-delete-entry="${esc(r.id)}">Elimina</button></article>`).join(""):empty("Il tuo diario è pronto per la prima sessione.")}`;
  }
  function bindJournal(){
    $("#journal-form").onsubmit=e=>{e.preventDefault();const entries=local("journal",[]),previous=entries.find(r=>r.id===editingEntry),date=$("#journal-date").value;
      const night=data.astronomy?.[prefs.profile]?.nights?.find(n=>n.date===date),isFuture=night && date>=new Intl.DateTimeFormat("en-CA",{timeZone:data.station.timezone}).format(new Date());
      const entry={id:previous?.id||crypto.randomUUID(),date,target:$("#journal-target").value.trim(),quality:$("#journal-quality").value===""?null:Number($("#journal-quality").value),notes:$("#journal-notes").value,station_id:previous?.station_id||activeId,station_name:previous?.station_name||data.station.name,forecast:(previous?.date===date?previous.forecast:null)||(isFuture?{captured_at:data.generated_at,score:night.score,clouds:night.clouds_mean,good_hours:night.remaining_good_hours}:null)};
      if(save("journal",[...entries.filter(r=>r.id!==entry.id),entry].slice(-500))){drafts.delete("journal:"+(editingEntry||"new"));$("#journal-form").removeAttribute("data-draft-key");editingEntry=null;render();}};
    if($("#journal-cancel"))$("#journal-cancel").onclick=()=>{editingEntry=null;render();};
    document.querySelectorAll("[data-edit-entry]").forEach(b=>b.onclick=()=>{editingEntry=b.dataset.editEntry;render();$("#journal-target").focus();});
    document.querySelectorAll("[data-delete-entry]").forEach(b=>b.onclick=()=>{if(confirm("Eliminare questa sessione dal diario locale?")){save("journal",local("journal",[]).filter(r=>r.id!==b.dataset.deleteEntry));render();}});
    $("#journal-export").onclick=()=>download("meteo-diario.json",JSON.stringify({format:"meteo-journal-v1",entries:local("journal",[])},null,2));
    $("#journal-import").onchange=async e=>{try{const file=e.target.files[0];if(!file||file.size>1500000)throw Error("Copia troppo grande: massimo 1,5 MB.");const doc=JSON.parse(await file.text());if(doc.format!=="meteo-journal-v1"||!Array.isArray(doc.entries)||doc.entries.length>500)throw Error("Formato del diario non valido.");const valid=doc.entries.map(r=>{if(!/^[\w-]{1,80}$/.test(r.id)||!/^\d{4}-\d{2}-\d{2}$/.test(r.date)||typeof r.target!=="string"||typeof r.notes!=="string"||r.target.length>120||r.notes.length>4000||!(r.quality===null||[1,2,3,4,5].includes(r.quality)))throw Error("Una sessione contiene valori non validi.");return {id:r.id,date:r.date,target:r.target,notes:r.notes,quality:r.quality,station_id:String(r.station_id||"").slice(0,80),station_name:String(r.station_name||"").slice(0,120),forecast:r.forecast&&typeof r.forecast==="object"?{score:finite(r.forecast.score)?Number(r.forecast.score):null,clouds:finite(r.forecast.clouds)?Number(r.forecast.clouds):null,good_hours:finite(r.forecast.good_hours)?Number(r.forecast.good_hours):null,captured_at:Number.isFinite(Date.parse(r.forecast.captured_at))?new Date(r.forecast.captured_at).toISOString():null}:null};});const merged=new Map(local("journal",[]).map(r=>[r.id,r]));valid.forEach(r=>{if(!merged.has(r.id))merged.set(r.id,r);});if(merged.size>500)throw Error("Massimo 500 sessioni: esporta e riordina il diario.");if(save("journal",[...merged.values()]))render();}catch(error){notice(error.message);}};
  }

  function recordEvents(snapshot){
    const rules=local("event-rules",{pop:60,gust:40}), now=Date.now();
    if(now-Date.parse(snapshot.generated_at)>30*60000)return;
    const future=(snapshot.forecast||[]).filter(f=>Date.parse(f.valid_time)+3600000>now&&Date.parse(f.valid_time)<now+6*3600000);
    const forecastFresh=future.length && now-Date.parse(future[0].issued_at)<3*3600000;
    const night=snapshot.astronomy?.[prefs.profile]?.nights?.[0];
    const states={rain:forecastFresh&&future.every(f=>finite(f.precip_probability))?future.some(f=>f.precip_probability>=rules.pop):null,
      wind:forecastFresh&&future.every(f=>finite(f.wind_gust_kmh))?future.some(f=>f.wind_gust_kmh>=rules.gust):null,
      station:snapshot.observed_at?now-Date.parse(snapshot.observed_at)>20*60000:null,
      astronomy:forecastFresh&&night?night.remaining_good_hours>=2:null};
    const reasons={rain:`Probabilità di pioggia almeno ${rules.pop}% nelle prossime sei ore`,wind:`Raffiche previste almeno ${rules.gust} km/h nelle prossime sei ore`,station:"Ultima misura più vecchia di 20 minuti",astronomy:"Almeno due ore astronomiche favorevoli residue"};
    let events=local("events",[]);let changed=false;
    for(const [kind,active] of Object.entries(states)){if(active===null)continue;const current=events.find(e=>e.station_id===snapshot.station.id&&e.kind===kind&&!e.closed_at);
      if(active&&!current){events.push({id:crypto.randomUUID(),station_id:snapshot.station.id,station_name:snapshot.station.name,kind,reason:reasons[kind],opened_at:snapshot.generated_at,closed_at:null});changed=true;}
      else if(!active&&current){current.closed_at=snapshot.generated_at;changed=true;}}
    if(changed)save("events",events.slice(-200));
  }
  function inboxView(){const events=local("events",[]).filter(e=>e.station_id===activeId),rules=local("event-rules",{pop:60,gust:40});return `<h1>Il centro avvisi</h1><p>Registro delle condizioni rilevate durante l’uso del dispositivo. La ricezione delle notifiche push viene mostrata separatamente; non prova che tu le abbia lette.</p><form id="event-rules" class="card controls">${input("event-pop","Soglia pioggia %",rules.pop,30,100)}${input("event-gust","Soglia raffica km/h",rules.gust,20,150)}<button class="quiet">Salva soglie del registro</button><button type="button" data-go="notifications" class="primary">Gestisci notifiche push</button></form><section class="card"><h2>Eventi della località</h2>${events.length?table(["Evento","Rilevato","Stato"],[...events].reverse().map(e=>[esc(e.reason),dateTime(e.opened_at),e.closed_at?"Rientrato · "+dateTime(e.closed_at):"Rilevato attivo all’ultimo controllo"])):empty("Nessun evento registrato per questa località.")}<p class="metric-caption">Un’interruzione dei dati non chiude un evento. Il registro conserva gli ultimi 200 eventi sul dispositivo.</p></section><article class="card"><h2>Push ricevuti su questo dispositivo</h2><div id="received-push">Lettura registro…</div></article>`;}
  async function bindInbox(){ $("#event-rules").onsubmit=e=>{e.preventDefault();save("event-rules",{pop:Number($("#event-pop").value),gust:Number($("#event-gust").value)});recordEvents(data);render();};
    try{const response=await (await caches.open("meteo-v5-notifications")).match("/__meteo_notifications__");const rows=response?await response.json():[];if($("#received-push"))$("#received-push").innerHTML=rows.length?table(["Titolo","Messaggio","Ricevuto"],[...rows].reverse().map(r=>[esc(r.title),esc(r.body),dateTime(r.received_at)])):empty("Nessun push ricevuto e archiviato da questa versione.");}catch{if($("#received-push"))$("#received-push").textContent="Registro push non disponibile nel browser.";}}

  const activityDefaults={walk:{name:"Passeggiata",min:5,max:30,wind:20,gust:35,pop:30,rain:0.1,hours:1,daylight:true},bike:{name:"Bicicletta",min:8,max:28,wind:15,gust:25,pop:20,rain:0.1,hours:2,daylight:true},outdoor:{name:"Lavori all’aperto",min:5,max:32,wind:20,gust:35,pop:25,rain:0.1,hours:2,daylight:true}};
  let activity=local("activity-choice","walk");if(!activityDefaults[activity])activity="walk";
  function activityWindows(forecast,rule,now=Date.now()){
    const windows=[];let incomplete=0;for(const f of forecast){const start=Math.max(now,Date.parse(f.valid_time)),end=Date.parse(f.valid_time)+(finite(f.interval_hours)?Math.min(1,f.interval_hours):1)*3600000;if(end<=start||start>now+72*3600000)continue;
      if(![f.temp_c,f.wind_kmh,f.wind_gust_kmh,f.precip_probability,f.rain_mm].every(finite)||(rule.daylight&&!finite(f.is_day))){incomplete++;continue;}
      if(f.temp_c<rule.min||f.temp_c>rule.max||f.wind_kmh>rule.wind||f.wind_gust_kmh>rule.gust||f.precip_probability>rule.pop||f.rain_mm>rule.rain||(rule.daylight&&!f.is_day))continue;
      const last=windows.at(-1);if(last&&start===last.end)last.end=end;else windows.push({start,end});}
    return {windows:windows.filter(w=>(w.end-w.start)/3600000>=rule.hours),incomplete};
  }
  function activitiesView(){const rule={...activityDefaults[activity],...local("activity-rules",{})[activity]};const result=activityWindows(data.forecast||[],rule);return `<h1>Il momento giusto per uscire</h1><p>Finestre nelle prossime 72 ore, secondo le tue soglie. Il riepilogo principale conserva aria, pollini e astronomia.</p><div class="controls"><label>Attività<select id="activity-type">${Object.entries(activityDefaults).map(([id,r])=>`<option value="${id}" ${id===activity?"selected":""}>${r.name}</option>`).join("")}</select></label></div><form id="activity-form" class="card"><div class="form-grid">${input("act-min","Temperatura minima °C",rule.min,-30,45)}${input("act-max","Temperatura massima °C",rule.max,-20,50)}${input("act-wind","Vento massimo km/h",rule.wind,0,100)}${input("act-gust","Raffica massima km/h",rule.gust,0,150)}${input("act-pop","Probabilità massima %",rule.pop,0,100)}${input("act-rain","Pioggia massima mm/h",rule.rain,0,20,0.1)}${input("act-hours","Durata minima ore",rule.hours,0.5,12,0.5)}</div><label><input id="act-daylight" type="checkbox" ${rule.daylight?"checked":""}> Solo ore diurne</label><button class="primary">Trova le finestre</button></form><div class="grid">${result.windows.length?result.windows.slice(0,12).map(w=>`<article class="card span-4"><span class="tag">${esc(rule.name)}</span><h2>${dayLabel(new Date(w.start).toISOString())}</h2><div class="metric">${clock(new Date(w.start).toISOString())}–${clock(new Date(w.end).toISOString())}</div><p>${num((w.end-w.start)/3600000)} ore continue entro le soglie</p></article>`).join(""):empty("Nessuna finestra continua soddisfa tutte le soglie nei dati disponibili.")}</div><p class="metric-caption">${result.incomplete} intervalli esclusi per dati incompleti. Le ore mancanti interrompono la finestra. Indicazione meteo, da affiancare ai bollettini ufficiali.</p>`;}
  function bindActivities(){ $("#activity-type").onchange=e=>{activity=e.target.value;save("activity-choice",activity);render();};$("#activity-form").onsubmit=e=>{e.preventDefault();const rule={name:activityDefaults[activity].name,daylight:$("#act-daylight").checked};for(const field of ["min","max","wind","gust","pop","rain","hours"])rule[field]=Number($("#act-"+field).value);if(rule.min>rule.max){notice("La temperatura minima deve essere inferiore alla massima.");return;}save("activity-rules",{...local("activity-rules",{}),[activity]:rule});render();};}

  const views = new Map([["maps",radarView],["cities",citiesView],["planner",plannerView],["journal",journalView],["inbox",inboxView],["activities",activitiesView]]);
  const bindings = new Map([["maps",bindRadar],["cities",bindCities],["planner",bindPlanner],["journal",bindJournal],["inbox",bindInbox],["activities",bindActivities]]);
  window.MeteoExtra={
    view:p=>views.get(p)?.(),
    bind:p=>{restoreDrafts();return bindings.get(p)?.();},
    beforeRender:()=>{captureDrafts();playing=false;if(animation)cancelAnimationFrame(animation);},
    refreshCity:()=>{if(page==="cities"&&cityId)loadCity(cityId);},
    recordEvents,uncertaintyCard,verificationTable,activityWindows
  };
})();
