/* Coordinated SVG charts: shared UTC range/cursor, keyboard and touch controls. */
"use strict";
(() => {
  let station="", bounds=null, range=null, cursor=null, series=[];
  const specs=[["temp_c","Temperatura","°C"],["wind_kmh","Vento","km/h"],["rain_mm","Pioggia oraria","mm"],["clouds","Nuvole","%"]];
  const at = t => `${dayLabel(new Date(t).toISOString())} ${clock(new Date(t).toISOString())}`;
  const points=(rows,time,key)=>rows.map(r=>({t:Date.parse(r[time]),v:finite(r[key])?Number(r[key]):null,known:r.known_at})).filter(p=>Number.isFinite(p.t)).sort((a,b)=>a.t-b.t);
  function prepare(){
    if(station!==activeId){station=activeId;range=null;cursor=null;}
    bounds=[Date.now()-24*3600000,Date.now()+72*3600000];
    if(!range)range=[Date.now()-6*3600000,Date.now()+24*3600000];
    series=specs.map(([key,label,unit])=>({key,label,unit,lines:[
      {name:"Ecowitt osservata",color:"var(--teal)",gap:key==="rain_mm"?3600000:20*60000,points:points(key==="rain_mm"?(data.observed_hourly||[]):data.observations||[],"time",key)},
      {name:"Previsione pubblicata prima dell’ora",color:"var(--orange)",gap:3600000,dash:true,points:points(data.timeline_history||[],"valid_time",key)},
      {name:"Previsione attuale",color:"var(--blue)",gap:3600000,dash:true,points:points(data.forecast||[],"valid_time",key)}
    ]}));
  }
  function plot(s){
    const values=s.lines.flatMap(l=>l.points).filter(p=>p.t>=range[0]&&p.t<=range[1]&&p.v!==null).map(p=>p.v);
    let lo=values.length?Math.min(...values):0,hi=values.length?Math.max(...values):1;
    if(s.key==="rain_mm"||s.key==="clouds")lo=0;
    if(s.key==="clouds")hi=100;
    if(lo===hi){lo-=s.key==="temp_c"?1:0;hi+=1;}
    const x=t=>48+(t-range[0])/(range[1]-range[0])*840,y=v=>160-(v-lo)/(hi-lo)*140;
    let content="";
    for(let i=0;i<4;i++){const v=lo+(hi-lo)*i/3;content+=`<path class="axis" d="M48 ${y(v)}H888"/><text x="40" y="${y(v)+4}" text-anchor="end">${num(v,s.key==="clouds"?0:1)}</text>`;}
    for(let i=0;i<4;i++){const t=range[0]+(range[1]-range[0])*i/3;content+=`<text x="${x(t)}" y="188" text-anchor="${i===0?"start":i===3?"end":"middle"}">${esc(clock(new Date(t).toISOString()))}</text>`;}
    for(const l of s.lines){let path="",previous=null;for(const p of l.points){if(p.t<range[0]||p.t>range[1]||p.v===null){previous=null;continue;}path+=`${previous&&p.t-previous.t<=l.gap?"L":"M"}${x(p.t).toFixed(2)},${y(p.v).toFixed(2)} `;previous=p;}content+=`<path d="${path}" fill="none" stroke="${l.color}" stroke-width="2.6" ${l.dash?'stroke-dasharray="6 4"':""}/>`;}
    content+=`<path class="shared-cursor" d="M${x(cursor??range[0])} 14V165" stroke="var(--ink)" opacity=".55"/><rect class="range-selection" y="15" height="150" fill="var(--blue)" opacity=".15" width="0"/>`;
    return `<article class="card"><div class="card-header"><h2>${s.label} · ${s.unit}</h2><output data-timeline-readout="${s.key}"></output></div><svg data-timeline="${s.key}" class="chart linked-chart" viewBox="0 0 900 200" tabindex="0" role="img" aria-label="${esc(s.label)}. Frecce per spostare il cursore, più e meno per zoomare.">${content}</svg></article>`;
  }
  function view(){prepare();return `<h1>Il tempo, sulla stessa linea</h1><p>Cursore e intervallo comuni a temperatura, vento, pioggia e nuvole. Trascina sul grafico per selezionare un intervallo; usa due dita per lo zoom. Le frecce da tastiera spostano il cursore.</p><div class="controls"><label>Zoom attorno al cursore<select id="timeline-span">${[3,6,12,24,48,96].map(h=>`<option value="${h}" ${Math.abs((range[1]-range[0])/3600000-h)<.1?"selected":""}>${h} ore</option>`).join("")}</select></label><button class="quiet" id="timeline-back">← Prima</button><button class="quiet" id="timeline-forward">Dopo →</button><button class="quiet" id="timeline-reset">Ripristina</button></div><p id="timeline-range">${at(range[0])} — ${at(range[1])}</p><p id="timeline-cursor" role="status">Tocca o passa sul grafico per leggere i valori.</p><div class="chart-legend"><span><i class="swatch" style="border-color:var(--teal)"></i>Misure Ecowitt</span><span><i class="swatch forecast-swatch" style="border-color:var(--orange)"></i>Previsione pubblicata ≥1 h prima</span><span><i class="swatch forecast-swatch" style="border-color:var(--blue)"></i>Previsione attuale</span></div><section id="timeline-plots">${series.map(plot).join("")}</section><p class="metric-caption">I buchi restano visibili. Pioggia osservata: somma oraria solo con dodici campioni validi. Ecowitt non misura la copertura nuvolosa. Il grafico storico usa l’ultima previsione effettivamente pubblicata almeno un’ora prima: dove l’archivio manca non viene ricostruita.</p>`;}
  function constrain(a,b){const span=Math.max(3600000,Math.min(bounds[1]-bounds[0],b-a));a=Math.max(bounds[0],Math.min(a,bounds[1]-span));range=[a,a+span];cursor=Math.max(range[0],Math.min(cursor??a,range[1]));}
  function draw(){if(!$("#timeline-plots"))return;$("#timeline-plots").innerHTML=series.map(plot).join("");$("#timeline-range").textContent=at(range[0])+" — "+at(range[1]);const span=(range[1]-range[0])/3600000;$("#timeline-span").value=[3,6,12,24,48,96].find(h=>Math.abs(h-span)<.1)||"";bindPlots();readCursor();}
  function zoom(factor,center=cursor??(range[0]+range[1])/2){const span=(range[1]-range[0])*factor;constrain(center-span/2,center+span/2);draw();}
  function readCursor(){
    if(cursor===null)return;
    $("#timeline-cursor").textContent="Cursore · "+at(cursor);
    for(const s of series){const out=document.querySelector(`[data-timeline-readout="${s.key}"]`);out.textContent=s.lines.map(l=>{const p=l.points.reduce((a,b)=>!a||Math.abs(b.t-cursor)<Math.abs(a.t-cursor)?b:a,null);const tolerance=l.name==="Ecowitt osservata"&&s.key!=="rain_mm"?5*60000:30*60000;return p&&Math.abs(p.t-cursor)<=tolerance&&p.v!==null?`${l.name}: ${num(p.v)} ${s.unit} (${clock(new Date(p.t).toISOString())}${p.known?"; pubblicata "+dtKnown(p.known):""})`:null;}).filter(Boolean).join(" · ")||"Nessun campione vicino al cursore";}
    document.querySelectorAll(".shared-cursor").forEach(p=>p.setAttribute("d",`M${48+(cursor-range[0])/(range[1]-range[0])*840} 14V165`));
  }
  const dtKnown=v=>at(Date.parse(v));
  function bindPlots(){
    document.querySelectorAll("[data-timeline]").forEach(svg=>{
      const t=x=>{const b=svg.getBoundingClientRect();return range[0]+Math.max(0,Math.min(1,(900*(x-b.left)/b.width-48)/840))*(range[1]-range[0]);};
      let start=null,moved=false,pinch=null;const pointers=new Map();
      svg.onpointerdown=e=>{svg.setPointerCapture(e.pointerId);pointers.set(e.pointerId,e.clientX);if(pointers.size===2){const xs=[...pointers.values()];pinch={distance:Math.max(1,Math.abs(xs[1]-xs[0])),range:[...range],center:t((xs[0]+xs[1])/2)};start=null;}else{start=t(e.clientX);moved=false;cursor=start;readCursor();}};
      svg.onpointermove=e=>{if(pointers.has(e.pointerId))pointers.set(e.pointerId,e.clientX);if(pinch&&pointers.size===2)return;cursor=t(e.clientX);readCursor();if(start!==null&&pointers.size===1){moved=Math.abs(cursor-start)>(range[1]-range[0])*.02;const x=v=>48+(v-range[0])/(range[1]-range[0])*840;const rect=svg.querySelector(".range-selection");rect.setAttribute("x",x(Math.min(start,cursor)));rect.setAttribute("width",Math.abs(x(cursor)-x(start)));}};
      svg.onpointerup=e=>{if(pinch&&pointers.size===2){const xs=[...pointers.values()],factor=pinch.distance/Math.max(1,Math.abs(xs[1]-xs[0])),span=(pinch.range[1]-pinch.range[0])*factor;constrain(pinch.center-span/2,pinch.center+span/2);pinch=null;pointers.clear();start=null;draw();return;}pointers.delete(e.pointerId);if(start!==null&&moved){constrain(Math.min(start,cursor),Math.max(start,cursor));start=null;draw();}else start=null;};
      svg.onpointercancel=()=>{start=null;pinch=null;pointers.clear();svg.querySelector(".range-selection").setAttribute("width",0);};
      svg.onkeydown=e=>{if(e.key==="ArrowLeft"||e.key==="ArrowRight"){e.preventDefault();cursor=Math.max(range[0],Math.min(range[1],(cursor??range[0])+(e.key==="ArrowRight"?1:-1)*15*60000));readCursor();}else if(["+","=","-"].includes(e.key)){e.preventDefault();zoom(e.key==="-"?2:.5);document.querySelector(`[data-timeline="${svg.dataset.timeline}"]`)?.focus();}};
    });
  }
  function bind(){if(!$("#timeline-plots"))return;bindPlots();readCursor();$("#timeline-span").onchange=e=>zoom(Number(e.target.value)*3600000/(range[1]-range[0]));$("#timeline-back").onclick=()=>{const span=range[1]-range[0];constrain(range[0]-span/2,range[1]-span/2);draw();};$("#timeline-forward").onclick=()=>{const span=range[1]-range[0];constrain(range[0]+span/2,range[1]+span/2);draw();};$("#timeline-reset").onclick=()=>{range=[Date.now()-6*3600000,Date.now()+24*3600000];cursor=null;draw();};}
  window.MeteoTimeline={view:p=>p==="timeline"?view():undefined,bind};
})();
