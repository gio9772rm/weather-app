/* An ICRS tangent plane shared by the DSS cutout and the optical footprint. */
"use strict";
(() => {
  const rad = Math.PI / 180, size = 512;
  const wrap = deg => (deg % 360 + 360) % 360;
  const surveys = ["alasky.cds.unistra.fr", "alaskybis.cds.unistra.fr"];

  function inverse(ra, dec, x, y) {
    const east = -x, north = -y, rho = Math.hypot(east, north);
    if (rho === 0) return {ra, dec};
    const c = Math.atan(rho), d = dec * rad;
    return {
      ra: wrap(ra + Math.atan2(east * Math.sin(c), rho * Math.cos(d) * Math.cos(c) - north * Math.sin(d) * Math.sin(c)) / rad),
      dec: Math.asin(Math.max(-1, Math.min(1, Math.cos(c) * Math.sin(d) + north * Math.sin(c) * Math.cos(d) / rho))) / rad,
    };
  }

  function bearing(from, to) {
    const delta = (to.ra - from.ra) * rad, a = from.dec * rad, b = to.dec * rad;
    return wrap(Math.atan2(Math.sin(delta) * Math.cos(b), Math.cos(a) * Math.sin(b) - Math.sin(a) * Math.cos(b) * Math.cos(delta)) / rad);
  }

  function mosaic(field, target, cols, rows, overlap, angle) {
    const {width_deg:w, height_deg:h} = field, {ra_deg:ra, dec_deg:dec} = target;
    if (![w,h,ra,dec,cols,rows,overlap,angle].every(Number.isFinite) || !(w > 0 && h > 0 && w < 90 && h < 90) ||
        !Number.isInteger(cols) || !Number.isInteger(rows) || cols < 1 || rows < 1 || cols > 5 || rows > 5 ||
        overlap < 0.1 || overlap > 0.5 || Math.abs(dec) > 90 || ra < 0 || ra >= 360) {
      throw Error("Inquadratura non valida.");
    }
    const pw = 2*Math.tan(w*rad/2), ph = 2*Math.tan(h*rad/2);
    const theta = -angle*rad, ca = Math.cos(theta), sa = Math.sin(theta);
    const rotate = (x,y) => [ca*x - sa*y, sa*x + ca*y];
    const panels = [];
    let extent = 0;
    for (let j = 0; j < rows; j++) for (let i = 0; i < cols; i++) {
      const x = (i - (cols-1)/2)*pw*(1-overlap), y = (j - (rows-1)/2)*ph*(1-overlap);
      const middle = rotate(x,y), centre = inverse(ra,dec,...middle);
      const top = inverse(ra,dec,...rotate(x,y-ph*0.0001));
      const corners = [[-1,-1],[1,-1],[1,1],[-1,1]].map(([dx,dy]) => rotate(x+dx*pw/2,y+dy*ph/2));
      extent = Math.max(extent, ...corners.flat().map(Math.abs));
      panels.push({id: j*cols + i + 1, centre, angle:bearing(centre,top), middle, corners});
    }
    const radius = extent * 1.12, fov = 2*Math.atan(radius)/rad;
    if (fov > 60) throw Error("Campo troppo ampio per la proiezione di anteprima. Riduci il mosaico.");
    const pixel = v => size/2 + v * size / (2*radius);
    const wcs = { NAXIS1:size, NAXIS2:size, WCSAXES:2, CRPIX1:(size+1)/2, CRPIX2:(size+1)/2,
      CRVAL1:ra, CRVAL2:dec, CDELT1:-2*radius/(rad*size), CDELT2:2*radius/(rad*size),
      CUNIT1:"deg", CUNIT2:"deg", CTYPE1:"RA---TAN", CTYPE2:"DEC--TAN", RADESYS:"ICRS" };
    return {panels, pixel, wcs, fov};
  }

  function source(plan, host) {
    const params = new URLSearchParams({ hips:"CDS/P/DSS2/color", format:"jpg", wcs:JSON.stringify(plan.wcs) });
    return "https://" + host + "/hips-image-services/hips2fits?" + params.toString();
  }

  function suggestion(target) {
    const f = target.framing;
    const count = (extent, field) => Math.max(1, Math.min(5, 1 + Math.ceil((extent / field - 1) / .8)));
    if (f?.fits !== false) return [1, 1];
    const c = count(f.target_width_arcmin, f.field_width_arcmin), r = count(f.target_height_arcmin, f.field_height_arcmin);
    return c*r > 1 ? [c,r] : [2,1];
  }

  function open(report, target) {
    $("#framing-dialog")?.remove();
    const dialog = document.createElement("dialog"), [cols, rows] = suggestion(target);
    dialog.id = "framing-dialog";
    dialog.innerHTML = `<form><div class="dialog-title"><h2>Inquadratura · ${esc(target.id)}</h2><button type="button" id="framing-close" class="quiet" aria-label="Chiudi">×</button></div>
      <p>Campo del sensore ${num(report.field.width_deg, 2)}° × ${num(report.field.height_deg, 2)}°. Nord in alto, est a sinistra.</p>
      <div class="form-grid"><label>Pannelli orizzontali<input id="framing-cols" type="number" min="1" max="5" value="${cols}" required></label>
      <label>Pannelli verticali<input id="framing-rows" type="number" min="1" max="5" value="${rows}" required></label>
      <label>Sovrapposizione · %<input id="framing-overlap" type="number" min="10" max="50" value="20" required></label>
      <label>Angolo camera · ° da Nord verso Est<input id="framing-angle" type="number" min="0" max="360" step=".1" value="${Number(report.constraints.rotation).toFixed(1)}" required></label></div>
      <div class="controls"><button class="primary">Aggiorna anteprima DSS2</button><button type="button" class="quiet" id="framing-export" disabled>Scarica centri CSV</button></div>
      <p id="framing-status" role="status"></p><div id="framing-preview" class="sky-framing-image"></div>
      <p class="metric-caption">Immagine <a href="https://alasky.cds.unistra.fr/hips-image-services/hips2fits" target="_blank" rel="noopener noreferrer">DSS2 Color · CDS, HiPS2FITS</a> · <a href="https://archive.stsci.edu/dss/copyright.html" target="_blank" rel="noopener noreferrer">crediti delle lastre STScI</a>. L’immagine è una mappa del cielo per inquadrare, non una ripresa attuale.</p>
    </form>`;
    document.body.append(dialog); dialog.showModal();
    const $d = selector => dialog.querySelector(selector);
    let current = null, renderId = 0, timer;
    dialog.onclose = () => {clearTimeout(timer); dialog.remove();};
    $d("#framing-close").onclick = () => dialog.close();
    const preview = $d("#framing-preview"), info = $d("#framing-status"), save = $d("#framing-export");
    function draw() {
      const id = ++renderId;
      clearTimeout(timer); current = null; save.disabled = true; preview.replaceChildren();
      try {
        const read = key => Number($d("#framing-"+key).value);
        const plan = current = mosaic(report.field, target, read("cols"), read("rows"), read("overlap")/100, read("angle"));
        const img = document.createElement("img");
        img.alt = "Atlante DSS2 del campo " + target.id;
        img.referrerPolicy = "no-referrer"; img.hidden = true;
        preview.append(img);
        preview.insertAdjacentHTML("beforeend", `<svg viewBox="0 0 ${size} ${size}" role="img" aria-label="Pannelli del mosaico">
          <text x="256" y="22">N</text><text x="20" y="256">E</text>
          ${plan.panels.map(p => `<polygon points="${p.corners.map(pair => pair.map(v => plan.pixel(v).toFixed(2)).join(",")).join(" ")}"/><text x="${plan.pixel(p.middle[0]).toFixed(2)}" y="${plan.pixel(p.middle[1]).toFixed(2)}">${p.id}</text>`).join("")}
        </svg>`);
        let attempt = 0;
        function failed() {
          clearTimeout(timer);
          if (!dialog.open || id !== renderId || !img.isConnected) return;
          attempt++;
          if (attempt < surveys.length) load();
          else {
            img.remove();
            info.textContent = "Immagine DSS2 non disponibile. Puoi comunque vedere i pannelli e scaricare i centri.";
          }
        }
        function load() {
          img.src = source(plan, surveys[attempt]);
          timer = setTimeout(failed, 10000);
        }
        img.onload = () => {
          if (!dialog.open || id !== renderId || !img.isConnected) return;
          clearTimeout(timer); img.hidden = false;
          info.textContent = plan.panels.length + (plan.panels.length === 1 ? " pannello · " : " pannelli · ") + "immagine DSS2 caricata.";
        };
        img.onerror = failed; info.textContent = "Carico il campo dall’atlante DSS2…"; load(); save.disabled = false;
      } catch (error) { info.textContent = error.message; }
    }
    dialog.querySelector("form").onsubmit = event => { event.preventDefault(); draw(); };
    dialog.querySelectorAll("input").forEach(input => input.oninput = () => {
      renderId++; clearTimeout(timer); current = null; save.disabled = true; info.textContent = "Aggiorna l’anteprima per applicare queste impostazioni.";
    });
    save.onclick = () => {
      if (!current) return;
      const csv = "panel;ra_deg;dec_deg;position_angle_deg\n" + current.panels.map(p =>
        [p.id, p.centre.ra.toFixed(6), p.centre.dec.toFixed(6), p.angle.toFixed(3)].join(";")
      ).join("\n") + "\n";
      MeteoIntegration.download(csv, "meteo-mosaico.csv");
    };
    draw();
  }
  globalThis.MeteoFraming = { mosaic, open };
})();
