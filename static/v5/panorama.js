/* Trace a level 360-degree equirectangular panorama without uploading it. */
"use strict";
(() => {
  function maskFromPoints(points, north, level) {
    if (!Number.isFinite(north) || !Number.isFinite(level)) throw Error("Indica Nord e quota 0° sulla foto.");
    const angles = points.map(({x, y}) => ({
      az: ((x - north) * 360 + 360) % 360, alt: (level - y) * 180,
    })).sort((a, b) => a.az - b.az);
    if (angles.length < 4 || angles.some(({az}, i) =>
      (angles[i + 1]?.az ?? angles[0].az + 360) - az > 90.01
    )) throw Error("Traccia tutto l’orizzonte: lascia al massimo 90° tra punti consecutivi.");
    return MeteoHorizon.parse(angles.map(({az, alt}) => az.toFixed(3) + " " + alt.toFixed(3)).join("\n"));
  }

  function open(onSave, replacing) {
    $("#panorama-dialog")?.remove();
    const dialog = document.createElement("dialog");
    dialog.id = "panorama-dialog";
    dialog.innerHTML = `<form><div class="dialog-title"><h2>Orizzonte da foto</h2><button type="button" id="panorama-close" class="quiet" aria-label="Chiudi">×</button></div>
      <p>Usa una panoramica sferica 360° × 180°, proporzione 2:1, livellata e ripresa dal punto del telescopio. Indica il Nord geografico e la linea a 0° d’altezza usando riferimenti noti.</p>
      <label>Panoramica<input id="panorama-file" type="file" accept="image/jpeg,image/png,image/webp"></label>
      <div class="controls"><button type="button" class="quiet" data-pano-mode="north">1 · Indica Nord</button><button type="button" class="quiet" data-pano-mode="level">2 · Indica 0°</button><button type="button" class="quiet" data-pano-mode="trace">3 · Traccia ostacoli</button><button type="button" class="quiet" id="panorama-undo">Togli ultimo punto</button></div>
      <canvas id="panorama-canvas" width="1200" height="600" aria-label="Panoramica con linea orizzonte" hidden></canvas>
      <details class="detail-row"><summary>Inserisci un punto con la tastiera</summary><div class="form-grid">
        <label>Posizione da sinistra · %<input id="panorama-x" type="number" min="0" max="100" step=".1" value="50"></label>
        <label>Posizione dall’alto · %<input id="panorama-y" type="number" min="0" max="100" step=".1" value="50"></label>
      </div><button id="panorama-point" type="button" class="quiet">Applica il punto nel modo selezionato</button></details>
      <p id="panorama-status" role="status">Carica una panoramica per iniziare.</p>
      ${replacing ? '<p class="metric-caption">Salvando sostituirai l’orizzonte della postazione.</p>' : ""}
      <p class="metric-caption">La foto resta nel browser; vengono salvati solo azimut e altezza.</p>
      <button class="primary" id="panorama-save" disabled>Salva orizzonte .hrz</button></form>`;
    document.body.append(dialog); dialog.showModal();
    dialog.onclose = () => dialog.remove();
    const $d = selector => dialog.querySelector(selector);
    const canvas = $d("#panorama-canvas"), ctx = canvas.getContext("2d"), info = $d("#panorama-status");
    const w = canvas.width, h = canvas.height, modes = [...dialog.querySelectorAll("[data-pano-mode]")];
    let image = null, mode = "north", north, level, mask, version = 0;
    const points = [];
    function draw() {
      if (!image) return;
      ctx.drawImage(image, 0, 0, w, h);
      ctx.lineWidth = 4;
      if (Number.isFinite(north)) {
        ctx.strokeStyle = "#52e4ff"; ctx.beginPath(); ctx.moveTo(north*w, 0); ctx.lineTo(north*w, h); ctx.stroke();
      }
      if (Number.isFinite(level)) {
        ctx.strokeStyle = "#fff59d"; ctx.beginPath(); ctx.moveTo(0, level*h); ctx.lineTo(w, level*h); ctx.stroke();
      }
      const ordered = [...points].sort((a,b) => a.x - b.x);
      ctx.strokeStyle = "#ff6d87"; ctx.fillStyle = "#ff6d87"; ctx.beginPath();
      ordered.forEach(({x,y}, i) => i ? ctx.lineTo(x*w,y*h) : ctx.moveTo(x*w,y*h)); ctx.stroke();
      ordered.forEach(({x,y}) => {ctx.beginPath(); ctx.arc(x*w,y*h,5,0,2*Math.PI); ctx.fill();});
    }
    function update() {
      draw();
      modes.forEach(b => { b.disabled = !image; b.setAttribute("aria-pressed", String(b.dataset.panoMode === mode)); });
      $d("#panorama-point").disabled = !image;
      $d("#panorama-undo").disabled = !points.length;
      $d("#panorama-save").disabled = true; mask = null;
      if (!image) return;
      try {
        mask = maskFromPoints(points, north, level); $d("#panorama-save").disabled = false;
        info.textContent = points.length + " punti · profilo pronto da salvare.";
      } catch (error) {
        info.textContent = (mode === "north" ? "Tocca dove vedi il Nord. " : mode === "level" ? "Tocca la linea a quota 0°. " : points.length + " punti. ") + error.message;
      }
    }
    function point(x, y) {
      if (!image || !(x >= 0 && x <= 1 && y >= 0 && y <= 1)) return;
      if (mode === "north") { north = x; mode = "level"; }
      else if (mode === "level") { level = y; mode = "trace"; }
      else if (points.length < 240) points.push({x, y});
      update();
    }
    modes.forEach(b => b.onclick = () => { mode = b.dataset.panoMode; update(); });
    $d("#panorama-close").onclick = () => dialog.close();
    $d("#panorama-undo").onclick = () => { points.pop(); update(); };
    $d("#panorama-point").onclick = () => point(Number($d("#panorama-x").value) / 100, Number($d("#panorama-y").value) / 100);
    canvas.onclick = event => {
      const box = canvas.getBoundingClientRect();
      point((event.clientX - box.left) / box.width, (event.clientY - box.top) / box.height);
    };
    $d("#panorama-file").onchange = async event => {
      const token = ++version, file = event.target.files[0];
      image = null; points.length = 0; north = level = undefined; mode = "north"; canvas.hidden = true; update();
      if (!file) return;
      if (!["image/png", "image/jpeg", "image/webp"].includes(file.type) || file.size > 15 * 1024 * 1024) {
        info.textContent = "Scegli JPG, PNG o WebP fino a 15 MB."; return;
      }
      const url = URL.createObjectURL(file);
      try {
        const decoded = new Image(); decoded.src = url; await decoded.decode();
        if (token !== version || !dialog.isConnected) return;
        const ratio = decoded.naturalWidth / decoded.naturalHeight;
        if (ratio < 1.95 || ratio > 2.05 || decoded.naturalWidth * decoded.naturalHeight > 40000000) {
          throw Error("Serve una foto sferica 2:1 fino a 40 megapixel.");
        }
        image = decoded; canvas.hidden = false; update();
      } catch (error) { if (token === version && dialog.isConnected) info.textContent = error.message; }
      finally { URL.revokeObjectURL(url); }
    };
    dialog.querySelector("form").onsubmit = event => {
      event.preventDefault();
      if (mask && onSave(mask)) dialog.close();
      else if (mask) info.textContent = "Rimuovi un orizzonte salvato prima di aggiungerne un altro.";
    };
    update();
  }
  globalThis.MeteoPanorama = { maskFromPoints, open };
})();
