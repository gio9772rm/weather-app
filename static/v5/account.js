/* Personal identity, preferences overview and optional Google sign-in. */
"use strict";
(() => {
  const key = "meteo.v5.identity", accents = [["teal", "Verde acqua"], ["blue", "Blu"], ["violet", "Viola"], ["orange", "Arancio"]];
  const identity = () => ({ name: "", note: "", avatar: "", accent: "teal", ...storage.get(key, {}) });
  const person = '<svg width="19" height="19" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" aria-hidden="true"><circle cx="12" cy="8" r="3.5"/><path d="M4.5 21v-2.2a5 5 0 0 1 5-5h5a5 5 0 0 1 5 5V21"/></svg>';
  const label = (profile, account) => profile.name || account?.google?.email || account?.username || "Ospite";
  const validAvatar = value => typeof value === "string" && value.length < 48001 && /^data:image\/jpeg;base64,[A-Za-z0-9+/]+={0,2}$/.test(value);
  function avatar(profile, account, large = false) {
    const initials = (profile.name || (account && label(profile, account)) || "").trim().split(/\s+/).slice(0, 2).map(word => word[0] || "").join("").toUpperCase();
    const accent = accents.some(([id]) => id === profile.accent) ? profile.accent : "teal";
    return `<span class="profile-avatar accent-${accent}${large ? " large" : ""}" aria-hidden="true">${validAvatar(profile.avatar) ? `<img src="${esc(profile.avatar)}" alt="">` : initials ? esc(initials) : person}</span>`;
  }
  function badge(account, verified) {
    const button = $("#account-shortcut");
    if (!button) return;
    const profile = identity(), name = label(profile, account);
    const text = "Account · " + name + (account && !verified ? " · accesso da verificare" : "");
    button.innerHTML = avatar(profile, account);
    button.setAttribute("aria-label", text); button.title = text;
    button.setAttribute("aria-current", page === "personal" ? "page" : "false");
  }
  function markup(account, verified) {
    const profile = identity(), sky = MeteoSky.settings();
    const astronomy = { deep_sky: "Fotografia cielo profondo", visual: "Osservazione visuale", planetary: "Luna e pianeti" };
    const initial = window.MeteoCity?.initial().label || "Roma";
    const cities = window.MeteoCity?.favorites().length || 0;
    return `<article class="card account-overview">
      <div class="account-identity">${avatar(profile, account, true)}<div><span class="eyebrow">${account ? verified ? "PROFILO PERSONALE" : "COPIA SUL DISPOSITIVO" : "PROFILO OSPITE"}</span>
        <h2>${esc(label(profile, account))}</h2>
        <p>${account ? esc(account.username) + " · " + (verified ? account.auth_method === "google" ? "Accesso Google" : "Accesso Meteo Pro" : "Accesso da verificare") : "Preferenze su questo dispositivo"}</p>
        ${account?.google?.email && profile.name ? `<p>Google: ${esc(account.google.email)}</p>` : ""}
        ${profile.note ? `<p class="metric-caption">${esc(profile.note)}</p>` : ""}</div>
        <button id="account-edit" type="button" class="quiet">Modifica profilo</button></div>
      <dl class="account-prefs">
        <div><dt>Tema</dt><dd>${prefs.theme === "dark" ? "Scuro" : "Chiaro"}</dd></div>
        <div><dt>Città di avvio</dt><dd>${esc(initial)}</dd></div>
        <div><dt>Scelte rapide</dt><dd>${cities} ${cities === 1 ? "città preferita" : "città preferite"}</dd></div>
        <div><dt>Attività in Cielo</dt><dd>${esc(astronomy[prefs.profile] || "—")}</dd></div>
        <div><dt>Cielo salvato</dt><dd>${sky.sites.length} ${sky.sites.length === 1 ? "postazione" : "postazioni"} · ${sky.projects.length} ${sky.projects.length === 1 ? "progetto" : "progetti"}</dd></div>
      </dl>
      <button type="button" class="quiet" data-settings>Modifica preferenze</button>
      ${account?.google && verified ? '<span class="account-google-tag">Google collegato</span>' : ""}
      <div id="account-google" class="account-google" hidden></div>
    </article>`;
  }
  async function photo(file) {
    if (!["image/png", "image/jpeg", "image/webp"].includes(file.type) || file.size > 5 * 1024 * 1024) {
      throw Error("Usa una foto JPG, PNG o WebP fino a 5 MB.");
    }
    const url = URL.createObjectURL(file);
    try {
      const image = new Image(); image.src = url; await image.decode();
      const { naturalWidth: w, naturalHeight: h } = image;
      if (!w || !h || w * h > 40000000) throw Error("Scegli una foto più piccola.");
      const side = Math.min(w, h), canvas = document.createElement("canvas");
      canvas.width = canvas.height = 128;
      const ctx = canvas.getContext("2d");
      ctx.fillStyle = "#e6eef4"; ctx.fillRect(0, 0, 128, 128);
      ctx.drawImage(image, (w - side) / 2, (h - side) / 2, side, side, 0, 0, 128, 128);
      return canvas.toDataURL("image/jpeg", .86);
    } finally { URL.revokeObjectURL(url); }
  }
  function edit(account) {
    $("#account-dialog")?.remove();
    const original = identity(), dialog = document.createElement("dialog");
    let image = original.avatar, loading = false;
    dialog.id = "account-dialog";
    dialog.innerHTML = `<form><div class="dialog-title"><h2>Personalizza il profilo</h2><button id="account-close" type="button" class="quiet" aria-label="Chiudi">×</button></div>
      <div id="account-preview">${avatar(original, account, true)}</div>
      <label>Immagine<input id="account-photo" type="file" accept="image/jpeg,image/png,image/webp"></label>
      <button id="account-photo-remove" type="button" class="quiet">Rimuovi immagine</button>
      <div class="form-grid"><label>Nome visibile a te<input id="account-display-name" maxlength="60" value="${esc(original.name)}"></label>
        <label>Colore del profilo<select id="account-accent">${accents.map(([id, name]) => `<option value="${id}"${original.accent === id ? " selected" : ""}>${name}</option>`).join("")}</select></label></div>
      <label>Nota personale<input id="account-note" maxlength="160" value="${esc(original.note)}" placeholder="Es. Postazione e strumento preferiti"></label>
      <p class="metric-caption">L’immagine è ridotta sul dispositivo e rimane nel tuo profilo.</p>
      <button class="primary" id="account-save-identity">Salva profilo</button><p id="account-edit-status" role="status"></p></form>`;
    document.body.append(dialog); dialog.showModal(); dialog.onclose = () => dialog.remove();
    const read = () => ({ name: $("#account-display-name").value.trim(), note: $("#account-note").value.trim(), accent: $("#account-accent").value, avatar: image });
    const preview = () => { $("#account-preview").innerHTML = avatar(read(), account, true); };
    $("#account-close").onclick = () => dialog.close();
    $("#account-photo-remove").onclick = () => { if (!loading) { image = ""; $("#account-photo").value = ""; preview(); } };
    $("#account-accent").onchange = preview;
    $("#account-display-name").oninput = preview;
    $("#account-photo").onchange = async () => {
      const file = $("#account-photo").files[0];
      if (!file || loading) return;
      loading = true; $("#account-save-identity").disabled = true;
      try {
        const value = await photo(file);
        if (!dialog.isConnected) return;
        image = value; preview(); $("#account-edit-status").textContent = "";
      } catch (error) {
        if (dialog.isConnected) $("#account-edit-status").textContent = error.message;
      } finally {
        loading = false;
        if (dialog.isConnected) $("#account-save-identity").disabled = false;
      }
    };
    dialog.querySelector("form").onsubmit = event => {
      event.preventDefault();
      if (loading) return;
      storage.set(key, read()); dialog.close(); render();
    };
  }
  let googleConfig, library, activeGoogle = null;
  function googleLibrary() {
    if (window.google?.accounts?.id) return Promise.resolve();
    if (library) return library;
    library = new Promise((resolve, reject) => {
      const script = document.createElement("script");
      script.src = "https://accounts.google.com/gsi/client"; script.async = true;
      const timer = setTimeout(() => { script.remove(); reject(Error("Google non ha risposto. Riprova.")); }, 12000);
      script.onload = () => { clearTimeout(timer); resolve(); };
      script.onerror = () => { clearTimeout(timer); script.remove(); reject(Error("Accesso Google non disponibile.")); };
      document.head.append(script);
    }).catch(error => { library = null; throw error; });
    return library;
  }
  async function bindGoogle(account, verified, api, accept) {
    const root = $("#account-google");
    if (!root || !navigator.onLine || offline || (account?.google && verified)) return;
    if (!googleConfig) googleConfig = api("google-config").catch(() => { googleConfig = null; return { enabled: false }; });
    if (!(await googleConfig).enabled || !root.isConnected) return;
    const mode = account && verified ? "link" : "signin";
    root.hidden = false;
    root.innerHTML = `<button id="account-google-prepare" type="button" class="quiet">${mode === "link" ? "Collega account Google" : "Continua con Google"}</button>
      <div id="account-google-button"></div><p id="account-google-status" class="metric-caption" role="status"></p>`;
    const button = root.querySelector("#account-google-prepare");
    const status = text => { if (root.isConnected) root.querySelector("#account-google-status").textContent = text; };
    button.onclick = async () => {
      button.disabled = true;
      const operation = {}; activeGoogle = operation;
      try {
        const [challenge] = await Promise.all([api("google-start", { mode }), googleLibrary()]);
        if (!root.isConnected || activeGoogle !== operation) return;
        window.google.accounts.id.initialize({
          client_id: challenge.client_id, nonce: challenge.nonce, ux_mode: "popup",
          use_fedcm_for_button: true, button_auto_select: false,
          callback: async reply => {
            if (activeGoogle !== operation || !root.isConnected) return;
            activeGoogle = null;
            try { status("Verifico l’account Google…"); await accept(await api("google", { credential: reply.credential }), mode); }
            catch (error) {
              status(error.message);
              root.querySelector("#account-google-button").replaceChildren();
              button.hidden = false; button.disabled = false;
            }
          },
        });
        window.google.accounts.id.renderButton(root.querySelector("#account-google-button"), {
          theme: prefs.theme === "dark" ? "filled_black" : "outline",
          size: "large", text: "continue_with", shape: "pill", locale: "it",
        });
        button.disabled = false; button.textContent = "Prepara di nuovo il pulsante Google";
        status(mode === "link" ? "Scegli l’account Google da collegare a questo profilo." : "");
      } catch (error) { status(error.message); button.disabled = false; }
    };
  }
  function reset() { $("#account-dialog")?.remove(); activeGoogle = null; }
  window.MeteoAccount = { badge, markup, edit, bindGoogle, reset };
})();
