"""Aggiunge favicon, anteprima social (Open Graph) e un manifest PWA minimo
all'interfaccia di Streamlit, che di norma non permette di personalizzare
questi elementi. Va eseguito una volta dopo `pip install`, prima di avviare
l'app (vedi buildCommand in render.yaml) e viene comunque ri-applicato a
ogni avvio dello script principale (vedi app_streamlit.py) perche' alcuni
host non rieseguono il comando di build quando cambia solo render.yaml su
un servizio gia' esistente.

Nota importante: Streamlit (>=1.31) NON serve file arbitrari messi nella
cartella "static" del proprio package - le uniche eccezioni sono i file
che Streamlit stesso gia' possiede (es. favicon.png), che possiamo
sovrascrivere in-place. I file NUOVI (manifest, service worker, icone)
sono invece committati direttamente nella cartella "static/" accanto ad
app_streamlit.py e serviti da Streamlit su "/app/static/<file>" grazie a
`server.enableStaticServing = true` (vedi .streamlit/config.toml) - questa
cartella deve esistere GIA' quando il server parte (Streamlit controlla la
sua presenza all'avvio del processo, non ad ogni richiesta), per questo
non puo' essere generata qui a runtime: va committata nel repository.
"""

from __future__ import annotations

import os
import shutil
import sys

MARKER = "<!-- meteo-v4-pwa-patch -->"

HEAD_INJECTION = """{marker}
<meta name="theme-color" content="#0b76b7" />
<meta name="description" content="Stazione meteo in tempo reale: previsioni, radar, qualit\u00e0 dell'aria e astronomia." />
<meta property="og:title" content="Meteo V4 \u00b7 Stazione meteo in tempo reale" />
<meta property="og:description" content="Previsioni, radar, qualit\u00e0 dell'aria e astronomia aggiornati in tempo reale dalla stazione locale." />
<meta property="og:type" content="website" />
<meta property="og:image" content="app/static/og-image.png" />
<meta name="twitter:card" content="summary_large_image" />
<link rel="apple-touch-icon" href="app/static/apple-touch-icon-180.png" />
<link rel="manifest" href="app/static/pwa-manifest.json" />
<script>
  if ("serviceWorker" in navigator) {{
    window.addEventListener("load", function () {{
      navigator.serviceWorker.register("app/static/sw.js").catch(function () {{}});
    }});
  }}
</script>
<script>
  // Controllo aggiornamenti del guscio nativo Android (TWA). L'app privata
  // apre sempre questa pagina live, quindi contenuti/funzionalita' del sito
  // sono gia' sempre aggiornati da soli. Solo il guscio nativo (icona, nome,
  // splash screen, colori di sistema) e' incorporato nell'APK e non puo'
  // aggiornarsi da solo senza passare dal Play Store: qui verifichiamo se la
  // versione del guscio installata (parametro "shell" nell'URL di avvio,
  // impostato dentro twa-manifest.json) e' inferiore all'ultima disponibile
  // e mostriamo un banner con un link diretto per scaricare e installare il
  // nuovo APK (richiede comunque una conferma dell'utente: e' un limite di
  // sicurezza di Android per le app non distribuite tramite store).
  document.addEventListener("DOMContentLoaded", function () {{
    try {{
      var params = new URLSearchParams(window.location.search);
      var isTwa = params.get("src") === "twa" || document.referrer.indexOf("android-app://") === 0;
      var installedShell = parseInt(params.get("shell") || "0", 10);
      if (!isTwa || !installedShell) return;
      fetch("app/static/android-version.json", {{cache: "no-store"}})
        .then(function (r) {{ return r.json(); }})
        .then(function (info) {{
          if (!info || !info.shellVersion || info.shellVersion <= installedShell) return;
          if (localStorage.getItem("meteov4_shell_dismissed") === String(info.shellVersion)) return;
          var bar = document.createElement("div");
          bar.style.cssText = "position:fixed;left:0;right:0;bottom:0;z-index:999999;background:#0b76b7;color:#fff;"
            + "padding:10px 14px;font-family:sans-serif;font-size:14px;display:flex;align-items:center;"
            + "justify-content:space-between;gap:12px;box-shadow:0 -2px 8px rgba(0,0,0,.25);";
          var label = document.createElement("span");
          label.textContent = "Nuova versione dell'app disponibile";
          var actions = document.createElement("div");
          actions.style.cssText = "display:flex;align-items:center;gap:16px;flex-shrink:0;";
          var link = document.createElement("a");
          link.href = info.apkUrl;
          link.textContent = "Aggiorna";
          link.style.cssText = "color:#fff;font-weight:bold;text-decoration:underline;";
          var close = document.createElement("button");
          close.textContent = "\u2715";
          close.setAttribute("aria-label", "Chiudi");
          close.style.cssText = "background:transparent;border:none;color:#fff;font-size:16px;cursor:pointer;line-height:1;";
          close.onclick = function () {{
            localStorage.setItem("meteov4_shell_dismissed", String(info.shellVersion));
            bar.remove();
          }};
          actions.appendChild(link);
          actions.appendChild(close);
          bar.appendChild(label);
          bar.appendChild(actions);
          document.body.appendChild(bar);
        }})
        .catch(function () {{}});
    }} catch (e) {{}}
  }});
</script>
""".format(marker=MARKER)


def find_streamlit_static_dir() -> str:
    import streamlit

    return os.path.join(os.path.dirname(streamlit.__file__), "static")


_PATCH_APPLIED = False


def apply_patch() -> int:
    """Idempotent, in-process variant of `main()`. Safe to import and call
    from the app itself on every rerun: after the first successful call in
    a given Python process it short-circuits immediately, so it does not
    depend on the hosting platform actually running this file as a build
    step (some platforms ignore a repo's build-command changes once a
    service already exists).
    """
    global _PATCH_APPLIED
    if _PATCH_APPLIED:
        return 0
    try:
        result = main()
    except Exception as exc:  # pragma: no cover - never break the app for this
        print(f"[patch_streamlit_pwa] patch non applicata: {exc}")
        result = 1
    _PATCH_APPLIED = True
    return result


def main() -> int:
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    assets_dir = os.path.join(repo_root, "assets", "pwa")
    streamlit_static_dir = find_streamlit_static_dir()

    # Favicon: Streamlit serve gia' "favicon.png" dalla propria cartella
    # static interna (e' uno dei pochi file "conosciuti" che sovrascrive
    # con successo anche se non e' nella cartella app/static). Lo
    # sovrascriviamo con la nostra versione, cosi' non serve modificare il
    # tag <link rel="shortcut icon"> di Streamlit.
    favicon_src = os.path.join(assets_dir, "favicon-32.png")
    if os.path.isdir(streamlit_static_dir) and os.path.isfile(favicon_src):
        shutil.copy(favicon_src, os.path.join(streamlit_static_dir, "favicon.png"))

    # HTML: titolo pagina + meta/manifest/service worker nel <head>.
    index_path = os.path.join(streamlit_static_dir, "index.html")
    if os.path.isfile(index_path):
        with open(index_path, "r", encoding="utf-8") as f:
            html = f.read()

        if MARKER not in html:
            html = html.replace("</head>", HEAD_INJECTION + "  </head>", 1)
        html = html.replace("<title>Streamlit</title>", "<title>Meteo V4</title>", 1)

        with open(index_path, "w", encoding="utf-8") as f:
            f.write(html)

    print("[patch_streamlit_pwa] favicon, meta anteprima social e manifest PWA applicati.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
