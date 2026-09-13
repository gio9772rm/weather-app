"""Aggiunge favicon, anteprima social (Open Graph) e un manifest PWA minimo
all'interfaccia di Streamlit, che di norma non permette di personalizzare
questi elementi. Va eseguito una volta dopo `pip install`, prima di avviare
l'app (vedi buildCommand in render.yaml).

Non tocca il comportamento dell'app: modifica solo l'HTML statico servito
da Streamlit e aggiunge alcuni file (icone, manifest, service worker) nella
stessa cartella "static" già usata da Streamlit per i propri asset.
"""

from __future__ import annotations

import os
import shutil
import sys

MARKER = "<!-- meteo-v4-pwa-patch -->"

MANIFEST_JSON = """{
  "name": "Meteo V4 \\u00b7 Stazione meteo",
  "short_name": "Meteo V4",
  "description": "Stazione meteo in tempo reale: previsioni, radar, qualit\\u00e0 dell'aria e astronomia.",
  "start_url": "./?src=pwa",
  "scope": "./",
  "display": "standalone",
  "background_color": "#0f3d78",
  "theme_color": "#0b76b7",
  "orientation": "portrait-primary",
  "icons": [
    {"src": "./icon-192.png", "sizes": "192x192", "type": "image/png", "purpose": "any"},
    {"src": "./icon-512.png", "sizes": "512x512", "type": "image/png", "purpose": "any"},
    {"src": "./icon-512-maskable.png", "sizes": "512x512", "type": "image/png", "purpose": "maskable"}
  ]
}
"""

SERVICE_WORKER_JS = """// Service worker minimo: serve solo a rendere l'app installabile
// (PWA / TWA Android). Nessuna cache di dati live: rete sempre in prima
// battuta, per non mostrare mai meteo non aggiornato.
self.addEventListener("install", () => self.skipWaiting());
self.addEventListener("activate", (event) => event.waitUntil(self.clients.claim()));
self.addEventListener("fetch", (event) => {
  event.respondWith(fetch(event.request).catch(() => caches.match(event.request)));
});
"""

HEAD_INJECTION = """{marker}
<meta name="theme-color" content="#0b76b7" />
<meta name="description" content="Stazione meteo in tempo reale: previsioni, radar, qualit\u00e0 dell'aria e astronomia." />
<meta property="og:title" content="Meteo V4 \u00b7 Stazione meteo in tempo reale" />
<meta property="og:description" content="Previsioni, radar, qualit\u00e0 dell'aria e astronomia aggiornati in tempo reale dalla stazione locale." />
<meta property="og:type" content="website" />
<meta property="og:image" content="./og-image.png" />
<meta name="twitter:card" content="summary_large_image" />
<link rel="apple-touch-icon" href="./apple-touch-icon-180.png" />
<link rel="manifest" href="./pwa-manifest.json" />
<script>
  if ("serviceWorker" in navigator) {{
    window.addEventListener("load", function () {{
      navigator.serviceWorker.register("./sw.js").catch(function () {{}});
    }});
  }}
</script>
""".format(marker=MARKER)


def find_static_dir() -> str:
    import streamlit

    return os.path.join(os.path.dirname(streamlit.__file__), "static")


def main() -> int:
    static_dir = find_static_dir()
    if not os.path.isdir(static_dir):
        print(f"[patch_streamlit_pwa] cartella static non trovata: {static_dir}")
        return 1

    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    assets_dir = os.path.join(repo_root, "assets", "pwa")

    icon_files = [
        "favicon-32.png",
        "apple-touch-icon-180.png",
        "icon-192.png",
        "icon-512.png",
        "icon-512-maskable.png",
        "og-image.png",
    ]
    for name in icon_files:
        src = os.path.join(assets_dir, name)
        if os.path.isfile(src):
            shutil.copy(src, os.path.join(static_dir, name))

    # Il favicon esistente ("favicon.png", 32x32) e' quello referenziato
    # dall'HTML di Streamlit: lo sovrascriviamo con la nostra versione,
    # cosi' non serve modificare il tag <link rel="shortcut icon">.
    favicon_src = os.path.join(assets_dir, "favicon-32.png")
    if os.path.isfile(favicon_src):
        shutil.copy(favicon_src, os.path.join(static_dir, "favicon.png"))

    with open(os.path.join(static_dir, "pwa-manifest.json"), "w", encoding="utf-8") as f:
        f.write(MANIFEST_JSON)
    with open(os.path.join(static_dir, "sw.js"), "w", encoding="utf-8") as f:
        f.write(SERVICE_WORKER_JS)

    index_path = os.path.join(static_dir, "index.html")
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
