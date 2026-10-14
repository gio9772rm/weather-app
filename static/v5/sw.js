"use strict";
const SHELL = "meteo-v5-shell-12",
  DATA = "meteo-v5-data",
  SETTINGS = "meteo-v5-settings";
const ASSETS = [
  "/",
  "/assets/v5/app.css",
  "/assets/v5/app.js",
  "/assets/v5/extras.js",
  "/assets/v5/radar.js",
  "/assets/v5/extras.css",
  "/assets/v5/insights.js",
  "/assets/v5/insights.css",
  "/assets/v5/v53.js",
  "/assets/v5/v54.js",
  "/assets/v5/v55.js",
  "/assets/v5/v55.css",
  "/assets/v5/v54.css",
  "/assets/v5/nina.js",
  "/assets/v5/timeline.js",
  "/assets/v5/v53.css",
  "/assets/v5/icon.svg",
  "/assets/v5/manifest.webmanifest",
  "/app/static/icon-192.png",
  "/app/static/icon-512.png",
  "/app/static/icon-512-maskable.png",
];
async function boundedFetch(request, options = {}) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 12000);
  try {
    return await fetch(request, { ...options, signal: controller.signal });
  } finally {
    clearTimeout(timer);
  }
}
function identified(response) {
  return response.headers.get("X-Meteo-App") === "v5";
}
async function homeDocument(response, network = false) {
  if (!response?.ok || (network && !identified(response))) return false;
  const body = await response.clone().text();
  return body.includes('id="content"') && body.includes("/assets/v5/app.js");
}
async function validData(response, url) {
  if (!response?.ok || !response.headers.get("Content-Type")?.includes("application/json")) return false;
  try {
    const value = await response.clone().json();
    return url.pathname === "/api/v5/stations"
      ? Array.isArray(value.stations) && value.stations.every(station => /^[a-z0-9-]{1,80}$/.test(station.id))
      : value.station?.id === url.pathname.split("/").pop() && typeof value.generated_at === "string";
  } catch { return false; }
}
function unavailable(pro = false) {
  return new Response(`<!doctype html><html lang="it"><meta name="viewport" content="width=device-width"><title>Meteo Pro · attivazione</title><body style="font:18px system-ui;max-width:38rem;margin:10vh auto;padding:24px;background:#f6f8fb;color:#10243d"><h1>Il servizio meteo si sta riattivando</h1><p>${pro ? "Gli strumenti avanzati richiedono il servizio online. Puoi intanto consultare l’ultima fotografia salvata nella home." : "Attendi qualche istante e riprova. La prima apertura dopo una pausa può richiedere più tempo."}</p><button onclick="location.reload()" style="font:inherit;padding:12px">Riprova</button> <a href="/">Apri la home</a></body></html>`, { status: 503, headers: { "Content-Type": "text/html;charset=utf-8", "Cache-Control": "no-store" } });
}
self.addEventListener("install", event => event.waitUntil((async () => {
  // Validate every response before replacing the previous working shell.
  // Render's wake-up HTML can have status 200 even for scripts or JSON.
  const responses = await Promise.all(ASSETS.map(async url => {
    const response = await boundedFetch(new Request(url, { cache: "reload" }));
    if (!response.ok || !identified(response) || (url === "/"
      ? !await homeDocument(response, true)
      : response.headers.get("Content-Type")?.includes("text/html"))) throw Error("Servizio in attivazione");
    return response;
  }));
  const cache = await caches.open(SHELL);
  await Promise.all(ASSETS.map((url, i) => cache.put(url, responses[i])));
  await self.skipWaiting();
})()));
self.addEventListener("activate", (event) =>
  event.waitUntil(
    (async () => {
      for (const name of await caches.keys()) {
        if (name.startsWith("meteo-v5-shell-") && name !== SHELL)
          await caches.delete(name);
      }
      await self.clients.claim();
    })(),
  ),
);
async function enabled() {
  const response = await (
    await caches.open(SETTINGS)
  ).match("/offline-setting");
  return response ? (await response.text()) === "true" : true;
}
self.addEventListener("message", (event) => {
  if (event.data?.type === "OFFLINE_SETTING") {
    event.waitUntil(
      (async () => {
        await (
          await caches.open(SETTINGS)
        ).put(
          "/offline-setting",
          new Response(String(Boolean(event.data.enabled))),
        );
        if (!event.data.enabled) await caches.delete(DATA);
      })(),
    );
  }
});
self.addEventListener("fetch", (event) => {
  const request = event.request,
    url = new URL(request.url);
  if (url.origin !== self.location.origin || request.method !== "GET") return;
  const isData =
    url.pathname === "/api/v5/stations" ||
    /^\/api\/v5\/snapshot\/[a-z0-9-]{1,80}$/.test(url.pathname);
  if (isData) {
    event.respondWith(
      (async () => {
        try {
          // The browser's HTTP cache must not turn an offline copy into a
          // seemingly live response. The page requests data only every 600s.
          const response = await boundedFetch(request, { cache: "no-store" });
          if (!await validData(response, url)) throw Error("offline");
          if (await enabled())
            await (await caches.open(DATA)).put(request, response.clone());
          return response;
        } catch {
          const saved = (await enabled())
            ? await (await caches.open(DATA)).match(request)
            : null;
          if (!await validData(saved, url))
            return new Response('{"error":"Nessun dato offline"}', {
              status: 503,
              headers: { "Content-Type": "application/json" },
            });
          const headers = new Headers(saved.headers);
          headers.set("X-Meteo-Offline", "1");
          return new Response(await saved.arrayBuffer(), {
            status: 200,
            headers,
          });
        }
      })(),
    );
    return;
  }
  // Admin, push, private Pro responses and arbitrary navigations are never cached.
  const privateQuery = ["tab", "admin", "admin_token", "city"].some(key => url.searchParams.has(key));
  if (request.mode === "navigate" && url.pathname === "/" && !privateQuery) {
    const update = (async () => {
      const response = await boundedFetch(new Request("/", { cache: "no-store" }));
      if (!await homeDocument(response, true)) throw Error("Servizio in attivazione");
      await (await caches.open(SHELL)).put("/", response.clone());
      return response;
    })();
    event.waitUntil(update.catch(() => {}));
    event.respondWith((async () => {
      const saved = await (await caches.open(SHELL)).match("/");
      if (await homeDocument(saved)) return saved;
      try { return await update; } catch { return unavailable(); }
    })());
    return;
  }
  if (request.mode === "navigate" && (url.pathname.startsWith("/pro") || (url.pathname === "/" && privateQuery))) {
    event.respondWith((async () => {
      try {
        const response = await boundedFetch(request);
        if (!identified(response)) throw Error("Servizio in attivazione");
        return response;
      } catch { return unavailable(true); }
    })());
    return;
  }
  if (ASSETS.includes(url.pathname) && url.pathname !== "/") {
    event.respondWith(
      caches.open(SHELL)
        .then((cache) => cache.match(request, { ignoreSearch: true }))
        .then((saved) => saved || fetch(request)),
    );
  }
});
self.addEventListener("push", (event) => {
  event.waitUntil(
    (async () => {
      let data;
      try {
        data = event.data.json();
      } catch {
        return;
      }
      if (!data || typeof data.body !== "string") return;
      try {
        const history = await caches.open("meteo-v5-notifications");
        const saved = await history.match("/__meteo_notifications__");
        const rows = saved ? await saved.json() : [];
        rows.push({title: String(data.title || "Meteo Pro").slice(0,100), body: String(data.body || "").slice(0,350), received_at: new Date().toISOString()});
        await history.put("/__meteo_notifications__", new Response(JSON.stringify(rows.slice(-100)), {headers: {"Content-Type":"application/json"}}));
      } catch {}
      await self.registration.showNotification(
        String(data.title || "Meteo Pro").slice(0, 100),
        {
          body: data.body.slice(0, 350),
          icon: "/app/static/icon-192.png",
          badge: "/app/static/icon-192.png",
          tag: String(data.station || "") + "-" + String(data.kind || "meteo"),
          data: {
            url:
              "/?" +
              new URLSearchParams({
                station: String(data.station || ""),
                page: ["astronomy", "forecast", "stations", "personal"].includes(data.page)
                  ? data.page
                  : "today",
              }),
          },
        },
      );
    })(),
  );
});
self.addEventListener("notificationclick", (event) => {
  event.notification.close();
  event.waitUntil(
    self.clients.openWindow(
      new URL(event.notification.data?.url || "/", self.location.origin).href,
    ),
  );
});
