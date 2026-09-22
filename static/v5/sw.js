"use strict";
const SHELL = "meteo-v5-shell-2",
  DATA = "meteo-v5-data",
  SETTINGS = "meteo-v5-settings";
const ASSETS = [
  "/",
  "/assets/v5/app.css",
  "/assets/v5/app.js",
  "/assets/v5/icon.svg",
  "/assets/v5/manifest.webmanifest",
  "/app/static/icon-192.png",
  "/app/static/icon-512.png",
  "/app/static/icon-512-maskable.png",
];
self.addEventListener("install", (event) =>
  event.waitUntil(
    caches
      .open(SHELL)
      .then((cache) => cache.addAll(ASSETS))
      .then(() => self.skipWaiting()),
  ),
);
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
          const response = await fetch(request, { cache: "no-store" });
          if (!response.ok) throw Error("offline");
          if (await enabled())
            await (await caches.open(DATA)).put(request, response.clone());
          return response;
        } catch {
          const saved = (await enabled())
            ? await (await caches.open(DATA)).match(request)
            : null;
          if (!saved)
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
  if (request.mode === "navigate" && url.pathname === "/") {
    event.respondWith(fetch(request).catch(() => caches.match("/")));
    return;
  }
  if (request.mode === "navigate" && url.pathname.startsWith("/pro")) {
    event.respondWith(
      fetch(request).catch(
        () =>
          new Response(
            '<!doctype html><html lang="it"><meta name="viewport" content="width=device-width"><title>Meteo Pro offline</title><body><h1>Gli strumenti Pro richiedono la connessione</h1><p>Puoi consultare la fotografia salvata nella <a href="/">home Meteo Pro</a>.</p></body></html>',
            { headers: { "Content-Type": "text/html;charset=utf-8" } },
          ),
      ),
    );
    return;
  }
  if (ASSETS.includes(url.pathname) && url.pathname !== "/") {
    event.respondWith(
      caches.match(request).then((saved) => saved || fetch(request)),
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
                page: ["astronomy", "forecast", "stations"].includes(data.page)
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
