// Service worker minimo: serve solo a rendere l'app installabile
// (PWA / TWA Android). Nessuna cache di dati live: rete sempre in prima
// battuta, per non mostrare mai meteo non aggiornato.
self.addEventListener("install", () => self.skipWaiting());
self.addEventListener("activate", (event) => event.waitUntil(self.clients.claim()));
self.addEventListener("fetch", (event) => {
  event.respondWith(fetch(event.request).catch(() => caches.match(event.request)));
});
