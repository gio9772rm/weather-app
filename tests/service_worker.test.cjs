const { test } = require('node:test');
const assert = require('node:assert/strict');
const vm = require('node:vm');
const fs = require('node:fs');

const origin = 'https://meteo.example';
const shell = 'meteo-v5-shell-12';
const key = request => new URL(typeof request === 'string' ? request : request.url, origin).pathname;
const home = () => new Response('<html><main id="content"></main><script src="/assets/v5/app.js"></script></html>', { headers: { 'Content-Type': 'text/html', 'X-Meteo-App': 'v5' } });
const loader = () => new Response('<html>Your app is almost live<script>location.reload()</script></html>', { headers: { 'Content-Type': 'text/html' } });
const snapshot = () => new Response(JSON.stringify({ station: { id: 'roma-primary' }, generated_at: '2026-10-14T20:00:00Z' }), { headers: { 'Content-Type': 'application/json' } });

function worker(fetcher) {
  const handlers = {}, stores = new Map();
  let skipped = 0;
  const caches = {
    async open(name) {
      if (!stores.has(name)) stores.set(name, new Map());
      const data = stores.get(name);
      return { async match(request) { return data.get(key(request))?.clone(); }, async put(request, response) { data.set(key(request), response.clone()); } };
    },
    async keys() { return [...stores.keys()]; },
    async delete(name) { return stores.delete(name); },
  };
  class AbsoluteRequest extends Request {
    constructor(input, options) { super(typeof input === 'string' ? new URL(input, origin) : input, options); }
  }
  vm.runInNewContext(fs.readFileSync('static/v5/sw.js', 'utf8'), {
    self: { location: { origin }, addEventListener(type, callback) { handlers[type] = callback; }, async skipWaiting() { skipped++; }, clients: { async claim() {} } },
    caches, fetch: fetcher, Request: AbsoluteRequest, Response, Headers, URL, URLSearchParams, AbortController,
    setTimeout: callback => setTimeout(callback, 5), clearTimeout,
  });
  function event(type, path = '/') {
    const pending = [];
    const result = { request: { url: origin + path, method: 'GET', mode: type === 'fetch' && path.startsWith('/api/') ? 'cors' : 'navigate' }, waitUntil(promise) { pending.push(promise); }, respondWith(promise) { result.response = promise; }, pending };
    handlers[type](result);
    return result;
  }
  return { caches, stores, event, skipped: () => skipped };
}

test('saved home opens while Render wakes, and wake-up HTML never replaces it', async () => {
  const w = worker(async () => loader());
  await (await w.caches.open(shell)).put('/', home());
  const e = w.event('fetch');
  assert.match(await (await e.response).text(), /id="content"/);
  await Promise.all(e.pending);
  assert.match(await (await (await w.caches.open(shell)).match('/')).text(), /id="content"/);
});

test('a stalled network is bounded and the saved home remains usable', async () => {
  const w = worker((request, options) => new Promise((resolve, reject) => options.signal.addEventListener('abort', () => reject(Error('timeout')))));
  await (await w.caches.open(shell)).put('/', home());
  const e = w.event('fetch');
  assert.match(await (await e.response).text(), /id="content"/);
  await Promise.all(e.pending);
});

test('real application home updates the shell', async () => {
  const w = worker(async () => home());
  const e = w.event('fetch');
  assert.equal((await e.response).status, 200);
  await Promise.all(e.pending);
  assert.ok(await (await w.caches.open(shell)).match('/'));
});

test('Render HTML from a JSON endpoint uses explicitly dated saved data', async () => {
  const w = worker(async () => loader());
  const path = '/api/v5/snapshot/roma-primary';
  await (await w.caches.open('meteo-v5-data')).put(path, snapshot());
  const response = await w.event('fetch', path).response;
  assert.equal(response.headers.get('X-Meteo-Offline'), '1');
  assert.equal((await response.json()).station.id, 'roma-primary');
});

test('invalid saved data and snapshots for another station are rejected', async () => {
  const w = worker(async () => snapshot());
  const path = '/api/v5/snapshot/secondary-one';
  await (await w.caches.open('meteo-v5-data')).put(path, loader());
  assert.equal((await w.event('fetch', path).response).status, 503);
});

test('Pro and legacy admin links never receive the public cached home', async () => {
  const w = worker(async () => loader());
  await (await w.caches.open(shell)).put('/', home());
  for (const path of ['/pro/?tab=precision', '/?tab=system&admin=test-only']) {
    const response = await w.event('fetch', path).response;
    assert.equal(response.status, 503);
    const text = await response.text();
    assert.match(text, /Riprova/);
    assert.doesNotMatch(text, /location\.reload\(\).*<\/script>|id="content"/);
  }
});

test('bad installation keeps the previous working shell and does not activate', async () => {
  const w = worker(async () => loader());
  await (await w.caches.open('meteo-v5-shell-11')).put('/', home());
  const result = await Promise.allSettled(w.event('install').pending);
  assert.equal(result[0].status, 'rejected');
  assert.equal(w.skipped(), 0);
  assert.equal(w.stores.has(shell), false);
  assert.equal(w.stores.has('meteo-v5-shell-11'), true);
});

test('verified installation activates the complete shell', async () => {
  const w = worker(async request => key(request) === '/' ? home() : new Response('asset', { headers: { 'Content-Type': 'application/javascript', 'X-Meteo-App': 'v5' } }));
  await Promise.all(w.event('install').pending);
  assert.equal(w.skipped(), 1);
  assert.ok(await (await w.caches.open(shell)).match('/'));
});

test('turning off saved weather data disables the JSON fallback', async () => {
  const w = worker(async () => loader());
  await (await w.caches.open('meteo-v5-settings')).put('/offline-setting', new Response('false'));
  await (await w.caches.open('meteo-v5-data')).put('/api/v5/snapshot/roma-primary', snapshot());
  const response = await w.event('fetch', '/api/v5/snapshot/roma-primary').response;
  assert.equal(response.status, 503);
  assert.equal(response.headers.get('X-Meteo-Offline'), null);
});
