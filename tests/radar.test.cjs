const {test} = require('node:test');
const assert = require('node:assert/strict');
const vm = require('node:vm');
const fs = require('node:fs');
const context = {window: {}, URLSearchParams, Date, Number, esc: String,
  num: (v) => v == null ? '—' : String(v), dateTime: v => v || 'non disponibile'};
vm.createContext(context);
vm.runInContext(fs.readFileSync('static/v5/radar.js', 'utf8'), context);
const radar = context.window.MeteoRadar;

test('expert maps separate observations from forecasts and use public city centres', () => {
  for (const [role, lat, lon] of [['primary', '41.90', '12.50'], ['secondary', '44.69', '12.18']]) {
    for (const [layer, product] of [['radar', 'radar'], ['satellite', 'satellite'], ['clouds', 'ecmwf']]) {
      const url = new URL(radar.embedUrl(role, layer));
      assert.equal(url.origin, 'https://embed.windy.com');
      assert.equal(url.searchParams.get('lat'), lat);
      assert.equal(url.searchParams.get('lon'), lon);
      assert.equal(url.searchParams.get('overlay'), layer);
      assert.equal(url.searchParams.get('product'), product);
      assert.equal(url.searchParams.get('marker'), null);
    }
  }
  assert.equal(radar.embedUrl('unknown', 'radar'), null);
  assert.equal(radar.embedUrl('primary', '//other.example'), null);
  assert.equal(radar.embedUrl('__proto__', 'radar'), null);
});

test('old, missing and future radar timestamps cannot be labelled recent', () => {
  const now = Date.parse('2026-09-30T20:00:00Z');
  assert.equal(radar.status('2026-09-30T19:50Z', now), 'Dato recente');
  assert.equal(radar.status('2026-09-30T18:00Z', now), 'Dato datato');
  for (const v of [null, undefined, '', 'invalid', '2026-10-01T00:00Z']) {
    assert.equal(radar.status(v, now), 'Orario non disponibile');
  }
});

test('lightning counts need their own timestamp, including archived old-schema snapshots', () => {
  const missing = radar.dpcCard({observed_at: '2026-09-30T20:00Z', lightning_25km: 7});
  assert.match(missing, /fotogramma non disponibile/);
  assert.match(missing, /Fulmini entro 25 km<\/span><b>—<\/b>/);
  const available = radar.dpcCard({lightning_observed_at: '2026-09-30T20:00Z', lightning_25km: 0});
  assert.match(available, /Fulmini entro 25 km<\/span><b>0<\/b>/);
});
