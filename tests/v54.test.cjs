const { test } = require('node:test');
const assert = require('node:assert/strict');
require('../static/v5/v54.js');
require('../static/v5/nina.js');
const now = Date.parse('2026-09-28T19:05:00Z');
const iso = n => new Date(n).toISOString();
const hours = () => Array.from({length: 12}, (_, i) => ({valid_time: iso(Math.floor(now / 3600000) * 3600000 + i * 3600000), issued_at: iso(now - 3600000), temp_c: 20, rain_mm: 0, precip_probability: 10, wind_kmh: 6, wind_gust_kmh: 10, clouds: 10}));
const snapshot = () => ({forecast: hours(), generated_at: iso(now), observed_at: iso(now), window_validation: {n: 0}});

test('missing data and absent hours never become dry or clear', () => {
  const s = snapshot(); s.forecast[1].rain_mm = null; s.forecast[2].precip_probability = null;
  s.forecast[3].clouds = null; s.forecast.splice(4, 1);
  const result = MeteoV54.summarize(s, now);
  assert.equal(result.rainHours, 3); assert.equal(result.skyHours, 4);
  assert.equal(result.clearing, null); assert.equal(result.rain, undefined);
  assert.equal(MeteoV54.reliability(s, now).missing, 4);
  assert.equal(MeteoV54.summarize({}, now).rainHours, 0);
});
test('changes need contiguous hours; a rain amount is not hidden by missing probability', () => {
  const s = snapshot(); s.forecast[0].clouds = 80; s.forecast[3].rain_mm = .4; s.forecast[3].precip_probability = null;
  assert.equal(MeteoV54.summarize(s, now).clearing.valid_time, s.forecast[1].valid_time);
  assert.equal(MeteoV54.summarize(s, now).rain.valid_time, s.forecast[3].valid_time);
  s.forecast.splice(1, 1);
  assert.equal(MeteoV54.summarize(s, now).clearing, null);
});
test('data age, model spread and statistical sample remain independent', () => {
  const s = snapshot(); s.observed_at = iso(now - 30 * 60000);
  s.model_spread = {evaluated_at: iso(now), rows: [{valid_time: s.forecast[0].valid_time, model_count: 2, spread_c: 4}, {valid_time: iso(now - 86400000), model_count: 2, spread_c: 99}]};
  const r = MeteoV54.reliability(s, now);
  assert.equal(r.oldObservation, true); assert.equal(r.oldSnapshot, false); assert.equal(r.spread, 4); assert.equal(r.sample, 0);
  assert.equal(MeteoV54.reliability(s, now, true).spread, null);
  assert.equal(MeteoV54.reliability(s, now + 21 * 60000).spread, null);
  s.forecast[0].issued_at = null;
  assert.equal(MeteoV54.reliability(s, now).forecastStale, true);
});
const fixture = () => ({
  plan: {profile: 'deep_sky', generated_at: iso(now), timezone: 'Europe/Rome', start: '2026-09-28T19:15:00Z', end: '2026-09-29T04:00:00Z', schedule: {blocks: [
    {target: 'Northern test', prepare_start: '2026-09-28T19:15:00Z', start: '2026-09-28T19:45:00Z', end: '2026-09-28T22:00:00Z'},
    {target: 'Southern test', prepare_start: '2026-09-28T22:00:00Z', start: '2026-09-28T22:15:00Z', end: '2026-09-29T04:00:00Z'}]}},
  config: {profile: 'deep_sky', rotation: 105},
  catalog: [{name: 'Northern test', ra_deg: 359.99999999, dec_deg: 41.25}, {name: 'Southern test', ra_deg: 249.65062, dec_deg: -.25}],
  settings: {exposure: 180, gain: -1, offset: -1, binning: 1, meridianFlip: true}
});
const build = f => MeteoNina.sequence(f.plan, f.config, f.catalog, f.settings, now);
function nodes(value) { return value && typeof value === 'object' ? [value, ...Object.values(value).flatMap(nodes)] : []; }
test('native NINA sequence has three root areas, resolvable unique references and bounded exposure loops', () => {
  const result = JSON.parse(JSON.stringify(build(fixture()))), all = nodes(result);
  const ids = all.filter(n => n.$id).map(n => n.$id);
  assert.equal(ids.length, new Set(ids).size);
  assert.ok(all.filter(n => n.$ref).every(n => ids.includes(n.$ref)));
  assert.match(result.$type, /SequenceRootContainer/);
  assert.deepEqual(result.Items.$values.map(n => n.Name), ['Start', 'Targets', 'End']);
  const targets = result.Items.$values[1].Items.$values;
  assert.equal(targets.length, 2);
  for (const t of targets) {
    assert.equal(t.Conditions.$values.find(c => c.$type.includes('LoopCondition')).Iterations, 1);
    assert.ok(t.Conditions.$values.some(c => c.$type.includes('TimeCondition')));
    const loop = t.Items.$values.at(-1);
    assert.match(loop.Conditions.$values[0].$type, /TimeCondition/);
    assert.equal(loop.Items.$values[0].ImageType, 'LIGHT');
    assert.equal(loop.Items.$values[0].ExposureTime, 180);
    assert.equal(loop.Items.$values[0].ExposureCount, 0);
    assert.equal(loop.Triggers.$values.length, 1);
    assert.equal(t.Items.$values[1].ErrorBehavior, 1); // Skip failed target, never expose after a failed centering.
  }
  assert.equal(targets[0].Target.InputCoordinates.RAHours, 0); // rounded 24h wraps
  const c = targets[1].Target.InputCoordinates;
  assert.equal(c.NegativeDec, true); assert.equal(c.DecDegrees, 0); assert.equal(c.DecMinutes, 15);
  assert.equal(targets[1].Items.$values[2].Hours, 0); assert.equal(targets[1].Items.$values[2].Minutes, 15);
  assert.equal(targets[1].Items.$values.at(-1).Conditions.$values[0].Hours, 6);
  assert.ok(all.filter(n => n.$type).every(n => /, (NINA\.(Sequencer|Astrometry|Core)|System.ObjectModel)$/.test(n.$type)));
});
test('NINA rejects stale plans, expired/overlapping blocks, invalid parameters and missing coordinates', () => {
  for (const mutate of [
    f => f.plan.generated_at = iso(now - 21 * 60000),
    f => f.plan.schedule.blocks[0].start = iso(now - 60000),
    f => f.plan.schedule.blocks[1].prepare_start = '2026-09-28T21:00Z',
    f => f.settings.exposure = 3601,
    f => f.settings.gain = '',
    f => f.settings.binning = 1.5,
    f => f.catalog[1].dec_deg = null,
    f => f.plan.profile = 'visual',
    f => f.plan.timezone = undefined,
    f => f.plan.schedule.blocks = []
  ]) { const f = fixture(); mutate(f); assert.throws(() => build(f)); }
});
test('NINA rejects ambiguous daylight-saving nights and a second night', () => {
  let f = fixture(); f.plan.schedule.blocks[1].end = '2026-09-29T22:00Z'; f.plan.end = f.plan.schedule.blocks[1].end;
  assert.throws(() => build(f), /sola notte/);
  f = fixture(); f.plan.schedule.blocks = [{target:'Northern test',prepare_start:'2026-10-24T20:00Z',start:'2026-10-24T21:00Z',end:'2026-10-25T04:00Z'}];
  f.plan.start = f.plan.schedule.blocks[0].prepare_start; f.plan.end = f.plan.schedule.blocks[0].end;
  assert.throws(() => build(f), /ora legale/);
});
