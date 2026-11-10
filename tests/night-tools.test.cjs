const assert = require("node:assert/strict");
const { readFileSync } = require("node:fs");
const { resolve } = require("node:path");
const { test } = require("node:test");
const vm = require("node:vm");

const context = vm.createContext({});
for (const file of ["sky-horizon", "integration", "panorama", "framing"]) {
  vm.runInContext(readFileSync(resolve(__dirname, "../static/v5/" + file + ".js"), "utf8"), context);
}
const I = context.MeteoIntegration, json = x => JSON.parse(JSON.stringify(x));

test("CSV handles Italian decimals, accepted poses and exact observing date", () => {
  const parsed = I.parse("\ufeffOggetto;Filtro;Scatti;Perfetti;Durata (s);Esito;Data serata\r\n" +
    'NGC 2070;QEF;20;10;"30,5";Perfetta;10/11/2026\r\n' +
    "NGC2070;QEF;9;8;120;Scartata;2026-11-10\nNGC 2070;QEF;9;8;120;Perfetta;2026-11-09\nM31;OSC;9;8;120;Perfetta;2026-11-10");
  const out = I.counts(parsed, I.suggest(parsed.headers), {id:"NGC2070", name:"Tarantola"}, "2026-11-10");
  assert.deepEqual(json(out), {rows:[["QEF",{frames:10,seconds:305}]], skipped:3});
  assert.throws(() => I.parse('filter,duration\n"QEF,300'), /CSV/);
});

test("revising a nightly import replaces it and same file cannot count twice", () => {
  const project = { target:"M81", filters:[{name:"L", acquired_seconds:100}] };
  const first = {day:"2026-11-10",batch:"Principale",digest:"a".repeat(64),filters:[{name:"L",seconds:120,frames:1}]};
  const imported = I.replace(project, first);
  assert.equal(I.totals(imported).filters[0].acquired_seconds, 220);
  assert.equal(I.totals(I.replace(imported, {...first,filters:[{name:"L",seconds:240,frames:2}]})).filters[0].acquired_seconds, 340);
  assert.throws(() => I.replace(imported, {...first, day:"2026-11-09"}), /già importato/);
  assert.equal(project.filters[0].acquired_seconds, 100);
});

test("panorama uses explicit north and elevation zero and rejects incomplete azimuth coverage", () => {
  const points = [0,.25,.5,.75].map(x => ({x,y:.4}));
  const m = context.MeteoPanorama.maskFromPoints(points, .25, .5);
  assert.deepEqual(json(m), {"0":18,"90":18,"180":18,"270":18});
  assert.throws(() => context.MeteoPanorama.maskFromPoints(points.slice(0,3), .25, .5), /tutto l’orizzonte/);
});

test("mosaic positions wrap in RA and share scale and orientation with the WCS", () => {
  const field = {width_deg:2,height_deg:1}, target = {ra_deg:359.9,dec_deg:20};
  const m = context.MeteoFraming.mosaic(field,target,2,1,.2,0);
  assert.equal(m.panels.length, 2);
  assert(m.panels[0].centre.ra < 1 && m.panels[1].centre.ra > 358);
  assert(m.wcs.CDELT1 < 0 && m.wcs.CDELT2 > 0);
  assert.equal(m.pixel(0),256);
  assert(m.panels.every(p => p.corners.flat().every(v => m.pixel(v) > 0 && m.pixel(v) < 512)));
  const rotated = context.MeteoFraming.mosaic(field,target,1,1,.2,90);
  assert(Math.abs(rotated.panels[0].angle - 90) < .001);
});
