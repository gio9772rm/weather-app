const assert = require("node:assert/strict");
const { readFileSync } = require("node:fs");
const { resolve } = require("node:path");
const { test } = require("node:test");
const vm = require("node:vm");

const context = vm.createContext({});
vm.runInContext(readFileSync(resolve(__dirname, "../static/v5/sky-horizon.js"), "utf8"), context);
const { parse, exportText, key } = context.MeteoHorizon;

test("imports a horizon with a continuous north seam and preserves negative altitudes", () => {
  const mask = parse("\uFEFF# Az Alt\r\n90  -4.5 # East\r\n270\t8,5\r\n");
  assert.equal(mask[0], 2);
  assert.equal(mask[90], -4.5);
  assert.equal(mask[270], 8.5);
  assert.deepEqual(parse(exportText(mask)), mask);
  assert.equal(parse("0 2\n180 -4\n360 5\n")[0], 5);
});

test("rejects a malformed file and keeps masks distinct by observing site", () => {
  assert.throws(() => parse("0 2\n360 4"), /due azimut/);
  assert.throws(() => parse("0 2\n181 91"), /fuori intervallo/);
  assert.throws(() => parse("0 2\nNord 6"), /due numeri/);
  assert.notEqual(key(null, "roma"), key(null, "comacchio"));
  assert.notEqual(key({kind: "city", geo_id: 123}, "roma"), key(null, "roma"));
});
