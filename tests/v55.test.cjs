const test = require("node:test");
const assert = require("node:assert/strict");
const vm = require("node:vm");
const fs = require("node:fs");

function view(data, offline = false) {
  const ctx = { data, offline, Date, dayLabel: v => v, clock: () => "12:00",
    finite: v => v !== null && v !== undefined && Number.isFinite(Number(v)),
    num: v => v === null || v === undefined ? "—" : String(v),
    esc: v => String(v).replaceAll("<", "&lt;").replaceAll(">", "&gt;") };
  vm.createContext(ctx);
  vm.runInContext(fs.readFileSync("static/v5/v55.js", "utf8"), ctx);
  return ctx.MeteoV55;
}

test("missing statistical products are pending, not evidence of success", () => {
  const v = view({});
  assert.match(v.readinessCard(), /in preparazione/);
  assert.match(v.alertCard(), /in preparazione/);
  assert.doesNotMatch(v.alertCard(), /100%/);
});

test("a ready study never claims active calibration; offline and untrusted labels are explicit", () => {
  const v = view({ station: { name: "<Roma>" }, window_validation: {
    evaluated_at: new Date().toISOString(), n: 200,
    collection: { cycles: 120, retention_days: 90 },
    readiness: { status: "ready_for_study", requirements:
      ["<script>", "<SCRIPT>", "<ScRiPt src=x>"].map(label => ({ label, met: true })) }
  } }, true);
  const html = v.readinessCard();
  assert.match(html, /REQUISITI PER LO STUDIO RAGGIUNTI/);
  assert.match(html, /Calibrazione delle probabilità non attiva/);
  assert.match(html, /copia salvata/);
  assert.match(html, /&lt;Roma&gt;/);
  assert.match(html, /&lt;SCRIPT&gt;/);
  assert.match(html, /&lt;ScRiPt src=x&gt;/);
  assert.doesNotMatch(html, /<script\b/i);
});

test("undefined precision stays unavailable while a real zero recall remains zero", () => {
  const html = view({ alert_verification: { period_days: 30, evaluated_at: new Date().toISOString(),
    rows: [{ kind: "rain", threshold: 60, precision_percent: null, recall_percent: 0 }] } }).alertCard();
  assert.match(html, /<td>—<\/td><td>0%<\/td>/);
  assert.match(html, /Assenze corrette/);
  assert.match(html, /Mancate/);
});

test("a validated local correction is explicit and its offline copy is suspended", () => {
  const data = { station: { name: "Roma" }, window_validation: {
    evaluated_at: new Date().toISOString(), n: 200,
    collection: { cycles: 120 }, readiness: { requirements: [] }
  }, window_calibration: { status: "active", evaluated_at: new Date().toISOString(),
    expires_at: new Date(Date.now() + 86400000).toISOString(), reason: "<script>x</script>",
    validation: { raw_brier: 0.2, calibrated_brier: 0.1, reference_brier: 0.3,
      holdout: { n: 60, days: 7 }, improvement_percent: 50 }
  } };
  const html = view(data).readinessCard();
  assert.match(html, /Calibrazione locale attiva/);
  assert.match(html, /Brier originale/);
  assert.doesNotMatch(html, /<script\b/i);
  const offline = view(data, true).readinessCard();
  assert.match(offline, /copia offline: usa le frequenze originali/);
  assert.doesNotMatch(offline, /Calibrazione locale attiva/);
});
