"use strict";

const fs = require("fs");
const path = require("path");

const ROOT = __dirname;
const enrichedPath = path.join(ROOT, "data", "callrail_enriched.json");
const outPath = path.join(ROOT, "js", "case-driver-snapshot.js");

function srcKey(c) {
  const s = c.source;
  return s && String(s).trim() ? String(s).trim() : "(no source)";
}

function aggregate(calls) {
  const m = {};
  for (const c of calls) {
    const k = srcKey(c);
    if (!m[k]) m[k] = { source: k, calls: 0, strict: 0, signed: 0, matched: 0 };
    const o = m[k];
    o.calls++;
    if (c.match_type === "strict") o.strict++;
    if (c.signed_flag) o.signed++;
    if (c.lead_created) o.matched++;
  }
  return m;
}

function matchRank(mt) {
  if (mt === "strict") return 0;
  if (mt === "24hr") return 1;
  if (mt === "phone_only") return 2;
  return 3;
}

function deltaVal(c) {
  const d = c.match_delta_minutes;
  if (d == null) return Infinity;
  const n = Number(d);
  return Number.isFinite(n) ? n : Infinity;
}

function main() {
  if (!fs.existsSync(enrichedPath)) {
    console.error("Missing", enrichedPath);
    process.exit(1);
  }
  const data = JSON.parse(fs.readFileSync(enrichedPath, "utf8"));
  const calls = Array.isArray(data.calls) ? data.calls : [];

  let matched = 0;
  let signed = 0;
  let strict = 0;
  for (const c of calls) {
    if (c.lead_created) matched++;
    if (c.signed_flag) signed++;
    if (c.match_type === "strict") strict++;
  }

  const summaryFile = data.summary || {};
  const agg = aggregate(calls);
  const list = Object.values(agg);

  const working = list
    .filter((x) => x.signed > 0)
    .sort((a, b) => b.signed - a.signed || b.calls - a.calls);

  const notWorking = list
    .filter((x) => x.calls >= 10 && x.signed === 0)
    .sort((a, b) => b.calls - a.calls);

  const proofCalls = calls
    .filter((c) => c.signed_flag === true)
    .sort((a, b) => {
      const rr = matchRank(a.match_type) - matchRank(b.match_type);
      if (rr !== 0) return rr;
      return deltaVal(a) - deltaVal(b);
    })
    .slice(0, 15)
    .map((c) => ({
      call_start_time: c.call_start_time,
      start_time: c.start_time,
      created_at: c.created_at,
      customer_phone_number: c.customer_phone_number,
      source: c.source,
      match_type: c.match_type,
      match_delta_minutes: c.match_delta_minutes,
      lead_status: c.lead_status,
    }));

  const snapshot = {
    bundled_from: "data/callrail_enriched.json",
    enriched_generated_at: data.generated_at || null,
    built_at: new Date().toISOString(),
    summary: {
      total_calls: calls.length,
      matched_leads: matched,
      signed_cases: signed,
      strict_matches: strict,
      calls_with_customer_phone:
        summaryFile.calls_with_customer_phone != null
          ? summaryFile.calls_with_customer_phone
          : calls.filter((c) => String(c.customer_phone_number || "").trim()).length,
    },
    working,
    not_working: notWorking,
    proof_calls: proofCalls,
  };

  const banner =
    "/* Auto-generated from data/callrail_enriched.json — refresh: npm run report:case-driver */\n";
  const body = `window.CASE_DRIVER_SNAPSHOT=${JSON.stringify(snapshot)};\n`;
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, banner + body, "utf8");
  console.log("Wrote", path.relative(ROOT, outPath), `(${calls.length} calls)`);
}

main();
