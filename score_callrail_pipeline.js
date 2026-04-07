/**
 * Single-file scoring: transcripts → callrail_scored_calls.json (flat).
 * For month splits, aggregates, and charts use: node callrail_mom_pipeline.js
 */
const fs = require("fs");
const path = require("path");
const { scoreOneRecord, WASTE_TYPES, OPPORTUNITY_THRESHOLD, SWITCH_THRESHOLD } = require("./callrail-score-core");

const INPUT = path.join(__dirname, "callrail_transcripts_last60days.json");
const OUTPUT = path.join(__dirname, "callrail_scored_calls.json");

function loadTranscripts() {
  try {
    const raw = fs.readFileSync(INPUT, { encoding: "utf8", flag: "r" });
    const data = JSON.parse(raw);
    return Array.isArray(data) ? data : [];
  } catch {
    return [];
  }
}

function main() {
  const rows = loadTranscripts();
  const scored = [];
  for (let i = 0; i < rows.length; i++) {
    if (!rows[i] || typeof rows[i] !== "object") continue;
    try {
      const one = scoreOneRecord(rows[i], i);
      if (one) scored.push(one);
    } catch {
      /* skip */
    }
  }

  fs.writeFileSync(OUTPUT, JSON.stringify(scored, null, 2), "utf8");

  const n = scored.length;
  const opportunity_calls = scored.filter(
    (r) => r.call_type === "true_pi_opportunity" || r.opportunity_score >= OPPORTUNITY_THRESHOLD
  ).length;
  const waste_calls = scored.filter((r) => WASTE_TYPES.includes(r.call_type)).length;
  const switch_signal_calls = scored.filter((r) => r.attorney_switch_probability >= SWITCH_THRESHOLD).length;

  console.log("callrail scoring pipeline");
  console.log("  total_calls (scored):     " + n);
  console.log("  opportunity_calls (≥" + OPPORTUNITY_THRESHOLD + "):   " + opportunity_calls);
  console.log("  waste_calls:             " + waste_calls);
  console.log("  switch_signal_calls:     " + switch_signal_calls);
  console.log("  wrote: " + OUTPUT);
  console.log("  (full MoM + charts: npm run intel:build)");
}

main();
