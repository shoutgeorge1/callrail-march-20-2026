/**
 * Print % of records with parseable call_start_time, start_time, created_at.
 * Usage: node scripts/report_timestamp_coverage.js [path-to-json ...]
 */
const fs = require("fs");
const path = require("path");

function validTime(v) {
  if (v == null) return false;
  const s = String(v).trim();
  if (!s) return false;
  const ms = Date.parse(s);
  return !Number.isNaN(ms);
}

function analyzeRecords(arr, label) {
  if (!Array.isArray(arr)) {
    console.log(label + ": not an array, skip");
    return;
  }
  const n = arr.length;
  if (n === 0) {
    console.log(label + ": 0 records");
    return;
  }
  let nCallStart = 0;
  let nStart = 0;
  let nCreated = 0;
  for (let i = 0; i < n; i++) {
    const r = arr[i];
    if (validTime(r.call_start_time)) nCallStart++;
    if (validTime(r.start_time)) nStart++;
    if (validTime(r.created_at)) nCreated++;
  }
  const pct = (x) => ((100 * x) / n).toFixed(2);
  console.log("\n" + label + " (n = " + n + ")");
  console.log("  call_start_time (valid / parseable): " + pct(nCallStart) + "%");
  console.log("  start_time (valid / parseable):       " + pct(nStart) + "%");
  console.log("  created_at (valid / parseable):       " + pct(nCreated) + "%");
}

const root = path.join(__dirname, "..");
const defaultFiles = [
  path.join(root, "callrail_transcripts_last60days.json"),
  path.join(root, "callrail_scored_calls_latest.json"),
  path.join(root, "callrail_scored_calls.json"),
];

const files = process.argv.slice(2).length ? process.argv.slice(2) : defaultFiles;

for (const file of files) {
  if (!fs.existsSync(file)) {
    console.log("\n(missing) " + file);
    continue;
  }
  let data;
  try {
    data = JSON.parse(fs.readFileSync(file, "utf8"));
  } catch (e) {
    console.log("\n(read error) " + file + ": " + e.message);
    continue;
  }
  const arr = Array.isArray(data) ? data : Array.isArray(data.calls) ? data.calls : null;
  analyzeRecords(arr, path.basename(file));
}
