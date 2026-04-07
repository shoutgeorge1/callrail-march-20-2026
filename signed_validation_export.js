/**
 * Hard validation export from data/callrail_enriched.json
 * Produces signed_case_proof.csv and top_sources_real.csv (does not alter enrich or dashboard).
 */
const fs = require("fs");
const path = require("path");

const ROOT = __dirname;
const DATA = path.join(ROOT, "data");
const IN_JSON = path.join(DATA, "callrail_enriched.json");
const OUT_PROOF = path.join(DATA, "signed_case_proof.csv");
const OUT_SOURCES = path.join(DATA, "top_sources_real.csv");

function csvEscape(s) {
  if (s == null || s === "") return "";
  const str = String(s);
  if (/[",\r\n]/.test(str)) return `"${str.replace(/"/g, '""')}"`;
  return str;
}

function matchTypeRank(t) {
  if (t === "strict") return 0;
  if (t === "24hr") return 1;
  if (t === "phone_only") return 2;
  return 99;
}

function sourceKey(c) {
  return c.source && String(c.source).trim() ? String(c.source).trim() : "(no source)";
}

function main() {
  if (!fs.existsSync(IN_JSON)) {
    console.error("Missing:", IN_JSON);
    process.exit(1);
  }

  let raw;
  try {
    raw = JSON.parse(fs.readFileSync(IN_JSON, "utf8"));
  } catch (e) {
    console.error("Failed to parse JSON:", e.message);
    process.exit(1);
  }

  const calls = Array.isArray(raw.calls) ? raw.calls : [];

  const signed = calls.filter((c) => c && c.signed_flag === true);
  signed.sort((a, b) => {
    const ra = matchTypeRank(a.match_type);
    const rb = matchTypeRank(b.match_type);
    if (ra !== rb) return ra - rb;
    const da = Number(a.match_delta_minutes);
    const db = Number(b.match_delta_minutes);
    const na = Number.isFinite(da) ? da : 0;
    const nb = Number.isFinite(db) ? db : 0;
    return na - nb;
  });

  const top50 = signed.slice(0, 50);

  const proofHeader = [
    "call_id",
    "customer_phone_number",
    "call_start_time",
    "source",
    "duration",
    "match_type",
    "match_delta_minutes",
    "lead_status",
    "case_type",
    "marketing_source",
  ];

  const proofLines = [proofHeader.join(",")];
  for (const c of top50) {
    const row = [
      csvEscape(c.call_id),
      csvEscape(c.customer_phone_number),
      csvEscape(c.call_start_time || c.start_time || ""),
      csvEscape(c.source),
      csvEscape(c.duration),
      csvEscape(c.match_type),
      csvEscape(c.match_delta_minutes),
      csvEscape(c.lead_status),
      csvEscape(c.case_type),
      csvEscape(c.marketing_source),
    ];
    proofLines.push(row.join(","));
  }
  fs.writeFileSync(OUT_PROOF, proofLines.join("\n"), "utf8");

  const bySource = new Map();
  for (const c of calls) {
    if (!c || typeof c !== "object") continue;
    const src = sourceKey(c);
    if (!bySource.has(src)) {
      bySource.set(src, {
        source: src,
        calls: 0,
        strict_matches: 0,
        matches_24hr: 0,
        signed_flag_count: 0,
      });
    }
    const o = bySource.get(src);
    o.calls++;
    if (c.match_type === "strict") o.strict_matches++;
    else if (c.match_type === "24hr") o.matches_24hr++;
    if (c.signed_flag === true) o.signed_flag_count++;
  }

  const sourceRows = [...bySource.values()].sort((a, b) => b.signed_flag_count - a.signed_flag_count);

  const srcHeader = ["source", "calls", "strict_matches", "matches_24hr", "signed_flag_count"];
  const srcLines = [srcHeader.join(",")];
  for (const r of sourceRows) {
    srcLines.push(
      [r.source, r.calls, r.strict_matches, r.matches_24hr, r.signed_flag_count].map(csvEscape).join(",")
    );
  }
  fs.writeFileSync(OUT_SOURCES, srcLines.join("\n"), "utf8");

  console.log("Signed-case validation export");
  console.log("  signed rows in data:     ", signed.length);
  console.log("  wrote proof (max 50):   ", OUT_PROOF);
  console.log("  source rollup rows:     ", sourceRows.length, "->", OUT_SOURCES);
}

main();
