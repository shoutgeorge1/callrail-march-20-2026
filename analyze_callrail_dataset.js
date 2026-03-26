/**
 * Read-only analysis of callrail_transcripts_last60days.json
 * Does not modify the source file or call CallRail.
 */
const fs = require("fs");
const path = require("path");

const INPUT = path.join(process.cwd(), "callrail_transcripts_last60days.json");
const OUTPUT = path.join(process.cwd(), "callrail_dataset_summary.json");

const METADATA_CANDIDATES = [
  "duration",
  "direction",
  "source",
  "utm_source",
  "utm_medium",
  "utm_campaign",
  "landing_page",
  "landing",
  "tags",
  "tag_list",
  "customer_phone_number",
  "caller_phone",
  "call_status",
  "lead_status",
  "answered",
  "recording",
  "recording_url",
  "recording_player",
  "transcription",
  "transcript",
  "conversational_transcript",
];

function detectTranscriptFieldName(keys) {
  const lower = new Set(keys.map((k) => k.toLowerCase()));
  if (lower.has("transcription")) return "transcription";
  if (lower.has("conversational_transcript")) return "conversational_transcript";
  if (lower.has("transcript")) return "transcript";
  for (const k of keys) {
    if (/transcript/i.test(k)) return k;
  }
  return null;
}

function hasTranscript(record, fieldName) {
  if (!fieldName || !Object.prototype.hasOwnProperty.call(record, fieldName)) return false;
  const v = record[fieldName];
  if (v == null) return false;
  if (typeof v !== "string") return false;
  return v.trim().length > 0;
}

function typeLabel(v) {
  if (v === null) return "null";
  if (Array.isArray(v)) return "array";
  return typeof v;
}

function main() {
  const raw = fs.readFileSync(INPUT, { encoding: "utf8", flag: "r" });
  const data = JSON.parse(raw);

  if (!Array.isArray(data)) {
    console.error("Expected top-level JSON array.");
    process.exit(1);
  }

  const total_calls = data.length;
  const fieldSet = new Set();
  for (const row of data) {
    if (row && typeof row === "object" && !Array.isArray(row)) {
      for (const k of Object.keys(row)) fieldSet.add(k);
    }
  }
  const detected_fields = [...fieldSet].sort();

  const first = data[0];
  const firstKeys =
    first && typeof first === "object" && !Array.isArray(first) ? Object.keys(first) : [];

  const transcript_field_name = detectTranscriptFieldName(detected_fields);

  let calls_with_transcripts = 0;
  let calls_without_transcripts = 0;
  for (const row of data) {
    if (hasTranscript(row, transcript_field_name)) calls_with_transcripts += 1;
    else calls_without_transcripts += 1;
  }

  const presentMeta = METADATA_CANDIDATES.filter((k) => fieldSet.has(k));
  const absentMeta = METADATA_CANDIDATES.filter((k) => !fieldSet.has(k));

  const summary = {
    total_calls,
    calls_with_transcripts,
    calls_without_transcripts,
    detected_fields,
    transcript_field_name,
  };

  fs.writeFileSync(OUTPUT, JSON.stringify(summary, null, 2), "utf8");

  console.log("=== CallRail dataset summary (local file only) ===\n");
  console.log(`Source: ${INPUT} (read-only, not modified)\n`);
  console.log(`Total number of call records: ${total_calls}\n`);

  console.log("First call object — keys and value types:");
  if (!first || typeof first !== "object") {
    console.log("  (no first record)");
  } else {
    for (const key of firstKeys) {
      const v = first[key];
      let extra = "";
      if (key === transcript_field_name && typeof v === "string") {
        extra = ` (length ${v.length} chars)`;
      }
      console.log(`  ${key}: ${typeLabel(v)}${extra}`);
    }
  }

  console.log("\nTranscript text field:");
  console.log(
    transcript_field_name
      ? `  "${transcript_field_name}"`
      : "  (no transcript-like field detected)"
  );

  console.log("\nMetadata / attribution fields — present in this file:");
  console.log(presentMeta.length ? presentMeta.map((k) => `  • ${k}`).join("\n") : "  (none of the common set)");

  console.log("\nMetadata / attribution fields — not present (common CallRail names checked):");
  console.log(absentMeta.map((k) => `  • ${k}`).join("\n"));

  console.log("\n--- Counts ---");
  console.log(`Calls with non-empty transcript: ${calls_with_transcripts}`);
  console.log(`Calls with missing / empty transcript: ${calls_without_transcripts}`);

  console.log(`\nWrote: ${OUTPUT}`);
}

main();
