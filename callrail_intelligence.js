require("dotenv").config();
const axios = require("axios");
const fs = require("fs");
const path = require("path");
const createCsvWriter = require("csv-writer").createObjectCsvWriter;
const OpenAI = require("openai");

const CALLRAIL_API_KEY = process.env.CALLRAIL_API_KEY;
const CALLRAIL_ACCOUNT_ID = process.env.CALLRAIL_ACCOUNT_ID;
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;

const BASE = `https://api.callrail.com/v3/a/${CALLRAIL_ACCOUNT_ID}`;

function requireEnv() {
  const missing = [];
  if (!CALLRAIL_API_KEY) missing.push("CALLRAIL_API_KEY");
  if (!CALLRAIL_ACCOUNT_ID) missing.push("CALLRAIL_ACCOUNT_ID");
  if (!OPENAI_API_KEY) missing.push("OPENAI_API_KEY");
  if (missing.length) {
    console.error("Missing env: " + missing.join(", "));
    process.exit(1);
  }
}

function authHeaders() {
  return { Authorization: `Token token="${CALLRAIL_API_KEY}"` };
}

function isoDate(d) {
  return d.toISOString().slice(0, 10);
}

function dateRangeLast7Days() {
  const end = new Date();
  const start = new Date();
  start.setDate(start.getDate() - 7);
  return { start_date: isoDate(start), end_date: isoDate(end) };
}

function transcriptFromDetail(detail) {
  const t = detail.transcription;
  if (t != null && String(t).trim()) return String(t).trim();
  const ct = detail.conversational_transcript;
  if (ct == null) return "";
  if (typeof ct === "string") {
    const s = ct.trim();
    if (!s) return "";
    try {
      const parsed = JSON.parse(s);
      return transcriptFromConversational(parsed);
    } catch {
      return s;
    }
  }
  if (Array.isArray(ct)) return transcriptFromConversational(ct);
  return "";
}

function transcriptFromConversational(parts) {
  if (!Array.isArray(parts)) return "";
  return parts
    .map((p) => {
      if (!p || typeof p !== "object") return "";
      const sp = p.speaker ? `${p.speaker}: ` : "";
      return sp + (p.phrase || p.text || "");
    })
    .filter(Boolean)
    .join("\n")
    .trim();
}

async function fetchAllCallSummaries() {
  const { start_date, end_date } = dateRangeLast7Days();
  const summaries = [];
  let nextUrl =
    `${BASE}/calls.json?` +
    new URLSearchParams({
      start_date,
      end_date,
      per_page: "100",
      relative_pagination: "true",
    }).toString();
  let pageNum = 1;

  while (nextUrl) {
    console.log(`Fetching page ${pageNum}`);
    const { data } = await axios.get(nextUrl, { headers: authHeaders() });
    const calls = data.calls || [];
    summaries.push(...calls);
    if (data.has_next_page && data.next_page) {
      nextUrl = data.next_page;
      pageNum += 1;
    } else {
      nextUrl = null;
    }
  }
  return summaries;
}

async function fetchCallDetail(callId) {
  const url = `${BASE}/calls/${callId}.json`;
  const { data } = await axios.get(url, {
    headers: authHeaders(),
    params: {
      fields:
        "transcription,conversational_transcript,recording_player,tracking_phone_number,source,duration,id",
    },
  });
  return data;
}

function recordingValue(detail) {
  return detail.recording || detail.recording_player || "";
}

async function scoreTranscript(openai, transcript, meta) {
  const system = `You score legal intake phone calls. Reply with one JSON object only (no markdown). Keys and types:
call_type: string — classify the matter (e.g. "Severe injury", "PD only", "Other PI", "Not a case").
qualified_score: number 0–100 lead quality for a plaintiff firm.
case_value_estimate: string — brief estimate band (e.g. "Low", "Medium", "High", "N/A").
attorney_switch_probability: number 0–1 chance caller may switch attorneys.
intake_quality: string — "Poor" | "Fair" | "Good" | "Excellent" for how the firm handled intake.
emotional_intensity: string — "Low" | "Moderate" | "High".
notes: string — 1–3 sentences on facts, risks, and follow-ups.`;

  const user = `Call metadata (JSON):\n${JSON.stringify(meta)}\n\nTranscript:\n${transcript}`;

  const completion = await openai.chat.completions.create({
    model: process.env.OPENAI_MODEL || "gpt-4o-mini",
    response_format: { type: "json_object" },
    messages: [
      { role: "system", content: system },
      { role: "user", content: user },
    ],
  });

  const raw = completion.choices[0]?.message?.content || "{}";
  return JSON.parse(raw);
}

function normalizeCallType(s) {
  return String(s || "").toLowerCase();
}

function isSevereInjury(callType) {
  const t = normalizeCallType(callType);
  return (
    t.includes("severe") ||
    t.includes("catastrophic") ||
    t.includes("wrongful death") ||
    t.includes("significant injury")
  );
}

function isPdOnly(callType) {
  const t = normalizeCallType(callType);
  return (
    t.includes("pd only") ||
    t.includes("property damage only") ||
    (t.includes("property") && t.includes("damage") && !t.includes("injury"))
  );
}

function topRiskPattern(rows) {
  const patterns = [
    { label: "Incomplete fact-finding", re: /\b(didn'?t ask|failed to ask|missed|no question|forgot to ask)\b/i },
    { label: "Low empathy / tone issues", re: /\b(cold|rude|dismissive|empathy|tone|hurried)\b/i },
    { label: "Follow-up / callback gaps", re: /\b(callback|follow[- ]?up|call back|never called|didn'?t call back)\b/i },
    { label: "Attorney shopping signals", re: /\b(other lawyer|already have|switch|second opinion|unhappy with)\b/i },
    { label: "Documentation / evidence gaps", re: /\b(no photos|no police report|no medical|documentation)\b/i },
  ];
  const counts = Object.fromEntries(patterns.map((p) => [p.label, 0]));
  for (const r of rows) {
    const n = r.notes || "";
    for (const p of patterns) {
      if (p.re.test(n)) counts[p.label] += 1;
    }
  }
  let best = "None detected";
  let max = 0;
  for (const p of patterns) {
    if (counts[p.label] > max) {
      max = counts[p.label];
      best = p.label;
    }
  }
  return max === 0 ? "None detected" : `${best} (${max} calls)`;
}

async function main() {
  requireEnv();
  const openai = new OpenAI({ apiKey: OPENAI_API_KEY });

  const summaries = await fetchAllCallSummaries();
  const rows = [];

  for (const summary of summaries) {
    const callId = summary.id;
    let detail;
    try {
      detail = await fetchCallDetail(callId);
    } catch (e) {
      console.error(`Detail fetch failed for ${callId}:`, e.message);
      continue;
    }

    const transcript = transcriptFromDetail(detail);
    if (!transcript) continue;

    const record = {
      call_id: detail.id ?? callId,
      tracking_phone_number: detail.tracking_phone_number ?? "",
      source: detail.source ?? "",
      duration: detail.duration ?? "",
      recording: recordingValue(detail),
      transcription: transcript,
    };

    console.log(`Scoring call ${record.call_id}`);
    let score;
    try {
      score = await scoreTranscript(openai, transcript, {
        call_id: record.call_id,
        source: record.source,
        duration: record.duration,
      });
    } catch (e) {
      console.error(`OpenAI scoring failed for ${record.call_id}:`, e.message);
      continue;
    }

    rows.push({
      ...record,
      call_type: score.call_type ?? "",
      qualified_score: Number(score.qualified_score),
      case_value_estimate: score.case_value_estimate ?? "",
      attorney_switch_probability: Number(score.attorney_switch_probability),
      intake_quality: score.intake_quality ?? "",
      emotional_intensity: score.emotional_intensity ?? "",
      notes: score.notes ?? "",
    });
  }

  console.log("Saving results");
  const outDir = process.cwd();
  const jsonPath = path.join(outDir, "scored_calls.json");
  fs.writeFileSync(jsonPath, JSON.stringify(rows, null, 2), "utf8");
  /* Dashboard uses npm run intel:build (callrail_mom_pipeline.js), not this file. */

  const csvPath = path.join(outDir, "scored_calls.csv");
  const csvWriter = createCsvWriter({
    path: csvPath,
    header: [
      { id: "call_id", title: "call_id" },
      { id: "tracking_phone_number", title: "tracking_phone_number" },
      { id: "source", title: "source" },
      { id: "duration", title: "duration" },
      { id: "recording", title: "recording" },
      { id: "transcription", title: "transcription" },
      { id: "call_type", title: "call_type" },
      { id: "qualified_score", title: "qualified_score" },
      { id: "case_value_estimate", title: "case_value_estimate" },
      { id: "attorney_switch_probability", title: "attorney_switch_probability" },
      { id: "intake_quality", title: "intake_quality" },
      { id: "emotional_intensity", title: "emotional_intensity" },
      { id: "notes", title: "notes" },
    ],
  });
  await csvWriter.writeRecords(rows);

  const n = rows.length;
  const severe = rows.filter((r) => isSevereInjury(r.call_type)).length;
  const pdOnly = rows.filter((r) => isPdOnly(r.call_type)).length;
  const switchOpp = rows.filter((r) => !Number.isNaN(r.attorney_switch_probability) && r.attorney_switch_probability >= 0.5).length;
  const scores = rows.map((r) => r.qualified_score).filter((x) => !Number.isNaN(x));
  const avgQ = scores.length ? scores.reduce((a, b) => a + b, 0) / scores.length : 0;
  const intakeCounts = {};
  for (const r of rows) {
    const k = r.intake_quality || "Unknown";
    intakeCounts[k] = (intakeCounts[k] || 0) + 1;
  }
  const intakeSummary = Object.entries(intakeCounts)
    .sort((a, b) => b[1] - a[1])
    .map(([k, v]) => `${k}: ${v}`)
    .join("; ");

  console.log("\n======== EXECUTIVE REPORT ========");
  console.log(`Total calls analyzed: ${n}`);
  console.log(`Severe injury count: ${severe}`);
  console.log(`PD only count: ${pdOnly}`);
  console.log(`Attorney switch opportunities (probability >= 0.5): ${switchOpp}`);
  console.log(`Intake performance summary: ${intakeSummary || "N/A"}`);
  console.log(`Average qualified score: ${avgQ.toFixed(1)}`);
  console.log(`Top risk pattern detected: ${topRiskPattern(rows)}`);
  console.log("==================================\n");

  console.log("node callrail_intelligence.js");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
