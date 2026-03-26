/**
 * READ ONLY: CallRail API v3 — GET list + GET detail only (last 60 days).
 */
require("dotenv").config();
const axios = require("axios");
const fs = require("fs");
const path = require("path");

const REQUEST_GAP_MS = 500;

const CALLRAIL_API_KEY = process.env.CALLRAIL_API_KEY;
const CALLRAIL_ACCOUNT_ID = process.env.CALLRAIL_ACCOUNT_ID;
const BASE = `https://api.callrail.com/v3/a/${CALLRAIL_ACCOUNT_ID}`;

let firstApiRequest = true;

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function throttledGet(url, config) {
  if (!firstApiRequest) await delay(REQUEST_GAP_MS);
  firstApiRequest = false;
  const merged = { ...config, timeout: 120000 };
  for (;;) {
    try {
      return await axios.get(url, merged);
    } catch (e) {
      const status = e.response && e.response.status;
      if (status === 429) {
        const ra = e.response.headers["retry-after"];
        const sec = parseInt(ra, 10);
        const waitMs = Number.isFinite(sec) ? Math.max(1000, sec * 1000) : 60000;
        console.error(`Rate limited (429). Waiting ${waitMs}ms, then retrying...`);
        await delay(waitMs);
        continue;
      }
      throw e;
    }
  }
}

function requireEnv() {
  const missing = [];
  if (!CALLRAIL_API_KEY) missing.push("CALLRAIL_API_KEY");
  if (!CALLRAIL_ACCOUNT_ID) missing.push("CALLRAIL_ACCOUNT_ID");
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

function rangeLast60Days() {
  const end = new Date();
  const start = new Date();
  start.setDate(start.getDate() - 60);
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

function recordingUrl(detail) {
  return detail.recording || detail.recording_player || "";
}

async function fetchAllCallSummaries() {
  const { start_date, end_date } = rangeLast60Days();
  const summaries = [];
  const params = new URLSearchParams({
    start_date,
    end_date,
    per_page: "100",
    relative_pagination: "true",
  });
  let nextUrl = `${BASE}/calls.json?${params.toString()}`;
  let pageNum = 1;

  while (nextUrl) {
    console.log(`Fetched page ${pageNum}`);
    const { data } = await throttledGet(nextUrl, { headers: authHeaders() });
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
  const { data } = await throttledGet(url, {
    headers: authHeaders(),
    params: {
      fields:
        "transcription,conversational_transcript,recording,recording_player,tracking_phone_number,source,duration,id,start_time,created_at",
    },
  });
  return data;
}

async function main() {
  requireEnv();

  const summaries = await fetchAllCallSummaries();
  const totalCallsPulled = summaries.length;
  const records = [];
  let transcriptsSaved = 0;

  for (const summary of summaries) {
    const callId = summary.id;
    let detail;
    try {
      detail = await fetchCallDetail(callId);
    } catch (e) {
      console.error(`GET /calls/${callId}.json failed:`, e.message);
      continue;
    }

    const transcription = transcriptFromDetail(detail);
    if (!transcription) continue;

    transcriptsSaved += 1;
    console.log(`Saved transcript ${transcriptsSaved}`);

    const rec = {
      call_id: detail.id ?? callId,
      tracking_phone_number: detail.tracking_phone_number ?? "",
      source: detail.source ?? "",
      duration: detail.duration ?? "",
      recording_url: recordingUrl(detail),
      transcription,
    };
    if (detail.call_start_time != null) rec.call_start_time = detail.call_start_time;
    if (detail.start_time != null) rec.start_time = detail.start_time;
    if (detail.created_at != null) rec.created_at = detail.created_at;
    records.push(rec);
  }

  const outPath = path.join(process.cwd(), "callrail_transcripts_last60days.json");
  fs.writeFileSync(outPath, JSON.stringify(records, null, 2), "utf8");

  console.log("");
  console.log(`Total calls pulled: ${totalCallsPulled}`);
  console.log(`Total transcripts saved: ${transcriptsSaved}`);
  console.log("node callrail_transcript_ingest.js");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
