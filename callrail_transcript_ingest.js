/**
 * CallRail API v3 — full paginated ingest, 429 handling, resilient retries,
 * incremental saves every 50 calls, meta + post-run intel:build.
 */
require("dotenv").config();
const axios = require("axios");
const fs = require("fs");
const path = require("path");
const { spawnSync } = require("child_process");

/** Default 1.4s between requests to reduce 429s; override with INGEST_REQUEST_GAP_MS */
const REQUEST_GAP_MS = Math.max(200, parseInt(process.env.INGEST_REQUEST_GAP_MS, 10) || 1400);
const SAVE_EVERY = 50;
const SOFT_RETRY_MS = 5000;
const HARD_WAIT_MS = 120000;
const FAILS_BEFORE_HARD_WAIT = 3;

const CALLRAIL_API_KEY = process.env.CALLRAIL_API_KEY;
const CALLRAIL_ACCOUNT_ID = process.env.CALLRAIL_ACCOUNT_ID;
const BASE = `https://api.callrail.com/v3/a/${CALLRAIL_ACCOUNT_ID}`;

const OUT_TRANSCRIPTS = path.join(process.cwd(), "callrail_transcripts_last60days.json");
const OUT_META = path.join(process.cwd(), "callrail_ingest_meta.json");

let firstApiRequest = true;

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function authHeaders() {
  return { Authorization: `Token token="${CALLRAIL_API_KEY}"` };
}

/**
 * Single GET with 429 loop: Retry-After header, else 60s.
 */
async function getWith429Retry(url, config) {
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
        console.error(`Rate limited (429). Waiting ${waitMs}ms, then retrying…`);
        await delay(waitMs);
        continue;
      }
      throw e;
    }
  }
}

/**
 * On non-429 failures: retry; after FAILS_BEFORE_HARD_WAIT consecutive failures, wait 120s and reset.
 */
async function resilientGet(url, config, label) {
  let streak = 0;
  for (;;) {
    try {
      const res = await getWith429Retry(url, config);
      streak = 0;
      return res;
    } catch (e) {
      const status = e.response && e.response.status;
      if (status && status >= 400 && status < 500 && status !== 429) {
        // Non-retryable client error for this request; caller decides skip/abort.
        throw e;
      }
      streak++;
      console.error(`[${label}] Error (${streak}): ${e.message}`);
      const wait = streak >= FAILS_BEFORE_HARD_WAIT ? HARD_WAIT_MS : SOFT_RETRY_MS;
      if (streak >= FAILS_BEFORE_HARD_WAIT) {
        console.error(`[${label}] Repeated failures � waiting ${wait / 1000}s, then retrying same request�`);
        streak = 0;
      }
      await delay(wait);
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

function safeStr(v) {
  if (v == null) return "";
  return String(v).trim();
}

function parseableIso(s) {
  if (!safeStr(s)) return false;
  return !Number.isNaN(Date.parse(s));
}

/** Calendar month YYYY-MM from first parseable among call_start_time, start_time, created_at */
function monthBucket(rec) {
  const order = [rec.call_start_time, rec.start_time, rec.created_at];
  for (const v of order) {
    if (!parseableIso(v)) continue;
    const d = new Date(Date.parse(v));
    return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}`;
  }
  return "undated";
}

function dedupeSummaries(summaries) {
  const seen = new Map();
  const out = [];
  for (const s of summaries) {
    const id = s && (s.id != null ? String(s.id) : null);
    if (!id || seen.has(id)) continue;
    seen.set(id, true);
    out.push(s);
  }
  return out;
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
    console.log(`Fetching list page ${pageNum}…`);
    const { data } = await resilientGet(nextUrl, { headers: authHeaders() }, `list page ${pageNum}`);
    const calls = data.calls || [];
    summaries.push(...calls);
    if (data.has_next_page && data.next_page) {
      nextUrl = data.next_page;
      pageNum += 1;
    } else {
      nextUrl = null;
    }
  }
  return dedupeSummaries(summaries);
}

/** CallRail rejects unknown `fields` with 400 — use only documented keys (no call_start_time; use start_time). */
const DETAIL_FIELDS =
  "transcription,conversational_transcript,recording,recording_player,tracking_phone_number,customer_phone_number,source,duration,id,start_time,created_at,direction";

async function fetchCallDetailResilient(callId) {
  const url = `${BASE}/calls/${callId}.json`;
  const { data } = await resilientGet(
    url,
    {
      headers: authHeaders(),
      params: { fields: DETAIL_FIELDS },
    },
    `call ${callId}`
  );
  return data;
}

function buildRecord(detail, callId, summary) {
  const transcription = transcriptFromDetail(detail);
  const apiCallStart = safeStr(detail.call_start_time);
  const apiStart = safeStr(detail.start_time);
  const createdAt = detail.created_at != null ? safeStr(detail.created_at) : "";

  const storedCallStart = apiCallStart || apiStart;

  const timestamp_valid = parseableIso(apiCallStart) || parseableIso(apiStart);

  const rec = {
    call_id: detail.id != null ? String(detail.id) : String(callId),
    tracking_phone_number: safeStr(detail.tracking_phone_number),
    customer_phone_number: safeStr(detail.customer_phone_number),
    source: safeStr(detail.source || detail.formatted_tracking_source),
    duration: detail.duration != null && detail.duration !== "" ? Number(detail.duration) : null,
    direction: safeStr(detail.direction),
    recording_url: recordingUrl(detail),
    transcription,
    city: safeStr(detail.customer_city || detail.city),
    state: safeStr(detail.customer_state || detail.state),
    timestamp_valid,
  };

  if (storedCallStart) rec.call_start_time = storedCallStart;
  if (apiStart) rec.start_time = apiStart;
  if (createdAt) rec.created_at = createdAt;

  return rec;
}

function flushTranscripts(records) {
  fs.writeFileSync(OUT_TRANSCRIPTS, JSON.stringify(records, null, 2), "utf8");
}

/** Reuse rows from last checkpoint so a restart skips API for those IDs */
function loadExistingByCallId() {
  const map = new Map();
  if (!fs.existsSync(OUT_TRANSCRIPTS)) return map;
  try {
    const arr = JSON.parse(fs.readFileSync(OUT_TRANSCRIPTS, "utf8"));
    if (!Array.isArray(arr)) return map;
    for (const r of arr) {
      if (r && r.call_id != null) map.set(String(r.call_id), r);
    }
  } catch {
    /* */
  }
  return map;
}

function computeMeta(records) {
  const strictValid = records.filter((r) => r.timestamp_valid === true).length;
  const timestamp_coverage_pct = records.length ? Math.round((100 * strictValid) / records.length * 10) / 10 : 0;

  const msList = [];
  for (const r of records) {
    if (!r.timestamp_valid) continue;
    const s = safeStr(r.call_start_time) || safeStr(r.start_time);
    if (!parseableIso(s)) continue;
    msList.push(Date.parse(s));
  }
  msList.sort((a, b) => a - b);
  const earliest_timestamp =
    msList.length > 0 ? new Date(msList[0]).toISOString() : null;
  const latest_timestamp =
    msList.length > 0 ? new Date(msList[msList.length - 1]).toISOString() : null;

  return {
    generated_at: new Date().toISOString(),
    date_range: rangeLast60Days(),
    total_calls_ingested: records.length,
    timestamp_coverage_pct,
    earliest_timestamp,
    latest_timestamp,
    records_timestamp_valid_strict: strictValid,
  };
}

function printFinalSummary(records) {
  const meta = computeMeta(records);
  const noTranscript = records.filter((r) => !safeStr(r.transcription)).length;

  const byMonth = {};
  for (const r of records) {
    const k = monthBucket(r);
    byMonth[k] = (byMonth[k] || 0) + 1;
  }
  const months = Object.keys(byMonth).sort();

  const srcMap = new Map();
  for (const r of records) {
    const k = r.source || "(no source)";
    srcMap.set(k, (srcMap.get(k) || 0) + 1);
  }
  const topSources = [...srcMap.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, 5);

  console.log("");
  console.log("=== Ingest summary ===");
  console.log(`Total calls ingested: ${records.length}`);
  console.log(`Timestamp coverage % (API call_start_time or start_time parseable): ${meta.timestamp_coverage_pct}%`);
  console.log("Calls grouped by YYYY-MM (uses call_start_time → start_time → created_at for month):");
  for (const m of months) {
    console.log(`  ${m}: ${byMonth[m]}`);
  }
  console.log("Top 5 sources by volume:");
  for (let i = 0; i < topSources.length; i++) {
    console.log(`  ${i + 1}. ${topSources[i][0]}: ${topSources[i][1]}`);
  }
  console.log(`Calls with no transcript: ${noTranscript}`);
}

async function main() {
  requireEnv();

  const existingById = loadExistingByCallId();
  if (existingById.size > 0) {
    console.log(`Resume: ${existingById.size} call(s) loaded from checkpoint file (will skip re-fetch).`);
  }

  const summaries = await fetchAllCallSummaries();
  const records = [];
  let recordErrors = 0;
  let skippedResume = 0;

  for (let i = 0; i < summaries.length; i++) {
    const summary = summaries[i];
    const callId = summary.id;
    const idStr = String(callId);

    const cached = existingById.get(idStr);
    /** Re-fetch if checkpoint row has no transcript (e.g. older ingest omitted `fields`) */
    const resumeOk = existingById.has(idStr) && cached && safeStr(cached.transcription);

    if (resumeOk) {
      records.push(cached);
      skippedResume += 1;
      if (records.length % SAVE_EVERY === 0 || i === summaries.length - 1) {
        flushTranscripts(records);
        console.log(`Checkpoint: saved ${records.length} calls → ${path.basename(OUT_TRANSCRIPTS)}`);
      }
      if ((i + 1) % 100 === 0 || i === summaries.length - 1) {
        console.log(`Detail ${i + 1} / ${summaries.length} (resume skip ${skippedResume})`);
      }
      continue;
    }

    let detail;
    try {
      detail = await fetchCallDetailResilient(callId);
    } catch (e) {
      recordErrors += 1;
      const st = e.response && e.response.status;
      const suffix = st ? ` (status ${st})` : "";
      console.error(`detail fetch failed for ${callId}${suffix}:`, e.message);
      continue;
    }
    let rec;
    try {
      rec = buildRecord(detail, callId, summary);
    } catch (e) {
      recordErrors += 1;
      console.error(`buildRecord failed for ${callId}:`, e.message);
      continue;
    }

    records.push(rec);

    if (records.length % SAVE_EVERY === 0 || i === summaries.length - 1) {
      flushTranscripts(records);
      console.log(`Checkpoint: saved ${records.length} calls → ${path.basename(OUT_TRANSCRIPTS)}`);
    }

    if ((i + 1) % 25 === 0 || i === summaries.length - 1) {
      console.log(`Detail ${i + 1} / ${summaries.length}`);
    }
  }

  if (skippedResume) console.log(`Resume complete: skipped ${skippedResume} existing, fetched new details for the rest.`);

  flushTranscripts(records);

  const meta = computeMeta(records);
  meta.record_build_errors = recordErrors;
  meta.total_calls_listed_unique = summaries.length;
  fs.writeFileSync(OUT_META, JSON.stringify(meta, null, 2), "utf8");

  printFinalSummary(records);
  console.log("");
  console.log(`Wrote ${path.basename(OUT_META)}`);

  if (process.env.INGEST_SKIP_INTEL_BUILD === "1") {
    console.log("");
    console.log("INGEST_SKIP_INTEL_BUILD=1 — run `npm run intel:build` when ready.");
    return;
  }

  console.log("");
  console.log("Running npm run intel:build …");
  const npmCmd = process.platform === "win32" ? "npm.cmd" : "npm";
  const r = spawnSync(npmCmd, ["run", "intel:build"], {
    stdio: "inherit",
    cwd: process.cwd(),
    shell: false,
  });
  if (r.error) {
    console.error("intel:build spawn error:", r.error.message);
    process.exit(1);
  }
  if (r.status !== 0) {
    console.error("intel:build exited with code", r.status);
    process.exit(r.status || 1);
  }

  console.log("");
  console.log("=== Done: ingest + intel:build ===");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});





