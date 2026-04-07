/**
 * Match CallRail calls to signed-case rows (CSV). Multi-signal: phone windows 7d / 30d / 90d / phone_only,
 * then name_match, then transcript_match. Conflict: tighter tier wins, then smaller Δt, then earlier case.
 * Reads:  data/callrail_transcripts_last60days.json, data/signed_cases_last60days.csv
 * Writes: data/callrail_cases_enriched.json
 *
 * CLI: node enrich_callrail_signed_cases.js [callsJsonPath] [signedCasesCsvPath]
 */
const fs = require("fs");
const path = require("path");
const { getPreferredTimestamp } = require("./callrail-score-core");

const ROOT = __dirname;
const DATA = path.join(ROOT, "data");
const DEFAULT_CALLS = path.join(DATA, "callrail_transcripts_last60days.json");
const DEFAULT_SIGNED_CSV = path.join(DATA, "signed_cases_last60days.csv");
const OUT = path.join(DATA, "callrail_cases_enriched.json");

const DAY7_MS = 7 * 24 * 60 * 60 * 1000;
const DAY30_MS = 30 * 24 * 60 * 60 * 1000;
const DAY90_MS = 90 * 24 * 60 * 60 * 1000;

const STOP = new Set([
  "obo",
  "wife",
  "husband",
  "pending",
  "none",
  "customer",
  "caller",
  "the",
  "and",
  "for",
  "from",
  "with",
  "this",
  "that",
  "your",
  "our",
]);

const LEGAL_KEYWORDS = [
  "accident",
  "hospital",
  "lawyer",
  "attorney",
  "injury",
  "lawsuit",
  "settlement",
  "malpractice",
  "consultation",
  "litigation",
  "retained",
  "claim",
  "damages",
  "negligence",
];

const STRONG_LEGAL_PHRASES = [
  "personal injury",
  "car accident",
  "free consultation",
  "motor vehicle",
  "medical malpractice",
  "slip and fall",
  "workers comp",
  "wrongful death",
];

function warn(msg) {
  console.warn(`[signed-cases] ${msg}`);
}

/** RFC 4180 CSV */
function parseCsv(text) {
  const rows = [];
  let row = [];
  let field = "";
  let i = 0;
  let inQuotes = false;
  if (text.charCodeAt(0) === 0xfeff) i = 1;
  while (i < text.length) {
    const c = text[i];
    if (inQuotes) {
      if (c === '"') {
        if (text[i + 1] === '"') {
          field += '"';
          i += 2;
          continue;
        }
        inQuotes = false;
        i++;
        continue;
      }
      field += c;
      i++;
      continue;
    }
    if (c === '"') {
      inQuotes = true;
      i++;
      continue;
    }
    if (c === ",") {
      row.push(field);
      field = "";
      i++;
      continue;
    }
    if (c === "\r" || c === "\n") {
      if (c === "\r" && text[i + 1] === "\n") i++;
      row.push(field);
      field = "";
      if (row.some((cell) => String(cell).length > 0)) rows.push(row);
      row = [];
      i++;
      continue;
    }
    field += c;
    i++;
  }
  row.push(field);
  if (row.some((cell) => String(cell).length > 0)) rows.push(row);
  return rows;
}

function normalizeHeader(h) {
  return String(h || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

function headerToSnake(h) {
  return normalizeHeader(h).replace(/ /g, "_");
}

function isPhoneNumberColumn(headerRaw) {
  const s = headerToSnake(headerRaw);
  const n = normalizeHeader(headerRaw);
  if (s === "phone_call_source" || n === "phone call source") return false;
  if (s.includes("created_by") && s.includes("phone")) return false;
  if (s === "phone" || s.endsWith("_phone") || n.includes("phone")) {
    if (/source|call source|by id/i.test(headerRaw)) return false;
    return true;
  }
  return false;
}

function findColumnIndex(headers, matchers) {
  const norm = headers.map(normalizeHeader);
  for (const m of matchers) {
    for (let j = 0; j < norm.length; j++) {
      if (typeof m === "string" && norm[j] === m) return j;
      if (m instanceof RegExp && m.test(headers[j] || "")) return j;
      if (typeof m === "function" && m(norm[j], headers[j])) return j;
    }
  }
  return -1;
}

function normalizePhoneDigits(s) {
  if (s == null) return "";
  const d = String(s).replace(/\D/g, "");
  if (!d) return "";
  let x = d.length === 11 && d[0] === "1" ? d.slice(1) : d;
  if (x.length > 10) x = x.slice(-10);
  if (x.length === 10) return x;
  return "";
}

function extractAllPhonesFromRow(row, headers) {
  const seen = new Set();
  const out = [];
  for (let i = 0; i < headers.length; i++) {
    if (!isPhoneNumberColumn(headers[i])) continue;
    const n = normalizePhoneDigits(row[i]);
    if (n && !seen.has(n)) {
      seen.add(n);
      out.push(n);
    }
  }
  return out;
}

function phonesMatch(a, b) {
  if (!a || !b) return false;
  if (a === b) return true;
  if (a.length >= 10 && b.length >= 10 && a.slice(-10) === b.slice(-10)) return true;
  return false;
}

function callMatchesCasePhones(callNorm, casePhones) {
  if (!callNorm || !casePhones || !casePhones.length) return false;
  for (const p of casePhones) {
    if (phonesMatch(callNorm, p)) return true;
  }
  return false;
}

function parseDate(val) {
  if (val == null || val === "") return null;
  const s = String(val).trim();
  if (!s) return null;
  const ms = Date.parse(s);
  if (Number.isNaN(ms)) return null;
  return new Date(ms);
}

function isSignedCaseStatus(statusLower) {
  const t = (statusLower || "").trim();
  if (!t) return false;
  if (/\bunsigned\b/.test(t) || /\bnot\s+signed\b/.test(t)) return false;
  if (t.includes("case signed")) return true;
  if (/\bretainer\b/.test(t)) return true;
  if (/\bretained\b/.test(t)) return true;
  if (/\bsigned\b/.test(t)) return true;
  if (/\bclosed\b/.test(t)) return true;
  return false;
}

function normalizeForName(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9\s]/gi, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function tokenizeName(s) {
  return normalizeForName(s)
    .split(" ")
    .filter((t) => t.length >= 2 && !STOP.has(t));
}

function levenshtein(a, b) {
  const m = a.length;
  const n = b.length;
  if (!m) return n;
  if (!n) return m;
  const dp = Array(n + 1);
  for (let j = 0; j <= n; j++) dp[j] = j;
  for (let i = 1; i <= m; i++) {
    let prev = dp[0];
    dp[0] = i;
    for (let j = 1; j <= n; j++) {
      const t = dp[j];
      const cost = a[i - 1] === b[j - 1] ? 0 : 1;
      dp[j] = Math.min(dp[j] + 1, dp[j - 1] + 1, prev + cost);
      prev = t;
    }
  }
  return dp[n];
}

function similarityRatio(a, b) {
  if (!a || !b) return 0;
  const max = Math.max(a.length, b.length);
  if (!max) return 1;
  return 1 - levenshtein(a, b) / max;
}

function tokenOverlapCount(tokensA, tokensB) {
  const setB = new Set(tokensB);
  let n = 0;
  const seen = new Set();
  for (const t of tokensA) {
    if (setB.has(t) && !seen.has(t)) {
      seen.add(t);
      n++;
    }
  }
  return n;
}

function extractCallerIntro(transcription) {
  if (!transcription) return "";
  const m = String(transcription).match(/Caller:\s*([\s\S]*?)(?=Agent:|$)/i);
  return m ? String(m[1]).trim().slice(0, 500) : "";
}

function extractNameFromIntro(intro) {
  if (!intro) return "";
  const patterns = [
    /my name is ([^.?\n]{2,120})/i,
    /first and last name is ([^.?\n]{2,120})/i,
    /may i have your first and last name, please\?\s*Caller:\s*([^.?\n]{2,120})/i,
  ];
  for (const re of patterns) {
    const m = intro.match(re);
    if (m) return normalizeForName(m[1]);
  }
  return "";
}

function countLegalKeywords(transcriptLower) {
  if (!transcriptLower) return 0;
  let n = 0;
  for (const kw of LEGAL_KEYWORDS) {
    if (transcriptLower.includes(kw)) n++;
  }
  return n;
}

function strongPhraseHits(transcriptLower) {
  if (!transcriptLower) return 0;
  let n = 0;
  for (const p of STRONG_LEGAL_PHRASES) {
    if (transcriptLower.includes(p)) n++;
  }
  return n;
}

/** Capitalized tokens from transcript as weak name candidates */
function extractTranscriptNameTokens(transcription) {
  const out = [];
  const seen = new Set();
  const re = /\b[A-Z][a-z]{2,}\b/g;
  let m;
  const s = String(transcription || "");
  while ((m = re.exec(s)) !== null) {
    const t = m[0].toLowerCase();
    if (!STOP.has(t) && t.length >= 3 && !seen.has(t)) {
      seen.add(t);
      out.push(t);
    }
  }
  return out;
}

function callCustomerPhone(call) {
  return (
    call.customer_phone_number ||
    call.caller_phone_number ||
    call.formatted_customer_phone_number ||
    ""
  );
}

function callTimestampMs(call) {
  const ts = getPreferredTimestamp(call);
  if (!ts) return null;
  const ms = Date.parse(ts);
  return Number.isNaN(ms) ? null : ms;
}

function buildCallSignals(call, index) {
  const phoneNorm = normalizePhoneDigits(callCustomerPhone(call));
  const callMs = callTimestampMs(call);
  const transcription = call.transcription || call.transcript || "";
  const transcriptLower = String(transcription).toLowerCase();

  const crmName =
    call.customer_name ||
    call.customer_name_full ||
    call.caller_name ||
    call.contact_name ||
    "";
  let nameJoined = normalizeForName(crmName);
  const intro = extractCallerIntro(transcription);
  if (!nameJoined && intro) {
    nameJoined = extractNameFromIntro(intro);
  }
  const introTokens = tokenizeName(intro.slice(0, 300));
  const nameTokens = nameJoined ? tokenizeName(nameJoined) : [];
  const nameTokensMerged = [...new Set([...nameTokens, ...introTokens])];

  const transcriptNameTokens = extractTranscriptNameTokens(transcription);
  const keywordHits = countLegalKeywords(transcriptLower);
  const phraseHits = strongPhraseHits(transcriptLower);

  return {
    index,
    phoneNorm,
    callMs,
    nameJoined,
    nameTokens: nameTokensMerged,
    transcriptLower,
    transcriptNameTokens,
    legalKeywordHits: keywordHits,
    strongPhraseHits: phraseHits,
  };
}

function loadCalls(p) {
  if (!fs.existsSync(p)) {
    warn(`Calls file missing: ${p}`);
    return [];
  }
  try {
    const data = JSON.parse(fs.readFileSync(p, "utf8"));
    return Array.isArray(data) ? data : [];
  } catch (e) {
    warn(`Failed to parse calls JSON: ${e.message}`);
    return [];
  }
}

function loadSignedCases(csvPath) {
  if (!fs.existsSync(csvPath)) {
    warn(`Signed cases CSV missing: ${csvPath}`);
    return { cases: [], headers: [], warnings: [] };
  }
  let text;
  try {
    text = fs.readFileSync(csvPath, "utf8");
  } catch (e) {
    return { cases: [], headers: [], warnings: [`read error: ${e.message}`] };
  }
  const rows = parseCsv(text);
  if (!rows.length) return { cases: [], headers: [], warnings: ["empty csv"] };
  const headers = rows[0].map((h) => String(h));
  const dataRows = rows.slice(1);
  const warnings = [];

  const statusIdx = findColumnIndex(headers, ["status", /^status$/i, "lead status", "case status"]);
  const createdIdx = findColumnIndex(headers, [
    "created date",
    "created at",
    "date created",
    /created date/i,
  ]);
  const signedDateIdx = findColumnIndex(headers, [
    "signed up date",
    "signed_date",
    "signed date",
    "pd only courtesy sign up date",
    /signed up date/i,
  ]);
  const caseTypeIdx = findColumnIndex(headers, ["case type", /case type/i]);
  const marketingIdx = findColumnIndex(headers, [
    "marketing source",
    (norm) => norm === "marketing source",
  ]);
  const firstIdx = findColumnIndex(headers, ["first name", /^first name$/i]);
  const lastIdx = findColumnIndex(headers, ["last name", /^last name$/i]);

  const cases = [];
  for (let r = 0; r < dataRows.length; r++) {
    const row = dataRows[r];
    if (!row || !row.length) continue;
    const statusRaw = statusIdx >= 0 ? row[statusIdx] : "";
    const statusLower = String(statusRaw || "")
      .trim()
      .toLowerCase();
    if (!isSignedCaseStatus(statusLower)) continue;

    const phones = extractAllPhonesFromRow(row, headers);
    const first =
      firstIdx >= 0 ? String(row[firstIdx] ?? "").trim() : "";
    const last =
      lastIdx >= 0 ? String(row[lastIdx] ?? "").trim() : "";
    const contactRaw = `${first} ${last}`.trim();
    const contact_tokens = tokenizeName(contactRaw);
    const contact_norm = normalizeForName(contactRaw);

    if (!phones.length && !contact_tokens.length) {
      warnings.push(`row ${r + 2}: signed case skip — no phone and no contact name`);
      continue;
    }
    if (!phones.length) {
      warnings.push(`row ${r + 2}: no dialable phone — name/transcript signals only`);
    }

    const signedAt =
      signedDateIdx >= 0 ? parseDate(row[signedDateIdx]) : null;
    const createdAt =
      createdIdx >= 0 ? parseDate(row[createdIdx]) : null;
    const caseMs =
      (signedAt && signedAt.getTime()) ||
      (createdAt && createdAt.getTime()) ||
      null;
    if (caseMs == null) {
      warnings.push(`row ${r + 2}: signed case skip — no case date`);
      continue;
    }

    const case_type =
      caseTypeIdx >= 0 ? String(row[caseTypeIdx] ?? "").trim() : "";
    const marketing_source =
      marketingIdx >= 0 ? String(row[marketingIdx] ?? "").trim() : "";
    const case_status = String(statusRaw || "").trim();

    cases.push({
      _row: r + 2,
      phones,
      caseMs,
      case_status,
      case_type: case_type || null,
      marketing_source: marketing_source || null,
      contact_tokens,
      contact_norm,
    });
  }

  return { cases, headers, warnings };
}

function matchConfidenceForType(t) {
  if (t === "7d" || t === "30d") return "HIGH";
  if (t === "90d") return "MEDIUM";
  return "LOW";
}

function matchTypeRank(t) {
  const order = ["7d", "30d", "90d", "name_match", "transcript_match", "phone_only"];
  const i = order.indexOf(t);
  return i < 0 ? 99 : i;
}

function betterClaim(a, b) {
  const ra = matchTypeRank(a.match_type);
  const rb = matchTypeRank(b.match_type);
  if (ra !== rb) return ra < rb ? a : b;
  if (a.deltaMs !== b.deltaMs) return a.deltaMs < b.deltaMs ? a : b;
  return a.theCase.caseMs <= b.theCase.caseMs ? a : b;
}

function pickClosestFromSignals(pool, caseMs) {
  if (!pool.length) return null;
  let best = pool[0];
  for (let k = 1; k < pool.length; k++) {
    if (pool[k].callMs > best.callMs) best = pool[k];
  }
  const deltaMs = caseMs - best.callMs;
  if (deltaMs <= 0) return null;
  return { bestSig: best, deltaMs };
}

function namesStrongMatch(sig, C) {
  if (!C.contact_tokens.length) return false;
  const callStr = sig.nameJoined || sig.nameTokens.join(" ");
  const overlap = tokenOverlapCount(sig.nameTokens, C.contact_tokens);
  if (overlap >= 2) return true;
  if (callStr && C.contact_norm) {
    if (similarityRatio(callStr, C.contact_norm) > 0.8) return true;
  }
  return false;
}

/** Name tier: CRM / intro names only (not full transcript proper nouns). */
function nameTierMatch(sig, C) {
  if (!C.contact_tokens.length) return false;
  if (namesStrongMatch(sig, C)) return true;
  return false;
}

/** Transcript tier: proper nouns in transcript + case, and/or legal intent + case token. */
function transcriptTierMatch(sig, C) {
  if (!C.contact_tokens.length || !sig.transcriptLower) return false;
  const ov =
    tokenOverlapCount(sig.transcriptNameTokens, C.contact_tokens) >= 2 ||
    (C.contact_norm &&
      sig.transcriptLower.includes(C.contact_norm.replace(/\s+/g, " ").slice(0, 40)));
  const nameHit = ov || tokenOverlapCount(sig.transcriptNameTokens, C.contact_tokens) >= 1;
  const intentStrong =
    sig.strongPhraseHits >= 1 ||
    sig.legalKeywordHits >= 4 ||
    (sig.legalKeywordHits >= 2 && sig.strongPhraseHits >= 1);
  if (nameHit && intentStrong) return true;
  if (tokenOverlapCount(sig.transcriptNameTokens, C.contact_tokens) >= 2) return true;
  if (intentStrong && tokenOverlapCount(sig.transcriptNameTokens, C.contact_tokens) >= 1)
    return true;
  return false;
}

function makeClaim(C, sig, match_type, deltaMs) {
  return {
    theCase: C,
    callIndex: sig.index,
    match_type,
    match_delta_minutes: Math.round((deltaMs / 60000) * 100) / 100,
    deltaMs,
    match_confidence: matchConfidenceForType(match_type),
  };
}

function claimForCase(C, signals) {
  const before = signals.filter((s) => s.callMs != null && s.callMs < C.caseMs);
  if (!before.length) return null;

  const phoneBefore = before.filter(
    (s) =>
      s.phoneNorm &&
      s.phoneNorm.length === 10 &&
      callMatchesCasePhones(s.phoneNorm, C.phones)
  );

  const tryPhoneWindow = (ms, match_type) => {
    const pool = phoneBefore.filter((s) => C.caseMs - s.callMs <= ms);
    const picked = pickClosestFromSignals(pool, C.caseMs);
    if (!picked) return null;
    return makeClaim(C, picked.bestSig, match_type, picked.deltaMs);
  };

  let c = tryPhoneWindow(DAY7_MS, "7d");
  if (c) return c;
  c = tryPhoneWindow(DAY30_MS, "30d");
  if (c) return c;
  c = tryPhoneWindow(DAY90_MS, "90d");
  if (c) return c;
  const poolAllPhone = phoneBefore;
  const pickedPhone = pickClosestFromSignals(poolAllPhone, C.caseMs);
  if (pickedPhone) {
    c = makeClaim(C, pickedPhone.bestSig, "phone_only", pickedPhone.deltaMs);
    return c;
  }

  const namePool = before.filter((s) => nameTierMatch(s, C));
  const pickedName = pickClosestFromSignals(namePool, C.caseMs);
  if (pickedName) {
    return makeClaim(C, pickedName.bestSig, "name_match", pickedName.deltaMs);
  }

  const trPool = before.filter(
    (s) => transcriptTierMatch(s, C) && !nameTierMatch(s, C)
  );
  const pickedTr = pickClosestFromSignals(trPool, C.caseMs);
  if (pickedTr) {
    return makeClaim(C, pickedTr.bestSig, "transcript_match", pickedTr.deltaMs);
  }

  return null;
}

function assignCasesToCalls(callsRaw, signedCases) {
  const signals = [];
  for (let i = 0; i < callsRaw.length; i++) {
    const call = callsRaw[i];
    if (!call || typeof call !== "object") continue;
    signals.push(buildCallSignals(call, i));
  }

  const claimantsByCall = new Map();
  for (const C of signedCases) {
    const claim = claimForCase(C, signals);
    if (!claim) continue;
    const idx = claim.callIndex;
    const arr = claimantsByCall.get(idx) || [];
    arr.push(claim);
    claimantsByCall.set(idx, arr);
  }

  const winnerByCall = new Map();
  for (const [idx, arr] of claimantsByCall) {
    winnerByCall.set(idx, arr.reduce((w, c) => (w ? betterClaim(w, c) : c)));
  }
  return winnerByCall;
}

function main() {
  const callsPath = process.argv[2] || DEFAULT_CALLS;
  const csvPath = process.argv[3] || DEFAULT_SIGNED_CSV;

  if (!fs.existsSync(DATA)) fs.mkdirSync(DATA, { recursive: true });

  const callsRaw = loadCalls(callsPath);
  const { cases: signedCases, warnings } = loadSignedCases(csvPath);
  for (const w of warnings.slice(0, 5)) warn(w);

  const winnerByCall = assignCasesToCalls(callsRaw, signedCases);

  const enriched = [];
  let matchedCallCount = 0;

  for (let i = 0; i < callsRaw.length; i++) {
    const call = callsRaw[i];
    if (!call || typeof call !== "object") {
      enriched.push(call);
      continue;
    }
    const win = winnerByCall.get(i);
    const base = {
      ...call,
      case_signed: !!win,
      case_type: win ? win.theCase.case_type : null,
      marketing_source: win ? win.theCase.marketing_source : null,
      match_type: win ? win.match_type : null,
      match_delta_minutes: win ? win.match_delta_minutes : null,
      match_confidence: win ? win.match_confidence : null,
    };
    if (win) matchedCallCount++;
    enriched.push(base);
  }

  const total_calls = enriched.length;
  const signedInputCount = signedCases.length;
  const match_rate =
    total_calls > 0 ? Math.round((matchedCallCount / total_calls) * 100000) / 100000 : 0;

  const match_breakdown = {
    "7d": 0,
    "30d": 0,
    "90d": 0,
    name_match: 0,
    transcript_match: 0,
    phone_only: 0,
  };
  const confidence_breakdown = { HIGH: 0, MEDIUM: 0, LOW: 0 };
  for (const c of enriched) {
    if (!c || !c.case_signed || !c.match_type) continue;
    const t = c.match_type;
    if (match_breakdown[t] !== undefined) match_breakdown[t]++;
    const conf = c.match_confidence;
    if (conf && confidence_breakdown[conf] !== undefined) confidence_breakdown[conf]++;
  }

  const out = {
    generated_at: new Date().toISOString(),
    inputs: {
      calls_path: path.resolve(callsPath),
      signed_cases_csv: path.resolve(csvPath),
      signed_cases_rows_after_filter: signedInputCount,
      match_policy: {
        phone_windows_days: [7, 30, 90],
        phone_fallback: "phone_only_closest_before_case",
        then: ["name_match", "transcript_match"],
        conflict: "tighter_window_then_smaller_delta_then_earlier_case_time",
      },
    },
    summary: {
      total_calls,
      matched_cases: matchedCallCount,
      match_rate,
      match_breakdown,
      confidence_breakdown,
    },
    calls: enriched,
  };

  fs.writeFileSync(OUT, JSON.stringify(out, null, 2), "utf8");

  console.log("CallRail ↔ signed cases (multi-signal)");
  console.log("  total calls:            ", total_calls);
  console.log("  signed cases:           ", signedInputCount, "(CSV rows after status filter)");
  console.log("  matched cases:          ", matchedCallCount, "(calls with case_signed)");
  console.log(
    "  match rate:             ",
    match_rate,
    "(" + (total_calls ? (match_rate * 100).toFixed(2) : "0.00") + "% of calls)"
  );
  console.log("  match_type breakdown:   ", JSON.stringify(match_breakdown));
  console.log("  confidence breakdown:   ", JSON.stringify(confidence_breakdown));
  console.log("  wrote:                  ", OUT);
}

main();
