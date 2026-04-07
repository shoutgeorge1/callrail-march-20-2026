/**
 * Match CallRail calls to Lead Docket leads (lead-centric intake: each lead → latest call before
 * CRM created_at). Classify: strict (≤60m), 24hr (≤24h), phone_only (>24h). Same call + multiple
 * leads: keep strict over weaker, then smallest time gap.
 * Reads:  data/callrail_transcripts_last60days.json (optional CLI: path to calls JSON)
 *         data/leaddocket_cases_clean.json (preferred; run build_leaddocket_cases_clean.js on Signed Up export)
 *         else data/leaddocket_leads_last60days.xlsx or .csv
 * Writes: data/callrail_enriched.json (summary: total_calls, matched_leads, signed_cases, strict_matches, match_breakdown)
 */
const fs = require("fs");
const path = require("path");
const XLSX = require("xlsx");
const { parseMonthKeyFromRecord, getPreferredTimestamp } = require("./callrail-score-core");

const ROOT = __dirname;
const DATA = path.join(ROOT, "data");
/** Lead created after call; Δ = lead − call. */
const STRICT_MS = 4 * 60 * 60 * 1000;
const HOUR24_MS = 24 * 60 * 60 * 1000;
const DAY3_MS = 3 * 24 * 60 * 60 * 1000;
const DAY7_MS = 7 * 24 * 60 * 60 * 1000;
const DAY14_MS = 14 * 24 * 60 * 60 * 1000;

const DEFAULT_CALLS = [
  path.join(ROOT, "callrail_scored_calls.json"),
  path.join(DATA, "callrail_scored_calls.json"),
  path.join(ROOT, "callrail_scored_calls_latest.json"),
  path.join(DATA, "callrail_transcripts_last60days.json"),
  path.join(ROOT, "callrail_transcripts_last60days.json"),
];
const DEFAULT_LEADS_CLEAN = path.join(DATA, "leaddocket_cases_clean.json");
const DEFAULT_LEADS_XLSX = path.join(DATA, "leaddocket_leads_last60days.xlsx");
const DEFAULT_LEADS_CSV = path.join(DATA, "leaddocket_leads_last60days.csv");
const OUT_ENRICHED = path.join(DATA, "callrail_enriched.json");
const OUT_MATCH_DEBUG = path.join(DATA, "leaddocket_callrail_match_debug.json");

function warn(msg) {
  console.warn(`[enrich] ${msg}`);
}

function resolveFirstExisting(paths) {
  for (const p of paths) {
    if (fs.existsSync(p)) return p;
  }
  return null;
}

/** Prefer `data/leaddocket_cases_clean.json`, then legacy `.xlsx` / `.csv`. */
function resolveLeadsPath(arg) {
  if (arg) return arg;
  if (fs.existsSync(DEFAULT_LEADS_CLEAN)) return DEFAULT_LEADS_CLEAN;
  if (fs.existsSync(DEFAULT_LEADS_XLSX)) return DEFAULT_LEADS_XLSX;
  if (fs.existsSync(DEFAULT_LEADS_CSV)) {
    warn(
      `Using ${path.basename(DEFAULT_LEADS_CSV)} — prefer ${path.basename(DEFAULT_LEADS_CLEAN)} from build_leaddocket_cases_clean.js.`
    );
    return DEFAULT_LEADS_CSV;
  }
  return DEFAULT_LEADS_CLEAN;
}

/** RFC 4180-style CSV parser */
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

/** snake_case from header text (spaces → underscores, lowercase). */
function headerToSnake(h) {
  return normalizeHeader(h).replace(/ /g, "_");
}

/** True if this column holds a dialable phone (not e.g. phone_call_source). */
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
    for (let i = 0; i < norm.length; i++) {
      if (typeof m === "string" && norm[i] === m) return i;
      if (m instanceof RegExp && m.test(headers[i] || "")) return i;
      if (typeof m === "function" && m(norm[i], headers[i])) return i;
    }
  }
  return -1;
}

/** Every cell in columns that look like phone numbers → unique normalized 10-digit list. */
function extractAllPhonesFromRow(row, headers) {
  const seen = new Set();
  const out = [];
  for (let i = 0; i < headers.length; i++) {
    if (!isPhoneNumberColumn(headers[i])) continue;
    const v = row[i];
    const n = normalizePhoneDigits(v);
    if (n && !seen.has(n)) {
      seen.add(n);
      out.push(n);
    }
  }
  return out;
}

function digitsOnlyLoose(s) {
  if (s == null) return "";
  let d = String(s).replace(/\D/g, "");
  if (d.length === 11 && d[0] === "1") d = d.slice(1);
  return d;
}

function extractPhoneDetailsFromRow(row, headers) {
  const seen = new Set();
  const out = [];
  for (let i = 0; i < headers.length; i++) {
    if (!isPhoneNumberColumn(headers[i])) continue;
    const raw = row[i];
    const digits = digitsOnlyLoose(raw);
    const normalized = normalizePhoneDigits(raw);
    if (!digits && !normalized) continue;
    const key = `${headers[i]}|${normalized || digits}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push({
      header: String(headers[i] || ""),
      raw: raw == null ? "" : String(raw),
      digits,
      normalized,
    });
  }
  return out;
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

function suffix7(s) {
  const d = digitsOnlyLoose(s);
  return d.length >= 7 ? d.slice(-7) : "";
}

function classifyPhoneMatch(callPhone, leadPhone) {
  const callNorm = normalizePhoneDigits(callPhone);
  const leadNorm = normalizePhoneDigits(leadPhone);
  const callLoose = digitsOnlyLoose(callPhone);
  const leadLoose = digitsOnlyLoose(leadPhone);
  if (callNorm && leadNorm && callNorm === leadNorm) {
    return { matched: true, matchType: "exact", rank: 0 };
  }
  const call7 = suffix7(callPhone);
  const lead7 = suffix7(leadPhone);
  if (call7 && lead7 && call7 === lead7) {
    return { matched: true, matchType: "last7", rank: 1 };
  }
  if (
    callLoose &&
    leadLoose &&
    callLoose.length >= 4 &&
    leadLoose.length >= 4 &&
    (callLoose.includes(leadLoose) || leadLoose.includes(callLoose))
  ) {
    return { matched: true, matchType: "contains", rank: 2 };
  }
  return { matched: false, matchType: "none", rank: 99 };
}

function parseLeadDate(val) {
  if (val == null || val === "") return null;
  if (val instanceof Date && !Number.isNaN(val.getTime())) return val;
  if (typeof val === "number" && Number.isFinite(val)) {
    const parsed = XLSX.SSF.parse_date_code(val);
    if (parsed && parsed.y != null) {
      const d = new Date(
        Date.UTC(parsed.y, parsed.m - 1, parsed.d, parsed.H || 0, parsed.M || 0, parsed.S || 0)
      );
      if (!Number.isNaN(d.getTime())) return d;
    }
  }
  const s = String(val).trim();
  if (!s) return null;
  const ms = Date.parse(s);
  if (Number.isNaN(ms)) return null;
  return new Date(ms);
}

function normalizeLeadStatus(s) {
  if (s == null) return "";
  return String(s).trim().toLowerCase();
}

function normalizeName(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/** Case-insensitive substring rules; avoid "unsigned" / "not signed" false positives. */
function signedFlagFromStatus(statusLower) {
  const t = (statusLower || "").trim();
  if (!t) return false;
  if (/\bunsigned\b/.test(t) || /\bnot\s+signed\b/.test(t)) return false;
  const phrases = [
    "closed - signed",
    "retainer received",
    "case signed",
    "signed up",
    "retainer",
    "retained",
    "signed",
    "closed",
  ];
  return phrases.some((p) => t.includes(p));
}

function callPhoneCandidates(call) {
  return [
    { field: "customer_phone_number", raw: call.customer_phone_number || "" },
    { field: "caller_phone_number", raw: call.caller_phone_number || "" },
    { field: "formatted_customer_phone_number", raw: call.formatted_customer_phone_number || "" },
  ]
    .map((x) => ({
      field: x.field,
      raw: x.raw,
      digits: digitsOnlyLoose(x.raw),
      normalized: normalizePhoneDigits(x.raw),
    }))
    .filter((x) => x.raw || x.digits || x.normalized);
}

function bestPhoneMatch(callPhones, leadPhoneDetails) {
  let best = null;
  const attempts = [];
  for (const cp of callPhones) {
    for (const lp of leadPhoneDetails) {
      const res = classifyPhoneMatch(cp.raw || cp.normalized || cp.digits, lp.raw || lp.normalized || lp.digits);
      const attempt = {
        call_field: cp.field,
        call_original_phone: cp.raw,
        call_normalized_phone: cp.normalized || "",
        lead_field: lp.header,
        lead_original_phone: lp.raw,
        lead_normalized_phone: lp.normalized || "",
        match_type: res.matchType,
      };
      attempts.push(attempt);
      if (!res.matched) continue;
      const candidate = { ...attempt, rank: res.rank };
      if (!best || candidate.rank < best.rank) best = candidate;
    }
  }
  return { best, attempts };
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

function callDurationSec(call) {
  const n = Number(call && call.duration);
  return Number.isFinite(n) ? n : 0;
}

function callDirectionRank(call) {
  return String((call && call.direction) || "").toLowerCase() === "inbound" ? 0 : 1;
}

function callNameText(call) {
  const bits = [
    call && call.customer_name,
    call && call.caller_name,
    call && call.name,
    call && call.contact_name,
  ];
  return normalizeName(bits.filter(Boolean).join(" "));
}

function loadCalls(resolvedPath) {
  if (!resolvedPath) {
    warn("No CallRail JSON found at data/callrail_transcripts_last60days.json.");
    return [];
  }
  try {
    const raw = fs.readFileSync(resolvedPath, "utf8");
    const data = JSON.parse(raw);
    return Array.isArray(data) ? data : [];
  } catch (e) {
    warn(`Failed to read calls JSON (${resolvedPath}): ${e.message}`);
    return [];
  }
}

function loadLeadsXlsx(filePath) {
  let wb;
  try {
    wb = XLSX.readFile(filePath, { cellDates: true, raw: false });
  } catch (e) {
    warn(`Could not read Excel: ${e.message}`);
    return { leads: [], headers: [], warnings: ["xlsx_read_error"], format: "xlsx", sourcePath: null };
  }
  const sheetName = wb.SheetNames[0];
  if (!sheetName) {
    warn("Excel workbook has no sheets.");
    return { leads: [], headers: [], warnings: ["empty_xlsx"], format: "xlsx", sourcePath: filePath };
  }
  const sheet = wb.Sheets[sheetName];
  const matrix = XLSX.utils.sheet_to_json(sheet, { header: 1, defval: "", raw: false });
  if (!matrix.length) {
    return { leads: [], headers: [], warnings: ["empty_xlsx"], format: "xlsx", sourcePath: filePath };
  }
  const headers = matrix[0].map((h) => String(h));
  const dataRows = matrix
    .slice(1)
    .filter((row) => row && row.some((cell) => String(cell).trim() !== ""));
  return buildLeadsFromTable(headers, dataRows, filePath, "xlsx");
}

function loadLeadsCsv(csvPath) {
  let text;
  try {
    text = fs.readFileSync(csvPath, "utf8");
  } catch (e) {
    warn(`Could not read CSV: ${e.message}`);
    return { leads: [], headers: [], warnings: ["csv_read_error"], format: "csv", sourcePath: null };
  }
  const rows = parseCsv(text);
  if (!rows.length) {
    return { leads: [], headers: [], warnings: ["empty_csv"], format: "csv", sourcePath: csvPath };
  }
  const headers = rows[0].map((h) => String(h));
  const dataRows = rows.slice(1);
  return buildLeadsFromTable(headers, dataRows, csvPath, "csv");
}

function loadLeadsCleanJson(filePath) {
  let data;
  try {
    data = JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch (e) {
    warn(`Could not read clean leads JSON: ${e.message}`);
    return {
      leads: [],
      headers: [],
      warnings: ["clean_json_read_error"],
      format: "clean_json",
      sourcePath: null,
    };
  }
  if (!Array.isArray(data)) {
    warn("leaddocket_cases_clean.json must be a JSON array.");
    return {
      leads: [],
      headers: [],
      warnings: ["clean_json_not_array"],
      format: "clean_json",
      sourcePath: filePath,
    };
  }
  const warnings = [];
  const leads = [];
  for (let i = 0; i < data.length; i++) {
    const row = data[i];
    if (!row || typeof row !== "object") continue;
    const signed = parseLeadDate(row.signed_date);
    if (!signed) {
      warnings.push(`clean json index ${i}: skip — bad signed_date`);
      continue;
    }
    const normalized =
      row.primary_phone != null && String(row.primary_phone).trim() !== ""
        ? normalizePhoneDigits(row.primary_phone)
        : "";
    const phones = normalized.length === 10 ? [normalized] : [];
    const phoneDetails =
      phones.length === 1
        ? [
            {
              header: "primary_phone",
              raw: String(row.primary_phone),
              digits: digitsOnlyLoose(row.primary_phone),
              normalized: phones[0],
            },
          ]
        : [];
    const fullNameRaw = row.full_name != null ? String(row.full_name).trim() : "";
    const full_name_norm = normalizeName(fullNameRaw);
    if (!phones.length && !full_name_norm) {
      warnings.push(`clean json index ${i}: skip — no phone and no full_name`);
      continue;
    }
    const caseIdRaw = row.case_id != null ? String(row.case_id).trim() : "";
    const leadIdRaw = row.lead_id != null ? String(row.lead_id).trim() : "";
    leads.push({
      lead_id: leadIdRaw || null,
      case_id: caseIdRaw || leadIdRaw || null,
      row_number: i + 1,
      phones,
      phoneDetails,
      phoneNorm: phones[0] || null,
      full_name: fullNameRaw || null,
      full_name_norm: full_name_norm || null,
      createdMs: signed.getTime(),
      lead_status:
        row.lead_status != null ? normalizeLeadStatus(row.lead_status) : "signed up",
      signed_flag: true,
      case_type: row.case_type != null ? String(row.case_type).trim() || null : null,
      marketing_source:
        row.marketing_source != null ? String(row.marketing_source).trim() || null : null,
    });
  }
  return { leads, headers: [], warnings, format: "clean_json", sourcePath: filePath };
}

function loadLeads(resolvedPath) {
  if (!resolvedPath || !fs.existsSync(resolvedPath)) {
    warn(
      `Lead Docket input not found at ${resolvedPath || DEFAULT_LEADS_CLEAN}. Run node build_leaddocket_cases_clean.js on the Signed Up export, or place legacy .xlsx/.csv in /data.`
    );
    return {
      leads: [],
      headers: [],
      warnings: ["missing_leads_file"],
      format: null,
      sourcePath: null,
    };
  }
  const lower = resolvedPath.toLowerCase();
  if (lower.endsWith(".json")) return loadLeadsCleanJson(resolvedPath);
  if (lower.endsWith(".xlsx") || lower.endsWith(".xls")) return loadLeadsXlsx(resolvedPath);
  return loadLeadsCsv(resolvedPath);
}

function buildLeadsFromTable(headers, dataRows, sourcePath, format) {
  const createdIdx = findColumnIndex(headers, [
    "created date",
    "created at",
    "date created",
    "lead created",
    "date/time",
    /created/i,
  ]);
  const statusIdx = findColumnIndex(headers, [
    "lead status",
    "case status",
    "status",
    "stage",
    "lead stage",
    /lead status/i,
    /case status/i,
    /^status$/,
  ]);
  const caseIdx = findColumnIndex(headers, ["case type", "case_type", "matter type", "practice area", /case type/i]);
  const firstNameIdx = findColumnIndex(headers, ["first name", "first_name", /^first$/i]);
  const lastNameIdx = findColumnIndex(headers, ["last name", "last_name", /^last$/i]);
  const fullNameIdx = findColumnIndex(headers, ["full name", "full_name", "contact name", "contact_name", /^name$/i]);
  let marketingResolved = findColumnIndex(headers, [
    (norm) => norm === "marketing source",
    (norm) => norm === "lead source",
    (norm) => norm === "referral source",
    "marketing source",
    "utm source",
  ]);
  if (marketingResolved < 0) {
    marketingResolved = findColumnIndex(headers, [(norm) => norm === "source"]);
  }

  if (createdIdx < 0) warn('No "created" date column detected; check headers.');
  const leadIdIdx = findColumnIndex(headers, ["lead id", "lead_id", "source_lead_id", /^lead id$/i, /^source lead id$/i]);
  const caseIdIdx = findColumnIndex(headers, ["case id", "case_id", "matter id", "matter_id", /^case id$/i, /^matter id$/i]);
  const warnings = [];

  const leads = [];
  for (let r = 0; r < dataRows.length; r++) {
    const row = dataRows[r];
    try {
      if (!row || !row.length) continue;
      const phones = extractAllPhonesFromRow(row, headers);
      const createdVal = createdIdx >= 0 ? row[createdIdx] : "";
      const created = parseLeadDate(createdVal);
      if (!phones.length) {
        warnings.push(`row ${r + 2}: skip — phone missing or not 10 digits`);
        continue;
      }
      if (!created) {
        warnings.push(`row ${r + 2}: skip — bad created date`);
        continue;
      }
      const statusRaw = statusIdx >= 0 ? row[statusIdx] : "";
      const lead_status = normalizeLeadStatus(statusRaw);
      const signed_flag = signedFlagFromStatus(lead_status);
      const case_type = caseIdx >= 0 ? String(row[caseIdx] ?? "").trim() : "";
      const marketing_source =
        marketingResolved >= 0 ? String(row[marketingResolved] ?? "").trim() : "";
      const fullNameRaw =
        fullNameIdx >= 0
          ? String(row[fullNameIdx] ?? "").trim()
          : [firstNameIdx >= 0 ? String(row[firstNameIdx] ?? "").trim() : "", lastNameIdx >= 0 ? String(row[lastNameIdx] ?? "").trim() : ""]
              .filter(Boolean)
              .join(" ");

      leads.push({
        lead_id: leadIdIdx >= 0 ? String(row[leadIdIdx] ?? "").trim() || null : null,
        case_id: caseIdIdx >= 0 ? String(row[caseIdIdx] ?? "").trim() || null : null,
        row_number: r + 2,
        phones,
        phoneDetails: extractPhoneDetailsFromRow(row, headers),
        phoneNorm: phones[0],
        full_name: fullNameRaw || null,
        full_name_norm: normalizeName(fullNameRaw),
        createdMs: created.getTime(),
        lead_status: lead_status || null,
        signed_flag,
        case_type: case_type || null,
        marketing_source: marketing_source || null,
      });
    } catch (e) {
      warnings.push(`row ${r + 2}: ${e.message}`);
    }
  }

  return {
    leads,
    headers: headers.map((h) => headerToSnake(h)),
    warnings,
    format,
    sourcePath,
  };
}

function indexLeadsByPhone(leads) {
  const map = new Map();
  for (const L of leads) {
    for (const p of L.phones) {
      if (!map.has(p)) map.set(p, []);
      map.get(p).push(L);
    }
  }
  for (const arr of map.values()) arr.sort((a, b) => a.createdMs - b.createdMs);
  return map;
}

function leadDedupeKey(L) {
  return `${L.lead_id || L.case_id || ""}|${[...L.phones].sort().join(",")}|${L.createdMs}`;
}

function uniqueLeadsList(byPhone, allLeads) {
  const seen = new Set();
  const out = [];
  for (const arr of byPhone.values()) {
    for (const L of arr) {
      const k = leadDedupeKey(L);
      if (seen.has(k)) continue;
      seen.add(k);
      out.push(L);
    }
  }
  for (const L of allLeads) {
    if (L.phones && L.phones.length) continue;
    const k = leadDedupeKey(L);
    if (seen.has(k)) continue;
    seen.add(k);
    out.push(L);
  }
  return out;
}

function matchTypeRank(t) {
  if (t === "strict") return 0;
  if (t === "24hr") return 1;
  return 2;
}

function confidenceRank(c) {
  if (c === "high") return 0;
  if (c === "medium") return 1;
  if (c === "low") return 2;
  if (c === "extended") return 3;
  if (c === "very_low") return 4;
  return 99;
}

function passRank(passLabel) {
  return passLabel === "primary" ? 0 : 1;
}

function classifyDelta(deltaMs) {
  if (deltaMs <= STRICT_MS) return "strict";
  if (deltaMs <= HOUR24_MS) return "24hr";
  return "phone_only";
}

function confidenceFromMatch(phoneMatchType, deltaMsAbs) {
  if (phoneMatchType === "exact" && deltaMsAbs <= STRICT_MS) return "high";
  if (phoneMatchType === "exact" && deltaMsAbs <= HOUR24_MS) return "medium";
  if ((phoneMatchType === "last7" || phoneMatchType === "contains") && deltaMsAbs <= HOUR24_MS) {
    return "low";
  }
  return null;
}

function classifyExtendedConfidence(phoneMatchType, deltaMsAbs, nameAssist) {
  const primary = confidenceFromMatch(phoneMatchType, deltaMsAbs);
  if (primary) {
    return {
      match_confidence: primary,
      pass_label: "primary",
      match_strategy: nameAssist ? "name_assisted_match" : "phone_primary",
    };
  }
  if (phoneMatchType === "exact" && deltaMsAbs <= DAY7_MS) {
    return { match_confidence: "low", pass_label: "second_pass", match_strategy: "extended_exact_phone" };
  }
  if (phoneMatchType === "exact" && deltaMsAbs <= DAY14_MS) {
    return { match_confidence: "extended", pass_label: "second_pass", match_strategy: "extended_exact_phone" };
  }
  if ((phoneMatchType === "last7" || phoneMatchType === "contains") && deltaMsAbs <= DAY3_MS) {
    return {
      match_confidence: "very_low",
      pass_label: "second_pass",
      match_strategy: "partial_phone_extended",
    };
  }
  if (nameAssist && deltaMsAbs <= DAY7_MS) {
    return {
      match_confidence: "very_low",
      pass_label: "second_pass",
      match_strategy: "name_assisted_match",
    };
  }
  return null;
}

/** Prefer stricter tier, then smaller gap, then earlier lead created time. */
function betterClaim(a, b) {
  const ca = confidenceRank(a.match_confidence);
  const cb = confidenceRank(b.match_confidence);
  if (ca !== cb) return ca < cb ? a : b;
  const pa = passRank(a.pass_label);
  const pb = passRank(b.pass_label);
  if (pa !== pb) return pa < pb ? a : b;
  const ra = matchTypeRank(a.match_type);
  const rb = matchTypeRank(b.match_type);
  if (ra !== rb) return ra < rb ? a : b;
  if (a.deltaMs !== b.deltaMs) return a.deltaMs < b.deltaMs ? a : b;
  const da = a.durationSec || 0;
  const db = b.durationSec || 0;
  if (da !== db) return da > db ? a : b;
  const ia = a.directionRank == null ? 99 : a.directionRank;
  const ib = b.directionRank == null ? 99 : b.directionRank;
  if (ia !== ib) return ia < ib ? a : b;
  return a.lead.createdMs <= b.lead.createdMs ? a : b;
}

function pickUnmatchedReason(attempts) {
  if (!attempts.length) return "no_candidate_leads_within_14_days";
  const matchedKinds = attempts.filter((a) => a.match_type !== "none");
  if (!matchedKinds.length) return "no_phone_or_name_match_in_window";
  if (matchedKinds.some((a) => a.match_type === "last7" || a.match_type === "contains")) {
    return "partial_phone_only_outside_allowed_rules";
  }
  return "phone_match_present_but_below_confidence_rules";
}

function reasonForUnmatchedCall(cr, leads) {
  let anyWithin14 = false;
  let anyExact = false;
  let anyPartial = false;
  let anyName = false;
  for (const L of leads) {
    const deltaMsAbs = Math.abs(cr.callMs - L.createdMs);
    if (deltaMsAbs > DAY14_MS) continue;
    anyWithin14 = true;
    const phoneResult = bestPhoneMatch(cr.callPhones, L.phoneDetails || []);
    if (phoneResult.best) {
      if (phoneResult.best.match_type === "exact") anyExact = true;
      if (phoneResult.best.match_type === "last7" || phoneResult.best.match_type === "contains") anyPartial = true;
    }
    if (
      L.full_name_norm &&
      cr.callName &&
      (cr.callName.includes(L.full_name_norm) || L.full_name_norm.includes(cr.callName))
    ) {
      anyName = true;
    }
  }
  if (!anyWithin14) return "no_candidate_leads_within_14_days";
  if (!anyExact && !anyPartial && !anyName) return "no_phone_or_name_match_in_window";
  if (anyPartial && !anyExact) return "partial_phone_only_outside_allowed_rules";
  if (anyName && !anyExact) return "name_signal_only_below_threshold";
  return "competing_better_match_claimed_call";
}

/**
 * Primary pass keeps <=24h logic; second pass adds <=7d, <=14d exact matches and controlled partial/name assists.
 * Leads are linked to their single best call first; then call collisions are resolved by the same rank order.
 */
function assignLeadsToCalls(callsRaw, leads) {
  const callRecords = [];
  for (let i = 0; i < callsRaw.length; i++) {
    const call = callsRaw[i];
    if (!call || typeof call !== "object") continue;
    const callPhones = callPhoneCandidates(call);
    const callName = callNameText(call);
    const callMs = callTimestampMs(call);
    if ((callPhones.length || callName) && callMs != null) {
      callRecords.push({
        index: i,
        callMs,
        callPhones,
        callName,
        call,
        durationSec: callDurationSec(call),
        directionRank: callDirectionRank(call),
      });
    }
  }
  const debug = [];
  const leadWinners = [];

  for (const L of leads) {
    const attempts = [];
    let bestForLead = null;
    for (const cr of callRecords) {
      const deltaSignedMs = cr.callMs - L.createdMs;
      const deltaMsAbs = Math.abs(deltaSignedMs);
      if (deltaMsAbs > DAY14_MS) continue;

      const phoneResult = bestPhoneMatch(cr.callPhones, L.phoneDetails || []);
      const nameAssist =
        !!L.full_name_norm &&
        !!cr.callName &&
        (cr.callName.includes(L.full_name_norm) || L.full_name_norm.includes(cr.callName));
      for (const attempt of phoneResult.attempts) {
        attempts.push({
          ...attempt,
          lead_id: L.lead_id,
          case_id: L.case_id,
          lead_row_number: L.row_number,
          call_id: cr.call.call_id || cr.call.id || null,
          lead_created_at: new Date(L.createdMs).toISOString(),
          time_difference_minutes: Math.round((deltaSignedMs / 60000) * 100) / 100,
          name_assist: nameAssist,
        });
      }
      if (!phoneResult.best && !nameAssist) continue;

      const phoneMatchType = phoneResult.best ? phoneResult.best.match_type : "name_assisted";
      const classified = classifyExtendedConfidence(phoneMatchType, deltaMsAbs, nameAssist);
      if (!classified) continue;

      const claim = {
        lead: L,
        call_index: cr.index,
        call: cr.call,
        match_type: classifyDelta(Math.min(deltaMsAbs, DAY14_MS)),
        match_confidence: classified.match_confidence,
        pass_label: classified.pass_label,
        match_strategy: classified.match_strategy,
        phone_match_type: phoneMatchType,
        match_delta_minutes: Math.round((deltaSignedMs / 60000) * 100) / 100,
        deltaMs: deltaMsAbs,
        durationSec: cr.durationSec,
        directionRank: cr.directionRank,
        debug_best_phone: phoneResult.best || null,
      };
      bestForLead = bestForLead ? betterClaim(bestForLead, claim) : claim;
    }

    debug.push({
      lead_id: L.lead_id,
      case_id: L.case_id,
      lead_row_number: L.row_number,
      lead_created_at: new Date(L.createdMs).toISOString(),
      lead_name: L.full_name,
      matched: !!bestForLead,
      selected_match: bestForLead
        ? {
            call_index: bestForLead.call_index,
            call_id: bestForLead.call.call_id || bestForLead.call.id || null,
            match_confidence: bestForLead.match_confidence,
            pass_label: bestForLead.pass_label,
            match_type: bestForLead.match_type,
            phone_match_type: bestForLead.phone_match_type,
            match_strategy: bestForLead.match_strategy,
            time_difference_minutes: bestForLead.match_delta_minutes,
          }
        : null,
      attempts,
      unmatched_reason: bestForLead ? null : pickUnmatchedReason(attempts),
    });

    if (bestForLead) leadWinners.push(bestForLead);
  }

  const claimantsByCall = new Map();
  const leadsMatched = new Set();
  for (const claim of leadWinners) {
    const arr = claimantsByCall.get(claim.call_index) || [];
    arr.push(claim);
    claimantsByCall.set(claim.call_index, arr);
    leadsMatched.add(leadDedupeKey(claim.lead));
  }
  const winnerByCall = new Map();
  for (const [idx, arr] of claimantsByCall) {
    winnerByCall.set(idx, arr.reduce((w, c) => (w ? betterClaim(w, c) : c)));
  }

  const unmatchedCallReasonCounts = {};
  for (const cr of callRecords) {
    if (winnerByCall.has(cr.index)) continue;
    const reason = reasonForUnmatchedCall(cr, leads);
    unmatchedCallReasonCounts[reason] = (unmatchedCallReasonCounts[reason] || 0) + 1;
  }

  return { winnerByCall, debug, leadsMatched, callRecords, unmatchedCallReasonCounts };
}

function aggregateBySource(calls) {
  const map = new Map();
  for (const c of calls) {
    const name = c.source && String(c.source).trim() ? String(c.source).trim() : "(no source)";
    if (!map.has(name)) {
      map.set(name, {
        name,
        calls: 0,
        leads: 0,
        signed: 0,
        strict_matches: 0,
        matches_24hr: 0,
        phone_only_matches: 0,
        signed_flag_count: 0,
      });
    }
    const o = map.get(name);
    o.calls++;
    if (c.lead_created) o.leads++;
    if (c.lead_created && c.signed_flag) o.signed++;
    if (c.signed_flag) o.signed_flag_count++;
    if (c.match_type === "strict") o.strict_matches++;
    else if (c.match_type === "24hr") o.matches_24hr++;
    else if (c.match_type === "phone_only") o.phone_only_matches++;
  }
  const rows = [...map.values()].sort((a, b) => b.calls - a.calls);
  for (const o of rows) {
    const n = o.calls;
    o.high_conf_conversion_rate = n > 0 ? Math.round((o.strict_matches / n) * 10000) / 10000 : null;
    o.total_conversion_rate =
      n > 0 ? Math.round(((o.strict_matches + o.matches_24hr) / n) * 10000) / 10000 : null;
  }
  return rows;
}

/** Global reporting: confidence split + derived rates (all calls in window). */
function rollupMatchConfidence(enriched) {
  let strict_matches = 0;
  let matches_24hr = 0;
  let phone_only_matches = 0;
  let high = 0;
  let medium = 0;
  let low = 0;
  let extended = 0;
  let very_low = 0;
  let signed_flag_count = 0;
  for (const c of enriched) {
    if (c.match_type === "strict") strict_matches++;
    else if (c.match_type === "24hr") matches_24hr++;
    else if (c.match_type === "phone_only") phone_only_matches++;
    if (c.match_confidence === "high") high++;
    else if (c.match_confidence === "medium") medium++;
    else if (c.match_confidence === "low") low++;
    else if (c.match_confidence === "extended") extended++;
    else if (c.match_confidence === "very_low") very_low++;
    if (c.signed_flag) signed_flag_count++;
  }
  const total = enriched.length;
  return {
    strict_matches,
    matches_24hr,
    phone_only_matches,
    high,
    medium,
    low,
    extended,
    very_low,
    signed_flag_count,
    high_conf_conversion_rate:
      total > 0 ? Math.round((high / total) * 10000) / 10000 : null,
    total_conversion_rate:
      total > 0 ? Math.round(((high + medium + low + extended + very_low) / total) * 10000) / 10000 : null,
  };
}

function monthRollup(calls) {
  const months = {};
  for (const c of calls) {
    const mk = c.month_key || "unknown";
    if (!months[mk]) months[mk] = { calls: 0, leads: 0, signed: 0 };
    months[mk].calls++;
    if (c.lead_created) months[mk].leads++;
    if (c.lead_created && c.signed_flag) months[mk].signed++;
  }
  return months;
}

function summarizeCallLoadStats(callsRaw) {
  const stats = {
    total_calls_loaded: 0,
    calls_with_phone: 0,
    calls_missing_phone: 0,
    calls_with_timestamp: 0,
    calls_missing_timestamp: 0,
    calls_with_call_id: 0,
    calls_missing_call_id: 0,
    calls_used_for_matching: 0,
    calls_dropped_no_timestamp: 0,
    calls_dropped_no_phone_or_name: 0,
  };
  for (const call of callsRaw) {
    if (!call || typeof call !== "object") continue;
    stats.total_calls_loaded++;
    const hasPhone = callPhoneCandidates(call).length > 0;
    const hasTs = callTimestampMs(call) != null;
    const hasName = !!callNameText(call);
    const hasCallId = !!(call.call_id || call.id);
    if (hasPhone) stats.calls_with_phone++;
    else stats.calls_missing_phone++;
    if (hasTs) stats.calls_with_timestamp++;
    else {
      stats.calls_missing_timestamp++;
      stats.calls_dropped_no_timestamp++;
    }
    if (hasCallId) stats.calls_with_call_id++;
    else stats.calls_missing_call_id++;
    if (!hasTs) continue;
    if (!hasPhone && !hasName) {
      stats.calls_dropped_no_phone_or_name++;
      continue;
    }
    stats.calls_used_for_matching++;
  }
  return stats;
}

function main() {
  const callsPath = process.argv[2] || resolveFirstExisting(DEFAULT_CALLS);
  const leadsPath = resolveLeadsPath(process.argv[3]);

  if (!fs.existsSync(DATA)) fs.mkdirSync(DATA, { recursive: true });

  const callsRaw = loadCalls(callsPath);
  const {
    leads,
    warnings: leadWarnings,
    format: leadsFormat,
    sourcePath: leadsSourcePath,
  } = loadLeads(leadsPath);
  const callLoadStats = summarizeCallLoadStats(callsRaw);
  const byPhone = indexLeadsByPhone(leads);
  const uniqueLeads = uniqueLeadsList(byPhone, leads);
  const allWarnings = [...leadWarnings];
  const { winnerByCall, debug, leadsMatched, callRecords, unmatchedCallReasonCounts } = assignLeadsToCalls(
    callsRaw,
    uniqueLeads
  );

  let matched = 0;
  let signedTotal = 0;
  let withPhone = 0;
  const enriched = [];
  const matchBreakdown = { strict: 0, "24hr": 0, phone_only: 0 };

  for (let i = 0; i < callsRaw.length; i++) {
    const call = callsRaw[i];
    if (!call || typeof call !== "object") {
      allWarnings.push(`call index ${i}: skipped non-object`);
      continue;
    }
    const phoneNorm = normalizePhoneDigits(callCustomerPhone(call));
    const callMs = callTimestampMs(call);
    if (phoneNorm.length >= 10) withPhone++;

    let lead_created = false;
    let lead_status = null;
    let signed_flag = false;
    let case_type = null;
    let marketing_source = null;
    let matched_lead_created_ms = null;
    let match_type = null;
    let match_confidence = null;
    let phone_match_type = null;
    let match_pass = null;
    let match_strategy = null;
    let matched_lead_id = null;
    let matched_case_id = null;
    let match_delta_minutes = null;
    let match_debug = null;

    if ((phoneNorm.length >= 10 || callPhoneCandidates(call).length) && callMs != null) {
      const win = winnerByCall.get(i);
      if (win) {
        const hit = win.lead;
        lead_created = true;
        lead_status = hit.lead_status;
        signed_flag = !!hit.signed_flag;
        case_type = hit.case_type;
        marketing_source = hit.marketing_source;
        matched_lead_created_ms = hit.createdMs;
        match_type = win.match_type;
        match_confidence = win.match_confidence;
        phone_match_type = win.phone_match_type;
        match_pass = win.pass_label;
        match_strategy = win.match_strategy;
        matched_lead_id = hit.lead_id;
        matched_case_id = hit.case_id;
        match_delta_minutes = win.match_delta_minutes;
        match_debug = win.debug_best_phone;
        matched++;
        matchBreakdown[win.match_type]++;
        if (signed_flag) signedTotal++;
      }
    } else {
      if (callMs == null) allWarnings.push(`call ${call.call_id || i}: no parseable call timestamp`);
    }

    const month_key = parseMonthKeyFromRecord(call) || "unknown";
    enriched.push({
      ...call,
      month_key,
      lead_created,
      lead_status,
      signed_flag,
      case_type,
      marketing_source,
      match_type,
      match_confidence,
      phone_match_type,
      match_pass,
      match_strategy,
      matched_lead_id,
      matched_case_id,
      match_delta_minutes,
      match_debug,
      lead_match_delta_ms:
        matched_lead_created_ms != null && callMs != null ? matched_lead_created_ms - callMs : null,
    });
  }

  const total = enriched.length;
  const matchRate = total ? (100 * matched) / total : 0;
  const months = monthRollup(enriched);
  const bySource = {};
  for (const mk of Object.keys(months)) {
    const subset = enriched.filter((c) => c.month_key === mk);
    bySource[mk] = aggregateBySource(subset);
  }
  const matchConfidenceSummary = rollupMatchConfidence(enriched);
  const unmatchedCalls = enriched
    .filter((c) => !c.lead_created)
    .map((c) => ({
      call_id: c.call_id || c.id || null,
      call_start_time: c.call_start_time || c.start_time || c.created_at || null,
      customer_phone_number: c.customer_phone_number || c.caller_phone_number || null,
      normalized_phone: normalizePhoneDigits(callCustomerPhone(c)) || "",
      source: c.source || c.formatted_tracking_source || null,
    }));
  const unmatchedReasonCountsFull = { ...unmatchedCallReasonCounts };
  for (const c of enriched) {
    if (c.lead_created) continue;
    const hasTs =
      c.call_start_time != null || c.start_time != null || c.created_at != null
        ? callTimestampMs(c) != null
        : false;
    const hasPhone = callPhoneCandidates(c).length > 0;
    if (!hasTs) {
      unmatchedReasonCountsFull.no_parseable_call_timestamp =
        (unmatchedReasonCountsFull.no_parseable_call_timestamp || 0) + 1;
    } else if (!hasPhone) {
      unmatchedReasonCountsFull.no_usable_call_phone =
        (unmatchedReasonCountsFull.no_usable_call_phone || 0) + 1;
    }
  }
  const unmatchedLeads = uniqueLeads
    .filter((L) => !leadsMatched.has(leadDedupeKey(L)))
    .map((L) => ({
      lead_id: L.lead_id,
      case_id: L.case_id,
      row_number: L.row_number,
      created_at: new Date(L.createdMs).toISOString(),
      phones: (L.phoneDetails || []).map((p) => ({
        field: p.header,
        original_phone: p.raw,
        normalized_phone: p.normalized || "",
      })),
      lead_status: L.lead_status,
      case_type: L.case_type,
      marketing_source: L.marketing_source,
    }));
  const unmatchedCallReasonsTop = Object.entries(unmatchedReasonCountsFull)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 10)
    .map(([reason, count]) => ({ reason, count }));

  const out = {
    generated_at: new Date().toISOString(),
    inputs: {
      calls_path: callsPath,
      leads_format: leadsFormat,
      call_load_stats: callLoadStats,
      /** Path to leads file when loaded (.xlsx or .csv). `leads_csv_path` kept for dashboard compat. */
      leads_csv_path: leadsSourcePath,
      match_policy: {
        description:
          "Fuzzy phone matching across all Lead Docket phone fields, constrained to +/- 24 hours between call time and lead creation.",
        normalize_phone: "strip non-digits; if 11 digits and starts with 1, remove the leading 1; canonical target is 10 digits",
        preferred_window_minutes: 240,
        max_window_minutes: 1440,
        second_pass_windows: {
          exact_phone_days: [7, 14],
          partial_phone_days: 3,
          name_assisted_days: 7,
        },
        phone_rules: ["exact normalized 10-digit", "last 7-digit fallback", "contains fallback"],
        confidence_rules: {
          high: "exact phone + within 4 hours",
          medium: "exact phone + within 24 hours",
          low: "exact phone + within 7 days (plus preserved legacy partial <=24h)",
          extended: "exact phone + within 14 days",
          very_low: "partial phone + within 3 days or name-assisted <=7 days",
        },
      },
      lead_row_count: leads.length,
      parse_warnings_sample: allWarnings.slice(0, 50),
      parse_warnings_total: allWarnings.length,
    },
    summary: {
      total_calls: total,
      matched_leads: matched,
      signed_cases: signedTotal,
      strict_matches: matchBreakdown.strict,
      match_rate_pct: Math.round(matchRate * 10) / 10,
      total_signed: signedTotal,
      calls_with_customer_phone: withPhone,
      match_breakdown: matchBreakdown,
      match_confidence: matchConfidenceSummary,
      unmatched_calls: unmatchedCalls.length,
      unmatched_leads: unmatchedLeads.length,
      unmatched_call_reasons_top: unmatchedCallReasonsTop,
    },
    months,
    by_source: bySource,
    unmatched: {
      calls: unmatchedCalls,
      leads: unmatchedLeads,
    },
    calls: enriched,
  };

  fs.writeFileSync(OUT_ENRICHED, JSON.stringify(out, null, 2), "utf8");
  fs.writeFileSync(
    OUT_MATCH_DEBUG,
    JSON.stringify(
      {
        generated_at: new Date().toISOString(),
        inputs: {
          calls_path: callsPath,
          leads_path: leadsSourcePath,
          calls_considered: callRecords.length,
          unique_leads_considered: uniqueLeads.length,
        },
        attempts: debug,
      },
      null,
      2
    ),
    "utf8"
  );

  const statusSet = new Set();
  for (const L of leads) {
    const st = L.lead_status != null && String(L.lead_status).trim() ? String(L.lead_status).trim() : "(empty)";
    statusSet.add(st);
  }
  const statusesSorted = [...statusSet].sort();

  console.log("Lead Docket <-> CallRail enrich");
  console.log("  calls path:        ", callsPath);
  console.log("  load stats:        ", JSON.stringify(callLoadStats));
  console.log("  total calls:       ", total);
  console.log("  matched leads:     ", matched, "(calls with lead_created)");
  console.log("  signed cases:      ", signedTotal);
  console.log("  strict matches:    ", matchBreakdown.strict, "(calls)");
  console.log("  match rate %:      ", (Math.round(matchRate * 10) / 10).toFixed(1));
  console.log("  total_signed:      ", signedTotal, "(counts signed_flag on calls)");
  console.log("  unmatched calls:   ", unmatchedCalls.length);
  console.log("  unmatched leads:   ", unmatchedLeads.length);
  console.log(
    "  confidence split:  ",
    "high:",
    matchConfidenceSummary.high,
    " medium:",
    matchConfidenceSummary.medium,
    " low:",
    matchConfidenceSummary.low,
    " extended:",
    matchConfidenceSummary.extended,
    " very_low:",
    matchConfidenceSummary.very_low
  );
  console.log(
    "  match breakdown:   ",
    "strict:",
    matchBreakdown.strict,
    " 24hr:",
    matchBreakdown["24hr"],
    " phone_only:",
    matchBreakdown.phone_only
  );
  console.log("  unique lead_status values (" + statusesSorted.length + "):");
  console.log("   ", statusesSorted.join(" | "));
  console.log(
    "  conversion (all): high_conf=",
    matchConfidenceSummary.high_conf_conversion_rate,
    " total_strict+24hr=",
    matchConfidenceSummary.total_conversion_rate,
    " signed_flag_count=",
    matchConfidenceSummary.signed_flag_count
  );
  console.log("  wrote:             ", OUT_ENRICHED);
  console.log("  debug wrote:       ", OUT_MATCH_DEBUG);
  if (unmatchedCallReasonsTop.length) {
    console.log(
      "  top unmatched:     ",
      unmatchedCallReasonsTop.map((x) => `${x.reason}:${x.count}`).join(" | ")
    );
  }
  if (allWarnings.length) console.log("  warnings (sample): ", allWarnings.slice(0, 5).join(" | "));
}

main();
