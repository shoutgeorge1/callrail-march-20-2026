/**
 * Build data/leaddocket_cases_clean.json from Lead Docket "Leads (Signed Up)" export (XLSX/CSV).
 * CLI: node build_leaddocket_cases_clean.js [path/to/export.xlsx|.csv]
 * Default input: data/leaddocket_signed_up_export.xlsx (copy export there or pass path).
 */
const fs = require("fs");
const path = require("path");
const XLSX = require("xlsx");

const ROOT = __dirname;
const DATA = path.join(ROOT, "data");
const OUT = path.join(DATA, "leaddocket_cases_clean.json");
const DEFAULT_INPUT = path.join(DATA, "leaddocket_signed_up_export.xlsx");

function normalizeHeader(h) {
  return String(h || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

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

function findColumnIndex(headers, matchers) {
  const norm = headers.map(normalizeHeader);
  for (const m of matchers) {
    for (let i = 0; i < norm.length; i++) {
      if (typeof m === "string" && norm[i] === m) return i;
      if (m instanceof RegExp && m.test(headers[i] || "")) return i;
    }
  }
  return -1;
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

function normalizePhoneToTen(s) {
  if (s == null || s === "") return null;
  const d = String(s).replace(/\D/g, "");
  if (!d) return null;
  let x = d.length === 11 && d[0] === "1" ? d.slice(1) : d;
  if (x.length > 10) x = x.slice(-10);
  if (x.length === 10) return x;
  return null;
}

function statusPassesFilter(statusRaw) {
  const t = String(statusRaw || "")
    .trim()
    .toLowerCase();
  if (!t) return null;
  if (/\bunsigned\b/.test(t) || /\bnot\s+signed\b/.test(t)) return false;
  if (t.includes("signed") || t.includes("retainer") || t.includes("closed")) return true;
  return false;
}

function matrixFromFile(filePath) {
  const lower = filePath.toLowerCase();
  if (lower.endsWith(".xlsx") || lower.endsWith(".xls")) {
    const wb = XLSX.readFile(filePath, { cellDates: true, raw: false });
    const sheetName = wb.SheetNames[0];
    if (!sheetName) return [];
    const sheet = wb.Sheets[sheetName];
    return XLSX.utils.sheet_to_json(sheet, { header: 1, defval: "", raw: false });
  }
  const text = fs.readFileSync(filePath, "utf8");
  return parseCsv(text);
}

function detectHeaderRowIndex(matrix) {
  if (!matrix.length) return 0;
  const a = String(matrix[0][0] || "").trim();
  if (/lead docket\s*\|/i.test(a) || /^lead docket$/i.test(a)) return 1;
  return 0;
}

function buildCleanRecords(matrix) {
  const warnings = [];
  const hi = detectHeaderRowIndex(matrix);
  if (hi >= matrix.length) {
    warnings.push("no header row");
    return { records: [], warnings };
  }
  const headers = matrix[hi].map((h) => String(h));
  const dataRows = matrix
    .slice(hi + 1)
    .filter((row) => row && row.some((cell) => String(cell).trim() !== ""));

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
  const caseIdIdx = findColumnIndex(headers, [
    "case id",
    "case_id",
    "matter id",
    "matter_id",
    /^case id$/i,
    /^matter id$/i,
  ]);
  const leadIdIdx = findColumnIndex(headers, [
    "lead id",
    "lead_id",
    "source_lead_id",
    /^lead id$/i,
    /^source lead id$/i,
  ]);
  const signedIdx = findColumnIndex(headers, [
    "signed date",
    "signed up date",
    "signed_date",
    /signed up date/i,
    /signed date/i,
  ]);
  const retainIdx = findColumnIndex(headers, [
    "retain date",
    "retainer date",
    "retained date",
    /retain/i,
  ]);
  const closedIdx = findColumnIndex(headers, ["closed date", "date closed", /closed date/i]);
  const phoneIdx = findColumnIndex(headers, [
    "primary phone",
    "phone",
    "mobile",
    "cell",
    "contact phone",
    "phone number",
    "primary_phone",
  ]);
  const sourceIdx = findColumnIndex(headers, [
    (norm) => norm === "marketing source",
    (norm) => norm === "lead source",
    "marketing source",
    "utm source",
    (norm) => norm === "source",
    "source",
  ]);
  const nameIdx = findColumnIndex(headers, ["full name", "full_name", "contact name", "name", /^name$/i]);
  const caseTypeIdx = findColumnIndex(headers, ["case type", "case_type", "matter type", /case type/i]);

  const hasExplicitStatus = statusIdx >= 0;
  const records = [];

  for (let r = 0; r < dataRows.length; r++) {
    const row = dataRows[r];
    const excelRow = hi + 2 + r;

    if (hasExplicitStatus) {
      const st = row[statusIdx];
      const pass = statusPassesFilter(st);
      if (pass === false) continue;
    }

    let signedAt = null;
    if (signedIdx >= 0) signedAt = parseLeadDate(row[signedIdx]);
    if (!signedAt && retainIdx >= 0) signedAt = parseLeadDate(row[retainIdx]);
    if (!signedAt && closedIdx >= 0) signedAt = parseLeadDate(row[closedIdx]);

    if (!signedAt) {
      warnings.push(`row ${excelRow}: skip — no signed/retain/closed date`);
      continue;
    }

    const caseId =
      caseIdIdx >= 0 ? String(row[caseIdIdx] ?? "").trim() || null : null;
    const leadId =
      leadIdIdx >= 0 ? String(row[leadIdIdx] ?? "").trim() || null : null;
    const id = caseId || leadId || `su:${excelRow}`;

    const primary_phone = phoneIdx >= 0 ? normalizePhoneToTen(row[phoneIdx]) : null;
    const marketing_source =
      sourceIdx >= 0 ? String(row[sourceIdx] ?? "").trim() || null : null;
    const full_name = nameIdx >= 0 ? String(row[nameIdx] ?? "").trim() || null : null;
    const case_type = caseTypeIdx >= 0 ? String(row[caseTypeIdx] ?? "").trim() || null : null;

    const rec = {
      case_id: id,
      signed_date: signedAt.toISOString(),
      primary_phone,
      marketing_source,
    };
    if (full_name) rec.full_name = full_name;
    if (case_type) rec.case_type = case_type;
    records.push(rec);
  }

  return { records, warnings };
}

function main() {
  const inputPath = process.argv[2] || DEFAULT_INPUT;
  if (!fs.existsSync(inputPath)) {
    console.error(`Input not found: ${inputPath}`);
    console.error("Usage: node build_leaddocket_cases_clean.js [export.xlsx|.csv]");
    process.exit(1);
  }
  if (!fs.existsSync(DATA)) fs.mkdirSync(DATA, { recursive: true });

  const matrix = matrixFromFile(inputPath);
  const { records, warnings } = buildCleanRecords(matrix);
  fs.writeFileSync(OUT, JSON.stringify(records, null, 2), "utf8");
  console.log(`Wrote ${records.length} rows → ${OUT}`);
  console.log(`Source: ${inputPath}`);
  if (warnings.length) console.log(`Warnings (sample): ${warnings.slice(0, 8).join(" | ")}`);
}

main();
