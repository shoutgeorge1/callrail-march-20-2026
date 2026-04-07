/**
 * Month buckets from real timestamps only. month_key "unknown" when undated.
 * No synthetic splitting. Writes scored files, summary, trends, callrail_build_summary.json
 */
const fs = require("fs");
const path = require("path");
const {
  scoreOneRecord,
  parseMonthKeyFromRecord,
  getPreferredTimestamp,
  hasValidTimestampRecord,
  WASTE_TYPES,
  OPPORTUNITY_THRESHOLD,
  SWITCH_THRESHOLD,
} = require("./callrail-score-core");

const ROOT = __dirname;
const INPUT = path.join(ROOT, "callrail_transcripts_last60days.json");
const INGEST_META = path.join(ROOT, "callrail_ingest_meta.json");
const OUT_SUMMARY = path.join(ROOT, "callrail_month_summary.json");
const OUT_LATEST = path.join(ROOT, "callrail_scored_calls_latest.json");
const OUT_COMBINED = path.join(ROOT, "callrail_scored_calls.json");
const OUT_BUILD_SUMMARY = path.join(ROOT, "callrail_build_summary.json");
const OUT_SOURCE_TREND = path.join(ROOT, "source_mix_trend.json");
const OUT_OPP_TREND = path.join(ROOT, "opportunity_rate_trend.json");
const OUT_DUR_TREND = path.join(ROOT, "duration_trend.json");

const BUCKETS = ["google_ads", "gmb", "direct", "chat", "referral", "unknown"];
const UNKNOWN_KEY = "unknown";

function loadTranscripts() {
  try {
    const raw = fs.readFileSync(INPUT, { encoding: "utf8", flag: "r" });
    const data = JSON.parse(raw);
    return Array.isArray(data) ? data : [];
  } catch {
    return [];
  }
}

function loadIngestMeta() {
  try {
    const raw = fs.readFileSync(INGEST_META, { encoding: "utf8", flag: "r" });
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function median(arr) {
  if (!arr.length) return null;
  const s = arr.slice().sort((a, b) => a - b);
  const mid = Math.floor(s.length / 2);
  if (s.length % 2) return s[mid];
  return (s[mid - 1] + s[mid]) / 2;
}

function isOpportunityCall(c) {
  if (c.call_type === "true_pi_opportunity") return true;
  return c.opportunity_score >= OPPORTUNITY_THRESHOLD;
}

function aggregateMonth(calls) {
  const n = calls.length;
  const durs = calls.map((c) => c.duration).filter((d) => Number.isFinite(d));
  const opportunity_calls = calls.filter(isOpportunityCall).length;
  const waste_calls = calls.filter((c) => WASTE_TYPES.includes(c.call_type)).length;
  const switch_signal_calls = calls.filter((c) => c.attorney_switch_probability >= SWITCH_THRESHOLD).length;
  const hidden_opportunity_count = calls.filter((c) => c.hidden_opportunity_flag === true).length;

  const opportunity_by_source = {};
  for (const b of BUCKETS) {
    opportunity_by_source[b] = { total: 0, opportunity_count: 0 };
  }
  for (const c of calls) {
    const b = BUCKETS.includes(c.source_bucket) ? c.source_bucket : "unknown";
    if (!opportunity_by_source[b]) opportunity_by_source[b] = { total: 0, opportunity_count: 0 };
    opportunity_by_source[b].total++;
    if (isOpportunityCall(c)) opportunity_by_source[b].opportunity_count++;
  }

  return {
    total_calls: n,
    opportunity_calls,
    opportunity_rate: n ? Math.round((opportunity_calls / n) * 1000) / 10 : 0,
    waste_calls,
    waste_share: n ? Math.round((waste_calls / n) * 1000) / 10 : 0,
    switch_signal_calls,
    median_duration: median(durs),
    opportunity_by_source,
    hidden_opportunity_count,
  };
}

function monthFileName(key) {
  if (key === UNKNOWN_KEY) return path.join(ROOT, "callrail_scored_calls_unknown.json");
  const [y, m] = key.split("-");
  return path.join(ROOT, `callrail_scored_calls_${y}_${m}.json`);
}

function sortMonthKeys(keys) {
  const real = keys.filter((k) => k !== UNKNOWN_KEY && /^\d{4}-\d{2}$/.test(k));
  real.sort();
  const out = [...real];
  if (keys.includes(UNKNOWN_KEY)) out.push(UNKNOWN_KEY);
  return out;
}

function formatUsDate(iso) {
  if (!iso) return null;
  const ms = Date.parse(iso);
  if (Number.isNaN(ms)) return null;
  const d = new Date(ms);
  const mon = [
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
  ][d.getMonth()];
  return `${mon} ${d.getDate()}, ${d.getFullYear()}`;
}

function aggregateRawSources(calls) {
  const map = new Map();
  for (const c of calls) {
    const name = c.source || "(no source)";
    if (!map.has(name)) map.set(name, { calls: 0, qualified_estimate: 0 });
    const o = map.get(name);
    o.calls++;
    if (isOpportunityCall(c)) o.qualified_estimate++;
  }
  return [...map.entries()]
    .map(([name, v]) => ({ name, calls: v.calls, qualified_estimate: v.qualified_estimate }))
    .sort((a, b) => b.calls - a.calls);
}

function main() {
  const rows = loadTranscripts();

  if (!rows.length) {
    fs.writeFileSync(OUT_COMBINED, "[]", "utf8");
    fs.writeFileSync(OUT_LATEST, "[]", "utf8");
    fs.writeFileSync(OUT_SUMMARY, JSON.stringify({}, null, 2), "utf8");
    fs.writeFileSync(OUT_SOURCE_TREND, "[]", "utf8");
    fs.writeFileSync(OUT_OPP_TREND, "[]", "utf8");
    fs.writeFileSync(OUT_DUR_TREND, "[]", "utf8");
    fs.writeFileSync(OUT_BUILD_SUMMARY, JSON.stringify({ error: "empty_input" }, null, 2), "utf8");
    console.log("callrail_mom_pipeline: empty input, wrote empty outputs.");
    return;
  }

  const byMonth = {};
  for (let k = 0; k < rows.length; k++) {
    const r = rows[k];
    if (!r || typeof r !== "object") continue;
    const mk = parseMonthKeyFromRecord(r) || UNKNOWN_KEY;
    if (!byMonth[mk]) byMonth[mk] = [];
    byMonth[mk].push(r);
  }

  let monthKeys = sortMonthKeys(Object.keys(byMonth));

  if (monthKeys.length === 0) {
    console.error("callrail_mom_pipeline: no month buckets (unexpected).");
    return;
  }

  const summary = {};
  const scoredByMonth = {};

  for (const mk of monthKeys) {
    const bucketRows = byMonth[mk];
    const scored = [];
    for (let i = 0; i < bucketRows.length; i++) {
      try {
        const one = scoreOneRecord(bucketRows[i], i);
        if (one) {
          one.month_key = mk;
          scored.push(one);
        }
      } catch {
        /* */
      }
    }
    if (scored.length === 0) continue;
    scoredByMonth[mk] = scored;
    fs.writeFileSync(monthFileName(mk), JSON.stringify(scored, null, 2), "utf8");
    summary[mk] = aggregateMonth(scored);
  }

  monthKeys = sortMonthKeys(Object.keys(summary));
  const realKeys = monthKeys.filter((k) => k !== UNKNOWN_KEY && /^\d{4}-\d{2}$/.test(k));
  const latestRealKey = realKeys.length ? realKeys[realKeys.length - 1] : null;
  const latestKey = latestRealKey || (monthKeys.includes(UNKNOWN_KEY) ? UNKNOWN_KEY : monthKeys[monthKeys.length - 1]);
  const priorRealKey = realKeys.length >= 2 ? realKeys[realKeys.length - 2] : null;
  const priorMonthReconciled = priorRealKey != null;

  const latestCalls = latestKey && scoredByMonth[latestKey] ? scoredByMonth[latestKey] : [];
  const priorCalls = priorRealKey && scoredByMonth[priorRealKey] ? scoredByMonth[priorRealKey] : [];

  const combined = [];
  for (const mk of monthKeys) {
    if (scoredByMonth[mk]) combined.push(...scoredByMonth[mk]);
  }

  fs.writeFileSync(OUT_SUMMARY, JSON.stringify(summary, null, 2), "utf8");
  fs.writeFileSync(OUT_LATEST, JSON.stringify(latestCalls, null, 2), "utf8");
  fs.writeFileSync(OUT_COMBINED, JSON.stringify(combined, null, 2), "utf8");

  const sourceMixTrend = [];
  const oppRateTrend = [];
  const durTrend = [];

  for (const mk of monthKeys) {
    if (mk === UNKNOWN_KEY) continue;
    const agg = summary[mk];
    if (!agg) continue;
    const row = { month: mk, total: agg.total_calls };
    for (const b of BUCKETS) {
      row[b] = agg.opportunity_by_source[b] ? agg.opportunity_by_source[b].total : 0;
    }
    sourceMixTrend.push(row);
    oppRateTrend.push({
      month: mk,
      total_calls: agg.total_calls,
      opportunity_calls: agg.opportunity_calls,
      opportunity_rate: agg.opportunity_rate,
    });
    durTrend.push({
      month: mk,
      median_duration_sec: agg.median_duration,
    });
  }

  fs.writeFileSync(OUT_SOURCE_TREND, JSON.stringify(sourceMixTrend, null, 2), "utf8");
  fs.writeFileSync(OUT_OPP_TREND, JSON.stringify(oppRateTrend, null, 2), "utf8");
  fs.writeFileSync(OUT_DUR_TREND, JSON.stringify(durTrend, null, 2), "utf8");

  const tsMsList = [];
  for (const c of combined) {
    const t = getPreferredTimestamp(c);
    if (t) {
      const ms = Date.parse(t);
      if (!Number.isNaN(ms)) tsMsList.push(ms);
    }
  }
  tsMsList.sort((a, b) => a - b);
  const minIso = tsMsList.length ? new Date(tsMsList[0]).toISOString() : null;
  const maxIso = tsMsList.length ? new Date(tsMsList[tsMsList.length - 1]).toISOString() : null;
  const rangeDisplay =
    minIso && maxIso ? `${formatUsDate(minIso)} – ${formatUsDate(maxIso)}` : "— (no parseable timestamps)";

  const ingestMeta = loadIngestMeta();
  const tsCoveragePct =
    ingestMeta && ingestMeta.timestamp_coverage_pct != null
      ? ingestMeta.timestamp_coverage_pct
      : ingestMeta && ingestMeta.pct_valid_timestamp != null
        ? ingestMeta.pct_valid_timestamp
        : Math.round((100 * combined.filter(hasValidTimestampRecord).length) / Math.max(1, combined.length) * 10) / 10;

  const unknownCount = summary[UNKNOWN_KEY] ? summary[UNKNOWN_KEY].total_calls : 0;
  const curAgg = latestKey ? summary[latestKey] : null;
  const callsThisMonth = curAgg ? curAgg.total_calls : 0;
  const qualifiedEstimate = curAgg ? curAgg.opportunity_calls : 0;

  const sourcesCurrent = aggregateRawSources(latestCalls);
  const sourcesPrior = priorCalls.length ? aggregateRawSources(priorCalls) : [];
  const topSources = sourcesCurrent.slice(0, 8);

  const buildSummary = {
    generated_at: new Date().toISOString(),
    bucket_policy: "calendar_month_from_call_timestamps_only",
    unknown_month_call_count: unknownCount,
    timestamp_coverage_pct: tsCoveragePct,
    preferred_timestamp_min_iso: minIso,
    preferred_timestamp_max_iso: maxIso,
    date_range_display: rangeDisplay,
    current_month_key: latestKey,
    calls_this_month: callsThisMonth,
    qualified_leads_estimate: qualifiedEstimate,
    prior_month_reconciled: priorMonthReconciled,
    prior_month_key: priorRealKey,
    calls_prior_month: priorMonthReconciled && summary[priorRealKey] ? summary[priorRealKey].total_calls : null,
    qualified_prior_estimate:
      priorMonthReconciled && summary[priorRealKey] ? summary[priorRealKey].opportunity_calls : null,
    sources_current_month: sourcesCurrent,
    sources_prior_month: priorMonthReconciled ? sourcesPrior : [],
    top_sources: topSources,
    ingest_meta: ingestMeta,
  };

  fs.writeFileSync(OUT_BUILD_SUMMARY, JSON.stringify(buildSummary, null, 2), "utf8");

  console.log("callrail_mom_pipeline");
  console.log("  months written:", monthKeys.filter((k) => summary[k]).join(", ") || "(none)");
  console.log("  latest month file:", latestKey || "—");
  console.log("  unknown (no date) calls:", unknownCount);
  console.log("  combined scored:", combined.length);
  if (curAgg) {
    console.log("  latest injury-shaped / high-score count (estimate):", curAgg.opportunity_calls);
    console.log("  latest transcript-modeled qualified share:", curAgg.opportunity_rate + "%");
  }

  console.log("");
  console.log("=== Build summary (dashboard + ops) ===");
  console.log("  Calls this month (" + (latestKey || "—") + "):", callsThisMonth);
  console.log("  Qualified leads estimate (transcript model):", qualifiedEstimate);
  console.log("  Timestamp coverage % (ingest or scored):", tsCoveragePct);
  console.log("  Top sources by call count:");
  for (let i = 0; i < Math.min(5, topSources.length); i++) {
    const s = topSources[i];
    console.log(`    ${i + 1}. ${s.name}: ${s.calls} calls (${s.qualified_estimate} injury-shaped est.)`);
  }
  if (!priorMonthReconciled) {
    console.log("  Prior month: not yet reconciled (need 2+ calendar months with timestamps in range).");
  } else {
    console.log("  Prior month:", priorRealKey, "calls:", buildSummary.calls_prior_month);
  }
}

main();
