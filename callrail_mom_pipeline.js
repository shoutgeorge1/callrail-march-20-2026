/**
 * Month buckets → scored files, aggregates, chart JSON, latest + combined export.
 * Reads: callrail_transcripts_last60days.json
 */
const fs = require("fs");
const path = require("path");
const {
  scoreOneRecord,
  parseMonthKeyFromRecord,
  syntheticMonthKey,
  normalizeSourceBucket,
  WASTE_TYPES,
  OPPORTUNITY_THRESHOLD,
  SWITCH_THRESHOLD,
} = require("./callrail-score-core");

const ROOT = __dirname;
const INPUT = path.join(ROOT, "callrail_transcripts_last60days.json");
const OUT_SUMMARY = path.join(ROOT, "callrail_month_summary.json");
const OUT_LATEST = path.join(ROOT, "callrail_scored_calls_latest.json");
const OUT_COMBINED = path.join(ROOT, "callrail_scored_calls.json");
const OUT_SOURCE_TREND = path.join(ROOT, "source_mix_trend.json");
const OUT_OPP_TREND = path.join(ROOT, "opportunity_rate_trend.json");
const OUT_DUR_TREND = path.join(ROOT, "duration_trend.json");

const BUCKETS = ["google_ads", "gmb", "direct", "chat", "referral", "unknown"];

function loadTranscripts() {
  try {
    const raw = fs.readFileSync(INPUT, { encoding: "utf8", flag: "r" });
    const data = JSON.parse(raw);
    return Array.isArray(data) ? data : [];
  } catch {
    return [];
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
  const [y, m] = key.split("-");
  return path.join(ROOT, `callrail_scored_calls_${y}_${m}.json`);
}

function main() {
  const rows = loadTranscripts();
  const endDate = new Date();

  if (!rows.length) {
    fs.writeFileSync(OUT_COMBINED, "[]", "utf8");
    fs.writeFileSync(OUT_LATEST, "[]", "utf8");
    fs.writeFileSync(OUT_SUMMARY, JSON.stringify({}, null, 2), "utf8");
    fs.writeFileSync(OUT_SOURCE_TREND, "[]", "utf8");
    fs.writeFileSync(OUT_OPP_TREND, "[]", "utf8");
    fs.writeFileSync(OUT_DUR_TREND, "[]", "utf8");
    console.log("callrail_mom_pipeline: empty input, wrote empty outputs.");
    return;
  }

  let dated = 0;
  for (let j = 0; j < rows.length; j++) {
    if (rows[j] && parseMonthKeyFromRecord(rows[j])) dated++;
  }
  const useSynthetic = dated < Math.max(5, Math.floor(rows.length * 0.05));

  const byMonth = {};
  const undated = [];
  for (let k = 0; k < rows.length; k++) {
    const r = rows[k];
    if (!r || typeof r !== "object") continue;
    let mk = parseMonthKeyFromRecord(r);
    if (mk == null) {
      if (useSynthetic) mk = syntheticMonthKey(k, rows.length, endDate);
      else {
        undated.push(r);
        continue;
      }
    }
    if (!byMonth[mk]) byMonth[mk] = [];
    byMonth[mk].push(r);
  }

  let monthKeys = Object.keys(byMonth).sort();
  if (!useSynthetic && undated.length) {
    if (monthKeys.length) {
      const target = monthKeys[monthKeys.length - 1];
      byMonth[target] = (byMonth[target] || []).concat(undated);
    } else {
      for (let u = 0; u < undated.length; u++) {
        const mk = syntheticMonthKey(u, undated.length, endDate);
        if (!byMonth[mk]) byMonth[mk] = [];
        byMonth[mk].push(undated[u]);
      }
      monthKeys = Object.keys(byMonth).sort();
    }
  }

  if (monthKeys.length === 0) {
    const valid = rows.filter((x) => x && typeof x === "object");
    const mk = syntheticMonthKey(0, Math.max(1, valid.length), endDate);
    byMonth[mk] = valid;
    monthKeys = Object.keys(byMonth).sort();
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

  fs.writeFileSync(OUT_SUMMARY, JSON.stringify(summary, null, 2), "utf8");

  const latestKey = monthKeys.length ? monthKeys[monthKeys.length - 1] : null;
  const latestCalls = latestKey && scoredByMonth[latestKey] ? scoredByMonth[latestKey] : [];
  fs.writeFileSync(OUT_LATEST, JSON.stringify(latestCalls, null, 2), "utf8");

  const combined = [];
  for (const mk of monthKeys) {
    if (scoredByMonth[mk]) combined.push(...scoredByMonth[mk]);
  }
  fs.writeFileSync(OUT_COMBINED, JSON.stringify(combined, null, 2), "utf8");

  const sourceMixTrend = [];
  const oppRateTrend = [];
  const durTrend = [];

  for (const mk of monthKeys) {
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

  const last = monthKeys.length ? summary[monthKeys[monthKeys.length - 1]] : null;
  console.log("callrail_mom_pipeline");
  console.log("  months written:", monthKeys.filter((k) => summary[k]).join(", ") || "(none)");
  console.log("  latest month:", latestKey || "—");
  console.log("  combined scored:", combined.length + " calls");
  if (last) {
    console.log("  latest opportunity_rate:", last.opportunity_rate + "%");
    console.log("  latest waste_share:", last.waste_share + "%");
  }
  if (useSynthetic) console.log("  note: used synthetic month split (few real timestamps in transcripts)");
}

main();
