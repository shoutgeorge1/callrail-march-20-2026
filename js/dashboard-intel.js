/**
 * CallRail Intake & Call Quality Report — CallRail truth first; transcripts for patterns only.
 */
(function () {
  "use strict";

  var CALL_TYPE_ORDER = [
    "true_pi_opportunity",
    "unclear",
    "no_transcript",
    "property_damage_only",
    "wrong_firm",
    "vendor_sales",
    "existing_client",
    "admin",
  ];

  var CALL_TYPE_LABEL = {
    true_pi_opportunity: "Possible injury matter",
    unclear: "Unclear from transcript",
    no_transcript: "No transcript available",
    property_damage_only: "Property damage",
    wrong_firm: "Wrong firm / wrong number",
    vendor_sales: "Vendor / spam",
    existing_client: "Existing client or case status",
    admin: "Outside practice area / admin",
  };

  var MONTH_NAMES = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
  ];

  function monthTitleFromKey(key) {
    if (!key || key === "unknown") return "Unknown calendar month";
    var p = String(key).split("-");
    if (p.length !== 2) return key;
    var mi = parseInt(p[1], 10) - 1;
    if (mi < 0 || mi > 11) return key;
    return MONTH_NAMES[mi] + " " + p[0];
  }

  function dayKeyFromRecord(c) {
    var ts = c && (c.call_start_time || c.start_time || c.created_at);
    if (!validTime(ts)) return null;
    var d = new Date(Date.parse(ts));
    return (
      d.getFullYear() +
      "-" +
      String(d.getMonth() + 1).padStart(2, "0") +
      "-" +
      String(d.getDate()).padStart(2, "0")
    );
  }

  function callsPerDay(calls) {
    if (!calls || !calls.length) return null;
    var seen = {};
    var dayCount = 0;
    for (var i = 0; i < calls.length; i++) {
      var k = dayKeyFromRecord(calls[i]);
      if (!k) continue;
      if (!seen[k]) {
        seen[k] = 1;
        dayCount++;
      }
    }
    if (!dayCount) return null;
    return calls.length / dayCount;
  }

  function detectPartialMonths(summary, latestKey, latestCalls) {
    var out = [];
    if (!summary || !latestKey || !Number.isFinite(latestCalls) || latestCalls <= 0) return out;
    var keys = Object.keys(summary);
    for (var i = 0; i < keys.length; i++) {
      var k = keys[i];
      if (k === "unknown" || k === latestKey) continue;
      var total = num(summary[k] && summary[k].total_calls);
      if (!Number.isFinite(total)) continue;
      if (total < 0.5 * latestCalls) out.push(k);
    }
    out.sort();
    return out;
  }

  function el(id) {
    return document.getElementById(id);
  }

  function num(v) {
    var n = Number(v);
    return Number.isFinite(n) ? n : NaN;
  }

  function normalizeRecords(raw) {
    if (!raw) return [];
    if (!Array.isArray(raw)) {
      if (Array.isArray(raw.calls)) return raw.calls;
      if (Array.isArray(raw.records)) return raw.records;
      return [];
    }
    return raw;
  }

  function fetchJson(url) {
    return fetch(url, { cache: "no-store" })
      .then(function (r) {
        return r.ok ? r.json() : Promise.reject(new Error(url));
      })
      .catch(function () {
        return null;
      });
  }

  function validTime(v) {
    if (v == null) return false;
    var s = String(v).trim();
    if (!s) return false;
    return !Number.isNaN(Date.parse(s));
  }

  /** Share of records with at least one usable CallRail-style timestamp */
  function timestampShare(calls) {
    if (!calls.length) return 0;
    var ok = 0;
    for (var i = 0; i < calls.length; i++) {
      var c = calls[i];
      if (validTime(c.call_start_time) || validTime(c.start_time) || validTime(c.created_at)) ok++;
    }
    return ok / calls.length;
  }

  function formatNum(n) {
    if (!Number.isFinite(n)) return "Not available";
    if (Math.abs(n - Math.round(n)) < 1e-6) return String(Math.round(n));
    return n.toFixed(1);
  }

  function formatPct1(x) {
    if (!Number.isFinite(x)) return "Not available";
    return x.toFixed(1) + "%";
  }

  function pctVsPrior(prev, curr) {
    if (prev == null || curr == null) return "Not available";
    if (prev === 0) return curr === 0 ? "0%" : "Not available";
    var x = ((curr - prev) / prev) * 100;
    return (x >= 0 ? "+" : "") + x.toFixed(1) + "%";
  }

  function qualifiedShare(raw, qual) {
    if (raw == null || qual == null || raw === 0) return null;
    return (qual / raw) * 100;
  }

  /** Enriched JSON present (may be built without Lead Docket CSV). */
  function enrichedReady(enriched) {
    return enriched && enriched.summary && enriched.months && enriched.by_source;
  }

  /** Lead Docket CSV was loaded for matching — show LD columns instead of transcript-only estimates. */
  function leadDocketLinked(enriched) {
    return enrichedReady(enriched) && enriched.inputs && enriched.inputs.leads_csv_path != null;
  }

  function monthStats(enriched, key) {
    if (!enriched || !enriched.months || !key) return null;
    return enriched.months[key] || null;
  }

  function sourceMapFromEnrichedRows(arr) {
    var m = {};
    if (!arr || !arr.length) return m;
    for (var i = 0; i < arr.length; i++) {
      var r = arr[i];
      if (r && r.name) m[r.name] = r;
    }
    return m;
  }

  function unionSourceNames(mapA, mapB) {
    var o = {};
    var k;
    for (k in mapA) o[k] = 1;
    for (k in mapB) o[k] = 1;
    return Object.keys(o);
  }

  function formatPctOrNA(x) {
    if (!Number.isFinite(x)) return "Not available";
    return formatPct1(x);
  }

  function signedConvPct(calls, signed) {
    if (!Number.isFinite(calls) || calls <= 0 || !Number.isFinite(signed)) return null;
    return (signed / calls) * 100;
  }

  function tr() {
    return document.createElement("tr");
  }

  function td(text, className) {
    var x = document.createElement("td");
    x.textContent = text;
    if (className) x.className = className;
    return x;
  }

  function aggregateCallTypes(calls) {
    var o = {};
    for (var i = 0; i < CALL_TYPE_ORDER.length; i++) o[CALL_TYPE_ORDER[i]] = 0;
    for (var j = 0; j < calls.length; j++) {
      var t = calls[j].call_type || "unclear";
      if (o[t] === undefined) o[t] = 0;
      o[t]++;
    }
    return o;
  }

  function priorSourceRow(sourcesPrior, name) {
    if (!sourcesPrior || !sourcesPrior.length) return { raw: null, qualified: null };
    for (var i = 0; i < sourcesPrior.length; i++) {
      if (sourcesPrior[i].name === name) {
        return {
          raw: num(sourcesPrior[i].raw),
          qualified: num(sourcesPrior[i].qualified),
        };
      }
    }
    return { raw: null, qualified: null };
  }

  function renderPerfPrior(tbody, truth) {
    tbody.innerHTML = "";
    var prior = truth && truth.callrail && truth.callrail.prior;
    if (!prior || prior.raw_leads == null) {
      var r = tr();
      var c = document.createElement("td");
      c.colSpan = 2;
      c.textContent =
        "Not yet reconciled with CallRail. Prior-month cells stay empty on purpose until you add callrail.prior (and optional sources_prior) to callrail_report_truth.json.";
      r.appendChild(c);
      tbody.appendChild(r);
      return;
    }
    var raw = num(prior.raw_leads);
    var q = num(prior.qualified_leads);
    var sh = qualifiedShare(raw, q);
    var rows = [
      ["Total calls", formatNum(raw)],
      ["Potential Lead Signals", formatNum(q)],
      ["Lead Signal Rate", sh == null ? "Not available" : formatPct1(sh)],
    ];
    for (var i = 0; i < rows.length; i++) {
      var row = tr();
      row.appendChild(td(rows[i][0]));
      row.appendChild(td(rows[i][1], "num"));
      tbody.appendChild(row);
    }
  }

  function renderPerfCurrent(tbody, truth) {
    tbody.innerHTML = "";
    var cur = truth && truth.callrail && truth.callrail.current;
    if (!cur || cur.raw_leads == null) {
      tbody.appendChild(tr()).appendChild(td("Add callrail.current to callrail_report_truth.json."));
      return;
    }
    var raw = num(cur.raw_leads);
    var q = num(cur.qualified_leads);
    var sh = qualifiedShare(raw, q);
    var rows = [
      ["Total calls", formatNum(raw)],
      ["Potential Lead Signals", formatNum(q)],
      ["Lead Signal Rate", sh == null ? "Not available" : formatPct1(sh)],
    ];
    for (var i = 0; i < rows.length; i++) {
      var row = tr();
      row.appendChild(td(rows[i][0]));
      row.appendChild(td(rows[i][1], "num"));
      tbody.appendChild(row);
    }
  }

  function renderMomTable(tbody, truth) {
    tbody.innerHTML = "";
    var prior = truth && truth.callrail && truth.callrail.prior;
    var cur = truth && truth.callrail && truth.callrail.current;
    if (!cur || cur.raw_leads == null) {
      tbody.appendChild(tr()).appendChild(td("Current CallRail totals missing from truth file."));
      return;
    }
    if (!prior || prior.raw_leads == null) {
      var r = tr();
      var c = document.createElement("td");
      c.colSpan = 8;
      c.textContent =
        "Prior month is not on file. Month-over-month numbers are not shown so nothing is invented. Add callrail.prior to callrail_report_truth.json when you have reconciled CallRail totals.";
      r.appendChild(c);
      tbody.appendChild(r);
      return;
    }
    var pr = num(prior.raw_leads);
    var pq = num(prior.qualified_leads);
    var cr = num(cur.raw_leads);
    var cq = num(cur.qualified_leads);
    var ps = qualifiedShare(pr, pq);
    var cs = qualifiedShare(cr, cq);

    function addRow(label, pVal, cVal, isRate) {
      var row = tr();
      row.appendChild(td(label));
      row.appendChild(td(isRate ? formatPct1(pVal) : formatNum(pVal), "num"));
      row.appendChild(td(isRate ? formatPct1(cVal) : formatNum(cVal), "num"));
      row.appendChild(td(isRate ? formatNum(cVal - pVal) + " pts" : formatNum(cVal - pVal), "num"));
      row.appendChild(td(pctVsPrior(pVal, cVal), "num"));
      tbody.appendChild(row);
    }

    addRow("Total calls", pr, cr, false);
    addRow("Potential Lead Signals", pq, cq, false);
    if (ps != null && cs != null) {
      var row = tr();
      row.appendChild(td("Lead Signal Rate"));
      row.appendChild(td(formatPct1(ps), "num"));
      row.appendChild(td(formatPct1(cs), "num"));
      row.appendChild(td(formatNum(cs - ps) + " pts", "num"));
      row.appendChild(td(pctVsPrior(ps, cs), "num"));
      tbody.appendChild(row);
    }
  }

  function heardForSourceName(truth, name) {
    if (!truth || !truth.sources) return "Not available";
    for (var i = 0; i < truth.sources.length; i++) {
      if (truth.sources[i].name === name) {
        var h = truth.sources[i].heard;
        return h === undefined || h === "" ? "Not available" : h;
      }
    }
    return "Not available";
  }

  function renderSourceTable(tbody, truth) {
    tbody.innerHTML = "";
    var sources = (truth && truth.sources) || [];
    var sourcesPrior = (truth && truth.sources_prior) || null;
    var prior = truth && truth.callrail && truth.callrail.prior;
    var hasPrior = prior && prior.raw_leads != null;

    for (var i = 0; i < sources.length; i++) {
      var s = sources[i];
      var name = s.name || "Not available";
      var pr = priorSourceRow(sourcesPrior, name);
      var cr = num(s.raw);
      var cq = num(s.qualified);
      var pRaw = pr.raw;
      var pQual = pr.qualified;
      var share = qualifiedShare(cr, cq);
      var row = tr();
      row.appendChild(td(name));
      row.appendChild(td(hasPrior && pRaw != null && Number.isFinite(pRaw) ? formatNum(pRaw) : "Not available", "num"));
      row.appendChild(td(hasPrior && pQual != null && Number.isFinite(pQual) ? formatNum(pQual) : "Not available", "num"));
      row.appendChild(td(formatNum(cr), "num"));
      row.appendChild(td(formatNum(cq), "num"));
      row.appendChild(td(share == null ? "Not available" : formatPct1(share), "num"));
      row.appendChild(td(pRaw != null && Number.isFinite(pRaw) ? pctVsPrior(pRaw, cr) : "Not available", "num"));
      row.appendChild(td(s.heard ? s.heard : "Not available"));
      tbody.appendChild(row);
    }
  }

  function priorSourceCount(build, name) {
    if (!build || !build.sources_prior_month) return { calls: null, q: null };
    for (var i = 0; i < build.sources_prior_month.length; i++) {
      if (build.sources_prior_month[i].name === name)
        return { calls: build.sources_prior_month[i].calls, q: build.sources_prior_month[i].qualified_estimate };
    }
    return { calls: null, q: null };
  }

  function renderSourceTableFromBuild(tbody, build, truth, enriched) {
    var table = tbody.closest("table");
    var theadRow = table && table.querySelector("thead tr");
    var curKey = build && build.current_month_key;
    var priKey = build && build.prior_month_reconciled && build.prior_month_key;
    var hasPriSrc = priKey && leadDocketLinked(enriched) && Array.isArray(enriched.by_source[priKey]);
    var hasCurSrc = curKey && leadDocketLinked(enriched) && Array.isArray(enriched.by_source[curKey]);
    var useLd = leadDocketLinked(enriched) && curKey && priKey && hasPriSrc && hasCurSrc;

    if (theadRow) {
      if (useLd) {
        theadRow.innerHTML =
          "<th>Source</th>" +
          '<th class="num" id="th-src-prior-raw">Prior calls</th>' +
          '<th class="num" id="th-src-prior-leads">Prior leads</th>' +
          '<th class="num" id="th-src-prior-signed">Prior signed</th>' +
          '<th class="num" id="th-src-prior-conv">Prior conv %</th>' +
          '<th class="num" id="th-src-cur-raw">Current calls</th>' +
          '<th class="num" id="th-src-cur-leads">Current leads</th>' +
          '<th class="num" id="th-src-cur-signed">Current signed</th>' +
          '<th class="num" id="th-src-cur-conv">Current conv %</th>' +
          '<th class="num" id="th-src-pct">% vs prior calls</th>' +
          "<th>What we heard in calls</th>";
        var prLab = monthTitleFromKey(priKey);
        var crLab = monthTitleFromKey(curKey);
        if (el("th-src-prior-raw")) el("th-src-prior-raw").textContent = prLab + " calls";
        if (el("th-src-prior-leads")) el("th-src-prior-leads").textContent = prLab + " leads";
        if (el("th-src-prior-signed")) el("th-src-prior-signed").textContent = prLab + " signed";
        if (el("th-src-prior-conv")) el("th-src-prior-conv").textContent = prLab + " conv %";
        if (el("th-src-cur-raw")) el("th-src-cur-raw").textContent = crLab + " calls";
        if (el("th-src-cur-leads")) el("th-src-cur-leads").textContent = crLab + " leads";
        if (el("th-src-cur-signed")) el("th-src-cur-signed").textContent = crLab + " signed";
        if (el("th-src-cur-conv")) el("th-src-cur-conv").textContent = crLab + " conv %";
      } else {
        theadRow.innerHTML =
          "<th>Source</th>" +
          '<th class="num" id="th-src-prior-raw">Prior raw</th>' +
          '<th class="num" id="th-src-prior-qual">Prior potential lead signals</th>' +
          '<th class="num" id="th-src-cur-raw">Current raw</th>' +
          '<th class="num" id="th-src-cur-qual">Current potential lead signals</th>' +
          '<th class="num">Lead signal rate</th>' +
          '<th class="num" id="th-src-pct">% vs prior raw</th>' +
          "<th>What we heard in calls</th>";
        var prLabTb = build && build.prior_month_reconciled ? "Prior month" : "Prior (n/a)";
        if (el("th-src-prior-raw")) el("th-src-prior-raw").textContent = prLabTb + " raw";
        if (el("th-src-prior-qual")) el("th-src-prior-qual").textContent = prLabTb + " potential lead signals";
        if (el("th-src-cur-raw")) el("th-src-cur-raw").textContent = "Current raw";
        if (el("th-src-cur-qual")) el("th-src-cur-qual").textContent = "Current potential lead signals";
        if (el("th-src-pct")) el("th-src-pct").textContent = "% vs prior raw";
      }
    }

    tbody.innerHTML = "";
    if (useLd) {
      var mapPrior = sourceMapFromEnrichedRows(enriched.by_source[priKey]);
      var mapCur = sourceMapFromEnrichedRows(enriched.by_source[curKey]);
      var names = unionSourceNames(mapPrior, mapCur);
      names.sort(function (a, b) {
        return num((mapCur[b] || {}).calls) - num((mapCur[a] || {}).calls);
      });
      for (var li = 0; li < names.length; li++) {
        var nm = names[li];
        var p = mapPrior[nm] || { calls: 0, leads: 0, signed: 0 };
        var c = mapCur[nm] || { calls: 0, leads: 0, signed: 0 };
        var pc = num(p.calls);
        var cc = num(c.calls);
        var pConv = signedConvPct(pc, num(p.signed));
        var cConv = signedConvPct(cc, num(c.signed));
        var row = tr();
        row.appendChild(td(nm));
        row.appendChild(td(formatNum(pc), "num"));
        row.appendChild(td(formatNum(num(p.leads)), "num"));
        row.appendChild(td(formatNum(num(p.signed)), "num"));
        row.appendChild(td(formatPctOrNA(pConv), "num"));
        row.appendChild(td(formatNum(cc), "num"));
        row.appendChild(td(formatNum(num(c.leads)), "num"));
        row.appendChild(td(formatNum(num(c.signed)), "num"));
        row.appendChild(td(formatPctOrNA(cConv), "num"));
        row.appendChild(td(Number.isFinite(pc) ? pctVsPrior(pc, cc) : "Not available", "num"));
        row.appendChild(td(heardForSourceName(truth, nm)));
        tbody.appendChild(row);
      }
      if (!names.length) {
        var er = tr();
        var ec = td("No Lead Docket source rows.");
        ec.colSpan = 11;
        er.appendChild(ec);
        tbody.appendChild(er);
      }
      return;
    }

    var list = (build && build.sources_current_month) || [];
    var hasPrior = build && build.prior_month_reconciled;
    for (var i = 0; i < list.length; i++) {
      var s = list[i];
      var name = s.name || "Not available";
      var pr = priorSourceCount(build, name);
      var cr = num(s.calls);
      var cq = num(s.qualified_estimate);
      var share = qualifiedShare(cr, cq);
      var row2 = tr();
      row2.appendChild(td(name));
      row2.appendChild(td(hasPrior && pr.calls != null ? formatNum(pr.calls) : "Not available", "num"));
      row2.appendChild(td(hasPrior && pr.q != null ? formatNum(pr.q) : "Not available", "num"));
      row2.appendChild(td(formatNum(cr), "num"));
      row2.appendChild(td(formatNum(cq), "num"));
      row2.appendChild(td(share == null ? "Not available" : formatPct1(share), "num"));
      row2.appendChild(td(hasPrior && pr.calls != null ? pctVsPrior(pr.calls, cr) : "Not available", "num"));
      row2.appendChild(td(heardForSourceName(truth, name)));
      tbody.appendChild(row2);
    }
    if (!list.length) tbody.appendChild(tr()).appendChild(td("No source rows in callrail_build_summary.json."));
  }


  function sourceRowsFromBuild(build) {
    return build && Array.isArray(build.sources_current_month) ? build.sources_current_month : [];
  }

  function bestQualitySource(rows) {
    var best = null;
    var bestPct = -1;
    for (var i = 0; i < rows.length; i++) {
      var c = num(rows[i].calls);
      var q = num(rows[i].qualified_estimate);
      if (!Number.isFinite(c) || c <= 0 || !Number.isFinite(q)) continue;
      var pct = (q / c) * 100;
      if (pct > bestPct) {
        bestPct = pct;
        best = rows[i];
      }
    }
    return { row: best, pct: bestPct };
  }

  function topVolumeSource(rows) {
    var best = null;
    for (var i = 0; i < rows.length; i++) {
      if (!best || num(rows[i].calls) > num(best.calls)) best = rows[i];
    }
    return best;
  }

  function enrichedCallsForSnapshot(enriched, build) {
    if (!enriched || !Array.isArray(enriched.calls)) return [];
    var all = enriched.calls;
    if (build && build.current_month_key) {
      var filtered = all.filter(function (c) {
        return c.month_key === build.current_month_key;
      });
      if (filtered.length) return filtered;
    }
    return all;
  }

  function renderExecutiveSnapshot(enriched, build) {
    var box = el("executive-summary");
    if (!box) return;
    var subset = enrichedCallsForSnapshot(enriched, build);
    if (!subset.length) {
      box.innerHTML =
        '<p><strong>Total Calls:</strong> Not available</p>' +
        '<p><strong>Matched Leads:</strong> Not available</p>' +
        '<p><strong>Signed Cases:</strong> Not available</p>' +
        '<p><strong>High Confidence Matches:</strong> Not available</p>' +
        '<p style="color:#57534e;font-size:0.9rem;margin-top:0.5rem;">Run <code>npm run enrich:leads</code> to build <code>data/callrail_enriched.json</code>.</p>';
      return;
    }
    var total = subset.length;
    var matched = 0;
    var signed = 0;
    var strictN = 0;
    for (var i = 0; i < subset.length; i++) {
      var c = subset[i];
      if (c.lead_created) matched++;
      if (c.signed_flag) signed++;
      if (c.match_type === "strict") strictN++;
    }
    box.innerHTML =
      '<p><strong>Total Calls:</strong> ' + formatNum(total) + '</p>' +
      '<p><strong>Matched Leads:</strong> ' + formatNum(matched) + '</p>' +
      '<p><strong>Signed Cases:</strong> ' + formatNum(signed) + '</p>' +
      '<p><strong>High Confidence Matches:</strong> ' + formatNum(strictN) + '</p>';
  }

  function renderTopSourcesSignedCases(enriched, build) {
    var tbody = el("tbody-top-sources-signed");
    if (!tbody) return;
    tbody.innerHTML = "";
    var subset = enrichedCallsForSnapshot(enriched, build);
    if (!subset.length) {
      var r0 = tr();
      var c0 = document.createElement("td");
      c0.colSpan = 5;
      c0.textContent =
        "No rows in data/callrail_enriched.json for this view. Run npm run enrich:leads.";
      r0.appendChild(c0);
      tbody.appendChild(r0);
      return;
    }
    var map = {};
    for (var i = 0; i < subset.length; i++) {
      var row = subset[i];
      var src = row.source && String(row.source).trim() ? String(row.source).trim() : "(no source)";
      if (!map[src]) map[src] = { source: src, calls: 0, strict: 0, hr: 0, signed: 0 };
      var o = map[src];
      o.calls++;
      if (row.match_type === "strict") o.strict++;
      if (row.match_type === "24hr") o.hr++;
      if (row.signed_flag) o.signed++;
    }
    var list = Object.keys(map).map(function (k) {
      return map[k];
    });
    list.sort(function (a, b) {
      if (b.signed !== a.signed) return b.signed - a.signed;
      return b.calls - a.calls;
    });
    for (var j = 0; j < list.length; j++) {
      var x = list[j];
      var trr = tr();
      trr.appendChild(td(x.source));
      trr.appendChild(td(formatNum(x.calls), "num"));
      trr.appendChild(td(formatNum(x.strict), "num"));
      trr.appendChild(td(formatNum(x.hr), "num"));
      trr.appendChild(td(formatNum(x.signed), "num"));
      tbody.appendChild(trr);
    }
  }

  function renderSourceQualityTable(tbody, build, enriched) {
    if (!tbody) return;
    var table = tbody.closest("table");
    var theadRow = table && table.querySelector("thead tr");
    var curKey = build && build.current_month_key;
    var priKey = build && build.prior_month_reconciled && build.prior_month_key;
    var hasPriSrc = priKey && leadDocketLinked(enriched) && Array.isArray(enriched.by_source[priKey]);
    var hasCurSrc = curKey && leadDocketLinked(enriched) && Array.isArray(enriched.by_source[curKey]);
    var useLd = leadDocketLinked(enriched) && curKey && priKey && hasPriSrc && hasCurSrc;

    if (theadRow) {
      if (useLd) {
        theadRow.innerHTML =
          "<th>Source</th>" +
          '<th class="num" id="th-lq-p-calls">Prior calls</th>' +
          '<th class="num">Prior leads</th>' +
          '<th class="num">Prior signed</th>' +
          '<th class="num">Prior conv %</th>' +
          '<th class="num" id="th-lq-c-calls">Current calls</th>' +
          '<th class="num">Current leads</th>' +
          '<th class="num">Current signed</th>' +
          '<th class="num">Current conv %</th>' +
          '<th class="num">Δ Conv %</th>' +
          '<th class="num">Δ Call volume</th>';
        var thPC = el("th-lq-p-calls");
        var thCC = el("th-lq-c-calls");
        if (thPC) thPC.textContent = monthTitleFromKey(priKey) + " calls";
        if (thCC) thCC.textContent = monthTitleFromKey(curKey) + " calls";
      } else {
        theadRow.innerHTML =
          "<th>Source</th>" +
          '<th class="num" id="th-lead-src-feb">Prior calls</th>' +
          '<th class="num">Prior lead signal rate</th>' +
          '<th class="num" id="th-lead-src-mar">Current calls</th>' +
          '<th class="num">Current potential lead signals</th>' +
          '<th class="num">Current lead signal rate</th>' +
          '<th class="num">Change in lead signal rate</th>' +
          '<th class="num">Change in call volume</th>';
        if (build && build.prior_month_reconciled && build.prior_month_key) {
          var tf = el("th-lead-src-feb");
          var tm = el("th-lead-src-mar");
          if (tf) tf.textContent = monthTitleFromKey(build.prior_month_key) + " calls";
          if (tm) tm.textContent = monthTitleFromKey(build.current_month_key) + " calls";
        }
      }
    }

    tbody.innerHTML = "";
    if (useLd) {
      var mapPrior = sourceMapFromEnrichedRows(enriched.by_source[priKey]);
      var mapCur = sourceMapFromEnrichedRows(enriched.by_source[curKey]);
      var names = unionSourceNames(mapPrior, mapCur);
      names.sort(function (a, b) {
        return num((mapCur[b] || {}).calls) - num((mapCur[a] || {}).calls);
      });
      var bestSignedRate = -1;
      var bestName = null;
      for (var bi = 0; bi < names.length; bi++) {
        var bn = names[bi];
        var crw = mapCur[bn] || { calls: 0, leads: 0, signed: 0 };
        var sc = signedConvPct(num(crw.calls), num(crw.signed));
        if (sc != null && num(crw.calls) >= 3 && sc > bestSignedRate) {
          bestSignedRate = sc;
          bestName = bn;
        }
      }
      for (var i = 0; i < names.length; i++) {
        var name = names[i];
        var p = mapPrior[name] || { calls: 0, leads: 0, signed: 0 };
        var c = mapCur[name] || { calls: 0, leads: 0, signed: 0 };
        var pc = num(p.calls);
        var pLeads = num(p.leads);
        var pSig = num(p.signed);
        var cc = num(c.calls);
        var cLeads = num(c.leads);
        var cSig = num(c.signed);
        var pConv = signedConvPct(pc, pSig);
        var cConv = signedConvPct(cc, cSig);
        var row = tr();
        row.appendChild(td(name || "Not available"));
        row.appendChild(td(formatNum(pc), "num"));
        row.appendChild(td(formatNum(pLeads), "num"));
        row.appendChild(td(formatNum(pSig), "num"));
        row.appendChild(td(formatPctOrNA(pConv), "num"));
        row.appendChild(td(formatNum(cc), "num"));
        row.appendChild(td(formatNum(cLeads), "num"));
        row.appendChild(td(formatNum(cSig), "num"));
        var convTd = td(formatPctOrNA(cConv), "num");
        if (bestName && name === bestName) {
          convTd.style.color = "#0f766e";
          convTd.style.fontWeight = "700";
        }
        row.appendChild(convTd);
        var deltaConv = pConv != null && cConv != null ? cConv - pConv : null;
        row.appendChild(
          td(deltaConv == null ? "Not available" : (deltaConv > 0 ? "+" : "") + formatNum(deltaConv) + " pts", "num")
        );
        var volD = Number.isFinite(pc) && Number.isFinite(cc) ? cc - pc : null;
        row.appendChild(
          td(volD == null ? "Not available" : (volD > 0 ? "+" : "") + formatNum(volD), "num")
        );
        tbody.appendChild(row);
      }
      if (!names.length) {
        var er = tr();
        var ec = td("No Lead Docket source rows for these months.");
        ec.colSpan = 11;
        er.appendChild(ec);
        tbody.appendChild(er);
      }
      return;
    }

    var rows = sourceRowsFromBuild(build).slice();
    var priorMap = {};
    if (build && Array.isArray(build.sources_prior_month)) {
      for (var pi = 0; pi < build.sources_prior_month.length; pi++) {
        priorMap[build.sources_prior_month[pi].name] = build.sources_prior_month[pi];
      }
    }
    rows.sort(function (a, b) {
      return num(b.qualified_estimate) - num(a.qualified_estimate);
    });
    var best = bestQualitySource(rows);

    for (var j = 0; j < rows.length; j++) {
      var src = rows[j];
      var prior = priorMap[src.name] || {};
      var calls = num(src.calls);
      var qual = num(src.qualified_estimate);
      var pct = qualifiedShare(calls, qual);
      var row2 = tr();
      row2.appendChild(td(src.name || "Not available"));
      row2.appendChild(td(formatNum(num(prior.calls)), "num"));
      var priorRate = qualifiedShare(num(prior.calls), num(prior.qualified_estimate));
      row2.appendChild(td(priorRate == null ? "Not available" : formatPct1(priorRate), "num"));
      row2.appendChild(td(formatNum(calls), "num"));
      row2.appendChild(td(formatNum(qual), "num"));
      var pctTd2 = td(pct == null ? "Not available" : formatPct1(pct), "num");
      if (best.row && src.name === best.row.name && Number.isFinite(best.pct)) {
        pctTd2.style.color = "#0f766e";
        pctTd2.style.fontWeight = "700";
      }
      row2.appendChild(pctTd2);
      var deltaRaw = priorRate == null || pct == null ? null : pct - priorRate;
      var deltaRate = deltaRaw == null ? "Not available" : (deltaRaw > 0 ? "+" : "") + formatNum(deltaRaw) + " pts";
      row2.appendChild(td(deltaRate, "num"));
      var volDelta = prior.calls == null ? null : calls - num(prior.calls);
      row2.appendChild(td(volDelta == null ? "Not available" : (volDelta > 0 ? "+" : "") + formatNum(volDelta), "num"));
      tbody.appendChild(row2);
    }
    if (!rows.length) {
      var emptyRow = tr();
      var emptyCell = td("Not available");
      emptyCell.colSpan = 8;
      emptyRow.appendChild(emptyCell);
      tbody.appendChild(emptyRow);
    }
  }

  function drawHourlyChart(canvas, calls) {
    if (!canvas || !calls || !calls.length) return;
    var hourly = [];
    for (var h = 0; h < 24; h++) hourly.push({ calls: 0, qual: 0 });

    for (var i = 0; i < calls.length; i++) {
      var hr = num(calls[i].hour_of_day);
      if (!Number.isFinite(hr) || hr < 0 || hr > 23) continue;
      var idx = Math.floor(hr);
      hourly[idx].calls += 1;
      if (calls[i].call_type === "true_pi_opportunity" || num(calls[i].opportunity_score) >= 58) {
        hourly[idx].qual += 1;
      }
    }

    var dpr = window.devicePixelRatio || 1;
    var rect = canvas.getBoundingClientRect();
    var w = rect.width || 900;
    var h = rect.height || 220;
    canvas.width = w * dpr;
    canvas.height = h * dpr;
    var ctx = canvas.getContext("2d");
    ctx.scale(dpr, dpr);
    ctx.clearRect(0, 0, w, h);
    ctx.fillStyle = "#fafaf9";
    ctx.fillRect(0, 0, w, h);

    var maxCalls = 1;
    for (var k = 0; k < 24; k++) if (hourly[k].calls > maxCalls) maxCalls = hourly[k].calls;

    var left = 28;
    var right = 12;
    var top = 12;
    var bottom = 28;
    var plotW = w - left - right;
    var plotH = h - top - bottom;
    var barW = plotW / 24;

    ctx.strokeStyle = "#e7e5e4";
    for (var g = 0; g <= 3; g++) {
      var gy = top + (g / 3) * plotH;
      ctx.beginPath();
      ctx.moveTo(left, gy);
      ctx.lineTo(w - right, gy);
      ctx.stroke();
    }

    for (var j = 0; j < 24; j++) {
      var v = hourly[j].calls;
      var bh = (v / maxCalls) * plotH;
      var x = left + j * barW + 1;
      var y = top + (plotH - bh);
      ctx.fillStyle = "#0f766e";
      ctx.fillRect(x, y, Math.max(1, barW - 2), bh);
    }

    ctx.beginPath();
    ctx.strokeStyle = "#b45309";
    ctx.lineWidth = 2;
    var moved = false;
    for (var m = 0; m < 24; m++) {
      var c = hourly[m].calls;
      var q = hourly[m].qual;
      if (!c) continue;
      var pct = (q / c) * 100;
      var px = left + m * barW + barW / 2;
      var py = top + (plotH - (pct / 100) * plotH);
      if (!moved) {
        ctx.moveTo(px, py);
        moved = true;
      } else {
        ctx.lineTo(px, py);
      }
    }
    if (moved) ctx.stroke();

    function hourLabel(hh) {
      var ap = hh >= 12 ? "PM" : "AM";
      var h12 = hh % 12;
      if (h12 === 0) h12 = 12;
      return h12 + " " + ap;
    }
    ctx.fillStyle = "#57534e";
    ctx.font = "10px Segoe UI, sans-serif";
    for (var t = 0; t < 24; t += 3) {
      ctx.fillText(hourLabel(t), left + t * barW - 8, h - 6);
    }
  }

  function hourlySummaryText(calls) {
    if (!calls || !calls.length) return "No timestamped call pattern is available yet.";
    var hourly = [];
    var leadHourly = [];
    for (var h = 0; h < 24; h++) {
      hourly.push(0);
      leadHourly.push(0);
    }
    for (var i = 0; i < calls.length; i++) {
      var hr = num(calls[i].hour_of_day);
      if (Number.isFinite(hr) && hr >= 0 && hr < 24) {
        var idx = Math.floor(hr);
        hourly[idx]++;
        if (calls[i].call_type === "true_pi_opportunity") leadHourly[idx]++;
      }
    }
    var bestHr = 0;
    var bestVal = 0;
    for (var j = 0; j < 24; j++) {
      if (hourly[j] > bestVal) {
        bestVal = hourly[j];
        bestHr = j;
      }
    }
    if (bestVal === 0) return "Call timing appears evenly spread or missing in available records.";
    var eveCalls = 0;
    var eveLeads = 0;
    var dayCalls = 0;
    var dayLeads = 0;
    for (var k = 0; k < 24; k++) {
      if (k >= 17 && k <= 21) {
        eveCalls += hourly[k];
        eveLeads += leadHourly[k];
      } else if (k >= 9 && k <= 16) {
        dayCalls += hourly[k];
        dayLeads += leadHourly[k];
      }
    }
    var eveRate = qualifiedShare(eveCalls, eveLeads);
    var dayRate = qualifiedShare(dayCalls, dayLeads);
    var ap = bestHr >= 12 ? "PM" : "AM";
    var h12 = bestHr % 12;
    if (h12 === 0) h12 = 12;
    var base = "Call volume appears strongest around " + h12 + " " + ap + " based on analyzed records.";
    if (eveRate != null && dayRate != null && eveRate > dayRate + 2) return base + " Lead signal rates also appear somewhat stronger in the evening.";
    if (eveRate != null && dayRate != null && eveRate > dayRate) return base + " Evening lead signal rates look slightly higher, but the gap is modest.";
    return base;
  }

  function drawHourHeatmap(canvas, calls) {
    if (!canvas || !calls.length) return;
    var bins = [];
    for (var i = 0; i < 24; i++) bins.push(0);
    for (var j = 0; j < calls.length; j++) {
      var hr = num(calls[j].hour_of_day);
      if (Number.isFinite(hr) && hr >= 0 && hr < 24) bins[Math.floor(hr)]++;
    }
    var maxV = Math.max.apply(null, bins.concat([1]));
    var dpr = window.devicePixelRatio || 1;
    var rect = canvas.getBoundingClientRect();
    var w = rect.width || 400;
    var h = rect.height || 200;
    canvas.width = w * dpr;
    canvas.height = h * dpr;
    var ctx = canvas.getContext("2d");
    ctx.scale(dpr, dpr);
    ctx.clearRect(0, 0, w, h);
    var cw = (w - 30) / 24;
    for (var k = 0; k < 24; k++) {
      var alpha = bins[k] / maxV;
      ctx.fillStyle = "rgba(15,118,110," + (0.15 + alpha * 0.85) + ")";
      ctx.fillRect(15 + k * cw, 40, cw - 1, 85);
      if (k % 3 === 0) {
        ctx.fillStyle = "#57534e";
        ctx.font = "10px Segoe UI, sans-serif";
        ctx.fillText(String(k), 15 + k * cw, 140);
      }
    }
    ctx.fillStyle = "#1c1917";
    ctx.font = "11px Segoe UI, sans-serif";
    ctx.fillText("Darker cells = more calls by hour", 15, 20);
  }

  function drawIntentBars(canvas, calls) {
    if (!canvas || !calls.length) return;
    var keys = ["true_pi_opportunity", "unclear", "wrong_firm", "vendor_sales", "existing_client", "property_damage_only", "outside_practice_area", "admin"];
    var labels = {
      true_pi_opportunity: "Possible injury",
      unclear: "Unclear",
      wrong_firm: "Wrong firm",
      vendor_sales: "Vendor/spam",
      existing_client: "Existing client",
      property_damage_only: "Property only",
      outside_practice_area: "Outside area",
      admin: "Admin"
    };
    var counts = {};
    for (var i = 0; i < keys.length; i++) counts[keys[i]] = 0;
    for (var j = 0; j < calls.length; j++) {
      var t = calls[j].call_type;
      if (counts[t] != null) counts[t]++;
    }
    var rows = keys.map(function (k) { return { k: k, n: counts[k] || 0 }; }).sort(function (a, b) { return b.n - a.n; }).slice(0, 6);
    var dpr = window.devicePixelRatio || 1;
    var rect = canvas.getBoundingClientRect();
    var w = rect.width || 400;
    var h = rect.height || 200;
    canvas.width = w * dpr;
    canvas.height = h * dpr;
    var ctx = canvas.getContext("2d");
    ctx.scale(dpr, dpr);
    ctx.clearRect(0, 0, w, h);
    var maxV = Math.max.apply(null, rows.map(function (r) { return r.n; }).concat([1]));
    for (var r = 0; r < rows.length; r++) {
      var y = 20 + r * 27;
      var bw = (rows[r].n / maxV) * (w - 170);
      ctx.fillStyle = "#0f766e";
      ctx.fillRect(130, y, bw, 16);
      ctx.fillStyle = "#1c1917";
      ctx.font = "10px Segoe UI, sans-serif";
      ctx.fillText(labels[rows[r].k] || rows[r].k, 8, y + 12);
      ctx.fillText(String(rows[r].n), 136 + bw, y + 12);
    }
  }

  function drawReferenceCharts(callsPrior, callsCurrent) {
    var prior = Array.isArray(callsPrior) ? callsPrior : [];
    var current = Array.isArray(callsCurrent) ? callsCurrent : [];
    if (!prior.length && !current.length) return;
    var SOURCE_ORDER = ["google_ads", "gmb", "direct", "chat", "referral", "unknown"];
    function sourceCounts(calls, mk) {
      var row = { month: mk };
      for (var i = 0; i < SOURCE_ORDER.length; i++) row[SOURCE_ORDER[i]] = 0;
      for (var j = 0; j < calls.length; j++) {
        var b = String(calls[j].source_bucket || "unknown");
        if (row[b] == null) b = "unknown";
        row[b]++;
      }
      return row;
    }
    drawGroupedSource(el("ref-source-mix"), [sourceCounts(prior, "2026-02"), sourceCounts(current, "2026-03")], SOURCE_ORDER);
    function bucketCounts(calls, mk) {
      var out = { month: mk, possible_injury: 0, unclear: 0, wrong_firm: 0, vendor_spam: 0, existing_client: 0, other_admin: 0 };
      for (var i = 0; i < calls.length; i++) {
        var t = calls[i].call_type;
        if (t === "true_pi_opportunity") out.possible_injury++;
        else if (t === "unclear") out.unclear++;
        else if (t === "wrong_firm") out.wrong_firm++;
        else if (t === "vendor_sales") out.vendor_spam++;
        else if (t === "existing_client") out.existing_client++;
        else out.other_admin++;
      }
      return out;
    }
    drawGroupedSource(el("ref-buckets-mom"), [bucketCounts(prior, "2026-02"), bucketCounts(current, "2026-03")], ["possible_injury", "unclear", "wrong_firm", "vendor_spam", "existing_client", "other_admin"]);
    drawIntentBars(el("ref-intent-mix"), current.length ? current : prior);
    drawDurationHist(el("ref-duration-buckets"), current.length ? current : prior);
    drawHourHeatmap(el("ref-heatmap"), current.length ? current : prior);
    var bySource = {};
    for (var s = 0; s < current.length; s++) {
      var src = String(current[s].source_bucket || "unknown");
      if (!bySource[src]) bySource[src] = { month: src, possible_injury: 0, unclear: 0, other: 0 };
      if (current[s].call_type === "true_pi_opportunity") bySource[src].possible_injury++;
      else if (current[s].call_type === "unclear") bySource[src].unclear++;
      else bySource[src].other++;
    }
    var srcRows = Object.keys(bySource).slice(0, 6).map(function (k) { return bySource[k]; });
    drawGroupedSource(el("ref-source-bucket"), srcRows, ["possible_injury", "unclear", "other"]);
    drawFunnel(el("ref-funnel"), current.length ? current : prior);
    drawDurationHist(el("ref-duration-hist"), current.length ? current : prior);
  }

  function renderPerfCurrentFromBuild(tbody, build, enriched) {
    tbody.innerHTML = "";
    var raw = num(build.calls_this_month);
    var bucketRowLabel =
      build.current_month_key === "unknown"
        ? "Calls in unknown-date bucket (no parseable timestamp)"
        : "Calls this month (scored, calendar bucket)";
    var curKey = build.current_month_key;
    var ld = leadDocketLinked(enriched) && curKey ? monthStats(enriched, curKey) : null;
    var rows;
    if (ld && ld.leads != null) {
      var ldLeads = num(ld.leads);
      var ldSigned = num(ld.signed);
      var lr = qualifiedShare(raw, ldLeads);
      var sr = qualifiedShare(raw, ldSigned);
      rows = [
        [bucketRowLabel, formatNum(raw)],
        ["Total leads (Lead Docket)", formatNum(ldLeads)],
        ["Total signed", formatNum(ldSigned)],
        ["Lead match rate", lr == null ? "Not available" : formatPct1(lr)],
        ["Signed rate (of calls)", sr == null ? "Not available" : formatPct1(sr)],
      ];
    } else {
      var q = num(build.qualified_leads_estimate);
      var sh = qualifiedShare(raw, q);
      rows = [
        [bucketRowLabel, formatNum(raw)],
        ["Potential Lead Signals", formatNum(q)],
        ["Lead Signal Rate", sh == null ? "Not available" : formatPct1(sh)],
      ];
    }
    if (build.unknown_month_call_count > 0)
      rows.push(["Calls with no parseable date (bucket: unknown)", formatNum(build.unknown_month_call_count)]);
    for (var i = 0; i < rows.length; i++) {
      var row = tr();
      row.appendChild(td(rows[i][0]));
      row.appendChild(td(rows[i][1], "num"));
      tbody.appendChild(row);
    }
  }

  function renderPerfPriorFromBuild(tbody, build, enriched) {
    tbody.innerHTML = "";
    if (!build.prior_month_reconciled) {
      var r = tr();
      var c = document.createElement("td");
      c.colSpan = 2;
      c.textContent =
        "Prior month not yet reconciled. Need at least two calendar months with real call timestamps in the ingest window.";
      r.appendChild(c);
      tbody.appendChild(r);
      return;
    }
    var raw = num(build.calls_prior_month);
    var priKey = build.prior_month_key;
    var ld = leadDocketLinked(enriched) && priKey ? monthStats(enriched, priKey) : null;
    var rows;
    if (ld && ld.leads != null) {
      var ldLeads = num(ld.leads);
      var ldSigned = num(ld.signed);
      var lr = qualifiedShare(raw, ldLeads);
      var sr = qualifiedShare(raw, ldSigned);
      rows = [
        ["Calls in prior month (scored)", formatNum(raw)],
        ["Total leads (Lead Docket)", formatNum(ldLeads)],
        ["Total signed", formatNum(ldSigned)],
        ["Lead match rate", lr == null ? "Not available" : formatPct1(lr)],
        ["Signed rate (of calls)", sr == null ? "Not available" : formatPct1(sr)],
      ];
    } else {
      var q = num(build.qualified_prior_estimate);
      var sh = qualifiedShare(raw, q);
      rows = [
        ["Calls in prior month (scored)", formatNum(raw)],
        ["Potential Lead Signals", formatNum(q)],
        ["Lead Signal Rate", sh == null ? "Not available" : formatPct1(sh)],
      ];
    }
    for (var j = 0; j < rows.length; j++) {
      var row = tr();
      row.appendChild(td(rows[j][0]));
      row.appendChild(td(rows[j][1], "num"));
      tbody.appendChild(row);
    }
  }

  function renderMomFromBuild(tbody, build, enriched) {
    tbody.innerHTML = "";
    if (!build.prior_month_reconciled) {
      var r = tr();
      var c = document.createElement("td");
      c.colSpan = 5;
      c.textContent =
        "Prior month not yet reconciled. Month-over-month is hidden until two calendar months exist in scored data.";
      r.appendChild(c);
      tbody.appendChild(r);
      return;
    }
    var pr = num(build.calls_prior_month);
    var cr = num(build.calls_this_month);
    var priKey = build.prior_month_key;
    var curKey = build.current_month_key;
    var ldPri = leadDocketLinked(enriched) && priKey ? monthStats(enriched, priKey) : null;
    var ldCur = leadDocketLinked(enriched) && curKey ? monthStats(enriched, curKey) : null;
    var useLd = ldPri && ldCur && ldPri.leads != null && ldCur.leads != null;

    function addRow(label, pVal, cVal, isRate) {
      var row = tr();
      row.appendChild(td(label));
      row.appendChild(td(isRate ? formatPct1(pVal) : formatNum(pVal), "num"));
      row.appendChild(td(isRate ? formatPct1(cVal) : formatNum(cVal), "num"));
      row.appendChild(td(isRate ? formatNum(cVal - pVal) + " pts" : formatNum(cVal - pVal), "num"));
      row.appendChild(td(pctVsPrior(pVal, cVal), "num"));
      tbody.appendChild(row);
    }

    if (useLd) {
      var pqL = num(ldPri.leads);
      var cqL = num(ldCur.leads);
      var pqS = num(ldPri.signed);
      var cqS = num(ldCur.signed);
      var psL = qualifiedShare(pr, pqL);
      var csL = qualifiedShare(cr, cqL);
      var psS = qualifiedShare(pr, pqS);
      var csS = qualifiedShare(cr, cqS);
      addRow("Calls", pr, cr, false);
      addRow("Total leads (Lead Docket)", pqL, cqL, false);
      addRow("Total signed", pqS, cqS, false);
      if (psL != null && csL != null) {
        var rowL = tr();
        rowL.appendChild(td("Lead match rate"));
        rowL.appendChild(td(formatPct1(psL), "num"));
        rowL.appendChild(td(formatPct1(csL), "num"));
        rowL.appendChild(td(formatNum(csL - psL) + " pts", "num"));
        rowL.appendChild(td(pctVsPrior(psL, csL), "num"));
        tbody.appendChild(rowL);
      }
      if (psS != null && csS != null) {
        var rowS = tr();
        rowS.appendChild(td("Signed rate (of calls)"));
        rowS.appendChild(td(formatPct1(psS), "num"));
        rowS.appendChild(td(formatPct1(csS), "num"));
        rowS.appendChild(td(formatNum(csS - psS) + " pts", "num"));
        rowS.appendChild(td(pctVsPrior(psS, csS), "num"));
        tbody.appendChild(rowS);
      }
      return;
    }

    var pq = num(build.qualified_prior_estimate);
    var cq = num(build.qualified_leads_estimate);
    var ps = qualifiedShare(pr, pq);
    var cs = qualifiedShare(cr, cq);

    addRow("Calls", pr, cr, false);
    addRow("Potential Lead Signals", pq, cq, false);
    if (ps != null && cs != null) {
      var row2 = tr();
      row2.appendChild(td("Lead Signal Rate"));
      row2.appendChild(td(formatPct1(ps), "num"));
      row2.appendChild(td(formatPct1(cs), "num"));
      row2.appendChild(td(formatNum(cs - ps) + " pts", "num"));
      row2.appendChild(td(pctVsPrior(ps, cs), "num"));
      tbody.appendChild(row2);
    }
  }

  function buildChangedFromPipeline(build, enriched) {
    var out = [];
    if (!build.prior_month_reconciled) {
      out.push("<strong>Prior month is not reconciled</strong> → hold major spend shifts until baseline is confirmed.");
      out.push("<strong>Missing date quality can skew trends</strong> → fix timestamp capture in call routing.");
      out.push("<strong>Source quality is still usable</strong> → move budget toward channels with better outcomes.");
      return out.slice(0, 5);
    }
    var pr = num(build.calls_prior_month);
    var cr = num(build.calls_this_month);
    var priKey = build.prior_month_key;
    var curKey = build.current_month_key;
    var ldPri = leadDocketLinked(enriched) && priKey ? monthStats(enriched, priKey) : null;
    var ldCur = leadDocketLinked(enriched) && curKey ? monthStats(enriched, curKey) : null;
    var useLd = ldPri && ldCur;

    if (cr > pr) out.push("<strong>Call volume increased</strong> → ensure intake staffing is strong at peak hours.");
    else if (cr < pr) out.push("<strong>Call volume declined</strong> → audit campaign delivery and daypart pacing now.");

    if (useLd) {
      var pqL = num(ldPri.leads);
      var cqL = num(ldCur.leads);
      var pqS = num(ldPri.signed);
      var cqS = num(ldCur.signed);
      if (cqL > pqL) out.push("<strong>Lead Docket matches increased</strong> → directional link from calls to CRM improved or intake volume rose.");
      else if (cqL < pqL) out.push("<strong>Lead Docket matches declined</strong> → check export coverage and phone capture on calls.");
      if (cqS > pqS) out.push("<strong>Signed cases increased</strong> → review which channels contributed.");
      else if (cqS < pqS) out.push("<strong>Signed cases declined</strong> → compare to case pipeline outside this call window.");
      var psL = qualifiedShare(pr, pqL);
      var csL = qualifiedShare(cr, cqL);
      if (psL != null && csL != null) {
        if (csL > psL + 0.5) out.push("<strong>Lead match rate improved</strong> → calls and CRM timestamps may be aligning better.");
        else if (csL < psL - 0.5) out.push("<strong>Lead match rate slipped</strong> → verify customer phone on ingest and CSV date fields.");
      }
    } else {
      var pq = num(build.qualified_prior_estimate);
      var cq = num(build.qualified_leads_estimate);
      if (cq > pq) out.push("<strong>Potential Lead Signals increased</strong> → review which sources contributed most.");
      else if (cq < pq) out.push("<strong>Potential Lead Signals declined</strong> → review targeting and intake handling.");
      var ps = qualifiedShare(pr, pq);
      var cs = qualifiedShare(cr, cq);
      if (ps != null && cs != null) {
        if (cs > ps + 0.5) out.push("<strong>Qualification rate improved</strong> → increase spend where quality remains above average.");
        else if (cs < ps - 0.5) out.push("<strong>Qualification rate slipped</strong> → retrain intake and trim weak traffic windows.");
      }
    }
    if (!out.length) out.push("<strong>Performance is stable</strong> → keep spend steady and watch source quality changes.");
    return out.slice(0, 5);
  }

  function renderQualityTable(tbody, typesPrior, typesCurr, hasPriorSubset) {
    tbody.innerHTML = "";
    for (var i = 0; i < CALL_TYPE_ORDER.length; i++) {
      var k = CALL_TYPE_ORDER[i];
      var p = hasPriorSubset ? typesPrior[k] || 0 : null;
      var c = typesCurr[k] || 0;
      var row = tr();
      row.appendChild(td(CALL_TYPE_LABEL[k] || k));
      row.appendChild(td(hasPriorSubset ? formatNum(p) : "Not available", "num"));
      row.appendChild(td(formatNum(c), "num"));
      row.appendChild(td(hasPriorSubset ? pctVsPrior(p, c) : "Not available", "num"));
      tbody.appendChild(row);
    }
  }

  function buildChangedBullets(truth) {
    var out = [];
    var prior = truth && truth.callrail && truth.callrail.prior;
    var cur = truth && truth.callrail && truth.callrail.current;
    if (!prior || prior.raw_leads == null) {
      out.push("Prior month is not in the truth file, so we are not stating what changed versus last month.");
      return out;
    }
    if (!cur) return out;
    var pr = num(prior.raw_leads);
    var cr = num(cur.raw_leads);
    var pq = num(prior.qualified_leads);
    var cq = num(cur.qualified_leads);
    if (cr > pr) out.push("Raw lead volume is up versus the prior period on file.");
    else if (cr < pr) out.push("Raw lead volume is down versus the prior period on file.");
    if (cq > pq) out.push("Potential Lead Signals are up versus the prior period on file.");
    else if (cq < pq) out.push("Potential Lead Signals are down versus the prior period on file.");
    var ps = qualifiedShare(pr, pq);
    var cs = qualifiedShare(cr, cq);
    if (ps != null && cs != null) {
      if (cs > ps + 0.5) out.push("Lead signal rate improved versus the prior period.");
      else if (cs < ps - 0.5) out.push("Lead signal rate slipped versus the prior period.");
    }
    return out.slice(0, 4);
  }

  function countKeywords(calls, re) {
    var n = 0;
    for (var i = 0; i < calls.length; i++) {
      var t = String(calls[i].transcription || calls[i].transcript_excerpt || "");
      if (re.test(t)) n++;
    }
    return n;
  }

  function buildPatternBullets(calls) {
    var out = [];
    if (!calls.length) return out;
    if (countKeywords(calls, /\b(truck|semi|tractor|commercial vehicle|delivery van|fleet)\b/i) >= 5)
      out.push("<strong>Some recent calls mention commercial vehicles</strong> → review whether intake scripts need clearer routing.");
    if (
      countKeywords(
        calls,
        /\b(unhappy with (my |)(lawyer|attorney)|fire my (lawyer|attorney)|second opinion|switch (to |)(a |)(lawyer|attorney))\b/i
      ) >= 3
    )
      out.push("<strong>Some callers appear to be comparing firms or prior experiences</strong> → keep callback and consultation response times tight.");
    var shortN = 0;
    for (var j = 0; j < calls.length; j++) {
      var d = num(calls[j].duration);
      if (Number.isFinite(d) && d < 90) shortN++;
    }
    if (shortN / calls.length > 0.25)
      out.push("<strong>Short-call volume is elevated</strong> → review ad terms and first-minute intake flow.");
    if (out.length < 3)
      out.push("<strong>Lead signal rates vary by source</strong> → prioritize review before shifting budget.");
    return out.slice(0, 5);
  }

  function missedPlainLanguage(calls) {
    if (!calls.length) return "No transcript file loaded for review.";
    var n = 0;
    for (var i = 0; i < calls.length; i++) {
      if (calls[i].hidden_opportunity_flag) n++;
    }
    if (n === 0)
      return (
        "In " +
        calls.length +
        " analyzed calls with transcripts, no clear near-miss pattern appeared. Spot-checking is still recommended."
      );
    return (
      "About " +
      n +
      " of " +
      calls.length +
      " analyzed calls with transcripts included injury-related language but were not clearly categorized as viable matters. These may be worth spot-checking for intake handling or missed opportunities."
    );
  }

  function buildSourceTakeaway(build, enriched) {
    var curKey = build && build.current_month_key;
    var priKey = build && build.prior_month_reconciled && build.prior_month_key;
    var hasCurSrc = curKey && leadDocketLinked(enriched) && Array.isArray(enriched.by_source[curKey]);
    var hasPriSrc = priKey && leadDocketLinked(enriched) && Array.isArray(enriched.by_source[priKey]);
    if (hasCurSrc && hasPriSrc) {
      var mapCur = sourceMapFromEnrichedRows(enriched.by_source[curKey]);
      var names = Object.keys(mapCur);
      names.sort(function (a, b) {
        return num((mapCur[b] || {}).calls) - num((mapCur[a] || {}).calls);
      });
      var topVol = names[0] ? { name: names[0], row: mapCur[names[0]] } : null;
      var bestS = null;
      var bestPct = -1;
      for (var i = 0; i < names.length; i++) {
        var rw = mapCur[names[i]];
        var sc = signedConvPct(num(rw.calls), num(rw.signed));
        if (sc != null && num(rw.calls) >= 3 && sc > bestPct) {
          bestPct = sc;
          bestS = { name: names[i], row: rw };
        }
      }
      if (topVol && bestS && topVol.name !== bestS.name) {
        return (
          "Key takeaway (Lead Docket): " +
          topVol.name +
          " drives the most calls. " +
          bestS.name +
          " shows a higher signed-per-call rate in this directional match (lower volume)."
        );
      }
      if (topVol) {
        return (
          "Key takeaway (Lead Docket): " +
          topVol.name +
          " leads call volume this month. Match rates are approximate (phone + 10-minute window)."
        );
      }
    }

    var list = sourceRowsFromBuild(build);
    if (!list.length) return "Source detail is not available yet.";
    var top = topVolumeSource(list);
    var best = bestQualitySource(list);
    if (top && best.row && top.name !== best.row.name) {
      return (
        "Key takeaway: " +
        top.name +
        " drives the most call volume. " +
        best.row.name +
        " appears to show stronger lead signal rate, but at lower volume."
      );
    }
    if (top) {
      return (
        "Key takeaway: " +
        top.name +
        " is the main volume driver this month. Validate lead quality with periodic call reviews."
      );
    }
    return "Key takeaway: source-level differences are limited in this data window.";
  }

  function drawLineChart(canvas, points, color) {
    if (!canvas || !points || !points.length) return;
    var dpr = window.devicePixelRatio || 1;
    var rect = canvas.getBoundingClientRect();
    var w = rect.width || 400;
    var h = rect.height || 200;
    canvas.width = w * dpr;
    canvas.height = h * dpr;
    var ctx = canvas.getContext("2d");
    ctx.scale(dpr, dpr);
    ctx.clearRect(0, 0, w, h);
    ctx.fillStyle = "#fafaf9";
    ctx.fillRect(0, 0, w, h);
    var vals = points.map(function (p) {
      return p.y;
    });
    var minV = Math.min.apply(null, vals.concat([0]));
    var maxV = Math.max.apply(null, vals.concat([1]));
    var pad = (maxV - minV) * 0.12 || 1;
    minV -= pad;
    maxV += pad;
    var n = points.length;
    function xAt(i) {
      return n <= 1 ? w / 2 : (i / (n - 1)) * (w - 24) + 12;
    }
    function yAt(v) {
      return h - 14 - ((v - minV) / (maxV - minV || 1)) * (h - 28);
    }
    ctx.strokeStyle = "#e7e5e4";
    for (var g = 0; g <= 3; g++) {
      var gy = 8 + (g / 3) * (h - 22);
      ctx.beginPath();
      ctx.moveTo(12, gy);
      ctx.lineTo(w - 12, gy);
      ctx.stroke();
    }
    ctx.strokeStyle = color || "#0f766e";
    ctx.lineWidth = 2;
    ctx.beginPath();
    for (var i = 0; i < n; i++) {
      var x = xAt(i);
      var y = yAt(points[i].y);
      if (i === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    }
    ctx.stroke();
    ctx.fillStyle = "#57534e";
    ctx.font = "10px Segoe UI, sans-serif";
    for (var j = 0; j < n; j++) {
      ctx.fillText(points[j].label || "", xAt(j) - 10, h - 2);
    }
  }

  function drawGroupedSource(canvas, rows, keys) {
    if (!canvas || !rows || !rows.length) return;
    var dpr = window.devicePixelRatio || 1;
    var rect = canvas.getBoundingClientRect();
    var w = rect.width || 400;
    var h = rect.height || 200;
    canvas.width = w * dpr;
    canvas.height = h * dpr;
    var ctx = canvas.getContext("2d");
    ctx.scale(dpr, dpr);
    ctx.clearRect(0, 0, w, h);
    ctx.fillStyle = "#fafaf9";
    ctx.fillRect(0, 0, w, h);
    var months = rows.length;
    var bw = (w - 36) / (keys.length * months + months);
    var maxC = 1;
    for (var r = 0; r < rows.length; r++) {
      for (var b = 0; b < keys.length; b++) {
        maxC = Math.max(maxC, rows[r][keys[b]] || 0);
      }
    }
    var colors = ["#0f766e", "#14b8a6", "#5eead4", "#99f6e4", "#ccfbf1", "#78716c"];
    for (var mi = 0; mi < months; mi++) {
      var row = rows[mi];
      for (var bi = 0; bi < keys.length; bi++) {
        var cnt = row[keys[bi]] || 0;
        var bh = maxC ? (cnt / maxC) * (h - 32) : 0;
        var x = 16 + mi * (bw * (keys.length + 1)) + bi * bw;
        var y = h - 18 - bh;
        ctx.fillStyle = colors[bi % colors.length];
        ctx.fillRect(x, y, bw * 0.85, bh);
      }
      ctx.fillStyle = "#57534e";
      ctx.font = "9px Segoe UI, sans-serif";
      ctx.fillText(row.month.slice(5), 16 + mi * (bw * (keys.length + 1)), h - 4);
    }
  }

  function drawFunnel(canvas, calls) {
    if (!canvas || !calls.length) return;
    var dpr = window.devicePixelRatio || 1;
    var rect = canvas.getBoundingClientRect();
    var w = rect.width || 600;
    var h = rect.height || 200;
    canvas.width = w * dpr;
    canvas.height = h * dpr;
    var ctx = canvas.getContext("2d");
    ctx.scale(dpr, dpr);
    ctx.clearRect(0, 0, w, h);
    ctx.fillStyle = "#fafaf9";
    ctx.fillRect(0, 0, w, h);

    var total = calls.length;
    var wasteTypes = { vendor_sales: 1, wrong_firm: 1, property_damage_only: 1, admin: 1, existing_client: 1 };
    var waste = 0;
    var pi = 0;
    for (var i = 0; i < calls.length; i++) {
      var t = calls[i].call_type;
      if (t === "true_pi_opportunity") pi++;
      if (wasteTypes[t]) waste++;
    }
    var other = total - pi - waste;
    if (other < 0) other = 0;

    var stages = [
      { label: "All calls in transcript subset", n: total, color: "#0f766e" },
      { label: "Possible injury matter", n: pi, color: "#14b8a6" },
      { label: "Outside practice / admin / spam", n: waste, color: "#fdba74" },
      { label: "Other or unclear", n: other, color: "#a8a29e" },
    ];

    var barH = 36;
    var gap = 14;
    var maxW = w - 320;
    for (var s = 0; s < stages.length; s++) {
      var st = stages[s];
      var frac = total ? st.n / total : 0;
      var bw = Math.max(8, maxW * frac);
      var y = 16 + s * (barH + gap);
      ctx.fillStyle = st.color;
      ctx.fillRect(190, y, bw, barH);
      ctx.fillStyle = "#1c1917";
      ctx.font = "12px Segoe UI, sans-serif";
      ctx.fillText(st.label, 8, y + barH / 2 + 4);
      ctx.fillStyle = "#57534e";
      ctx.font = "11px Segoe UI, sans-serif";
      ctx.textAlign = "right";
      ctx.fillText(formatNum(st.n) + " (" + formatPct1(frac * 100) + ")", w - 8, y + barH / 2 + 4);
      ctx.textAlign = "left";
    }
  }

  function drawDurationHist(canvas, calls) {
    if (!canvas || !calls.length) return;
    var labels = ["Under 1 min", "1–5 min", "5–10 min", "10–20 min", "20+ min"];
    var counts = [0, 0, 0, 0, 0];
    for (var i = 0; i < calls.length; i++) {
      var d = num(calls[i].duration);
      if (!Number.isFinite(d)) continue;
      if (d < 60) counts[0]++;
      else if (d < 300) counts[1]++;
      else if (d < 600) counts[2]++;
      else if (d < 1200) counts[3]++;
      else counts[4]++;
    }

    var dpr = window.devicePixelRatio || 1;
    var rect = canvas.getBoundingClientRect();
    var w = rect.width || 600;
    var h = rect.height || 200;
    canvas.width = w * dpr;
    canvas.height = h * dpr;
    var ctx = canvas.getContext("2d");
    ctx.scale(dpr, dpr);
    ctx.clearRect(0, 0, w, h);
    ctx.fillStyle = "#fafaf9";
    ctx.fillRect(0, 0, w, h);

    var maxC = Math.max.apply(null, counts.concat([1]));
    var bw = (w - 48) / counts.length - 8;
    for (var j = 0; j < counts.length; j++) {
      var bh = (counts[j] / maxC) * (h - 48);
      var x = 24 + j * (bw + 8);
      var y = h - 28 - bh;
      ctx.fillStyle = "#0f766e";
      ctx.fillRect(x, y, bw, bh);
      ctx.fillStyle = "#57534e";
      ctx.font = "11px Segoe UI, sans-serif";
      ctx.fillText(labels[j], x - 2, h - 10);
      ctx.fillText(formatNum(counts[j]), x + bw / 4, y - 4);
    }
  }

  function renderList(ul, items) {
    ul.innerHTML = "";
    for (var i = 0; i < items.length; i++) {
      var li = document.createElement("li");
      if (String(items[i]).indexOf("<") >= 0) li.innerHTML = items[i];
      else li.textContent = items[i];
      ul.appendChild(li);
    }
  }

  function setTrendsVisible(showCharts) {
    var ph = el("trends-placeholder-wrap");
    var ch = el("trends-charts-wrap");
    if (ph && ch) {
      ph.style.display = showCharts ? "none" : "grid";
      ch.style.display = showCharts ? "grid" : "none";
    }
  }

  function monthFileUrl(key) {
    if (!key || key === "unknown") return null;
    var p = key.split("-");
    if (p.length !== 2) return null;
    return "callrail_scored_calls_" + p[0] + "_" + p[1] + ".json";
  }

  function verifiedMatchTypeRank(t) {
    if (t === "strict") return 0;
    if (t === "24hr") return 1;
    if (t === "phone_only") return 2;
    return 99;
  }

  /** Top 25 signed_flag rows from callrail_enriched.json for spot-checking attribution. */
  function renderVerifiedSignedExamples(enriched) {
    var tbody = el("tbody-verified-signed-examples");
    if (!tbody) return;
    tbody.innerHTML = "";
    var list = [];
    if (enriched && enriched.calls && enriched.calls.length) {
      for (var vi = 0; vi < enriched.calls.length; vi++) {
        var rec = enriched.calls[vi];
        if (rec && rec.signed_flag === true) list.push(rec);
      }
    }
    list.sort(function (a, b) {
      var ra = verifiedMatchTypeRank(a.match_type);
      var rb = verifiedMatchTypeRank(b.match_type);
      if (ra !== rb) return ra - rb;
      var da = num(a.match_delta_minutes);
      var db = num(b.match_delta_minutes);
      if (!Number.isFinite(da)) da = 0;
      if (!Number.isFinite(db)) db = 0;
      return da - db;
    });
    var top = list.slice(0, 25);
    if (!top.length) {
      var emptyR = tr();
      var emptyC = document.createElement("td");
      emptyC.colSpan = 8;
      emptyC.textContent =
        "No signed_flag examples in data/callrail_enriched.json. Run npm run enrich:leads after Lead Docket export.";
      emptyR.appendChild(emptyC);
      tbody.appendChild(emptyR);
      return;
    }
    for (var vj = 0; vj < top.length; vj++) {
      var x = top[vj];
      var row = tr();
      var startTs = x.call_start_time || x.start_time || "";
      row.appendChild(td(startTs ? String(startTs) : "Not available"));
      row.appendChild(td(x.customer_phone_number != null ? String(x.customer_phone_number) : "Not available"));
      row.appendChild(td(x.source != null ? String(x.source) : "Not available"));
      row.appendChild(
        td(Number.isFinite(num(x.duration)) ? formatNum(num(x.duration)) : "Not available", "num")
      );
      row.appendChild(td(x.match_type != null ? String(x.match_type) : "Not available"));
      row.appendChild(
        td(Number.isFinite(num(x.match_delta_minutes)) ? formatNum(num(x.match_delta_minutes)) : "Not available", "num")
      );
      row.appendChild(td(x.lead_status != null ? String(x.lead_status) : "Not available"));
      row.appendChild(td(x.case_type != null ? String(x.case_type) : "Not available"));
      tbody.appendChild(row);
    }
  }

  function run() {
    Promise.all([
      fetchJson("callrail_build_summary.json"),
      fetchJson("callrail_report_truth.json"),
      fetchJson("callrail_scored_calls_latest.json"),
      fetchJson("callrail_scored_calls.json"),
      fetchJson("callrail_month_summary.json"),
      fetchJson("data/callrail_enriched.json"),
    ]).then(function (results) {
      var build = results[0];
      var truth = results[1];
      var latest = results[2];
      var fallback = results[3];
      var summaryRaw = results[4];
      var enriched = results[5];

      var calls = normalizeRecords(latest);
      var allCalls = normalizeRecords(fallback);
      if (!calls.length) calls = allCalls;
      if (!allCalls.length) allCalls = calls;

      var useBuild =
        build &&
        build.current_month_key &&
        build.error !== "empty_input" &&
        !build.error;

      var priorFetch = Promise.resolve(null);
      if (useBuild && build.prior_month_reconciled && build.prior_month_key) {
        var pu = monthFileUrl(build.prior_month_key);
        if (pu) priorFetch = fetchJson(pu);
      }

      priorFetch.then(function (priorCallsRaw) {
        renderVerifiedSignedExamples(enriched);
        var snapBuild = useBuild ? build : null;
        renderExecutiveSnapshot(enriched, snapBuild);
        renderTopSourcesSignedCases(enriched, snapBuild);

        var callsPriorScored = normalizeRecords(priorCallsRaw);
        var typesPrior = aggregateCallTypes(callsPriorScored);
        var hasPriorTypes = useBuild && build.prior_month_reconciled && callsPriorScored.length > 0;

        var tsShare =
          useBuild && build.timestamp_coverage_pct != null
            ? build.timestamp_coverage_pct / 100
            : timestampShare(calls);
        function msgOnly(tbodyId, colspan, msg) {
          var tb = el(tbodyId);
          tb.innerHTML = "";
          var r = tr();
          var c = document.createElement("td");
          c.colSpan = colspan;
          c.textContent = msg;
          r.appendChild(c);
          tb.appendChild(r);
        }

        if (useBuild) {
          var summaryObj =
            summaryRaw && typeof summaryRaw === "object" && !Array.isArray(summaryRaw)
              ? summaryRaw
              : {};
          var partialMonths = detectPartialMonths(
            summaryObj,
            build.current_month_key,
            num(build.calls_this_month)
          );
          var priorIsPartial =
            build.prior_month_reconciled &&
            Number.isFinite(num(build.calls_prior_month)) &&
            Number.isFinite(num(build.calls_this_month)) &&
            num(build.calls_prior_month) < 0.5 * num(build.calls_this_month);

          el("data-window-range").textContent =
            build.date_range_display || build.preferred_timestamp_min_iso || "Not available";
          var transcriptCount = 0;
          for (var ac = 0; ac < allCalls.length; ac++) {
            if (String(allCalls[ac].transcription || "").trim()) transcriptCount++;
          }
          el("data-window-raw").textContent = formatNum(num(allCalls.length));
          var curKeyDw = build.current_month_key;
          var ldDw = leadDocketLinked(enriched) && curKeyDw ? monthStats(enriched, curKeyDw) : null;
          if (ldDw && ldDw.leads != null) {
            el("data-window-qualified").textContent =
              formatNum(num(ldDw.leads)) + " leads (LD) / " + formatNum(num(ldDw.signed)) + " signed";
          } else {
            el("data-window-qualified").textContent = formatNum(num(build.qualified_leads_estimate));
          }
          el("data-window-transcript-n").textContent = String(transcriptCount);
          el("data-window-provenance").textContent =
            "Total calls: reporting window. Matched leads and signed cases use data/callrail_enriched.json (phone + time proximity to Lead Docket). Otherwise counts use the intel build.";
          var partialWarn = el("partial-month-warning");
          if (partialWarn) {
            if (partialMonths.length) {
              partialWarn.textContent =
                monthTitleFromKey(partialMonths[0]) +
                " data is partial and should not be used for full month comparisons.";
            } else {
              partialWarn.textContent = "";
            }
          }

          el("heading-prior-performance").textContent = "February 2026 performance";
          el("heading-current-performance").textContent = "March 2026 performance";

          el("th-mom-prior").textContent = "February" + (priorIsPartial ? " (partial)" : "");
          el("th-mom-current").textContent = "March";
          var momPartialNote = el("mom-partial-note");
          if (momPartialNote) {
            momPartialNote.style.display = priorIsPartial ? "block" : "none";
          }

          el("th-type-prior").textContent = build.prior_month_reconciled
            ? monthTitleFromKey(build.prior_month_key)
            : "Prior (n/a)";
          el("th-type-current").textContent = monthTitleFromKey(build.current_month_key);

          renderPerfPriorFromBuild(el("tbody-perf-prior"), build, enriched);
          renderPerfCurrentFromBuild(el("tbody-perf-current"), build, enriched);
          var marchNote = el("march-performance-note");
          if (marchNote) {
            var cCalls = num(build.calls_this_month);
            var pCalls = num(build.calls_prior_month);
            var priK = build.prior_month_key;
            var curK = build.current_month_key;
            var ldPM = leadDocketLinked(enriched) && priK ? monthStats(enriched, priK) : null;
            var ldCM = leadDocketLinked(enriched) && curK ? monthStats(enriched, curK) : null;
            if (ldPM && ldCM) {
              var cLR = qualifiedShare(cCalls, num(ldCM.leads));
              var pLR = qualifiedShare(pCalls, num(ldPM.leads));
              if (cCalls > pCalls && cLR != null && pLR != null && cLR >= pLR) {
                marchNote.textContent =
                  "Current period has higher call volume and a higher Lead Docket match rate than the prior period (directional: phone + 10-minute window).";
              } else {
                marchNote.textContent =
                  "Call volume and Lead Docket matches are directional only; re-run npm run enrich:leads after updating the CSV.";
              }
            } else {
              var cRate = qualifiedShare(cCalls, num(build.qualified_leads_estimate));
              var pRate = qualifiedShare(pCalls, num(build.qualified_prior_estimate));
              if (cCalls > pCalls && cRate != null && pRate != null && cRate >= pRate) {
                marchNote.textContent = "March produced both higher call volume and a higher transcript-based lead signal rate than February. That suggests volume increased without obvious deterioration in lead quality.";
              } else {
                marchNote.textContent = "March call volume increased versus February. Lead signal trends are directional and should be interpreted with caution.";
              }
            }
          }
          renderMomFromBuild(el("tbody-mom"), build, enriched);
          renderSourceQualityTable(el("tbody-source-quality"), build, enriched);
          renderSourceTableFromBuild(el("tbody-source"), build, truth, enriched);
          renderList(el("insight-source-takeaway"), [buildSourceTakeaway(build, enriched)]);

          var typesCurr = aggregateCallTypes(calls);
          renderQualityTable(el("tbody-quality"), typesPrior, typesCurr, hasPriorTypes);

          renderList(el("insight-changed"), buildChangedFromPipeline(build, enriched));
          renderList(el("insight-patterns"), buildPatternBullets(calls));
          el("insight-missed").textContent = missedPlainLanguage(calls);

          drawFunnel(el("chart-funnel"), calls);
          drawDurationHist(el("chart-duration-hist"), calls);
          drawHourlyChart(el("chart-hourly"), calls);
          var hourlySummary = el("chart-hourly-summary");
          if (hourlySummary) hourlySummary.textContent = hourlySummaryText(calls);
          return;
        }

        if (!truth || !truth.callrail) {
          el("data-window-range").textContent =
            "Run ingest and npm run intel:build, or add callrail_report_truth.json.";
          el("data-window-raw").textContent = "Not available";
          el("data-window-qualified").textContent = "Not available";
          el("data-window-transcript-n").textContent = String(calls.length);
          el("data-window-provenance").textContent =
            "Missing callrail_build_summary.json and truth file.";
          var pmwMissing = el("partial-month-warning");
          if (pmwMissing) pmwMissing.textContent = "";
          var momNoteMissing = el("mom-partial-note");
          if (momNoteMissing) momNoteMissing.style.display = "none";
          el("heading-prior-performance").textContent = "Prior month not yet reconciled";
          el("heading-current-performance").textContent = "Current month performance";
          msgOnly("tbody-perf-prior", 2, "Run pipeline to generate callrail_build_summary.json.");
          msgOnly("tbody-perf-current", 2, "Run pipeline to generate callrail_build_summary.json.");
          msgOnly("tbody-mom", 5, "No build summary yet.");
          msgOnly("tbody-source-quality", 8, "No source quality rows yet.");
          msgOnly("tbody-source", 8, "No build summary yet.");
          renderList(el("insight-source-takeaway"), ["Key takeaway: source-level comparison will appear after the refresh completes."]);
          renderList(el("insight-changed"), ["<strong>Data refresh is pending</strong> → run ingest and intel build before making spend decisions."]);
          renderList(el("insight-patterns"), buildPatternBullets(calls));
          el("insight-missed").textContent = missedPlainLanguage(calls);
          renderQualityTable(el("tbody-quality"), {}, aggregateCallTypes(calls), false);
          drawFunnel(el("chart-funnel"), calls);
          drawDurationHist(el("chart-duration-hist"), calls);
          drawHourlyChart(el("chart-hourly"), calls);
          var hsMissing = el("chart-hourly-summary");
          if (hsMissing) hsMissing.textContent = hourlySummaryText(calls);
          return;
        }

        var cp = truth.current_period || {};
        el("data-window-range").textContent = cp.range_display || cp.label || "Not available";
        var cur = truth.callrail.current || {};
        el("data-window-raw").textContent = formatNum(num(allCalls.length));
        el("data-window-qualified").textContent = formatNum(num(cur.qualified_leads));
        var transcriptCountTruth = 0;
        for (var ati = 0; ati < allCalls.length; ati++) {
          if (String(allCalls[ati].transcription || "").trim()) transcriptCountTruth++;
        }
        el("data-window-transcript-n").textContent = String(transcriptCountTruth);
        el("data-window-provenance").textContent =
          "Total calls and transcript-based lead estimates: callrail_report_truth.json. " +
          "Matched leads / signed cases: data/callrail_enriched.json when present. " +
          "Timestamp coverage on scored rows: " +
          formatPct1(tsShare * 100) +
          "%.";
        var pmwTruth = el("partial-month-warning");
        if (pmwTruth) pmwTruth.textContent = "";
        var momNoteTruth = el("mom-partial-note");
        if (momNoteTruth) momNoteTruth.style.display = "none";

        var priorPeriod = truth.prior_period;
        var hasPriorCallrail = truth.callrail.prior && truth.callrail.prior.raw_leads != null;

        el("heading-prior-performance").textContent = "February 2026 performance";
        el("heading-current-performance").textContent = "March 2026 performance";

        el("th-mom-prior").textContent = "February";
        el("th-mom-current").textContent = "March";

        var prLabel = hasPriorCallrail ? "Prior (CallRail)" : "Prior (not on file)";
        el("th-src-prior-raw").textContent = prLabel + " raw";
        el("th-src-prior-qual").textContent = prLabel + " potential lead signals";
        el("th-src-cur-raw").textContent = "Current raw";
        el("th-src-cur-qual").textContent = "Current potential lead signals";
        el("th-src-pct").textContent = "% vs prior raw";

        el("th-type-prior").textContent = hasPriorCallrail ? "Prior subset" : "Prior (n/a)";
        el("th-type-current").textContent = "Transcript subset";
        var thFebTruth = el("th-lead-src-feb");
        var thMarTruth = el("th-lead-src-mar");
        if (thFebTruth) thFebTruth.textContent = "February Calls";
        if (thMarTruth) thMarTruth.textContent = "March Calls";

        renderPerfPrior(el("tbody-perf-prior"), truth);
        renderPerfCurrent(el("tbody-perf-current"), truth);
        renderMomTable(el("tbody-mom"), truth);
        msgOnly("tbody-source-quality", 8, "Source quality table is available after intel build summary is present.");
        renderSourceTable(el("tbody-source"), truth);
        renderList(el("insight-source-takeaway"), ["Key takeaway: source quality comparison is clearer when build summary data is available."]);

        var typesPriorT = {};
        for (var z = 0; z < CALL_TYPE_ORDER.length; z++) typesPriorT[CALL_TYPE_ORDER[z]] = 0;
        var hasPriorSubset =
          hasPriorCallrail &&
          truth.transcript_quality_prior &&
          typeof truth.transcript_quality_prior === "object";
        if (hasPriorSubset) {
          for (var k in truth.transcript_quality_prior) {
            if (typesPriorT[k] !== undefined) typesPriorT[k] = num(truth.transcript_quality_prior[k]) || 0;
          }
        }
        var typesCurrT = aggregateCallTypes(calls);
        renderQualityTable(el("tbody-quality"), typesPriorT, typesCurrT, hasPriorSubset);

        renderList(el("insight-changed"), buildChangedBullets(truth));
        renderList(el("insight-patterns"), buildPatternBullets(calls));
        el("insight-missed").textContent = missedPlainLanguage(calls);

        drawFunnel(el("chart-funnel"), calls);
        drawDurationHist(el("chart-duration-hist"), calls);
        drawHourlyChart(el("chart-hourly"), calls);
        var hsTruth = el("chart-hourly-summary");
        if (hsTruth) hsTruth.textContent = hourlySummaryText(calls);
      });
    });
  }

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", run);
  else run();
})();
