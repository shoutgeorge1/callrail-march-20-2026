/**
 * Call quality & intake report — CallRail truth first; transcripts for patterns only.
 */
(function () {
  "use strict";

  var CALL_TYPE_ORDER = [
    "true_pi_opportunity",
    "unclear",
    "property_damage_only",
    "wrong_firm",
    "vendor_sales",
    "existing_client",
    "admin",
  ];

  var CALL_TYPE_LABEL = {
    true_pi_opportunity: "Injury-shaped (transcript label)",
    unclear: "Unclear — needs another listen",
    property_damage_only: "Property damage only (transcript)",
    wrong_firm: "Wrong firm / wrong number",
    vendor_sales: "Vendor or spam",
    existing_client: "Existing client or case status",
    admin: "Admin or non-injury intake",
  };

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
    if (!Number.isFinite(n)) return "—";
    if (Math.abs(n - Math.round(n)) < 1e-6) return String(Math.round(n));
    return n.toFixed(1);
  }

  function formatPct1(x) {
    if (!Number.isFinite(x)) return "—";
    return x.toFixed(1) + "%";
  }

  function pctVsPrior(prev, curr) {
    if (prev == null || curr == null) return "—";
    if (prev === 0) return curr === 0 ? "0%" : "—";
    var x = ((curr - prev) / prev) * 100;
    return (x >= 0 ? "+" : "") + x.toFixed(1) + "%";
  }

  function qualifiedShare(raw, qual) {
    if (raw == null || qual == null || raw === 0) return null;
    return (qual / raw) * 100;
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
      ["Raw leads", formatNum(raw)],
      ["Qualified leads", formatNum(q)],
      ["Qualified share (qualified ÷ raw)", sh == null ? "—" : formatPct1(sh)],
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
      ["Raw leads", formatNum(raw)],
      ["Qualified leads", formatNum(q)],
      ["Qualified share (qualified ÷ raw)", sh == null ? "—" : formatPct1(sh)],
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
      c.colSpan = 5;
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

    addRow("Raw leads", pr, cr, false);
    addRow("Qualified leads", pq, cq, false);
    if (ps != null && cs != null) {
      var row = tr();
      row.appendChild(td("Qualified share"));
      row.appendChild(td(formatPct1(ps), "num"));
      row.appendChild(td(formatPct1(cs), "num"));
      row.appendChild(td(formatNum(cs - ps) + " pts", "num"));
      row.appendChild(td(pctVsPrior(ps, cs), "num"));
      tbody.appendChild(row);
    }
  }

  function renderSourceTable(tbody, truth) {
    tbody.innerHTML = "";
    var sources = (truth && truth.sources) || [];
    var sourcesPrior = (truth && truth.sources_prior) || null;
    var prior = truth && truth.callrail && truth.callrail.prior;
    var hasPrior = prior && prior.raw_leads != null;

    for (var i = 0; i < sources.length; i++) {
      var s = sources[i];
      var name = s.name || "—";
      var pr = priorSourceRow(sourcesPrior, name);
      var cr = num(s.raw);
      var cq = num(s.qualified);
      var pRaw = pr.raw;
      var pQual = pr.qualified;
      var share = qualifiedShare(cr, cq);
      var row = tr();
      row.appendChild(td(name));
      row.appendChild(td(hasPrior && pRaw != null && Number.isFinite(pRaw) ? formatNum(pRaw) : "—", "num"));
      row.appendChild(td(hasPrior && pQual != null && Number.isFinite(pQual) ? formatNum(pQual) : "—", "num"));
      row.appendChild(td(formatNum(cr), "num"));
      row.appendChild(td(formatNum(cq), "num"));
      row.appendChild(td(share == null ? "—" : formatPct1(share), "num"));
      row.appendChild(td(pRaw != null && Number.isFinite(pRaw) ? pctVsPrior(pRaw, cr) : "—", "num"));
      row.appendChild(td(s.heard && s.heard !== "—" ? s.heard : s.heard === "—" ? "—" : ""));
      tbody.appendChild(row);
    }
  }

  function renderQualityTable(tbody, typesPrior, typesCurr, hasPriorSubset) {
    tbody.innerHTML = "";
    for (var i = 0; i < CALL_TYPE_ORDER.length; i++) {
      var k = CALL_TYPE_ORDER[i];
      var p = hasPriorSubset ? typesPrior[k] || 0 : null;
      var c = typesCurr[k] || 0;
      var row = tr();
      row.appendChild(td(CALL_TYPE_LABEL[k] || k));
      row.appendChild(td(hasPriorSubset ? formatNum(p) : "—", "num"));
      row.appendChild(td(formatNum(c), "num"));
      row.appendChild(td(hasPriorSubset ? pctVsPrior(p, c) : "—", "num"));
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
    if (cq > pq) out.push("Qualified lead count is up versus the prior period on file.");
    else if (cq < pq) out.push("Qualified lead count is down versus the prior period on file.");
    var ps = qualifiedShare(pr, pq);
    var cs = qualifiedShare(cr, cq);
    if (ps != null && cs != null) {
      if (cs > ps + 0.5) out.push("Qualified share of raw leads improved versus the prior period.");
      else if (cs < ps - 0.5) out.push("Qualified share of raw leads slipped versus the prior period.");
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
      out.push("Several calls mention trucks or commercial vehicles.");
    if (
      countKeywords(
        calls,
        /\b(unhappy with (my |)(lawyer|attorney)|fire my (lawyer|attorney)|second opinion|switch (to |)(a |)(lawyer|attorney))\b/i
      ) >= 3
    )
      out.push("Some callers sound like they may be unhappy with another lawyer.");
    var shortN = 0;
    for (var j = 0; j < calls.length; j++) {
      var d = num(calls[j].duration);
      if (Number.isFinite(d) && d < 90) shortN++;
    }
    if (shortN / calls.length > 0.25) out.push("Many calls end in under ninety seconds.");
    if (out.length < 2) out.push("No single theme dominates the transcript sample.");
    return out.slice(0, 4);
  }

  function missedPlainLanguage(calls) {
    if (!calls.length) return "No transcript file loaded for review.";
    var n = 0;
    for (var i = 0; i < calls.length; i++) {
      if (calls[i].hidden_opportunity_flag) n++;
    }
    if (n === 0)
      return (
        "In the " +
        calls.length +
        " calls in the transcript subset, none were auto-tagged as a possible near-miss. Spot-checks are still useful."
      );
    return (
      "About " +
      n +
      " of " +
      calls.length +
      " calls in the transcript subset mention injury-related details but were sorted as non-case or unclear—a quick listen may catch ones intake should keep."
    );
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
      { label: "Injury-shaped (transcript label)", n: pi, color: "#14b8a6" },
      { label: "Off-practice, admin, or spam", n: waste, color: "#fdba74" },
      { label: "Other or unclear", n: other, color: "#a8a29e" },
    ];

    var barH = 36;
    var gap = 14;
    var maxW = w - 200;
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
    var labels = ["0–30s", "30–60s", "1–2m", "2–5m", "5m+"];
    var counts = [0, 0, 0, 0, 0];
    for (var i = 0; i < calls.length; i++) {
      var d = num(calls[i].duration);
      if (!Number.isFinite(d)) continue;
      if (d < 30) counts[0]++;
      else if (d < 60) counts[1]++;
      else if (d < 120) counts[2]++;
      else if (d < 300) counts[3]++;
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
      li.textContent = items[i];
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

  function run() {
    Promise.all([
      fetchJson("callrail_report_truth.json"),
      fetchJson("callrail_scored_calls_latest.json"),
      fetchJson("callrail_scored_calls.json"),
      fetchJson("callrail_month_summary.json"),
      fetchJson("opportunity_rate_trend.json"),
      fetchJson("duration_trend.json"),
      fetchJson("source_mix_trend.json"),
    ]).then(function (results) {
      var truth = results[0];
      var latest = results[1];
      var fallback = results[2];
      var summaryRaw = results[3];
      var oppTrend = Array.isArray(results[4]) ? results[4] : [];
      var durTrend = Array.isArray(results[5]) ? results[5] : [];
      var sourceTrendFile = Array.isArray(results[6]) ? results[6] : [];

      var calls = normalizeRecords(latest);
      if (!calls.length) calls = normalizeRecords(fallback);

      var tsShare = timestampShare(calls);
      var trendsOk = tsShare >= 0.05;

      if (!truth || !truth.callrail) {
        el("data-window-range").textContent = "Add callrail_report_truth.json next to this page.";
        el("data-window-raw").textContent = "—";
        el("data-window-qualified").textContent = "—";
        el("data-window-transcript-n").textContent = String(calls.length);
        el("data-window-provenance").textContent =
          "Truth file missing. Transcript count is from scored JSON only.";
        el("heading-prior-performance").textContent = "Prior month performance (not reconciled)";
        el("heading-current-performance").textContent = "Current month performance";
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
        msgOnly("tbody-perf-prior", 2, "Load callrail_report_truth.json for CallRail-backed rows.");
        msgOnly("tbody-perf-current", 2, "Load callrail_report_truth.json for CallRail-backed rows.");
        msgOnly("tbody-mom", 5, "Month-over-month table needs the truth file.");
        msgOnly("tbody-source", 8, "Source table needs the truth file.");
        renderList(el("insight-changed"), [
          "Add callrail_report_truth.json with CallRail totals to unlock volume tables.",
        ]);
        renderList(el("insight-patterns"), buildPatternBullets(calls));
        el("insight-missed").textContent = missedPlainLanguage(calls);
        renderQualityTable(el("tbody-quality"), {}, aggregateCallTypes(calls), false);
        drawFunnel(el("chart-funnel"), calls);
        drawDurationHist(el("chart-duration-hist"), calls);
        setTrendsVisible(false);
        return;
      }

      var cp = truth.current_period || {};
      el("data-window-range").textContent = cp.range_display || cp.label || "—";
      var cur = truth.callrail.current || {};
      el("data-window-raw").textContent = formatNum(num(cur.raw_leads));
      el("data-window-qualified").textContent = formatNum(num(cur.qualified_leads));
      el("data-window-transcript-n").textContent = String(calls.length);
      el("data-window-provenance").textContent =
        "Raw and qualified leads: CallRail totals from callrail_report_truth.json. " +
        "Transcript subset: scored JSON (same pool used for wording notes). " +
        "Timestamp coverage on transcripts: " +
        formatPct1(tsShare * 100) +
        " of records with a parseable call_start_time, start_time, or created_at.";

      var priorPeriod = truth.prior_period;
      var hasPriorCallrail = truth.callrail.prior && truth.callrail.prior.raw_leads != null;

      el("heading-prior-performance").textContent = hasPriorCallrail
        ? (priorPeriod && priorPeriod.label ? priorPeriod.label : "Prior month") + " performance"
        : "Prior month performance (not reconciled)";

      el("heading-current-performance").textContent =
        (cp.label || "Current period") + " performance";

      el("th-mom-prior").textContent = hasPriorCallrail
        ? (priorPeriod && priorPeriod.range_display ? "Prior" : "Prior")
        : "Prior";
      el("th-mom-current").textContent = "Current";

      var prLabel = hasPriorCallrail ? "Prior (CallRail)" : "Prior (not on file)";
      el("th-src-prior-raw").textContent = prLabel + " raw";
      el("th-src-prior-qual").textContent = prLabel + " qualified";
      el("th-src-cur-raw").textContent = "Current raw";
      el("th-src-cur-qual").textContent = "Current qualified";
      el("th-src-pct").textContent = "% vs prior raw";

      el("th-type-prior").textContent = hasPriorCallrail ? "Prior subset" : "Prior (n/a)";
      el("th-type-current").textContent = "Transcript subset";

      renderPerfPrior(el("tbody-perf-prior"), truth);
      renderPerfCurrent(el("tbody-perf-current"), truth);
      renderMomTable(el("tbody-mom"), truth);
      renderSourceTable(el("tbody-source"), truth);

      var typesPrior = {};
      for (var z = 0; z < CALL_TYPE_ORDER.length; z++) typesPrior[CALL_TYPE_ORDER[z]] = 0;
      var hasPriorSubset =
        hasPriorCallrail &&
        truth.transcript_quality_prior &&
        typeof truth.transcript_quality_prior === "object";
      if (hasPriorSubset) {
        for (var k in truth.transcript_quality_prior) {
          if (typesPrior[k] !== undefined) typesPrior[k] = num(truth.transcript_quality_prior[k]) || 0;
        }
      }
      var typesCurr = aggregateCallTypes(calls);
      renderQualityTable(
        el("tbody-quality"),
        typesPrior,
        typesCurr,
        hasPriorSubset
      );

      renderList(el("insight-changed"), buildChangedBullets(truth));
      renderList(el("insight-patterns"), buildPatternBullets(calls));
      el("insight-missed").textContent = missedPlainLanguage(calls);

      drawFunnel(el("chart-funnel"), calls);
      drawDurationHist(el("chart-duration-hist"), calls);

      setTrendsVisible(trendsOk);
      if (trendsOk && window.requestAnimationFrame) {
        window.requestAnimationFrame(function () {
          var summary =
            summaryRaw && typeof summaryRaw === "object" && !Array.isArray(summaryRaw)
              ? summaryRaw
              : {};
          var keys = Object.keys(summary).sort();
          var SOURCE_ORDER = ["google_ads", "gmb", "direct", "chat", "referral", "unknown"];
          if (oppTrend.length)
            drawLineChart(
              el("chart-opp-rate"),
              oppTrend.map(function (r) {
                return { label: r.month.slice(5), y: r.opportunity_rate };
              }),
              "#0f766e"
            );
          if (durTrend.length)
            drawLineChart(
              el("chart-duration-trend"),
              durTrend.map(function (r) {
                return {
                  label: r.month.slice(5),
                  y: r.median_duration_sec != null ? r.median_duration_sec : 0,
                };
              }),
              "#b45309"
            );
          var mixRows = [];
          for (var m = 0; m < keys.length; m++) {
            var mk = keys[m];
            var agg = summary[mk];
            var row = { month: mk };
            for (var si = 0; si < SOURCE_ORDER.length; si++) {
              var bk = SOURCE_ORDER[si];
              row[bk] =
                agg && agg.opportunity_by_source && agg.opportunity_by_source[bk]
                  ? agg.opportunity_by_source[bk].total
                  : 0;
            }
            mixRows.push(row);
          }
          var srcRows = sourceTrendFile.length ? sourceTrendFile : mixRows;
          drawGroupedSource(el("chart-source-mix"), srcRows, SOURCE_ORDER);
        });
      }
    });
  }

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", run);
  else run();
})();
