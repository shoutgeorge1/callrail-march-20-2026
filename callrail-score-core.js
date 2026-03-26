/**
 * Shared deterministic CallRail scoring (Node). Used by score_callrail_pipeline + callrail_mom_pipeline.
 */
const CITIES = [
  "los angeles",
  "san diego",
  "san francisco",
  "orange county",
  "sacramento",
  "fresno",
  "long beach",
  "oakland",
  "bakersfield",
  "anaheim",
  "santa ana",
  "riverside",
  "stockton",
  "irvine",
  "chula vista",
  "fremont",
  "san bernardino",
  "modesto",
  "fontana",
  "oxnard",
  "glendale",
  "huntington beach",
  "santa clarita",
  "pasadena",
  "torrance",
  "east la",
  "west covina",
  "northridge",
  "van nuys",
  "hollywood",
  "compton",
  "inglewood",
  "moreno valley",
];

function safeStr(v) {
  if (v == null) return "";
  return String(v).trim();
}

function durationSec(record) {
  const n = Number(record.duration);
  return Number.isFinite(n) ? n : null;
}

/** Prefer call_start_time, then start_time, then created_at */
function getPreferredTimestamp(record) {
  const a = record.call_start_time;
  const b = record.start_time;
  const c = record.created_at;
  if (a != null && String(a).trim()) return String(a).trim();
  if (b != null && String(b).trim()) return String(b).trim();
  if (c != null && String(c).trim()) return String(c).trim();
  return null;
}

function parseMonthKeyFromRecord(record) {
  const ts = getPreferredTimestamp(record);
  if (!ts) return null;
  const ms = Date.parse(ts);
  if (Number.isNaN(ms)) return null;
  const d = new Date(ms);
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  return `${y}-${m}`;
}

function normalizeSourceBucket(src) {
  const s = safeStr(src).toLowerCase();
  if (/google ads|ppc|paid search|adwords/.test(s)) return "google_ads";
  if (/\bchat\b|apex|live chat|ctl/.test(s)) return "chat";
  if (/referral|referred|friend|word of mouth/.test(s)) return "referral";
  if (/gmb|google business|maps|local|multi use tracking/.test(s)) return "gmb";
  if (/^direct|direct\b/.test(s)) return "direct";
  return "unknown";
}

function extractGeoHint(text) {
  const t = text.toLowerCase();
  const freeways = t.match(
    /\b(i[- ]?[0-9]{1,3}|sr[- ]?[0-9]{1,3}|us[- ]?[0-9]{1,3}|highway\s*[0-9]{1,3}|route\s*[0-9]{1,3}|the\s+405|the\s+101|the\s+5|the\s+10|the\s+110|the\s+605)\b/i
  );
  if (freeways) return freeways[0].replace(/\s+/g, " ").trim();
  for (const c of CITIES) {
    const re = new RegExp("\\b" + c.replace(/ /g, "\\s+") + "\\b", "i");
    if (re.test(t)) {
      return c
        .split(" ")
        .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
        .join(" ");
    }
  }
  const hood = t.match(/\b(neighborhood|downtown|midtown|valley|south (la|gate))\b/i);
  if (hood) return hood[0];
  return null;
}

function parseTiming(record) {
  const ts = getPreferredTimestamp(record);
  if (ts == null) return { hour_of_day: null, weekday: null };
  const ms = Date.parse(ts);
  if (Number.isNaN(ms)) return { hour_of_day: null, weekday: null };
  const d = new Date(ms);
  return { hour_of_day: d.getHours(), weekday: d.getDay() };
}

function attorneySwitchProbability(t) {
  let p = 0.06;
  if (/\b(other|different|new)\s+(lawyer|attorney|firm)\b/i.test(t)) p += 0.28;
  if (/second opinion|switch (to |)(a |)(lawyer|attorney)|fire my (lawyer|attorney)/i.test(t)) p += 0.22;
  if (/unhappy with (my |)(lawyer|attorney)|frustrated with (my |)(lawyer|attorney)/i.test(t)) p += 0.2;
  if (/won'?t call me back|never (calls|returns)|ghosting|no communication|can'?t reach my (lawyer|attorney)/i.test(t))
    p += 0.18;
  if (/my (lawyer|attorney) (said|told|recommended)/i.test(t)) p += 0.05;
  return Math.min(1, Math.round(p * 100) / 100);
}

function opportunityScore(text) {
  const t = text.toLowerCase();
  let s = 38;
  const add = [
    [/\b(hospital|ambulance|emergency room|\ber\b|surgery|surgical|icu|intensive care)\b/i, 12],
    [/\b(fracture|broken (bone|leg|arm)|mri|ct scan|whiplash|concussion|stitches)\b/i, 10],
    [/\b(severe|serious injury|catastrophic|wrongful death|paralyzed|permanent)\b/i, 14],
    [/\b(pain|hurts|can'?t walk|headache|neck pain|back pain|injured)\b/i, 7],
    [/\b(truck|semi|tractor|commercial vehicle|delivery van|uber|lyft)\b/i, 8],
    [/\b(multi[- ]vehicle|multiple cars|pile[- ]?up|three (cars|vehicles))\b/i, 7],
    [/\b(their fault|ran (a |)red|rear[- ]?end|hit me|t[- ]bone|liability)\b/i, 8],
    [/\b(just happened|yesterday|this morning|last night|two days ago|earlier today)\b/i, 7],
    [/\b(unhappy with (my |)(attorney|lawyer)|other (attorney|lawyer)|second opinion)\b/i, 9],
    [/\b(insurance (won'?t|isn'?t)|adjuster|denied|settlement offer|lowball|underinsured)\b/i, 6],
    [/\b(can'?t work|lost wages|missed work|off work|disability|fmla)\b/i, 9],
    [/\b(accident|crash|collision|wreck|motor vehicle|car accident|slip and fall)\b/i, 6],
  ];
  for (const [re, pts] of add) {
    if (re.test(t)) s += pts;
  }
  const sub = [
    [/\b(property damage only|no(t |) injured|not injured|just the car|only (the |)vehicle|rental coverage)\b/i, -22],
    [/\b(vendor|spam|robocall|solicit|wrong number|telemarket|sales call)\b/i, -35],
    [/\b(wrong firm|wrong office|not insider|different firm|called the wrong)\b/i, -18],
    [/\b(restraining order|family law|divorce|criminal|bankruptcy|immigration|employment lawyer)\b/i, -25],
    [/\b(respond(ing)? to (a |)(demand|letter)|your client|one of your clients)\b/i, -15],
    [/\b(how much (does|will) it cost|courthouse|serve(r| )the paper|deliver the paper)\b/i, -12],
    [/\b(not a case|can'?t help|don'?t handle|not something we|only deal with)\b/i, -12],
    [/\b(administrative|billing department|records request)\b/i, -10],
  ];
  for (const [re, pts] of sub) {
    if (re.test(t)) s += pts;
  }
  return Math.max(0, Math.min(100, Math.round(s)));
}

function callerHeavyText(t) {
  const parts = String(t).split(/\b(?:caller|customer):\s*/i);
  if (parts.length < 2) return String(t).toLowerCase().replace(/\s+/g, " ");
  return parts
    .slice(1)
    .join(" ")
    .toLowerCase()
    .replace(/\s+/g, " ");
}

function classifyCallType(t, score) {
  const low = t.toLowerCase();
  const cLow = callerHeavyText(t);
  if (/\b(vendor|spam|robocall|solicit|wrong number|telemarket|sales call)\b/i.test(low)) return "vendor_sales";
  if (/\b(wrong firm|wrong office|not insider|different (law |)firm|called the wrong)\b/i.test(low)) return "wrong_firm";
  if (
    /\b(respond(ing)? to (your |a |)(demand|letter)|your client|one of your clients|against your client)\b/i.test(low)
  )
    return "existing_client";
  if (
    /\b(restraining order|courthouse|deliver the paper|serve(r| )the paper|family law|divorce)\b/i.test(low) &&
    !/\b(hurt|injured|pain|hospital|ambulance|whiplash|broken|concussion)\b/i.test(low)
  )
    return "admin";
  if (
    /\bnot for an accident\b/i.test(cLow) &&
    !/\b(hurt|hospital|ambulance|whiplash|broken bone|fracture|surgery)\b/i.test(cLow)
  )
    return "property_damage_only";
  if (
    /\b(property damage only|not injured|no injury|just (the |)car|coverage (issue|dispute)|rental car|only covered)\b/i.test(
      cLow
    ) &&
    !/\b(hurt|injured|my neck|my back|hospital|ambulance|whiplash|broken bone)\b/i.test(cLow)
  )
    return "property_damage_only";
  if (
    /\b(accident|injury|injured|hurt|pain|crash|collision|wreck|motor vehicle|personal injury|slip|fall|liability)\b/i.test(
      cLow
    ) &&
    score >= 42
  )
    return "true_pi_opportunity";
  return "unclear";
}

function confusionLanguage(t) {
  return /\b(confus|not sure|unclear|don'?t know if|i think|maybe|kind of)\b/i.test(t);
}

function excerpt(text, max) {
  const s = safeStr(text).replace(/\s+/g, " ");
  if (s.length <= max) return s;
  return s.slice(0, max).trim() + "…";
}

const WASTE_TYPES = ["vendor_sales", "wrong_firm", "property_damage_only", "admin", "existing_client"];

function scoreOneRecord(record, index) {
  const call_id = safeStr(record.call_id) || safeStr(record.id) || "unknown_" + index;
  const transcription = safeStr(record.transcription || record.transcript);
  if (!transcription) return null;

  const sourceRaw = safeStr(record.source) || "unknown";
  const duration = durationSec(record);
  const tracking = safeStr(record.tracking_phone_number);

  const opp = opportunityScore(transcription);
  const switchP = attorneySwitchProbability(transcription);
  const type = classifyCallType(transcription, opp);
  const { hour_of_day, weekday } = parseTiming(record);
  const geo = extractGeoHint(transcription);
  const bucket = normalizeSourceBucket(sourceRaw);

  const durForShort = duration != null ? duration : null;
  const shortCall = durForShort != null && durForShort < 90;
  const conf = confusionLanguage(transcription);
  const hidden = opp >= 55 && (type !== "true_pi_opportunity" || shortCall === true || conf === true);

  const out = {
    call_id,
    opportunity_score: opp,
    attorney_switch_probability: switchP,
    call_type: type,
    hidden_opportunity_flag: hidden,
    duration: duration != null ? duration : null,
    source_bucket: bucket,
    geo_hint: geo,
    transcript_excerpt: excerpt(transcription, 200),
    source: sourceRaw,
    transcription,
    qualified_score: opp,
    notes: "",
    hour_of_day,
    weekday,
  };

  if (record.call_start_time != null && String(record.call_start_time).trim())
    out.call_start_time = record.call_start_time;
  if (record.start_time != null && String(record.start_time).trim()) out.start_time = record.start_time;
  if (record.created_at != null && String(record.created_at).trim()) out.created_at = record.created_at;

  if (tracking) out.tracking_phone_number = tracking;
  if (safeStr(record.recording_url)) out.recording_url = record.recording_url;

  return out;
}

function syntheticMonthKey(index, total, endDate) {
  const endMs = endDate instanceof Date ? endDate.getTime() : Date.now();
  const windowMs = 60 * 24 * 60 * 60 * 1000;
  const startMs = endMs - windowMs;
  const t = total <= 1 ? endMs : startMs + (index / Math.max(1, total - 1)) * (endMs - startMs);
  const d = new Date(t);
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}`;
}

module.exports = {
  scoreOneRecord,
  parseMonthKeyFromRecord,
  getPreferredTimestamp,
  syntheticMonthKey,
  normalizeSourceBucket,
  WASTE_TYPES,
  OPPORTUNITY_THRESHOLD: 58,
  SWITCH_THRESHOLD: 0.35,
};
