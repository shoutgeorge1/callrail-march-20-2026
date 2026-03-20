# -*- coding: utf-8 -*-
"""
February vs March 2026 CallRail MoM report: normalize, classify, project, HTML + charts.
"""
from __future__ import annotations

import html
import re
import warnings
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import pandas as pd
import seaborn as sns

warnings.filterwarnings("ignore", category=UserWarning)

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "report_output"
CHARTS = ROOT / "charts"
OUT.mkdir(parents=True, exist_ok=True)
CHARTS.mkdir(parents=True, exist_ok=True)

# February = export file that contains Feb-only rows; March = through 3/20 export
PATH_FEB = Path(r"c:\Users\georgea\Downloads\Call List-2026-03-20 (2) (1).xlsx")
PATH_MAR = Path(r"c:\Users\georgea\Downloads\Call List-2026-03-20 (1).xlsx")

MAR_CUTOFF = pd.Timestamp("2026-03-20 23:59:59")
FEB_START = pd.Timestamp("2026-02-01")
FEB_END = pd.Timestamp("2026-02-28 23:59:59")
MAR_PROJ_DAYS = 31
MAR_ELAPSED_DAYS = 20  # through Mar 20 inclusive


def load_calls(path: Path) -> pd.DataFrame:
    xl = pd.ExcelFile(path)
    df = pd.read_excel(xl, sheet_name=xl.sheet_names[0])
    df.columns = [str(c).strip() for c in df.columns]
    if "Start Time" not in df.columns:
        raise ValueError(f"Missing Start Time in {path}: {list(df.columns)}")
    return df


def normalize(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    rename = {}
    for c in d.columns:
        lc = c.strip().lower()
        if lc in ("duration (seconds)", "duration_seconds", "duration"):
            rename[c] = "Duration (seconds)"
    d = d.rename(columns=rename)
    d["Start Time"] = pd.to_datetime(d["Start Time"], errors="coerce")
    d["Duration (seconds)"] = pd.to_numeric(d.get("Duration (seconds)", 0), errors="coerce").fillna(0).astype(int)
    for col in [
        "Call Status",
        "Number Name",
        "Tracking Number",
        "Source",
        "Name",
        "Phone Number",
        "Email",
        "City",
        "State",
        "Medium",
        "Landing Page",
        "Tags",
        "Qualified",
        "Recording Url",
        "Note",
    ]:
        if col not in d.columns:
            d[col] = np.nan
        d[col] = d[col].fillna("").astype(str).str.strip()
    if "First-Time Caller" in d.columns:
        d["First-Time Caller"] = d["First-Time Caller"].map(
            lambda x: str(x).lower() in ("true", "1", "yes") if pd.notna(x) else False
        )
    else:
        d["First-Time Caller"] = False
    d["Value"] = pd.to_numeric(d.get("Value", np.nan), errors="coerce")
    return d


def dedupe(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    url = d["Recording Url"].fillna("")
    has_url = url.ne("") & url.ne("nan")
    d1 = d[has_url].drop_duplicates(subset=["Recording Url"], keep="first")
    d2 = d[~has_url].copy()
    d2["_k"] = (
        d2["Phone Number"].astype(str)
        + "|"
        + d2["Start Time"].dt.round("min").astype(str)
        + "|"
        + d2["Duration (seconds)"].astype(str)
    )
    d2 = d2.drop_duplicates(subset=["_k"], keep="first").drop(columns=["_k"])
    return pd.concat([d1, d2], ignore_index=True)


def is_internal_test(row: pd.Series) -> bool:
    t = _norm(str(row.get("Tags", "")) + " " + str(row.get("Name", "")))
    if "internal test" in t or "dc: test calls" in t:
        return True
    if re.search(r"\bgeorge\b", t) and "test" in t:
        return True
    if "nextiva" in t and "test" in t:
        return True
    return False


def _norm(s: str) -> str:
    return s.lower().replace("–", "-").replace("—", "-")


def fmt_duration_human(sec: int) -> str:
    sec = max(0, int(sec))
    m, s = divmod(sec, 60)
    if m == 0:
        return f"{s} sec"
    return f"{m} min {s} sec"


def duration_comment(sec: int, median_ref: float) -> str:
    """Short plain-English note for typical intake context."""
    sec = float(sec)
    if sec < 20:
        return "Very short — often a hang-up, misdial, or failed connect."
    if sec < 90:
        return "Short — might be screening, wrong number, or quick disqualification."
    if sec < int(median_ref):
        return f"Below typical handle time (~{fmt_duration_human(int(median_ref))}) — could be efficient or rushed."
    if sec < 600:
        return "Typical consult range — enough time to qualify most calls."
    return "Long — complex story, language barrier, or wrap-up; worth a listen."


def hour_label_ampm(h: int) -> str:
    h = int(h) % 24
    if h == 0:
        return "12am"
    if h < 12:
        return f"{h}am"
    if h == 12:
        return "12pm"
    return f"{h - 12}pm"


def classify_source_group(src: str) -> str:
    s = _norm(src)
    if "google ads" in s or s == "google ads":
        return "Google Ads"
    if s == "direct":
        return "Direct"
    if "gmb" in s or "google my business" in s:
        return "GMB / Google My Business"
    if "facebook" in s or "meta" in s:
        return "Facebook / Meta"
    if "yelp" in s:
        return "Yelp"
    if "aa website" in s or "blog" in s:
        return "AA Website & Blog"
    if s == "website" or "insideraccident" in s:
        return "Website"
    if "legacy" in s:
        return "Legacy / other"
    if "apex" in s or "chat" in s:
        return "APEX / chat"
    if "landing" in s or "844" in s or "brand" in s:
        return "Landing / brand numbers"
    return "Other"


def classify_strategic_bucket(row: pd.Series) -> str:
    """Mutually exclusive MoM buckets (priority order)."""
    status = _norm(str(row.get("Call Status", "")))
    tags = _norm(str(row.get("Tags", "")))
    qual = _norm(str(row.get("Qualified", "")))
    dur = int(row.get("Duration (seconds)", 0) or 0)
    c = f"{tags} {qual} {status}"

    def has(*w: str) -> bool:
        return any(x in c for x in w)

    # 1) Operational / silent / dead air / abandon
    if "abandoned call" in status:
        return "OPERATIONAL_SILENT_DEAD_AIR"
    if has(
        "dead air",
        "agent silent",
        "hang up",
        "hang-up",
        "4 rings",
        "mo: abandoned",
        "missed calls",
        "sec - hang up",
        "incomplete intake",
        "transfer failed",
    ):
        return "OPERATIONAL_SILENT_DEAD_AIR"
    if status == "answered call" and dur <= 10 and qual == "not scored" and not tags.replace("nan", "").strip():
        return "OPERATIONAL_SILENT_DEAD_AIR"

    # 2) Attorney switch / dropped case
    if has(
        "attorney switch",
        "attorney drop",
        "dropped case",
        "seeking new counsel",
        "second opinion",
        "case transfer",
        "reassignment",
        "potential case transfer",
    ):
        return "ATTORNEY_SWITCH_DROPPED"

    # 3) Medical symptom / neuropathy / illness confusion
    if has(
        "neuropathy",
        "medical illness",
        "non-accident",
        "medical emergency / non-pi",
        "psychiatric",
        "symptom inquiry",
        "device scam",
        "non-pi — medical",
    ):
        return "MEDICAL_SYMPTOM_CONFUSION"

    # 4) Vendor / sales
    if has(
        "vendor",
        "vendore",
        "lexisnexis",
        "b2b sales",
        "marketing call",
        "solicitation",
        "collections",
        "funding company",
        "lien holder",
        "opposing counsel",
        "referral solicitation",
    ):
        return "VENDOR_PROVIDER_SALES"

    # 5) Wrong firm / admin / existing client (combined per brief)
    if has(
        "wrong firm",
        "misdial",
        "wrong number",
        "competitor",
        "sweet james",
        "abrams",
        "jacoby",
        "navigation error",
        "closed case paperwork",
        "existing client",
        "existing customer",
        "cb: existing",
        "prior case",
        "settlement phase",
        "case management",
        "attorney inquiry – existing",
        "attorney call — case status",
    ):
        return "WRONG_FIRM_ADMIN_EXISTING"

    # 6) Non-PI wrong practice
    if has(
        "wrong practice area",
        "non-pi",
        "non pi",
        "not pi",
        "employment",
        "family law",
        "criminal",
        "tax",
        "bankruptcy",
        "landlord",
        "workers comp",
        "intellectual property",
    ):
        return "NON_PI_WRONG_PRACTICE"

    # 7) Minor / soft tissue PI
    if has(
        "soft tissue",
        "low value pi",
        "minor injury",
        "rear end",
        "rear-end",
        "low impact",
        "pd only",
        "property damage only",
        "spanish – not qualified",
    ):
        return "MINOR_SOFT_TISSUE_PI"

    # 8) True PI
    if qual == "qualified lead":
        return "TRUE_POTENTIAL_PI"
    if has(
        "potential pi",
        "high-intent",
        "high intent pi",
        "fresh injury",
        "mva",
        "premises",
        "slip",
        "dog bite",
        "pedestrian",
        "truck",
        "catastrophic",
        "nursing home",
        "wrongful death",
        "pi – auto",
        "pi – bicycle",
        "new lead — injury",
    ):
        return "TRUE_POTENTIAL_PI"

    if has("property damage only", "lead not viable"):
        return "MINOR_SOFT_TISSUE_PI"

    return "UNCLEAR_UNTAGGED"


def add_local_time(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    st = d["Start Time"]
    # Treat naive timestamps as Pacific wall time (CallRail local export assumption)
    try:
        d["start_pt"] = st.dt.tz_localize("America/Los_Angeles", ambiguous="NaT", nonexistent="shift_forward")
    except (TypeError, AttributeError):
        d["start_pt"] = pd.to_datetime(st).dt.tz_localize("America/Los_Angeles", ambiguous="NaT", nonexistent="shift_forward")
    d["hour_pt"] = d["start_pt"].dt.hour
    d["weekday"] = d["start_pt"].dt.day_name()
    return d


def compute_kpis(df: pd.DataFrame, internal_mask: pd.Series) -> dict:
    core = df[~internal_mask]
    n = len(core)
    answered = (core["Call Status"].str.lower() == "answered call").sum()
    abandoned = (core["Call Status"].str.lower() == "abandoned call").sum()
    ft = core["First-Time Caller"].sum() if "First-Time Caller" in core.columns else 0
    dur = core["Duration (seconds)"]
    tagged = (core["Tags"].str.len() > 0) & (core["Tags"].ne("nan"))
    scored_q = core["Qualified"].str.lower() == "qualified lead"
    not_scored = core["Qualified"].str.lower() == "not scored"
    ql_rate = scored_q.mean() * 100 if n else 0
    true_pi = (core.apply(classify_strategic_bucket, axis=1) == "TRUE_POTENTIAL_PI").sum()
    return {
        "n": n,
        "answered": int(answered),
        "abandoned": int(abandoned),
        "first_time": int(ft),
        "avg_dur": float(dur.mean()) if n else 0.0,
        "median_dur": float(dur.median()) if n else 0.0,
        "tagged": int(tagged.sum()),
        "qualified_leads": int(scored_q.sum()),
        "not_scored": int(not_scored.sum()),
        "ql_rate": ql_rate,
        "true_pi_est": int(true_pi),
    }


def pace_project(value: float, elapsed_days: int, month_days: int) -> float:
    if elapsed_days <= 0:
        return value
    return value / elapsed_days * month_days


def pct_change(new: float, old: float) -> str:
    if old == 0:
        return "—"
    return f"{((new - old) / old * 100):+.1f}%"


def setup_charts():
    sns.set_theme(style="whitegrid", context="talk", font_scale=1.05)
    plt.rcParams["figure.dpi"] = 120
    plt.rcParams["savefig.facecolor"] = "#ffffff"
    plt.rcParams["font.size"] = 12


def bucket_duration_counts(series: pd.Series) -> tuple[list[int], list[str]]:
    """
    Wider, more evenly spread buckets (seconds) so bars are comparable across the range.
    """
    s = series.astype(int)
    edges = [0, 30, 120, 300, 600, 1200, 10**9]
    labels = [
        "Under 30 sec",
        "30 sec–2 min",
        "2–5 min",
        "5–10 min",
        "10–20 min",
        "20+ min",
    ]
    out = []
    for lo, hi in zip(edges[:-1], edges[1:]):
        out.append(int(((s >= lo) & (s < hi)).sum()))
    return out, labels


# Stacked bar: three gradient families (opportunity / friction / ops & noise)
BUCKET_COLOR = {
    "TRUE_POTENTIAL_PI": "#047857",
    "MINOR_SOFT_TISSUE_PI": "#059669",
    "ATTORNEY_SWITCH_DROPPED": "#10b981",
    "MEDICAL_SYMPTOM_CONFUSION": "#6ee7b7",
    "NON_PI_WRONG_PRACTICE": "#c2410c",
    "WRONG_FIRM_ADMIN_EXISTING": "#fb923c",
    "VENDOR_PROVIDER_SALES": "#6b21a8",
    "OPERATIONAL_SILENT_DEAD_AIR": "#a855f7",
    "UNCLEAR_UNTAGGED": "#d8b4fe",
}


def main():
    setup_charts()
    assert PATH_FEB.exists(), f"Missing {PATH_FEB}"
    assert PATH_MAR.exists(), f"Missing {PATH_MAR}"

    feb_raw = dedupe(normalize(load_calls(PATH_FEB)))
    mar_raw = dedupe(normalize(load_calls(PATH_MAR)))

    feb_raw = feb_raw[(feb_raw["Start Time"] >= FEB_START) & (feb_raw["Start Time"] <= FEB_END)]
    mar_raw = mar_raw[(mar_raw["Start Time"] >= pd.Timestamp("2026-03-01")) & (mar_raw["Start Time"] <= MAR_CUTOFF)]

    feb_raw["period"] = "February 2026"
    mar_raw["period"] = "March 2026 (through Mar 20)"

    for df in (feb_raw, mar_raw):
        df["internal_test"] = df.apply(is_internal_test, axis=1)
        df["strategic_bucket"] = df.apply(classify_strategic_bucket, axis=1)
        df["source_group"] = df["Source"].apply(classify_source_group)

    feb_int = feb_raw["internal_test"].sum()
    mar_int = mar_raw["internal_test"].sum()

    k_feb = compute_kpis(feb_raw, feb_raw["internal_test"])
    k_mar = compute_kpis(mar_raw, mar_raw["internal_test"])

    # March projection (core metrics only)
    def proj(k: dict) -> dict:
        out = {}
        for key in k:
            if key == "ql_rate":
                out[key] = k[key]  # rate stays
            else:
                out[key] = pace_project(float(k[key]), MAR_ELAPSED_DAYS, MAR_PROJ_DAYS)
        out["true_pi_est"] = int(round(out["true_pi_est"]))
        out["n"] = int(round(out["n"]))
        out["answered"] = int(round(out["answered"]))
        out["abandoned"] = int(round(out["abandoned"]))
        out["first_time"] = int(round(out["first_time"]))
        out["tagged"] = int(round(out["tagged"]))
        out["qualified_leads"] = int(round(out["qualified_leads"]))
        out["not_scored"] = int(round(out["not_scored"]))
        return out

    k_mar_proj = proj(k_mar)

    feb_core = feb_raw[~feb_raw["internal_test"]]
    mar_core = mar_raw[~mar_raw["internal_test"]]
    feb_b = feb_core["strategic_bucket"].value_counts()
    mar_b = mar_core["strategic_bucket"].value_counts()

    # --- Charts ---
    # 1) Source comparison
    src_order = [
        "Google Ads",
        "Direct",
        "GMB / Google My Business",
        "Facebook / Meta",
        "Yelp",
        "Website",
        "AA Website & Blog",
        "APEX / chat",
        "Landing / brand numbers",
        "Other",
    ]
    feb_sg = feb_raw[~feb_raw["internal_test"]].groupby("source_group").size().reindex(src_order, fill_value=0)
    mar_sg = mar_raw[~mar_raw["internal_test"]].groupby("source_group").size().reindex(src_order, fill_value=0)
    x = np.arange(len(src_order))
    w = 0.35
    fig, ax = plt.subplots(figsize=(16, 7))
    ax.bar(x - w / 2, feb_sg.values, width=w, label="February (full month)", color="#2563eb", edgecolor="white", linewidth=0.8)
    ax.bar(x + w / 2, mar_sg.values, width=w, label="March through 3/20", color="#f97316", edgecolor="white", linewidth=0.8)
    ax.set_xticks(x)
    ax.set_xticklabels(src_order, rotation=35, ha="right", fontsize=13)
    ax.set_ylabel("Calls (excl. internal tests)", fontsize=14)
    ax.set_title("Source mix: February vs March (through Mar 20)", fontweight="bold", fontsize=16)
    ax.tick_params(axis="y", labelsize=13)
    ax.legend(fontsize=13, loc="upper right")
    plt.tight_layout()
    fig.savefig(CHARTS / "mom_source_comparison.png", bbox_inches="tight")
    plt.close()

    # 2) Strategic bucket grouped
    bucket_order = [
        "TRUE_POTENTIAL_PI",
        "MINOR_SOFT_TISSUE_PI",
        "ATTORNEY_SWITCH_DROPPED",
        "NON_PI_WRONG_PRACTICE",
        "VENDOR_PROVIDER_SALES",
        "WRONG_FIRM_ADMIN_EXISTING",
        "MEDICAL_SYMPTOM_CONFUSION",
        "OPERATIONAL_SILENT_DEAD_AIR",
        "UNCLEAR_UNTAGGED",
    ]
    labels = {
        "TRUE_POTENTIAL_PI": "True PI",
        "MINOR_SOFT_TISSUE_PI": "Minor / soft-tissue PI",
        "ATTORNEY_SWITCH_DROPPED": "Attorney switch / dropped",
        "NON_PI_WRONG_PRACTICE": "Non-PI / wrong practice",
        "VENDOR_PROVIDER_SALES": "Vendor / sales",
        "WRONG_FIRM_ADMIN_EXISTING": "Wrong firm / admin / existing",
        "MEDICAL_SYMPTOM_CONFUSION": "Symptom / illness confusion",
        "OPERATIONAL_SILENT_DEAD_AIR": "Silent / abandon / connect",
        "UNCLEAR_UNTAGGED": "Unclear / untagged",
    }
    fb = feb_raw[~feb_raw["internal_test"]]["strategic_bucket"].value_counts().reindex(bucket_order, fill_value=0)
    mb = mar_raw[~mar_raw["internal_test"]]["strategic_bucket"].value_counts().reindex(bucket_order, fill_value=0)
    fig, ax = plt.subplots(figsize=(16, 7.5))
    x = np.arange(len(bucket_order))
    ax.bar(x - w / 2, fb.values, width=w, label="February", color="#1d4ed8", edgecolor="white", linewidth=0.8)
    ax.bar(x + w / 2, mb.values, width=w, label="March through 3/20", color="#ea580c", edgecolor="white", linewidth=0.8)
    ax.set_xticks(x)
    ax.set_xticklabels([labels[b] for b in bucket_order], rotation=28, ha="right", fontsize=12)
    ax.set_ylabel("Calls", fontsize=14)
    ax.set_title("Strategic buckets (modeled from tags + status)", fontweight="bold", fontsize=16)
    ax.tick_params(axis="y", labelsize=13)
    ax.legend(fontsize=13)
    plt.tight_layout()
    fig.savefig(CHARTS / "mom_bucket_comparison.png", bbox_inches="tight")
    plt.close()

    # 3) Duration buckets — wider spans for a more even spread across call lengths
    feb_dur, dur_labels = bucket_duration_counts(feb_raw[~feb_raw["internal_test"]]["Duration (seconds)"])
    mar_dur, _ = bucket_duration_counts(mar_raw[~mar_raw["internal_test"]]["Duration (seconds)"])
    fig, ax = plt.subplots(figsize=(14, 7))
    xh = np.arange(len(dur_labels))
    ax.bar(xh - w / 2, feb_dur, width=w, label="February (full month)", color="#2563eb", edgecolor="white", linewidth=0.8)
    ax.bar(xh + w / 2, mar_dur, width=w, label="March through 3/20", color="#ea580c", edgecolor="white", linewidth=0.8)
    ax.set_xticks(xh)
    ax.set_xticklabels(dur_labels, fontsize=14)
    ax.set_ylabel("Number of calls", fontsize=14)
    ax.set_title("Call length — February vs March (same bucket definitions)", fontweight="bold", fontsize=16)
    ax.tick_params(axis="y", labelsize=13)
    ax.legend(fontsize=13)
    plt.tight_layout()
    fig.savefig(CHARTS / "mom_duration_buckets.png", bbox_inches="tight")
    plt.close()

    # 4) Heatmap March — hour labels AM/PM
    mar_local = add_local_time(mar_raw[~mar_raw["internal_test"]])
    heat = mar_local.pivot_table(index="weekday", columns="hour_pt", values="Call Status", aggfunc="count", fill_value=0)
    day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    heat = heat.reindex([d for d in day_order if d in heat.index])
    fig, ax = plt.subplots(figsize=(15, 6))
    sns.heatmap(heat, cmap="YlOrRd", ax=ax, cbar_kws={"label": "Calls"}, annot=False)
    ax.set_xlabel("Hour (local)", fontsize=13)
    labs = [hour_label_ampm(int(h)) for h in heat.columns]
    ax.set_xticklabels(labs, rotation=45, ha="right", fontsize=11)
    ax.set_yticklabels(ax.get_yticklabels(), rotation=0, fontsize=12)
    ax.set_title("March (through 3/20) — weekday × hour", fontweight="bold", fontsize=16)
    plt.tight_layout()
    fig.savefig(CHARTS / "mom_heatmap_march.png", bbox_inches="tight")
    plt.close()

    # 5) Source × bucket stacked — gradient families + large legend
    sub = mar_raw[~mar_raw["internal_test"]]
    top_sources = sub["source_group"].value_counts().head(8).index
    pivot = pd.crosstab(sub[sub["source_group"].isin(top_sources)]["source_group"], sub["strategic_bucket"])
    pivot = pivot.reindex(columns=bucket_order, fill_value=0).loc[list(top_sources)]
    fig, ax = plt.subplots(figsize=(18, 10))
    left = np.zeros(len(pivot))
    for col in bucket_order:
        if col not in pivot.columns:
            continue
        vals = pivot[col].values
        ax.barh(
            pivot.index,
            vals,
            left=left,
            color=BUCKET_COLOR[col],
            label=labels[col],
            height=0.76,
            edgecolor="white",
            linewidth=0.6,
        )
        left = left + vals
    ax.set_xlabel("Calls", fontsize=15)
    ax.set_title("March through 3/20 — source × bucket (top sources)", fontweight="bold", fontsize=17)
    ax.tick_params(axis="y", labelsize=14)
    ax.tick_params(axis="x", labelsize=13)
    ax.legend(
        bbox_to_anchor=(1.02, 1),
        loc="upper left",
        fontsize=13,
        title="Color key (grouped)",
        title_fontsize=14,
        frameon=True,
        fancybox=True,
    )
    plt.tight_layout()
    fig.savefig(CHARTS / "mom_source_bucket_stacked.png", bbox_inches="tight")
    plt.close()

    # 6) Pie March — labels + % on slices (nonzero wedges only for readability)
    mc = mar_raw[~mar_raw["internal_test"]]["strategic_bucket"].value_counts().reindex(bucket_order, fill_value=0)
    total_m = int(mc.sum())
    short_pie = {
        "TRUE_POTENTIAL_PI": "True PI",
        "MINOR_SOFT_TISSUE_PI": "Minor / soft PI",
        "ATTORNEY_SWITCH_DROPPED": "Atty switch",
        "NON_PI_WRONG_PRACTICE": "Non-PI",
        "VENDOR_PROVIDER_SALES": "Vendor",
        "WRONG_FIRM_ADMIN_EXISTING": "Wrong firm / admin",
        "MEDICAL_SYMPTOM_CONFUSION": "Symptom",
        "OPERATIONAL_SILENT_DEAD_AIR": "Silent / abandon",
        "UNCLEAR_UNTAGGED": "Unclear",
    }
    pairs = [(b, int(mc[b])) for b in bucket_order if mc[b] > 0]
    fig, ax = plt.subplots(figsize=(12, 12))
    if total_m and pairs:
        vals = [p[1] for p in pairs]
        keys = [p[0] for p in pairs]
        pie_cols = [BUCKET_COLOR[k] for k in keys]
        pie_lbl = [short_pie[k] for k in keys]

        def autopct_fn(pct):
            return f"{pct:.1f}%" if pct >= 2.5 else ""

        wedges, texts, autotexts = ax.pie(
            vals,
            labels=pie_lbl,
            autopct=autopct_fn,
            colors=pie_cols,
            startangle=90,
            pctdistance=0.75,
            labeldistance=1.08,
            textprops={"fontsize": 11},
            wedgeprops={"linewidth": 2, "edgecolor": "white"},
        )
        for t in autotexts:
            t.set_fontsize(11)
            t.set_fontweight("bold")
        for t in texts:
            t.set_fontsize(11)
    else:
        ax.text(0.5, 0.5, "No data", ha="center", va="center", transform=ax.transAxes)
    ax.set_title("March (through 3/20) — call intent mix", fontsize=17, fontweight="bold", pad=16)
    plt.tight_layout()
    fig.savefig(CHARTS / "mom_intent_pie_march.png", bbox_inches="tight")
    plt.close()

    # --- HTML ---
    med_ref = k_mar["median_dur"]

    ppc_f = int(feb_core[feb_core["source_group"].isin(["Google Ads", "Facebook / Meta"])].shape[0])
    ppc_m = int(mar_core[mar_core["source_group"].isin(["Google Ads", "Facebook / Meta"])].shape[0])
    br_f = int(feb_core[feb_core["source_group"].isin(["Direct", "GMB / Google My Business"])].shape[0])
    br_m = int(mar_core[mar_core["source_group"].isin(["Direct", "GMB / Google My Business"])].shape[0])

    silent_f = int(feb_b.get("OPERATIONAL_SILENT_DEAD_AIR", 0))
    silent_m = int(mar_b.get("OPERATIONAL_SILENT_DEAD_AIR", 0))
    wf_f = int(feb_b.get("WRONG_FIRM_ADMIN_EXISTING", 0))
    wf_m = int(mar_b.get("WRONG_FIRM_ADMIN_EXISTING", 0))
    npi_f = int(feb_b.get("NON_PI_WRONG_PRACTICE", 0))
    npi_m = int(mar_b.get("NON_PI_WRONG_PRACTICE", 0))
    as_f = int(feb_b.get("ATTORNEY_SWITCH_DROPPED", 0))
    as_m = int(mar_b.get("ATTORNEY_SWITCH_DROPPED", 0))
    med_f = int(feb_b.get("MEDICAL_SYMPTOM_CONFUSION", 0))
    med_m = int(mar_b.get("MEDICAL_SYMPTOM_CONFUSION", 0))
    unc_f = int(feb_b.get("UNCLEAR_UNTAGGED", 0))
    unc_m = int(mar_b.get("UNCLEAR_UNTAGGED", 0))

    disclaimer_p1 = (
        "This report was produced with assistance from Cursor AI. Figures and modeled categories may contain errors and should be validated before operational use. "
        "February labeling is uneven in places; March is partial-month; projections use simple pacing."
    )
    disclaimer_p2 = (
        "Tags and modeled buckets summarize call content; they do not replace a formal intake QA program or a legal determination of a “lead.”"
    )
    disclaimer_p3 = (
        "Only calls marked “Qualified Lead” carry value-back scoring to Google. That marking reflects operator judgment from listening to recordings — not an independent measure of lead quality. "
        "Other calls remain in the default CallRail disposition until explicitly marked."
    )

    what_changed_table = f"""
<table class="change-grid">
<thead><tr><th>Area</th><th>February (export)</th><th>March (through 3/20)</th><th>Notes</th></tr></thead>
<tbody>
<tr><td>Total calls</td><td>{k_feb["n"]:,}</td><td>{k_mar["n"]:,}</td><td>March window is partial; per-day pace is higher.</td></tr>
<tr><td>Share marked qualified (value-back)</td><td>{k_feb["ql_rate"]:.1f}%</td><td>{k_mar["ql_rate"]:.1f}%</td><td>Qualified marks are applied only where value-back is intended.</td></tr>
<tr><td>Default disposition (unmarked)</td><td>{k_feb["not_scored"]:,}</td><td>{k_mar["not_scored"]:,}</td><td>Calls without a value-back mark; not an intake quality score.</td></tr>
<tr><td>Google Ads + Meta</td><td>{ppc_f}</td><td>{ppc_m}</td><td>Paid search / paid social.</td></tr>
<tr><td>Direct + GMB</td><td>{br_f}</td><td>{br_m}</td><td>Brand-leaning and direct-line sources.</td></tr>
<tr><td>Modeled: silent / abandon / connect</td><td>{silent_f}</td><td>{silent_m}</td><td>Tag-based; directional.</td></tr>
<tr><td>Modeled: wrong firm / admin / existing</td><td>{wf_f}</td><td>{wf_m}</td><td>—</td></tr>
<tr><td>Modeled: non-PI wrong practice</td><td>{npi_f}</td><td>{npi_m}</td><td>—</td></tr>
<tr><td>Modeled: attorney-switch signals</td><td>{as_f}</td><td>{as_m}</td><td>—</td></tr>
<tr><td>Modeled: symptom confusion</td><td>{med_f}</td><td>{med_m}</td><td>—</td></tr>
<tr><td>Modeled: unclear / untagged</td><td>{unc_f}</td><td>{unc_m}</td><td>—</td></tr>
</tbody>
</table>
"""

    html_out = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>February vs March 2026 — Intake & Call Quality</title>
<style>
:root {{
  --text: #1c1917;
  --muted: #57534e;
  --line: #e7e5e4;
  --accent: #0f766e;
  --bg: #fafaf9;
  --card: #ffffff;
}}
* {{ box-sizing: border-box; }}
body {{
  font-family: "Segoe UI", system-ui, -apple-system, sans-serif;
  color: var(--text);
  background: var(--bg);
  line-height: 1.6;
  margin: 0;
  padding: 0 1.25rem 3rem;
}}
.wrap {{ max-width: 980px; margin: 0 auto; }}
header {{
  padding: 2rem 0 1rem;
  border-bottom: 4px solid var(--accent);
}}
h1 {{ font-size: 1.85rem; margin: 0 0 0.5rem; font-weight: 700; }}
.sub {{ color: var(--muted); font-size: 1.05rem; max-width: 52rem; }}
.disclaimer {{
  background: #fef3c7;
  border: 1px solid #fcd34d;
  border-radius: 8px;
  padding: 1rem 1.25rem;
  margin: 1.25rem 0 1.75rem;
  font-size: 0.95rem;
  color: #44403c;
}}
.disclaimer p {{ margin: 0.35rem 0; }}
.disclaimer p:first-child {{ margin-top: 0; }}
.disclaimer p:last-child {{ margin-bottom: 0; }}
.change-grid {{ width: 100%; border-collapse: collapse; font-size: 0.88rem; margin: 1rem 0; background: var(--card); box-shadow: 0 1px 3px rgba(0,0,0,.06); }}
.change-grid th, .change-grid td {{ border: 1px solid var(--line); padding: 0.5rem 0.6rem; text-align: left; vertical-align: top; }}
.change-grid th {{ background: #ecfdf5; font-weight: 600; }}
.change-grid tr:nth-child(even) td {{ background: #fafaf9; }}
.takeaway-line {{ font-size: 0.95rem; color: var(--muted); margin: 0.4rem 0 1rem; max-width: 48rem; }}
h2 {{
  font-size: 1.35rem;
  color: var(--accent);
  margin-top: 2.25rem;
  margin-bottom: 0.75rem;
  font-weight: 700;
}}
h3 {{ font-size: 1.1rem; margin-top: 1.5rem; color: #44403c; }}
table.data {{
  width: 100%;
  border-collapse: collapse;
  font-size: 0.92rem;
  background: var(--card);
  box-shadow: 0 1px 3px rgba(0,0,0,.08);
  margin: 1rem 0;
}}
table.data th, table.data td {{
  border: 1px solid var(--line);
  padding: 0.55rem 0.65rem;
  text-align: left;
}}
table.data th {{ background: #f5f5f4; font-weight: 600; }}
.def {{
  background: #fffbeb;
  border-left: 4px solid #f59e0b;
  padding: 1rem 1.25rem;
  margin: 1rem 0;
  border-radius: 0 8px 8px 0;
}}
.def strong {{ color: #92400e; }}
.figure {{ margin: 1.5rem 0; text-align: center; }}
.figure img {{ max-width: 100%; height: auto; border: 1px solid var(--line); border-radius: 6px; background: #fff; }}
.figure p {{ font-size: 0.88rem; color: var(--muted); margin-top: 0.5rem; }}
ul.tight li {{ margin: 0.35rem 0; }}
.takeaway {{
  font-size: 1.05rem;
  line-height: 1.75;
  background: linear-gradient(180deg, #f8fafc 0%, #fff 100%);
  border: 1px solid var(--line);
  border-radius: 10px;
  padding: 1.5rem 1.75rem;
  margin-top: 2rem;
}}
footer.note {{ font-size: 0.85rem; color: var(--muted); margin-top: 2rem; }}
</style>
</head>
<body>
<div class="wrap">
<header>
  <h1>February vs March 2026 — Intake &amp; Call Quality</h1>
  <p class="sub">March figures are <strong>through March 20, 2026</strong>. February reflects the <strong>export window in file</strong> (starts Feb 2 in this dataset). 
  March month-end projection scales the first 20 days to 31 days — illustrative only.</p>
</header>

<div class="disclaimer">
  <p>{html.escape(disclaimer_p1)}</p>
  <p>{html.escape(disclaimer_p2)}</p>
  <p>{html.escape(disclaimer_p3)}</p>
</div>

<h2>Definitions</h2>
<div class="def">
<p><strong>Qualified Lead (value-back)</strong> — Calls marked in CallRail for Google value-back. In this workflow, value and scoring are applied only there, based on operator judgment from listening to recordings.</p>
<p><strong>True PI (modeled)</strong> — A coarse category from tags and status for reporting; not equivalent to a retained case or a firm’s intake definition.</p>
<p><strong>Default disposition (unmarked)</strong> — Calls without a value-back mark; neutral administrative state.</p>
<p><strong>Silent / abandon / connect (modeled)</strong> — Hang-ups, abandoned rings, silent or dead-air tags, or very short connects — grouped for volume analysis, not as a critique of intake staff.</p>
</div>

<h2>Snapshot KPIs</h2>
<table class="data">
<thead><tr><th>Metric</th><th>Feb actual</th><th>Mar through 3/20</th><th>Mar projected (full month)</th><th>Mar vs Feb</th></tr></thead>
<tbody>
<tr><td>Total calls (excl. internal tests)</td><td>{k_feb["n"]:,}</td><td>{k_mar["n"]:,}</td><td>{k_mar_proj["n"]:,}</td><td>{pct_change(k_mar["n"], k_feb["n"])} (actual)</td></tr>
<tr><td>Answered</td><td>{k_feb["answered"]:,}</td><td>{k_mar["answered"]:,}</td><td>{k_mar_proj["answered"]:,}</td><td>—</td></tr>
<tr><td>Abandoned</td><td>{k_feb["abandoned"]:,}</td><td>{k_mar["abandoned"]:,}</td><td>{k_mar_proj["abandoned"]:,}</td><td>—</td></tr>
<tr><td>First-time callers (flagged)</td><td>{k_feb["first_time"]:,}</td><td>{k_mar["first_time"]:,}</td><td>{int(round(k_mar_proj["first_time"])):,}</td><td>—</td></tr>
<tr><td>Average duration</td><td>{fmt_duration_human(int(round(k_feb["avg_dur"])))}</td><td>{fmt_duration_human(int(round(k_mar["avg_dur"])))}</td><td>—</td><td>—</td></tr>
<tr><td>Median duration</td><td>{fmt_duration_human(int(round(k_feb["median_dur"])))}</td><td>{fmt_duration_human(int(round(k_mar["median_dur"])))}</td><td>—</td><td>—</td></tr>
<tr><td>Calls with tags</td><td>{k_feb["tagged"]:,}</td><td>{k_mar["tagged"]:,}</td><td>{int(round(k_mar_proj["tagged"])):,}</td><td>—</td></tr>
<tr><td>Marked qualified (Google value-back)</td><td>{k_feb["qualified_leads"]:,}</td><td>{k_mar["qualified_leads"]:,}</td><td>{int(round(k_mar_proj["qualified_leads"])):,}</td><td>—</td></tr>
<tr><td>Default disposition (unmarked)</td><td>{k_feb["not_scored"]:,}</td><td>{k_mar["not_scored"]:,}</td><td>{int(round(k_mar_proj["not_scored"])):,}</td><td>{pct_change(k_mar["not_scored"], k_feb["not_scored"])} (actual)</td></tr>
<tr><td>Share marked qualified (value-back)</td><td>{k_feb["ql_rate"]:.1f}%</td><td>{k_mar["ql_rate"]:.1f}%</td><td>{k_mar["ql_rate"]:.1f}%</td><td>{pct_change(k_mar["ql_rate"], k_feb["ql_rate"])}</td></tr>
<tr><td>True PI (modeled count)</td><td>{k_feb["true_pi_est"]:,}</td><td>{k_mar["true_pi_est"]:,}</td><td>{k_mar_proj["true_pi_est"]:,}</td><td>{pct_change(k_mar["true_pi_est"], k_feb["true_pi_est"])}</td></tr>
<tr><td>Internal tests (raw)</td><td>{int(feb_int)}</td><td>{int(mar_int)}</td><td>—</td><td>—</td></tr>
</tbody>
</table>
<p style="font-size:0.9rem;color:var(--muted)">February export begins <strong>Feb 2</strong> in this file (no Feb 1 rows). March projection assumes March pace stays flat — see projection commentary.</p>

<h2>What changed (February → March)</h2>
{what_changed_table}
<p class="takeaway-line"><strong>Reading the table:</strong> Volume and source mix describe demand; value-back share describes how much traffic carries a Google value signal; modeled rows summarize tag language for trend spotting.</p>

<h2>Source mix</h2>
<p class="takeaway-line">Where the phones rang. Compare shape, not just height — one source can add volume without adding cases.</p>
<div class="figure"><img src="charts/mom_source_comparison.png" alt="Source comparison"/></div>

<h2>Strategic buckets — February vs March</h2>
<p class="takeaway-line">Same bucket logic both months. Good for direction; not a substitute for listening to calls.</p>
<div class="figure"><img src="charts/mom_bucket_comparison.png" alt="Buckets"/></div>

<h2>March call intent mix</h2>
<p class="takeaway-line">Share of March (through 3/20). Labels and percentages appear on the chart (slices under ~2.5% may omit the percentage to reduce clutter).</p>
<div class="figure"><img src="charts/mom_intent_pie_march.png" alt="Intent pie"/></div>

<h2>Call length by bucket</h2>
<p class="takeaway-line">Same duration buckets for both months (wider spans than raw seconds so the distribution is easier to read side by side). 
Median March handle time: <strong>{fmt_duration_human(int(round(k_mar["median_dur"])))}</strong> ({duration_comment(int(k_mar["median_dur"]), med_ref)})</p>
<div class="figure"><img src="charts/mom_duration_buckets.png" alt="Duration buckets"/></div>

<h2>When calls arrive — March heatmap</h2>
<p class="takeaway-line">Local time (12-hour labels). Warmer cells indicate higher call counts by weekday and hour.</p>
<div class="figure"><img src="charts/mom_heatmap_march.png" alt="Heatmap"/></div>

<h2>Source × bucket — March</h2>
<p class="takeaway-line">Greens = opportunity-style modeled buckets; oranges = non-PI / wrong firm &amp; admin; purples = vendor, silent/abandon/connect, unclear. Legend matches the chart.</p>
<div class="figure"><img src="charts/mom_source_bucket_stacked.png" alt="Stacked"/></div>

<h2>Projection commentary (March month-end)</h2>
<ul class="tight">
<li><strong>Projected March calls (excl. tests):</strong> ~{k_mar_proj["n"]:,} if the first 20 days were representative of the full month.</li>
<li><strong>Caveat:</strong> Pace can land high or low depending on weekends, budget caps, or campaign changes in the final weeks.</li>
<li><strong>Volume vs. story:</strong> call counts can rise while tag mix and value-back marks tell a separate story.</li>
</ul>

<h2>Operational context</h2>
<ul class="tight">
<li>Connect timing and routing interact with marketing volume; heatmaps help align staffing with observed peaks.</li>
<li>Complex or non-standard inquiries may benefit from consistent triage scripts as part of normal scaling.</li>
</ul>

<h2>Practical notes</h2>
<p class="takeaway-line">These are optional ideas — not a checklist. They’re the kind of things many PI firms look at as volume grows; use what fits.</p>
<ul class="tight">
<li><strong>Value-back marking:</strong> Keeping a simple, consistent idea of which calls get a Google value-back mark tends to make month-to-month reporting easier to read.</li>
<li><strong>First few seconds:</strong> A short, steady opening (who the firm is, what they handle, where) usually helps callers get oriented.</li>
<li><strong>Busy hours:</strong> The heatmap is a rough guide — if staffing lines up with the peaks, fewer calls hit voicemail or hold.</li>
<li><strong>Vendor / admin traffic:</strong> When that volume is noticeable, a separate path or number for it can keep case lines open for injury calls.</li>
<li><strong>Paid search:</strong> Glancing at search terms and negatives every so often is often enough to catch drift before spend does.</li>
<li><strong>Sanity checks:</strong> Occasionally comparing marked-qualified counts to what actually signed in the firm can help calibrate — no pressure to do it every week.</li>
</ul>

<div class="takeaway">
<h2 style="margin-top:0;border:none;">Summary</h2>
<p>The comparison is intended as a neutral snapshot: volume and source mix, selective value-back marking, and a coarse tag-based view of call themes. 
March shows higher daily pace than the February export window; the appropriate response is firm-specific. 
Operational improvements (routing, coverage, triage consistency) often scale alongside media spend.</p>
<p style="margin-bottom:0;font-size:0.95rem;"><strong>Reminder:</strong> See the disclaimer box — AI-assisted, contextual tags, partial March, February export starts Feb 2; local time on heatmap; internal tests excluded from main KPI rows.</p>
</div>

<footer class="note">
Source files: {html.escape(PATH_FEB.name)} (February window) · {html.escape(PATH_MAR.name)} (March through 3/20).
</footer>
</div>
</body>
</html>"""

    out_path = ROOT / "index.html"
    out_path.write_text(html_out, encoding="utf-8")
    alt = OUT / "MOM_REPORT_FEB_MAR_2026.html"
    alt.write_text(html_out, encoding="utf-8")
    print(f"Wrote {out_path}")
    print(f"Wrote {alt}")


if __name__ == "__main__":
    main()
