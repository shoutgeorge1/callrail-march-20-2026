# -*- coding: utf-8 -*-
"""
Call Rail intake performance: clean, classify, visualize, export Markdown report.
"""
from __future__ import annotations

import re
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import pandas as pd
import seaborn as sns

# --- Paths ---
ROOT = Path(__file__).resolve().parent
DATA = Path(r"c:\Users\georgea\Downloads\Call List-2026-03-20 (1).xlsx")
OUT = ROOT / "report_output"
CHARTS = OUT / "charts"
OUT.mkdir(parents=True, exist_ok=True)
CHARTS.mkdir(parents=True, exist_ok=True)

# Paid / high-intent sources for waste/opportunity context
PAID_SOURCES = {
    "google ads",
    "facebook",
    "yelp",
    "google my business",
    "gmb (+ multi use tracking number)",
    "aa website & blog",
    "website",
    "landing: 844-inside-5",
    "brand / organic – static (844-467-4335)",
    "www.insideraccidentlawyers.com",
    "apex chat",
    "instagram",
    "super lawyers",
    "super lawyers - los angeles",
    "bing organic",
}


def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    d.columns = [c.strip() for c in d.columns]
    d["Start Time"] = pd.to_datetime(d["Start Time"], errors="coerce")
    d["Duration (seconds)"] = pd.to_numeric(d["Duration (seconds)"], errors="coerce").fillna(0).astype(int)
    d["Tags"] = d["Tags"].fillna("").astype(str).str.strip()
    d["Source"] = d["Source"].fillna("Unknown").astype(str).str.strip()
    d["Qualified"] = d["Qualified"].fillna("Not Scored").astype(str).str.strip()
    d["Call Status"] = d["Call Status"].fillna("Unknown").astype(str).str.strip()
    d["Medium"] = d["Medium"].fillna("").astype(str).str.strip()
    d["Number Name"] = d["Number Name"].fillna("").astype(str).str.strip()
    d["Phone Number"] = d["Phone Number"].fillna("").astype(str).str.strip()
    return d


def _norm_text(s: str) -> str:
    return s.lower().replace("–", "-").replace("—", "-")


def classify_bucket(row: pd.Series) -> str:
    """
    Mutually exclusive strategic bucket. Priority: intake failure → internal QA →
    vendor → wrong firm → existing client → PD/non-case → true PI → other.
    """
    status = str(row.get("Call Status", ""))
    tags = _norm_text(str(row.get("Tags", "")))
    qual = _norm_text(str(row.get("Qualified", "")))
    dur = int(row.get("Duration (seconds)", 0) or 0)
    combined = f"{tags} {qual} {status.lower()}"

    def has_any(*words: str) -> bool:
        return any(w in combined for w in words)

    # Intake failure signals
    if status.lower() == "abandoned call":
        return "INTAKE_FAILURE_SIGNALS"
    if has_any(
        "dead air",
        "agent silent",
        "hang up",
        "hang-up",
        "hang up after",
        "4 rings",
        "mo: abandoned",
        "missed calls",
        "hang up -",
        "sec - hang up",
    ):
        return "INTAKE_FAILURE_SIGNALS"
    if dur <= 12 and has_any("hang", "silent", "abandon"):
        return "INTAKE_FAILURE_SIGNALS"

    # Internal QA / test
    if has_any("internal test", "dc: test calls", "test calls", "referral — internal marketing", "internal marketing lead") or tags.strip() in ("test",):
        return "VENDOR_SALES_NON_LEAD"

    # Intake / transfer failure (distinct from generic hang-up)
    if has_any("incomplete intake", "transfer failed", "return call / no inquiry"):
        return "INTAKE_FAILURE_SIGNALS"

    # Vendor / sales / non-lead (incl. co-counsel vendor, collections, marketing)
    if has_any(
        "vendor",
        "vendore",
        "lexisnexis",
        "b2b sales",
        "marketing call",
        "solicitation",
        "referral solicitation",
        "collections (",
        "billing / collections",
        "vendor case status",
        "vendor -",
        "vendor /",
        "vendor –",
        "opposing counsel",
        "attorney call – opposing",
        "lien holder",
        "funding company",
        "existing insurance adjuster",
        "⭐ existing case — insurance adjuster",
        "lien / pre-settlement loan",
        "pre-settlement loan",
    ):
        return "VENDOR_SALES_NON_LEAD"

    # Wrong firm / misdial / brand confusion
    if has_any(
        "wrong firm",
        "misdial",
        "wrong number",
        "wrong practice area",
        "competitor",
        "sweet james",
        "abrams law",
        "jacoby & meyers",
        "wilshire law",
        "lifelaw",
        "wrong firm /",
        "dc - wrong firm",
        "dc – wrong firm",
        "navigation error",
        "closed case paperwork",
        "asking for email",
    ):
        return "WRONG_FIRM_MISDIAL_BRAND"

    # Existing client traffic
    if has_any(
        "existing client",
        "existing customer",
        "cb: existing",
        "attorney inquiry – existing client",
        "existing case",
        "prior case",
        "existing conversation",
        "settlement phase inquiry",
        "case management inquiry",
        "case assignment pending",
        "demand phase",
        "evidence follow-up",
        "existing client — status",
        "existing client – settlement",
        "attorney call — case status",
        "case status / file request",
    ):
        return "EXISTING_CLIENT_TRAFFIC"

    # Referrals / high-intent inquiries before broad "non-PI" sweep
    if has_any(
        "attorney referral inquiry",
        "referral – elder financial abuse",
        "elder financial abuse",
        "caala",
        "not qualified – sexual abuse",
        "sexual abuse",
        "environmental claim inquiry",
        "mass tort",
    ):
        return "TRUE_POTENTIAL_PI_LEADS"

    if has_any("pre-intake — location", "office proximity"):
        return "TRUE_POTENTIAL_PI_LEADS"

    # Property damage / non-case / non-PI civil
    if has_any(
        "auto accident defense",
        "non-pi — at fault",
        "lost job",
        "low income",
        "legal advice seeker",
    ):
        return "PROPERTY_DAMAGE_NON_CASE"

    if has_any(
        "property damage only",
        "pd only",
        "not pi",
        "non-pi",
        "non pi",
        "spanish – not qualified",
        "consumer dispute",
        "lemon law",
        "warranty dispute",
        "small claims",
        "breach of contract",
        "name change",
        "landlord",
        "rental issue",
        "real estate",
        "employment / wrongful",
        "non-pi — employment",
        "criminal defense",
        "tax / irs",
        "workers comp",
        "bankruptcy",
        "probate",
        "family law",
        "divorce",
        "intellectual property",
        "medical malpractice",
        "out of market",
        "out of state",
        "lead not viable",
        "medical emergency / non-pi",
        "medical illness",
        "neuropathy",
        "romance scam",
        "mental health",
        "domestic crisis",
    ):
        return "PROPERTY_DAMAGE_NON_CASE"

    # True PI opportunity signals
    if qual == "qualified lead":
        return "TRUE_POTENTIAL_PI_LEADS"
    if has_any(
        "potential pi",
        "high-intent",
        "high intent pi",
        "🚨 high-intent",
        "fresh injury",
        "mva",
        "motor vehicle",
        "rear-end",
        "rear end",
        "premises liability",
        "slip",
        "trip & fall",
        "dog bite",
        "pedestrian",
        "bicycle",
        "truck accident",
        "catastrophic",
        "nursing home",
        "elder neglect",
        "wrongful death",
        "pi – auto",
        "pi – bicycle",
        "pi – transportation",
        "possible pi",
        "new lead — injury",
        "attorney switch",
        "attorney drop",
        "second opinion",
        "yelp, 👉 potential pi",
        "✅ slip / fall",
        "✅ nursing home",
        "✅ old accident lead",
    ):
        return "TRUE_POTENTIAL_PI_LEADS"

    # Duration-only heuristic: very short answered without qualification
    if status.lower() == "answered call" and dur <= 8:
        return "INTAKE_FAILURE_SIGNALS"

    # Undisposed calls: no tag text — treat ultra-short as friction, else leave as gap
    tag_clean = re.sub(r"\s+", " ", tags.replace("nan", "")).strip()
    if not tag_clean and qual == "not scored" and status.lower() == "answered call":
        if dur <= 12:
            return "INTAKE_FAILURE_SIGNALS"
        return "UNTAGGED_OR_AMBIGUOUS"

    # Remaining: not scored / ambiguous
    return "UNTAGGED_OR_AMBIGUOUS"


def source_is_paid(source: str) -> bool:
    s = _norm_text(source)
    return any(p in s for p in PAID_SOURCES) or "google" in s or "ppc" in s or "cpc" in s


def setup_style():
    sns.set_theme(style="whitegrid", context="talk", font_scale=0.85)
    plt.rcParams["font.family"] = "DejaVu Sans"
    plt.rcParams["figure.dpi"] = 120
    plt.rcParams["savefig.facecolor"] = "#ffffff"
    plt.rcParams["axes.facecolor"] = "#fafafa"
    plt.rcParams["axes.edgecolor"] = "#cccccc"
    plt.rcParams["figure.facecolor"] = "#ffffff"


COLORS = {
    "TRUE_POTENTIAL_PI_LEADS": "#1a5f4a",
    "EXISTING_CLIENT_TRAFFIC": "#2e6dad",
    "VENDOR_SALES_NON_LEAD": "#8b5a6b",
    "WRONG_FIRM_MISDIAL_BRAND": "#c45c3e",
    "INTAKE_FAILURE_SIGNALS": "#b8860b",
    "PROPERTY_DAMAGE_NON_CASE": "#6b6b6b",
    "UNTAGGED_OR_AMBIGUOUS": "#9aa0a6",
}

LABEL_ORDER = [
    "TRUE_POTENTIAL_PI_LEADS",
    "EXISTING_CLIENT_TRAFFIC",
    "VENDOR_SALES_NON_LEAD",
    "WRONG_FIRM_MISDIAL_BRAND",
    "INTAKE_FAILURE_SIGNALS",
    "PROPERTY_DAMAGE_NON_CASE",
    "UNTAGGED_OR_AMBIGUOUS",
]

HUMAN = {
    "TRUE_POTENTIAL_PI_LEADS": "True Potential PI Leads",
    "EXISTING_CLIENT_TRAFFIC": "Existing Client Traffic",
    "VENDOR_SALES_NON_LEAD": "Vendor / Sales / Non-Lead",
    "WRONG_FIRM_MISDIAL_BRAND": "Wrong Firm / Misdial / Brand Confusion",
    "INTAKE_FAILURE_SIGNALS": "Intake Failure Signals",
    "PROPERTY_DAMAGE_NON_CASE": "Property Damage / Non-Case",
    "UNTAGGED_OR_AMBIGUOUS": "Untagged / Ambiguous",
}


def main():
    setup_style()
    raw = pd.read_excel(DATA, sheet_name="Calls")
    df = normalize_df(raw)
    df["Bucket"] = df.apply(classify_bucket, axis=1)
    df["Hour"] = df["Start Time"].dt.hour
    df["Weekday"] = df["Start Time"].dt.day_name()
    df["Paid_Source"] = df["Source"].apply(source_is_paid)

    n = len(df)
    counts = df["Bucket"].value_counts().reindex(LABEL_ORDER, fill_value=0)
    pct = (counts / n * 100).round(2)

    # Core KPIs
    true_pi = counts.get("TRUE_POTENTIAL_PI_LEADS", 0)
    existing = counts.get("EXISTING_CLIENT_TRAFFIC", 0)
    vendor = counts.get("VENDOR_SALES_NON_LEAD", 0)
    wrong = counts.get("WRONG_FIRM_MISDIAL_BRAND", 0)
    intake_fail = counts.get("INTAKE_FAILURE_SIGNALS", 0)
    pd_non = counts.get("PROPERTY_DAMAGE_NON_CASE", 0)
    ambiguous = counts.get("UNTAGGED_OR_AMBIGUOUS", 0)

    waste_num = vendor + wrong + pd_non
    marketing_waste_pct = round(waste_num / n * 100, 2)
    opportunity_pct = round(true_pi / n * 100, 2)
    existing_pct = round(existing / n * 100, 2)
    intake_friction_pct = round(intake_fail / n * 100, 2)
    answered = (df["Call Status"] == "Answered Call").sum()
    abandoned_rate = round((df["Call Status"] == "Abandoned Call").sum() / n * 100, 2)

    top_hours = df.groupby("Hour").size().sort_values(ascending=False).head(3)
    peak_windows = ", ".join([f"{int(h)}:00–{int(h) + 1}:00 ({int(c)} calls)" for h, c in top_hours.items()])

    # Funnel chart
    fig, ax = plt.subplots(figsize=(10, 5.8))
    stages = ["Total Calls", "Answered", "Qualified Lead (CallRail)", "True PI (modeled bucket)"]
    ql_raw = (df["Qualified"] == "Qualified Lead").sum()
    answered_n = answered
    vals = [n, answered_n, ql_raw, int(true_pi)]
    y_pos = np.arange(len(stages))
    colors_bar = ["#2c3e50", "#34495e", "#1a5f4a", "#1a7f62"]
    bars = ax.barh(y_pos, vals, color=colors_bar, height=0.55, edgecolor="white", linewidth=1.2)
    ax.set_yticks(y_pos)
    ax.set_yticklabels(stages)
    ax.invert_yaxis()
    ax.set_xlabel("Volume")
    ax.set_title("Intake Funnel — Total Calls → True Opportunity", fontsize=14, fontweight="bold", pad=12)
    for b, v in zip(bars, vals):
        ax.text(v + max(vals) * 0.01, b.get_y() + b.get_height() / 2, f"{v:,}", va="center", fontsize=11)
    ax.set_xlim(0, max(vals) * 1.22)
    plt.tight_layout()
    fig.savefig(CHARTS / "funnel.png", bbox_inches="tight")
    plt.close()

    # Pie
    fig, ax = plt.subplots(figsize=(9.5, 9))
    pie_labels = [HUMAN[k] for k in LABEL_ORDER if counts[k] > 0]
    pie_vals = [counts[k] for k in LABEL_ORDER if counts[k] > 0]
    pie_colors = [COLORS[k] for k in LABEL_ORDER if counts[k] > 0]
    wedges, texts, autotexts = ax.pie(
        pie_vals,
        labels=pie_labels,
        autopct=lambda p: f"{p:.1f}%\n({int(p * n / 100)})",
        colors=pie_colors,
        pctdistance=0.72,
        textprops={"fontsize": 9},
        wedgeprops={"linewidth": 1, "edgecolor": "white"},
    )
    ax.set_title("Call Intent Distribution (Strategic Buckets)", fontsize=14, fontweight="bold", pad=16)
    plt.tight_layout()
    fig.savefig(CHARTS / "pie_distribution.png", bbox_inches="tight")
    plt.close()

    # Histogram duration
    fig, ax = plt.subplots(figsize=(10, 5.5))
    d = df["Duration (seconds)"].clip(upper=600)
    ax.hist(d, bins=48, color="#2c5f7c", edgecolor="white", linewidth=0.6)
    ax.axvline(df["Duration (seconds)"].median(), color="#c45c3e", ls="--", lw=2, label=f"Median {df['Duration (seconds)'].median():.0f}s")
    ax.set_xlabel("Duration (seconds)")
    ax.set_ylabel("Calls")
    ax.set_title("Call Duration Distribution (capped at 600s for display)", fontsize=13, fontweight="bold")
    ax.legend()
    plt.tight_layout()
    fig.savefig(CHARTS / "duration_histogram.png", bbox_inches="tight")
    plt.close()

    # Heatmap hour x weekday
    heat = df.pivot_table(index="Weekday", columns="Hour", values="Call Status", aggfunc="count", fill_value=0)
    day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    heat = heat.reindex([d for d in day_order if d in heat.index])
    fig, ax = plt.subplots(figsize=(12, 4.8))
    sns.heatmap(heat, cmap="YlOrRd", ax=ax, cbar_kws={"label": "Calls"})
    ax.set_title("Call Volume Heatmap — Hour × Weekday", fontsize=13, fontweight="bold")
    ax.set_xlabel("Hour of day (local)")
    plt.tight_layout()
    fig.savefig(CHARTS / "heatmap_hour_weekday.png", bbox_inches="tight")
    plt.close()

    # Hour-only heatmap (1 row) for "time-of-day clusters"
    by_hour = df.groupby("Hour").size().reindex(range(24), fill_value=0).to_frame("calls")
    fig, ax = plt.subplots(figsize=(12, 2.4))
    sns.heatmap(by_hour.T, cmap="Blues", ax=ax, cbar_kws={"label": "Calls"})
    ax.set_yticks([])
    ax.set_xlabel("Hour of day")
    ax.set_title("Calls by Hour — Opportunity Clusters", fontsize=12, fontweight="bold")
    plt.tight_layout()
    fig.savefig(CHARTS / "heatmap_hour.png", bbox_inches="tight")
    plt.close()

    # Stacked bar by source (top 12 sources)
    top_sources = df["Source"].value_counts().head(12).index
    sub = df[df["Source"].isin(top_sources)]
    pivot = pd.crosstab(sub["Source"], sub["Bucket"])
    pivot = pivot.reindex(columns=LABEL_ORDER, fill_value=0)
    pivot = pivot.loc[top_sources]

    fig, ax = plt.subplots(figsize=(11, 6.2))
    left = np.zeros(len(pivot))
    for col in LABEL_ORDER:
        if col not in pivot.columns:
            continue
        vals = pivot[col].values
        ax.barh(pivot.index, vals, left=left, color=COLORS[col], label=HUMAN[col], height=0.65)
        left = left + vals
    ax.set_xlabel("Calls")
    ax.set_title("Stacked Volume by Source × Strategic Bucket (Top 12 Sources)", fontsize=12, fontweight="bold")
    ax.legend(bbox_to_anchor=(1.02, 1), loc="upper left", fontsize=8)
    plt.tight_layout()
    fig.savefig(CHARTS / "stacked_by_source.png", bbox_inches="tight")
    plt.close()

    # Source quality table
    rows = []
    for src in df["Source"].value_counts().index[:18]:
        sdf = df[df["Source"] == src]
        tn = len(sdf)
        pi_rate = (sdf["Bucket"] == "TRUE_POTENTIAL_PI_LEADS").mean() * 100
        waste_rate = (sdf["Bucket"].isin(["VENDOR_SALES_NON_LEAD", "WRONG_FIRM_MISDIAL_BRAND", "PROPERTY_DAMAGE_NON_CASE"])).mean() * 100
        fail_rate = (sdf["Bucket"] == "INTAKE_FAILURE_SIGNALS").mean() * 100
        rows.append(
            {
                "Source": src,
                "Calls": tn,
                "True PI %": round(pi_rate, 1),
                "Noise+PD %": round(waste_rate, 1),
                "Intake Fail %": round(fail_rate, 1),
            }
        )
    qual_df = pd.DataFrame(rows)

    # Summary stats table
    summary_rows = [
        ("Total calls", n),
        ("Answered calls", int(answered)),
        ("Abandoned calls", int((df["Call Status"] == "Abandoned Call").sum())),
        ("Abandon rate (of total)", f"{abandoned_rate}%"),
        ("Median duration (sec)", float(df["Duration (seconds)"].median())),
        ("Mean duration (sec)", round(df["Duration (seconds)"].mean(), 1)),
        ("True PI (modeled) %", f"{opportunity_pct}%"),
        ("Existing client load %", f"{existing_pct}%"),
        ("Estimated marketing noise %", f"{marketing_waste_pct}%"),
        ("Intake failure / friction %", f"{intake_friction_pct}%"),
        ("Untagged / ambiguous %", f"{round(ambiguous/n*100,2)}%"),
        ("Top volume windows (local time)", peak_windows),
        ("CallRail Qualified Lead (raw)", int((df["Qualified"] == "Qualified Lead").sum())),
    ]

    def md_table(headers, data):
        lines = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
        for row in data:
            lines.append("| " + " | ".join(str(x) for x in row) + " |")
        return "\n".join(lines)

    md_counts = [["Strategic bucket", "Calls", "% of total"]]
    for k in LABEL_ORDER:
        md_counts.append([HUMAN[k], int(counts[k]), f"{pct[k]:.2f}%"])

    # Write markdown
    md = []
    md.append("# Executive & Tactical Intake Performance Report")
    md.append("")
    md.append("**Prepared for:** Personal Injury — High-Spend Acquisition Program  ")
    md.append("**Dataset:** Call Rail export — `Call List-2026-03-20 (1).xlsx`  ")
    md.append(f"**Scope:** {n:,} calls · **Generated:** automated classification + QA review recommended for edge cases")
    md.append("")
    md.append("---")
    md.append("")
    md.append("## Snapshot KPIs")
    md.append("")
    md.append(md_table(["Metric", "Value"], summary_rows))
    md.append("")
    md.append("**Definitions (operational):**")
    md.append("")
    md.append("- **True Potential PI Leads** — modeled from CallRail `Qualified`, tags (e.g., MVA, premises, high-intent), and attorney-switch intent; excludes obvious wrong-firm/vendor/non-PI.")
    md.append("- **Estimated marketing noise %** — share of calls that are **vendor/sales**, **wrong firm/misdial/brand confusion**, or **property damage / non-case** (non-actionable acquisition demand).")
    md.append("- **Intake failure signals** — abandoned calls plus tag/duration patterns for dead air, hang-ups, and ultra-short connects.")
    md.append("")
    md.append("---")
    md.append("")
    md.append("## Strategic Bucket Mix")
    md.append("")
    md.append(md_table(md_counts[0], md_counts[1:]))
    md.append("")
    md.append("### Figure — Funnel")
    md.append("![Funnel](charts/funnel.png)")
    md.append("")
    md.append("### Figure — Intent distribution")
    md.append("![Pie](charts/pie_distribution.png)")
    md.append("")
    md.append("### Figure — Duration distribution")
    md.append("![Histogram](charts/duration_histogram.png)")
    md.append("")
    md.append("### Figure — Hour × weekday heatmap")
    md.append("![Heatmap weekday](charts/heatmap_hour_weekday.png)")
    md.append("")
    md.append("### Figure — Hourly clusters")
    md.append("![Heatmap hour](charts/heatmap_hour.png)")
    md.append("")
    md.append("### Figure — Source × bucket (stacked)")
    md.append("![Stacked](charts/stacked_by_source.png)")
    md.append("")
    md.append("---")
    md.append("")
    md.append("## Source-Level Quality Indicators")
    md.append("")
    md.append("Higher **True PI %** with lower **Noise+PD %** indicates stronger targeting and routing. Use **Intake Fail %** to separate *media* problems from *answer* problems.")
    md.append("")
    md.append(md_table(list(qual_df.columns), qual_df.values.tolist()))
    md.append("")
    md.append("---")
    md.append("")
    md.append("## Insight — Marketing Signal Quality Diagnosis")
    md.append("")
    md.append(f"- **Signal dilution is material:** only **~{opportunity_pct}%** of volume clears the bar as a **true PI opportunity** under this model. The rest is operations, noise, or failure — not incremental case inventory.")
    md.append(f"- **Paid-channel mix** still concentrates in Google Ads and high-intent surfaces; **noise+PD burden** (~{marketing_waste_pct}% of all calls) is the primary lever on **effective CAC** if those calls are still paid.")
    md.append(
        f"- **Tagging discipline** is uneven: **~{round(ambiguous/n*100,1)}%** of calls remain **untagged or ambiguous** (mostly **Not Scored** with **no tag**), which **breaks downstream attribution** and makes **CAC math optimistic**."
    )
    md.append("")
    md.append("---")
    md.append("")
    md.append("## Insight — Intake Conversion Risk Analysis")
    md.append("")
    md.append(f"- **Abandonment and micro-duration** patterns (**{intake_friction_pct}%** bucketed as intake failure signals) directly tax **conversion from ring to retained consult** — often fixable with staffing, IVR, and speed-to-answer.")
    md.append("- **Silent / dead air / hang-up tags** in the same bucket imply **agent-side** failure modes — not just marketing.")
    md.append("- **Gap:** `Qualified Lead` count in CallRail vs modeled **True PI** may diverge; reconcile weekly to avoid **false confidence** in funnel reporting.")
    md.append("")
    md.append("---")
    md.append("")
    md.append("## Insight — Brand Positioning Leakage")
    md.append("")
    md.append("- **Wrong firm / competitor / misdial** volume is a **brand and geo-targeting tax** — it consumes capacity and skews quality signals in Google and LSA.")
    md.append("- **Multi-brand tracking numbers** (GMB + multi-use pools) will aggregate **mixed intent**; segment reporting by number pool and campaign to avoid blending **brand** with **performance**.")
    md.append("")
    md.append("---")
    md.append("")
    md.append("## Insight — Operational Bottlenecks")
    md.append("")
    md.append(f"- **Existing client load (~{existing_pct}% of calls)** competes with **net-new PI** on the same lines — without a **dedicated client-care queue**, **speed-to-lead** for new matters suffers.")
    md.append("- **Vendor/medical** calls are **high-volume** in tags — treat as **routing**, not **sales**; they inflate handle time and mask agent availability.")
    md.append(f"- **Peak windows:** {peak_windows} — align **headcount + overflow** to these intervals first; **understaffed peaks** amplify **abandonment** and **silent-failure** rates.")
    md.append("")
    md.append("---")
    md.append("")
    md.append("## Insight — Immediate Revenue Optimization Actions")
    md.append("")
    md.append("- **Cut effective CAC** by suppressing **non-case** and **wrong-firm** traffic from paid search where query intent matches; reallocate to **high-intent injury** clusters.")
    md.append("- **Lift case volume** by reducing **abandonment** in peak hours (see heatmaps) — **schedule + overflow** to match **call clusters**.")
    md.append("- **Increase signed cases per 100 calls** by forcing **disposition + tag** on every call — **Not Scored** is a **blind spot** in optimization.")
    md.append("")
    md.append("---")
    md.append("")
    md.append("## Final Executive Summary")
    md.append("")
    md.append(
        f"This line is **not a marketing funnel** — it is a **demand blender**. Out of **{n:,}** calls, **{opportunity_pct}%** present as **true PI opportunity** under a conservative tag+qualification model. "
        f"Roughly **{marketing_waste_pct}%** of volume is **non-actionable acquisition noise** (vendor, wrong firm, PD/non-case), before accounting for **existing client** load (**{existing_pct}%**) and **intake failure** (**{intake_friction_pct}%**). "
        "**Brutal truth:** if you are buying clicks on the same keywords as **general legal intent** and **brand confusion**, you are **taxing CAC** with conversations that will never become cases. "
        "**Strategic mandate:** separate **new client acquisition** from **client service** and **vendor** traffic, **staff peak hours** where heatmaps show clustering, and **close the loop** on **Not Scored** dispositions so media and intake stop flying blind. "
        "**Case volume growth** is not primarily a **more spend** problem — it is a **higher quality call + faster capture** problem."
    )
    md.append("")
    md.append("---")
    md.append("")
    md.append("## Action Playbook")
    md.append("")
    md.append("### Intake script upgrades")
    md.append("- **3-second opener** with firm name + PI + geography; **branch** existing clients to **client-care** extension.")
    md.append("- **Mandatory capture:** name, incident date, injuries, hospital/EMS, **at-fault party**, **insurance** — before narrative.")
    md.append("- **Hang-up save:** one **closing question** for silent/dead-air recoveries.")
    md.append("")
    md.append("### Call routing improvements")
    md.append("- **VIP route** for **Spanish + injury** vs **PD-only**; **vendor** numbers to **admin** or **auto-attendant**.")
    md.append("- **After-hours** (APEX/RSN) rules aligned to **heatmap peaks**; **overflow** to **contract intake** for **top sources**.")
    md.append("")
    md.append("### Marketing targeting fixes")
    md.append("- **Negative keywords** for employment, tax, family law, criminal, **competitor names** surfacing in search terms.")
    md.append("- **Campaign-level** negatives for **wrong firm** / **misdial** clusters; **tighten geo** on high-misdial geos.")
    md.append("")
    md.append("### Brand differentiation tactics")
    md.append("- **Unique verbal ID** in ads and landing pages; **distinct local numbers** per **brand** vs **performance** to reduce leakage.")
    md.append("- **LSA/Maps** creative with **firm name + injury** (not generic “lawyers”).")
    md.append("")
    md.append("### Tracking architecture fixes")
    md.append("- **100% disposition** — eliminate persistent **Not Scored** except true system errors.")
    md.append("- **UTM + GCLID** on all landing pages; **source-level** QA in CallRail matching **Google Ads** campaign structure.")
    md.append("- **Weekly reconciliation:** CallRail `Qualified Lead` vs **CRM signed** — **attribution** and **intake** accountability.")
    md.append("")
    md.append("---")
    md.append("")
    md.append("*Classification model: rule-based priority stack; validate with spot-check of `UNTAGGED_OR_AMBIGUOUS` rows.*")
    md.append("")

    out_md = OUT / "INTAKE_PERFORMANCE_REPORT.md"
    out_md.write_text("\n".join(md), encoding="utf-8")
    df.to_csv(OUT / "calls_classified.csv", index=False, encoding="utf-8")

    print(f"Wrote {out_md}")
    print(f"Wrote {OUT / 'calls_classified.csv'}")


if __name__ == "__main__":
    main()
