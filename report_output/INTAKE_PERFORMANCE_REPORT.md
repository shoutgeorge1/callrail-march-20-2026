# Executive & Tactical Intake Performance Report

**Prepared for:** Personal Injury — High-Spend Acquisition Program  
**Dataset:** Call Rail export — `Call List-2026-03-20 (1).xlsx`  
**Scope:** 543 calls · **Generated:** automated classification + QA review recommended for edge cases

---

## Snapshot KPIs

| Metric | Value |
| --- | --- |
| Total calls | 543 |
| Answered calls | 521 |
| Abandoned calls | 22 |
| Abandon rate (of total) | 4.05% |
| Median duration (sec) | 157.0 |
| Mean duration (sec) | 312.7 |
| True PI (modeled) % | 15.84% |
| Existing client load % | 10.68% |
| Estimated marketing noise % | 47.51% |
| Intake failure / friction % | 11.97% |
| Untagged / ambiguous % | 14.0% |
| Top volume windows (local time) | 13:00–14:00 (62 calls), 11:00–12:00 (62 calls), 12:00–13:00 (59 calls) |
| CallRail Qualified Lead (raw) | 65 |

**Definitions (operational):**

- **True Potential PI Leads** — modeled from CallRail `Qualified`, tags (e.g., MVA, premises, high-intent), and attorney-switch intent; excludes obvious wrong-firm/vendor/non-PI.
- **Estimated marketing noise %** — share of calls that are **vendor/sales**, **wrong firm/misdial/brand confusion**, or **property damage / non-case** (non-actionable acquisition demand).
- **Intake failure signals** — abandoned calls plus tag/duration patterns for dead air, hang-ups, and ultra-short connects.

---

## Strategic Bucket Mix

| Strategic bucket | Calls | % of total |
| --- | --- | --- |
| True Potential PI Leads | 86 | 15.84% |
| Existing Client Traffic | 58 | 10.68% |
| Vendor / Sales / Non-Lead | 93 | 17.13% |
| Wrong Firm / Misdial / Brand Confusion | 99 | 18.23% |
| Intake Failure Signals | 65 | 11.97% |
| Property Damage / Non-Case | 66 | 12.15% |
| Untagged / Ambiguous | 76 | 14.00% |

### Figure — Funnel
![Funnel](charts/funnel.png)

### Figure — Intent distribution
![Pie](charts/pie_distribution.png)

### Figure — Duration distribution
![Histogram](charts/duration_histogram.png)

### Figure — Hour × weekday heatmap
![Heatmap weekday](charts/heatmap_hour_weekday.png)

### Figure — Hourly clusters
![Heatmap hour](charts/heatmap_hour.png)

### Figure — Source × bucket (stacked)
![Stacked](charts/stacked_by_source.png)

---

## Source-Level Quality Indicators

Higher **True PI %** with lower **Noise+PD %** indicates stronger targeting and routing. Use **Intake Fail %** to separate *media* problems from *answer* problems.

| Source | Calls | True PI % | Noise+PD % | Intake Fail % |
| --- | --- | --- | --- | --- |
| Google Ads | 231 | 21.6 | 44.6 | 15.6 |
| Direct | 107 | 13.1 | 59.8 | 5.6 |
| GMB (+ Multi Use Tracking Number) | 56 | 21.4 | 44.6 | 10.7 |
| AA Website & Blog | 29 | 0.0 | 44.8 | 10.3 |
| Google My Business | 26 | 3.8 | 61.5 | 7.7 |
| Website | 20 | 0.0 | 40.0 | 10.0 |
| Yelp | 14 | 21.4 | 50.0 | 0.0 |
| Facebook | 12 | 0.0 | 25.0 | 41.7 |
| Legacy Phone Number | 10 | 0.0 | 60.0 | 10.0 |
| APEX CHAT | 9 | 0.0 | 0.0 | 0.0 |
| Google Organic | 8 | 25.0 | 50.0 | 12.5 |
| Landing: 844-Inside-5 | 7 | 42.9 | 14.3 | 14.3 |
| Brand / Organic – Static (844-467-4335) | 3 | 0.0 | 100.0 | 0.0 |
| www.insideraccidentlawyers.com | 2 | 0.0 | 0.0 | 100.0 |
| Apex (after-hour transfers to Carlos) | 2 | 0.0 | 0.0 | 0.0 |
| Super Lawyers - Los Angeles | 2 | 50.0 | 50.0 | 0.0 |
| Bing Organic | 2 | 0.0 | 100.0 | 0.0 |
| Instagram | 1 | 0.0 | 100.0 | 0.0 |

---

## Insight — Marketing Signal Quality Diagnosis

- **Signal dilution is material:** only **~15.84%** of volume clears the bar as a **true PI opportunity** under this model. The rest is operations, noise, or failure — not incremental case inventory.
- **Paid-channel mix** still concentrates in Google Ads and high-intent surfaces; **noise+PD burden** (~47.51% of all calls) is the primary lever on **effective CAC** if those calls are still paid.
- **Tagging discipline** is uneven: **~14.0%** of calls remain **untagged or ambiguous** (mostly **Not Scored** with **no tag**), which **breaks downstream attribution** and makes **CAC math optimistic**.

---

## Insight — Intake Conversion Risk Analysis

- **Abandonment and micro-duration** patterns (**11.97%** bucketed as intake failure signals) directly tax **conversion from ring to retained consult** — often fixable with staffing, IVR, and speed-to-answer.
- **Silent / dead air / hang-up tags** in the same bucket imply **agent-side** failure modes — not just marketing.
- **Gap:** `Qualified Lead` count in CallRail vs modeled **True PI** may diverge; reconcile weekly to avoid **false confidence** in funnel reporting.

---

## Insight — Brand Positioning Leakage

- **Wrong firm / competitor / misdial** volume is a **brand and geo-targeting tax** — it consumes capacity and skews quality signals in Google and LSA.
- **Multi-brand tracking numbers** (GMB + multi-use pools) will aggregate **mixed intent**; segment reporting by number pool and campaign to avoid blending **brand** with **performance**.

---

## Insight — Operational Bottlenecks

- **Existing client load (~10.68% of calls)** competes with **net-new PI** on the same lines — without a **dedicated client-care queue**, **speed-to-lead** for new matters suffers.
- **Vendor/medical** calls are **high-volume** in tags — treat as **routing**, not **sales**; they inflate handle time and mask agent availability.
- **Peak windows:** 13:00–14:00 (62 calls), 11:00–12:00 (62 calls), 12:00–13:00 (59 calls) — align **headcount + overflow** to these intervals first; **understaffed peaks** amplify **abandonment** and **silent-failure** rates.

---

## Insight — Immediate Revenue Optimization Actions

- **Cut effective CAC** by suppressing **non-case** and **wrong-firm** traffic from paid search where query intent matches; reallocate to **high-intent injury** clusters.
- **Lift case volume** by reducing **abandonment** in peak hours (see heatmaps) — **schedule + overflow** to match **call clusters**.
- **Increase signed cases per 100 calls** by forcing **disposition + tag** on every call — **Not Scored** is a **blind spot** in optimization.

---

## Final Executive Summary

This line is **not a marketing funnel** — it is a **demand blender**. Out of **543** calls, **15.84%** present as **true PI opportunity** under a conservative tag+qualification model. Roughly **47.51%** of volume is **non-actionable acquisition noise** (vendor, wrong firm, PD/non-case), before accounting for **existing client** load (**10.68%**) and **intake failure** (**11.97%**). **Brutal truth:** if you are buying clicks on the same keywords as **general legal intent** and **brand confusion**, you are **taxing CAC** with conversations that will never become cases. **Strategic mandate:** separate **new client acquisition** from **client service** and **vendor** traffic, **staff peak hours** where heatmaps show clustering, and **close the loop** on **Not Scored** dispositions so media and intake stop flying blind. **Case volume growth** is not primarily a **more spend** problem — it is a **higher quality call + faster capture** problem.

---

## Action Playbook

### Intake script upgrades
- **3-second opener** with firm name + PI + geography; **branch** existing clients to **client-care** extension.
- **Mandatory capture:** name, incident date, injuries, hospital/EMS, **at-fault party**, **insurance** — before narrative.
- **Hang-up save:** one **closing question** for silent/dead-air recoveries.

### Call routing improvements
- **VIP route** for **Spanish + injury** vs **PD-only**; **vendor** numbers to **admin** or **auto-attendant**.
- **After-hours** (APEX/RSN) rules aligned to **heatmap peaks**; **overflow** to **contract intake** for **top sources**.

### Marketing targeting fixes
- **Negative keywords** for employment, tax, family law, criminal, **competitor names** surfacing in search terms.
- **Campaign-level** negatives for **wrong firm** / **misdial** clusters; **tighten geo** on high-misdial geos.

### Brand differentiation tactics
- **Unique verbal ID** in ads and landing pages; **distinct local numbers** per **brand** vs **performance** to reduce leakage.
- **LSA/Maps** creative with **firm name + injury** (not generic “lawyers”).

### Tracking architecture fixes
- **100% disposition** — eliminate persistent **Not Scored** except true system errors.
- **UTM + GCLID** on all landing pages; **source-level** QA in CallRail matching **Google Ads** campaign structure.
- **Weekly reconciliation:** CallRail `Qualified Lead` vs **CRM signed** — **attribution** and **intake** accountability.

---

*Classification model: rule-based priority stack; validate with spot-check of `UNTAGGED_OR_AMBIGUOUS` rows.*
