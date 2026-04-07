#!/usr/bin/env python3
"""
LeadDocket: all countrywidetriallawyers*.xlsx in Downloads (no merged CSV merge — avoids dupes).
Match to CallRail → lead_callrail_matched_full_FINAL.csv + summary.
"""
from __future__ import annotations

import difflib
import os
import re
from collections import defaultdict
from pathlib import Path

import pandas as pd

from match_leaddocket_callrail import row_contact

ROOT = Path(__file__).resolve().parent
DOWNLOADS = Path(r"c:\Users\georgea\Downloads")
LEAD_CSV_FALLBACK = ROOT / "lead_docket_sample.csv"
CALL_CSV = ROOT / "callrail_export.csv"
CALL_XLSX = ROOT / "callrail_export.xlsx"
CALL_XLSX_FALLBACK = Path(r"c:\Users\georgea\Downloads\Call List-2026-03-27 (2) (1).xlsx")
OUT_CSV = ROOT / "lead_callrail_matched_full_FINAL.csv"
SIGNED_EXPORT = ROOT / "signed_cases_export.csv"
DEFAULT_SIGNED_XLSX = Path(r"c:\Users\georgea\Downloads\Leads (2) (1).xlsx")
OUT_WITH_SIGNED = ROOT / "lead_callrail_matched_with_signed.csv"
SIGNED_ATTRIBUTION_OUT = ROOT / "signed_cases_attribution.csv"


def rating_to_numeric_score(v) -> float:
    """Map star text or numbers to a 1–5 style score for averaging."""
    if pd.isna(v) or str(v).strip() == "":
        return float("nan")
    s = str(v).strip()
    stars = s.count("\u2605") + s.count("*")
    if stars > 0:
        return float(stars)
    x = pd.to_numeric(s, errors="coerce")
    return float(x) if pd.notna(x) else float("nan")


def normalize_person_name(v) -> str:
    if pd.isna(v):
        return ""
    t = str(v).lower().strip()
    t = re.sub(r"[^\w\s]", "", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def phone_clean_val(v) -> str | None:
    if pd.isna(v):
        return None
    d = re.sub(r"\D", "", str(v).strip())
    if len(d) < 10:
        return None
    return d[-10:]


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [
        re.sub(r"_+", "_", re.sub(r"[^a-z0-9]+", "_", str(c).strip().lower())).strip("_")
        for c in df.columns
    ]
    return df


def pick_phone_column(df: pd.DataFrame, label: str) -> str:
    for c in (
        "customer_phone_number",
        "phone_number",
        "caller_phone",
        "phone",
        "mobile",
    ):
        if c in df.columns:
            return c
    for c in df.columns:
        if "tracking" in c:
            continue
        if "customer_phone" in c or "caller" in c:
            return c
        if "phone" in c or "mobile" in c:
            return c
    raise SystemExit(f"Could not find phone column ({label})")


def pick_callrail_fields(df: pd.DataFrame) -> dict:
    """Column names must already be normalized."""
    out = {}
    time_c = None
    for c in ("start_time", "created_at", "call_start_time"):
        if c in df.columns:
            time_c = c
            break
    if time_c is None:
        for c in df.columns:
            if "start" in c and "time" in c:
                time_c = c
                break
    out["time"] = time_c
    out["source"] = "source" if "source" in df.columns else None
    camp = None
    for c in ("campaign", "utm_campaign"):
        if c in df.columns:
            camp = c
            break
    med = "medium" if "medium" in df.columns else None
    out["campaign_col"] = camp
    out["medium_col"] = med
    dur = None
    for c in ("duration_seconds", "duration", "duration_in_seconds"):
        if c in df.columns:
            dur = c
            break
    if dur is None:
        for c in df.columns:
            if c.startswith("duration"):
                dur = c
                break
    out["duration"] = dur
    return out


def load_lead_combined() -> tuple[pd.DataFrame, int]:
    recs: list[dict] = []
    paths = sorted(DOWNLOADS.glob("countrywidetriallawyers*.xlsx"))
    for p in paths:
        sheet = pd.read_excel(p, sheet_name=0)
        cols = [c for c in sheet.columns if not str(c).startswith("_")]
        for _, row in sheet.iterrows():
            c = row_contact(row, cols)
            if not c:
                continue
            pc = phone_clean_val(c["phone"])
            if not pc:
                continue
            em = c.get("email")
            if em is None or (isinstance(em, float) and pd.isna(em)):
                em = ""
            else:
                em = str(em).strip()
            recs.append({"phone_clean": pc, "name": c["contact_name"], "email": em})

    if not recs and LEAD_CSV_FALLBACK.is_file():
        cdf = normalize_column_names(pd.read_csv(LEAD_CSV_FALLBACK, encoding="utf-8"))
        pcol = pick_phone_column(cdf, "LeadDocket CSV fallback")
        for _, row in cdf.iterrows():
            pc = phone_clean_val(row[pcol])
            if not pc:
                continue
            nm = row.get("name", row.get("contact_name", ""))
            if pd.isna(nm):
                nm = ""
            else:
                nm = str(nm).strip()
            em = row.get("email", "")
            if pd.isna(em):
                em = ""
            else:
                em = str(em).strip()
            recs.append({"phone_clean": pc, "name": nm, "email": em})

    if not recs:
        raise SystemExit(
            "No LeadDocket data: add countrywidetriallawyers*.xlsx under Downloads, "
            f"or place {LEAD_CSV_FALLBACK.name} in the project folder."
        )

    lead = pd.DataFrame(recs)
    total_before_dedupe = len(lead)
    lead = lead.drop_duplicates(subset=["phone_clean"], keep="first")
    return lead, total_before_dedupe


def load_all_contact_pairs() -> pd.DataFrame:
    """All LeadDocket rows: norm_name + phone_clean (for name→phone layers)."""
    recs: list[dict] = []
    paths = sorted(DOWNLOADS.glob("countrywidetriallawyers*.xlsx"))
    for p in paths:
        sheet = pd.read_excel(p, sheet_name=0)
        cols = [c for c in sheet.columns if not str(c).startswith("_")]
        for _, row in sheet.iterrows():
            c = row_contact(row, cols)
            if not c:
                continue
            pc = phone_clean_val(c["phone"])
            if not pc:
                continue
            nn = normalize_person_name(c["contact_name"])
            if not nn:
                continue
            recs.append({"norm_name": nn, "phone_clean": pc})

    if not recs and LEAD_CSV_FALLBACK.is_file():
        cdf = normalize_column_names(pd.read_csv(LEAD_CSV_FALLBACK, encoding="utf-8"))
        pcol = pick_phone_column(cdf, "LeadDocket CSV fallback")
        for _, row in cdf.iterrows():
            pc = phone_clean_val(row[pcol])
            if not pc:
                continue
            nm = row.get("name", row.get("contact_name", ""))
            if pd.isna(nm):
                nm = ""
            nn = normalize_person_name(str(nm).strip())
            if not nn:
                continue
            recs.append({"norm_name": nn, "phone_clean": pc})

    if not recs:
        raise SystemExit("No contacts for attribution: add countrywidetriallawyers*.xlsx or lead CSV.")
    df = pd.DataFrame(recs).drop_duplicates(subset=["norm_name", "phone_clean"])
    return df


def _pick_call_row_for_phone(
    calls: pd.DataFrame, phone: str | None, signed_dt: pd.Timestamp | None
) -> pd.Series | None:
    if not phone:
        return None
    sub = calls[calls["phone_clean"].astype(str) == str(phone)]
    if len(sub) == 0:
        return None
    if signed_dt is not None and pd.notna(signed_dt) and sub["call_dt"].notna().any():
        dlt = (sub["call_dt"] - signed_dt).abs()
        return sub.loc[dlt.idxmin()]
    return sub.iloc[0]


def _fuzzy_best_phone(
    signed_nn: str, pairs: pd.DataFrame, min_ratio: float = 0.78
) -> tuple[str | None, float]:
    if not signed_nn or len(signed_nn) < 2:
        return None, 0.0
    fc = signed_nn[0]
    ln = len(signed_nn)
    sub = pairs[pairs["norm_name"].str.slice(0, 1) == fc]
    sub = sub[
        (sub["norm_name"].str.len() >= max(2, ln - 14))
        & (sub["norm_name"].str.len() <= ln + 18)
    ]
    if len(sub) == 0:
        sub = pairs[pairs["norm_name"].str.slice(0, 1) == fc]
    if len(sub) == 0:
        sub = pairs.head(0)
    best_p, best_r = None, 0.0
    stoks = set(signed_nn.split())
    for _, r in sub.iterrows():
        cn = r["norm_name"]
        if not cn:
            continue
        ratio = difflib.SequenceMatcher(None, signed_nn, cn).ratio()
        ctoks = set(cn.split())
        if len(stoks) >= 2 and stoks <= ctoks:
            ratio = max(ratio, 0.88)
        if len(ctoks) >= 2 and ctoks <= stoks:
            ratio = max(ratio, 0.88)
        if len(signed_nn) >= 5 and signed_nn in cn:
            ratio = max(ratio, 0.9)
        if len(cn) >= 5 and cn in signed_nn:
            ratio = max(ratio, 0.85)
        if ratio >= min_ratio and ratio > best_r:
            best_r = ratio
            best_p = str(r["phone_clean"])
    return best_p, best_r


def _method_date_proximity(
    signed_nn: str,
    signed_dt: pd.Timestamp | None,
    calls: pd.DataFrame,
    days: int = 7,
    min_name_ratio: float = 0.52,
) -> pd.Series | None:
    if signed_dt is None or pd.isna(signed_dt):
        return None
    win = pd.Timedelta(days=days)
    cand = calls[calls["call_dt"].notna()].copy()
    cand = cand[(cand["call_dt"] - signed_dt).abs() <= win]
    if len(cand) == 0:
        return None
    if len(cand) > 800 and signed_nn:
        c0 = signed_nn[0]
        nm = cand["name"].fillna("").astype(str).str.lower().str.replace(r"[^\w]", "", regex=True)
        cand = cand[nm.str.slice(0, 1) == c0]
    if len(cand) == 0:
        return None
    best_idx = None
    best_sc = 0.0
    for idx, cr in cand.iterrows():
        cn = normalize_person_name(cr.get("name", ""))
        if not cn:
            continue
        ratio = difflib.SequenceMatcher(None, signed_nn, cn).ratio()
        if len(signed_nn) >= 4 and (signed_nn in cn or cn in signed_nn):
            ratio = max(ratio, 0.68)
        st, ct = set(signed_nn.split()), set(cn.split())
        if len(st) >= 2 and st & ct:
            ratio = max(ratio, 0.55 + 0.1 * len(st & ct) / max(len(st), 1))
        if ratio > best_sc:
            best_sc = ratio
            best_idx = idx
    if best_idx is None or best_sc < min_name_ratio:
        return None
    return cand.loc[best_idx]


def attribution_report() -> None:
    if not OUT_CSV.is_file():
        raise SystemExit(f"Missing {OUT_CSV.name}")

    contacts = load_all_contact_pairs()
    exact_name_to_phone = contacts.groupby("norm_name")["phone_clean"].first().to_dict()

    calls = pd.read_csv(OUT_CSV, encoding="utf-8")
    calls = calls[calls["matched_call"].astype(str).str.upper() == "TRUE"].copy()
    calls["call_dt"] = pd.to_datetime(calls["call_start_time"], errors="coerce")
    calls["phone_clean"] = calls["phone_clean"].astype(str).str.replace(r"\.0$", "", regex=True)

    sig = load_signed_export()
    name_c, date_c = _pick_signed_name_date(sig)
    if not name_c:
        raise SystemExit("Signed export: missing name column.")
    rating_c = _pick_rating_column(sig)

    rows_out: list[dict] = []
    method_counts: dict[str, int] = {
        "exact_name_contact": 0,
        "fuzzy_name_contact": 0,
        "date_proximity": 0,
        "unmatched": 0,
    }
    fail_reasons = defaultdict(int)

    for _, sr in sig.iterrows():
        raw_name = sr[name_c]
        sn = normalize_person_name(raw_name)
        signed_dt = pd.to_datetime(sr[date_c], errors="coerce") if date_c else pd.NaT
        if pd.isna(signed_dt):
            signed_dt = None

        rating_val = ""
        if rating_c and rating_c in sig.columns:
            rv = sr[rating_c]
            rating_val = "" if pd.isna(rv) else str(rv).strip()

        method = None
        phone_guess = None
        call_row = None

        if sn:
            ph = exact_name_to_phone.get(sn)
            call_row = _pick_call_row_for_phone(calls, ph, signed_dt)
            if call_row is not None:
                method = "exact_name_contact"
                phone_guess = str(ph)

        if call_row is None and sn:
            ph2, _r = _fuzzy_best_phone(sn, contacts, min_ratio=0.78)
            if ph2:
                cr = _pick_call_row_for_phone(calls, ph2, signed_dt)
                if cr is not None:
                    method = "fuzzy_name_contact"
                    phone_guess = ph2
                    call_row = cr

        if call_row is None:
            cr3 = _method_date_proximity(sn, signed_dt, calls, days=7, min_name_ratio=0.52)
            if cr3 is not None:
                method = "date_proximity"
                phone_guess = str(cr3.get("phone_clean", ""))
                call_row = cr3

        if method:
            method_counts[method] += 1
            rows_out.append(
                {
                    "signed_name": raw_name,
                    "norm_name_signed": sn,
                    "signed_date": sr[date_c] if date_c else "",
                    "rating": rating_val,
                    "match_method": method,
                    "phone_clean": phone_guess or "",
                    "call_start_time": call_row.get("call_start_time", ""),
                    "source": call_row.get("source", ""),
                    "campaign_or_medium": call_row.get("campaign_or_medium", ""),
                    "duration": call_row.get("duration", ""),
                }
            )
        else:
            method_counts["unmatched"] += 1
            fp_guess, _ = _fuzzy_best_phone(sn, contacts, 0.78) if sn else (None, 0.0)
            ex_ph = exact_name_to_phone.get(sn) if sn else None
            if not sn:
                fail_reasons["blank_signed_name"] += 1
            elif ex_ph and _pick_call_row_for_phone(calls, ex_ph, signed_dt) is None:
                fail_reasons["exact_contact_match_but_no_call_row_for_phone"] += 1
            elif fp_guess is None:
                fail_reasons["no_lead_docket_name_hit_exact_or_fuzzy"] += 1
            elif _pick_call_row_for_phone(calls, fp_guess, signed_dt) is None:
                fail_reasons["fuzzy_contact_phone_but_no_call_row"] += 1
            else:
                fail_reasons["date_window_and_call_name_similarity_too_low"] += 1
            rows_out.append(
                {
                    "signed_name": raw_name,
                    "norm_name_signed": sn,
                    "signed_date": sr[date_c] if date_c else "",
                    "rating": rating_val,
                    "match_method": "unmatched",
                    "phone_clean": "",
                    "call_start_time": "",
                    "source": "",
                    "campaign_or_medium": "",
                    "duration": "",
                }
            )

    out_df = pd.DataFrame(rows_out)
    out_df.to_csv(SIGNED_ATTRIBUTION_OUT, index=False, encoding="utf-8")

    n_sig = len(sig)
    n_matched = n_sig - method_counts["unmatched"]
    rate = (n_matched / n_sig * 100) if n_sig else 0.0

    print("SECTION 1: Match Results")
    print(f"  total signed cases: {n_sig}")
    print(f"  matched cases (any method): {n_matched}")
    print(f"  match rate (%): {round(rate, 2)}")
    print()

    matched = out_df[out_df["match_method"] != "unmatched"].copy()
    matched["rating_score"] = matched["rating"].map(rating_to_numeric_score)

    def section_src(col: str) -> None:
        if len(matched) == 0:
            print("  (no matched cases)")
            print()
            return
        gcol = matched[col].fillna("(blank)").replace("", "(blank)")
        for key, sub in matched.groupby(gcol, sort=False):
            cnt = len(sub)
            avg_r = sub["rating_score"].mean()
            dist = sub["rating"].fillna("(blank)").value_counts().to_dict()
            dist_s = "; ".join(f"{k}:{v}" for k, v in sorted(dist.items(), key=lambda x: -x[1])[:8])
            print(
                f"  {key}\n    total signed (matched): {cnt}\n    avg rating (numeric/stars): "
                f"{round(float(avg_r), 3) if pd.notna(avg_r) else 'n/a'}\n    rating distribution: {dist_s}"
            )
        print()

    print("SECTION 2: By Source (matched cases only)")
    section_src("source")

    print("SECTION 3: By campaign_or_medium (matched cases only)")
    section_src("campaign_or_medium")

    print("SECTION 4: Match Method Breakdown")
    for k in ("exact_name_contact", "fuzzy_name_contact", "date_proximity", "unmatched"):
        print(f"  {k}: {method_counts[k]}")
    print()

    print("SECTION 5: Limitations / unmatched")
    print(
        "  Directional matching: names differ between LeadDocket, CallRail caller ID, and signed sheet; "
        "fuzzy and date rules can over-link. Validate high-value rows manually."
    )
    if method_counts["unmatched"]:
        print(f"  Unmatched count: {method_counts['unmatched']}")
        for k, v in sorted(fail_reasons.items(), key=lambda x: -x[1]):
            print(f"    - {k}: {v}")
    print()
    print(f"  Full table: {SIGNED_ATTRIBUTION_OUT.name}")


def load_callrail() -> pd.DataFrame:
    if CALL_CSV.is_file():
        raw = pd.read_csv(CALL_CSV, encoding="utf-8")
    elif CALL_XLSX.is_file():
        raw = pd.read_excel(CALL_XLSX, sheet_name=0)
    elif CALL_XLSX_FALLBACK.is_file():
        raw = pd.read_excel(CALL_XLSX_FALLBACK, sheet_name=0)
    else:
        raise SystemExit("No CallRail file found (csv/xlsx in project or fallback xlsx).")
    return normalize_column_names(raw)


def main() -> None:
    lead, total_contacts = load_lead_combined()
    unique_phones = len(lead)

    raw = load_callrail()
    call_pcol = pick_phone_column(raw, "CallRail")
    fields = pick_callrail_fields(raw)

    calls = pd.DataFrame({"phone_clean": raw[call_pcol].apply(phone_clean_val)})
    if fields.get("time"):
        calls["call_start_time"] = raw[fields["time"]]
    else:
        calls["call_start_time"] = pd.NA
    if fields["source"]:
        calls["source"] = raw[fields["source"]]
    else:
        calls["source"] = pd.NA
    cc, mc = fields["campaign_col"], fields["medium_col"]
    if cc and mc:
        calls["campaign_or_medium"] = raw[cc].combine_first(raw[mc])
    elif cc:
        calls["campaign_or_medium"] = raw[cc]
    elif mc:
        calls["campaign_or_medium"] = raw[mc]
    else:
        calls["campaign_or_medium"] = pd.NA
    if fields["duration"]:
        calls["duration"] = raw[fields["duration"]]
    else:
        calls["duration"] = pd.NA

    calls = calls[calls["phone_clean"].notna()].copy()

    call_phone_set = set(calls["phone_clean"].unique())
    matched_phone_count = int(lead["phone_clean"].isin(call_phone_set).sum())
    match_rate = (matched_phone_count / unique_phones * 100) if unique_phones else 0.0

    merged = lead.merge(calls, on="phone_clean", how="left")
    merged["matched_call"] = merged["call_start_time"].notna()

    out = pd.DataFrame(
        {
            "phone_clean": merged["phone_clean"],
            "name": merged["name"],
            "email": merged["email"],
            "call_start_time": merged["call_start_time"],
            "source": merged["source"],
            "campaign_or_medium": merged["campaign_or_medium"],
            "duration": merged["duration"],
            "matched_call": merged["matched_call"].map({True: "TRUE", False: "FALSE"}),
        }
    )
    out.to_csv(OUT_CSV, index=False, encoding="utf-8")

    hit = merged[merged["matched_call"]]

    print(f"total contacts (before dedupe): {total_contacts}")
    print(f"unique phones (after dedupe): {unique_phones}")
    print(f"total matched phones: {matched_phone_count}")
    print(f"match rate %: {match_rate:.2f}")
    print("top 5 sources (matched count)")
    if len(hit) and fields["source"]:
        for k, v in hit["source"].fillna("(blank)").value_counts().head(5).items():
            print(f"  {v}\t{k}")
    else:
        print("  (n/a)")
    print("top 5 campaign_or_medium")
    if len(hit):
        for k, v in hit["campaign_or_medium"].fillna("(blank)").value_counts().head(5).items():
            print(f"  {v}\t{k}")
    else:
        print("  (n/a)")


def summarize_matched_sources() -> None:
    if not OUT_CSV.is_file():
        raise SystemExit(f"Missing {OUT_CSV.name}; run without 'summarize' first.")
    df = pd.read_csv(OUT_CSV, encoding="utf-8")
    m = df[df["matched_call"].astype(str).str.upper() == "TRUE"].copy()
    m["duration"] = pd.to_numeric(m["duration"], errors="coerce")
    m["source"] = m["source"].fillna("(blank)").replace("", "(blank)")
    m["campaign_or_medium"] = m["campaign_or_medium"].fillna("(blank)").replace(
        "", "(blank)"
    )

    def by_col(col: str) -> pd.DataFrame:
        rows = []
        for key, sub in m.groupby(col, sort=False):
            tc = len(sub)
            avg_d = sub["duration"].mean()
            hi = int((sub["duration"] >= 300).sum())
            rate = (hi / tc * 100) if tc else 0.0
            rows.append(
                {
                    col: key,
                    "total_calls": tc,
                    "avg_duration": round(avg_d, 2) if pd.notna(avg_d) else None,
                    "high_intent_calls": hi,
                    "high_intent_rate": round(rate, 2),
                }
            )
        out = pd.DataFrame(rows)
        return out.sort_values("total_calls", ascending=False).reset_index(drop=True)

    src_tbl = by_col("source")
    camp_tbl = by_col("campaign_or_medium")

    total_calls = len(m)
    total_hi = int((m["duration"] >= 300).sum())
    overall_rate = (total_hi / total_calls * 100) if total_calls else 0.0

    print("SECTION 1: Top Sources Table")
    print(src_tbl.to_string(index=False))
    print()
    print("SECTION 2: Top Campaigns / Medium")
    print(camp_tbl.to_string(index=False))
    print()
    print(f"Total matched calls: {total_calls}")
    print(f"Total high intent calls (>=300s): {total_hi}")
    print(f"Overall high intent rate (%): {round(overall_rate, 2)}")


def _normalize_signed_df_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [
        re.sub(r"_+", "_", re.sub(r"[^a-z0-9]+", "_", str(c).strip().lower())).strip("_")
        for c in df.columns
    ]
    return df


def load_signed_export() -> pd.DataFrame:
    env_p = os.environ.get("SIGNED_CASES_CSV", "").strip()
    if SIGNED_EXPORT.is_file():
        raw = pd.read_csv(SIGNED_EXPORT, encoding="utf-8")
    elif env_p and Path(env_p).is_file():
        ep = Path(env_p)
        raw = (
            pd.read_excel(ep, sheet_name=0)
            if ep.suffix.lower() in (".xlsx", ".xls")
            else pd.read_csv(ep, encoding="utf-8")
        )
    elif DEFAULT_SIGNED_XLSX.is_file():
        raw = pd.read_excel(DEFAULT_SIGNED_XLSX, sheet_name=0)
    else:
        raise SystemExit(
            "Signed cases not found. Use one of:\n"
            f"  - {SIGNED_EXPORT.name} in project folder\n"
            f"  - {DEFAULT_SIGNED_XLSX}\n"
            "  - set SIGNED_CASES_CSV to a CSV path"
        )
    return _normalize_signed_df_columns(raw)


def _pick_rating_column(df: pd.DataFrame) -> str | None:
    if "rating" in df.columns:
        return "rating"
    for c in df.columns:
        if "rating" in c or c.endswith("_score"):
            return c
    return None


def _pick_signed_phone_column(df: pd.DataFrame) -> str | None:
    for c in (
        "phone_number",
        "customer_phone_number",
        "contact_phone",
        "phone",
        "mobile",
        "caller_phone",
        "tel",
    ):
        if c in df.columns:
            return c
    for c in df.columns:
        cl = str(c).lower()
        if "tracking" in cl:
            continue
        if "phone" in cl or "mobile" in cl or "tel" in cl:
            return c
    return None


def _pick_signed_name_date(df: pd.DataFrame) -> tuple[str, str]:
    name_c = None
    for c in ("name", "client_name", "lead_name", "full_name", "contact_name", "plaintiff"):
        if c in df.columns:
            name_c = c
            break
    if name_c is None:
        for c in df.columns:
            if "name" in c and "number" not in c:
                name_c = c
                break
    date_c = None
    for c in (
        "signed_date",
        "date_signed",
        "sign_date",
        "contract_date",
        "date_retained",
        "retained_date",
    ):
        if c in df.columns:
            date_c = c
            break
    if date_c is None:
        for c in df.columns:
            if "signed" in c and "date" in c:
                date_c = c
                break
    if date_c is None and "date" in df.columns:
        date_c = "date"
    return name_c or "", date_c or ""


def attach_signed_match(
    main: pd.DataFrame, sig: pd.DataFrame
) -> tuple[pd.DataFrame, set[int], dict]:
    """
    Prefer phone_clean match when signed file has a phone column with usable values;
    else fall back to normalized name (+ closest signed_date when available).
    Returns (main with is_signed, rating, meta dict with match_mode).
    """
    sig = sig.copy()
    sig["_srow"] = range(len(sig))

    phone_c = _pick_signed_phone_column(sig)
    sig["_sig_phone"] = sig[phone_c].apply(phone_clean_val) if phone_c else pd.Series(
        [pd.NA] * len(sig)
    )
    use_phone = bool(phone_c) and sig["_sig_phone"].notna().any()

    name_c, date_c = _pick_signed_name_date(sig)
    if date_c:
        sig["signed_dt"] = pd.to_datetime(sig[date_c], errors="coerce")
    else:
        sig["signed_dt"] = pd.NA

    rating_c = _pick_rating_column(sig)

    main = main.copy()
    main["call_dt"] = pd.to_datetime(main["call_start_time"], errors="coerce")

    meta: dict = {
        "match_mode": "phone" if use_phone else "name",
        "had_phone_column": bool(phone_c),
        "phone_column_usable": use_phone,
    }

    is_signed: list[bool] = []
    ratings_out: list[str] = []
    linked_signed: set[int] = set()

    if use_phone:
        from collections import defaultdict

        by_phone: dict[str, list[int]] = defaultdict(list)
        for j in range(len(sig)):
            p = sig.iloc[j]["_sig_phone"]
            if p is None or (isinstance(p, float) and pd.isna(p)):
                continue
            by_phone[str(p)].append(j)

        for i in range(len(main)):
            row = main.iloc[i]
            if str(row.get("matched_call", "")).upper() != "TRUE":
                is_signed.append(False)
                ratings_out.append("")
                continue
            pc = phone_clean_val(row.get("phone_clean"))
            if not pc or pc not in by_phone:
                is_signed.append(False)
                ratings_out.append("")
                continue
            jlist = by_phone[pc]
            cand = sig.iloc[jlist]
            valid = cand[cand["signed_dt"].notna()]
            if len(valid) == 0:
                best = cand.iloc[0]
            else:
                ct = row["call_dt"]
                if pd.isna(ct):
                    best = valid.iloc[0]
                else:
                    best = valid.loc[(valid["signed_dt"] - ct).abs().idxmin()]
            linked_signed.add(int(best["_srow"]))
            is_signed.append(True)
            if rating_c and rating_c in best.index:
                rv = best[rating_c]
                ratings_out.append("" if pd.isna(rv) else str(rv).strip())
            else:
                ratings_out.append("")

        main["is_signed"] = ["TRUE" if x else "FALSE" for x in is_signed]
        main["rating"] = ratings_out
        main = main.drop(columns=["call_dt"], errors="ignore")
        return main, linked_signed, meta

    # --- Name fallback ---
    if not name_c:
        raise SystemExit("Signed export: no phone match possible and no name column found.")

    sig["norm_name"] = sig[name_c].apply(normalize_person_name)
    main["norm_name"] = main["name"].apply(normalize_person_name)

    junk_names = {"", "no name in export", "unknown caller", "(no name in export)"}

    sig_by = sig.groupby("norm_name", sort=False)

    for i in range(len(main)):
        row = main.iloc[i]
        if str(row.get("matched_call", "")).upper() != "TRUE":
            is_signed.append(False)
            ratings_out.append("")
            continue
        nn = row["norm_name"]
        if not nn or nn in junk_names:
            is_signed.append(False)
            ratings_out.append("")
            continue
        try:
            cand = sig_by.get_group(nn)
        except KeyError:
            is_signed.append(False)
            ratings_out.append("")
            continue
        valid = cand[cand["signed_dt"].notna()]
        if len(valid) == 0:
            if len(cand) == 0:
                is_signed.append(False)
                ratings_out.append("")
                continue
            best = cand.iloc[0]
            is_signed.append(True)
        else:
            ct = row["call_dt"]
            if pd.isna(ct):
                best = valid.iloc[0]
            else:
                best = valid.loc[(valid["signed_dt"] - ct).abs().idxmin()]
            is_signed.append(True)
        linked_signed.add(int(best["_srow"]))
        if rating_c and rating_c in best.index:
            rv = best[rating_c]
            ratings_out.append("" if pd.isna(rv) else str(rv).strip())
        else:
            ratings_out.append("")

    main["is_signed"] = ["TRUE" if x else "FALSE" for x in is_signed]
    main["rating"] = ratings_out
    main = main.drop(columns=["norm_name", "call_dt"], errors="ignore")
    return main, linked_signed, meta


def match_signed_cases() -> None:
    if not OUT_CSV.is_file():
        raise SystemExit(f"Missing {OUT_CSV.name}")

    main = pd.read_csv(OUT_CSV, encoding="utf-8")
    sig = load_signed_export()
    main, linked_signed, meta = attach_signed_match(main, sig)
    main.to_csv(OUT_WITH_SIGNED, index=False, encoding="utf-8")

    m = main[main["matched_call"].astype(str).str.upper() == "TRUE"].copy()
    m["is_signed_bool"] = m["is_signed"].astype(str).str.upper() == "TRUE"

    def section(col: str) -> pd.DataFrame:
        rows = []
        for key, sub in m.groupby(
            m[col].fillna("(blank)").replace("", "(blank)"), sort=False
        ):
            tc = len(sub)
            sc = int(sub["is_signed_bool"].sum())
            rate = (sc / tc * 100) if tc else 0.0
            rows.append(
                {
                    col: key,
                    "total_calls": tc,
                    "signed_cases": sc,
                    "signed_rate": round(rate, 2),
                }
            )
        out = pd.DataFrame(rows)
        return out.sort_values("total_calls", ascending=False).reset_index(drop=True)

    src_tbl = section("source")
    camp_tbl = section("campaign_or_medium")

    total_matched = len(m)
    overall = (
        (int(m["is_signed_bool"].sum()) / total_matched * 100) if total_matched else 0.0
    )

    print("SECTION 1: By Source")
    print(src_tbl.to_string(index=False))
    print()
    print("SECTION 2: By campaign_or_medium")
    print(camp_tbl.to_string(index=False))
    print()
    calls_signed = int(m["is_signed_bool"].sum())
    print(f"Total signed matches (matched call rows): {calls_signed}")
    print(f"Unique signed-case rows linked: {len(linked_signed)}")
    print(f"Overall signed rate (%): {round(overall, 2)}")
    print(f"Match method: {meta['match_mode']} (signed file had phone column: {meta['had_phone_column']}, usable phone values: {meta['phone_column_usable']})")
    if overall < 5.0:
        print()
        print("WHY match rate is very low (<5%):")
        if meta["match_mode"] == "phone":
            print(
                "- Phone mode: few call rows share the same phone_clean as the signed export "
                "(wrong numbers on signed sheet, different cohort/date range, or formatting)."
            )
        else:
            if not meta["had_phone_column"]:
                print(
                    "- No phone column in signed cases export, so matching used names only."
                )
            elif not meta["phone_column_usable"]:
                print(
                    "- Signed file has a phone column but no valid 10-digit values; fell back to names."
                )
            print(
                "- Name mode: call names often differ from signed names (e.g. 'no name in export', "
                "'Wireless Caller', First/Last order, spelling), so few rows align."
            )
    print(f"(Output: {OUT_WITH_SIGNED.name})")


def _funnel_group_table(m: pd.DataFrame, col: str) -> pd.DataFrame:
    dur = pd.to_numeric(m["duration"], errors="coerce")
    m = m.assign(_dur=dur)
    rows = []
    for key, sub in m.groupby(
        m[col].fillna("(blank)").replace("", "(blank)"), sort=False
    ):
        tc = len(sub)
        hi = int((sub["_dur"] >= 300).sum())
        signed = sub[sub["is_signed"].astype(str).str.upper() == "TRUE"]
        sc = len(signed)
        sr = round((sc / tc * 100), 2) if tc else 0.0
        rnum = signed["rating"].map(rating_to_numeric_score)
        ar = round(float(rnum.mean()), 3) if rnum.notna().any() else None
        rows.append(
            {
                col: key,
                "total_calls": tc,
                "high_intent_calls": hi,
                "signed_cases": sc,
                "signed_rate": sr,
                "avg_rating": ar,
            }
        )
    out = pd.DataFrame(rows)
    return out.sort_values("total_calls", ascending=False).reset_index(drop=True)


def funnel_report() -> None:
    if not OUT_CSV.is_file():
        raise SystemExit(f"Missing {OUT_CSV.name}")

    main = pd.read_csv(OUT_CSV, encoding="utf-8")
    sig = load_signed_export()
    main, _, match_meta = attach_signed_match(main, sig)
    main.to_csv(OUT_WITH_SIGNED, index=False, encoding="utf-8")

    m = main[main["matched_call"].astype(str).str.upper() == "TRUE"].copy()

    src = _funnel_group_table(m, "source")
    camp = _funnel_group_table(m, "campaign_or_medium")

    print("SECTION 1: Source Funnel Performance")
    print(src.to_string(index=False))
    print()
    print("SECTION 2: Campaign / Medium Performance")
    print(camp.to_string(index=False))
    print()

    total_sig = int((m["is_signed"].astype(str).str.upper() == "TRUE").sum())

    # --- Section 3: insights (simple thresholds)
    min_calls = 30
    min_signed_rating = 2

    vol = src.iloc[0] if len(src) else None
    conv = src[src["total_calls"] >= min_calls].sort_values(
        "signed_rate", ascending=False
    )
    best_conv = conv.iloc[0] if len(conv) else None

    qual = src[src["signed_cases"] >= min_signed_rating].copy()
    qual = qual[qual["avg_rating"].notna()].sort_values(
        "avg_rating", ascending=False
    )
    best_qual = qual.iloc[0] if len(qual) else None

    waste = src[(src["total_calls"] >= min_calls) & (src["signed_rate"] < 0.5)].sort_values(
        "total_calls", ascending=False
    )

    print("SECTION 3: Key Insights")
    if total_sig == 0:
        if match_meta.get("match_mode") == "phone":
            print(
                "- No matched calls linked on phone_clean to the signed export. Check phone fields, "
                "cohort overlap, and that both files use the same 10-digit normalization."
            )
        else:
            print(
                "- No matched calls linked by normalized name (and closest signed date when available). "
                "Signed export has no usable phone column or values; add phone to the signed sheet or "
                "align name formats and date ranges."
            )
    if vol is not None:
        print(
            f"- Highest call volume: \"{vol['source']}\" ({int(vol['total_calls'])} matched calls)."
        )
    if total_sig > 0 and best_qual is not None:
        print(
            f"- Highest average star rating (among sources with at least {min_signed_rating} "
            f"signed match(es)): \"{best_qual['source']}\" "
            f"(avg score {best_qual['avg_rating']}, {int(best_qual['signed_cases'])} signed)."
        )
    elif total_sig > 0:
        print("- Signed matches exist but not enough per source to compare average ratings reliably.")
    if total_sig > 0 and best_conv is not None and best_conv["signed_rate"] > 0:
        print(
            f"- Best signed rate among sources with at least {min_calls} calls: "
            f"\"{best_conv['source']}\" ({best_conv['signed_rate']}% signed)."
        )
    if total_sig > 0 and len(waste):
        top_waste = waste.head(3)
        wlist = ", ".join(
            f'"{r["source"]}" ({int(r["total_calls"])} calls, {r["signed_rate"]}% signed)'
            for _, r in top_waste.iterrows()
        )
        print(
            f"- High volume with very low attributed signed rate (>{min_calls} calls, signed rate under 0.5%): "
            f"{wlist}. Validate matching before treating as waste."
        )
    elif total_sig > 0:
        print(
            f"- No source with >={min_calls} calls had signed rate under 0.5% in this attribution pass."
        )
    print()

    print("SECTION 4: Simple Recommendations")
    recs = []
    if total_sig == 0:
        recs.append(
            "First: fix linkage (names + date range + optional phone match). Until signed rows attach to calls, "
            "use call volume and high-intent share only for budget decisions."
        )
    else:
        if best_conv is not None and vol is not None and best_conv["signed_rate"] > 0:
            if str(best_conv["source"]) != str(vol["source"]):
                recs.append(
                    f"Scale testing: keep volume on \"{vol['source']}\" but pilot more budget on "
                    f"\"{best_conv['source']}\" where signed rate is higher."
                )
            else:
                recs.append(
                    f"\"{vol['source']}\" drives both volume and signed rate here—defend budget and "
                    "document what creatives/keywords correlate with signed cases."
                )
        if best_qual is not None:
            recs.append(
                f"Double down on quality patterns from \"{best_qual['source']}\" (highest avg rating where "
                "multiple signings exist); mirror landing and intake scripts on weaker sources."
            )
        if len(waste):
            recs.append(
                "For high-volume, low-attributed-signed sources: audit match quality, then tighten targeting "
                "or add intake filters—not a cut until linkage is trusted."
            )
    if not recs:
        recs.append("Extend the signed export window or enrich matching keys to sharpen recommendations.")
    for r in recs:
        print(f"- {r}")
    print()
    print(f"Full attributed file (with is_signed, rating): {OUT_WITH_SIGNED.name}")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "summarize":
        summarize_matched_sources()
    elif len(sys.argv) > 1 and sys.argv[1] == "signed":
        match_signed_cases()
    elif len(sys.argv) > 1 and sys.argv[1] == "funnel":
        funnel_report()
    elif len(sys.argv) > 1 and sys.argv[1] == "attribution":
        attribution_report()
    else:
        main()
