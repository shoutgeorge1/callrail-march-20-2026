#!/usr/bin/env python3
"""
Combine two Lead Docket Excel exports into one normalized CSV for CallRail matching.
Does not modify ingest or dashboard code.
"""
from __future__ import annotations

import csv
import re
from pathlib import Path

import pandas as pd

# Default inputs (February + March exports)
FILE_FEB = Path(r"c:\Users\georgea\Downloads\Unsaved Report-2026-03-30 134439.xlsx")
FILE_MAR = Path(r"c:\Users\georgea\Downloads\Unsaved Report-2026-03-30 134458.xlsx")
OUT_DIR = Path(__file__).resolve().parent / "data"
OUT_CSV = OUT_DIR / "leaddocket_leads_last60days.csv"

# Explicit header → snake_case (overrides generic rule)
HEADER_TO_KEY = {
    "lead id": "lead_id",
    "first name": "first_name",
    "last name": "last_name",
    "created date": "created_at",
    "home phone": "home_phone",
    "mobile phone": "mobile_phone",
    "phone call source": "phone_call_source",
    "work phone": "work_phone",
    "primary phone": "primary_phone",
    "contact source": "contact_source",
    "entry source": "entry_source",
    "marketing source": "marketing_source",
    "marketing source details": "marketing_source_details",
    "opportunity source": "opportunity_source",
    "source lead id": "source_lead_id",
    "created by first name": "created_by_first_name",
    "created by full name": "created_by_full_name",
    "created by id": "created_by_id",
    "created by last name": "created_by_last_name",
    "case type": "case_type",
    "case type code": "case_type_code",
    "describe other type": "describe_other_type",
    "mva subtype": "mva_subtype",
    "lead status": "lead_status",
    "status": "lead_status",
}


def header_to_snake(name: str) -> str:
    raw = str(name).strip().lower()
    if raw in HEADER_TO_KEY:
        return HEADER_TO_KEY[raw]
    s = re.sub(r"[^a-z0-9]+", "_", raw)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "column"


def norm_phone_digits(val) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    d = re.sub(r"\D", "", str(val).strip())
    if not d:
        return ""
    if len(d) == 11 and d[0] == "1":
        d = d[1:]
    if len(d) > 10:
        d = d[-10:]
    if len(d) == 10:
        return d
    return ""


def cell_str(val) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    if isinstance(val, pd.Timestamp):
        return val.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(val, float) and val.is_integer():
        return str(int(val))
    if isinstance(val, int):
        return str(val)
    s = str(val).strip()
    return s


def merge_phone_row(row: dict) -> str:
    """Prefer primary, then mobile, home, work."""
    for key in ("primary_phone", "mobile_phone", "home_phone", "work_phone"):
        p = norm_phone_digits(row.get(key, ""))
        if p:
            return p
    return ""


def read_one(path: Path) -> pd.DataFrame:
    if not path.is_file():
        raise FileNotFoundError(path)
    return pd.read_excel(path)


def main() -> None:
    df_a = read_one(FILE_FEB)
    df_b = read_one(FILE_MAR)

    all_cols = list(dict.fromkeys(list(df_a.columns) + list(df_b.columns)))
    df_a = df_a.reindex(columns=all_cols)
    df_b = df_b.reindex(columns=all_cols)

    combined = pd.concat([df_a, df_b], ignore_index=True)

    rename = {c: header_to_snake(c) for c in combined.columns}
    # avoid duplicate keys after rename
    seen: dict[str, int] = {}
    final_rename = {}
    for old, new in rename.items():
        k = new
        if k in seen:
            seen[k] += 1
            k = f"{new}_{seen[k]}"
        else:
            seen[new] = 0
        final_rename[old] = k
    combined = combined.rename(columns=final_rename)

    rows_out: list[dict] = []
    for _, row in combined.iterrows():
        r = {col: cell_str(row[col]) for col in combined.columns}
        if not any(v for v in r.values()):
            continue
        r["phone"] = merge_phone_row(r)
        if "lead_status" not in r:
            r["lead_status"] = ""
        rows_out.append(r)

    # Final column order: required first, then stable rest
    base_keys = set()
    for r in rows_out:
        base_keys.update(r.keys())
    phone_sources = {"home_phone", "mobile_phone", "work_phone", "primary_phone"}
    skip = {"phone", "created_at", "lead_status"} | phone_sources
    rest = sorted(k for k in base_keys if k not in skip)
    fieldnames = ["phone", "created_at", "lead_status"] + rest

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for r in rows_out:
            out = {k: r.get(k, "") for k in fieldnames}
            w.writerow(out)

    n = len(rows_out)
    missing_phone = sum(1 for r in rows_out if not r.get("phone"))
    missing_created = sum(1 for r in rows_out if not str(r.get("created_at", "")).strip())

    print("Lead Docket combine -> CSV")
    print(f"  files:           {FILE_FEB.name} + {FILE_MAR.name}")
    print(f"  output:          {OUT_CSV}")
    print(f"  total rows:      {n}")
    print(f"  missing phone:   {missing_phone}")
    print(f"  missing created_at: {missing_created}")


if __name__ == "__main__":
    main()
