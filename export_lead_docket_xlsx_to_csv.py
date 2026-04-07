#!/usr/bin/env python3
"""
Build lead_docket_sample.csv from LeadDocket table-scraper xlsx exports in Downloads.
Expects: countrywidetriallawyers.xlsx and countrywidetriallawyers (1).xlsx … (19).xlsx
"""
from __future__ import annotations

import csv
from pathlib import Path

import pandas as pd

from match_leaddocket_callrail import dedupe_contacts, parse_leaddocket_dir, row_contact

DOWNLOADS = Path(r"c:\Users\georgea\Downloads")
OUT = Path(__file__).resolve().parent / "lead_docket_sample.csv"


def main() -> None:
    df = parse_leaddocket_dir(DOWNLOADS)
    columns = [c for c in df.columns if not str(c).startswith("_")]
    raw: list[dict] = []
    for _, row in df.iterrows():
        c = row_contact(row, columns)
        if not c:
            continue
        c["source_file"] = row.get("_source_file", "")
        raw.append(c)

    by_phone = dedupe_contacts(raw)
    rows_out = sorted(by_phone.values(), key=lambda x: x["phone"])

    with OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["phone", "name", "email", "source_file"],
        )
        w.writeheader()
        for r in rows_out:
            em = r.get("email")
            w.writerow(
                {
                    "phone": r["phone"],
                    "name": r["contact_name"],
                    "email": em if em is not None and not pd.isna(em) else "",
                    "source_file": r.get("source_file", ""),
                }
            )

    print(f"Wrote {len(rows_out)} deduped contacts to {OUT}")
    print(f"Raw rows with phone: {len(raw)}")


if __name__ == "__main__":
    main()
