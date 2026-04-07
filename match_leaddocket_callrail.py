#!/usr/bin/env python3
"""
Match LeadDocket contact exports (xlsx) to CallRail inbound calls by normalized
US phone number. Requires CALLRAIL_API_KEY and CALLRAIL_ACCOUNT_ID in .env.
"""
from __future__ import annotations

import re
import time
from collections import Counter
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import requests

DOWNLOADS_DEFAULT = Path(r"c:\Users\georgea\Downloads")
# CallRail retention is ~2 years; wider ranges return 400.
RETENTION_DAYS = 729
REQUEST_GAP_S = 1.4


def load_env(path: Path) -> dict[str, str]:
    env: dict[str, str] = {}
    if not path.exists():
        return env
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def normalize_phone(val) -> str | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    digits = re.sub(r"\D", "", str(val))
    if len(digits) == 11 and digits[0] == "1":
        digits = digits[1:]
    if len(digits) != 10:
        return None
    return digits


def valid_us_national(d10: str) -> bool:
    if not d10 or len(d10) != 10:
        return False
    if d10[0] in "01" or d10[3] in "01":
        return False
    return True


def looks_like_email(s: str) -> bool:
    s = str(s).strip()
    return "@" in s and "." in s.split("@")[-1]


_US_STATE = frozenset(
    "AL AK AZ AR CA CO CT DE DC FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO MT NE NV NH NJ NM NY NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY".split()
)


def row_contact(row, columns: list[str]) -> dict | None:
    href_col = "btn href" if "btn href" in columns else None
    vals: list[tuple[str, object]] = []
    for c in columns:
        if c == href_col or str(c).startswith("_"):
            continue
        vals.append((c, row[c]))

    phone = None
    phone_idx = None
    for i, (_, v) in enumerate(vals):
        n = normalize_phone(v)
        if n and valid_us_national(n):
            phone = n
            phone_idx = i
            break
    if not phone:
        return None

    email = None
    for _, v in vals:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            continue
        s = str(v).strip()
        if looks_like_email(s):
            email = s
            break

    def cell_str(v: object) -> str:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return ""
        return str(v).strip()

    # Six-column layout: first name, last name, location, phone, email (location may be state or address)
    if phone_idx == 3 and len(vals) >= 4:
        parts = [
            cell_str(vals[0][1]),
            cell_str(vals[1][1]),
        ]
        parts = [p for p in parts if p and p not in ("-",) and p.lower() != "unknown"]
        name = " ".join(parts).strip() or "(no name in export)"
        return {"phone": phone, "contact_name": name, "email": email}

    # Five-column layout: display name, state, phone, extra
    if phone_idx == 2 and len(vals) >= 3 and cell_str(vals[1][1]) in _US_STATE:
        n0 = cell_str(vals[0][1])
        if n0 and n0 not in ("-",) and n0.lower() != "unknown":
            name = n0
        else:
            name = "(no name in export)"
        return {"phone": phone, "contact_name": name, "email": email}

    name_bits: list[str] = []
    for i, (_, v) in enumerate(vals):
        if v is None or (isinstance(v, float) and pd.isna(v)):
            continue
        s = str(v).strip()
        if not s or s.lower() == "nan":
            continue
        if looks_like_email(s):
            continue
        if i == phone_idx:
            continue
        if normalize_phone(s) == phone:
            continue
        if s in ("-", "—"):
            continue
        if s.lower() == "unknown":
            continue
        if s in _US_STATE:
            continue
        name_bits.append(s)
    name = " ".join(name_bits).strip()
    if not name:
        name = "(no name in export)"
    return {"phone": phone, "contact_name": name, "email": email}


def parse_leaddocket_dir(downloads: Path) -> pd.DataFrame:
    names = ["countrywidetriallawyers.xlsx"] + [
        f"countrywidetriallawyers ({i}).xlsx" for i in range(1, 20)
    ]
    parts = []
    for name in names:
        p = downloads / name
        if not p.exists():
            continue
        df = pd.read_excel(p, sheet_name=0)
        df["_source_file"] = p.name
        parts.append(df)
    if not parts:
        raise SystemExit(f"No LeadDocket xlsx found under {downloads}")
    return pd.concat(parts, ignore_index=True)


def dedupe_contacts(contacts: list[dict]) -> dict[str, dict]:
    def score(x: dict) -> tuple[int, int]:
        return (len(x["contact_name"]), 1 if x.get("email") else 0)

    best: dict[str, dict] = {}
    for c in contacts:
        p = c["phone"]
        prev = best.get(p)
        if prev is None or score(c) > score(prev):
            best[p] = c
    return best


def fetch_callrail_calls(
    account_id: str, api_key: str, start_d: date, end_d: date
) -> list[dict]:
    fields = (
        "customer_phone_number,source,formatted_tracking_source,"
        "start_time,direction,id,utm_campaign,campaign"
    )
    base = f"https://api.callrail.com/v3/a/{account_id}/calls.json"
    params = {
        "start_date": start_d.isoformat(),
        "end_date": end_d.isoformat(),
        "per_page": 100,
        "relative_pagination": "true",
        "direction": "inbound",
        "fields": fields,
    }
    out: list[dict] = []
    url: str | None = base
    first = True
    page = 1
    headers = {"Authorization": f'Token token="{api_key}"'}

    while url:
        if not first:
            time.sleep(REQUEST_GAP_S)
        if first:
            r = requests.get(url, headers=headers, params=params, timeout=120)
            first = False
        else:
            r = requests.get(url, headers=headers, timeout=120)
        r.raise_for_status()
        data = r.json()
        calls = data.get("calls") or []
        out.extend(calls)
        print(f"  CallRail page {page}: +{len(calls)} calls (running total {len(out)})")
        if data.get("has_next_page") and data.get("next_page"):
            url = data["next_page"]
            page += 1
        else:
            url = None
    return out


def campaign_label(call: dict) -> str:
    c = call.get("campaign")
    if c:
        return str(c)
    u = call.get("utm_campaign")
    if u:
        return str(u)
    return ""


def main() -> None:
    root = Path(__file__).resolve().parent
    env = load_env(root / ".env")
    api_key = env.get("CALLRAIL_API_KEY")
    account_id = env.get("CALLRAIL_ACCOUNT_ID")
    if not api_key or not account_id:
        raise SystemExit("Set CALLRAIL_API_KEY and CALLRAIL_ACCOUNT_ID in .env")

    downloads = DOWNLOADS_DEFAULT
    print("Loading LeadDocket exports from", downloads)
    df = parse_leaddocket_dir(downloads)
    columns = [c for c in df.columns if not str(c).startswith("_")]
    raw: list[dict | None] = []
    for _, row in df.iterrows():
        raw.append(row_contact(row, columns))
    contacts_by_phone = dedupe_contacts([c for c in raw if c])
    total_rows = len(df)
    rows_with_phone = sum(1 for c in raw if c)
    unique_phones = len(contacts_by_phone)

    end_d = date.today()
    start_d = end_d - timedelta(days=RETENTION_DAYS)
    print(
        f"CallRail window: {start_d.isoformat()}–{end_d.isoformat()} "
        f"(last {RETENTION_DAYS + 1} days, inbound only). Fetching pages…"
    )
    calls = fetch_callrail_calls(account_id, api_key, start_d, end_d)
    inbound = [c for c in calls if c.get("direction") == "inbound"]

    matches: list[dict] = []
    source_counter: Counter[str] = Counter()
    for call in inbound:
        cp = call.get("customer_phone_number")
        n = normalize_phone(cp) if cp else None
        if not n or not valid_us_national(n):
            continue
        if n not in contacts_by_phone:
            continue
        con = contacts_by_phone[n]
        src = call.get("source") or call.get("formatted_tracking_source") or ""
        matches.append(
            {
                "phone": n,
                "contact_name": con["contact_name"],
                "call_date": call.get("start_time") or "",
                "call_source": src,
                "campaign": campaign_label(call),
                "call_id": call.get("id"),
            }
        )
        source_counter[src or "(blank)"] += 1

    phones_matched = len({m["phone"] for m in matches})
    rate = (phones_matched / unique_phones * 100) if unique_phones else 0.0

    out_csv = root / "leaddocket_callrail_matches.csv"
    pd.DataFrame(matches).to_csv(out_csv, index=False)

    print()
    print("=== SUMMARY ===")
    print(f"Total contact rows (all workbooks):     {total_rows}")
    print(f"Rows with valid US phone extracted:     {rows_with_phone}")
    print(f"Unique valid phone numbers (deduped):    {unique_phones}")
    print(f"CallRail inbound calls in window:       {len(inbound)}")
    print(f"Matching CallRail rows (call events):   {len(matches)}")
    print(f"Unique LeadDocket phones with a match:   {phones_matched}")
    print(
        "Match rate (unique matched phones / unique LeadDocket phones): "
        f"{rate:.2f}%"
    )
    print()
    print("=== TOP SOURCES (matched calls) ===")
    for src, cnt in source_counter.most_common(15):
        print(f"  {cnt:5d}  {src}")
    print()
    print("Wrote:", out_csv)


if __name__ == "__main__":
    main()
