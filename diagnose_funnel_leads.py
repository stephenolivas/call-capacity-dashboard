#!/usr/bin/env python3
"""
diagnose_funnel_leads.py — Inspect which leads make up the dashboard's funnel counts.

Use case: dashboard shows 16 Internal Webinar calls for today, but Close shows 17.
This script lists every lead with `First Sales Call Booked Date` on the target
date(s), groups by funnel, and tags each as INCLUDED on the dashboard or EXCLUDED
with the exact reason. Lets you cross-reference Close one-to-one.

Run via GitHub Actions:
  Actions → "Diagnose Funnel Leads" → Run workflow → enter inputs.

Run locally:
  export CLOSE_API_KEY=...
  python diagnose_funnel_leads.py                          # today, all funnels
  python diagnose_funnel_leads.py --date 2026-06-18
  python diagnose_funnel_leads.py --funnel "Internal Webinar"
  python diagnose_funnel_leads.py --all-days               # all 14 days
  python diagnose_funnel_leads.py --no-excluded            # hide the excluded section

The script mirrors update_dashboard.py's exact lead-filtering logic — anything
this script marks INCLUDED is counted on the dashboard; anything EXCLUDED is not.
If those two diverge, the dashboard logic has changed and this script needs to
be updated to match.
"""

import argparse
import os
import sys
import time
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone

import requests

# ─── Config (mirrors update_dashboard.py) ───────────────────────────────────

CLOSE_API_KEY = os.environ.get("CLOSE_API_KEY")
if not CLOSE_API_KEY:
    print("❌ CLOSE_API_KEY environment variable is not set.", file=sys.stderr)
    sys.exit(1)

CLOSE_API_BASE = "https://api.close.com/api/v1"
API_THROTTLE   = 0.3  # seconds between calls (slightly tighter than main run; one-shot)

# Custom field IDs
CF_FIRST_SALES_CALL = "cf_LFdYEQ6bsgp49YjZzefypDmdVx8iwuakWDSLPLpVrBq"
CF_FUNNEL_NAME_DEAL = "cf_xqDQE8fkPsWa0RNEve7hcaxKblCe6489XeZGRDzyPdX"
CF_LEAD_OWNER       = "cf_gOfS9pFwext58oberEegLyix8hZzeHrxhCZOVh3P3rd"

FIELD_FIRST_SALES_CALL = f"custom.{CF_FIRST_SALES_CALL}"
FIELD_FUNNEL_NAME_DEAL = f"custom.{CF_FUNNEL_NAME_DEAL}"
FIELD_LEAD_OWNER       = f"custom.{CF_LEAD_OWNER}"

# Lead statuses that are excluded from dashboard counts
EXCLUDED_LEAD_STATUS_IDS = {
    "stat_hWIGHjzyNpl4YjIFSFz3VK4fp2ny10SFJLKAihmo4KT": "Canceled (by Lead)",
    "stat_YV4ZngDB4IGjLjlOf0YTFEWuKZJ6fhNxVkzQkvKYfdB": "Outside the US",
}

# Lane reps — leads must have an owner in this set to be counted.
LANE_1_REPS = {
    "user_7F059xEinVentOEvkRMP77fWZyvwUiTRTUOuhD11J0e",  # Robin Perkins
    "user_wF5aATmDljO6g6AHqehRPVmfCmH5j9VszbO6Q6Pjzm4",  # Eric Piccione
    "user_F0VeLnOQlWpkDncNW8rBl1V2QJ08fnDt6DcUjNATUJK",  # Scott Seymour
    "user_pKEujUcHJfsEyI5lM6L56aXM2s5nNOU994JRjRSlAdA",  # Chris Wanke (historical)
    "user_fYWHvOuCKDuaQxSp6lROlv2rmvZZYq1kzjGvaF7OrAL",  # Jake Skinner
    "user_wHm1vcLde4RExd3vv9UOjnms5Oz8ssXg8600mQuxMPb",  # Christian Hartwell
    "user_1xDZSeOa8omjfxHXD80twTf8OieXfQ6tNCaYbVygtv1",  # Dubem Adindu
    "user_lUjlATIIgFg8mELa0GFzZUj0lG4Cs7PwQsxbi34I6Su",  # Joe Dysert
    "user_7HSxi55O8q5jO11khvrTcAGoL2nlcoa3kZ6loAY6i78",  # Joseph Vaughan
    "user_Ap8we63okFA5Cw9pvr5xgccvqDlIfisKVtFKt6oBe6p",  # Luis Galarza
    "user_XEbPgLixZy4dhuLp34WogOzCIChkKEnrffDnHlxOnA7",  # Danny Santolaya
    "user_1TKtkacQ7ZMKkcqnmCERikTYWwGltp5XUjEE9Hshple",  # Shreya Bechra
    "user_vyiPzY0qxbLwnW5Ubwae8vY2MLviPuozSTIsEKcyrFE",  # Zac Clover
}
LANE_2_REPS = {
    "user_ulI4pdlkBQGJpFBjSfdf3U2deAXQATVPSAurnbL80T9",  # Bryan Barcus (historical)
    "user_L0aaUNmM45X52HE7rj3VPWkxhahpoYobhDVAXamQMMD",  # Steven Starnes (historical)
    "user_Bov31jjnHhENBy8uWNTTL8KKax8VX7o6DugLzBYOHBG",  # Lyle Hubbard
    "user_WquWudQN7dghZsAPiNY80eJUmg1EadQg2UCQdvgbif7",  # Kelly Schrader
    "user_I0cHZ04mBXXBvbFcnwmsc2KrcMsLsKxqjW8DtJ783Hr",  # Elvis Ellis
    "user_5pAfnzGONQLUVLKqFQVpQ3570YV1gurVCTp1MMgfCDL",  # John Kirk
    "user_UpJb11fzX2TuFHf7fFyWpfXr84lg2Ui7i7p5CtQkIaW",  # Cameron Caswell
    "user_MrBLkl5wCqTm7QxHxPo2ydNV5KxMllg6YZDVc12Aqzj",  # Jason Aaron
    "user_pKEujUcHJfsEyI5lM6L56aXM2s5nNOU994JRjRSlAdA",  # Chris Wanke (LTF Quiz Calendar from 05/18)
}
ALL_LANE_REPS = LANE_1_REPS | LANE_2_REPS

# Owner display names — broader than dashboard's display map: includes removed reps
# so the diagnostic can label every owner even if they've been hidden from rep details.
REP_NAME_FULL = {
    "user_7F059xEinVentOEvkRMP77fWZyvwUiTRTUOuhD11J0e": "Robin Perkins",
    "user_wF5aATmDljO6g6AHqehRPVmfCmH5j9VszbO6Q6Pjzm4": "Eric Piccione",
    "user_F0VeLnOQlWpkDncNW8rBl1V2QJ08fnDt6DcUjNATUJK": "Scott Seymour",
    "user_fYWHvOuCKDuaQxSp6lROlv2rmvZZYq1kzjGvaF7OrAL": "Jake Skinner",
    "user_wHm1vcLde4RExd3vv9UOjnms5Oz8ssXg8600mQuxMPb": "Christian Hartwell",
    "user_1xDZSeOa8omjfxHXD80twTf8OieXfQ6tNCaYbVygtv1": "Dubem Adindu",
    "user_lUjlATIIgFg8mELa0GFzZUj0lG4Cs7PwQsxbi34I6Su": "Joe Dysert",
    "user_7HSxi55O8q5jO11khvrTcAGoL2nlcoa3kZ6loAY6i78": "Joseph Vaughan",
    "user_Ap8we63okFA5Cw9pvr5xgccvqDlIfisKVtFKt6oBe6p": "Luis Galarza",
    "user_XEbPgLixZy4dhuLp34WogOzCIChkKEnrffDnHlxOnA7": "Danny Santolaya",
    "user_1TKtkacQ7ZMKkcqnmCERikTYWwGltp5XUjEE9Hshple": "Shreya Bechra",
    "user_vyiPzY0qxbLwnW5Ubwae8vY2MLviPuozSTIsEKcyrFE": "Zac Clover",
    "user_pKEujUcHJfsEyI5lM6L56aXM2s5nNOU994JRjRSlAdA": "Chris Wanke / LTF Quiz Calendar",
    "user_Bov31jjnHhENBy8uWNTTL8KKax8VX7o6DugLzBYOHBG": "Lyle Hubbard",
    "user_WquWudQN7dghZsAPiNY80eJUmg1EadQg2UCQdvgbif7": "Kelly Schrader",
    "user_I0cHZ04mBXXBvbFcnwmsc2KrcMsLsKxqjW8DtJ783Hr": "Elvis Ellis",
    "user_5pAfnzGONQLUVLKqFQVpQ3570YV1gurVCTp1MMgfCDL": "John Kirk",
    "user_UpJb11fzX2TuFHf7fFyWpfXr84lg2Ui7i7p5CtQkIaW": "Cameron Caswell",
    "user_MrBLkl5wCqTm7QxHxPo2ydNV5KxMllg6YZDVc12Aqzj": "Jason Aaron",
    "user_ulI4pdlkBQGJpFBjSfdf3U2deAXQATVPSAurnbL80T9": "Bryan Barcus (removed)",
    "user_L0aaUNmM45X52HE7rj3VPWkxhahpoYobhDVAXamQMMD": "Steven Starnes (removed)",
}

# Pacific time helper (approximate — for default "today" only; date math is fine)
PACIFIC = timezone(timedelta(hours=-7))  # PDT June default


# ─── HTTP helper ─────────────────────────────────────────────────────────────

session = requests.Session()
session.auth = (CLOSE_API_KEY, "")


def close_get(endpoint, params=None):
    time.sleep(API_THROTTLE)
    url = f"{CLOSE_API_BASE}/{endpoint}"
    for attempt in range(5):
        resp = session.get(url, params=params or {}, timeout=60)
        if resp.status_code == 429:
            wait = float(resp.headers.get("Retry-After", 5))
            print(f"   ⏳ Rate limited (attempt {attempt+1}), waiting {wait}s...", file=sys.stderr)
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.json()
    resp.raise_for_status()


# ─── Lead fetching ───────────────────────────────────────────────────────────

def fetch_field_leads(start_date, end_date):
    """Fetch every lead with FSCBD in [start_date, end_date) — mirrors update_dashboard.py."""
    fields = ",".join([
        "id", "display_name", "name", "status_id", "status_label",
        FIELD_FIRST_SALES_CALL,
        FIELD_FUNNEL_NAME_DEAL,
        FIELD_LEAD_OWNER,
    ])
    query = (
        f'{FIELD_FIRST_SALES_CALL} >= "{start_date.isoformat()}" '
        f'and {FIELD_FIRST_SALES_CALL} < "{end_date.isoformat()}"'
    )
    leads = []
    skip = 0
    while True:
        data = close_get("lead", {
            "_fields": fields,
            "query":   query,
            "_limit":  100,
            "_skip":   skip,
        })
        batch = data.get("data", [])
        leads.extend(batch)
        if not data.get("has_more", False):
            break
        skip += len(batch) or 100
    return leads


# ─── Classification ──────────────────────────────────────────────────────────

def classify_lead(lead):
    """Return (included: bool, reason: str | None) mirroring dashboard's filtering."""
    # Status check first (cheapest, most common exclusion)
    status_id = lead.get("status_id")
    if status_id in EXCLUDED_LEAD_STATUS_IDS:
        return False, f"Status excluded: {EXCLUDED_LEAD_STATUS_IDS[status_id]}"

    # Owner check
    owner_id = lead.get(FIELD_LEAD_OWNER)
    if not owner_id:
        return False, "No Lead Owner set"
    if owner_id not in ALL_LANE_REPS:
        return False, f"Owner not on a lane (id: {owner_id[:14]}…)"

    return True, None


# ─── Pretty printing ─────────────────────────────────────────────────────────

WIDTH_NAME   = 36
WIDTH_OWNER  = 28
WIDTH_FUNNEL = 30


def truncate(s, width):
    if len(s) > width:
        return s[: width - 1] + "…"
    return s


def print_day(target_date, included_by_funnel, excluded_by_funnel, show_excluded):
    inc_total = sum(len(v) for v in included_by_funnel.values())
    exc_total = sum(len(v) for v in excluded_by_funnel.values())
    header_date = target_date.strftime("%a %b %d, %Y")

    print()
    print("═" * 76)
    print(f"  {header_date}")
    print("═" * 76)

    print()
    print(f"  ✓ INCLUDED on dashboard: {inc_total} lead{'s' if inc_total != 1 else ''}")
    if inc_total == 0:
        print("    (none)")
    for funnel in sorted(included_by_funnel.keys(), key=lambda f: (-len(included_by_funnel[f]), f)):
        entries = included_by_funnel[funnel]
        print()
        print(f"  ┌─ {funnel}  ({len(entries)})")
        for e in entries:
            print(f"  │  {truncate(e['name'], WIDTH_NAME):<{WIDTH_NAME}}  {truncate(e['owner'], WIDTH_OWNER):<{WIDTH_OWNER}}")
            print(f"  │     {e['url']}")
        print(f"  └{'─' * 73}")

    if show_excluded:
        print()
        print(f"  ✗ EXCLUDED from dashboard: {exc_total} lead{'s' if exc_total != 1 else ''}")
        if exc_total == 0:
            print("    (none — every lead with FSCBD on this date is counted)")
        for funnel in sorted(excluded_by_funnel.keys(), key=lambda f: (-len(excluded_by_funnel[f]), f)):
            entries = excluded_by_funnel[funnel]
            print()
            print(f"  ┌─ {funnel}  ({len(entries)})")
            for e in entries:
                print(f"  │  {truncate(e['name'], WIDTH_NAME):<{WIDTH_NAME}}  {truncate(e['owner'], WIDTH_OWNER):<{WIDTH_OWNER}}  ← {e['reason']}")
                print(f"  │     {e['url']}")
            print(f"  └{'─' * 73}")


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Inspect what leads make up the dashboard's per-funnel counts."
    )
    parser.add_argument("--date", help="Target date YYYY-MM-DD (default: today PT)")
    parser.add_argument("--funnel", help="Filter to funnels containing this string (case-insensitive)")
    parser.add_argument("--all-days", action="store_true",
                        help="Scan the full 14-day rolling window instead of one date")
    parser.add_argument("--no-excluded", action="store_true",
                        help="Hide the EXCLUDED section (show only counted leads)")
    args = parser.parse_args()

    today = datetime.now(PACIFIC).date()
    if args.all_days:
        target_dates = [today - timedelta(days=4) + timedelta(days=i) for i in range(14)]
    elif args.date:
        try:
            target_dates = [date.fromisoformat(args.date)]
        except ValueError:
            print(f"❌ Bad --date format: {args.date!r}. Use YYYY-MM-DD.", file=sys.stderr)
            sys.exit(2)
    else:
        target_dates = [today]

    funnel_filter = args.funnel.lower() if args.funnel else None
    show_excluded = not args.no_excluded
    target_set    = set(target_dates)

    fetch_start = min(target_dates)
    fetch_end   = max(target_dates) + timedelta(days=1)

    print(f"📥 Fetching leads with FSCBD between {fetch_start} and {fetch_end} (exclusive)…")
    leads = fetch_field_leads(fetch_start, fetch_end)
    print(f"   ✓ {len(leads)} leads returned")
    if funnel_filter:
        print(f"   🔍 Funnel filter: {args.funnel!r}")
    print(f"   📅 Target date{'s' if len(target_dates) > 1 else ''}: "
          f"{', '.join(d.isoformat() for d in target_dates)}")

    # Group: {target_date: {"included": {funnel: [entries]}, "excluded": {funnel: [entries]}}}
    per_day = defaultdict(lambda: {"included": defaultdict(list), "excluded": defaultdict(list)})
    total_skipped_filter = 0
    total_bad_date       = 0

    for lead in leads:
        # FSCBD → target date
        fscbd_str = lead.get(FIELD_FIRST_SALES_CALL)
        if not fscbd_str:
            total_bad_date += 1
            continue
        try:
            fscbd = date.fromisoformat(fscbd_str)
        except (ValueError, TypeError):
            total_bad_date += 1
            continue
        if fscbd not in target_set:
            continue

        # Funnel filter (case-insensitive substring)
        funnel = lead.get(FIELD_FUNNEL_NAME_DEAL) or "(no funnel)"
        if funnel_filter and funnel_filter not in funnel.lower():
            total_skipped_filter += 1
            continue

        # Classify against dashboard's exact rules
        included, reason = classify_lead(lead)
        owner_id = lead.get(FIELD_LEAD_OWNER) or ""
        owner    = REP_NAME_FULL.get(owner_id, f"User {owner_id[:10]}…" if owner_id else "(no owner)")
        name     = lead.get("display_name") or lead.get("name") or "(no name)"
        lead_id  = lead.get("id", "")
        url      = f"https://app.close.com/lead/{lead_id}/"
        entry    = {"name": name, "owner": owner, "url": url}

        bucket = "included" if included else "excluded"
        if not included:
            entry["reason"] = reason
        per_day[fscbd][bucket][funnel].append(entry)

    # Sort entries within each funnel: alpha by lead name
    for d_data in per_day.values():
        for bucket in ("included", "excluded"):
            for funnel in d_data[bucket]:
                d_data[bucket][funnel].sort(key=lambda e: e["name"].lower())

    # Print one section per target date (only days with data unless single-day mode)
    days_to_print = sorted(target_dates) if not args.all_days else sorted(target_dates)
    grand_inc = 0
    grand_exc = 0
    for d in days_to_print:
        b = per_day.get(d, {"included": {}, "excluded": {}})
        inc_n = sum(len(v) for v in b["included"].values())
        exc_n = sum(len(v) for v in b["excluded"].values())
        grand_inc += inc_n
        grand_exc += exc_n
        if args.all_days and inc_n == 0 and exc_n == 0:
            continue  # skip empty days in all-days mode for readability
        print_day(d, b["included"], b["excluded"], show_excluded)

    # Summary footer
    print()
    print("═" * 76)
    print(f"  Summary across {len(days_to_print)} day{'s' if len(days_to_print) != 1 else ''}: "
          f"{grand_inc} included · {grand_exc} excluded")
    if total_skipped_filter:
        print(f"  ({total_skipped_filter} lead{'s' if total_skipped_filter != 1 else ''} hidden by --funnel filter)")
    if total_bad_date:
        print(f"  ({total_bad_date} lead{'s' if total_bad_date != 1 else ''} skipped: missing/bad FSCBD)")
    print("═" * 76)


if __name__ == "__main__":
    main()
