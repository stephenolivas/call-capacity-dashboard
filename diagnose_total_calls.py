#!/usr/bin/env python3
"""
Diagnostic: dump exactly which meetings are being counted as "Total Calls" in the
rep details section of the call capacity dashboard.

Outputs:
  1. Per-rep meeting list with date, title, status, lead_id (so you can spot mis-counts
     like "Sales Huddle" or "Follow Up Block")
  2. Title frequency table across the whole dataset (most-counted titles ranked)
  3. Field inventory on a sample meeting (so we can spot tighter filter signals
     like source_id, external_url, calendar_event_uuid, etc.)
  4. Exclusion summary so you can see what's being filtered and why

Usage:
    export CLOSE_API_KEY="api_..."
    python3 diagnose_total_calls.py                          # last 7 days
    python3 diagnose_total_calls.py --start 2026-05-18 --end 2026-05-24
    python3 diagnose_total_calls.py --csv out.csv            # also dump counted rows to CSV
"""
import os
import sys
import csv
import time
import argparse
from datetime import date, datetime, timezone, timedelta
from collections import defaultdict, Counter

import requests

# ── Config (mirrors update_dashboard.py) ─────────────────────────────────────
CLOSE_API_KEY = os.environ.get("CLOSE_API_KEY")
if not CLOSE_API_KEY:
    sys.exit("ERROR: CLOSE_API_KEY environment variable required")

BASE = "https://api.close.com/api/v1"
THROTTLE = 0.5

LANE_1_REP_NAMES = {
    "user_7F059xEinVentOEvkRMP77fWZyvwUiTRTUOuhD11J0e": "Robin Perkins",
    "user_wF5aATmDljO6g6AHqehRPVmfCmH5j9VszbO6Q6Pjzm4": "Eric Piccione",
    "user_F0VeLnOQlWpkDncNW8rBl1V2QJ08fnDt6DcUjNATUJK": "Scott Seymour",
    "user_pKEujUcHJfsEyI5lM6L56aXM2s5nNOU994JRjRSlAdA": "Chris Wanke",
    "user_fYWHvOuCKDuaQxSp6lROlv2rmvZZYq1kzjGvaF7OrAL": "Jake Skinner",
    "user_wHm1vcLde4RExd3vv9UOjnms5Oz8ssXg8600mQuxMPb": "Christian Hartwell",
    "user_1xDZSeOa8omjfxHXD80twTf8OieXfQ6tNCaYbVygtv1": "Dubem Adindu",
    "user_lUjlATIIgFg8mELa0GFzZUj0lG4Cs7PwQsxbi34I6Su": "Joe Dysert",
}
LANE_2_REP_NAMES = {
    "user_ulI4pdlkBQGJpFBjSfdf3U2deAXQATVPSAurnbL80T9": "Bryan Barcus",
    "user_L0aaUNmM45X52HE7rj3VPWkxhahpoYobhDVAXamQMMD": "Steven Starnes",
    "user_Bov31jjnHhENBy8uWNTTL8KKax8VX7o6DugLzBYOHBG": "Lyle Hubbard",
    "user_WquWudQN7dghZsAPiNY80eJUmg1EadQg2UCQdvgbif7": "Kelly Schrader",
    "user_I0cHZ04mBXXBvbFcnwmsc2KrcMsLsKxqjW8DtJ783Hr": "Elvis Ellis",
    "user_5pAfnzGONQLUVLKqFQVpQ3570YV1gurVCTp1MMgfCDL": "John Kirk",
    "user_UpJb11fzX2TuFHf7fFyWpfXr84lg2Ui7i7p5CtQkIaW": "Cameron Caswell",
    "user_MrBLkl5wCqTm7QxHxPo2ydNV5KxMllg6YZDVc12Aqzj": "Jason Aaron",
}
ALL_REP_NAMES = {**LANE_1_REP_NAMES, **LANE_2_REP_NAMES}
ALL_LANE_USER_IDS = set(ALL_REP_NAMES.keys())

# Same filters used in update_dashboard.py fetch_rep_total_meetings()
EXCLUSION_PATTERNS = [
    "lunch", "break", "ooo", "pto", "out of office",
    "internal", "team meeting", "1:1", "standup", "training",
]
EXCLUDED_STATUSES = {"canceled", "declined"}

PACIFIC = timezone(timedelta(hours=-7))  # PDT in May

# ── API ──────────────────────────────────────────────────────────────────────
def close_get(endpoint, params=None):
    time.sleep(THROTTLE)
    r = requests.get(f"{BASE}/{endpoint}/", auth=(CLOSE_API_KEY, ""), params=params or {}, timeout=60)
    r.raise_for_status()
    return r.json()


def parse_pacific_date(starts_at):
    if not starts_at:
        return None
    try:
        dt = datetime.fromisoformat(starts_at.replace("Z", "+00:00"))
        return dt.astimezone(PACIFIC).date()
    except Exception:
        return None


def parse_pacific_time(starts_at):
    if not starts_at:
        return ""
    try:
        dt = datetime.fromisoformat(starts_at.replace("Z", "+00:00"))
        return dt.astimezone(PACIFIC).strftime("%H:%M")
    except Exception:
        return ""


# ── Core diagnostic ──────────────────────────────────────────────────────────
def run(start_date, end_date, csv_path=None):
    print(f"\n{'='*78}")
    print(f"  TOTAL CALLS DIAGNOSTIC")
    print(f"  Window: {start_date} to {end_date}")
    print(f"  Reps tracked: {len(ALL_REP_NAMES)} (Lane 1 + Lane 2)")
    print(f"{'='*78}\n")

    start_iso = start_date.isoformat()
    end_iso   = (end_date + timedelta(days=1)).isoformat()

    counted = []          # list of dicts — meetings that PASSED all filters
    excluded_meetings = []  # (meeting, reason) — for borderline visibility
    excluded_counts = Counter()
    raw_total = 0
    sample_meeting = None  # for the field inventory dump

    skip = 0
    page = 0
    while True:
        data = close_get("activity/meeting", {
            "date_start__gte": start_iso,
            "date_start__lt":  end_iso,
            "_skip":           skip,
            "_limit":          100,
        })
        batch = data.get("data", [])
        if not batch:
            break
        page += 1
        raw_total += len(batch)
        if sample_meeting is None and batch:
            sample_meeting = batch[0]

        for m in batch:
            md = parse_pacific_date(m.get("starts_at"))
            uid = m.get("user_id")
            lead_id = m.get("lead_id")
            status = (m.get("status") or "").lower()
            title = (m.get("title") or "")

            # Apply same filter chain as production
            if md is None or md < start_date or md > end_date:
                excluded_counts["out_of_range"] += 1
                continue
            if uid not in ALL_LANE_USER_IDS:
                excluded_counts["not_lane_rep"] += 1
                continue
            if not lead_id:
                excluded_counts["no_lead"] += 1
                # Keep a few examples for visibility
                if excluded_counts["no_lead"] <= 5:
                    excluded_meetings.append((m, "no_lead"))
                continue
            if status in EXCLUDED_STATUSES:
                excluded_counts["status"] += 1
                continue
            if any(p in title.lower() for p in EXCLUSION_PATTERNS):
                excluded_counts["title_pattern"] += 1
                if excluded_counts["title_pattern"] <= 5:
                    excluded_meetings.append((m, "title_pattern"))
                continue

            counted.append({
                "rep": ALL_REP_NAMES.get(uid, uid),
                "user_id": uid,
                "date": md,
                "time": parse_pacific_time(m.get("starts_at")),
                "title": title,
                "status": status,
                "lead_id": lead_id,
                "raw": m,
            })

        if not data.get("has_more", False):
            break
        skip += 100

    print(f"Fetched {raw_total} meetings across {page} pages")
    print(f"Counted (passes all filters): {len(counted)}")
    print(f"Excluded breakdown:")
    for reason, n in excluded_counts.most_common():
        print(f"  {reason:<18} {n}")

    # ── 1. Per-rep listing ──────────────────────────────────────────────────
    print(f"\n{'='*78}")
    print("  PER-REP LISTING — what's currently counted")
    print(f"{'='*78}")
    by_rep = defaultdict(list)
    for c in counted:
        by_rep[c["rep"]].append(c)

    for rep in sorted(by_rep.keys()):
        rows = sorted(by_rep[rep], key=lambda r: (r["date"], r["time"]))
        print(f"\n── {rep} ({len(rows)} meetings) ──")
        for r in rows:
            print(f"  {r['date']} {r['time']}  status={r['status']:<10}  lead={r['lead_id']}  title=\"{r['title']}\"")

    reps_with_zero = sorted(set(ALL_REP_NAMES.values()) - set(by_rep.keys()))
    if reps_with_zero:
        print(f"\n── Reps with 0 counted meetings ──")
        for r in reps_with_zero:
            print(f"  {r}")

    # ── 2. Title frequency ──────────────────────────────────────────────────
    print(f"\n{'='*78}")
    print("  TITLE FREQUENCY — sorted high to low")
    print("  Look for: internal-sounding titles ('Sales Huddle', 'Follow Up Block',")
    print("  'Office Hours' etc.) that should probably be excluded.")
    print(f"{'='*78}")
    titles = Counter(c["title"] for c in counted)
    for title, n in titles.most_common():
        print(f"  {n:>4}  {title}")

    # ── 3. Field inventory on a sample ──────────────────────────────────────
    print(f"\n{'='*78}")
    print("  SAMPLE MEETING FIELDS — to spot tighter filter signals")
    print("  Look for: source_id, external_url, calendar_event_uuid, event_type, etc.")
    print(f"{'='*78}")
    if sample_meeting:
        for k in sorted(sample_meeting.keys()):
            v = sample_meeting[k]
            if isinstance(v, (dict, list)):
                v = str(v)[:120] + ("..." if len(str(v)) > 120 else "")
            print(f"  {k:<28} = {v}")
    else:
        print("  (no meetings found in window)")

    # ── 4. Excluded examples ────────────────────────────────────────────────
    if excluded_meetings:
        print(f"\n{'='*78}")
        print("  SAMPLE EXCLUDED MEETINGS — verify nothing legit is being filtered")
        print(f"{'='*78}")
        for m, reason in excluded_meetings:
            md = parse_pacific_date(m.get("starts_at"))
            print(f"  [{reason:<14}] {md} user={m.get('user_id')} lead={m.get('lead_id')} "
                  f"status={m.get('status')} title=\"{m.get('title')}\"")

    # ── 5. CSV export ────────────────────────────────────────────────────────
    if csv_path:
        with open(csv_path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["rep", "user_id", "date", "time_pt", "status", "lead_id", "title", "meeting_id"])
            for c in sorted(counted, key=lambda r: (r["rep"], r["date"], r["time"])):
                w.writerow([c["rep"], c["user_id"], c["date"], c["time"], c["status"],
                            c["lead_id"], c["title"], c["raw"].get("id", "")])
        print(f"\n  CSV written: {csv_path} ({len(counted)} rows)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", help="YYYY-MM-DD; defaults to today - 7")
    parser.add_argument("--end",   help="YYYY-MM-DD inclusive; defaults to today")
    parser.add_argument("--csv",   help="optional path to dump counted meetings as CSV")
    args = parser.parse_args()

    today = date.today()
    start = date.fromisoformat(args.start) if args.start else today - timedelta(days=7)
    end   = date.fromisoformat(args.end)   if args.end   else today
    run(start, end, csv_path=args.csv)
