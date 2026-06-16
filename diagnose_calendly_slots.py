#!/usr/bin/env python3
"""
Diagnostic: dump exactly what the Calendly event_type_available_times endpoint
returns for the Consultation and Accelerator calendars across a date range.

Use when the dashboard reports 0 Open Availability for a day where the public
Calendly booking page clearly shows open slots.

For each date, shows:
  - The exact query window sent to Calendly (UTC)
  - Consultation slot count + first 10 slot times in Pacific
  - Accelerator slot count + first 10 slot times in Pacific
  - Any API errors verbatim (HTTP code + response body, or exception)
  - What the dashboard's current logic would report for that day

Usage:
    export CALENDLY_API_KEY="..."
    python3 diagnose_calendly_slots.py                       # today + 9 days
    python3 diagnose_calendly_slots.py --start 2026-06-16 --end 2026-06-22
"""
import os
import sys
import argparse
from datetime import date, datetime, timezone, timedelta

import requests

CALENDLY_API_KEY = os.environ.get("CALENDLY_API_KEY")
if not CALENDLY_API_KEY:
    sys.exit("ERROR: CALENDLY_API_KEY env var required")

BASE = "https://api.calendly.com"
PACIFIC = timezone(timedelta(hours=-7))  # PDT

# Same URIs the dashboard uses
CONSULTATION_URI = "https://api.calendly.com/event_types/3acb4582-147a-4652-ad6b-5effe4a1b755"
ACCELERATOR_URI  = "https://api.calendly.com/event_types/f1a11c05-d0c0-41b7-aaec-b60bf5d96f39"


def query_calendar(event_type_uri, start_iso, end_iso, label):
    """Returns (slot_count, error_or_None, slot_times_pacific_list)."""
    try:
        r = requests.get(
            f"{BASE}/event_type_available_times",
            headers={"Authorization": f"Bearer {CALENDLY_API_KEY}"},
            params={"event_type": event_type_uri, "start_time": start_iso, "end_time": end_iso},
            timeout=30,
        )
        if r.status_code != 200:
            return 0, f"HTTP {r.status_code}: {r.text[:300]}", []
        slots = r.json().get("collection", [])
        slot_times = []
        for s in slots:
            ts = s.get("start_time")
            if ts:
                try:
                    dt = datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(PACIFIC)
                    slot_times.append(dt.strftime("%I:%M %p"))
                except Exception:
                    slot_times.append(ts)
        return len(slots), None, slot_times
    except Exception as e:
        return 0, f"EXCEPTION {type(e).__name__}: {str(e)[:300]}", []


def run(start_date, end_date):
    print("=" * 78)
    print("  CALENDLY SLOT AVAILABILITY DIAGNOSTIC")
    print(f"  Range: {start_date} → {end_date}")
    print(f"  Now (PT): {datetime.now(PACIFIC).strftime('%Y-%m-%d %H:%M %Z')}")
    print("=" * 78)

    today_pt = datetime.now(PACIFIC).date()
    days = (end_date - start_date).days + 1

    for i in range(days):
        d = start_date + timedelta(days=i)
        is_today = (d == today_pt)

        if is_today:
            start_iso = (datetime.now(timezone.utc) + timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            start_iso = f"{d.isoformat()}T00:00:00Z"
        end_iso = f"{d.isoformat()}T23:59:59Z"

        marker = "  (TODAY)" if is_today else ""
        print(f"\n━━━ {d.strftime('%a %m/%d')}{marker} ━━━")
        print(f"  Query window (UTC): {start_iso} → {end_iso}")

        consult_n, consult_err, consult_slots = query_calendar(CONSULTATION_URI, start_iso, end_iso, "Consultation")
        accel_n, accel_err, accel_slots = query_calendar(ACCELERATOR_URI, start_iso, end_iso, "Accelerator")

        # Consultation row
        print(f"  Consultation:  {consult_n} slots", end="")
        if consult_err:
            print(f"  ⚠ {consult_err}")
        else:
            print()
            if consult_slots:
                preview = ", ".join(consult_slots[:10])
                more = f" ... ({len(consult_slots)} total)" if len(consult_slots) > 10 else ""
                print(f"    times (PT): {preview}{more}")

        # Accelerator row
        print(f"  Accelerator:   {accel_n} slots", end="")
        if accel_err:
            print(f"  ⚠ {accel_err}")
        else:
            print()
            if accel_slots:
                preview = ", ".join(accel_slots[:10])
                more = f" ... ({len(accel_slots)} total)" if len(accel_slots) > 10 else ""
                print(f"    times (PT): {preview}{more}")

        # What dashboard would report under current logic
        if consult_n > 0:
            dashboard_reports = f"{consult_n} (from Consultation)"
        elif accel_n > 0:
            dashboard_reports = f"{accel_n} (Accelerator fallback)"
        elif consult_err or accel_err:
            dashboard_reports = "0 — but an error was silently swallowed for at least one calendar"
        else:
            dashboard_reports = "0 (both calendars genuinely empty)"
        print(f"  → Dashboard would report: {dashboard_reports}")

    print("\n" + "=" * 78)
    print("  Done. Compare 'times (PT)' against what the public Calendly booking")
    print("  page shows for the same dates. Any mismatch points to the root cause.")
    print("=" * 78)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", help="YYYY-MM-DD; defaults to today")
    parser.add_argument("--end",   help="YYYY-MM-DD inclusive; defaults to today + 9")
    args = parser.parse_args()

    today = datetime.now(PACIFIC).date()
    start = date.fromisoformat(args.start) if args.start else today
    end   = date.fromisoformat(args.end)   if args.end   else today + timedelta(days=9)
    run(start, end)
