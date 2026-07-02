#!/usr/bin/env python3
"""
test_eod_email.py — Preview the EOD email before it goes out live.

Runs the same build → format → send pipeline as the 8 PM PT production email
but with the recipient overridden to whatever address is in TEST_EMAIL_TO.
Nothing else is committed or written; strictly a one-shot preview.

Reuses the existing update_dashboard.py machinery — no duplicate logic — so
whatever you see in the preview is exactly what the live email will render.

Usage locally:
  export CLOSE_API_KEY=...
  export GMAIL_APP_PASSWORD=...
  export EMAIL_FROM=...
  export TEST_EMAIL_TO=you@example.com
  python test_eod_email.py

Usage via GitHub Actions:
  Actions → "Test EOD Email" → Run workflow.
  Requires a new repo secret TEST_EMAIL_TO with your email address.
"""

import os
import sys
from datetime import datetime, timedelta

# Import the production machinery — we run everything through update_dashboard's
# own functions, only overriding the recipient.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import update_dashboard as ud

TEST_EMAIL_TO = os.environ.get("TEST_EMAIL_TO", "").strip()
if not TEST_EMAIL_TO:
    print("❌ TEST_EMAIL_TO env var is not set. Set it to your email and re-run.")
    sys.exit(1)

if not ud.CLOSE_API_KEY:
    print("❌ CLOSE_API_KEY not set.")
    sys.exit(1)
if not ud.GMAIL_APP_PASSWORD:
    print("❌ GMAIL_APP_PASSWORD not set.")
    sys.exit(1)
if not ud.EMAIL_FROM:
    print("❌ EMAIL_FROM not set.")
    sys.exit(1)


def build_minimal_rolling_data(today):
    """Build the smallest possible rolling_data dict that satisfies build_eod_data.
    We only need today + tomorrow's booked counts, and today's lead IDs (for the
    show rate calc). Everything else in build_eod_data comes from separate fetches
    (won opps, lost status changes, per-lead show_up data)."""
    tomorrow      = today + timedelta(days=1)
    day_after     = today + timedelta(days=2)

    print(f"📥 Fetching field_leads with FSCBD between {today} and {day_after}…")
    field_leads = ud.fetch_field_leads(today, day_after)
    print(f"   ✓ {len(field_leads)} leads returned")

    # Bucket by FSCBD date. Field-based counting (matches the dashboard exactly).
    from datetime import date as _date
    daily = {today: {"booked": 0}, tomorrow: {"booked": 0}}
    valid_meetings = []

    for lead in field_leads:
        if lead.get("status_id") in ud.EXCLUDED_LEAD_STATUS_IDS:
            continue
        owner_id = lead.get(ud.FIELD_LEAD_OWNER)
        if not owner_id or owner_id not in ud.ALL_LANE_REPS:
            continue
        fscbd_str = lead.get(ud.FIELD_FIRST_SALES_CALL)
        if not fscbd_str:
            continue
        try:
            fscbd = _date.fromisoformat(fscbd_str)
        except (ValueError, TypeError):
            continue
        if fscbd == today:
            daily[today]["booked"] += 1
            valid_meetings.append({"lead_id": lead["id"], "date": today})
        elif fscbd == tomorrow:
            daily[tomorrow]["booked"] += 1
            valid_meetings.append({"lead_id": lead["id"], "date": tomorrow})

    print(f"   ✓ Today ({today}) booked: {daily[today]['booked']}")
    print(f"   ✓ Tomorrow ({tomorrow}) booked: {daily[tomorrow]['booked']}")

    return {"daily_data": daily, "valid_meetings": valid_meetings}


def main():
    today = datetime.now(ud.PACIFIC).date()
    print(f"═══ EOD Email Preview — {today} ═══")
    print(f"Recipient (override): {TEST_EMAIL_TO}")
    print()

    rolling_data = build_minimal_rolling_data(today)
    print()

    # send_eod_email already accepts a recipients override — just pass ours.
    # It logs its own status. Any error is caught and logged (won't crash).
    ud.send_eod_email(rolling_data, today, recipients=[TEST_EMAIL_TO])

    print()
    print(f"═══ Preview sent — check inbox at {TEST_EMAIL_TO} ═══")


if __name__ == "__main__":
    main()
