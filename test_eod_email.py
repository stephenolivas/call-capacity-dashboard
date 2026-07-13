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

import re

# TEST_EMAIL_TO may hold one address or several. Accept common separators
# (comma, semicolon, newline, whitespace) so multi-recipient testing works
# without accidentally jamming addresses into a single string with a newline
# — SMTP rejects those with "folded header contains newline".
_raw_test_to  = os.environ.get("TEST_EMAIL_TO", "")
TEST_EMAIL_TO = [addr.strip() for addr in re.split(r"[,;\s]+", _raw_test_to) if addr.strip()]
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
    We need:
      - Today + tomorrow's booked counts (for the numbers table)
      - Today's lead IDs, with lead_owner (for show rate + per-rep new-call counts)
      - rep_total_meetings + rep_meetings_by_category (for the by-rep breakdown)
    Everything else comes from separate targeted fetches inside build_eod_data
    (won opps, lost status changes, per-lead show_up data)."""
    tomorrow      = today + timedelta(days=1)
    day_after     = today + timedelta(days=2)

    print(f"📥 Fetching field_leads with FSCBD between {today} and {day_after}…")
    field_leads = ud.fetch_field_leads(today, day_after)
    print(f"   ✓ {len(field_leads)} leads returned")

    # Bucket by FSCBD date. Field-based counting (matches the dashboard exactly).
    from datetime import date as _date
    daily = {today: {"booked": 0}, tomorrow: {"booked": 0}}
    valid_meetings   = []
    lead_to_funnel   = {}
    leads_with_fscbd = set()

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
        if fscbd not in (today, tomorrow):
            continue
        daily[fscbd]["booked"] += 1
        # Note: valid_meetings needs "lead_owner" for the by-rep new-call counter.
        valid_meetings.append({
            "lead_id":    lead["id"],
            "date":       fscbd,
            "lead_owner": owner_id,
        })
        # For fetch_rep_total_meetings — same lookup dicts main() builds
        lead_to_funnel[lead["id"]]      = lead.get(ud.FIELD_FUNNEL_NAME_DEAL) or ""
        leads_with_fscbd.add((lead["id"], fscbd))

    print(f"   ✓ Today ({today}) booked: {daily[today]['booked']}")
    print(f"   ✓ Tomorrow ({tomorrow}) booked: {daily[tomorrow]['booked']}")

    # Fetch rep data for today only — the by-rep breakdown section needs it.
    print(f"📥 Fetching per-rep meetings for the by-rep section (today: {today})…")
    rep_total_meetings, rep_meetings_by_category, _non_new = ud.fetch_rep_total_meetings(
        today, tomorrow, ud.ALL_LANE_REPS, lead_to_funnel, leads_with_fscbd
    )
    # Apply the NEW_CALLS_ONLY_REPS clamp — mirrors what build_dashboard_data does.
    # Without this, the preview would over-count self-sourcing reps.
    for uid in ud.NEW_CALLS_ONLY_REPS:
        if uid not in rep_total_meetings:
            continue
        for d, _tot in list(rep_total_meetings[uid].items()):
            # Their total collapses to their new-call count
            new_count_that_day = sum(
                1 for m in valid_meetings
                if m["date"] == d and m["lead_owner"] == uid
            )
            rep_total_meetings[uid][d] = new_count_that_day
            if uid in rep_meetings_by_category and d in rep_meetings_by_category[uid]:
                rep_meetings_by_category[uid][d] = {"fu": 0, "resch": 0}

    return {
        "daily_data":               daily,
        "valid_meetings":           valid_meetings,
        "rep_total_meetings":       rep_total_meetings,
        "rep_meetings_by_category": rep_meetings_by_category,
    }


def main():
    today = datetime.now(ud.PACIFIC).date()
    print(f"═══ EOD Email Preview — {today} ═══")
    print(f"Recipients ({len(TEST_EMAIL_TO)}): {', '.join(TEST_EMAIL_TO)}")
    print()

    rolling_data = build_minimal_rolling_data(today)
    print()

    # send_eod_email already accepts a recipients override — just pass ours.
    # It logs its own status. Any error is caught and logged (won't crash).
    ud.send_eod_email(rolling_data, today, recipients=TEST_EMAIL_TO)

    print()
    print(f"═══ Preview sent — check inboxes at {', '.join(TEST_EMAIL_TO)} ═══")


if __name__ == "__main__":
    main()
