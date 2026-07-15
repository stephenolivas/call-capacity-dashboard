#!/usr/bin/env python3
"""
diagnose_vendhub.py — Inspect specific leads to figure out why they aren't
showing up in the EOD email's VendHub Calls section.

Accepts either **email addresses** or **Close lead_ids** (starting with lead_).
Lead IDs are more reliable — Close's email-based search sometimes 404s.
Grab lead_id from the URL: app.close.com/lead/<LEAD_ID>/

For each lead, this script:
  1. Fetches the lead from Close
  2. Prints the lead's key fields (owner, FSCBD, funnel, VendHub Call, status)
  3. Reports whether the lead would pass each filter gate in build_eod_data
     — Lane filter, FSCBD-is-today filter, VendHub-populated filter

Doesn't send any email or modify anything. Just reads and reports.

Usage (locally):
  CLOSE_API_KEY=xxx python diagnose_vendhub.py lead_pUCvjyj... lead_hvFwM6T... lead_FGaVKrB...
  CLOSE_API_KEY=xxx python diagnose_vendhub.py mberna99@gmail.com

Usage (via GitHub Actions):
  Trigger the "Diagnose VendHub" workflow — paste emails OR lead_ids as the input,
  run, view results in the run logs.
"""

import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import update_dashboard as ud


def find_lead_by_email(email):
    """Find a lead using Close's /lead/ list endpoint with an email query.
    Returns the first matching lead dict, or None if not found.
    (Note: /data/search 404s for query strings; /lead/?query=... is the
    supported search pattern.)"""
    try:
        data = ud.close_get("lead", {
            "query": f'email:"{email}"',
            "_limit": 5,
        })
        results = data.get("data", [])
        if not results:
            return None
        return results[0]  # /lead/ already returns full lead objects
    except Exception as e:
        print(f"    ⚠ search failed: {e}")
        return None


def fetch_lead_by_id(lid):
    """Fetch a lead directly by its Close lead_id."""
    try:
        return ud.close_get(f"lead/{lid}")
    except Exception as e:
        print(f"    ⚠ fetch by id failed: {e}")
        return None


def diagnose(identifier):
    """`identifier` can be either an email address or a Close lead_id (starts with lead_)."""
    print(f"\n{'═' * 70}")
    print(f"  {identifier}")
    print("═" * 70)

    if identifier.startswith("lead_"):
        lead = fetch_lead_by_id(identifier)
    else:
        lead = find_lead_by_email(identifier)

    if not lead:
        print("  ❌ No lead found in Close.")
        return

    lid    = lead.get("id", "(no id)")
    name   = lead.get("display_name") or lead.get("name") or "(no name)"
    status = lead.get("status_label") or "(no status)"
    print(f"  Lead:   {name}  ·  {lid}")
    print(f"  Status: {status}")

    # Raw field values (repr so we see types + weird whitespace)
    fields_to_probe = [
        ("Lead Owner",         ud.CF_LEAD_OWNER_NAME),
        ("FSCBD",              "cf_LFdYEQ6bsgp49YjZzefypDmdVx8iwuakWDSLPLpVrBq"),
        ("Funnel (DEAL)",      ud.CF_FUNNEL_DEAL),
        ("Show Up",            ud.CF_SHOW_UP),
        ("Reactivation Setter", ud.CF_REACT_SETTER),
        ("VendHub Call",       ud.CF_VENDHUB),
    ]
    print()
    print("  Raw field values:")
    for label, cf_id in fields_to_probe:
        val = lead.get(f"custom.{cf_id}")
        print(f"    {label:<20} = {val!r}")

    # Verdict on each filter gate
    print()
    print("  Filter-gate verdicts:")

    # Gate 1 — Owner is in ALL_LANE_REPS
    owner_uid = lead.get(f"custom.{ud.CF_LEAD_OWNER_NAME}") or ""
    in_lane   = owner_uid in ud.ALL_LANE_REPS
    owner_nm  = ud.ALL_LANE_REP_NAMES.get(owner_uid, "(not in ALL_LANE_REP_NAMES)")
    print(f"    Owner in ALL_LANE_REPS?  "
          f"{'✓ yes — ' + owner_nm if in_lane else '✗ NO — this is the drop point'}")

    # Gate 2 — VendHub Call field is populated
    vh_raw   = lead.get(f"custom.{ud.CF_VENDHUB}")
    vh_type  = type(vh_raw).__name__
    vh_empty = not vh_raw or (isinstance(vh_raw, str) and not vh_raw.strip())
    print(f"    VendHub Call populated?  "
          f"{'✓ yes (type: ' + vh_type + ')' if not vh_empty else '✗ NO — value is falsy'}")

    # Gate 3 — Has a meeting today PT (per-lead meeting query, matches production)
    from datetime import timedelta as _td
    lid = lead.get("id")
    day_start_pt = datetime(datetime.now(ud.PACIFIC).year, datetime.now(ud.PACIFIC).month,
                             datetime.now(ud.PACIFIC).day, tzinfo=ud.PACIFIC)
    day_end_pt = day_start_pt + _td(days=1)
    meeting_today = None
    try:
        m_data = ud.close_get("activity/meeting", {"lead_id": lid, "_limit": 50})
        for m in m_data.get("data", []):
            starts_at = m.get("starts_at")
            if not starts_at:
                continue
            status = (m.get("status") or "").lower()
            if status.startswith(("canceled", "declined")):
                continue
            try:
                s_dt = datetime.fromisoformat(starts_at.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                continue
            if day_start_pt <= s_dt < day_end_pt:
                meeting_today = m
                break
    except Exception as e:
        print(f"    ⚠ Meeting query failed: {e}")

    if meeting_today:
        print(f"    Has meeting today?  ✓ yes — '{meeting_today.get('title', '?')}' at {meeting_today.get('starts_at')} "
              f"(status: {meeting_today.get('status')!r})")
    else:
        print(f"    Has meeting today?  ✗ NO — no /activity/meeting/ record with starts_at in today PT")

    # FSCBD (info only)
    fscbd_raw = lead.get("custom.cf_LFdYEQ6bsgp49YjZzefypDmdVx8iwuakWDSLPLpVrBq")
    print(f"    FSCBD (info only, not a gate):  {fscbd_raw!r}")

    # Overall
    print()
    would_count = in_lane and not vh_empty and meeting_today is not None
    if would_count:
        print("  ✅ VERDICT: This lead SHOULD be counted in VendHub Calls.")
        print("     Via the 'VendHub-flagged + meeting today' signal in build_eod_data.")
    else:
        print("  ❌ VERDICT: This lead would NOT be counted. Reason:")
        if not in_lane:
            print("     → Lead owner isn't in ALL_LANE_REPS.")
        if vh_empty:
            print("     → VendHub Call field is empty.")
        if not meeting_today:
            print("     → No meeting today. The /activity/meeting/ endpoint returned no record")
            print("       for this lead with starts_at falling in today PT. This is the same")
            print("       signal build_eod_data uses — so if the meeting exists in Close but")
            print("       isn't showing here, it's likely stored differently (e.g., as a call")
            print("       activity) OR the meeting record has a status like 'canceled'.")


def main():
    if len(sys.argv) < 2:
        print("Usage: python diagnose_vendhub.py email1|lead_id1 [email2|lead_id2 ...]")
        print("       or set VENDHUB_EMAILS env var (comma/newline separated)")
        # Also accept env var so the GitHub Actions workflow can pass a list
        env_val = os.environ.get("VENDHUB_EMAILS", "").strip()
        if not env_val:
            sys.exit(1)
        import re
        identifiers = [e.strip() for e in re.split(r"[,;\s]+", env_val) if e.strip()]
    else:
        identifiers = sys.argv[1:]

    print(f"Diagnosing {len(identifiers)} lead(s)...")
    print(f"Current PT date: {datetime.now(ud.PACIFIC).date().isoformat()}")

    for identifier in identifiers:
        diagnose(identifier)

    print(f"\n{'═' * 70}")
    print("Done.")
    print("═" * 70)


if __name__ == "__main__":
    main()
