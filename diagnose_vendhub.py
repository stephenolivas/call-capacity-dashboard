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

    # Gate 2 — FSCBD is today
    fscbd_raw = lead.get("custom.cf_LFdYEQ6bsgp49YjZzefypDmdVx8iwuakWDSLPLpVrBq")
    today_pt  = datetime.now(ud.PACIFIC).date().isoformat()
    is_today  = fscbd_raw == today_pt
    print(f"    FSCBD == today ({today_pt})?  "
          f"{'✓ yes' if is_today else f'✗ NO — FSCBD is {fscbd_raw!r}'}")

    # Gate 3 — VendHub field is populated (any truthy value)
    vh_raw   = lead.get(f"custom.{ud.CF_VENDHUB}")
    vh_type  = type(vh_raw).__name__
    vh_empty = not vh_raw or (isinstance(vh_raw, str) and not vh_raw.strip())
    print(f"    VendHub Call populated?  "
          f"{'✓ yes (type: ' + vh_type + ')' if not vh_empty else '✗ NO — value is falsy'}")

    # Overall
    print()
    would_count = in_lane and is_today and not vh_empty
    if would_count:
        print("  ✅ VERDICT: This lead SHOULD be counted in VendHub Calls.")
        print("     If it isn't, something else is wrong — share this output.")
    else:
        print("  ❌ VERDICT: This lead would NOT be counted. Reason:")
        if not in_lane:
            print("     → Lead owner isn't in ALL_LANE_REPS. Either the lead isn't")
            print("       assigned to a Lane 1/Lane 2 rep, or that rep needs to be")
            print("       added to the lane sets in update_dashboard.py.")
        if not is_today:
            print("     → FSCBD isn't today's date. VendHub aggregation only")
            print("       considers leads with FSCBD == today (Pacific).")
        if vh_empty:
            print("     → VendHub Call field is empty. The dropdown needs to have")
            print("       a value selected — the field can't just be labeled.")


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
