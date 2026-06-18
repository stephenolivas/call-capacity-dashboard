#!/usr/bin/env python3
"""
Call Capacity Dashboard Generator (v13)

New:
- Funnel sections: External / In-House / Uncategorized
- Monthly goals per funnel, divided by working days
- All funnels always displayed (even with 0 counts)
- "AK TikTok" and "Anthony IG" rows (Tik Tok + Anthony IG in Close)
- Count/Goal format (e.g. 7/10) or count-only for funnels without goals
- Archive system: daily snapshots + weekly summaries + monthly summaries
"""

import os
import sys
import json
import re
import time
import calendar
from datetime import datetime, timedelta, date, timezone
from zoneinfo import ZoneInfo
from pathlib import Path
import requests

# ─── Configuration ───────────────────────────────────────────────────────────

CLOSE_API_KEY = os.environ.get("CLOSE_API_KEY", "")
CLOSE_API_BASE = "https://api.close.com/api/v1"
PACIFIC = ZoneInfo("America/Los_Angeles")

CAPACITY_OLD = {0: 57, 1: 60, 2: 60, 3: 60, 4: 60, 5: 4, 6: 0}
CAPACITY_NEW = {0: 42, 1: 42, 2: 42, 3: 42, 4: 42, 5: 0, 6: 0}
CAPACITY_CUTOVER = date(2026, 5, 1)  # New capacity starts on this date

def get_capacity(d):
    """Return static capacity goal for a given date."""
    if d >= CAPACITY_CUTOVER:
        return CAPACITY_NEW[d.weekday()]
    return CAPACITY_OLD[d.weekday()]

# ─── Calendly Integration (Staging) ──────────────────────────────────────────

CALENDLY_API_KEY = os.environ.get("CALENDLY_API_KEY", "")
CALENDLY_API_BASE = "https://api.calendly.com"
CAPACITY_CACHE_FILE = "capacity_cache.json"

# Team calendar URIs — Consultation has 2-3 day window, Accelerator has 5-day window
# They share rep availability, so we query Consultation first and only use
# Accelerator for days beyond Consultation's range (to avoid double-counting).
CALENDLY_CONSULTATION_URI = "https://api.calendly.com/event_types/3acb4582-147a-4652-ad6b-5effe4a1b755"
CALENDLY_ACCELERATOR_URI = "https://api.calendly.com/event_types/f1a11c05-d0c0-41b7-aaec-b60bf5d96f39"


def calendly_get(endpoint, params=None):
    url = endpoint if endpoint.startswith("http") else f"{CALENDLY_API_BASE}{endpoint}"
    resp = requests.get(url, headers={
        "Authorization": f"Bearer {CALENDLY_API_KEY}",
        "Content-Type": "application/json",
    }, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json()


def fetch_calendly_available_slots(dates):
    """Fetch available time slots from Calendly team calendars per day.
    Uses Consultation calendar first (2-3 day window). For days where
    Consultation returns 0, falls back to Accelerator (5-day window).
    Calendars share availability, so we never sum them.

    Query window uses PACIFIC date boundaries (midnight PT → next midnight PT)
    converted to UTC, not UTC date boundaries — otherwise late-evening PT runs
    produce queries where start_time is in the past or end_time is before
    start_time, and Calendly returns HTTP 400.

    Returns: {date_obj: int} — available slot count per day.
    """
    if not CALENDLY_API_KEY:
        log("   ⚠ CALENDLY_API_KEY not set — skipping Calendly")
        return {}

    log("📅 Fetching Calendly available slots (team calendars)...")
    now_utc = datetime.now(timezone.utc)
    now_pacific = datetime.now(PACIFIC)
    today_pacific = now_pacific.date()
    result = {}

    for d in dates:
        # Build PT-day boundary in UTC: PT midnight on d → PT midnight on d+1
        day_start_pt = datetime.combine(d, datetime.min.time(), tzinfo=PACIFIC)
        day_end_pt   = day_start_pt + timedelta(days=1)
        day_start_utc = day_start_pt.astimezone(timezone.utc)
        day_end_utc   = day_end_pt.astimezone(timezone.utc) - timedelta(seconds=1)

        # If PT day hasn't started yet → query full PT day in UTC
        # If PT day is in progress → start from now + 1min buffer
        # If PT day has fully ended → skip (no future slots possible)
        if now_utc >= day_end_utc:
            result[d] = 0
            log(f"   {d.strftime('%a %m/%d')}: PT day already ended → 0")
            continue
        start_dt = max(day_start_utc, now_utc + timedelta(minutes=1))
        end_dt   = day_end_utc
        start = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        end   = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        is_today = d == today_pacific

        # Try Consultation first
        consult_count = 0
        consult_error = None
        try:
            data = calendly_get(
                f"{CALENDLY_API_BASE}/event_type_available_times",
                {"event_type": CALENDLY_CONSULTATION_URI, "start_time": start, "end_time": end}
            )
            consult_count = len(data.get("collection", []))
        except Exception as e:
            consult_error = str(e)[:200]

        if consult_count > 0:
            result[d] = consult_count
            log(f"   {d.strftime('%a %m/%d')}{' (TODAY)' if is_today else ''}: {consult_count} slots (Consultation)")
            continue

        # Consultation returned 0 — try Accelerator
        accel_count = 0
        accel_error = None
        try:
            data = calendly_get(
                f"{CALENDLY_API_BASE}/event_type_available_times",
                {"event_type": CALENDLY_ACCELERATOR_URI, "start_time": start, "end_time": end}
            )
            accel_count = len(data.get("collection", []))
        except Exception as e:
            accel_error = str(e)[:200]

        if accel_count > 0:
            result[d] = accel_count
            log(f"   {d.strftime('%a %m/%d')}{' (TODAY)' if is_today else ''}: {accel_count} slots (Accelerator fallback)")
        else:
            result[d] = 0
            # Always surface errors — silent swallowing here is what hid this exact bug
            err_parts = []
            if consult_error: err_parts.append(f"Consult ERR: {consult_error}")
            if accel_error:   err_parts.append(f"Accel ERR: {accel_error}")
            err_suffix = (" — " + " | ".join(err_parts)) if err_parts else ""
            log(f"   {d.strftime('%a %m/%d')}{' (TODAY)' if is_today else ''}: 0 slots (no availability){err_suffix}")

    if not result:
        log("   ⚠ No available slots found (may be outside scheduling window)")
    return result


def load_capacity_cache():
    try:
        with open(CAPACITY_CACHE_FILE, "r") as f:
            raw = json.load(f)
        return {date.fromisoformat(k): v for k, v in raw.items()}
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_capacity_cache(cache):
    cutoff = date.today() - timedelta(days=30)
    trimmed = {k.isoformat(): v for k, v in cache.items() if k >= cutoff}
    with open(CAPACITY_CACHE_FILE, "w") as f:
        json.dump(trimmed, f, indent=2)
    log(f"   💾 Capacity cache saved ({len(trimmed)} days)")


def fetch_calendly_calendar_source(dates):
    """Fetch scheduled events from Calendly grouped by event name per day.
    Returns: {date_obj: {event_name: count}}
    Used for the Calendar Source section in the day detail panel.
    """
    if not CALENDLY_API_KEY:
        return {}

    log("📅 Fetching Calendly calendar source data...")
    try:
        user_data = calendly_get(f"{CALENDLY_API_BASE}/users/me")
        org_uri = user_data["resource"]["current_organization"]
    except Exception as e:
        log(f"   ⚠ Could not get Calendly org: {e}")
        return {}

    result = {}
    for d in dates:
        start = f"{d.isoformat()}T00:00:00Z"
        end = f"{d.isoformat()}T23:59:59Z"
        try:
            sched = calendly_get(f"{CALENDLY_API_BASE}/scheduled_events", {
                "organization": org_uri,
                "min_start_time": start,
                "max_start_time": end,
                "status": "active",
                "count": 100,
            })
            by_name = {}
            for ev in sched.get("collection", []):
                name = ev.get("name", "Unknown")
                by_name[name] = by_name.get(name, 0) + 1
            if by_name:
                result[d] = by_name
                total = sum(by_name.values())
                log(f"   {d.strftime('%a %m/%d')}: {total} events across {len(by_name)} calendar types")
        except Exception as e:
            pass

    return result

EXCLUDED_USER_IDS = {
    "user_EmhqCmaHERTfgfWnPADiLGEqQw3ENvRYd3u1VEmblIp",
    "user_4sfuKGMbv0LQZ4hpS8ipASv406kKTSNP5Xx79jOwSqM",
    "user_5cZRqXu8kb4O1IeBVA98UMcMEhYZUhx1fnCHfSL0YMV",
    "user_yRF070m26JE67J6CJqzkAB3IqY7btNm1K5RisCglKa6",
}

# Setter/discovery users — excluded from main count but tracked for LTF detail
SETTER_USER_IDS = {
    "user_EmhqCmaHERTfgfWnPADiLGEqQw3ENvRYd3u1VEmblIp",  # Kristin Nelson
    "user_4sfuKGMbv0LQZ4hpS8ipASv406kKTSNP5Xx79jOwSqM",  # Spencer Reynolds
}

# Non-sales users — truly excluded from everything (no setter tracking either)
HARD_EXCLUDED_USER_IDS = {
    "user_5cZRqXu8kb4O1IeBVA98UMcMEhYZUhx1fnCHfSL0YMV",  # Stephen Olivas
    "user_yRF070m26JE67J6CJqzkAB3IqY7btNm1K5RisCglKa6",  # Ahmad Bukhari
}

FIELD_FUNNEL_NAME_DEAL = "custom.cf_xqDQE8fkPsWa0RNEve7hcaxKblCe6489XeZGRDzyPdX"
FIELD_FIRST_SALES_CALL = "custom.cf_LFdYEQ6bsgp49YjZzefypDmdVx8iwuakWDSLPLpVrBq"
FIELD_LEAD_OWNER       = "custom.cf_gOfS9pFwext58oberEegLyix8hZzeHrxhCZOVh3P3rd"
FIELD_REACTIVATION_SETTER = "custom.cf_vz6kNiu4ItFxRA8Y9HKlWIoQMq3TsdaQqKekQ2YuxVk"  # only meaningful on Reactivation Scrapers leads
LEAD_FIELDS = ",".join(["id", "display_name", "name", "status_id", FIELD_FUNNEL_NAME_DEAL])

# Lane 1 reps — Christian Hartwell is Lane 1 Lead
LANE_1_REPS = {
    "user_7F059xEinVentOEvkRMP77fWZyvwUiTRTUOuhD11J0e",  # Robin Perkins
    "user_wF5aATmDljO6g6AHqehRPVmfCmH5j9VszbO6Q6Pjzm4",  # Eric Piccione
    "user_F0VeLnOQlWpkDncNW8rBl1V2QJ08fnDt6DcUjNATUJK",  # Scott Seymour
    "user_pKEujUcHJfsEyI5lM6L56aXM2s5nNOU994JRjRSlAdA",  # Chris Wanke
    "user_fYWHvOuCKDuaQxSp6lROlv2rmvZZYq1kzjGvaF7OrAL",  # Jake Skinner
    "user_wHm1vcLde4RExd3vv9UOjnms5Oz8ssXg8600mQuxMPb",  # Christian Hartwell (Lead)
    "user_1xDZSeOa8omjfxHXD80twTf8OieXfQ6tNCaYbVygtv1",  # Dubem Adindu
    "user_lUjlATIIgFg8mELa0GFzZUj0lG4Cs7PwQsxbi34I6Su",  # Joe Dysert (overflow — see LANE_FUNNEL_RESTRICTIONS)
    "user_7HSxi55O8q5jO11khvrTcAGoL2nlcoa3kZ6loAY6i78",  # Joseph Vaughan
    "user_Ap8we63okFA5Cw9pvr5xgccvqDlIfisKVtFKt6oBe6p",  # Luis Galarza
    "user_XEbPgLixZy4dhuLp34WogOzCIChkKEnrffDnHlxOnA7",  # Danny Santolaya
    "user_1TKtkacQ7ZMKkcqnmCERikTYWwGltp5XUjEE9Hshple",  # Shreya Bechra
}
LANE_1_REP_NAMES = {
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
    # Chris Wanke removed from rep details (no longer with company) — user_id retained in LANE_1_REPS so historical calls still count
}
LANE_1_LEAD = "user_wHm1vcLde4RExd3vv9UOjnms5Oz8ssXg8600mQuxMPb"  # Christian Hartwell

# Lane 2 reps — Jason Aaron is Lane 2 Lead
LANE_2_REPS = {
    "user_ulI4pdlkBQGJpFBjSfdf3U2deAXQATVPSAurnbL80T9",  # Bryan Barcus
    "user_L0aaUNmM45X52HE7rj3VPWkxhahpoYobhDVAXamQMMD",  # Steven Starnes
    "user_Bov31jjnHhENBy8uWNTTL8KKax8VX7o6DugLzBYOHBG",  # Lyle Hubbard
    "user_WquWudQN7dghZsAPiNY80eJUmg1EadQg2UCQdvgbif7",  # Kelly Schrader
    "user_I0cHZ04mBXXBvbFcnwmsc2KrcMsLsKxqjW8DtJ783Hr",  # Elvis Ellis
    "user_5pAfnzGONQLUVLKqFQVpQ3570YV1gurVCTp1MMgfCDL",  # John Kirk
    "user_UpJb11fzX2TuFHf7fFyWpfXr84lg2Ui7i7p5CtQkIaW",  # Cameron Caswell
    "user_MrBLkl5wCqTm7QxHxPo2ydNV5KxMllg6YZDVc12Aqzj",  # Jason Aaron (Lead)
    "user_pKEujUcHJfsEyI5lM6L56aXM2s5nNOU994JRjRSlAdA",  # Chris Wanke → "LTF Quiz Calendar - General" (from 05/18)
}
LANE_2_REP_NAMES = {
    "user_Bov31jjnHhENBy8uWNTTL8KKax8VX7o6DugLzBYOHBG": "Lyle Hubbard",
    "user_WquWudQN7dghZsAPiNY80eJUmg1EadQg2UCQdvgbif7": "Kelly Schrader",
    "user_I0cHZ04mBXXBvbFcnwmsc2KrcMsLsKxqjW8DtJ783Hr": "Elvis Ellis",
    "user_5pAfnzGONQLUVLKqFQVpQ3570YV1gurVCTp1MMgfCDL": "John Kirk",
    "user_UpJb11fzX2TuFHf7fFyWpfXr84lg2Ui7i7p5CtQkIaW": "Cameron Caswell",
    "user_MrBLkl5wCqTm7QxHxPo2ydNV5KxMllg6YZDVc12Aqzj": "Jason Aaron",
    "user_pKEujUcHJfsEyI5lM6L56aXM2s5nNOU994JRjRSlAdA": "LTF Quiz Calendar - General",
    # Bryan Barcus and Steven Starnes removed from rep details (no longer with company) — user_ids retained in LANE_2_REPS so historical calls still count
}
LANE_2_LEAD = "user_MrBLkl5wCqTm7QxHxPo2ydNV5KxMllg6YZDVc12Aqzj"  # Jason Aaron

# ── Single-Team Mode (Phase 1 lane merge) ────────────────────────────────────
# As of 2026-06-16 the Lane 1 / Lane 2 split was retired in the dashboard UI.
# Lane 2 has evolved into a scraper role (self-sourced calls). All call counting,
# rep details, and the EOD email now operate on a single combined team.
# The LANE_1_* / LANE_2_* constants above are PRESERVED as historical reference
# in case we ever need to re-split — but the dashboard now uses ALL_LANE_REPS
# and ALL_LANE_REP_NAMES below for everything visible.
ALL_LANE_REPS      = LANE_1_REPS | LANE_2_REPS
ALL_LANE_REP_NAMES = {**LANE_2_REP_NAMES, **LANE_1_REP_NAMES}  # LANE_1 wins on Chris Wanke / LTF Quiz Calendar conflict
ALL_LANE_LEAD      = LANE_1_LEAD  # Christian Hartwell continues as team lead badge

# ── New-Calls-Only Reps (Lane 2 transition mode) ────────────────────────────
# These reps' "Total Meetings" displays clamp to their "New Calls" count instead
# of including follow-ups, reschedules, Q&A, etc. Affects two surfaces:
#   1. Top-section "Total Meetings Booked" row (sums lane-wide)
#   2. Rep Details "Total Calls" row (per-rep)
# Reason: Lane 2 is evolving into a scraper-focused team taking self-sourced
# calls; showing their full meeting volume creates more noise than signal for
# sales/marketing leadership during this transition. Verified with head of sales.
NEW_CALLS_ONLY_REPS = {
    "user_I0cHZ04mBXXBvbFcnwmsc2KrcMsLsKxqjW8DtJ783Hr",  # Elvis Ellis
    "user_WquWudQN7dghZsAPiNY80eJUmg1EadQg2UCQdvgbif7",  # Kelly Schrader
    "user_UpJb11fzX2TuFHf7fFyWpfXr84lg2Ui7i7p5CtQkIaW",  # Cameron Caswell
    "user_MrBLkl5wCqTm7QxHxPo2ydNV5KxMllg6YZDVc12Aqzj",  # Jason Aaron
    "user_Bov31jjnHhENBy8uWNTTL8KKax8VX7o6DugLzBYOHBG",  # Lyle Hubbard
}

# ── Date-Based Lane Transitions ──────────────────────────────────────────────
# Chris Wanke moves from Lane 1 to Lane 2 for calls booked on 05/18+
# He appears in BOTH lane sets; the date-based filter handles which lane counts him.
LANE_TRANSITIONS = {
    "user_pKEujUcHJfsEyI5lM6L56aXM2s5nNOU994JRjRSlAdA": {  # Chris Wanke
        "cutover": date(2026, 5, 18),
        "from_lane": "Lane 1",
        "to_lane": "Lane 2",
    },
}

# ── Funnel-Restricted Reps ───────────────────────────────────────────────────
# Reps whose calls only count for specific funnels (and optional date range).
# Outside the funnel allowlist OR outside the date range = call is excluded.
# Used for overflow situations where someone takes calls but isn't a permanent lane rep.
LANE_FUNNEL_RESTRICTIONS = {
    "user_lUjlATIIgFg8mELa0GFzZUj0lG4Cs7PwQsxbi34I6Su": {  # Joe Dysert
        "funnels": {"Internal Webinar"},
        "since": date(2026, 5, 18),  # inclusive
        "until": date(2026, 5, 24),  # inclusive — overflow for week of 5/18
    },
}

def passes_funnel_restriction(user_id, funnel, call_date):
    """True if the user has no restriction, or if (funnel, date) matches the restriction.
    Returns False when a restriction exists and the call falls outside it — meaning the
    call should NOT be counted at all."""
    r = LANE_FUNNEL_RESTRICTIONS.get(user_id)
    if not r:
        return True
    since = r.get("since")
    until = r.get("until")
    if since and call_date < since: return False
    if until and call_date > until: return False
    return funnel in r.get("funnels", set())


# ── Tiered Capacity Target Schedule ──────────────────────────────────────────
# Daily Mon-Fri booking target that drives the "Capacity to Target %" metric.
# Each entry = (effective_date, target). The most recent effective_date <= the
# given date wins. Add new tiers here when revenue goals change — historical
# dates keep their original target.
CAPACITY_TARGET_SCHEDULE = [
    (date(2026, 6, 15), 44),
    (date(2026, 6, 8),  40),
    (date(2026, 6, 1),  35),
]
DEFAULT_CAPACITY_TARGET = 42  # used for any date before the earliest schedule entry

def get_capacity_target(d):
    """Returns the Capacity Target in effect on date d. Weekends return None
    (no target on Sat/Sun)."""
    if d.weekday() >= 5:
        return None
    for eff_date, target in CAPACITY_TARGET_SCHEDULE:  # sorted by effective_date desc
        if d >= eff_date:
            return target
    return DEFAULT_CAPACITY_TARGET

# Lead statuses excluded from capacity count (matches rep scorecard methodology)
EXCLUDED_LEAD_STATUS_IDS = {
    "stat_hWIGHjzyNpl4YjIFSFz3VK4fp2ny10SFJLKAihmo4KT",  # Canceled (by Lead)
    "stat_YV4ZngDB4IGjLjlOf0YTFEWuKZJ6fhNxVkzQkvKYfdB",  # Outside the US
}

OUTPUT_FILE = os.environ.get("OUTPUT_FILE", "index.html")
ARCHIVE_DIR = os.environ.get("ARCHIVE_DIR", "archive")

# ─── Changelog & Steering Committee ──────────────────────────────────────────
# Each entry: {"date": "YYYY-MM-DD HH:MM PT", "notes": ["bullet 1", "bullet 2"]}

CHANGELOG_ENTRIES = [
    {"date": "2026-06-16 1:30 PM PT", "notes": [
        "Dashboard redesign at the top — the Capacity Metrics table has been replaced with a new hero card row showing focused day-by-day stats. Three cards visible at a time: previous day on the left, focused day in the middle, next day on the right. Click the arrows (or click a side card directly) to navigate forward or back across the 13-day window.",
        "Each card shows: New Meetings Booked (large headline number), then Total Meetings Booked broken down into F/U Meetings, Reschedule Meetings, and Other (catch-all that includes new calls + anything else). Below that: Open Calendar Slots with Booking Window Missed as a sub-row. Card footer shows New Meetings Target as a percentage with red/amber/green color thresholds.",
        "Click the focused (center) card to open the full day detail panel with funnel breakdown, rep breakdown, calendar source, and when-booked stats — same panel that's always been there.",
        "Funnel Breakdown and Rep Details tables now have clickable date column headers as a secondary entry point to that detail panel.",
        "F/U / Reschedule classification is based on meeting title (case-insensitive substring match: 'F/U', 'Follow-Up', 'Follow Up', 'Followup', 'Reschedule', 'Resched', 'Resch'). Anything that doesn't match either pattern falls into 'Other' — so Other will be the largest bucket, since it includes all new sales calls plus Q&A / onboarding / etc.",
    ]},
    {"date": "2026-06-16 9:00 AM PT", "notes": [
        "New funnel added to the In-House Funnel Breakdown: 'LTF - In-House'. No monthly goal set yet — let me know if/when one should be configured.",
    ]},
    {"date": "2026-06-15 7:45 PM PT", "notes": [
        "Calendly availability fix: query window now uses Pacific date boundaries (midnight PT → next midnight PT, converted to UTC) instead of UTC date boundaries. Late-evening runs (after 5 PM PT) were producing Calendly queries with start_time in the past, which the API rejected with HTTP 400 — silently swallowed by the dashboard and shown as '0 open slots'. Affected today + tomorrow during PT evening runs.",
        "Error logging fix: Calendly API errors now log for every day, not just today. The original silent-error swallowing for non-today days is what hid this bug.",
    ]},
    {"date": "2026-06-01 1:15 PM PT", "notes": [
        "Funnel Breakdown order swapped — In-House now renders above External (Uncategorized stays at the bottom).",
    ]},
    {"date": "2026-06-01 1:00 PM PT", "notes": [
        "Added new Lane 1 reps: Joseph Vaughan, Luis Galarza, Danny Santolaya, Shreya Bechra. Their leads will be counted toward Lane 1 totals, they'll appear in Rep Details with funnel breakdowns, and their meetings will be picked up by the Total Calls row.",
    ]},
    {"date": "2026-06-01 11:30 AM PT", "notes": [
        "Reactivation Scrapers funnel row is now expandable — click to drill down by setter (Reactivation - Setter Name field). Collapsed by default. Setters sorted by total descending; missing setter values appear as 'Unknown Setter' for visibility.",
    ]},
    {"date": "2026-06-01 9:00 AM PT", "notes": [
        "Capacity Target is now tiered by date: 35 (06/01–06/07), 40 (06/08–06/14), 44 (06/15 onward). Historical dates before 06/01 keep the prior 42.",
        "New row in Capacity Metrics: 'Total Meetings Booked' — all meetings on this lane's reps' calendars (first calls + follow-ups + reschedules etc.), lane-filtered. Same data source as the Total Calls row in Rep Details.",
        "'Booked' row renamed to 'New Calls Booked' to disambiguate from the new total above. Same data, same calculation — drives Calendar Capacity % and Capacity to Target %.",
    ]},
    {"date": "2026-05-28 3:00 PM PT", "notes": [
        "Rep details: Chris Wanke (Lane 1), Bryan Barcus and Steven Starnes (Lane 2) removed — no longer with the company",
        "Their historical calls still count in lane Booked totals, funnel breakdowns, and Calendar Capacity — only their individual rep rows in the Rep Details section are hidden",
    ]},
    {"date": "2026-05-28 1:30 PM PT", "notes": [
        "Total Calls row accuracy fix: now correctly excludes cancelled and declined meetings (was missing Close's variants like 'declined-by-org', 'canceled-by-lead', etc.)",
        "Total Calls row dedup fix: multi-invitee meetings (one calendar event with multiple lead records) now count once instead of once per lead",
    ]},
    {"date": "2026-05-21 11:00 AM PT", "notes": [
        "Joe Dysert added to Lane 1 as funnel-restricted overflow rep for Internal Webinar calls (week of 05/18–05/24 only)",
        "His calls only count when Funnel Name DEAL = Internal Webinar; outside the date window his calls don't count at all (he's not a permanent Lane 1 rep)",
        "New LANE_FUNNEL_RESTRICTIONS config block lets us add similar overflow assignments later without touching downstream logic",
    ]},
    {"date": "2026-05-18 4:30 PM PT", "notes": [
        "New row in Rep Details: Total Calls — counts ALL meetings on each rep's calendar (first calls + follow-ups + reschedules + Q&A), excluding internal/admin events (lunches, breaks, OOO, 1:1s, etc.). Helps reconcile what the dashboard counts vs. what shows on a sales manager's calendar view.",
        "Filters: meeting must have a lead attached, not canceled/declined, title must not match admin patterns.",
    ]},
    {"date": "2026-05-18 2:00 PM PT", "notes": [
        "New row: Capacity Target — 42 meetings Mon-Fri, the daily count we need to hit revenue goals",
        "New row: Capacity to Target % — Booked vs. Capacity Target (42), styled as the summary row at the bottom of the block. Red <75%, amber 75-89%, green ≥90%",
        "Kept Calendar Capacity % — still shows Booked vs. Calendar Availability for capacity-fullness signal",
        "Weekends show '–' for Capacity Target and 'N/A' for Capacity to Target %",
    ]},
    {"date": "2026-05-12 2:00 PM PT", "notes": [
        "Calendar Availability now uses pre-day snapshot (captured the night before, frozen throughout the day)",
        "New row: Booking Window Missed — shows slots that expired without being booked (today + trailing only)",
        "Added Dubem Adindu to Lane 1 reps",
    ]},
    {"date": "2026-05-11 6:00 PM PT", "notes": [
        "Calendly integration live: Calendar Availability, Open Availability, and Calendar Capacity now use real calendar data",
        "Queries team calendars (Vendingprenuers Consultation + Vending Accelerator Call) for actual available slots",
        "Day detail panel Calendar Source shows event name breakdown from Close meeting data",
    ]},
    {"date": "2026-05-11 4:00 PM PT", "notes": [
        "Day Detail Panel: click any day column to see funnel %, rep breakdown, and when calls were booked",
        "Top 4 funnels + 'Other' bucket with visual bar chart in panel",
        "Calendar Source section placeholder (Calendly integration in progress)",
    ]},
    {"date": "2026-05-11 2:00 PM PT", "notes": [
        "Added Changelog and Steering Committee Updates pages",
        "Changelog link added to dashboard header for transparency",
    ]},
    {"date": "2026-05-09 11:00 AM PT", "notes": [
        "EOD email reverted to all-reps data (no lane filter) — subject stays 'EOD Stats {date}'",
        "Removed Instagram Setter from External funnel list",
    ]},
    {"date": "2026-05-08 3:00 PM PT", "notes": [
        "Dual-lane toggle: Lane 1 / Lane 2 buttons with instant switching",
        "Dynamic funnels: only rows with ≥1 booked call appear",
        "Rep Details section with per-rep funnel breakdown and Lead badges",
    ]},
    {"date": "2026-05-08 10:00 AM PT", "notes": [
        "Lane 2 capacity shows '–' and 'N/A' (no capacity tracking for Lane 2)",
        "Lane toggle buttons styled with dashboard green (#1b7a2e)",
        "Jason Aaron moved to Lane 2 only",
    ]},
    {"date": "2026-05-07 4:00 PM PT", "notes": [
        "Added Lane 1 rep filter — dashboard counts only Lane 1 leads via Lead Owner field",
        "Title updated to 'Call Capacity Dashboard' (Lane buttons indicate active view)",
    ]},
    {"date": "2026-05-07 10:00 AM PT", "notes": [
        "Split AK TikTok and Anthony IG into separate funnel rows (1/day each)",
        "LTF - Quiz Funnel added to External section",
        "MTD Funnel link updated to mtd-funnel-dashboard",
    ]},
    {"date": "2026-05-01 9:00 AM PT", "notes": [
        "Capacity changed from 57-60/day to 42/day (Mon-Fri) starting 05/01",
        "Saturday/Sunday capacity set to 0",
    ]},
    {"date": "2026-04-15 2:00 PM PT", "notes": [
        "Migrated from meeting title classification to First Sales Call Booked Date field",
        "Field-based counting eliminates dedup issues and UTC mismatches",
    ]},
]

STEERING_COMMITTEE_ENTRIES = [
    # User provides these — format: {"date": "YYYY-MM-DD", "notes": ["bullet 1", ...]}
    # Example:
    # {"date": "2026-05-10", "notes": ["Adjusted lead scoring threshold from 50 to 65", "Added new calendar routing for LTF Quiz Funnel"]},
]
API_THROTTLE = 0.5

# ─── Funnel Configuration ───────────────────────────────────────────────────
# Each entry: display_name, close_values (list), monthly_goal (None = no goal), section
# close_values is a list of Close field values that map to this funnel row
# "AK TikTok" and "Anthony IG" are separate rows (Tik Tok + Anthony IG in Close)

FUNNEL_CONFIG = [
    # ── External ──
    {"name": "Low Ticket Funnel",       "close_values": ["Low Ticket Funnel"], "monthly_goal": 400, "section": "external"},
    {"name": "LTF - Quiz Funnel",       "close_values": ["LTF - Quiz Funnel"], "monthly_goal": None, "section": "external"},
    {"name": "Instagram",               "close_values": ["Instagram"],         "monthly_goal": 240, "section": "external"},
    {"name": "YouTube",                 "close_values": ["YouTube - OG - Cam"],"monthly_goal": 132, "section": "inhouse"},
    {"name": "X",                       "close_values": ["X"],                 "monthly_goal": 30,  "section": "external"},
    {"name": "Linkedin",                "close_values": ["Linkedin"],          "monthly_goal": 30,  "section": "external"},
    {"name": "Meta Ads",                "close_values": ["Meta Ads"],          "monthly_goal": 44,  "section": "inhouse"},
    # ── In-House ──
    {"name": "VSL",                     "close_values": ["VSL"],               "monthly_goal": 110, "section": "inhouse"},
    {"name": "Website",                 "close_values": ["Website"],           "monthly_goal": 100, "section": "inhouse"},
    {"name": "Internal Webinar",        "close_values": ["Internal Webinar"],  "monthly_goal": 70,  "section": "inhouse"},
    {"name": "LTF - In-House",          "close_values": ["LTF - In-House"],    "monthly_goal": None, "section": "inhouse"},
    {"name": "Mike Newsletter",           "close_values": ["Mike Newsletter"],   "monthly_goal": 10,  "section": "inhouse"},
    {"name": "AK TikTok",                "close_values": ["Tik Tok"],           "monthly_goal": 22, "section": "inhouse"},
    {"name": "Anthony IG",               "close_values": ["Anthony IG"],        "monthly_goal": 22, "section": "inhouse"},
    {"name": "Side Hustle Nation/WWWS", "close_values": ["WWWS"],              "monthly_goal": 2,   "section": "inhouse"},
    {"name": "Passivepreneurs",         "close_values": ["Passivepreneurs"],   "monthly_goal": None, "section": "inhouse"},
    {"name": "Reactivation Email",      "close_values": ["Reactivation Email"],"monthly_goal": None, "section": "inhouse"},
    {"name": "Reactivation Scrapers",   "close_values": ["Reactivation Scrapers"],"monthly_goal": None, "section": "inhouse"},
    {"name": "LinkedIn Ads",            "close_values": ["LinkedIn Ads"],       "monthly_goal": None, "section": "inhouse"},
    {"name": "YouTube Ads",             "close_values": ["YouTube Ads"],        "monthly_goal": None, "section": "inhouse"},
    {"name": "Google Ads",              "close_values": ["Google Ads"],         "monthly_goal": None, "section": "inhouse"},
]

# Build reverse lookup: close_value → funnel display name
CLOSE_VALUE_TO_FUNNEL = {}
for fc in FUNNEL_CONFIG:
    for cv in fc["close_values"]:
        CLOSE_VALUE_TO_FUNNEL[cv] = fc["name"]

UNCATEGORIZED_FUNNELS = ["No Attribution", "Unknown (Needs Review)"]


# ─── Working Days Calculation ────────────────────────────────────────────────

def working_days_in_month(year, month):
    """Count Mon-Fri days in a given month."""
    num_days = calendar.monthrange(year, month)[1]
    count = 0
    for day in range(1, num_days + 1):
        if date(year, month, day).weekday() < 5:
            count += 1
    return count


def get_daily_goal(monthly_goal, year, month):
    """Divide monthly goal by working days, round to nearest integer."""
    if monthly_goal is None:
        return None
    wd = working_days_in_month(year, month)
    if wd == 0:
        return 0
    return max(1, round(monthly_goal / wd))


# ─── Title Classification ───────────────────────────────────────────────────

EXCLUDE_FOLLOW_UP_RE = re.compile(
    r"follow[\s\-]?up|fallow\s+up|\bf/?u\b|next\s+steps|rescheduled|reschedule",
    re.IGNORECASE
)
INCLUDE_PATTERNS_RE = re.compile(
    r"vending\s+strategy\s+call"
    r"|vendingpren[eu]+rs?\s+consultation"
    r"|vendingpren[eu]+rs?\s+strategy\s+call"
    r"|new\s+vendingpreneur\s+strategy\s+call"
    r"|post\s+masterclass\s+strategy\s+call",
    re.IGNORECASE
)
INCLUDE_AMBIGUOUS_RE = re.compile(r"vending\s+consult", re.IGNORECASE)
EXCLUDE_AMBIGUOUS_RE = re.compile(
    r"\benrollment\b|silver\s+start\s+up|bronze\s+enrollment|questions\s+on\s+enrollment",
    re.IGNORECASE
)

# Scraper "Next Steps" titles — these are legit sales calls booked via scraper outreach.
# Calendly appends prospect/rep names (e.g. "...with Jimmy and jason aaron").
# Must be checked BEFORE the follow-up/next-steps exclusion to avoid false exclusion.
INCLUDE_SCRAPER_RE = re.compile(
    r"vendingpren[eu]+rs?\s*-?\s*next\s+steps"
    r"|vendingpreneur\s+next\s+steps",
    re.IGNORECASE
)

def classify_meeting_title(title):
    if not title:
        return "exclude_other"
    if title.strip().lower().startswith("canceled"):
        return "exclude"
    if "vending quick discovery" in title.lower():
        return "exclude"
    # Check scraper "Next Steps" titles BEFORE the follow-up exclusion
    if INCLUDE_SCRAPER_RE.search(title.strip()):
        return "include"
    if EXCLUDE_FOLLOW_UP_RE.search(title):
        return "exclude"
    if "anthony" in title.lower() and "q&a" in title.lower():
        return "exclude"
    if EXCLUDE_AMBIGUOUS_RE.search(title):
        return "exclude"
    if INCLUDE_PATTERNS_RE.search(title):
        return "include"
    if INCLUDE_AMBIGUOUS_RE.search(title):
        return "include"
    return "exclude_other"


# ─── API Helpers ─────────────────────────────────────────────────────────────

def log(msg):
    print(msg, flush=True)

def elapsed_since(start):
    return f"{time.time() - start:.1f}s"

session = requests.Session()
session.auth = (CLOSE_API_KEY, "")
session.headers.update({"Content-Type": "application/json"})
_api_call_count = 0

def close_get(endpoint, params=None):
    global _api_call_count
    time.sleep(API_THROTTLE)
    url = f"{CLOSE_API_BASE}/{endpoint}"
    for attempt in range(5):
        resp = session.get(url, params=params or {}, timeout=60)
        _api_call_count += 1
        if resp.status_code == 429:
            wait = float(resp.headers.get("Retry-After", 5))
            log(f"   ⏳ Rate limited (attempt {attempt+1}), waiting {wait}s...")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.json()
    resp.raise_for_status()

def parse_meeting_date_pacific(meeting):
    raw = meeting.get("starts_at") or meeting.get("activity_at") or meeting.get("date_start")
    if not raw or not isinstance(raw, str):
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return dt.astimezone(PACIFIC).date()
    except (ValueError, TypeError):
        return None


# ─── Data Pipeline ───────────────────────────────────────────────────────────

def fetch_all_meetings():
    step_start = time.time()
    log("📥 Fetching all meetings from Close...")
    all_meetings = []
    skip = 0
    while True:
        data = close_get("activity/meeting", {"_skip": skip, "_limit": 100})
        all_meetings.extend(data.get("data", []))
        if len(all_meetings) % 1000 == 0:
            log(f"   ... {len(all_meetings)} meetings fetched")
        if not data.get("has_more", False):
            break
        skip += 100
    log(f"   ✓ {len(all_meetings)} total meetings [{elapsed_since(step_start)}]")
    return all_meetings



def classify_meetings(all_meetings, start_date, end_date):
    counts = {"include": 0, "exclude": 0, "exclude_other": 0, "user_excluded": 0, "out_of_range": 0, "status_excluded": 0}
    included = []
    exclude_other_titles = []
    for m in all_meetings:
        meeting_date = parse_meeting_date_pacific(m)
        if not meeting_date or meeting_date < start_date or meeting_date >= end_date:
            counts["out_of_range"] += 1
            continue
        # Meeting status check (canceled/declined from calendar)
        meeting_status = (m.get("status") or "").lower().strip()
        if meeting_status.startswith("canceled") or meeting_status.startswith("declined"):
            counts["status_excluded"] += 1
            continue
        uid = m.get("user_id", "")
        ulist = m.get("users", [])
        if uid in EXCLUDED_USER_IDS or any(u in EXCLUDED_USER_IDS for u in (ulist or [])):
            counts["user_excluded"] += 1
            continue
        title = m.get("title", "")
        cls = classify_meeting_title(title)
        counts[cls] += 1
        if cls == "include":
            m["_meeting_date"] = meeting_date
            included.append(m)
        elif cls == "exclude_other":
            exclude_other_titles.append({"title": title, "date": meeting_date.isoformat()})
    return included, exclude_other_titles, counts


def classify_setter_meetings(all_meetings, start_date, end_date):
    """
    Capture meetings from setter users (Kristin/Spencer) for LTF detail.
    These are excluded from the main count but tracked separately.
    Same date/status/title-exclude filters, but only setter user meetings.
    """
    setter_included = []
    for m in all_meetings:
        meeting_date = parse_meeting_date_pacific(m)
        if not meeting_date or meeting_date < start_date or meeting_date >= end_date:
            continue
        # Same status filter
        meeting_status = (m.get("status") or "").lower().strip()
        if meeting_status.startswith("canceled") or meeting_status.startswith("declined"):
            continue
        # Only setter users
        uid = m.get("user_id", "")
        ulist = m.get("users", [])
        is_setter = uid in SETTER_USER_IDS or any(u in SETTER_USER_IDS for u in (ulist or []))
        if not is_setter:
            continue
        # Skip hard-excluded users (shouldn't overlap, but safety)
        if uid in HARD_EXCLUDED_USER_IDS or any(u in HARD_EXCLUDED_USER_IDS for u in (ulist or [])):
            continue
        # Title-level excludes (follow-ups, rescheduled, canceled prefix, enrollment)
        title = m.get("title", "")
        if title.strip().lower().startswith("canceled"):
            continue
        if EXCLUDE_FOLLOW_UP_RE.search(title):
            continue
        if EXCLUDE_AMBIGUOUS_RE.search(title):
            continue
        # Everything else from setters counts (discovery calls, consults, etc.)
        m["_meeting_date"] = meeting_date
        setter_included.append(m)
    return setter_included


def fetch_leads_for_meetings(meetings):
    step_start = time.time()
    unique_ids = list(set(m["lead_id"] for m in meetings if m.get("lead_id")))
    log(f"📥 Fetching {len(unique_ids)} leads...")
    cache = {}
    for i, lid in enumerate(unique_ids, 1):
        if i % 20 == 0 or i == len(unique_ids):
            log(f"   ... {i}/{len(unique_ids)} leads ({_api_call_count} API calls)")
        try:
            cache[lid] = close_get(f"lead/{lid}", {"_fields": LEAD_FIELDS})
        except requests.HTTPError as e:
            log(f"  ⚠ Could not fetch lead {lid}: {e}")
            cache[lid] = None
    log(f"   ✓ Leads fetched [{elapsed_since(step_start)}]")
    return cache


def fetch_field_leads(start_date, end_date):
    """Fetch leads where First Sales Call Booked Date is within [start_date, end_date).
    Uses Close API lead search with custom field date filter.
    Returns list of lead dicts with id, display_name, status_id, funnel, and field date.
    """
    step_start = time.time()
    query = (
        f'{FIELD_FIRST_SALES_CALL} >= "{start_date.isoformat()}" '
        f'and {FIELD_FIRST_SALES_CALL} < "{end_date.isoformat()}"'
    )
    fields = ",".join(["id", "display_name", "status_id", FIELD_FIRST_SALES_CALL, FIELD_FUNNEL_NAME_DEAL, FIELD_LEAD_OWNER, FIELD_REACTIVATION_SETTER])
    leads = []
    skip = 0
    log(f"📥 Fetching leads by First Sales Call Booked Date ({start_date} to {end_date})...")
    while True:
        data = close_get("lead", {"query": query, "_fields": fields, "_skip": skip, "_limit": 200})
        batch = data.get("data", [])
        leads.extend(batch)
        if len(leads) % 100 == 0 and len(leads) > 0:
            log(f"   ... {len(leads)} leads fetched")
        if not data.get("has_more", False):
            break
        skip += 200
    log(f"   ✓ {len(leads)} leads found [{elapsed_since(step_start)}]")
    return leads


def map_funnel(raw_funnel):
    """Map a raw Close funnel value to the display funnel name."""
    if not raw_funnel or raw_funnel.strip() == "":
        return "Unknown (Needs Review)"
    if raw_funnel in CLOSE_VALUE_TO_FUNNEL:
        return CLOSE_VALUE_TO_FUNNEL[raw_funnel]
    # Check if it's already a known uncategorized
    if raw_funnel in UNCATEGORIZED_FUNNELS:
        return raw_funnel
    # Unknown value → uncategorized
    return raw_funnel


def fetch_meeting_booking_dates(valid_meetings):
    """For each lead in valid_meetings, fetch the meeting's created_at and title.
    Returns: {lead_id: date_obj} for booking dates
             Also populates meeting_titles: {lead_id: str} for calendar source
    """
    # Known Calendly event type names — used to extract calendar source from meeting titles
    KNOWN_CALENDAR_NAMES = [
        "Vendingprenuers Consultation",
        "Vending Accelerator Call",
        "Vending Strategy Call with Vendingpreneurs",
        "New Vendingpreneur Strategy Call",
        "Vending Route Consultation",
        "Cash-Flowing Vending Route Advisory Interview",
        "Vending Quick Discovery",
        "Acquisition Ace Strategy Call",
        "Vendingpreneurs Rescheduled Call",
        "Vendingpreneurs Follow-Up",
        "Vendingpreneurs Follow Up",
        "Vendingpreneurs Onboarding Call",
        "Post Masterclass Strategy Call",
    ]

    def extract_calendar_name(title):
        """Extract the calendar event type name from a meeting title that includes contact names."""
        if not title:
            return "Unknown"
        for name in KNOWN_CALENDAR_NAMES:
            if name.lower() in title.lower():
                return name
        return title  # Return full title if no known name matched

    step_start = time.time()
    unique_leads = {}
    for m in valid_meetings:
        lid = m.get("lead_id")
        if lid and lid not in unique_leads:
            unique_leads[lid] = m.get("date")  # The call date

    log(f"   🔍 Fetching meeting booking dates for {len(unique_leads)} leads...")
    booking_dates = {}  # lead_id → created_at date
    meeting_titles = {}  # lead_id → meeting title (calendar source)

    for i, (lead_id, call_date) in enumerate(unique_leads.items()):
        try:
            data = close_get("activity/meeting", {
                "lead_id": lead_id,
                "_limit": 10,
            })
            for meeting in data.get("data", []):
                starts_raw = meeting.get("starts_at") or meeting.get("date_start") or ""
                created_raw = meeting.get("date_created") or meeting.get("created_at") or ""
                title = meeting.get("title") or meeting.get("subject") or meeting.get("note") or "Unknown"
                if not starts_raw:
                    continue
                try:
                    starts_dt = datetime.fromisoformat(starts_raw.replace("Z", "+00:00"))
                    starts_date = starts_dt.astimezone(PACIFIC).date()
                except (ValueError, TypeError):
                    continue

                # Match meeting to the call date we know about
                if starts_date == call_date:
                    # Capture booking date
                    if created_raw:
                        try:
                            created_dt = datetime.fromisoformat(created_raw.replace("Z", "+00:00"))
                            booking_dates[lead_id] = created_dt.astimezone(PACIFIC).date()
                        except (ValueError, TypeError):
                            pass
                    # Capture meeting title for calendar source (extract event name, strip contact names)
                    meeting_titles[lead_id] = extract_calendar_name(title)
                    break
        except Exception as e:
            if i == 0:
                log(f"   ⚠ Meeting fetch error (first lead): {e}")

        if (i + 1) % 50 == 0:
            log(f"   ... {i + 1}/{len(unique_leads)} leads processed")

    log(f"   ✓ Got booking dates for {len(booking_dates)}/{len(unique_leads)} leads [{elapsed_since(step_start)}]")
    log(f"   ✓ Got meeting titles for {len(meeting_titles)}/{len(unique_leads)} leads")
    return booking_dates, meeting_titles


# ── Meeting Title Classification (Phase 2 hero cards) ───────────────────────
# Used by the hero card "Total Meetings" breakdown into F/U + Resch + Other.
# Case-insensitive substring match. "Other" is the catch-all — it includes new
# sales calls (which are separately counted via the First Sales Call field) AND
# anything else (Q&A, onboarding, ad-hoc, etc.). The math is intentionally:
#     fu + resch + other == total meetings booked
# So the "Other" sub-row is large by design.
FU_TITLE_PATTERNS    = ["f/u", "follow-up", "follow up", "followup"]
RESCH_TITLE_PATTERNS = ["reschedule", "resched", "resch"]

def classify_meeting_title(title):
    """Return 'fu', 'resch', or 'other' based on case-insensitive substring match."""
    t = (title or "").lower()
    if any(p in t for p in FU_TITLE_PATTERNS):
        return "fu"
    if any(p in t for p in RESCH_TITLE_PATTERNS):
        return "resch"
    return "other"


def fetch_rep_total_meetings(start_date, end_date, all_lane_user_ids, lead_to_funnel=None, leads_with_fscbd=None):
    """Fetch total non-internal meetings per rep per date in the window.

    Counts ALL meetings (first calls + follow-ups + reschedules + Q&A + onboarding etc.)
    so the rep details section can surface the gap between "what's on the calendar"
    and "what the dashboard counts as first sales calls."

    EXCLUDES:
      - Meetings without a lead_id attached (internal team events, lunches, etc.)
      - Canceled / declined meetings (wouldn't appear on the live calendar)
      - Titles matching admin/internal patterns (belt-and-suspenders backup filter)
      - For funnel-restricted reps (LANE_FUNNEL_RESTRICTIONS): meetings whose lead's funnel
        doesn't match the rep's allowed funnels for the configured date range.

    lead_to_funnel: {lead_id: funnel_name} for restriction checks. If None or a lead is
    missing, restricted reps' meetings for that lead are excluded (conservative default).

    leads_with_fscbd: set of (lead_id, date) pairs where the lead has First Sales Call Booked
    Date set to that date. Used in the priority hierarchy below — a meeting whose lead is in
    this set is classified as "new" (not F/U / Resch / Other) regardless of its title.

    Returns: (rep_totals, rep_categories, non_new_meetings) where:
      rep_totals     = {user_id: {date: count}}                                 — total meetings
      rep_categories = {user_id: {date: {"fu": N, "resch": N, "other": N}}}     — non-new meetings
                       classified by title (priority hierarchy: lead-FSCBD wins
                       over title patterns). "new" meetings are NOT in rep_categories — they're
                       counted separately via daily_data[d]["booked"] (lead-based).
                       Math invariant (per date, no double-count):
                         new (lead-based) + fu + resch + other == total
                       Small discrepancies possible from canceled meetings of FSCBD leads.
      non_new_meetings = [{"lead_id", "user_id", "category", "meeting_date", "title"}, ...]
                         — per-meeting records for the F/U / Resch / Other panel detail section.
                         Used to look up prospect names + funnels in main() for display.
    """
    log("📥 Fetching all rep meetings in window for Total Calls row + card breakdowns...")
    step_start = time.time()
    lead_to_funnel = lead_to_funnel or {}
    leads_with_fscbd = leads_with_fscbd or set()

    # Server-side date filter — Close ignores unknown params, so if these aren't
    # honored we just paginate more pages and the client-side date check still works.
    start_iso = start_date.isoformat()
    end_iso   = (end_date + timedelta(days=1)).isoformat()

    EXCLUSION_PATTERNS = [
        "lunch", "break", "ooo", "pto", "out of office",
        "internal", "team meeting", "1:1", "standup", "training",
    ]
    # Prefix match so we catch all variants: canceled, canceled-by-org, canceled-by-lead,
    # declined, declined-by-org, declined-by-lead, etc.
    EXCLUDED_STATUS_PREFIXES = ("canceled", "declined")

    rep_totals = {}  # {user_id: {date: count}}
    rep_categories = {}  # {user_id: {date: {"fu": N, "resch": N, "other": N}}}
    non_new_meetings = []  # [{"lead_id", "user_id", "category", "meeting_date", "title"}, ...]
                           # Used to power the "F/U, Reschedule & Other Details" panel section.
                           # Only meetings classified as fu / resch / other are captured here.
    seen_events = set()  # (user_id, starts_at, title_lower) — dedupes multi-invitee meetings
    skip = 0
    pages = 0
    raw_count = 0
    kept_count = 0
    excluded = {"no_lead": 0, "status": 0, "title": 0, "out_of_range": 0, "not_lane_rep": 0,
                "funnel_restricted": 0, "duplicate": 0}

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
        pages += 1
        raw_count += len(batch)

        for m in batch:
            meeting_date = parse_meeting_date_pacific(m)
            if meeting_date is None or meeting_date < start_date or meeting_date > end_date:
                excluded["out_of_range"] += 1
                continue
            user_id = m.get("user_id")
            if user_id not in all_lane_user_ids:
                excluded["not_lane_rep"] += 1
                continue
            lead_id = m.get("lead_id")
            if not lead_id:
                excluded["no_lead"] += 1
                continue
            status = (m.get("status") or "").lower()
            if status.startswith(EXCLUDED_STATUS_PREFIXES):
                excluded["status"] += 1
                continue
            title = (m.get("title") or "")
            if any(p in title.lower() for p in EXCLUSION_PATTERNS):
                excluded["title"] += 1
                continue

            # Funnel-restricted reps: only count meetings whose lead matches the allowed funnel.
            # If the lead isn't in our field_leads window we can't verify funnel, so we exclude.
            if user_id in LANE_FUNNEL_RESTRICTIONS:
                lead_funnel = lead_to_funnel.get(lead_id)
                if lead_funnel is None or not passes_funnel_restriction(user_id, lead_funnel, meeting_date):
                    excluded["funnel_restricted"] += 1
                    continue

            # Dedupe multi-invitee meetings: Close emits one record per lead linked to the
            # same calendar event. Same user + same start time + same title = same event.
            dedup_key = (user_id, m.get("starts_at"), title.lower())
            if dedup_key in seen_events:
                excluded["duplicate"] += 1
                continue
            seen_events.add(dedup_key)

            rep_totals.setdefault(user_id, {}).setdefault(meeting_date, 0)
            rep_totals[user_id][meeting_date] += 1

            # Priority hierarchy:
            #   1) lead has FSCBD == meeting date → "new"  (NOT tracked in rep_categories;
            #      counted separately via daily_data[d]["booked"] which is lead-based)
            #   2) title matches F/U pattern → "fu"
            #   3) title matches Resch pattern → "resch"
            #   4) otherwise → "other"
            # This guarantees each meeting is counted in exactly one bucket — no double-count
            # of a "new" meeting that happens to be titled with "F/U" or similar.
            rep_categories.setdefault(user_id, {}).setdefault(meeting_date, {"fu": 0, "resch": 0, "other": 0})
            if (lead_id, meeting_date) not in leads_with_fscbd:
                category = classify_meeting_title(title)
                rep_categories[user_id][meeting_date][category] += 1
                non_new_meetings.append({
                    "lead_id": lead_id,
                    "user_id": user_id,
                    "category": category,
                    "meeting_date": meeting_date,
                    "title": title,
                })

            kept_count += 1

        if not data.get("has_more", False):
            break
        skip += 100

    log(f"   ✓ {kept_count} meetings kept across {len(rep_totals)} reps "
        f"({raw_count} raw, {pages} pages) [{elapsed_since(step_start)}]")
    log(f"   ↪ Excluded: {excluded['no_lead']} no lead · {excluded['status']} canceled/declined · "
        f"{excluded['title']} admin titles · {excluded['out_of_range']} out of window · "
        f"{excluded['not_lane_rep']} not on a lane · {excluded['funnel_restricted']} funnel-restricted · "
        f"{excluded['duplicate']} multi-invitee duplicates")
    log(f"   ↪ Captured {len(non_new_meetings)} non-new meeting records for detail panel")
    return rep_totals, rep_categories, non_new_meetings


def build_day_detail(valid_meetings, booking_dates, lane_rep_names, meeting_titles=None, non_new_meetings_by_date=None):
    """Build per-day detail data for the day detail panel.
    meeting_titles: {lead_id: str} — meeting title from Close for Calendar Source
    non_new_meetings_by_date: {date_obj: [{category, category_label, lead_name, lead_url, funnel_name, owner_name}, ...]}
                              — per-day list of non-new (F/U / Resch / Other) meetings with
                              fully-resolved display info, sorted by category then lead name.
    """
    from collections import Counter

    by_day = {}
    for m in valid_meetings:
        d = m["date"]
        ds = d.isoformat()
        if ds not in by_day:
            by_day[ds] = {"funnels": Counter(), "reps": Counter(), "booked_on": Counter(), "cal_source": Counter(), "total": 0}
        by_day[ds]["funnels"][m["funnel"]] += 1
        rep_name = lane_rep_names.get(m.get("lead_owner", ""), "Other")
        by_day[ds]["reps"][rep_name] += 1
        by_day[ds]["total"] += 1

        lid = m.get("lead_id")

        # Booking date (when the call was scheduled)
        if lid in booking_dates:
            booked_date = booking_dates[lid]
            by_day[ds]["booked_on"][booked_date.isoformat()] += 1
        else:
            by_day[ds]["booked_on"]["Unknown"] += 1

        # Calendar source (meeting title from Close)
        if meeting_titles and lid in meeting_titles:
            by_day[ds]["cal_source"][meeting_titles[lid]] += 1
        else:
            by_day[ds]["cal_source"]["Unknown"] += 1

    # ── Merge non-new meeting days into by_day ─────────────────────────────────
    # A day might have non-new meetings but no field-leads (e.g., all activity that
    # day is follow-ups). Ensure those days still surface a panel entry.
    non_new_meetings_by_date = non_new_meetings_by_date or {}
    for d, items in non_new_meetings_by_date.items():
        ds = d.isoformat() if hasattr(d, "isoformat") else d
        if ds not in by_day:
            by_day[ds] = {"funnels": Counter(), "reps": Counter(), "booked_on": Counter(), "cal_source": Counter(), "total": 0}

    result = {}
    for ds, data in by_day.items():
        total = data["total"]
        # Allow days with 0 "new" meetings but non-new items to still render.
        # Pick up the matching non-new list if any.
        non_new_list = non_new_meetings_by_date.get(date.fromisoformat(ds), []) if ds else []
        if total == 0 and not non_new_list:
            continue

        # Funnels: top 4 + Other (handle day with 0 valid_meetings — no funnel breakdown)
        if total > 0:
            funnel_sorted = data["funnels"].most_common()
            if len(funnel_sorted) > 4:
                top4 = funnel_sorted[:4]
                other_count = sum(c for _, c in funnel_sorted[4:])
                funnel_list = [[f, c, round(c / total * 100)] for f, c in top4]
                funnel_list.append(["Other", other_count, round(other_count / total * 100)])
            else:
                funnel_list = [[f, c, round(c / total * 100)] for f, c in funnel_sorted]

            # Reps
            rep_list = [[r, c] for r, c in data["reps"].most_common()]

            # Booked on
            booked_items = sorted(data["booked_on"].items(), key=lambda x: x[0] if x[0] != "Unknown" else "9999")
            booked_list = [[d, c, round(c / total * 100)] for d, c in booked_items]
        else:
            funnel_list = []
            rep_list = []
            booked_list = []

        result[ds] = {
            "total": total,
            "funnels": funnel_list,
            "reps": rep_list,
            "booked_on": booked_list,
            # F/U / Reschedule / Other meeting details — fully resolved (name, funnel, owner, lead URL).
            "non_new_meetings": non_new_list,
        }

    return result


def build_dashboard_data(field_leads, dates, today=None, lane_reps=None, lane_label="", rep_total_meetings=None, rep_meetings_by_category=None):
    """Build dashboard data from field-based lead query.
    field_leads: list of lead dicts from fetch_field_leads (or similar).
    lane_reps: set of user IDs to filter by (if None, no lane filter applied).
    lane_label: label for logging (e.g., "Lane 1", "Lane 2").
    rep_total_meetings: optional {user_id: {date: count}} from fetch_rep_total_meetings()
                        for the "Total Calls" reconciliation row in rep details.
    """
    daily_data = {}
    all_funnels_seen = set()
    for d in dates:
        daily_data[d] = {"booked": 0, "capacity": get_capacity(d), "funnels": {}}

    # Per-rep tracking: { user_id: { date: { funnel: count } } }
    rep_data = {}
    if lane_reps:
        for uid in lane_reps:
            rep_data[uid] = {d: {} for d in dates}

    # Reactivation Scrapers setter drilldown: { setter_name: { date: count } }
    setter_data = {}

    valid_meetings = []
    status_excluded = 0
    lane_excluded = 0

    for lead in field_leads:
        # Exclude bad statuses
        if lead.get("status_id") in EXCLUDED_LEAD_STATUS_IDS:
            status_excluded += 1
            continue

        # Lane filter — only count leads owned by reps in this lane
        lead_owner = (lead.get(FIELD_LEAD_OWNER) or "").strip()
        if lane_reps and lead_owner not in lane_reps:
            lane_excluded += 1
            continue

        # Get the date from the field
        field_date_str = lead.get(FIELD_FIRST_SALES_CALL)
        if not field_date_str:
            continue
        try:
            field_date = date.fromisoformat(field_date_str)
        except (ValueError, TypeError):
            continue

        if field_date not in daily_data:
            continue

        # Date-based lane transitions (e.g., Chris Wanke → Lane 2 from 05/18)
        if lane_reps and lead_owner in LANE_TRANSITIONS:
            trans = LANE_TRANSITIONS[lead_owner]
            cutover = trans["cutover"]
            if field_date >= cutover and lane_label == trans["from_lane"]:
                lane_excluded += 1
                continue  # This call belongs to the other lane now
            if field_date < cutover and lane_label == trans["to_lane"]:
                lane_excluded += 1
                continue  # This call still belongs to the original lane

        # Get funnel
        raw_funnel = (lead.get(FIELD_FUNNEL_NAME_DEAL) or "")
        funnel = map_funnel(raw_funnel)

        # Funnel-restricted reps (e.g., overflow): skip if funnel/date doesn't match
        if not passes_funnel_restriction(lead_owner, funnel, field_date):
            lane_excluded += 1
            continue

        daily_data[field_date]["booked"] += 1
        all_funnels_seen.add(funnel)
        daily_data[field_date]["funnels"][funnel] = daily_data[field_date]["funnels"].get(funnel, 0) + 1

        # Track per-rep
        if lane_reps and lead_owner in rep_data:
            rep_data[lead_owner][field_date][funnel] = rep_data[lead_owner][field_date].get(funnel, 0) + 1

        # Track per-setter for the Reactivation Scrapers drilldown
        if funnel == "Reactivation Scrapers":
            setter = (lead.get(FIELD_REACTIVATION_SETTER) or "").strip() or "Unknown Setter"
            setter_data.setdefault(setter, {d: 0 for d in dates})
            setter_data[setter][field_date] = setter_data[setter].get(field_date, 0) + 1

        valid_meetings.append({
            "date": field_date,
            "title": lead.get("display_name", ""),
            "funnel": funnel,
            "lead_id": lead.get("id", ""),
            "lead_owner": lead_owner,
        })

    if status_excluded > 0:
        log(f"   ⚠ Excluded {status_excluded} leads (status: Canceled/Outside US)")
    if lane_excluded > 0:
        log(f"   ⚠ Excluded {lane_excluded} leads (Lead Owner not in {lane_label})")
    log(f"   📊 {len(valid_meetings)} {lane_label} leads counted across window")

    # Clamp Total Meetings for reps in NEW_CALLS_ONLY_REPS: their displayed total
    # equals their new-calls count (sum of rep_data funnels) instead of the raw
    # all-meetings count. Affects both the top-section "Total Meetings Booked" row
    # and the per-rep "Total Calls" row in Rep Details.
    # Mutates rep_total_meetings / rep_meetings_by_category in place — fine since
    # this is the only place downstream code reads from them for this build.
    if rep_total_meetings:
        for uid in NEW_CALLS_ONLY_REPS:
            if uid not in rep_data:
                continue
            rep_total_meetings.setdefault(uid, {})
            for d in dates:
                new_calls_count = sum(rep_data[uid].get(d, {}).values())
                rep_total_meetings[uid][d] = new_calls_count

                # Apply matching clamp to categories: their displayed total = their new
                # calls only. Zero out F/U and Resch contributions. Their "other" is
                # recomputed at team-level (see below), so the per-rep value is moot.
                if rep_meetings_by_category is not None:
                    rep_meetings_by_category.setdefault(uid, {})[d] = {
                        "fu": 0, "resch": 0, "other": 0,
                    }

    # Per-lane "Total Meetings Booked" count per date — sum of rep_total_meetings
    # filtered to user_ids in this lane. Mirrors how "Booked" is lane-filtered.
    total_meetings_by_date = {d: 0 for d in dates}
    for uid, dates_dict in (rep_total_meetings or {}).items():
        if lane_reps and uid not in lane_reps:
            continue
        for d, count in dates_dict.items():
            if d in total_meetings_by_date:
                total_meetings_by_date[d] += count

    # Team-level F/U / Resch / Other counts per date — for the hero card breakdown.
    # All three are summed directly from per-rep priority-classified counts.
    # Priority hierarchy in fetch_rep_total_meetings guarantees no double-count:
    #   - "new" meetings (lead has FSCBD == meeting date) were excluded from rep_categories
    #   - F/U / Resch / Other are mutually exclusive (title-based)
    #
    # Card math (per date, assuming no canceled-meeting discrepancies):
    #   new (lead-based, daily_data["booked"]) + fu + resch + other ≈ total_meetings
    # The hero card displays total = new + fu + resch + other so the math always adds up,
    # which may differ slightly from the raw all-meetings count when leads have FSCBD set
    # but their meeting was canceled.
    meetings_by_category_by_date = {}
    for d in dates:
        fu = resch = other = 0
        for uid, dates_dict in (rep_meetings_by_category or {}).items():
            if lane_reps and uid not in lane_reps:
                continue
            cats = dates_dict.get(d, {})
            fu    += cats.get("fu", 0)
            resch += cats.get("resch", 0)
            other += cats.get("other", 0)
        meetings_by_category_by_date[d] = {"fu": fu, "resch": resch, "other": other}

    return {
        "dates": dates,
        "daily_data": daily_data,
        "all_funnels_seen": all_funnels_seen,
        "valid_meetings": valid_meetings,
        "today": today,
        "rep_data": rep_data,
        "rep_total_meetings": rep_total_meetings or {},
        "total_meetings_by_date": total_meetings_by_date,
        "meetings_by_category_by_date": meetings_by_category_by_date,
        "setter_data": setter_data,
    }


def build_funnel_detail(closer_data, setter_meetings, lead_cache, dates, close_values, label, track_no_funnel=False):
    """
    Build closer vs setter breakdown per day for one or more funnels.
    close_values: a single Close field value OR a list of values
                  (e.g., "Low Ticket Funnel" or ["Instagram", "X", "Linkedin"])
    label: display label for logging (e.g., "LTF", "Instagram")
    track_no_funnel: if True, also count setter calls with no funnel assigned
    """
    if isinstance(close_values, str):
        close_values = [close_values]

    daily = {}
    for d in dates:
        closer_count = sum(
            closer_data["daily_data"][d]["funnels"].get(map_funnel(cv), 0)
            for cv in close_values
        )
        daily[d] = {"closer": closer_count, "setter": 0, "total": closer_count, "no_funnel": 0}

    # Cross-window dedup for setter meetings — most recent wins
    lead_best = {}
    for m in setter_meetings:
        lead_id = m.get("lead_id")
        if not lead_id:
            continue
        meeting_date = m.get("_meeting_date")
        if not meeting_date or meeting_date not in daily:
            continue
        lead_data = lead_cache.get(lead_id)
        if lead_data and lead_data.get("status_id") in EXCLUDED_LEAD_STATUS_IDS:
            continue
        if lead_id not in lead_best or meeting_date > lead_best[lead_id]["_meeting_date"]:
            lead_best[lead_id] = m

    setter_count_total = 0
    no_funnel_total = 0
    for lead_id, m in lead_best.items():
        meeting_date = m["_meeting_date"]
        lead_data = lead_cache.get(lead_id)
        raw_funnel = (lead_data.get(FIELD_FUNNEL_NAME_DEAL) if lead_data else None) or ""
        raw_funnel = raw_funnel.strip()

        if not raw_funnel and track_no_funnel:
            daily[meeting_date]["no_funnel"] += 1
            no_funnel_total += 1
        elif raw_funnel in close_values:
            daily[meeting_date]["setter"] += 1
            daily[meeting_date]["total"] += 1
            setter_count_total += 1

    nf_str = f", {no_funnel_total} no-funnel discovery calls" if track_no_funnel else ""
    log(f"   📊 {label} Detail: {setter_count_total} setter calls{nf_str}")
    return daily


# ─── Shared CSS ──────────────────────────────────────────────────────────────

COMMON_CSS = """
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600;700&display=swap');
* { margin:0; padding:0; box-sizing:border-box; }
body { font-family: 'Inter', -apple-system, system-ui, sans-serif; background: #ffffff; color: #1a1a1a; }
.header { background: #1b2e1b; color: #fff; padding: 0.8rem 1.5rem; display: flex; justify-content: space-between; align-items: center; }
.header h1 { font-size: 1.15rem; font-weight: 700; }
.header .sub { font-size: 0.68rem; color: #a3c4a3; margin-top: 2px; }
.header .right { text-align: right; font-family: 'JetBrains Mono', monospace; }
.header .right .date { font-size: 0.78rem; font-weight: 600; }
.header .right .time { font-size: 0.65rem; color: #a3c4a3; }
.dot { display: inline-block; width: 7px; height: 7px; background: #4ade80; border-radius: 50%; margin-right: 5px; }
.wrap { padding: 1rem 1.5rem 2rem; max-width: 1500px; margin: 0 auto; }
th.sec-label { color: #1b5e1b; font-size: 0.6rem; font-weight: 800; letter-spacing: 0.08em; text-transform: uppercase; background: #f8faf8; border-left: 3px solid #1b5e1b; }
/* Compact modifier — applied to longer labels (e.g. UNCATEGORIZED) so they fit
   in the 200px first column without forcing the other labels smaller too. */
th.sec-label.is-compact { font-size: 0.52rem; letter-spacing: 0.05em; }
.card { border: 1px solid #d4d4d4; border-radius: 4px; overflow-x: auto; margin-bottom: 1rem; background: #fff; }
table { width: 100%; border-collapse: collapse; table-layout: fixed; }
th { padding: 0.5rem 0.6rem; font-size: 0.68rem; font-weight: 700; text-align: center; color: #555; border-bottom: 2px solid #d4d4d4; white-space: nowrap; background: #fafafa; line-height: 1.4; }
th:first-child { text-align: left; padding-left: 0.6rem; }
td { padding: 0.35rem 0.6rem; border-bottom: 1px solid #ececec; font-size: 0.78rem; }
td.num { text-align: center; font-family: 'JetBrains Mono', monospace; font-size: 0.76rem; font-weight: 500; color: #333; }
td.label { font-weight: 500; font-size: 0.76rem; padding-left: 0.6rem; color: #1a1a1a; }
.metric { font-weight: 600; font-size: 0.78rem; padding-left: 0.6rem; color: #1a1a1a; }
th.col-date.today { background: #fdf3e0 !important; color: #b45309; }
td.today { background: #fdf8ee !important; }
th.col-date.past { background: #f5f5f5 !important; color: #999; }
td.past { background: #fafafa !important; }
.booked { color: #1b7a2e; font-weight: 700; }
.zero { color: #ccc; font-weight: 400; }
.has-count { color: #1b7a2e; font-weight: 700; }
.total-num { font-weight: 700; color: #1a1a1a; }
.util-low { color: #16a34a; font-weight: 700; }
.util-mid { color: #b45309; font-weight: 700; }
.util-high { color: #dc2626; font-weight: 700; }
td.tgt-red   { color: #dc2626; font-weight: 700; }
td.tgt-amber { color: #b45309; font-weight: 700; }
td.tgt-green { color: #16a34a; font-weight: 700; }
tr.total-row td { border-top: 2px solid #bbb; }
tr.section-divider td { border-top: 2px solid #d4d4d4; padding: 0; height: 0; }
tr.section-label-row td { font-size: 0.62rem; font-weight: 800; letter-spacing: 0.12em; text-transform: uppercase; padding: 0.5rem 0.6rem 0.25rem; border-bottom: 1px solid #d4d4d4; }
tr.section-label-row.sec-ext td { color: #1b5e1b; background: #f8faf8; border-top: none; }
tr.section-label-row.sec-inh td { color: #5e4b1b; background: #fdf8f0; border-top: 2px solid #d4d4d4; }
tr.section-label-row.sec-unc td { color: #666; background: #f5f5f5; border-top: 2px solid #d4d4d4; }
.footer { margin-top: 1.5rem; padding-top: 0.75rem; border-top: 1px solid #e0e0e0; font-size: 0.72rem; color: #888; display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 0.5rem; }
.footer a { color: #1b7a2e; text-decoration: none; font-weight: 600; }
.footer a:hover { text-decoration: underline; }
.excluded-section { margin-top: 0.5rem; }
.excluded-section summary { font-size: 0.72rem; font-weight: 600; color: #666; cursor: pointer; padding: 0.4rem 0; }
.excluded-section .exc-table { margin-top: 0.3rem; }
.excluded-section .exc-table td { font-size: 0.7rem; color: #666; padding: 0.2rem 0.6rem; }
.summary-cards { display: grid; grid-template-columns: repeat(3, 1fr); gap: 1rem; margin-bottom: 1.25rem; }
.s-card { background: #f8faf8; border: 1px solid #d4d4d4; border-radius: 6px; padding: 1rem; }
.s-card .s-label { font-size: 0.65rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.1em; color: #666; margin-bottom: 0.3rem; }
.s-card .s-value { font-family: 'JetBrains Mono', monospace; font-size: 1.5rem; font-weight: 700; }
.s-card .s-value.green { color: #1b7a2e; }
.sec-ltf { color: #1b3a5e; border-left-color: #2563eb; background: #f0f4ff; }
tr.section-label-row.sec-ltf td { color: #1b3a5e; background: #f0f4ff; border-top: 2px solid #d4d4d4; }
.ltf-closer { color: #1b7a2e; font-weight: 600; }
.ltf-setter { color: #2563eb; font-weight: 600; }
.ltf-total { color: #1a1a1a; font-weight: 700; }
.ltf-pct { color: #666; font-weight: 500; font-size: 0.7rem; }
.ltf-collapsible { margin-bottom: 1rem; }
.ltf-collapsible summary { font-size: 0.72rem; font-weight: 600; color: #1b5e1b; cursor: pointer; padding: 0.5rem 0.6rem; background: #f8faf8; border: 1px solid #d4d4d4; border-left: 3px solid #1b5e1b; border-radius: 4px; }
.ltf-collapsible summary:hover { background: #eef5ee; }
.ltf-collapsible details[open] summary { border-radius: 4px 4px 0 0; border-bottom: none; }
.ltf-collapsible details[open] .card { margin-top: 0 !important; border-top: none; border-radius: 0 0 4px 4px; }
@media (max-width: 900px) { .header { padding: 0.6rem 0.75rem; } .wrap { padding: 0.5rem 0.75rem; } .summary-cards { grid-template-columns: 1fr; } }
"""


def html_header_bar(title, subtitle, date_str, time_str):
    return f"""<div class="header">
  <div><h1>📞 {title}</h1><div class="sub">{subtitle}</div></div>
  <div class="right"><div class="date"><span class="dot"></span>{date_str}</div><div class="time">{time_str}</div></div>
</div>"""


def util_class(pct):
    if pct >= 80: return "util-high"
    if pct >= 40: return "util-mid"
    return "util-low"


def target_class(pct):
    """Color for Capacity to Target %: red <75, amber 75-89, green >=90."""
    if pct >= 90: return "tgt-green"
    if pct >= 75: return "tgt-amber"
    return "tgt-red"


# ─── Funnel Row Builder ─────────────────────────────────────────────────────

def build_funnel_rows(data, dates, today, daily_goal_map, section_filter):
    """Build HTML rows for funnels in a given section. Only shows funnels with ≥1 call."""
    daily = data["daily_data"]
    setter_data = data.get("setter_data", {})
    rows = ""

    # Get configured funnels for this section
    section_funnels = [fc for fc in FUNNEL_CONFIG if fc["section"] == section_filter]

    for fc in section_funnels:
        fname = fc["name"]
        dg = daily_goal_map.get(fname)

        # Dynamic: check if this funnel has any calls in the window
        has_any = False
        for d in dates:
            for cv in fc["close_values"]:
                mapped_name = CLOSE_VALUE_TO_FUNNEL.get(cv, cv)
                if daily[d]["funnels"].get(mapped_name, 0) > 0:
                    has_any = True
                    break
            if has_any:
                break
        if not has_any:
            continue

        cells = ""
        for d in dates:
            count = 0
            for cv in fc["close_values"]:
                mapped_name = CLOSE_VALUE_TO_FUNNEL.get(cv, cv)
                count += daily[d]["funnels"].get(mapped_name, 0)

            tc = " today" if d == today else (" past" if today and d < today else "")

            if dg is not None:
                if count > 0:
                    cells += f'<td class="num has-count{tc}">{count}/{dg}</td>'
                else:
                    cells += f'<td class="num zero{tc}">0/{dg}</td>'
            else:
                if count > 0:
                    cells += f'<td class="num has-count{tc}">{count}</td>'
                else:
                    cells += f'<td class="num zero{tc}">0</td>'

        # Special case: Reactivation Scrapers gets an expand chevron + per-setter sub-rows
        if fname == "Reactivation Scrapers" and setter_data:
            # Sort setters by total descending so heaviest contributors show first
            setters_sorted = sorted(
                setter_data.items(),
                key=lambda kv: (-sum(kv[1].values()), kv[0])
            )
            # Filter setters with no calls in the window (defensive — shouldn't happen but safe)
            setters_sorted = [(s, byd) for s, byd in setters_sorted if sum(byd.values()) > 0]

            rows += (
                f'<tr class="funnel-expandable" onclick="toggleSetterRows(this)">'
                f'<td class="label"><span class="chevron">▶</span> {fname}</td>'
                f'{cells}</tr>\n'
            )
            for setter_name, by_date in setters_sorted:
                sub_cells = ""
                for d in dates:
                    tc = " today" if d == today else (" past" if today and d < today else "")
                    c = by_date.get(d, 0)
                    sub_cells += f'<td class="num{tc}" style="color:#777;">{c}</td>' if c > 0 else f'<td class="num zero{tc}">0</td>'
                rows += (
                    f'<tr class="setter-row" style="display:none;">'
                    f'<td class="label" style="padding-left:2rem;font-size:0.72rem;color:#555;">{setter_name}</td>'
                    f'{sub_cells}</tr>\n'
                )
        else:
            rows += f'<tr><td class="label">{fname}</td>{cells}</tr>\n'

    return rows


def build_uncategorized_rows(data, dates, today):
    """Build rows for No Attribution, Unknown, and any unmapped funnels. Only shows rows with ≥1 call."""
    daily = data["daily_data"]
    all_seen = data.get("all_funnels_seen", set())

    configured_names = set(fc["name"] for fc in FUNNEL_CONFIG)
    unmapped = set()
    for f in all_seen:
        if f not in configured_names and f not in UNCATEGORIZED_FUNNELS:
            unmapped.add(f)

    uncat_list = UNCATEGORIZED_FUNNELS + sorted(unmapped)

    rows = ""
    for fname in uncat_list:
        # Dynamic: skip if no calls in window
        has_any = any(daily[d]["funnels"].get(fname, 0) > 0 for d in dates)
        if not has_any:
            continue

        cells = ""
        for d in dates:
            count = daily[d]["funnels"].get(fname, 0)
            tc = " today" if d == today else (" past" if today and d < today else "")
            if count > 0:
                cells += f'<td class="num has-count{tc}">{count}</td>'
            else:
                cells += f'<td class="num zero{tc}">0</td>'
        rows += f'<tr><td class="label">{fname}</td>{cells}</tr>\n'

    return rows


# ─── Rolling Dashboard HTML ─────────────────────────────────────────────────

def generate_lane_content(data, dates, today, daily_goal_map, n_cols, lane_rep_names, lane_lead, show_capacity=True):
    """Generate the inner HTML content for one lane (capacity + funnels + rep details)."""
    daily = data["daily_data"]

    def tc(d):
        if d == today: return " today"
        elif d < today: return " past"
        return ""

    # ── Hero Card Data (Phase 2) ──────────────────────────────────────────────
    # Build per-date card payloads. JS picks 3 to render (prev/current/next) based
    # on a focused index pointer. All 13 days are pre-rendered as JSON below.
    cats_by_date = data.get("meetings_by_category_by_date", {})
    # LANE-MERGE TRANSITION: Lane 1 booked map for the Booking Window Missed metric.
    # Falls back to {} if absent so the metric just shows 0/dash instead of crashing.
    # To roll back: delete this line and revert the `lane1_b = ...` + missed_val math below.
    lane1_booked_by_date = data.get("lane1_booked_by_date", {})
    card_data = {}
    for d in dates:
        ds = d.isoformat()
        b = daily[d]["booked"]
        cal_slots = daily[d].get("calendly_available")  # Live open Calendly slots
        max_total = daily[d].get("max_calendar_availability")
        day_target = get_capacity_target(d)
        cats = cats_by_date.get(d, {"fu": 0, "resch": 0, "other": 0})

        # Card "Total Meetings Booked" = New + F/U + Resch + Other (definitional, math always works).
        # Each meeting is counted in exactly one bucket via priority hierarchy in
        # fetch_rep_total_meetings, so this sum reflects all classifiable meetings on the calendar.
        total_meet = b + cats["fu"] + cats["resch"] + cats["other"]

        # Booking Window Missed = max_total - lane1_booked - open (Lane 1 perspective).
        # Cache snapshot is Lane 1 booked + team Calendly available, so the math stays
        # consistent. To roll back: change `lane1_b` to `b` in the line below.
        lane1_b = lane1_booked_by_date.get(d, b)  # falls back to combined if map missing
        missed_val = None
        if max_total and max_total > 0 and cal_slots is not None and d <= today:
            missed_val = max(0, max_total - lane1_b - cal_slots)

        # Open slots: prefer live Calendly value; fall back to static capacity diff for
        # today / future only. Past days never have "open slots" — they're done — so we
        # force None regardless of any leftover capacity number that might be in daily_data.
        if d < today:
            open_slots = None
        elif cal_slots is not None:
            open_slots = cal_slots
        elif daily[d]["capacity"] > 0:
            open_slots = max(0, daily[d]["capacity"] - b)
        else:
            open_slots = None

        # Target % = new calls / target (None on weekends)
        if day_target is not None and day_target > 0:
            target_pct = b / day_target * 100
        else:
            target_pct = None

        card_data[ds] = {
            "date": ds,
            "weekday_short": d.strftime("%a"),
            "month_day": d.strftime("%-m/%-d"),
            "is_today": d == today,
            "is_past": d < today,
            "is_future": d > today,
            "new_meetings": b,
            "total_meetings": total_meet,
            "fu": cats["fu"],
            "resch": cats["resch"],
            "other": cats["other"],
            "open_slots": open_slots,  # int or None
            "missed": missed_val,      # int or None
            "target": day_target,      # int or None (weekend)
            "target_pct": target_pct,  # float or None
            "target_class": target_class(target_pct) if target_pct is not None else "",
        }

    # Funnel section rows (dynamic — only funnels with >=1 call)
    ext_rows = build_funnel_rows(data, dates, today, daily_goal_map, "external")
    inh_rows = build_funnel_rows(data, dates, today, daily_goal_map, "inhouse")
    unc_rows = build_uncategorized_rows(data, dates, today)

    # Total row
    total_cells = ""
    for d in dates:
        t = tc(d)
        total_cells += f'<td class="num total-num{t}">{daily[d]["booked"]}</td>'

    # Date headers (clickable for detail panel)
    date_headers = ""
    for d in dates:
        label = "► TODAY" if d == today else d.strftime("%a").upper()
        ds = d.strftime("%m/%d")
        date_headers += f'<th class="col-date{tc(d)} day-clickable" title="Click for details" onclick="showDayDetail(\'{d.isoformat()}\')">{label}<br>{ds}</th>'

    # Rep Details
    rep_data = data.get("rep_data", {})
    rep_total_meetings = data.get("rep_total_meetings", {})
    rep_rows = ""
    rep_summary_parts = []

    # Sort reps alphabetically, lead last
    sorted_uids = sorted(lane_rep_names.keys(), key=lambda uid: (uid == lane_lead, lane_rep_names[uid]))

    for uid in sorted_uids:
        rep_name = lane_rep_names.get(uid, uid)
        badge = ' <span style="background:#1b7a2e;color:#fff;font-size:0.6rem;padding:1px 6px;border-radius:3px;margin-left:4px;">Lead</span>' if uid == lane_lead else ""
        day_data = rep_data.get(uid, {})
        rep_total_by_date = rep_total_meetings.get(uid, {})

        rep_funnels = set()
        rep_total = 0
        for d in dates:
            for f, c in day_data.get(d, {}).items():
                rep_funnels.add(f)
                rep_total += c

        if rep_total > 0:
            rep_summary_parts.append(f"{rep_name.split()[0]}: {rep_total}")

        header_cells = ""
        for d in dates:
            t = tc(d)
            day_total = sum(day_data.get(d, {}).values())
            header_cells += f'<td class="num ltf-total{t}">{day_total}</td>' if day_total > 0 else f'<td class="num zero{t}">0</td>'

        sep = ' style="border-top:2px solid #e0e0e0;"' if rep_rows else ""
        rep_rows += f'<tr{sep}><td class="metric" style="font-weight:700;padding-top:0.5rem;">{rep_name}{badge}</td>{header_cells}</tr>\n'

        for funnel in sorted(rep_funnels):
            funnel_cells = ""
            for d in dates:
                t = tc(d)
                c = day_data.get(d, {}).get(funnel, 0)
                funnel_cells += f'<td class="num{t}">{c}</td>' if c > 0 else f'<td class="num zero{t}">0</td>'
            rep_rows += f'<tr><td class="metric" style="padding-left:1.2rem;font-size:0.72rem;color:#555;">{funnel}</td>{funnel_cells}</tr>\n'

        # Total Calls row — all non-internal meetings (first calls + follow-ups + reschedules + Q&A etc.)
        # Informational only — rendered in muted gray.
        total_calls_cells = ""
        for d in dates:
            t = tc(d)
            total_count = rep_total_by_date.get(d, 0)
            if total_count > 0:
                total_calls_cells += f'<td class="num{t}" style="color:#777;">{total_count}</td>'
            else:
                total_calls_cells += f'<td class="num zero{t}">0</td>'
        rep_rows += (
            '<tr>'
            '<td class="metric" style="padding-left:1.2rem;font-size:0.72rem;color:#777;'
            'font-weight:600;border-top:1px solid #ececec;" '
            'title="All non-internal meetings on the calendar (first calls + follow-ups + reschedules). '
            'Informational — compare against the rep header above to spot mis-titled meetings.">'
            "Total Calls (Inc. F/Us, Resch., etc.)</td>"
            f'{total_calls_cells}</tr>\n'
        )

    rep_summary = "Rep Details — " + " · ".join(rep_summary_parts) if rep_summary_parts else "Rep Details — No calls"

    # Build section HTML, only include sections with rows
    # Section label sits IN the first cell of the date header row — visually aligned
    # with the date columns, no longer a banner above the table.
    funnel_html = ""
    if inh_rows:
        funnel_html += f"""
    <table><colgroup><col style="width:200px"><col span="{n_cols}"></colgroup>
      <thead><tr><th class="sec-label">FUNNEL DETAILS — IN-HOUSE</th>{date_headers}</tr></thead>
      <tbody>{inh_rows}</tbody>
    </table>"""
    if ext_rows:
        funnel_html += f"""
    <table><colgroup><col style="width:200px"><col span="{n_cols}"></colgroup>
      <thead><tr><th class="sec-label">FUNNEL DETAILS — EXTERNAL</th>{date_headers}</tr></thead>
      <tbody>{ext_rows}</tbody>
    </table>"""
    if unc_rows:
        funnel_html += f"""
    <table><colgroup><col style="width:200px"><col span="{n_cols}"></colgroup>
      <thead><tr><th class="sec-label is-compact">FUNNEL DETAILS — UNCATEGORIZED</th>{date_headers}</tr></thead>
      <tbody>{unc_rows}</tbody>
    </table>"""

    return f"""
  <div class="card hero-card-row" data-card-data='{json.dumps(card_data)}'>
    <button class="hero-arrow hero-arrow-left" onclick="shiftCardFocus(-1)" aria-label="Previous day">‹</button>
    <div class="hero-slot hero-slot-prev"   id="hero-slot-prev"   onclick="setCardFocus(_focusedIdx - 1)"></div>
    <div class="hero-slot hero-slot-current" id="hero-slot-current" onclick="openFocusedDayDetail()"></div>
    <div class="hero-slot hero-slot-next"   id="hero-slot-next"   onclick="setCardFocus(_focusedIdx + 1)"></div>
    <button class="hero-arrow hero-arrow-right" onclick="shiftCardFocus(1)" aria-label="Next day">›</button>
  </div>

  <div class="card">
    {funnel_html}
    <table><colgroup><col style="width:200px"><col span="{n_cols}"></colgroup>
      <tbody>
        <tr class="total-row"><td class="metric">TOTAL</td>{total_cells}</tr>
      </tbody>
    </table>
  </div>

  <div class="ltf-collapsible">
    <details>
      <summary>{rep_summary}</summary>
      <div class="card" style="margin-top:0.5rem;">
        <table><colgroup><col style="width:200px"><col span="{n_cols}"></colgroup>
          <thead><tr><th></th>{date_headers}</tr></thead>
          <tbody>
            {rep_rows}
          </tbody>
        </table>
      </div>
    </details>
  </div>"""


def generate_rolling_html(team_data, team_detail=None):
    dates = team_data["dates"]
    today = team_data["today"]

    now_pacific = datetime.now(PACIFIC)
    last_updated = now_pacific.strftime("%I:%M %p %Z")
    last_updated_date = now_pacific.strftime("%A, %B %-d, %Y")

    year, month = now_pacific.year, now_pacific.month
    daily_goal_map = {}
    for fc in FUNNEL_CONFIG:
        dg = get_daily_goal(fc["monthly_goal"], year, month)
        daily_goal_map[fc["name"]] = dg

    n_cols = len(dates)
    wd = working_days_in_month(year, month)

    # Check for recent changelog entry (within current week)
    recent_alert_html = ""
    if CHANGELOG_ENTRIES:
        latest = CHANGELOG_ENTRIES[0]
        latest_date_str = latest["date"].split(" PT")[0].split(" PM")[0].split(" AM")[0].strip()
        try:
            # Parse date like "2026-05-11 6:00"
            parts = latest["date"].split()
            latest_date = date.fromisoformat(parts[0])
        except (ValueError, IndexError):
            latest_date = None
        
        if latest_date and (today - latest_date).days < 7:
            notes_text = " — ".join(latest["notes"][:2])  # Show first 2 notes
            recent_alert_html = f"""
  <div class="new-changes-alert" onclick="window.location.href='changelog.html'">
    <span class="pulse-dot"></span>
    <span class="alert-text">New Changes! - {latest['date']} — {latest['notes'][0]}</span>
  </div>"""

    team_content = generate_lane_content(team_data, dates, today, daily_goal_map, n_cols, ALL_LANE_REP_NAMES, ALL_LANE_LEAD, show_capacity=True)

    # Embed detail data as JSON for the single team
    detail_json = json.dumps(team_detail or {})

    toggle_css = """
    .new-changes-alert { display:flex; align-items:center; gap:8px; background:#1b2e1b; color:#fff;
                         padding:6px 16px; font-size:0.72rem; cursor:pointer; margin:-0.5rem 0 0; }
    .new-changes-alert:hover { background:#243d24; }
    .alert-text { opacity:0.9; }
    @keyframes pulse { 0%, 100% { opacity:1; transform:scale(1); } 50% { opacity:0.4; transform:scale(0.7); } }
    .pulse-dot { width:10px; height:10px; border-radius:50%; background:#4ade80; display:inline-block;
                 animation: pulse 1.5s ease-in-out infinite; flex-shrink:0; }
    """

    panel_css = """
    .day-clickable { cursor:pointer; transition:background 0.15s, transform 0.1s; position:relative; }
    .day-clickable:hover { background:rgba(27,122,46,0.08); border-radius:4px; }
    .day-clickable:active { transform:scale(0.97); }
    .funnel-expandable { cursor:pointer; transition:background 0.15s; }
    .funnel-expandable:hover { background:rgba(27,122,46,0.05); }
    .funnel-expandable .chevron { display:inline-block; font-size:0.65rem; color:#888; margin-right:4px; transition:transform 0.15s; }
    .setter-row td.label { font-style:italic; }
    .day-panel-overlay { display:none; position:fixed; top:0; left:0; width:100%; height:100%; background:rgba(0,0,0,0.3); z-index:998; }
    .day-panel { display:none; position:fixed; top:0; right:0; width:380px; height:100%; background:#fff; box-shadow:-4px 0 20px rgba(0,0,0,0.15);
                 z-index:999; overflow-y:auto; padding:1.5rem; font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif; }
    .day-panel .dp-close { position:absolute; top:12px; right:16px; font-size:1.2rem; cursor:pointer; color:#888; background:none; border:none; }
    .day-panel .dp-close:hover { color:#333; }
    .day-panel .dp-title { font-size:1.1rem; font-weight:700; margin-bottom:0.2rem; }
    .day-panel .dp-subtitle { font-size:0.75rem; color:#888; margin-bottom:1.2rem; }
    .day-panel .dp-section { font-size:0.7rem; font-weight:700; color:#1b7a2e; text-transform:uppercase; letter-spacing:0.5px; margin:1.2rem 0 0.5rem; }
    .day-panel .dp-bar-row { display:flex; align-items:center; margin-bottom:0.4rem; font-size:0.78rem; }
    .day-panel .dp-bar-label { width:120px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
    .day-panel .dp-bar-track { flex:1; height:16px; background:#f0f0f0; border-radius:3px; margin:0 8px; overflow:hidden; }
    .day-panel .dp-bar-fill { height:100%; background:#1b7a2e; border-radius:3px; transition:width 0.3s; }
    .day-panel .dp-bar-val { width:55px; text-align:right; font-weight:600; font-size:0.72rem; color:#555; }
    .day-panel .dp-rep-row { display:flex; justify-content:space-between; padding:3px 0; font-size:0.78rem; border-bottom:1px solid #f5f5f5; }
    .day-panel .dp-rep-name { color:#333; }
    .day-panel .dp-rep-count { font-weight:600; color:#1b7a2e; }
    .day-panel .dp-booked-table { width:100%; font-size:0.78rem; border-collapse:collapse; }
    .day-panel .dp-booked-table td { padding:4px 6px; border-bottom:1px solid #f0f0f0; }
    .day-panel .dp-booked-table td:first-child { font-weight:600; }
    .day-panel .dp-booked-table td:last-child { text-align:right; color:#888; }
    .day-panel .dp-coming-soon { color:#aaa; font-size:0.78rem; font-style:italic; padding:0.5rem 0; }

    /* ── F/U, Reschedule & Other Details (collapsible) ───────────────────── */
    .day-panel .dp-section-collapsible { cursor:pointer; user-select:none; display:flex;
                                          align-items:center; gap:6px; }
    .day-panel .dp-section-collapsible:hover { color:#155e1e; }
    .day-panel .dp-chevron { font-size:0.6rem; transition:transform 0.15s; }
    .day-panel .dp-section-count { color:#888; font-weight:600; font-size:0.65rem; letter-spacing:normal;
                                   text-transform:none; margin-left:auto; }
    .day-panel .dp-nonnew-list { padding:0.25rem 0 0.5rem; }
    .day-panel .dp-nonnew-row { padding:6px 0; font-size:0.75rem; line-height:1.4; border-bottom:1px solid #f5f5f5;
                                display:flex; flex-wrap:wrap; align-items:center; gap:4px; }
    .day-panel .dp-nonnew-row:last-child { border-bottom:none; }
    .day-panel .dp-cat-pill { display:inline-block; font-size:0.6rem; font-weight:700; padding:2px 6px;
                              border-radius:3px; text-transform:uppercase; letter-spacing:0.04em;
                              min-width:42px; text-align:center; }
    .day-panel .dp-cat-fu    { background:#e6f3ec; color:#1b7a2e; }
    .day-panel .dp-cat-resch { background:#fff4e0; color:#a76200; }
    .day-panel .dp-cat-other { background:#f0f0f0; color:#555; }
    .day-panel .dp-lead-link { color:#1b5e1b; text-decoration:underline; font-weight:600; }
    .day-panel .dp-lead-link:hover { color:#0f3d10; }
    .day-panel .dp-nonnew-sep { color:#ccc; margin:0 2px; }
    .day-panel .dp-nonnew-meta { color:#666; }

    /* ── Hero Card Row (Phase 2) ─────────────────────────────────────────── */
    .hero-card-row { display:flex; align-items:stretch; gap:14px; padding:18px 14px !important;
                     background:#fafafa; position:relative; }
    .hero-arrow { background:#fff; border:2px solid #1b7a2e; color:#1b7a2e; width:36px; height:36px;
                  border-radius:50%; font-size:1.4rem; font-weight:800; cursor:pointer; align-self:center;
                  display:flex; align-items:center; justify-content:center; line-height:1; padding:0;
                  flex-shrink:0; transition:all 0.15s; }
    .hero-arrow:hover:not(:disabled) { background:#1b7a2e; color:#fff; transform:scale(1.08); }
    .hero-arrow:disabled { opacity:0.25; cursor:not-allowed; }
    .hero-slot { flex:1; min-height:240px; transition:opacity 0.15s; }
    .hero-slot-prev, .hero-slot-next { flex:0.7; opacity:0.75; cursor:pointer; }
    .hero-slot-prev:hover, .hero-slot-next:hover { opacity:1; }
    .hero-slot-current { cursor:pointer; }
    .hero-slot:empty { visibility:hidden; }

    .hero-card { background:#fff; border:1px solid #e5e5e5; border-radius:8px; padding:14px 16px;
                 height:100%; display:flex; flex-direction:column; box-sizing:border-box;
                 font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;
                 transition:transform 0.1s, box-shadow 0.15s; }
    .hero-slot-current .hero-card { border:2px solid #1b7a2e; box-shadow:0 4px 16px rgba(27,122,46,0.1);
                                    padding:18px 22px; }
    .hero-slot-prev .hero-card, .hero-slot-next .hero-card { padding:12px 14px; }
    .hero-slot-current .hero-card:hover { box-shadow:0 6px 20px rgba(27,122,46,0.18); transform:translateY(-1px); }
    .hero-slot-prev .hero-card:hover, .hero-slot-next .hero-card:hover {
        border-color:#1b7a2e; box-shadow:0 2px 8px rgba(27,122,46,0.12); }

    .hero-card.is-today { border-color:#1b7a2e; }
    .hero-card.is-past  { background:#fafafa; }

    .hc-header { display:flex; justify-content:space-between; align-items:flex-start; margin-bottom:8px; gap:12px; }
    .hc-header-text { display:flex; flex-direction:column; }
    .hc-day-label { font-size:0.7rem; color:#888; font-weight:700; text-transform:uppercase;
                    letter-spacing:0.05em; line-height:1.1; }
    .hero-slot-current .hc-day-label { font-size:0.78rem; color:#1b7a2e; }
    .hc-title { font-size:0.85rem; font-weight:700; color:#333; margin-top:2px; line-height:1.15; }
    .hero-slot-current .hc-title { font-size:1rem; }
    .hc-headline { font-size:2rem; font-weight:800; color:#1b7a2e; line-height:0.9; }
    .hero-slot-current .hc-headline { font-size:2.6rem; }
    /* Side cards: smaller headline. Shift it down so it visually aligns with the
       "New Meetings Booked" title line rather than sitting up next to the day label. */
    .hero-slot-prev .hc-headline, .hero-slot-next .hc-headline { font-size:1.6rem; margin-top:14px; }

    .hc-sep { border:none; border-top:1px solid #e5e5e5; margin:6px 0 10px; }

    .hc-section { margin-bottom:8px; }
    /* Slight gap between Total breakdown and Open Calendar Slots — just enough
       to feel like distinct sections without leaving an awkward dead zone. */
    .hc-section + .hc-section { margin-top:4px; }
    .hc-row { display:flex; justify-content:space-between; align-items:baseline; padding:2px 0;
              font-size:0.82rem; }
    .hero-slot-current .hc-row { font-size:0.92rem; }
    .hc-row-label { color:#444; }
    .hc-row-value { font-weight:700; color:#1a1a1a; font-variant-numeric:tabular-nums; }
    .hc-row-parent { font-weight:600; }
    .hc-row-parent .hc-row-value { color:#1a1a1a; }
    .hc-row-child { padding-left:16px; color:#666; font-size:0.78rem; }
    .hero-slot-current .hc-row-child { font-size:0.84rem; }
    .hc-row-child .hc-row-value { color:#666; font-weight:600; }
    .hc-row-missed .hc-row-value { color:#c0392b; }
    /* Total-summary row at the bottom of the breakdown — sits under F/U / Resch / Other.
       Separator line BELOW the Total row (between Total and Open Calendar Slots). */
    .hc-row-total { font-weight:700; margin-top:4px; padding-bottom:6px; border-bottom:1px solid #e5e5e5; }
    .hc-row-total .hc-row-label { color:#1a1a1a; }

    .hc-footer { margin-top:auto; padding-top:10px; border-top:1px solid #e5e5e5;
                 display:flex; justify-content:space-between; align-items:baseline; }
    .hc-footer-label { font-size:0.75rem; font-weight:700; color:#555;
                       text-transform:uppercase; letter-spacing:0.04em; }
    .hero-slot-current .hc-footer-label { font-size:0.82rem; }
    .hc-footer-value { font-size:1.4rem; font-weight:800; font-variant-numeric:tabular-nums; }
    .hero-slot-current .hc-footer-value { font-size:1.8rem; }
    .hc-footer-value.tgt-red    { color:#c0392b; }
    .hc-footer-value.tgt-amber  { color:#d4860c; }
    .hc-footer-value.tgt-green  { color:#1b7a2e; }
    .hc-footer-value.tgt-na     { color:#999; }
    """

    panel_js = """
    <script>
    var _dayDetail = """ + detail_json + """;

    function toggleSetterRows(row) {
      var chevron = row.querySelector('.chevron');
      var expanded = row.classList.toggle('expanded');
      chevron.textContent = expanded ? '▼' : '▶';
      // Toggle all immediately-following sibling .setter-row elements (until next non-setter row)
      var next = row.nextElementSibling;
      while (next && next.classList.contains('setter-row')) {
        next.style.display = expanded ? '' : 'none';
        next = next.nextElementSibling;
      }
    }

    function showDayDetail(dateStr) {
      var detail = _dayDetail[dateStr];
      if (!detail) return;
      var nonNewCount = (detail.non_new_meetings || []).length;
      // Allow panel to open if there's any data at all (new calls OR non-new meetings).
      if (detail.total === 0 && nonNewCount === 0) return;

      var panel = document.getElementById('dayPanel');
      var overlay = document.getElementById('dayOverlay');

      // Title
      var d = new Date(dateStr + 'T12:00:00');
      var days = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
      var months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
      panel.querySelector('.dp-title').textContent = days[d.getDay()] + ', ' + months[d.getMonth()] + ' ' + d.getDate();
      // Subtitle: include non-new count alongside new calls when present
      var subtitle = detail.total + ' new call' + (detail.total === 1 ? '' : 's');
      if (nonNewCount > 0) subtitle += ' · ' + nonNewCount + ' non-new';
      panel.querySelector('.dp-subtitle').textContent = subtitle;

      // Funnels — gracefully handle empty (e.g., day with only non-new meetings)
      var funnelHtml = '';
      if (detail.funnels && detail.funnels.length > 0) {
        var maxPct = Math.max.apply(null, detail.funnels.map(function(f){return f[2]}));
        detail.funnels.forEach(function(f) {
          var barW = maxPct > 0 ? (f[2] / maxPct * 100) : 0;
          funnelHtml += '<div class="dp-bar-row">' +
            '<span class="dp-bar-label">' + f[0] + '</span>' +
            '<div class="dp-bar-track"><div class="dp-bar-fill" style="width:' + barW + '%"></div></div>' +
            '<span class="dp-bar-val">' + f[1] + ' (' + f[2] + '%)</span></div>';
        });
      } else {
        funnelHtml = '<div class="dp-coming-soon">No new sales calls on this day.</div>';
      }
      document.getElementById('dpFunnels').innerHTML = funnelHtml;

      // Reps
      var repHtml = '';
      if (detail.reps && detail.reps.length > 0) {
        detail.reps.forEach(function(r) {
          repHtml += '<div class="dp-rep-row"><span class="dp-rep-name">' + r[0] + '</span><span class="dp-rep-count">' + r[1] + '</span></div>';
        });
      } else {
        repHtml = '<div class="dp-coming-soon">No rep data for new sales calls.</div>';
      }
      document.getElementById('dpReps').innerHTML = repHtml;

      // Booked on
      var bookedHtml = '<table class="dp-booked-table">';
      if (detail.booked_on && detail.booked_on.length > 0) {
        detail.booked_on.forEach(function(b) {
          var label = b[0];
          if (label !== 'Unknown') {
            var bd = new Date(label + 'T12:00:00');
            label = days[bd.getDay()] + ' ' + (bd.getMonth()+1) + '/' + bd.getDate();
          }
          bookedHtml += '<tr><td>' + label + '</td><td>' + b[1] + ' calls</td><td>' + b[2] + '%</td></tr>';
        });
      } else {
        bookedHtml += '<tr><td colspan="3" style="color:#aaa;">No booking date data</td></tr>';
      }
      bookedHtml += '</table>';
      document.getElementById('dpBooked').innerHTML = bookedHtml;

      // F/U, Reschedule & Other Details — starts collapsed
      var nonNew = detail.non_new_meetings || [];
      var countEl = document.getElementById('dpNonNewCount');
      var bodyEl  = document.getElementById('dpNonNewDetails');
      var chevEl  = document.getElementById('dpNonNewChevron');
      countEl.textContent = '(' + nonNew.length + ')';
      bodyEl.style.display = 'none';
      chevEl.textContent = '▶';
      if (nonNew.length === 0) {
        bodyEl.innerHTML = '<div class="dp-coming-soon">No follow-up, reschedule, or other meetings on this day.</div>';
      } else {
        var nnHtml = '<div class="dp-nonnew-list">';
        nonNew.forEach(function(m) {
          var catClass = 'dp-cat-' + m.category;
          var safeName = (m.lead_name || '(unknown lead)')
            .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
          var safeFunnel = (m.funnel_name || '(no funnel)')
            .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
          var safeOwner = (m.owner_name || '')
            .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
          nnHtml += '<div class="dp-nonnew-row">' +
            '<span class="dp-cat-pill ' + catClass + '">' + m.category_label + '</span> ' +
            '<a href="' + m.lead_url + '" target="_blank" rel="noopener" class="dp-lead-link">' + safeName + '</a> ' +
            '<span class="dp-nonnew-sep">|</span> ' +
            '<span class="dp-nonnew-meta">' + safeFunnel + '</span> ' +
            '<span class="dp-nonnew-sep">|</span> ' +
            '<span class="dp-nonnew-meta">' + safeOwner + '</span>' +
            '</div>';
        });
        nnHtml += '</div>';
        bodyEl.innerHTML = nnHtml;
      }

      panel.style.display = 'block';
      overlay.style.display = 'block';
    }

    function toggleNonNewDetails() {
      var body = document.getElementById('dpNonNewDetails');
      var chev = document.getElementById('dpNonNewChevron');
      var open = body.style.display !== 'none';
      body.style.display = open ? 'none' : 'block';
      chev.textContent  = open ? '▶' : '▼';
    }

    function closeDayPanel() {
      document.getElementById('dayPanel').style.display = 'none';
      document.getElementById('dayOverlay').style.display = 'none';
    }

    // ── Hero Card Row (Phase 2) ──────────────────────────────────────────
    var _cardData = {};
    var _datesList = [];
    var _focusedIdx = 0;

    function fmtMaybe(v) { return (v === null || v === undefined) ? '–' : v; }
    function fmtPct(v, cls) {
      if (v === null || v === undefined) {
        return '<span class="hc-footer-value tgt-na">N/A</span>';
      }
      return '<span class="hc-footer-value ' + (cls || '') + '">' + v.toFixed(1) + '%</span>';
    }

    function buildCardHTML(idx) {
      if (idx < 0 || idx >= _datesList.length) return '';
      var ds = _datesList[idx];
      var c = _cardData[ds];
      if (!c) return '';

      var classes = ['hero-card'];
      if (c.is_today)  classes.push('is-today');
      if (c.is_past)   classes.push('is-past');
      if (c.is_future) classes.push('is-future');

      var dayLabel = c.is_today ? '► TODAY' : c.weekday_short.toUpperCase() + ' ' + c.month_day;

      var openVal   = (c.open_slots === null || c.open_slots === undefined) ? '–' : c.open_slots;
      var missedVal = (c.missed     === null || c.missed     === undefined) ? '–' : c.missed;

      // Footer label includes the actual target value when available (e.g. "New Meetings Goal % (44)").
      // Weekends have no target, so just show the label.
      var goalLabel = (c.target !== null && c.target !== undefined)
        ? 'New Meetings Goal % (' + c.target + ')'
        : 'New Meetings Goal %';

      return '<div class="' + classes.join(' ') + '">' +
        '<div class="hc-header">' +
          '<div class="hc-header-text">' +
            '<div class="hc-day-label">' + dayLabel + '</div>' +
            '<div class="hc-title">New Meetings Booked</div>' +
          '</div>' +
          '<div class="hc-headline">' + c.new_meetings + '</div>' +
        '</div>' +
        '<hr class="hc-sep">' +
        '<div class="hc-section">' +
          '<div class="hc-row">' +
            '<span class="hc-row-label">F/U Meetings</span>' +
            '<span class="hc-row-value">' + c.fu + '</span>' +
          '</div>' +
          '<div class="hc-row">' +
            '<span class="hc-row-label">Resch. Meetings</span>' +
            '<span class="hc-row-value">' + c.resch + '</span>' +
          '</div>' +
          '<div class="hc-row">' +
            '<span class="hc-row-label">Other</span>' +
            '<span class="hc-row-value">' + c.other + '</span>' +
          '</div>' +
          '<div class="hc-row hc-row-total">' +
            '<span class="hc-row-label">Total Meetings Booked</span>' +
            '<span class="hc-row-value">' + c.total_meetings + '</span>' +
          '</div>' +
        '</div>' +
        '<div class="hc-section">' +
          '<div class="hc-row hc-row-parent">' +
            '<span class="hc-row-label">Open Calendar Slots</span>' +
            '<span class="hc-row-value">' + openVal + '</span>' +
          '</div>' +
          '<div class="hc-row hc-row-child hc-row-missed">' +
            '<span class="hc-row-label">Booking Window Missed</span>' +
            '<span class="hc-row-value">' + missedVal + '</span>' +
          '</div>' +
        '</div>' +
        '<div class="hc-footer">' +
          '<span class="hc-footer-label">' + goalLabel + '</span>' +
          fmtPct(c.target_pct, c.target_class) +
        '</div>' +
      '</div>';
    }

    function renderCards() {
      document.getElementById('hero-slot-prev').innerHTML    = buildCardHTML(_focusedIdx - 1);
      document.getElementById('hero-slot-current').innerHTML = buildCardHTML(_focusedIdx);
      document.getElementById('hero-slot-next').innerHTML    = buildCardHTML(_focusedIdx + 1);
      var leftArrow  = document.querySelector('.hero-arrow-left');
      var rightArrow = document.querySelector('.hero-arrow-right');
      if (leftArrow)  leftArrow.disabled  = (_focusedIdx <= 0);
      if (rightArrow) rightArrow.disabled = (_focusedIdx >= _datesList.length - 1);
    }

    function shiftCardFocus(delta) {
      var n = _focusedIdx + delta;
      if (n < 0 || n >= _datesList.length) return;
      _focusedIdx = n;
      renderCards();
    }

    function setCardFocus(idx) {
      if (idx < 0 || idx >= _datesList.length) return;
      _focusedIdx = idx;
      renderCards();
    }

    function openFocusedDayDetail() {
      var ds = _datesList[_focusedIdx];
      if (ds) showDayDetail(ds);
    }

    // Boot the hero card row: read embedded data + initial focus on today (or end of list)
    (function initHeroCards() {
      var row = document.querySelector('.hero-card-row');
      if (!row) return;
      try {
        _cardData = JSON.parse(row.getAttribute('data-card-data') || '{}');
      } catch (e) {
        console.error('Hero card data parse failed', e);
        return;
      }
      _datesList = Object.keys(_cardData).sort();
      // Find today's index; fall back to the last available date
      _focusedIdx = _datesList.length - 1;
      for (var i = 0; i < _datesList.length; i++) {
        if (_cardData[_datesList[i]].is_today) { _focusedIdx = i; break; }
      }
      renderCards();
    })();
    </script>
    """

    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Call Capacity Dashboard</title>
<style>{COMMON_CSS}
{toggle_css}
{panel_css}
</style>
</head><body>
{html_header_bar("Call Capacity Dashboard", f"4-Day Trailing + 10-Day Lookahead · First Meetings Only · {wd} working days in {now_pacific.strftime('%B')}", last_updated_date, "Last updated: " + last_updated + ' · <a href="changelog.html" style="color:#fff;opacity:0.7;text-decoration:none;font-weight:400;">📋 Changelog</a>')}
{recent_alert_html}
<div class="wrap">

  <div id="team">
  {team_content}
  </div>

  <div class="footer">
    <span>Source: First Sales Call Booked Date field · <a href="archive.html">📁 Archive</a></span>
    <a href="https://stephenolivas.github.io/mtd-funnel-dashboard/" target="_blank">📊 MTD Funnel Reporting →</a>
  </div>
</div>

<!-- Day Detail Panel -->
<div id="dayOverlay" class="day-panel-overlay" onclick="closeDayPanel()"></div>
<div id="dayPanel" class="day-panel">
  <button class="dp-close" onclick="closeDayPanel()">✕</button>
  <div class="dp-title"></div>
  <div class="dp-subtitle"></div>

  <div class="dp-section">Funnel Breakdown</div>
  <div id="dpFunnels"></div>

  <div class="dp-section">Rep Breakdown</div>
  <div id="dpReps"></div>

  <div class="dp-section">When Booked</div>
  <div id="dpBooked"></div>

  <div class="dp-section dp-section-collapsible" onclick="toggleNonNewDetails()">
    <span class="dp-chevron" id="dpNonNewChevron">▶</span> F/U, Reschedule &amp; Other Details
    <span class="dp-section-count" id="dpNonNewCount"></span>
  </div>
  <div id="dpNonNewDetails" style="display:none;"></div>
</div>

{panel_js}
</body></html>"""



# ─── Weekly Summary HTML ─────────────────────────────────────────────────────

def generate_weekly_html(data, week_start):
    dates = data["dates"]; daily = data["daily_data"]
    week_end = week_start + timedelta(days=6)
    total_booked = sum(daily[d]["booked"] for d in dates)
    total_cap = sum(daily[d]["capacity"] for d in dates)
    avg_util = (total_booked / total_cap * 100) if total_cap > 0 else 0

    date_headers = "".join(f'<th class="col-date">{d.strftime("%a").upper()}<br>{d.strftime("%m/%d")}</th>' for d in dates)
    date_headers += '<th class="col-date" style="background:#f0f0f0;">TOTAL</th>'
    n_cols = len(dates) + 1

    cap_r = booked_r = avail_r = util_r = ""
    tc_cap = tc_bk = 0
    for d in dates:
        c = daily[d]["capacity"]; b = daily[d]["booked"]; tc_cap += c; tc_bk += b
        cap_r += f'<td class="num">{c if c > 0 else "–"}</td>'
        booked_r += f'<td class="num {"booked" if b > 0 else "zero"}">{b}</td>'
        avail_r += f'<td class="num">{c - b if c > 0 else "–"}</td>'
        if c > 0:
            pct = b / c * 100
            util_r += f'<td class="num {util_class(pct)}">{pct:.1f}%</td>'
        else:
            util_r += f'<td class="num">N/A</td>'
    ou = (tc_bk / tc_cap * 100) if tc_cap > 0 else 0
    cap_r += f'<td class="num total-num">{tc_cap}</td>'
    booked_r += f'<td class="num total-num booked">{tc_bk}</td>'
    avail_r += f'<td class="num total-num">{tc_cap - tc_bk}</td>'
    util_r += f'<td class="num total-num {util_class(ou)}">{ou:.1f}%</td>'

    # Simple funnel totals for weekly
    funnel_totals = {}
    for m in data["valid_meetings"]:
        f = m["funnel"]; funnel_totals[f] = funnel_totals.get(f, 0) + 1
    funnel_rows = ""
    for fn in sorted(funnel_totals.keys()):
        cnt = funnel_totals[fn]
        funnel_rows += f'<tr><td class="label">{fn}</td><td class="num {"booked" if cnt > 0 else "zero"}">{cnt}</td></tr>\n'
    funnel_rows += f'<tr class="total-row"><td class="metric">TOTAL</td><td class="num total-num">{total_booked}</td></tr>\n'

    title = f"Weekly Summary — {week_start.strftime('%b %-d')} to {week_end.strftime('%b %-d, %Y')}"
    return f"""<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>{title}</title><style>{COMMON_CSS}</style></head><body>
{html_header_bar(title, "Monday through Sunday · First Meetings Only", week_start.strftime("%B %-d, %Y"), "Generated: " + datetime.now(PACIFIC).strftime("%b %-d at %I:%M %p %Z"))}
<div class="wrap">
  <div class="summary-cards">
    <div class="s-card"><div class="s-label">Total Booked</div><div class="s-value green">{total_booked}</div></div>
    <div class="s-card"><div class="s-label">Total Capacity</div><div class="s-value">{total_cap}</div></div>
    <div class="s-card"><div class="s-label">Avg Utilization</div><div class="s-value {util_class(avg_util)}">{avg_util:.1f}%</div></div>
  </div>
  <div class="card"><div class="sec">DAILY BREAKDOWN</div>
    <table><colgroup><col style="width:200px"><col span="{n_cols}"></colgroup>
    <thead><tr><th></th>{date_headers}</tr></thead>
    <tbody><tr><td class="metric">Capacity</td>{cap_r}</tr><tr><td class="metric">Booked</td>{booked_r}</tr><tr><td class="metric">Available</td>{avail_r}</tr><tr><td class="metric">Utilization %</td>{util_r}</tr></tbody></table>
  </div>
  <div class="card"><div class="sec">FUNNEL TOTALS</div>
    <table style="table-layout:auto; max-width:400px;"><thead><tr><th>Funnel</th><th>Total</th></tr></thead><tbody>{funnel_rows}</tbody></table>
  </div>
  <div class="footer"><a href="../archive.html">← Back to Archive</a><a href="https://stephenolivas.github.io/mtd-funnel-dashboard/" target="_blank">📊 MTD Funnel Reporting →</a></div>
</div></body></html>"""


# ─── Monthly Summary HTML ────────────────────────────────────────────────────

def generate_monthly_html(data, month_date):
    dates = data["dates"]; daily = data["daily_data"]
    total_booked = sum(daily[d]["booked"] for d in dates)
    total_cap = sum(daily[d]["capacity"] for d in dates)
    avg_util = (total_booked / total_cap * 100) if total_cap > 0 else 0

    day_names = ["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"]
    week_header = "".join(f'<th class="col-date">{dn}</th>' for dn in day_names)
    week_header += '<th class="col-date" style="background:#f0f0f0;">TOTAL</th>'

    weeks = []; cw = []
    for d in dates:
        cw.append(d)
        if d.weekday() == 6 or d == dates[-1]:
            weeks.append(cw); cw = []

    week_rows = ""; grand_total = 0; dow_totals = [0] * 7
    for week in weeks:
        wl = f"{week[0].strftime('%m/%d')}–{week[-1].strftime('%m/%d')}"
        slots = [None] * 7
        for d in week: slots[d.weekday()] = d
        cells = ""; wt = 0
        for i in range(7):
            if slots[i] and slots[i] in daily:
                b = daily[slots[i]]["booked"]; wt += b; dow_totals[i] += b
                cells += f'<td class="num {"booked" if b > 0 else "zero"}">{b}</td>'
            else:
                cells += '<td class="num" style="color:#ccc;">–</td>'
        grand_total += wt
        week_rows += f'<tr><td class="label">Week of {wl}</td>{cells}<td class="num total-num">{wt}</td></tr>\n'
    dow_cells = "".join(f'<td class="num total-num">{t}</td>' for t in dow_totals)
    dow_cells += f'<td class="num total-num">{grand_total}</td>'
    week_rows += f'<tr class="total-row"><td class="metric">TOTAL</td>{dow_cells}</tr>\n'

    funnel_totals = {}
    for m in data["valid_meetings"]:
        f = m["funnel"]; funnel_totals[f] = funnel_totals.get(f, 0) + 1
    funnel_rows = ""
    for fn in sorted(funnel_totals.keys()):
        funnel_rows += f'<tr><td class="label">{fn}</td><td class="num {"booked" if funnel_totals[fn] > 0 else "zero"}">{funnel_totals[fn]}</td></tr>\n'
    funnel_rows += f'<tr class="total-row"><td class="metric">TOTAL</td><td class="num total-num">{total_booked}</td></tr>\n'

    month_name = month_date.strftime("%B %Y")
    title = f"Monthly Summary — {month_name}"
    return f"""<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>{title}</title><style>{COMMON_CSS}</style></head><body>
{html_header_bar(title, f"{len(dates)} days · First Meetings Only", month_name, "Generated: " + datetime.now(PACIFIC).strftime("%b %-d at %I:%M %p %Z"))}
<div class="wrap">
  <div class="summary-cards">
    <div class="s-card"><div class="s-label">Total Booked</div><div class="s-value green">{total_booked}</div></div>
    <div class="s-card"><div class="s-label">Total Capacity</div><div class="s-value">{total_cap}</div></div>
    <div class="s-card"><div class="s-label">Avg Utilization</div><div class="s-value {util_class(avg_util)}">{avg_util:.1f}%</div></div>
  </div>
  <div class="card"><div class="sec">WEEK BY WEEK</div>
    <table><colgroup><col style="width:200px"><col span="8"></colgroup>
    <thead><tr><th>Week</th>{week_header}</tr></thead><tbody>{week_rows}</tbody></table>
  </div>
  <div class="card"><div class="sec">FUNNEL TOTALS</div>
    <table style="table-layout:auto; max-width:400px;"><thead><tr><th>Funnel</th><th>Total</th></tr></thead><tbody>{funnel_rows}</tbody></table>
  </div>
  <div class="footer"><a href="../archive.html">← Back to Archive</a><a href="https://stephenolivas.github.io/mtd-funnel-dashboard/" target="_blank">📊 MTD Funnel Reporting →</a></div>
</div></body></html>"""


# ─── Archive Index ───────────────────────────────────────────────────────────

def generate_archive_html(archive_dir):
    daily_files = []; weekly_files = []; monthly_files = []
    ap = Path(archive_dir)
    if ap.exists():
        for f in sorted(ap.glob("*.html"), reverse=True):
            n = f.stem
            if n.startswith("month-"): monthly_files.append(n)
            elif n.startswith("week-"): weekly_files.append(n)
            else:
                try: date.fromisoformat(n); daily_files.append(n)
                except ValueError: pass

    def make_links(files):
        if not files:
            return '<tr><td class="label" style="color:#999;">No archives yet</td></tr>'
        rows = ""
        for f in files:
            display = f
            if f.startswith("week-"):
                d = date.fromisoformat(f.replace("week-", "")); end = d + timedelta(days=6)
                display = f"Week of {d.strftime('%b %-d')} – {end.strftime('%b %-d, %Y')}"
            elif f.startswith("month-"):
                parts = f.replace("month-", "").split("-")
                display = f"{calendar.month_name[int(parts[1])]} {parts[0]}"
            else:
                try:
                    d = date.fromisoformat(f); display = d.strftime("%A, %B %-d, %Y")
                    if d.weekday() == 6: display += " (Sun – EOW)"
                except ValueError: pass
            rows += f'<tr><td class="label"><a href="archive/{f}.html" style="color:#1b7a2e; text-decoration:none; font-weight:500;">{display}</a></td></tr>\n'
        return rows

    now_pacific = datetime.now(PACIFIC)
    return f"""<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>Call Capacity Dashboard — Archive</title><style>{COMMON_CSS} a:hover {{ text-decoration: underline !important; }}</style></head><body>
{html_header_bar("Call Capacity Dashboard — Archive", "Historical snapshots and summaries", now_pacific.strftime("%A, %B %-d, %Y"), "Updated: " + now_pacific.strftime("%I:%M %p %Z"))}
<div class="wrap">
  <div style="margin-bottom:1rem;"><a href="index.html" style="color:#1b7a2e; font-weight:600; text-decoration:none; font-size:0.85rem;">← Back to Live Dashboard</a></div>
  <div class="card"><div class="sec">📈 MONTHLY SUMMARIES</div><table style="table-layout:auto"><tbody>{make_links(monthly_files)}</tbody></table></div>
  <div class="card"><div class="sec">📊 WEEKLY SUMMARIES</div><table style="table-layout:auto"><tbody>{make_links(weekly_files)}</tbody></table></div>
  <div class="card"><div class="sec">📅 DAILY SNAPSHOTS</div><table style="table-layout:auto"><tbody>{make_links(daily_files)}</tbody></table></div>
  <div class="footer"><span>Archive generated {now_pacific.strftime("%b %-d, %Y at %I:%M %p %Z")}</span><a href="https://stephenolivas.github.io/mtd-funnel-dashboard/" target="_blank">📊 MTD Funnel Reporting →</a></div>
</div></body></html>"""


# ─── Changelog HTML ───────────────────────────────────────────────────────────

def generate_changelog_html():
    """Generate the changelog page with Dashboard Changes and Steering Committee tabs."""
    now_pacific = datetime.now(PACIFIC)

    def render_entries(entries):
        if not entries:
            return '<p style="color:#888;font-style:italic;">No entries yet.</p>'
        html = ""
        for entry in entries:
            html += f'<div class="cl-entry"><div class="cl-date">{entry["date"]}</div><ul>'
            for note in entry["notes"]:
                html += f"<li>{note}</li>"
            html += "</ul></div>"
        return html

    dashboard_entries = render_entries(CHANGELOG_ENTRIES)
    steering_entries = render_entries(STEERING_COMMITTEE_ENTRIES)

    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Changelog — Call Capacity Dashboard</title>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
         background: #faf9f6; color: #333; padding: 2rem; max-width: 800px; margin: 0 auto; }}
  .back {{ font-size: 0.85rem; color: #1b7a2e; text-decoration: none; display: inline-block; margin-bottom: 1.5rem; }}
  .back:hover {{ text-decoration: underline; }}
  h1 {{ font-size: 1.5rem; font-weight: 700; margin-bottom: 0.5rem; }}
  .subtitle {{ font-size: 0.8rem; color: #888; margin-bottom: 1.5rem; }}
  .tabs {{ display: flex; gap: 0; border-bottom: 2px solid #e0e0e0; margin-bottom: 1.5rem; }}
  .tab {{ padding: 10px 20px; font-size: 0.85rem; font-weight: 600; cursor: pointer;
          border: none; background: none; color: #888; border-bottom: 2px solid transparent;
          margin-bottom: -2px; transition: all 0.15s; }}
  .tab.active {{ color: #1b7a2e; border-bottom-color: #1b7a2e; }}
  .tab:hover:not(.active) {{ color: #555; }}
  .tab-content {{ display: none; }}
  .tab-content.active {{ display: block; }}
  .cl-entry {{ border-left: 3px solid #1b7a2e; padding: 0.5rem 0 0.5rem 1rem; margin-bottom: 1.2rem; }}
  .cl-date {{ font-size: 0.75rem; font-weight: 700; color: #1b7a2e; margin-bottom: 0.3rem; }}
  .cl-entry ul {{ margin: 0; padding-left: 1.2rem; }}
  .cl-entry li {{ font-size: 0.82rem; line-height: 1.5; color: #444; margin-bottom: 0.15rem; }}
</style>
</head><body>
<a href="index.html" class="back">← Back to Dashboard</a>
<h1>📋 Changelog</h1>
<p class="subtitle">Last generated: {now_pacific.strftime("%b %-d, %Y at %I:%M %p %Z")}</p>

<div class="tabs">
  <button class="tab active" onclick="showTab('dashboard')">Dashboard Changes</button>
  <button class="tab" onclick="showTab('steering')">Steering Committee Updates</button>
</div>

<div id="dashboard" class="tab-content active">
  {dashboard_entries}
</div>

<div id="steering" class="tab-content">
  {steering_entries}
</div>

<script>
function showTab(id) {{
  document.querySelectorAll('.tab-content').forEach(el => el.classList.remove('active'));
  document.querySelectorAll('.tab').forEach(el => el.classList.remove('active'));
  document.getElementById(id).classList.add('active');
  event.target.classList.add('active');
}}
</script>
</body></html>"""


# ─── EOD Email ────────────────────────────────────────────────────────────────
#
# Runs ONLY at 8pm PT on M-F. No impact on any other run of this script.
# Meeting counts (today + tomorrow) come from rolling_data already in memory —
# zero extra API calls for those. The only additional Close API calls at 8pm are:
#   - fetch_todays_won_opps(): query won opportunities for today
#   - fetch_leads_for_email(): targeted lead fetches for show rate, closer name, funnel, ICP
#
# Required GitHub Secrets:
#   RESEND_API_KEY  — from resend.com (free tier, 3k emails/month)
#   EMAIL_FROM      — e.g. "EOD Reports <eod@yourdomain.com>" (domain verified in Resend)
#   EMAIL_TO        — comma-separated recipient list, e.g. "joe@co.com,manager@co.com"
#
# ─────────────────────────────────────────────────────────────────────────────

GMAIL_APP_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD", "")
EMAIL_FROM         = os.environ.get("EMAIL_FROM", "")
EMAIL_TO           = [e.strip() for e in os.environ.get("EMAIL_TO", "").split(",") if e.strip()]

# ── Field IDs used only by the EOD email ─────────────────────────────────────

CF_LEAD_OWNER_NAME = "cf_gOfS9pFwext58oberEegLyix8hZzeHrxhCZOVh3P3rd"
CF_SHOW_UP         = "cf_OPyvpU45RdvjLqfm8V1VWwNxrGKogEH2IBJmfCj0Uhq"
CF_FUNNEL_DEAL     = "cf_xqDQE8fkPsWa0RNEve7hcaxKblCe6489XeZGRDzyPdX"
CF_ICP             = "cf_OcYP2vXsG2tvbMDubwQNcidiqVegXa7CsyWkOR3f7KN"

# ── Short funnel labels for the email body ────────────────────────────────────

FUNNEL_SHORT = {
    "Low Ticket Funnel":        "LTF",
    "LTF - Quiz Funnel":        "LTF Quiz",
    "Instagram":                "IG",
    "YouTube":                  "YT",
    "X":                        "X",
    "Linkedin":                 "LinkedIn",
    "Meta Ads":                 "Meta Ads",
    "VSL":                      "VSL",
    "Website":                  "Website",
    "Internal Webinar":         "Webinar",
    "Mike Newsletter":          "Newsletter",
    "AK TikTok":                "AK TT",
    "Anthony IG":               "Anthony IG",
    "Side Hustle Nation/WWWS":  "SHN/WWWS",
    "Passivepreneurs":          "Passivepreneurs",
    "Reactivation Email":       "Reactivation Email",
    "Reactivation Scrapers":    "Reactivation Scrapers",
    "LinkedIn Ads":             "LinkedIn Ads",
    "YouTube Ads":              "YouTube Ads",
    "Google Ads":               "Google Ads",
}


def fetch_close_users():
    """Fetch all Close org users and return a user_id → display name dict."""
    try:
        data = close_get("user", {"_limit": 100})
        return {u["id"]: u.get("display_name") or u.get("first_name", "Unknown")
                for u in data.get("data", [])}
    except Exception as e:
        log(f"  ⚠ EOD email: Could not fetch Close users: {e}")
        return {}


def fetch_todays_won_opps(today_str):
    """Fetch all opportunities marked won today."""
    opps = []
    skip = 0
    while True:
        data = close_get("opportunity", {
            "date_won__gte": today_str,
            "date_won__lte": today_str,
            "status_type":   "won",
            "_skip":          skip,
            "_limit":         100,
        })
        opps.extend(data.get("data", []))
        if not data.get("has_more"):
            break
        skip += 100
    return opps


def fetch_leads_for_email(lead_ids):
    """
    Fetch leads with all fields the EOD email needs.
    Runs only at 8pm — separate from the main dashboard lead cache.
    """
    fields = ",".join([
        "id",
        "status_id",
        f"custom.{CF_LEAD_OWNER_NAME}",
        f"custom.{CF_SHOW_UP}",
        f"custom.{CF_FUNNEL_DEAL}",
        f"custom.{CF_ICP}",
    ])
    cache = {}
    for lid in lead_ids:
        try:
            cache[lid] = close_get(f"lead/{lid}", {"_fields": fields})
        except Exception as e:
            log(f"  ⚠ EOD email: Could not fetch lead {lid}: {e}")
            cache[lid] = None
    return cache


def build_eod_data(rolling_data, today):
    """Assemble all data points needed for the EOD email (single team)."""

    # Meeting counts come from rolling_data already in memory — zero extra API calls
    today_count    = rolling_data["daily_data"].get(today, {}).get("booked", 0)
    tomorrow       = today + timedelta(days=1)
    tomorrow_count = rolling_data["daily_data"].get(tomorrow, {}).get("booked", 0)

    # Lead IDs from today's meetings (needed for show rate)
    today_lead_ids = list(set(
        m["lead_id"]
        for m in rolling_data["valid_meetings"]
        if m["date"] == today and m.get("lead_id")
    ))

    # Fetch today's won opportunities
    today_str = today.isoformat()
    won_opps  = fetch_todays_won_opps(today_str)
    log(f"   📧 EOD: {len(won_opps)} won opps today, {today_count} meetings today")

    # Combine all lead IDs we need: today's meetings + won opp leads
    won_lead_ids = list(set(o["lead_id"] for o in won_opps if o.get("lead_id")))
    all_lead_ids = list(set(today_lead_ids + won_lead_ids))

    email_leads = fetch_leads_for_email(all_lead_ids)

    # Fetch user map for resolving user_id → display name on won opps
    user_map = fetch_close_users()

    # ── Show rate ─────────────────────────────────────────────────────────────
    # Exclude leads in "Reschedule" status from both numerator and denominator —
    # they didn't have a chance to show, so including them deflates the rate.
    RESCHEDULE_STATUS_ID = "stat_2SmOUMCp1vDFJF0TcJ011hNnpLYWDGwugyo4JyiRMEP"
    showable_ids = [
        lid for lid in today_lead_ids
        if email_leads.get(lid) and
           email_leads[lid].get("status_id") != RESCHEDULE_STATUS_ID
    ]
    shown = sum(
        1 for lid in showable_ids
        if str(email_leads[lid].get(f"custom.{CF_SHOW_UP}", "")).lower() == "yes"
    )
    show_rate = (shown / len(showable_ids) * 100) if showable_ids else 0.0

    # ── Revenue ───────────────────────────────────────────────────────────────
    # Close stores opportunity `value` in USD cents — divide by 100.
    # If revenue looks wrong, check Close Settings → Pipeline to confirm units.
    total_revenue = sum((o.get("value") or 0) for o in won_opps) / 100

    # ── Closers ───────────────────────────────────────────────────────────────
    # Won opps have a `user_id` (the assigned user). Resolve to display name via user_map.
    closer_counts = {}
    for o in won_opps:
        uid  = o.get("user_id") or ""
        name = user_map.get(uid) or uid or "Unknown"
        closer_counts[name] = closer_counts.get(name, 0) + 1

    # ── Closed Won Funnel / ICP ───────────────────────────────────────────────
    icp_lines = []
    for o in won_opps:
        lid  = o.get("lead_id")
        lead = email_leads.get(lid)
        if not lead:
            continue
        raw_funnel   = lead.get(f"custom.{CF_FUNNEL_DEAL}") or ""
        funnel_full  = CLOSE_VALUE_TO_FUNNEL.get(raw_funnel, raw_funnel) or "Unknown"
        funnel_label = FUNNEL_SHORT.get(funnel_full, funnel_full)
        icp          = lead.get(f"custom.{CF_ICP}") or "Unknown"
        icp_lines.append(f"{funnel_label} / {icp}")

    return {
        "today":          today,
        "today_count":    today_count,
        "tomorrow_count": tomorrow_count,
        "show_rate":      show_rate,
        "deals":          len(won_opps),
        "revenue":        total_revenue,
        "closer_counts":  closer_counts,
        "icp_lines":      icp_lines,
    }


def format_eod_email(data):
    """Format the EOD email — returns (subject, plain_text, html)."""
    today    = data["today"]
    date_str = today.strftime("%-m/%-d")
    day_full = today.strftime("%A, %B %-d, %Y")

    # Revenue
    rev = data["revenue"]
    if rev >= 1_000_000:
        rev_str = f"${rev / 1_000_000:.2f}M"
    elif rev >= 1_000:
        rev_str = f"${rev / 1_000:.1f}k"
    else:
        rev_str = f"${rev:,.0f}"

    # Closers: sorted alphabetically, "x2" suffix for multiples
    closer_parts = []
    for name, count in sorted(data["closer_counts"].items()):
        closer_parts.append(f"{name} x{count}" if count > 1 else name)
    closers_str = ", ".join(closer_parts) if closer_parts else "None"

    # ICP lines
    icp_lines_plain = (
        "\n".join(f"* {line}" for line in data["icp_lines"])
        if data["icp_lines"] else "* None"
    )

    subject = f"EOD Stats {date_str}"

    # ── Plain text ────────────────────────────────────────────────────────────
    plain = (
        f"EOD stats for {date_str}:\n\n"
        f"Revenue: {rev_str}\n"
        f"Deals Closed: {data['deals']}\n"
        f"Closers: {closers_str}\n\n"
        f"New Meetings Today:        {data['today_count']}\n"
        f"Show Rate:                 {data['show_rate']:.0f}%\n"
        f"Meetings Set for Tomorrow: {data['tomorrow_count']}\n\n"
        f"Closed won funnel / ICP:\n{icp_lines_plain}\n"
    )

    # ── HTML ──────────────────────────────────────────────────────────────────
    icp_rows = "".join(
        f'<tr><td style="padding:6px 0;border-bottom:1px solid #f0f0f0;color:#333;font-size:14px;">📌 {line}</td></tr>'
        for line in data["icp_lines"]
    ) if data["icp_lines"] else '<tr><td style="padding:6px 0;color:#999;font-size:14px;">None</td></tr>'

    def stat_row(label, value, value_color="#1a1a1a"):
        return f"""
        <tr>
          <td style="padding:10px 0;border-bottom:1px solid #f5f5f5;color:#666;font-size:13px;width:220px;">{label}</td>
          <td style="padding:10px 0;border-bottom:1px solid #f5f5f5;color:{value_color};font-size:14px;font-weight:600;">{value}</td>
        </tr>"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0"></head>
<body style="margin:0;padding:0;background:#f4f4f4;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#f4f4f4;padding:32px 16px;">
    <tr><td align="center">
      <table width="560" cellpadding="0" cellspacing="0" style="max-width:560px;width:100%;">

        <!-- Header -->
        <tr><td style="background:#1b2e1b;border-radius:8px 8px 0 0;padding:24px 28px;">
          <p style="margin:0;color:#a3c4a3;font-size:11px;font-weight:700;letter-spacing:0.12em;text-transform:uppercase;">Call Capacity Dashboard</p>
          <h1 style="margin:6px 0 0;color:#ffffff;font-size:22px;font-weight:700;">EOD Stats — {date_str}</h1>
          <p style="margin:4px 0 0;color:#a3c4a3;font-size:12px;">{day_full}</p>
        </td></tr>

        <!-- Main stats -->
        <tr><td style="background:#ffffff;padding:24px 28px;">
          <p style="margin:0 0 14px;font-size:11px;font-weight:800;letter-spacing:0.12em;text-transform:uppercase;color:#1b5e1b;border-left:3px solid #1b5e1b;padding-left:8px;">TODAY'S NUMBERS</p>
          <table width="100%" cellpadding="0" cellspacing="0">
            {stat_row("💰 Revenue", rev_str, "#1b7a2e")}
            {stat_row("🤝 Deals Closed", str(data['deals']))}
            {stat_row("👤 Closers", closers_str)}
            {stat_row("📅 New Meetings Today", str(data['today_count']))}
            {stat_row("✅ Show Rate", f"{data['show_rate']:.0f}%")}
            {stat_row("📆 Meetings Set for Tomorrow", str(data['tomorrow_count']))}
          </table>
        </td></tr>

        <!-- Divider -->
        <tr><td style="background:#ffffff;padding:0 28px;"><hr style="border:none;border-top:1px solid #ececec;margin:0;"></td></tr>

        <!-- ICP block -->
        <tr><td style="background:#ffffff;padding:20px 28px 28px;border-radius:0 0 8px 8px;">
          <p style="margin:0 0 12px;font-size:11px;font-weight:800;letter-spacing:0.12em;text-transform:uppercase;color:#1b5e1b;border-left:3px solid #1b5e1b;padding-left:8px;">CLOSED WON — FUNNEL / ICP</p>
          <table width="100%" cellpadding="0" cellspacing="0">
            {icp_rows}
          </table>
        </td></tr>

        <!-- Footer -->
        <tr><td style="padding:16px 0 0;text-align:center;">
          <p style="margin:0;color:#aaa;font-size:11px;">Auto-generated by Call Capacity Dashboard · <a href="https://stephenolivas.github.io/call-capacity-dashboard/index.html" style="color:#1b7a2e;text-decoration:none;">View Dashboard →</a></p>
        </td></tr>

      </table>
    </td></tr>
  </table>
</body>
</html>"""

    return subject, plain, html


def send_eod_email(rolling_data, today, recipients=None):
    """
    Build and send the EOD email via Gmail SMTP.
    recipients: list of email addresses to send to (defaults to EMAIL_TO).
    All failures are caught and logged — they will never crash the dashboard run.
    """
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    recipients = recipients or EMAIL_TO

    if not GMAIL_APP_PASSWORD:
        log("⚠ EOD Email: GMAIL_APP_PASSWORD not set — skipping.")
        return
    if not recipients:
        log("⚠ EOD Email: No recipients — skipping.")
        return
    if not EMAIL_FROM:
        log("⚠ EOD Email: EMAIL_FROM not set — skipping.")
        return

    try:
        data                 = build_eod_data(rolling_data, today)
        subject, plain, html = format_eod_email(data)

        log(f"   Sending: '{subject}' → {recipients}")

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = f"EOD Reports <{EMAIL_FROM}>"
        msg["To"]      = ", ".join(recipients)
        msg.attach(MIMEText(plain, "plain"))
        msg.attach(MIMEText(html,  "html"))

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(EMAIL_FROM, GMAIL_APP_PASSWORD)
            smtp.sendmail(EMAIL_FROM, recipients, msg.as_string())

        log(f"✅ EOD email sent to {recipients}")

    except Exception as e:
        log(f"❌ EOD email error (dashboard unaffected): {e}")


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    global _api_call_count
    if not CLOSE_API_KEY:
        log("❌ Error: CLOSE_API_KEY environment variable is not set."); sys.exit(1)

    _api_call_count = 0; start_time = time.time()
    log("🚀 Starting Call Capacity Dashboard update (v14 — dual-lane + rep details)...")

    # Capture time NOW before the ~5 min API calls, so hour checks at the end
    # reflect when this run *started*, not when it finished.
    now_at_start = datetime.now(PACIFIC)
    today        = now_at_start.date()
    run_hour     = now_at_start.hour
    run_minute   = now_at_start.minute
    run_weekday  = today.weekday()
    log(f"📅 Today: {today} ({today.strftime('%A')}) · Run started: {now_at_start.strftime('%I:%M %p %Z')}")
    Path(ARCHIVE_DIR).mkdir(exist_ok=True)

    # ── Rolling dashboard (field-based, both lanes) ──
    log("\n═══ Rolling Dashboard ═══")
    rolling_start = today - timedelta(days=4)
    rolling_end = today + timedelta(days=10)
    rolling_dates = [rolling_start + timedelta(days=i) for i in range(14)]
    field_leads = fetch_field_leads(rolling_start, rolling_end)

    # Build lead_id → funnel map so fetch_rep_total_meetings can apply funnel restrictions
    # (e.g., Joe Dysert's overflow Internal Webinar calls only)
    # Build lookup maps for fetch_rep_total_meetings:
    #   lead_to_funnel    — funnel name per lead (for LANE_FUNNEL_RESTRICTIONS)
    #   leads_with_fscbd  — set of (lead_id, date) pairs where lead has First Sales Call Booked Date
    #                       set to that date. Used for the priority hierarchy in meeting classification:
    #                       a meeting whose lead has FSCBD == meeting date is "new" regardless of title.
    lead_to_funnel = {}
    leads_with_fscbd = set()
    for lead in field_leads:
        lid = lead.get("id")
        if not lid:
            continue
        lead_to_funnel[lid] = map_funnel(lead.get(FIELD_FUNNEL_NAME_DEAL) or "")
        fscbd_str = lead.get(FIELD_FIRST_SALES_CALL)
        if fscbd_str:
            try:
                fscbd_date = date.fromisoformat(fscbd_str)
                leads_with_fscbd.add((lid, fscbd_date))
            except (ValueError, TypeError):
                pass

    # Fetch total meetings per rep (includes follow-ups, reschedules, Q&A, etc.)
    # Used for the "Total Calls" reconciliation row in rep details AND the hero card
    # F/U / Resch / Other breakdown (priority-classified: new > fu > resch > other).
    rep_total_meetings, rep_meetings_by_category, non_new_meetings = fetch_rep_total_meetings(
        rolling_start, rolling_end, ALL_LANE_REPS, lead_to_funnel, leads_with_fscbd
    )

    log("\n── Team (single-team mode) ──")
    team_data = build_dashboard_data(field_leads, rolling_dates, today=today, lane_reps=ALL_LANE_REPS, lane_label="Team", rep_total_meetings=rep_total_meetings, rep_meetings_by_category=rep_meetings_by_category)

    # ── Non-new meeting details (F/U / Resch / Other panel section) ─────────────
    # For each non-new meeting captured by fetch_rep_total_meetings, resolve the
    # lead's display name + funnel for the day-detail panel. Leads already in
    # field_leads (FSCBD window) are free — only out-of-window leads need a fetch.
    # Expected fetch count: typically 30-80 leads in a 14-day window.
    #
    # NEW_CALLS_ONLY_REPS filter: clamped reps (Elvis, Kelly, Cameron, Jason, Lyle)
    # have their non-new meetings excluded from the hero tile's F/U / Resch / Other
    # counts — we apply the same filter here so the panel matches the tile exactly.
    # Skipping these meetings also avoids fetching their leads entirely.
    log("\n── Resolving non-new meeting leads ──")
    visible_non_new = [m for m in non_new_meetings if m["user_id"] not in NEW_CALLS_ONLY_REPS]
    log(f"   {len(non_new_meetings)} total non-new meetings · "
        f"{len(non_new_meetings) - len(visible_non_new)} hidden (clamped reps) · "
        f"{len(visible_non_new)} to display")

    field_lead_cache = {lead.get("id"): lead for lead in field_leads if lead.get("id")}
    unique_non_new_lead_ids = set(m["lead_id"] for m in visible_non_new if m.get("lead_id"))
    unresolved_lead_ids = [lid for lid in unique_non_new_lead_ids if lid not in field_lead_cache]
    log(f"   {len(unique_non_new_lead_ids)} unique non-new-meeting leads · {len(unresolved_lead_ids)} need fetch")

    lead_lookup = dict(field_lead_cache)  # start with what we already have
    fetch_fields = ",".join(["id", "display_name", "name", "status_id", FIELD_FUNNEL_NAME_DEAL, FIELD_LEAD_OWNER])
    for lid in unresolved_lead_ids:
        try:
            lead_lookup[lid] = close_get(f"lead/{lid}", {"_fields": fetch_fields})
        except Exception as e:
            log(f"   ⚠ Could not fetch lead {lid}: {e}")
            lead_lookup[lid] = None

    # Build structured per-day list for the panel — sorted F/U → Resch → Other within each day.
    _category_order = {"fu": 0, "resch": 1, "other": 2}
    _category_label = {"fu": "F/U", "resch": "Reschedule", "other": "Other"}
    non_new_meetings_by_date = {}
    for m in visible_non_new:
        d = m["meeting_date"]
        if d not in non_new_meetings_by_date:
            non_new_meetings_by_date[d] = []
        lead = lead_lookup.get(m["lead_id"])
        if lead:
            lead_name = lead.get("display_name") or lead.get("name") or "(unknown lead)"
            funnel    = lead.get(FIELD_FUNNEL_NAME_DEAL) or "(no funnel)"
        else:
            lead_name = "(unknown lead)"
            funnel    = "(no funnel)"
        owner_name = ALL_LANE_REP_NAMES.get(m["user_id"]) or f"User {m['user_id'][:8]}"
        non_new_meetings_by_date[d].append({
            "category":       m["category"],
            "category_label": _category_label[m["category"]],
            "lead_name":      lead_name,
            "lead_url":       f"https://app.close.com/lead/{m['lead_id']}/",
            "funnel_name":    funnel,
            "owner_name":     owner_name,
        })
    # Sort within each day: F/U → Resch → Other, then alphabetical by lead name
    for d in non_new_meetings_by_date:
        non_new_meetings_by_date[d].sort(key=lambda r: (_category_order[r["category"]], r["lead_name"].lower()))
    team_data["non_new_meetings_by_date"] = non_new_meetings_by_date
    # ────────────────────────────────────────────────────────────────────────────

    # ── LANE-MERGE TRANSITION (2026-06-17) ──────────────────────────────────────
    # The Booking Window Missed metric is naturally a Lane-1 concept — Lane 2 reps
    # self-source their meetings and don't consume team Calendly slots. The cached
    # snapshots in capacity_cache.json are also historically Lane-1-only. So during
    # the transition (and arguably long-term) we compute "missed" using Lane 1 booked
    # rather than combined booked, otherwise Lane 2's bookings inflate the subtraction
    # and the metric reads incorrectly (often 0 when it should be positive).
    #
    # To roll back: remove this block, remove `team_data["lane1_booked_by_date"]`,
    # and revert the two uses of lane1_booked further below (in the Calendly loop
    # and inside generate_lane_content) to the combined `booked` value.
    log("\n── Lane 1 booked (for Booking Window Missed metric) ──")
    _lane1_data_for_missed = build_dashboard_data(field_leads, rolling_dates, today=today, lane_reps=LANE_1_REPS, lane_label="Lane 1 (missed)")
    lane1_booked_by_date = {d: _lane1_data_for_missed["daily_data"][d]["booked"] for d in rolling_dates}
    team_data["lane1_booked_by_date"] = lane1_booked_by_date
    # ────────────────────────────────────────────────────────────────────────────

    # ── Calendly Capacity with Last-Snapshot Tracking ──
    # Future days: always update cache with latest Available + Booked snapshot.
    # Today: use cached value from last night (frozen pre-day snapshot).
    # Trailing days: use cached value (already frozen).
    log("\n═══ Calendly Available Slots ═══")
    max_cache = load_capacity_cache()  # {date: calendar_availability_snapshot}

    # Manual overrides for days before tracking was live
    if date(2026, 5, 11) not in max_cache or max_cache[date(2026, 5, 11)] != 18:
        max_cache[date(2026, 5, 11)] = 18   # Monday — observed count
    if date(2026, 5, 12) not in max_cache or max_cache[date(2026, 5, 12)] != 30:
        max_cache[date(2026, 5, 12)] = 30   # Tuesday — observed count
    if max_cache.get(date(2026, 5, 13)) != 34:
        max_cache[date(2026, 5, 13)] = 34   # Wednesday — adjusted for Dubem mid-day add

    forward_dates = [d for d in rolling_dates if d >= today]
    calendly_slots = fetch_calendly_available_slots(forward_dates)

    for d in rolling_dates:
        booked = team_data["daily_data"][d]["booked"]
        # LANE-MERGE TRANSITION: cache snapshot uses Lane 1 booked so the snapshot
        # represents Lane 1 calendar perspective (Lane 2 self-sources). To roll back,
        # delete this line and change `current_total = live_available + lane1_booked`
        # back to `current_total = live_available + booked`.
        lane1_booked = lane1_booked_by_date[d]

        if d in calendly_slots:
            live_available = calendly_slots[d]
            team_data["daily_data"][d]["calendly_available"] = live_available
            current_total = live_available + lane1_booked

            if d > today:
                # Future day: always update with latest snapshot (overwrites previous)
                max_cache[d] = current_total
                team_data["daily_data"][d]["max_calendar_availability"] = current_total
                log(f"   {d.strftime('%a %m/%d')}: {live_available} open, {booked} booked (L1: {lane1_booked}) → snapshot {current_total}")
            else:
                # Today: use cached value from last night (don't update)
                cached = max_cache.get(d)
                if cached:
                    team_data["daily_data"][d]["max_calendar_availability"] = cached
                    log(f"   {d.strftime('%a %m/%d')} (TODAY): {live_available} open, {booked} booked (L1: {lane1_booked}) → pre-day snapshot {cached}")
                else:
                    # No cache for today (first run ever) — use current as fallback
                    max_cache[d] = current_total
                    team_data["daily_data"][d]["max_calendar_availability"] = current_total
                    log(f"   {d.strftime('%a %m/%d')} (TODAY): {live_available} open, {booked} booked (L1: {lane1_booked}) → no cache, using {current_total}")
        elif d in max_cache:
            # Trailing day — use cached snapshot
            team_data["daily_data"][d]["calendly_available"] = 0
            team_data["daily_data"][d]["max_calendar_availability"] = max_cache[d]
            log(f"   {d.strftime('%a %m/%d')}: snapshot {max_cache[d]} (cached), {booked} booked")
        else:
            # No Calendly data at all — fall back to static
            team_data["daily_data"][d]["calendly_available"] = None
            team_data["daily_data"][d]["max_calendar_availability"] = None

    save_capacity_cache(max_cache)

    # Build all-reps data for EOD email (no lane filter — counts all sales calls)
    log("\n── All Reps (EOD email) ──")
    rolling_data = build_dashboard_data(field_leads, rolling_dates, today=today, lane_reps=None, lane_label="All Reps")

    # ── Day Detail Panel data ──
    log("\n═══ Day Detail Panel ═══")

    log("── Team meeting booking dates + calendar source ──")
    booking_dates, meeting_titles = fetch_meeting_booking_dates(team_data["valid_meetings"])
    team_detail = build_day_detail(team_data["valid_meetings"], booking_dates, ALL_LANE_REP_NAMES, meeting_titles=meeting_titles, non_new_meetings_by_date=non_new_meetings_by_date)

    html = generate_rolling_html(team_data, team_detail=team_detail)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f: f.write(html)
    log(f"✅ {OUTPUT_FILE} written ({len(team_data['valid_meetings'])} leads)")

    # ── Changelog ──
    changelog_html = generate_changelog_html()
    with open("changelog.html", "w", encoding="utf-8") as f: f.write(changelog_html)
    log("✅ changelog.html written")

    # ── Daily snapshot ──
    log("\n═══ Daily Snapshot ═══")
    sp = f"{ARCHIVE_DIR}/{today.isoformat()}.html"
    with open(sp, "w", encoding="utf-8") as f: f.write(html)
    log(f"✅ {sp} saved")

    # ── Weekly summary (Monday) ──
    if today.weekday() == 0:
        log("\n═══ Weekly Summary ═══")
        pm = today - timedelta(days=7); ps = today - timedelta(days=1)
        wd = [pm + timedelta(days=i) for i in range(7)]
        w_leads = fetch_field_leads(pm, ps + timedelta(days=1))
        wdata = build_dashboard_data(w_leads, wd, lane_reps=ALL_LANE_REPS, lane_label="Team")
        # Apply Calendly capacity from cache to archive
        for d in wd:
            if d in max_cache:
                wdata["daily_data"][d]["capacity"] = max_cache[d]
        wh = generate_weekly_html(wdata, pm)
        wp = f"{ARCHIVE_DIR}/week-{pm.isoformat()}.html"
        with open(wp, "w", encoding="utf-8") as f: f.write(wh)
        log(f"✅ {wp} saved ({len(wdata['valid_meetings'])} leads)")
    else:
        log(f"\n⏭ Weekly: skipped ({today.strftime('%A')})")

    # ── Monthly summary (1st) ──
    if today.day == 1:
        log("\n═══ Monthly Summary ═══")
        pme = today - timedelta(days=1); pms = pme.replace(day=1)
        nd = (pme - pms).days + 1; md = [pms + timedelta(days=i) for i in range(nd)]
        m_leads = fetch_field_leads(pms, today)
        mdata = build_dashboard_data(m_leads, md, lane_reps=ALL_LANE_REPS, lane_label="Team")
        # Apply Calendly capacity from cache to archive
        for d in md:
            if d in max_cache:
                mdata["daily_data"][d]["capacity"] = max_cache[d]
        mh = generate_monthly_html(mdata, pms)
        mp = f"{ARCHIVE_DIR}/month-{pms.strftime('%Y-%m')}.html"
        with open(mp, "w", encoding="utf-8") as f: f.write(mh)
        log(f"✅ {mp} saved ({len(mdata['valid_meetings'])} leads)")
    else:
        log(f"\n⏭ Monthly: skipped (day {today.day})")

    # ── Archive index ──
    log("\n═══ Archive Index ═══")
    ah = generate_archive_html(ARCHIVE_DIR)
    with open("archive.html", "w", encoding="utf-8") as f: f.write(ah)
    log("✅ archive.html regenerated")

    # ── EOD Email (8pm PT, M-F only — or forced via FORCE_EOD_EMAIL=true for testing) ──
    # run_minute < 15 ensures only the first run of the hour fires, not all 4.
    # run_weekday < 5 ensures M-F only (0=Mon ... 4=Fri, 5=Sat, 6=Sun).
    force_email = os.environ.get("FORCE_EOD_EMAIL", "").lower() == "true"
    if (run_hour == 20 and run_weekday < 5 and run_minute < 15) or force_email:
        send_eod_email(rolling_data, today, EMAIL_TO)

    # ── Friday 4pm PT — send to Joe only ──
    if run_hour == 16 and run_weekday == 4 and run_minute < 15:
        log("\n═══ Friday 4pm Email (Joe) ═══")
        send_eod_email(rolling_data, today, ["joedysert@modern-amenities.com"])

    elapsed = time.time() - start_time
    log(f"\n🏁 Done! API calls: {_api_call_count} | Time: {elapsed:.1f}s")


if __name__ == "__main__":
    main()
