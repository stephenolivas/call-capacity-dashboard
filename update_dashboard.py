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
        # For today, use current UTC time as start (can't query past times)
        if d == today_pacific:
            # Use current UTC time + 1 min buffer to ensure "in the future"
            buffer_time = now_utc + timedelta(minutes=1)
            start = buffer_time.strftime("%Y-%m-%dT%H:%M:%SZ")
            is_today = True
        else:
            start = f"{d.isoformat()}T00:00:00Z"
            is_today = False
        end = f"{d.isoformat()}T23:59:59Z"

        # Try Consultation first (shorter window, primary calendar)
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

        if is_today:
            log(f"   {d.strftime('%a %m/%d')} (TODAY): Consultation={consult_count} (start={start}){' ERR: ' + consult_error if consult_error else ''}")

        if consult_count > 0:
            result[d] = consult_count
            if not is_today:
                log(f"   {d.strftime('%a %m/%d')}: {consult_count} slots (Consultation)")
        else:
            # Consultation returned 0 — try Accelerator (longer window)
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

            if is_today:
                log(f"   {d.strftime('%a %m/%d')} (TODAY): Accelerator={accel_count}{' ERR: ' + accel_error if accel_error else ''}")

            if accel_count > 0:
                result[d] = accel_count
                if not is_today:
                    log(f"   {d.strftime('%a %m/%d')}: {accel_count} slots (Accelerator)")
            else:
                # Both returned 0 — store 0 (no open slots, not "no data")
                result[d] = 0
                if not is_today:
                    log(f"   {d.strftime('%a %m/%d')}: 0 slots (no availability)")

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
}
LANE_1_REP_NAMES = {
    "user_7F059xEinVentOEvkRMP77fWZyvwUiTRTUOuhD11J0e": "Robin Perkins",
    "user_wF5aATmDljO6g6AHqehRPVmfCmH5j9VszbO6Q6Pjzm4": "Eric Piccione",
    "user_F0VeLnOQlWpkDncNW8rBl1V2QJ08fnDt6DcUjNATUJK": "Scott Seymour",
    "user_pKEujUcHJfsEyI5lM6L56aXM2s5nNOU994JRjRSlAdA": "Chris Wanke",
    "user_fYWHvOuCKDuaQxSp6lROlv2rmvZZYq1kzjGvaF7OrAL": "Jake Skinner",
    "user_wHm1vcLde4RExd3vv9UOjnms5Oz8ssXg8600mQuxMPb": "Christian Hartwell",
    "user_1xDZSeOa8omjfxHXD80twTf8OieXfQ6tNCaYbVygtv1": "Dubem Adindu",
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
    "user_ulI4pdlkBQGJpFBjSfdf3U2deAXQATVPSAurnbL80T9": "Bryan Barcus",
    "user_L0aaUNmM45X52HE7rj3VPWkxhahpoYobhDVAXamQMMD": "Steven Starnes",
    "user_Bov31jjnHhENBy8uWNTTL8KKax8VX7o6DugLzBYOHBG": "Lyle Hubbard",
    "user_WquWudQN7dghZsAPiNY80eJUmg1EadQg2UCQdvgbif7": "Kelly Schrader",
    "user_I0cHZ04mBXXBvbFcnwmsc2KrcMsLsKxqjW8DtJ783Hr": "Elvis Ellis",
    "user_5pAfnzGONQLUVLKqFQVpQ3570YV1gurVCTp1MMgfCDL": "John Kirk",
    "user_UpJb11fzX2TuFHf7fFyWpfXr84lg2Ui7i7p5CtQkIaW": "Cameron Caswell",
    "user_MrBLkl5wCqTm7QxHxPo2ydNV5KxMllg6YZDVc12Aqzj": "Jason Aaron",
    "user_pKEujUcHJfsEyI5lM6L56aXM2s5nNOU994JRjRSlAdA": "LTF Quiz Calendar - General",
}
LANE_2_LEAD = "user_MrBLkl5wCqTm7QxHxPo2ydNV5KxMllg6YZDVc12Aqzj"  # Jason Aaron

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
    {"date": "2026-05-18 2:00 PM PT", "notes": [
        "New row: Capacity Target — 42 meetings Mon-Fri, the daily count we need to hit revenue goals",
        "Renamed 'Calendar Capacity' to 'Capacity to Target %' — now measures Booked vs. Capacity Target (42) instead of Booked vs. Calendar Availability",
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
    fields = ",".join(["id", "display_name", "status_id", FIELD_FIRST_SALES_CALL, FIELD_FUNNEL_NAME_DEAL, FIELD_LEAD_OWNER])
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


def build_day_detail(valid_meetings, booking_dates, lane_rep_names, meeting_titles=None):
    """Build per-day detail data for the day detail panel.
    meeting_titles: {lead_id: str} — meeting title from Close for Calendar Source
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

    result = {}
    for ds, data in by_day.items():
        total = data["total"]
        if total == 0:
            continue

        # Funnels: top 4 + Other
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

        # Calendar source from meeting titles
        cal_source_sorted = data["cal_source"].most_common()
        cal_total = sum(c for _, c in cal_source_sorted)
        cal_source_list = [[name, count, round(count / cal_total * 100)] for name, count in cal_source_sorted] if cal_total > 0 else None

        result[ds] = {
            "total": total,
            "funnels": funnel_list,
            "reps": rep_list,
            "booked_on": booked_list,
            "calendar_source": cal_source_list,
        }

    return result


def build_dashboard_data(field_leads, dates, today=None, lane_reps=None, lane_label=""):
    """Build dashboard data from field-based lead query.
    field_leads: list of lead dicts from fetch_field_leads (or similar).
    lane_reps: set of user IDs to filter by (if None, no lane filter applied).
    lane_label: label for logging (e.g., "Lane 1", "Lane 2").
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

        daily_data[field_date]["booked"] += 1
        all_funnels_seen.add(funnel)
        daily_data[field_date]["funnels"][funnel] = daily_data[field_date]["funnels"].get(funnel, 0) + 1

        # Track per-rep
        if lane_reps and lead_owner in rep_data:
            rep_data[lead_owner][field_date][funnel] = rep_data[lead_owner][field_date].get(funnel, 0) + 1

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

    return {
        "dates": dates,
        "daily_data": daily_data,
        "all_funnels_seen": all_funnels_seen,
        "valid_meetings": valid_meetings,
        "today": today,
        "rep_data": rep_data,
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
.sec { font-size: 0.62rem; font-weight: 800; letter-spacing: 0.14em; text-transform: uppercase; color: #1b5e1b; padding: 0.55rem 0.6rem 0.3rem; border-left: 3px solid #1b5e1b; background: #f8faf8; }
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
.ltf-collapsible summary { font-size: 0.72rem; font-weight: 600; color: #1b3a5e; cursor: pointer; padding: 0.5rem 0.6rem; background: #f0f4ff; border: 1px solid #d4d4d4; border-radius: 4px; }
.ltf-collapsible summary:hover { background: #e4eaff; }
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


# ─── Funnel Row Builder ─────────────────────────────────────────────────────

def build_funnel_rows(data, dates, today, daily_goal_map, section_filter):
    """Build HTML rows for funnels in a given section. Only shows funnels with ≥1 call."""
    daily = data["daily_data"]
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

    # Capacity metrics (staging: Calendly-driven with max tracking)
    target_r = cal_avail_r = booked_r = open_r = missed_r = cap_pct_r = ""
    CAPACITY_TARGET = 42  # Daily target Mon-Fri to meet revenue goals
    for d in dates:
        b = daily[d]["booked"]
        cal_slots = daily[d].get("calendly_available")  # Live open slots from Calendly
        max_total = daily[d].get("max_calendar_availability")  # Max tracked total
        t = tc(d)
        is_weekday = d.weekday() < 5  # Mon-Fri = 0-4
        day_target = CAPACITY_TARGET if is_weekday else None

        if show_capacity:
            # Capacity Target = 42 on weekdays, – on weekends
            if day_target is not None:
                target_r += f'<td class="num{t}">{day_target}</td>'
            else:
                target_r += f'<td class="num{t}">–</td>'

            # Calendar Availability = max tracked total (stable number)
            if max_total is not None and max_total > 0:
                cal_avail_r += f'<td class="num{t}">{max_total}</td>'
            else:
                c = daily[d]["capacity"]
                max_total = c if c > 0 else 0
                cal_avail_r += f'<td class="num{t}">{c if c > 0 else "–"}</td>'

            # Booked
            booked_r += f'<td class="num {"booked" if b > 0 else "zero"}{t}">{b}</td>'

            # Open Availability = live Calendly slots
            if cal_slots is not None:
                open_r += f'<td class="num{t}">{cal_slots}</td>'
            elif daily[d]["capacity"] > 0:
                open_r += f'<td class="num{t}">{daily[d]["capacity"] - b}</td>'
            else:
                open_r += f'<td class="num{t}">–</td>'

            # Booking Window Missed = max_total - booked - open (only for today + past)
            if max_total and max_total > 0 and cal_slots is not None and d <= today:
                missed = max_total - b - cal_slots
                if missed > 0:
                    missed_r += f'<td class="num{t}" style="color:#c0392b;">{missed}</td>'
                else:
                    missed_r += f'<td class="num{t}">0</td>'
            else:
                missed_r += f'<td class="num{t}">–</td>'

            # Capacity to Target % = Booked / Capacity Target (no cap, weekends N/A)
            if day_target is not None:
                cap_pct = b / day_target * 100
                cap_pct_r += f'<td class="num {util_class(cap_pct)}{t}">{cap_pct:.1f}%</td>'
            else:
                cap_pct_r += f'<td class="num{t}">N/A</td>'
        else:
            target_r += f'<td class="num{t}">–</td>'
            cal_avail_r += f'<td class="num{t}">–</td>'
            booked_r += f'<td class="num {"booked" if b > 0 else "zero"}{t}">{b}</td>'
            open_r += f'<td class="num{t}">–</td>'
            missed_r += f'<td class="num{t}">–</td>'
            cap_pct_r += f'<td class="num{t}">N/A</td>'

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
    rep_rows = ""
    rep_summary_parts = []

    # Sort reps alphabetically, lead last
    sorted_uids = sorted(lane_rep_names.keys(), key=lambda uid: (uid == lane_lead, lane_rep_names[uid]))

    for uid in sorted_uids:
        rep_name = lane_rep_names.get(uid, uid)
        badge = ' <span style="background:#2563eb;color:#fff;font-size:0.6rem;padding:1px 6px;border-radius:3px;margin-left:4px;">Lead</span>' if uid == lane_lead else ""
        day_data = rep_data.get(uid, {})

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

    rep_summary = "Rep Details — " + " · ".join(rep_summary_parts) if rep_summary_parts else "Rep Details — No calls"

    # Build section HTML, only include sections with rows
    funnel_html = ""
    if ext_rows:
        funnel_html += f"""
    <div class="sec">FUNNEL BREAKDOWN — EXTERNAL</div>
    <table><colgroup><col style="width:200px"><col span="{n_cols}"></colgroup>
      <tbody>{ext_rows}</tbody>
    </table>"""
    if inh_rows:
        funnel_html += f"""
    <div class="sec">FUNNEL BREAKDOWN — IN-HOUSE</div>
    <table><colgroup><col style="width:200px"><col span="{n_cols}"></colgroup>
      <tbody>{inh_rows}</tbody>
    </table>"""
    if unc_rows:
        funnel_html += f"""
    <div class="sec">FUNNEL BREAKDOWN — UNCATEGORIZED</div>
    <table><colgroup><col style="width:200px"><col span="{n_cols}"></colgroup>
      <tbody>{unc_rows}</tbody>
    </table>"""

    return f"""
  <div class="card">
    <div class="sec">CAPACITY METRICS</div>
    <table><colgroup><col style="width:200px"><col span="{n_cols}"></colgroup>
      <thead><tr><th></th>{date_headers}</tr></thead>
      <tbody>
        <tr><td class="metric">Capacity Target</td>{target_r}</tr>
        <tr><td class="metric">Calendar Availability</td>{cal_avail_r}</tr>
        <tr><td class="metric">Booked</td>{booked_r}</tr>
        <tr><td class="metric">Open Availability</td>{open_r}</tr>
        <tr><td class="metric">Booking Window Missed</td>{missed_r}</tr>
        <tr><td class="metric">Capacity to Target %</td>{cap_pct_r}</tr>
      </tbody>
    </table>
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
          <tbody>
            {rep_rows}
          </tbody>
        </table>
      </div>
    </details>
  </div>"""


def generate_rolling_html(lane1_data, lane2_data, lane1_detail=None, lane2_detail=None):
    dates = lane1_data["dates"]
    today = lane1_data["today"]

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

    lane1_content = generate_lane_content(lane1_data, dates, today, daily_goal_map, n_cols, LANE_1_REP_NAMES, LANE_1_LEAD, show_capacity=True)
    lane2_content = generate_lane_content(lane2_data, dates, today, daily_goal_map, n_cols, LANE_2_REP_NAMES, LANE_2_LEAD, show_capacity=False)

    # Embed detail data as JSON for both lanes
    detail_json = json.dumps({"lane1": lane1_detail or {}, "lane2": lane2_detail or {}})

    toggle_css = """
    .lane-toggle { display:flex; gap:8px; margin-bottom:1rem; }
    .lane-btn { padding:10px 24px; font-size:0.95rem; font-weight:700; border:2px solid #1b7a2e;
                border-radius:6px; cursor:pointer; transition:all 0.15s; background:#fff; color:#1b7a2e; }
    .lane-btn.active { background:#1b7a2e; color:#fff; }
    .lane-btn:hover:not(.active) { background:#f0faf0; }

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
    """

    panel_js = """
    <script>
    var _activeLane = 1;
    var _dayDetail = """ + detail_json + """;

    function showLane(n) {
      _activeLane = n;
      document.getElementById('lane1').style.display = n===1 ? 'block' : 'none';
      document.getElementById('lane2').style.display = n===2 ? 'block' : 'none';
      document.getElementById('btn1').className = 'lane-btn' + (n===1 ? ' active' : '');
      document.getElementById('btn2').className = 'lane-btn' + (n===2 ? ' active' : '');
      closeDayPanel();
    }

    function showDayDetail(dateStr) {
      var laneKey = _activeLane === 1 ? 'lane1' : 'lane2';
      var detail = _dayDetail[laneKey][dateStr];
      if (!detail || detail.total === 0) return;

      var panel = document.getElementById('dayPanel');
      var overlay = document.getElementById('dayOverlay');

      // Title
      var d = new Date(dateStr + 'T12:00:00');
      var days = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
      var months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
      panel.querySelector('.dp-title').textContent = days[d.getDay()] + ', ' + months[d.getMonth()] + ' ' + d.getDate();
      panel.querySelector('.dp-subtitle').textContent = detail.total + ' calls · Lane ' + _activeLane;

      // Funnels
      var funnelHtml = '';
      var maxPct = Math.max.apply(null, detail.funnels.map(function(f){return f[2]}));
      detail.funnels.forEach(function(f) {
        var barW = maxPct > 0 ? (f[2] / maxPct * 100) : 0;
        funnelHtml += '<div class="dp-bar-row">' +
          '<span class="dp-bar-label">' + f[0] + '</span>' +
          '<div class="dp-bar-track"><div class="dp-bar-fill" style="width:' + barW + '%"></div></div>' +
          '<span class="dp-bar-val">' + f[1] + ' (' + f[2] + '%)</span></div>';
      });
      document.getElementById('dpFunnels').innerHTML = funnelHtml;

      // Reps
      var repHtml = '';
      if (detail.reps) {
        detail.reps.forEach(function(r) {
          repHtml += '<div class="dp-rep-row"><span class="dp-rep-name">' + r[0] + '</span><span class="dp-rep-count">' + r[1] + '</span></div>';
        });
      }
      document.getElementById('dpReps').innerHTML = repHtml;

      // Calendar Source
      var calHtml = '';
      if (detail.calendar_source && detail.calendar_source.length > 0) {
        calHtml = '<table class="dp-booked-table">';
        detail.calendar_source.forEach(function(cs) {
          calHtml += '<tr><td>' + cs[0] + '</td><td>' + cs[1] + '</td><td>' + cs[2] + '%</td></tr>';
        });
        calHtml += '</table>';
      } else {
        calHtml = '<div class="dp-coming-soon">No calendar data for this day</div>';
      }
      document.getElementById('dpCalSource').innerHTML = calHtml;

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

      panel.style.display = 'block';
      overlay.style.display = 'block';
    }

    function closeDayPanel() {
      document.getElementById('dayPanel').style.display = 'none';
      document.getElementById('dayOverlay').style.display = 'none';
    }
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

  <div class="lane-toggle">
    <button id="btn1" class="lane-btn active" onclick="showLane(1)">Lane 1</button>
    <button id="btn2" class="lane-btn" onclick="showLane(2)">Lane 2</button>
  </div>

  <div id="lane1">
  {lane1_content}
  </div>

  <div id="lane2" style="display:none;">
  {lane2_content}
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

  <div class="dp-section">Calendar Source</div>
  <div id="dpCalSource"></div>

  <div class="dp-section">When Booked</div>
  <div id="dpBooked"></div>
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


def build_eod_data(rolling_data, today, lane1_data=None, lane2_data=None):
    """Assemble all data points needed for the EOD email."""

    # Meeting counts come from rolling_data already in memory — zero extra API calls
    today_count    = rolling_data["daily_data"].get(today, {}).get("booked", 0)
    tomorrow       = today + timedelta(days=1)
    tomorrow_count = rolling_data["daily_data"].get(tomorrow, {}).get("booked", 0)

    # Per-lane meeting counts
    lane1_today = lane1_data["daily_data"].get(today, {}).get("booked", 0) if lane1_data else 0
    lane1_tomorrow = lane1_data["daily_data"].get(tomorrow, {}).get("booked", 0) if lane1_data else 0
    lane2_today = lane2_data["daily_data"].get(today, {}).get("booked", 0) if lane2_data else 0
    lane2_tomorrow = lane2_data["daily_data"].get(tomorrow, {}).get("booked", 0) if lane2_data else 0

    # Lead IDs from today's meetings (needed for show rate)
    today_lead_ids = list(set(
        m["lead_id"]
        for m in rolling_data["valid_meetings"]
        if m["date"] == today and m.get("lead_id")
    ))

    # Per-lane lead IDs for show rate
    lane1_lead_ids = list(set(
        m["lead_id"] for m in (lane1_data["valid_meetings"] if lane1_data else [])
        if m["date"] == today and m.get("lead_id")
    ))
    lane2_lead_ids = list(set(
        m["lead_id"] for m in (lane2_data["valid_meetings"] if lane2_data else [])
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

    # Per-lane show rates
    def calc_show_rate(lead_ids):
        s_ids = [lid for lid in lead_ids if email_leads.get(lid) and email_leads[lid].get("status_id") != RESCHEDULE_STATUS_ID]
        s_shown = sum(1 for lid in s_ids if str(email_leads[lid].get(f"custom.{CF_SHOW_UP}", "")).lower() == "yes")
        return (s_shown / len(s_ids) * 100) if s_ids else 0.0

    lane1_show_rate = calc_show_rate(lane1_lead_ids)
    lane2_show_rate = calc_show_rate(lane2_lead_ids)

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
        "lane1_today":    lane1_today,
        "lane1_tomorrow": lane1_tomorrow,
        "lane1_show_rate": lane1_show_rate,
        "lane2_today":    lane2_today,
        "lane2_tomorrow": lane2_tomorrow,
        "lane2_show_rate": lane2_show_rate,
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
        f"                    Lane 1    Lane 2\n"
        f"New Meetings Today:   {data['lane1_today']:<8}  {data['lane2_today']}\n"
        f"Show Rate:            {data['lane1_show_rate']:.0f}%{'':<6}{data['lane2_show_rate']:.0f}%\n"
        f"Meetings Tomorrow:    {data['lane1_tomorrow']:<8}  {data['lane2_tomorrow']}\n\n"
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
          </table>

          <!-- Lane 1 / Lane 2 Table -->
          <table width="100%" cellpadding="0" cellspacing="0" style="margin-top:16px;border-collapse:collapse;">
            <tr>
              <td style="width:40%;"></td>
              <td style="width:30%;text-align:center;background:#1b2e1b;color:#fff;font-size:11px;font-weight:800;letter-spacing:0.1em;padding:8px 12px;border-radius:4px 0 0 0;">Lane 1</td>
              <td style="width:30%;text-align:center;background:#1b2e1b;color:#fff;font-size:11px;font-weight:800;letter-spacing:0.1em;padding:8px 12px;border-radius:0 4px 0 0;">Lane 2</td>
            </tr>
            <tr>
              <td style="padding:10px 8px;font-size:13px;border-bottom:1px solid #f0f0f0;">📅 New Meetings Today</td>
              <td style="padding:10px 8px;text-align:center;font-size:15px;font-weight:700;border-bottom:1px solid #f0f0f0;">{data['lane1_today']}</td>
              <td style="padding:10px 8px;text-align:center;font-size:15px;font-weight:700;border-bottom:1px solid #f0f0f0;">{data['lane2_today']}</td>
            </tr>
            <tr>
              <td style="padding:10px 8px;font-size:13px;border-bottom:1px solid #f0f0f0;">✅ Show Rate</td>
              <td style="padding:10px 8px;text-align:center;font-size:15px;font-weight:700;border-bottom:1px solid #f0f0f0;">{data['lane1_show_rate']:.0f}%</td>
              <td style="padding:10px 8px;text-align:center;font-size:15px;font-weight:700;border-bottom:1px solid #f0f0f0;">{data['lane2_show_rate']:.0f}%</td>
            </tr>
            <tr>
              <td style="padding:10px 8px;font-size:13px;">📆 Meetings Set for Tomorrow</td>
              <td style="padding:10px 8px;text-align:center;font-size:15px;font-weight:700;">{data['lane1_tomorrow']}</td>
              <td style="padding:10px 8px;text-align:center;font-size:15px;font-weight:700;">{data['lane2_tomorrow']}</td>
            </tr>
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


def send_eod_email(rolling_data, today, recipients=None, lane1_data=None, lane2_data=None):
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
        data                 = build_eod_data(rolling_data, today, lane1_data=lane1_data, lane2_data=lane2_data)
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

    log("\n── Lane 1 ──")
    lane1_data = build_dashboard_data(field_leads, rolling_dates, today=today, lane_reps=LANE_1_REPS, lane_label="Lane 1")
    log("\n── Lane 2 ──")
    lane2_data = build_dashboard_data(field_leads, rolling_dates, today=today, lane_reps=LANE_2_REPS, lane_label="Lane 2")

    # ── Calendly Capacity with Last-Snapshot Tracking (Lane 1 only) ──
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
        booked = lane1_data["daily_data"][d]["booked"]

        if d in calendly_slots:
            live_available = calendly_slots[d]
            lane1_data["daily_data"][d]["calendly_available"] = live_available
            current_total = live_available + booked

            if d > today:
                # Future day: always update with latest snapshot (overwrites previous)
                max_cache[d] = current_total
                lane1_data["daily_data"][d]["max_calendar_availability"] = current_total
                log(f"   {d.strftime('%a %m/%d')}: {live_available} open, {booked} booked → snapshot {current_total}")
            else:
                # Today: use cached value from last night (don't update)
                cached = max_cache.get(d)
                if cached:
                    lane1_data["daily_data"][d]["max_calendar_availability"] = cached
                    log(f"   {d.strftime('%a %m/%d')} (TODAY): {live_available} open, {booked} booked → pre-day snapshot {cached}")
                else:
                    # No cache for today (first run ever) — use current as fallback
                    max_cache[d] = current_total
                    lane1_data["daily_data"][d]["max_calendar_availability"] = current_total
                    log(f"   {d.strftime('%a %m/%d')} (TODAY): {live_available} open, {booked} booked → no cache, using {current_total}")
        elif d in max_cache:
            # Trailing day — use cached snapshot
            lane1_data["daily_data"][d]["calendly_available"] = 0
            lane1_data["daily_data"][d]["max_calendar_availability"] = max_cache[d]
            log(f"   {d.strftime('%a %m/%d')}: snapshot {max_cache[d]} (cached), {booked} booked")
        else:
            # No Calendly data at all — fall back to static
            lane1_data["daily_data"][d]["calendly_available"] = None
            lane1_data["daily_data"][d]["max_calendar_availability"] = None

    save_capacity_cache(max_cache)

    # Build all-reps data for EOD email (no lane filter — counts all sales calls)
    log("\n── All Reps (EOD email) ──")
    rolling_data = build_dashboard_data(field_leads, rolling_dates, today=today, lane_reps=None, lane_label="All Reps")

    # ── Day Detail Panel data ──
    log("\n═══ Day Detail Panel ═══")

    log("── Lane 1 meeting booking dates + calendar source ──")
    l1_booking, l1_titles = fetch_meeting_booking_dates(lane1_data["valid_meetings"])
    l1_detail = build_day_detail(lane1_data["valid_meetings"], l1_booking, LANE_1_REP_NAMES, meeting_titles=l1_titles)
    log("── Lane 2 meeting booking dates + calendar source ──")
    l2_booking, l2_titles = fetch_meeting_booking_dates(lane2_data["valid_meetings"])
    l2_detail = build_day_detail(lane2_data["valid_meetings"], l2_booking, LANE_2_REP_NAMES, meeting_titles=l2_titles)

    html = generate_rolling_html(lane1_data, lane2_data, lane1_detail=l1_detail, lane2_detail=l2_detail)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f: f.write(html)
    log(f"✅ {OUTPUT_FILE} written (L1: {len(lane1_data['valid_meetings'])} · L2: {len(lane2_data['valid_meetings'])} leads)")

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
        wdata = build_dashboard_data(w_leads, wd, lane_reps=LANE_1_REPS, lane_label="Lane 1")
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
        mdata = build_dashboard_data(m_leads, md, lane_reps=LANE_1_REPS, lane_label="Lane 1")
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
        send_eod_email(rolling_data, today, EMAIL_TO, lane1_data=lane1_data, lane2_data=lane2_data)

    # ── Friday 4pm PT — send to Joe only ──
    if run_hour == 16 and run_weekday == 4 and run_minute < 15:
        log("\n═══ Friday 4pm Email (Joe) ═══")
        send_eod_email(rolling_data, today, ["joedysert@modern-amenities.com"], lane1_data=lane1_data, lane2_data=lane2_data)

    elapsed = time.time() - start_time
    log(f"\n🏁 Done! API calls: {_api_call_count} | Time: {elapsed:.1f}s")


if __name__ == "__main__":
    main()
