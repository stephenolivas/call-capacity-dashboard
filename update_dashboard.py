#!/usr/bin/env python3
"""
Call Capacity Dashboard Generator (v13)

New:
- Funnel sections: External / In-House / Uncategorized
- Monthly goals per funnel, divided by working days
- All funnels always displayed (even with 0 counts)
- Combined "AK TikTok/Instagram" row (Tik Tok + Anthony IG)
- Count/Goal format (e.g. 7/10) or count-only for funnels without goals
- Archive system: daily snapshots + weekly summaries + monthly summaries
"""

import os
import sys
import json
import re
import time
import calendar
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from pathlib import Path
import requests

# ─── Configuration ───────────────────────────────────────────────────────────

CLOSE_API_KEY = os.environ.get("CLOSE_API_KEY", "")
CLOSE_API_BASE = "https://api.close.com/api/v1"
PACIFIC = ZoneInfo("America/Los_Angeles")

CAPACITY = {0: 57, 1: 60, 2: 60, 3: 60, 4: 60, 5: 4, 6: 0}

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
LEAD_FIELDS = ",".join(["id", "display_name", "name", "status_id", FIELD_FUNNEL_NAME_DEAL])

# Lead statuses excluded from capacity count (matches rep scorecard methodology)
EXCLUDED_LEAD_STATUS_IDS = {
    "stat_hWIGHjzyNpl4YjIFSFz3VK4fp2ny10SFJLKAihmo4KT",  # Canceled (by Lead)
    "stat_YV4ZngDB4IGjLjlOf0YTFEWuKZJ6fhNxVkzQkvKYfdB",  # Outside the US
}

OUTPUT_FILE = os.environ.get("OUTPUT_FILE", "index.html")
ARCHIVE_DIR = os.environ.get("ARCHIVE_DIR", "archive")
API_THROTTLE = 0.5

# ─── Funnel Configuration ───────────────────────────────────────────────────
# Each entry: display_name, close_values (list), monthly_goal (None = no goal), section
# close_values is a list of Close field values that map to this funnel row
# "AK TikTok/Instagram" combines two Close values into one row

FUNNEL_CONFIG = [
    # ── External ──
    {"name": "Low Ticket Funnel",       "close_values": ["Low Ticket Funnel"], "monthly_goal": 400, "section": "external"},
    {"name": "Instagram",               "close_values": ["Instagram"],         "monthly_goal": 240, "section": "external"},
    {"name": "YouTube",                 "close_values": ["YouTube - OG - Cam"],"monthly_goal": 80,  "section": "inhouse"},
    {"name": "X",                       "close_values": ["X"],                 "monthly_goal": 30,  "section": "external"},
    {"name": "Linkedin",                "close_values": ["Linkedin"],          "monthly_goal": 30,  "section": "external"},
    {"name": "Instagram Setter",        "close_values": ["Instagram Setter"],  "monthly_goal": None, "section": "external"},
    {"name": "Meta Ads",                "close_values": ["Meta Ads"],          "monthly_goal": None, "section": "inhouse"},
    # ── In-House ──
    {"name": "VSL",                     "close_values": ["VSL"],               "monthly_goal": 200, "section": "inhouse"},
    {"name": "Website",                 "close_values": ["Website"],           "monthly_goal": 100, "section": "inhouse"},
    {"name": "Internal Webinar",        "close_values": ["Internal Webinar"],  "monthly_goal": 70,  "section": "inhouse"},
    {"name": "Mike Newsletter",           "close_values": ["Mike Newsletter"],   "monthly_goal": 10,  "section": "inhouse"},
    {"name": "AK TikTok/Instagram",     "close_values": ["Tik Tok", "Anthony IG"], "monthly_goal": 5, "section": "inhouse"},
    {"name": "Side Hustle Nation/WWWS", "close_values": ["WWWS"],              "monthly_goal": 2,   "section": "inhouse"},
    {"name": "Passivepreneurs",         "close_values": ["Passivepreneurs"],   "monthly_goal": None, "section": "inhouse"},
    {"name": "Reactivation Email",      "close_values": ["Reactivation Email"],"monthly_goal": None, "section": "inhouse"},
    {"name": "Reactivation Scrapers",   "close_values": ["Reactivation Scrapers"],"monthly_goal": None, "section": "inhouse"},
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


def build_dashboard_data(meetings, lead_cache, dates, today=None):
    valid_meetings = []
    status_excluded = 0
    for m in meetings:
        lead_id = m.get("lead_id")
        lead_data = lead_cache.get(lead_id) if lead_id else None

        # Exclude leads with Canceled (by Lead) or Outside the US status
        if lead_data and lead_data.get("status_id") in EXCLUDED_LEAD_STATUS_IDS:
            status_excluded += 1
            continue

        raw_funnel = (lead_data.get(FIELD_FUNNEL_NAME_DEAL) if lead_data else None) or ""
        funnel = map_funnel(raw_funnel)
        valid_meetings.append({"date": m["_meeting_date"], "title": m.get("title", ""), "funnel": funnel, "lead_id": lead_id or ""})

    if status_excluded > 0:
        log(f"   ⚠ Excluded {status_excluded} meetings (lead status: Canceled/Outside US)")

    # Deduplicate by lead_id across the FULL window — most recent meeting wins.
    # This matches the rep scorecard methodology: one lead = one count, period.
    # Group by lead_id, keep the meeting with the latest date.
    lead_best = {}  # lead_id → meeting dict with the latest date
    no_lead_meetings = []  # meetings without a lead_id (keep all)
    for m in valid_meetings:
        lid = m["lead_id"]
        if not lid:
            no_lead_meetings.append(m)
            continue
        if lid not in lead_best or m["date"] > lead_best[lid]["date"]:
            lead_best[lid] = m

    deduped_meetings = list(lead_best.values()) + no_lead_meetings
    dupes_removed = len(valid_meetings) - len(deduped_meetings)

    if dupes_removed > 0:
        log(f"   ⚠ Deduped {dupes_removed} meetings (cross-window lead dedup, most recent kept)")

    daily_data = {}
    all_funnels_seen = set()
    for d in dates:
        daily_data[d] = {"booked": 0, "capacity": CAPACITY[d.weekday()], "funnels": {}}
    for m in deduped_meetings:
        d = m["date"]
        if d not in daily_data:
            continue
        daily_data[d]["booked"] += 1
        f = m["funnel"]
        all_funnels_seen.add(f)
        daily_data[d]["funnels"][f] = daily_data[d]["funnels"].get(f, 0) + 1

    return {
        "dates": dates,
        "daily_data": daily_data,
        "all_funnels_seen": all_funnels_seen,
        "valid_meetings": deduped_meetings,
        "today": today,
    }


def build_ltf_detail(closer_data, setter_meetings, lead_cache, dates):
    """
    Build LTF closer vs setter breakdown per day, plus no-funnel setter calls.
    closer_data: the main dashboard data (already built)
    setter_meetings: raw setter meetings (from classify_setter_meetings)
    lead_cache: shared lead cache
    dates: list of dates to cover
    """
    # Closer counts come from the existing dashboard data
    ltf_daily = {}
    for d in dates:
        closer_count = closer_data["daily_data"][d]["funnels"].get("Low Ticket Funnel", 0)
        ltf_daily[d] = {"closer": closer_count, "setter": 0, "total": closer_count, "no_funnel": 0}

    # Cross-window dedup for setter meetings — most recent wins (matches main pipeline)
    lead_best = {}  # lead_id → meeting with latest date
    for m in setter_meetings:
        lead_id = m.get("lead_id")
        if not lead_id:
            continue
        meeting_date = m.get("_meeting_date")
        if not meeting_date or meeting_date not in ltf_daily:
            continue

        # Exclude leads with bad statuses
        lead_data = lead_cache.get(lead_id)
        if lead_data and lead_data.get("status_id") in EXCLUDED_LEAD_STATUS_IDS:
            continue

        if lead_id not in lead_best or meeting_date > lead_best[lead_id]["_meeting_date"]:
            lead_best[lead_id] = m

    # Now categorize each deduped setter meeting
    setter_count_total = 0
    no_funnel_total = 0
    for lead_id, m in lead_best.items():
        meeting_date = m["_meeting_date"]
        lead_data = lead_cache.get(lead_id)
        raw_funnel = (lead_data.get(FIELD_FUNNEL_NAME_DEAL) if lead_data else None) or ""
        raw_funnel = raw_funnel.strip()

        if not raw_funnel:
            ltf_daily[meeting_date]["no_funnel"] += 1
            no_funnel_total += 1
        elif raw_funnel == "Low Ticket Funnel":
            ltf_daily[meeting_date]["setter"] += 1
            ltf_daily[meeting_date]["total"] += 1
            setter_count_total += 1

    log(f"   📊 LTF Detail: {setter_count_total} LTF setter calls, {no_funnel_total} no-funnel discovery calls")
    return ltf_daily


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
    """Build HTML rows for funnels in a given section."""
    daily = data["daily_data"]
    rows = ""

    # Get configured funnels for this section
    section_funnels = [fc for fc in FUNNEL_CONFIG if fc["section"] == section_filter]

    for fc in section_funnels:
        fname = fc["name"]
        dg = daily_goal_map.get(fname)
        cells = ""
        for d in dates:
            # Sum all close_values that map to this funnel
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
    """Build rows for No Attribution, Unknown, and any unmapped funnels."""
    daily = data["daily_data"]
    all_seen = data.get("all_funnels_seen", set())

    # Known configured funnel display names
    configured_names = set(fc["name"] for fc in FUNNEL_CONFIG)

    # Find unmapped funnels (seen in data but not in config)
    unmapped = set()
    for f in all_seen:
        if f not in configured_names and f not in UNCATEGORIZED_FUNNELS:
            unmapped.add(f)

    # Always show No Attribution and Unknown (Needs Review), plus any unmapped
    uncat_list = UNCATEGORIZED_FUNNELS + sorted(unmapped)

    rows = ""
    for fname in uncat_list:
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

def generate_rolling_html(data, exclude_other, counts, ltf_daily=None):
    dates = data["dates"]
    daily = data["daily_data"]
    today = data["today"]

    now_pacific = datetime.now(PACIFIC)  # noqa — must stay on this line
    last_updated = now_pacific.strftime("%I:%M %p %Z")
    last_updated_date = now_pacific.strftime("%A, %B %-d, %Y")

    # Calculate daily goals for current month
    year, month = now_pacific.year, now_pacific.month
    daily_goal_map = {}
    for fc in FUNNEL_CONFIG:
        dg = get_daily_goal(fc["monthly_goal"], year, month)
        daily_goal_map[fc["name"]] = dg

    n_cols = len(dates)

    def tc(d):
        if d == today: return " today"
        elif d < today: return " past"
        return ""

    date_headers = ""
    for d in dates:
        label = "► TODAY" if d == today else d.strftime("%a").upper()
        ds = d.strftime("%m/%d")
        date_headers += f'<th class="col-date{tc(d)}">{label}<br>{ds}</th>'

    # Capacity metrics
    cap_r = booked_r = avail_r = util_r = ""
    for d in dates:
        c = daily[d]["capacity"]; b = daily[d]["booked"]; t = tc(d)
        cap_r += f'<td class="num{t}">{c if c > 0 else "–"}</td>'
        booked_r += f'<td class="num {"booked" if b > 0 else "zero"}{t}">{b}</td>'
        avail_r += f'<td class="num{t}">{c - b if c > 0 else "–"}</td>'
        if c > 0:
            pct = b / c * 100
            util_r += f'<td class="num {util_class(pct)}{t}">{pct:.2f}%</td>'
        else:
            util_r += f'<td class="num{t}">N/A</td>'

    # Funnel section rows
    ext_rows = build_funnel_rows(data, dates, today, daily_goal_map, "external")
    inh_rows = build_funnel_rows(data, dates, today, daily_goal_map, "inhouse")
    unc_rows = build_uncategorized_rows(data, dates, today)

    # Total row
    total_cells = ""
    for d in dates:
        t = tc(d)
        total_cells += f'<td class="num total-num{t}">{daily[d]["booked"]}</td>'

    # Excluded titles
    excluded_rows = ""
    for item in exclude_other:
        te = item["title"].replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        excluded_rows += f'<tr><td class="label">{te}</td><td class="num">{item["date"]}</td></tr>\n'

    # LTF Detail section (collapsible)
    ltf_html = ""
    if ltf_daily:
        ltf_closer_r = ltf_setter_r = ltf_total_r = ltf_pct_r = ltf_nofunnel_r = ""
        window_closer = window_setter = window_nofunnel = 0
        for d in dates:
            t = tc(d)
            ld = ltf_daily.get(d, {"closer": 0, "setter": 0, "total": 0, "no_funnel": 0})
            c_count = ld["closer"]
            s_count = ld["setter"]
            t_count = ld["total"]
            nf_count = ld["no_funnel"]
            window_closer += c_count
            window_setter += s_count
            window_nofunnel += nf_count

            if c_count > 0:
                ltf_closer_r += f'<td class="num ltf-closer{t}">{c_count}</td>'
            else:
                ltf_closer_r += f'<td class="num zero{t}">0</td>'

            if s_count > 0:
                ltf_setter_r += f'<td class="num ltf-setter{t}">{s_count}</td>'
            else:
                ltf_setter_r += f'<td class="num zero{t}">0</td>'

            if t_count > 0:
                ltf_total_r += f'<td class="num ltf-total{t}">{t_count}</td>'
            else:
                ltf_total_r += f'<td class="num zero{t}">0</td>'

            if t_count > 0:
                c_pct = round(c_count / t_count * 100)
                s_pct = 100 - c_pct
                ltf_pct_r += f'<td class="num ltf-pct{t}">{c_pct}% / {s_pct}%</td>'
            else:
                ltf_pct_r += f'<td class="num zero{t}">–</td>'

            if nf_count > 0:
                nf_cls = f"num{t}"
                ltf_nofunnel_r += f'<td class="{nf_cls}" style="color:#b45309;font-weight:600;">{nf_count}</td>'
            else:
                ltf_nofunnel_r += f'<td class="num zero{t}">0</td>'

        ltf_window_total = window_closer + window_setter
        if ltf_window_total > 0:
            w_c_pct = round(window_closer / ltf_window_total * 100)
            w_s_pct = 100 - w_c_pct
            pct_str = f" · {w_c_pct}% / {w_s_pct}%"
        else:
            pct_str = ""
        ltf_summary = f"LTF Detail — Closer: {window_closer} · Setter: {window_setter} · No Funnel: {window_nofunnel}{pct_str}"
        ltf_html = f"""
  <div class="ltf-collapsible">
    <details>
      <summary>{ltf_summary}</summary>
      <div class="card" style="margin-top:0.5rem;">
        <table><colgroup><col style="width:170px"><col span="{n_cols}"></colgroup>
          <tbody>
            <tr><td class="metric">Closer Calls</td>{ltf_closer_r}</tr>
            <tr><td class="metric">Setter Calls</td>{ltf_setter_r}</tr>
            <tr class="total-row"><td class="metric">LTF Total</td>{ltf_total_r}</tr>
            <tr><td class="metric">Closer % / Setter %</td>{ltf_pct_r}</tr>
            <tr style="border-top:2px solid #e0e0e0;"><td class="metric" style="color:#b45309;"><a href="https://app.close.com/leads/save_0pf4Svd4OxrDacy9sUSqL0pr9W9243ouu9nA3mzcv4B/" target="_blank" style="color:#b45309;text-decoration:none;border-bottom:1px dashed #b45309;font-size:0.68rem;">Discovery w/ No Funnel ↗</a></td>{ltf_nofunnel_r}</tr>
          </tbody>
        </table>
      </div>
    </details>
  </div>"""

    wd = working_days_in_month(year, month)

    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Call Capacity Dashboard</title>
<style>{COMMON_CSS}</style>
</head><body>
{html_header_bar("Call Capacity Dashboard", f"3-Day Trailing + 10-Day Lookahead · First Meetings Only · {wd} working days in {now_pacific.strftime('%B')}", last_updated_date, "Last updated: " + last_updated)}
<div class="wrap">

  <div class="card">
    <div class="sec">CAPACITY METRICS</div>
    <table><colgroup><col style="width:170px"><col span="{n_cols}"></colgroup>
      <thead><tr><th></th>{date_headers}</tr></thead>
      <tbody>
        <tr><td class="metric">Capacity</td>{cap_r}</tr>
        <tr><td class="metric">Booked</td>{booked_r}</tr>
        <tr><td class="metric">Available</td>{avail_r}</tr>
        <tr><td class="metric">Utilization %</td>{util_r}</tr>
      </tbody>
    </table>
  </div>

  <div class="card">
    <table><colgroup><col style="width:170px"><col span="{n_cols}"></colgroup>
      <tbody>
        <tr class="section-label-row sec-ext"><td colspan="{n_cols + 1}">Funnel Breakdown — External</td></tr>
        {ext_rows}
        <tr class="section-label-row sec-inh"><td colspan="{n_cols + 1}">Funnel Breakdown — In-House</td></tr>
        {inh_rows}
        <tr class="section-label-row sec-unc"><td colspan="{n_cols + 1}">Funnel Breakdown — Uncategorized</td></tr>
        {unc_rows}
        <tr class="total-row"><td class="metric">TOTAL</td>{total_cells}</tr>
      </tbody>
    </table>
  </div>
  {ltf_html}

  <div class="excluded-section">
    <details>
      <summary>📋 Excluded Titles ({len(exclude_other)} meetings not classified as first calls)</summary>
      <div class="card exc-table">
        <table style="table-layout:auto"><thead><tr><th>Title</th><th>Date</th></tr></thead>
        <tbody>{excluded_rows if excluded_rows else '<tr><td class="label" colspan="2">None</td></tr>'}</tbody></table>
      </div>
    </details>
  </div>

  <div class="footer">
    <span>Classification: {counts['include']} included · {counts['exclude']} excluded · {counts['status_excluded']} canceled/declined · {counts['exclude_other']} unclassified · <a href="archive.html">📁 Archive</a></span>
    <a href="https://stephenolivas.github.io/mtd-funnel-reporting/" target="_blank">📊 MTD Funnel Reporting →</a>
  </div>
</div></body></html>"""


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
    <table><colgroup><col style="width:170px"><col span="{n_cols}"></colgroup>
    <thead><tr><th></th>{date_headers}</tr></thead>
    <tbody><tr><td class="metric">Capacity</td>{cap_r}</tr><tr><td class="metric">Booked</td>{booked_r}</tr><tr><td class="metric">Available</td>{avail_r}</tr><tr><td class="metric">Utilization %</td>{util_r}</tr></tbody></table>
  </div>
  <div class="card"><div class="sec">FUNNEL TOTALS</div>
    <table style="table-layout:auto; max-width:400px;"><thead><tr><th>Funnel</th><th>Total</th></tr></thead><tbody>{funnel_rows}</tbody></table>
  </div>
  <div class="footer"><a href="../archive.html">← Back to Archive</a><a href="https://stephenolivas.github.io/mtd-funnel-reporting/" target="_blank">📊 MTD Funnel Reporting →</a></div>
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
    <table><colgroup><col style="width:170px"><col span="8"></colgroup>
    <thead><tr><th>Week</th>{week_header}</tr></thead><tbody>{week_rows}</tbody></table>
  </div>
  <div class="card"><div class="sec">FUNNEL TOTALS</div>
    <table style="table-layout:auto; max-width:400px;"><thead><tr><th>Funnel</th><th>Total</th></tr></thead><tbody>{funnel_rows}</tbody></table>
  </div>
  <div class="footer"><a href="../archive.html">← Back to Archive</a><a href="https://stephenolivas.github.io/mtd-funnel-reporting/" target="_blank">📊 MTD Funnel Reporting →</a></div>
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
  <div class="footer"><span>Archive generated {now_pacific.strftime("%b %-d, %Y at %I:%M %p %Z")}</span><a href="https://stephenolivas.github.io/mtd-funnel-reporting/" target="_blank">📊 MTD Funnel Reporting →</a></div>
</div></body></html>"""


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
    "Instagram":                "IG",
    "YouTube":                  "YT",
    "X":                        "X",
    "Linkedin":                 "LinkedIn",
    "Instagram Setter":         "IG Setter",
    "Meta Ads":                 "Meta Ads",
    "VSL":                      "VSL",
    "Website":                  "Website",
    "Internal Webinar":         "Webinar",
    "Mike Newsletter":          "Newsletter",
    "AK TikTok/Instagram":      "AK TT/IG",
    "Side Hustle Nation/WWWS":  "SHN/WWWS",
    "Passivepreneurs":          "Passivepreneurs",
    "Reactivation Email":       "Reactivation Email",
    "Reactivation Scrapers":    "Reactivation Scrapers",
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
    """Assemble all data points needed for the EOD email."""

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
    shown = sum(
        1 for lid in today_lead_ids
        if email_leads.get(lid) and
           str(email_leads[lid].get(f"custom.{CF_SHOW_UP}", "")).lower() == "yes"
    )
    show_rate = (shown / today_count * 100) if today_count > 0 else 0.0

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
        f"Closers: {closers_str}\n"
        f"Todays new meetings: {data['today_count']}\n"
        f"Show rate: {data['show_rate']:.0f}%\n"
        f"New meetings set for tomorrow: {data['tomorrow_count']}\n\n"
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
    log("🚀 Starting Call Capacity Dashboard update (v13 — funnel goals + sections)...")

    # Capture time NOW before the ~5 min API calls, so hour checks at the end
    # reflect when this run *started*, not when it finished.
    now_at_start = datetime.now(PACIFIC)
    today        = now_at_start.date()
    run_hour     = now_at_start.hour
    run_minute   = now_at_start.minute
    run_weekday  = today.weekday()
    log(f"📅 Today: {today} ({today.strftime('%A')}) · Run started: {now_at_start.strftime('%I:%M %p %Z')}")
    Path(ARCHIVE_DIR).mkdir(exist_ok=True)

    all_meetings = fetch_all_meetings()

    # ── Rolling dashboard ──
    log("\n═══ Rolling Dashboard ═══")
    rolling_start = today - timedelta(days=3)
    rolling_end = today + timedelta(days=10)
    rolling_dates = [rolling_start + timedelta(days=i) for i in range(13)]
    included, exclude_other, counts = classify_meetings(all_meetings, rolling_start, rolling_end)
    log(f"   INCLUDE: {counts['include']} | EXCLUDE: {counts['exclude']} | STATUS_EXCLUDED: {counts['status_excluded']} | OTHER: {counts['exclude_other']}")
    lead_cache = fetch_leads_for_meetings(included)
    rolling_data = build_dashboard_data(included, lead_cache, rolling_dates, today=today)

    # ── LTF Detail (setter calls from Kristin/Spencer) ──
    log("\n═══ LTF Detail ═══")
    setter_meetings = classify_setter_meetings(all_meetings, rolling_start, rolling_end)
    log(f"   Setter meetings in window: {len(setter_meetings)}")
    # Fetch leads for setter meetings not already cached
    setter_new_ids = set(m["lead_id"] for m in setter_meetings if m.get("lead_id")) - set(lead_cache.keys())
    if setter_new_ids:
        log(f"   Fetching {len(setter_new_ids)} additional leads for setter data...")
        for lid in setter_new_ids:
            try:
                lead_cache[lid] = close_get(f"lead/{lid}", {"_fields": LEAD_FIELDS})
            except requests.HTTPError:
                lead_cache[lid] = None
    ltf_daily = build_ltf_detail(rolling_data, setter_meetings, lead_cache, rolling_dates)

    html = generate_rolling_html(rolling_data, exclude_other, counts, ltf_daily=ltf_daily)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f: f.write(html)
    log(f"✅ {OUTPUT_FILE} written ({len(rolling_data['valid_meetings'])} meetings)")

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
        wi, _, _ = classify_meetings(all_meetings, pm, ps + timedelta(days=1))
        new_ids = set(m["lead_id"] for m in wi if m.get("lead_id")) - set(lead_cache.keys())
        for lid in new_ids:
            try: lead_cache[lid] = close_get(f"lead/{lid}", {"_fields": LEAD_FIELDS})
            except: lead_cache[lid] = None
        wdata = build_dashboard_data(wi, lead_cache, wd)
        wh = generate_weekly_html(wdata, pm)
        wp = f"{ARCHIVE_DIR}/week-{pm.isoformat()}.html"
        with open(wp, "w", encoding="utf-8") as f: f.write(wh)
        log(f"✅ {wp} saved ({len(wdata['valid_meetings'])} meetings)")
    else:
        log(f"\n⏭ Weekly: skipped ({today.strftime('%A')})")

    # ── Monthly summary (1st) ──
    if today.day == 1:
        log("\n═══ Monthly Summary ═══")
        pme = today - timedelta(days=1); pms = pme.replace(day=1)
        nd = (pme - pms).days + 1; md = [pms + timedelta(days=i) for i in range(nd)]
        mi, _, _ = classify_meetings(all_meetings, pms, today)
        new_ids = set(m["lead_id"] for m in mi if m.get("lead_id")) - set(lead_cache.keys())
        for lid in new_ids:
            try: lead_cache[lid] = close_get(f"lead/{lid}", {"_fields": LEAD_FIELDS})
            except: lead_cache[lid] = None
        mdata = build_dashboard_data(mi, lead_cache, md)
        mh = generate_monthly_html(mdata, pms)
        mp = f"{ARCHIVE_DIR}/month-{pms.strftime('%Y-%m')}.html"
        with open(mp, "w", encoding="utf-8") as f: f.write(mh)
        log(f"✅ {mp} saved ({len(mdata['valid_meetings'])} meetings)")
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
