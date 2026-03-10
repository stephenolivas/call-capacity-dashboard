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

FIELD_FUNNEL_NAME_DEAL = "custom.cf_xqDQE8fkPsWa0RNEve7hcaxKblCe6489XeZGRDzyPdX"
LEAD_FIELDS = ",".join(["id", "display_name", "name", FIELD_FUNNEL_NAME_DEAL])

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
    {"name": "YouTube - OG - Cam",      "close_values": ["YouTube - OG - Cam"],"monthly_goal": 80,  "section": "external"},
    {"name": "X",                       "close_values": ["X"],                 "monthly_goal": 30,  "section": "external"},
    {"name": "Linkedin",                "close_values": ["Linkedin"],          "monthly_goal": 30,  "section": "external"},
    {"name": "Instagram Setter",        "close_values": ["Instagram Setter"],  "monthly_goal": None, "section": "external"},
    {"name": "Meta Ads",                "close_values": ["Meta Ads"],          "monthly_goal": None, "section": "external"},
    # ── In-House ──
    {"name": "VSL",                     "close_values": ["VSL"],               "monthly_goal": 200, "section": "inhouse"},
    {"name": "Website",                 "close_values": ["Website"],           "monthly_goal": 100, "section": "inhouse"},
    {"name": "Internal Webinar",        "close_values": ["Internal Webinar"],  "monthly_goal": 70,  "section": "inhouse"},
    {"name": "Newsletter",              "close_values": ["Newsletter"],        "monthly_goal": 10,  "section": "inhouse"},
    {"name": "AK TikTok/Instagram",     "close_values": ["Tik Tok", "Anthony IG"], "monthly_goal": 5, "section": "inhouse"},
    {"name": "Side Hustle Nation/WWWS", "close_values": ["WWWS"],              "monthly_goal": 2,   "section": "inhouse"},
    {"name": "Passivepreneurs",         "close_values": ["Passivepreneurs"],   "monthly_goal": None, "section": "inhouse"},
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
    r"|new\s+vendingpreneur\s+strategy\s+call",
    re.IGNORECASE
)
INCLUDE_AMBIGUOUS_RE = re.compile(r"vending\s+consult", re.IGNORECASE)
EXCLUDE_AMBIGUOUS_RE = re.compile(
    r"\benrollment\b|silver\s+start\s+up|bronze\s+enrollment|questions\s+on\s+enrollment",
    re.IGNORECASE
)

def classify_meeting_title(title):
    if not title:
        return "exclude_other"
    if title.strip().lower().startswith("canceled:"):
        return "exclude"
    if "vending quick discovery" in title.lower():
        return "exclude"
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
    counts = {"include": 0, "exclude": 0, "exclude_other": 0, "user_excluded": 0, "out_of_range": 0}
    included = []
    exclude_other_titles = []
    for m in all_meetings:
        meeting_date = parse_meeting_date_pacific(m)
        if not meeting_date or meeting_date < start_date or meeting_date >= end_date:
            counts["out_of_range"] += 1
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
    for m in meetings:
        lead_id = m.get("lead_id")
        lead_data = lead_cache.get(lead_id) if lead_id else None
        raw_funnel = (lead_data.get(FIELD_FUNNEL_NAME_DEAL) if lead_data else None) or ""
        funnel = map_funnel(raw_funnel)
        valid_meetings.append({"date": m["_meeting_date"], "title": m.get("title", ""), "funnel": funnel})

    daily_data = {}
    all_funnels_seen = set()
    for d in dates:
        daily_data[d] = {"booked": 0, "capacity": CAPACITY[d.weekday()], "funnels": {}}
    for m in valid_meetings:
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
        "valid_meetings": valid_meetings,
        "today": today,
    }


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

def generate_rolling_html(data, exclude_other, counts):
    dates = data["dates"]
    daily = data["daily_data"]
    today = data["today"]

    now_pacific = datetime.now(PACIFIC)
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
    <div class="sec">FUNNEL BREAKDOWN</div>
    <table><colgroup><col style="width:170px"><col span="{n_cols}"></colgroup>
      <thead><tr><th>Funnel</th>{date_headers}</tr></thead>
      <tbody>
        <tr class="section-label-row sec-ext"><td colspan="{n_cols + 1}">External</td></tr>
        {ext_rows}
        <tr class="section-label-row sec-inh"><td colspan="{n_cols + 1}">In-House</td></tr>
        {inh_rows}
        <tr class="section-label-row sec-unc"><td colspan="{n_cols + 1}">Uncategorized</td></tr>
        {unc_rows}
        <tr class="total-row"><td class="metric">TOTAL</td>{total_cells}</tr>
      </tbody>
    </table>
  </div>
  </div>

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
    <span>Classification: {counts['include']} included · {counts['exclude']} excluded · {counts['exclude_other']} unclassified · <a href="archive.html">📁 Archive</a></span>
    <a href="https://anthony-funnel-reporting-mtd.tiiny.site/" target="_blank">📊 MTD Funnel Reporting →</a>
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
  <div class="footer"><a href="../archive.html">← Back to Archive</a><a href="https://anthony-funnel-reporting-mtd.tiiny.site/" target="_blank">📊 MTD Funnel Reporting →</a></div>
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
  <div class="footer"><a href="../archive.html">← Back to Archive</a><a href="https://anthony-funnel-reporting-mtd.tiiny.site/" target="_blank">📊 MTD Funnel Reporting →</a></div>
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
  <div class="footer"><span>Archive generated {now_pacific.strftime("%b %-d, %Y at %I:%M %p %Z")}</span><a href="https://anthony-funnel-reporting-mtd.tiiny.site/" target="_blank">📊 MTD Funnel Reporting →</a></div>
</div></body></html>"""


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    global _api_call_count
    if not CLOSE_API_KEY:
        log("❌ Error: CLOSE_API_KEY environment variable is not set."); sys.exit(1)

    _api_call_count = 0; start_time = time.time()
    log("🚀 Starting Call Capacity Dashboard update (v13 — funnel goals + sections)...")

    today = datetime.now(PACIFIC).date()
    log(f"📅 Today: {today} ({today.strftime('%A')})")
    Path(ARCHIVE_DIR).mkdir(exist_ok=True)

    all_meetings = fetch_all_meetings()

    # ── Rolling dashboard ──
    log("\n═══ Rolling Dashboard ═══")
    rolling_start = today - timedelta(days=3)
    rolling_end = today + timedelta(days=10)
    rolling_dates = [rolling_start + timedelta(days=i) for i in range(13)]
    included, exclude_other, counts = classify_meetings(all_meetings, rolling_start, rolling_end)
    log(f"   INCLUDE: {counts['include']} | EXCLUDE: {counts['exclude']} | OTHER: {counts['exclude_other']}")
    lead_cache = fetch_leads_for_meetings(included)
    rolling_data = build_dashboard_data(included, lead_cache, rolling_dates, today=today)
    html = generate_rolling_html(rolling_data, exclude_other, counts)
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

    elapsed = time.time() - start_time
    log(f"\n🏁 Done! API calls: {_api_call_count} | Time: {elapsed:.1f}s")


if __name__ == "__main__":
    main()
