#!/usr/bin/env python3
"""
Call Capacity Dashboard Generator (v12)

Features:
- Rolling 13-day dashboard (3 trailing + today + 9 forward)
- Daily snapshots saved to archive/
- Weekly summaries generated on Mondays (previous Mon-Sun)
- Monthly summaries generated on 1st (previous full month)
- Auto-generated archive.html index page

File outputs:
  index.html                         ← live rolling dashboard
  archive/2026-03-03.html            ← daily snapshot (rolling view)
  archive/week-2026-03-03.html       ← weekly summary (named by Monday)
  archive/month-2026-03.html         ← monthly summary
  archive.html                       ← index of all archives
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
    "user_EmhqCmaHERTfgfWnPADiLGEqQw3ENvRYd3u1VEmblIp",  # Kristin Nelson
    "user_4sfuKGMbv0LQZ4hpS8ipASv406kKTSNP5Xx79jOwSqM",  # Spencer Reynolds
    "user_5cZRqXu8kb4O1IeBVA98UMcMEhYZUhx1fnCHfSL0YMV",  # Stephen Olivas
    "user_yRF070m26JE67J6CJqzkAB3IqY7btNm1K5RisCglKa6",  # Ahmad Bukhari
}

FIELD_FUNNEL_NAME_DEAL = "custom.cf_xqDQE8fkPsWa0RNEve7hcaxKblCe6489XeZGRDzyPdX"
LEAD_FIELDS = ",".join(["id", "display_name", "name", FIELD_FUNNEL_NAME_DEAL])

OUTPUT_FILE = os.environ.get("OUTPUT_FILE", "index.html")
ARCHIVE_DIR = os.environ.get("ARCHIVE_DIR", "archive")
API_THROTTLE = 0.5

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
    """Fetch ALL meetings from Close (single paginated scan)."""
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
    """Filter meetings to date range and classify titles."""
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
    """Fetch lead data for unique leads in meeting list."""
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


def build_dashboard_data(meetings, lead_cache, dates, today=None):
    """Build structured data for HTML generation."""
    valid_meetings = []
    for m in meetings:
        lead_id = m.get("lead_id")
        lead_data = lead_cache.get(lead_id) if lead_id else None
        funnel = (lead_data.get(FIELD_FUNNEL_NAME_DEAL) if lead_data else None) or "Unknown (Needs Review)"
        valid_meetings.append({
            "date": m["_meeting_date"],
            "title": m.get("title", ""),
            "funnel": funnel,
        })

    daily_data = {}
    funnel_set = set()
    for d in dates:
        daily_data[d] = {"booked": 0, "capacity": CAPACITY[d.weekday()], "funnels": {}}
    for m in valid_meetings:
        d = m["date"]
        if d not in daily_data:
            continue
        daily_data[d]["booked"] += 1
        f = m["funnel"]
        funnel_set.add(f)
        daily_data[d]["funnels"][f] = daily_data[d]["funnels"].get(f, 0) + 1

    return {
        "dates": dates,
        "daily_data": daily_data,
        "funnels": sorted(funnel_set),
        "valid_meetings": valid_meetings,
        "today": today,
    }


# ─── Shared HTML Pieces ─────────────────────────────────────────────────────

COMMON_CSS = """
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600;700&display=swap');
* { margin:0; padding:0; box-sizing:border-box; }
body {
  font-family: 'Inter', -apple-system, system-ui, sans-serif;
  background: #ffffff; color: #1a1a1a;
}
.header {
  background: #1b2e1b; color: #fff;
  padding: 0.8rem 1.5rem;
  display: flex; justify-content: space-between; align-items: center;
}
.header h1 { font-size: 1.15rem; font-weight: 700; }
.header .sub { font-size: 0.68rem; color: #a3c4a3; margin-top: 2px; }
.header .right { text-align: right; font-family: 'JetBrains Mono', monospace; }
.header .right .date { font-size: 0.78rem; font-weight: 600; }
.header .right .time { font-size: 0.65rem; color: #a3c4a3; }
.dot { display: inline-block; width: 7px; height: 7px; background: #4ade80; border-radius: 50%; margin-right: 5px; }
.wrap { padding: 1rem 1.5rem 2rem; max-width: 1500px; margin: 0 auto; }
.sec {
  font-size: 0.62rem; font-weight: 800; letter-spacing: 0.14em;
  text-transform: uppercase; color: #1b5e1b;
  padding: 0.55rem 0.6rem 0.3rem;
  border-left: 3px solid #1b5e1b; background: #f8faf8;
}
.card {
  border: 1px solid #d4d4d4; border-radius: 4px;
  overflow-x: auto; margin-bottom: 1rem; background: #fff;
}
table { width: 100%; border-collapse: collapse; table-layout: fixed; }
th {
  padding: 0.5rem 0.6rem; font-size: 0.68rem; font-weight: 700;
  text-align: center; color: #555; border-bottom: 2px solid #d4d4d4;
  white-space: nowrap; background: #fafafa; line-height: 1.4;
}
th:first-child { text-align: left; padding-left: 0.6rem; }
td {
  padding: 0.35rem 0.6rem; border-bottom: 1px solid #ececec; font-size: 0.78rem;
}
td.num {
  text-align: center; font-family: 'JetBrains Mono', monospace;
  font-size: 0.76rem; font-weight: 500; color: #333;
}
td.label { font-weight: 500; font-size: 0.76rem; padding-left: 0.6rem; color: #1a1a1a; }
.metric { font-weight: 600; font-size: 0.78rem; padding-left: 0.6rem; color: #1a1a1a; }
th.col-date.today { background: #fdf3e0 !important; color: #b45309; }
td.today { background: #fdf8ee !important; }
th.col-date.past { background: #f5f5f5 !important; color: #999; }
td.past { background: #fafafa !important; color: #888 !important; }
.booked { color: #1b7a2e; font-weight: 700; }
.zero { color: #aaa; }
.total-num { font-weight: 700; color: #1a1a1a; }
.util-low { color: #16a34a; font-weight: 700; }
.util-mid { color: #b45309; font-weight: 700; }
.util-high { color: #dc2626; font-weight: 700; }
tr.total-row td { border-top: 2px solid #bbb; }
.footer {
  margin-top: 1.5rem; padding-top: 0.75rem; border-top: 1px solid #e0e0e0;
  font-size: 0.72rem; color: #888;
  display: flex; justify-content: space-between; align-items: center;
}
.footer a { color: #1b7a2e; text-decoration: none; font-weight: 600; }
.footer a:hover { text-decoration: underline; }
.excluded-section { margin-top: 0.5rem; }
.excluded-section summary { font-size: 0.72rem; font-weight: 600; color: #666; cursor: pointer; padding: 0.4rem 0; }
.excluded-section .exc-table { margin-top: 0.3rem; }
.excluded-section .exc-table td { font-size: 0.7rem; color: #666; padding: 0.2rem 0.6rem; }
.summary-cards {
  display: grid; grid-template-columns: repeat(3, 1fr); gap: 1rem; margin-bottom: 1.25rem;
}
.s-card {
  background: #f8faf8; border: 1px solid #d4d4d4; border-radius: 6px; padding: 1rem;
}
.s-card .s-label { font-size: 0.65rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.1em; color: #666; margin-bottom: 0.3rem; }
.s-card .s-value { font-family: 'JetBrains Mono', monospace; font-size: 1.5rem; font-weight: 700; }
.s-card .s-value.green { color: #1b7a2e; }
@media (max-width: 900px) {
  .header { padding: 0.6rem 0.75rem; }
  .wrap { padding: 0.5rem 0.75rem; }
  .summary-cards { grid-template-columns: 1fr; }
}
"""


def html_header_bar(title, subtitle, date_str, time_str):
    return f"""<div class="header">
  <div>
    <h1>📞 {title}</h1>
    <div class="sub">{subtitle}</div>
  </div>
  <div class="right">
    <div class="date"><span class="dot"></span>{date_str}</div>
    <div class="time">{time_str}</div>
  </div>
</div>"""


def util_class(pct):
    if pct >= 80:
        return "util-high"
    if pct >= 40:
        return "util-mid"
    return "util-low"


# ─── Rolling Dashboard HTML ─────────────────────────────────────────────────

def generate_rolling_html(data, exclude_other, counts):
    dates = data["dates"]
    daily = data["daily_data"]
    funnels = data["funnels"]
    today = data["today"]

    now_pacific = datetime.now(PACIFIC)
    last_updated = now_pacific.strftime("%I:%M %p %Z")
    last_updated_date = now_pacific.strftime("%A, %B %-d, %Y")

    n_cols = len(dates)

    def tc(d):
        if d == today:
            return " today"
        elif d < today:
            return " past"
        return ""

    date_headers = ""
    for d in dates:
        label = "► TODAY" if d == today else d.strftime("%a").upper()
        ds = d.strftime("%m/%d")
        date_headers += f'<th class="col-date{tc(d)}">{label}<br>{ds}</th>'

    cap_r = booked_r = avail_r = util_r = ""
    for d in dates:
        c = daily[d]["capacity"]
        b = daily[d]["booked"]
        t = tc(d)
        cap_r += f'<td class="num{t}">{c if c > 0 else "–"}</td>'
        booked_r += f'<td class="num {"booked" if b > 0 else "zero"}{t}">{b}</td>'
        avail_r += f'<td class="num{t}">{c - b if c > 0 else "–"}</td>'
        if c > 0:
            pct = b / c * 100
            util_r += f'<td class="num {util_class(pct)}{t}">{pct:.2f}%</td>'
        else:
            util_r += f'<td class="num{t}">N/A</td>'

    funnel_html = ""
    for fn in funnels:
        cells = ""
        for d in dates:
            cnt = daily[d]["funnels"].get(fn, 0)
            t = tc(d)
            cells += f'<td class="num {"booked" if cnt > 0 else "zero"}{t}">{cnt}</td>'
        funnel_html += f'<tr><td class="label">{fn}</td>{cells}</tr>\n'

    total_cells = ""
    for d in dates:
        t = tc(d)
        total_cells += f'<td class="num total-num{t}">{daily[d]["booked"]}</td>'

    excluded_rows = ""
    for item in exclude_other:
        te = item["title"].replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        excluded_rows += f'<tr><td class="label">{te}</td><td class="num">{item["date"]}</td></tr>\n'

    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Call Capacity Dashboard</title>
<style>{COMMON_CSS}</style>
</head><body>
{html_header_bar("Call Capacity Dashboard", "3-Day Trailing + 10-Day Lookahead · First Meetings Only", last_updated_date, "Last updated: " + last_updated)}
<div class="wrap">
  <div class="card">
    <div class="sec">CAPACITY METRICS</div>
    <table>
      <colgroup><col style="width:170px"><col span="{n_cols}"></colgroup>
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
    <table>
      <colgroup><col style="width:170px"><col span="{n_cols}"></colgroup>
      <thead><tr><th>Funnel</th>{date_headers}</tr></thead>
      <tbody>
        {funnel_html}
        <tr class="total-row"><td class="metric">TOTAL</td>{total_cells}</tr>
      </tbody>
    </table>
  </div>
  <div class="excluded-section">
    <details>
      <summary>📋 Excluded Titles ({len(exclude_other)} meetings not classified as first calls)</summary>
      <div class="card exc-table">
        <table style="table-layout:auto">
          <thead><tr><th>Title</th><th>Date</th></tr></thead>
          <tbody>{excluded_rows if excluded_rows else '<tr><td class="label" colspan="2">None</td></tr>'}</tbody>
        </table>
      </div>
    </details>
  </div>
  <div class="footer">
    <span>Classification: {counts['include']} included · {counts['exclude']} excluded · {counts['exclude_other']} unclassified ·
    <a href="archive.html">📁 Archive</a></span>
    <a href="https://anthony-funnel-reporting-mtd.tiiny.site/" target="_blank">📊 MTD Funnel Reporting →</a>
  </div>
</div></body></html>"""


# ─── Weekly Summary HTML ─────────────────────────────────────────────────────

def generate_weekly_html(data, week_start):
    dates = data["dates"]
    daily = data["daily_data"]
    funnels = data["funnels"]
    week_end = week_start + timedelta(days=6)

    total_booked = sum(daily[d]["booked"] for d in dates)
    total_cap = sum(daily[d]["capacity"] for d in dates)
    avg_util = (total_booked / total_cap * 100) if total_cap > 0 else 0

    date_headers = ""
    for d in dates:
        date_headers += f'<th class="col-date">{d.strftime("%a").upper()}<br>{d.strftime("%m/%d")}</th>'
    date_headers += '<th class="col-date" style="background:#f0f0f0;">TOTAL</th>'

    n_cols = len(dates) + 1

    cap_r = booked_r = avail_r = util_r = ""
    tc = 0
    tb = 0
    for d in dates:
        c = daily[d]["capacity"]
        b = daily[d]["booked"]
        tc += c
        tb += b
        cap_r += f'<td class="num">{c if c > 0 else "–"}</td>'
        booked_r += f'<td class="num {"booked" if b > 0 else "zero"}">{b}</td>'
        avail_r += f'<td class="num">{c - b if c > 0 else "–"}</td>'
        if c > 0:
            pct = b / c * 100
            util_r += f'<td class="num {util_class(pct)}">{pct:.1f}%</td>'
        else:
            util_r += f'<td class="num">N/A</td>'

    overall_util = (tb / tc * 100) if tc > 0 else 0
    cap_r += f'<td class="num total-num">{tc}</td>'
    booked_r += f'<td class="num total-num booked">{tb}</td>'
    avail_r += f'<td class="num total-num">{tc - tb}</td>'
    util_r += f'<td class="num total-num {util_class(overall_util)}">{overall_util:.1f}%</td>'

    funnel_html = ""
    for fn in funnels:
        cells = ""
        ft = 0
        for d in dates:
            cnt = daily[d]["funnels"].get(fn, 0)
            ft += cnt
            cells += f'<td class="num {"booked" if cnt > 0 else "zero"}">{cnt}</td>'
        cells += f'<td class="num total-num">{ft}</td>'
        funnel_html += f'<tr><td class="label">{fn}</td>{cells}</tr>\n'

    total_cells = ""
    gt = 0
    for d in dates:
        t = daily[d]["booked"]
        gt += t
        total_cells += f'<td class="num total-num">{t}</td>'
    total_cells += f'<td class="num total-num">{gt}</td>'

    title = f"Weekly Summary — {week_start.strftime('%b %-d')} to {week_end.strftime('%b %-d, %Y')}"
    subtitle = "Monday through Sunday · First Meetings Only"

    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{title}</title>
<style>{COMMON_CSS}</style>
</head><body>
{html_header_bar(title, subtitle, week_start.strftime("%B %-d, %Y"), "Generated: " + datetime.now(PACIFIC).strftime("%b %-d at %I:%M %p %Z"))}
<div class="wrap">
  <div class="summary-cards">
    <div class="s-card"><div class="s-label">Total Booked</div><div class="s-value green">{total_booked}</div></div>
    <div class="s-card"><div class="s-label">Total Capacity</div><div class="s-value">{total_cap}</div></div>
    <div class="s-card"><div class="s-label">Avg Utilization</div><div class="s-value {util_class(avg_util)}">{avg_util:.1f}%</div></div>
  </div>
  <div class="card">
    <div class="sec">DAILY BREAKDOWN</div>
    <table>
      <colgroup><col style="width:170px"><col span="{n_cols}"></colgroup>
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
    <table>
      <colgroup><col style="width:170px"><col span="{n_cols}"></colgroup>
      <thead><tr><th>Funnel</th>{date_headers}</tr></thead>
      <tbody>
        {funnel_html}
        <tr class="total-row"><td class="metric">TOTAL</td>{total_cells}</tr>
      </tbody>
    </table>
  </div>
  <div class="footer">
    <a href="../archive.html">← Back to Archive</a>
    <a href="https://anthony-funnel-reporting-mtd.tiiny.site/" target="_blank">📊 MTD Funnel Reporting →</a>
  </div>
</div></body></html>"""


# ─── Monthly Summary HTML ────────────────────────────────────────────────────

def generate_monthly_html(data, month_date):
    """Generate a monthly summary with week-by-week rows."""
    dates = data["dates"]
    daily = data["daily_data"]
    funnels = data["funnels"]

    total_booked = sum(daily[d]["booked"] for d in dates)
    total_cap = sum(daily[d]["capacity"] for d in dates)
    avg_util = (total_booked / total_cap * 100) if total_cap > 0 else 0

    # Group dates into weeks (Mon-Sun)
    weeks = []
    current_week = []
    for d in dates:
        current_week.append(d)
        if d.weekday() == 6 or d == dates[-1]:
            weeks.append(current_week)
            current_week = []

    # Week-by-week table: columns = Mon Tue Wed Thu Fri Sat Sun Total
    day_names = ["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"]
    week_header = "".join(f'<th class="col-date">{dn}</th>' for dn in day_names)
    week_header += '<th class="col-date" style="background:#f0f0f0;">TOTAL</th>'

    week_rows = ""
    grand_total = 0
    dow_totals = [0] * 7

    for week in weeks:
        week_label = f"{week[0].strftime('%m/%d')}–{week[-1].strftime('%m/%d')}"
        # Build a 7-slot array (Mon=0 ... Sun=6)
        slots = [None] * 7
        for d in week:
            slots[d.weekday()] = d

        cells = ""
        week_total = 0
        for i in range(7):
            if slots[i] and slots[i] in daily:
                b = daily[slots[i]]["booked"]
                week_total += b
                dow_totals[i] += b
                cells += f'<td class="num {"booked" if b > 0 else "zero"}">{b}</td>'
            else:
                cells += '<td class="num" style="color:#ccc;">–</td>'

        grand_total += week_total
        week_rows += f'<tr><td class="label">Week of {week_label}</td>{cells}<td class="num total-num">{week_total}</td></tr>\n'

    # DOW totals row
    dow_cells = "".join(f'<td class="num total-num">{t}</td>' for t in dow_totals)
    dow_cells += f'<td class="num total-num">{grand_total}</td>'
    week_rows += f'<tr class="total-row"><td class="metric">TOTAL</td>{dow_cells}</tr>\n'

    # Funnel summary (single total column)
    funnel_totals = {}
    for m in data["valid_meetings"]:
        f = m["funnel"]
        funnel_totals[f] = funnel_totals.get(f, 0) + 1

    funnel_rows = ""
    for fn in sorted(funnel_totals.keys()):
        cnt = funnel_totals[fn]
        funnel_rows += f'<tr><td class="label">{fn}</td><td class="num {"booked" if cnt > 0 else "zero"}">{cnt}</td></tr>\n'
    funnel_rows += f'<tr class="total-row"><td class="metric">TOTAL</td><td class="num total-num">{total_booked}</td></tr>\n'

    month_name = month_date.strftime("%B %Y")
    title = f"Monthly Summary — {month_name}"
    num_days = len(dates)

    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{title}</title>
<style>{COMMON_CSS}</style>
</head><body>
{html_header_bar(title, f"{num_days} days · First Meetings Only", month_name, "Generated: " + datetime.now(PACIFIC).strftime("%b %-d at %I:%M %p %Z"))}
<div class="wrap">
  <div class="summary-cards">
    <div class="s-card"><div class="s-label">Total Booked</div><div class="s-value green">{total_booked}</div></div>
    <div class="s-card"><div class="s-label">Total Capacity</div><div class="s-value">{total_cap}</div></div>
    <div class="s-card"><div class="s-label">Avg Utilization</div><div class="s-value {util_class(avg_util)}">{avg_util:.1f}%</div></div>
  </div>
  <div class="card">
    <div class="sec">WEEK BY WEEK</div>
    <table>
      <colgroup><col style="width:170px"><col span="8"></colgroup>
      <thead><tr><th>Week</th>{week_header}</tr></thead>
      <tbody>{week_rows}</tbody>
    </table>
  </div>
  <div class="card">
    <div class="sec">FUNNEL TOTALS</div>
    <table style="table-layout:auto; max-width:400px;">
      <thead><tr><th>Funnel</th><th>Total</th></tr></thead>
      <tbody>{funnel_rows}</tbody>
    </table>
  </div>
  <div class="footer">
    <a href="../archive.html">← Back to Archive</a>
    <a href="https://anthony-funnel-reporting-mtd.tiiny.site/" target="_blank">📊 MTD Funnel Reporting →</a>
  </div>
</div></body></html>"""


# ─── Archive Index HTML ──────────────────────────────────────────────────────

def generate_archive_html(archive_dir):
    """Scan archive/ and generate index page."""
    daily_files = []
    weekly_files = []
    monthly_files = []

    archive_path = Path(archive_dir)
    if archive_path.exists():
        for f in sorted(archive_path.glob("*.html"), reverse=True):
            name = f.stem
            if name.startswith("month-"):
                monthly_files.append(name)
            elif name.startswith("week-"):
                weekly_files.append(name)
            else:
                # Daily: filename is YYYY-MM-DD
                try:
                    date.fromisoformat(name)
                    daily_files.append(name)
                except ValueError:
                    pass

    def make_links(files, prefix=""):
        if not files:
            return '<tr><td class="label" style="color:#999;">No archives yet — check back tomorrow</td></tr>'
        rows = ""
        for f in files:
            display = f
            if f.startswith("week-"):
                d = date.fromisoformat(f.replace("week-", ""))
                end = d + timedelta(days=6)
                display = f"Week of {d.strftime('%b %-d')} – {end.strftime('%b %-d, %Y')}"
            elif f.startswith("month-"):
                parts = f.replace("month-", "").split("-")
                display = f"{calendar.month_name[int(parts[1])]} {parts[0]}"
            else:
                try:
                    d = date.fromisoformat(f)
                    display = d.strftime("%A, %B %-d, %Y")
                    if d.weekday() == 6:
                        display += " (Sun – EOW)"
                except ValueError:
                    pass
            rows += f'<tr><td class="label"><a href="archive/{f}.html" style="color:#1b7a2e; text-decoration:none; font-weight:500;">{display}</a></td></tr>\n'
        return rows

    now_pacific = datetime.now(PACIFIC)

    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Call Capacity Dashboard — Archive</title>
<style>{COMMON_CSS}
a:hover {{ text-decoration: underline !important; }}
</style>
</head><body>
{html_header_bar("Call Capacity Dashboard — Archive", "Historical snapshots and summaries", now_pacific.strftime("%A, %B %-d, %Y"), "Updated: " + now_pacific.strftime("%I:%M %p %Z"))}
<div class="wrap">
  <div style="margin-bottom:1rem;">
    <a href="index.html" style="color:#1b7a2e; font-weight:600; text-decoration:none; font-size:0.85rem;">← Back to Live Dashboard</a>
  </div>
  <div class="card">
    <div class="sec">📈 MONTHLY SUMMARIES</div>
    <table style="table-layout:auto"><tbody>{make_links(monthly_files)}</tbody></table>
  </div>
  <div class="card">
    <div class="sec">📊 WEEKLY SUMMARIES</div>
    <table style="table-layout:auto"><tbody>{make_links(weekly_files)}</tbody></table>
  </div>
  <div class="card">
    <div class="sec">📅 DAILY SNAPSHOTS</div>
    <table style="table-layout:auto"><tbody>{make_links(daily_files)}</tbody></table>
  </div>
  <div class="footer">
    <span>Archive generated {now_pacific.strftime("%b %-d, %Y at %I:%M %p %Z")}</span>
    <a href="https://anthony-funnel-reporting-mtd.tiiny.site/" target="_blank">📊 MTD Funnel Reporting →</a>
  </div>
</div></body></html>"""


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    global _api_call_count

    if not CLOSE_API_KEY:
        log("❌ Error: CLOSE_API_KEY environment variable is not set.")
        sys.exit(1)

    _api_call_count = 0
    start_time = time.time()
    log("🚀 Starting Call Capacity Dashboard update (v12 — with archives)...")

    today = datetime.now(PACIFIC).date()
    log(f"📅 Today: {today} ({today.strftime('%A')})")

    # Ensure archive directory
    Path(ARCHIVE_DIR).mkdir(exist_ok=True)

    # ── Fetch all meetings once ──
    all_meetings = fetch_all_meetings()

    # ── 1. Generate rolling dashboard (index.html) ──
    log("\n═══ Rolling Dashboard ═══")
    rolling_start = today - timedelta(days=3)
    rolling_end = today + timedelta(days=10)
    rolling_dates = [rolling_start + timedelta(days=i) for i in range(13)]

    included, exclude_other, counts = classify_meetings(all_meetings, rolling_start, rolling_end)
    log(f"   INCLUDE: {counts['include']} | EXCLUDE: {counts['exclude']} | OTHER: {counts['exclude_other']}")

    lead_cache = fetch_leads_for_meetings(included)
    rolling_data = build_dashboard_data(included, lead_cache, rolling_dates, today=today)

    html = generate_rolling_html(rolling_data, exclude_other, counts)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(html)
    log(f"✅ {OUTPUT_FILE} written ({len(rolling_data['valid_meetings'])} meetings)")

    # ── 2. Daily snapshot ──
    log("\n═══ Daily Snapshot ═══")
    snapshot_path = f"{ARCHIVE_DIR}/{today.isoformat()}.html"
    with open(snapshot_path, "w", encoding="utf-8") as f:
        f.write(html)
    log(f"✅ {snapshot_path} saved")

    # ── 3. Weekly summary (generate on Monday for previous Mon-Sun) ──
    if today.weekday() == 0:  # Monday
        log("\n═══ Weekly Summary (previous week) ═══")
        prev_mon = today - timedelta(days=7)
        prev_sun = today - timedelta(days=1)
        week_dates = [prev_mon + timedelta(days=i) for i in range(7)]

        w_included, _, _ = classify_meetings(all_meetings, prev_mon, prev_sun + timedelta(days=1))
        # Reuse lead_cache — may need additional leads
        w_new_ids = set(m["lead_id"] for m in w_included if m.get("lead_id")) - set(lead_cache.keys())
        if w_new_ids:
            log(f"   Fetching {len(w_new_ids)} additional leads for weekly summary...")
            for lid in w_new_ids:
                try:
                    lead_cache[lid] = close_get(f"lead/{lid}", {"_fields": LEAD_FIELDS})
                except requests.HTTPError:
                    lead_cache[lid] = None

        w_data = build_dashboard_data(w_included, lead_cache, week_dates)
        w_html = generate_weekly_html(w_data, prev_mon)
        w_path = f"{ARCHIVE_DIR}/week-{prev_mon.isoformat()}.html"
        with open(w_path, "w", encoding="utf-8") as f:
            f.write(w_html)
        log(f"✅ {w_path} saved ({len(w_data['valid_meetings'])} meetings)")
    else:
        log(f"\n⏭ Weekly summary: skipped (today is {today.strftime('%A')}, runs on Monday)")

    # ── 4. Monthly summary (generate on 1st for previous month) ──
    if today.day == 1:
        log("\n═══ Monthly Summary (previous month) ═══")
        prev_month_end = today - timedelta(days=1)
        prev_month_start = prev_month_end.replace(day=1)
        num_days = (prev_month_end - prev_month_start).days + 1
        month_dates = [prev_month_start + timedelta(days=i) for i in range(num_days)]

        m_included, _, _ = classify_meetings(all_meetings, prev_month_start, today)
        m_new_ids = set(m["lead_id"] for m in m_included if m.get("lead_id")) - set(lead_cache.keys())
        if m_new_ids:
            log(f"   Fetching {len(m_new_ids)} additional leads for monthly summary...")
            for lid in m_new_ids:
                try:
                    lead_cache[lid] = close_get(f"lead/{lid}", {"_fields": LEAD_FIELDS})
                except requests.HTTPError:
                    lead_cache[lid] = None

        m_data = build_dashboard_data(m_included, lead_cache, month_dates)
        m_html = generate_monthly_html(m_data, prev_month_start)
        m_path = f"{ARCHIVE_DIR}/month-{prev_month_start.strftime('%Y-%m')}.html"
        with open(m_path, "w", encoding="utf-8") as f:
            f.write(m_html)
        log(f"✅ {m_path} saved ({len(m_data['valid_meetings'])} meetings)")
    else:
        log(f"\n⏭ Monthly summary: skipped (today is the {today.day}th, runs on 1st)")

    # ── 5. Regenerate archive index ──
    log("\n═══ Archive Index ═══")
    archive_html = generate_archive_html(ARCHIVE_DIR)
    with open("archive.html", "w", encoding="utf-8") as f:
        f.write(archive_html)
    log("✅ archive.html regenerated")

    elapsed = time.time() - start_time
    log(f"\n🏁 All done! Total API calls: {_api_call_count} | Time: {elapsed:.1f}s")


if __name__ == "__main__":
    main()
