#!/usr/bin/env python3
"""
Call Capacity Dashboard Generator (v9)

Classification rules based on SLT_Classifier_Rules.docx:
- INCLUDE-based: only meetings matching known first-call title patterns are counted
- 4 excluded users: Kristin Nelson, Spencer Reynolds, Stephen Olivas, Ahmad Bukhari
- No reschedule check (First Call Booked Date is unreliable per doc)
- No existing-customer check (not in doc spec)
- Title classification follows Steps 1-6 from the doc

UI: Light cream theme matching existing capacity-10day-dashboard.tiiny.site
"""

import os
import sys
import json
import re
import time
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
import requests

# ─── Configuration ───────────────────────────────────────────────────────────

CLOSE_API_KEY = os.environ.get("CLOSE_API_KEY", "")
CLOSE_API_BASE = "https://api.close.com/api/v1"
PACIFIC = ZoneInfo("America/Los_Angeles")
UTC = ZoneInfo("UTC")

CAPACITY = {0: 44, 1: 47, 2: 47, 3: 47, 4: 47, 5: 4, 6: 0}

# 4 excluded users per SLT doc
EXCLUDED_USER_IDS = {
    "user_EmhqCmaHERTfgfWnPADiLGEqQw3ENvRYd3u1VEmblIp",  # Kristin Nelson
    "user_4sfuKGMbv0LQZ4hpS8ipASv406kKTSNP5Xx79jOwSqM",  # Spencer Reynolds
    "user_5cZRqXu8kb4O1IeBVA98UMcMEhYZUhx1fnCHfSL0YMV",  # Stephen Olivas
    "user_yRF070m26JE67J6CJqzkAB3IqY7btNm1K5RisCglKa6",  # Ahmad Bukhari
}

FIELD_FUNNEL_NAME_DEAL = "custom.cf_xqDQE8fkPsWa0RNEve7hcaxKblCe6489XeZGRDzyPdX"

LEAD_FIELDS = ",".join([
    "id", "display_name", "name",
    FIELD_FUNNEL_NAME_DEAL,
])

OUTPUT_FILE = os.environ.get("OUTPUT_FILE", "index.html")
API_THROTTLE = 0.5


# ─── Title Classification (SLT Doc Steps 1-6) ───────────────────────────────

# Step 3: Exclude patterns
EXCLUDE_FOLLOW_UP_RE = re.compile(
    r"follow[\s\-]?up|fallow\s+up|\bf/?u\b|next\s+steps|rescheduled|reschedule",
    re.IGNORECASE
)

# Step 5: Known first-call INCLUDE patterns
INCLUDE_PATTERNS_RE = re.compile(
    r"vending\s+strategy\s+call"
    r"|vendingpren[eu]+rs?\s+consultation"
    r"|vendingpren[eu]+rs?\s+strategy\s+call"
    r"|new\s+vendingpreneur\s+strategy\s+call",
    re.IGNORECASE
)

# Step 6: Ambiguous-but-INCLUDE patterns
INCLUDE_AMBIGUOUS_RE = re.compile(
    r"vending\s+consult"
    r"|vendingpren[eu]+rs?\s+w[\s/]"
    r"|vendingpren[eu]+rs?\s+call"
    r"|meet\s+with"
    r"|intro\s+call",
    re.IGNORECASE
)

# Step 6: Ambiguous-but-EXCLUDE patterns
EXCLUDE_AMBIGUOUS_RE = re.compile(
    r"\benrollment\b|silver\s+start\s+up|bronze\s+enrollment"
    r"|questions\s+on\s+enrollment",
    re.IGNORECASE
)


def classify_meeting_title(title):
    """
    Classify a meeting title per SLT doc Steps 1-6.
    Returns 'include', 'exclude', or 'exclude_other'.
    """
    if not title:
        return "exclude_other"

    # Step 1: Exclude if starts with "Canceled:"
    if title.strip().lower().startswith("canceled:"):
        return "exclude"

    # Step 2: Exclude Kristin's Discovery Calls
    if "vending quick discovery" in title.lower():
        return "exclude"

    # Step 3: Exclude follow-ups, F/U, next steps, rescheduled
    if EXCLUDE_FOLLOW_UP_RE.search(title):
        return "exclude"

    # Step 4: Exclude Anthony's Q&A
    if "anthony" in title.lower() and "q&a" in title.lower():
        return "exclude"

    # Step 6 excludes (check before includes to avoid false positives)
    if EXCLUDE_AMBIGUOUS_RE.search(title):
        return "exclude"

    # Step 5: Known first-call patterns → INCLUDE
    if INCLUDE_PATTERNS_RE.search(title):
        return "include"

    # Step 6: Ambiguous-but-include patterns → INCLUDE
    if INCLUDE_AMBIGUOUS_RE.search(title):
        return "include"

    # Everything else → excluded (not a recognized first call)
    return "exclude_other"


# ─── Helpers ─────────────────────────────────────────────────────────────────

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


# ─── Main Pipeline ───────────────────────────────────────────────────────────

def run_pipeline():
    global _api_call_count
    _api_call_count = 0
    pipeline_start = time.time()

    today = datetime.now(PACIFIC).date()
    end_date = today + timedelta(days=10)
    log(f"📅 Date range: {today} to {end_date} (Pacific)")

    # Step 1: Fetch ALL meetings
    step_start = time.time()
    log("📥 Step 1/4: Fetching all meetings...")
    all_meetings = []
    skip = 0
    limit = 100
    while True:
        data = close_get("activity/meeting", {"_skip": skip, "_limit": limit})
        batch = data.get("data", [])
        all_meetings.extend(batch)
        if len(all_meetings) % 1000 == 0:
            log(f"   ... {len(all_meetings)} meetings fetched")
        if not data.get("has_more", False):
            break
        skip += limit
    log(f"   ✓ {len(all_meetings)} total meetings [{elapsed_since(step_start)}]")

    # Step 2: Filter to 10-day window + classify titles
    step_start = time.time()
    log("🔍 Step 2/4: Filtering to window + classifying titles...")

    classified = {"include": 0, "exclude": 0, "exclude_other": 0, "user_excluded": 0, "out_of_range": 0}
    meetings_in_window = []

    for m in all_meetings:
        meeting_date = parse_meeting_date_pacific(m)
        if not meeting_date or meeting_date < today or meeting_date >= end_date:
            classified["out_of_range"] += 1
            continue

        # User exclusion
        user_id = m.get("user_id", "")
        users_list = m.get("users", [])
        if user_id in EXCLUDED_USER_IDS or any(u in EXCLUDED_USER_IDS for u in (users_list or [])):
            classified["user_excluded"] += 1
            continue

        # Title classification
        title = m.get("title", "")
        classification = classify_meeting_title(title)
        classified[classification] += 1

        if classification == "include":
            m["_meeting_date"] = meeting_date
            meetings_in_window.append(m)

    log(f"   Classification results:")
    log(f"     Out of date range: {classified['out_of_range']}")
    log(f"     User excluded: {classified['user_excluded']}")
    log(f"     Title INCLUDE: {classified['include']}")
    log(f"     Title EXCLUDE: {classified['exclude']}")
    log(f"     Title EXCLUDE_OTHER: {classified['exclude_other']}")
    log(f"   ✓ {len(meetings_in_window)} first calls in window [{elapsed_since(step_start)}]")

    # Step 3: Fetch leads for funnel data
    step_start = time.time()
    unique_lead_ids = list(set(m["lead_id"] for m in meetings_in_window if m.get("lead_id")))
    log(f"📥 Step 3/4: Fetching {len(unique_lead_ids)} leads...")
    lead_cache = {}
    for i, lid in enumerate(unique_lead_ids, 1):
        if i % 20 == 0 or i == len(unique_lead_ids):
            log(f"   ... {i}/{len(unique_lead_ids)} leads ({_api_call_count} API calls)")
        try:
            lead_cache[lid] = close_get(f"lead/{lid}", {"_fields": LEAD_FIELDS})
        except requests.HTTPError as e:
            log(f"  ⚠ Could not fetch lead {lid}: {e}")
            lead_cache[lid] = None
    log(f"   ✓ All leads fetched [{elapsed_since(step_start)}]")

    # Step 4: Build final results
    step_start = time.time()
    log("📊 Step 4/4: Building dashboard data...")
    valid_meetings = []
    for m in meetings_in_window:
        lead_id = m.get("lead_id")
        lead_data = lead_cache.get(lead_id) if lead_id else None
        funnel = (lead_data.get(FIELD_FUNNEL_NAME_DEAL) if lead_data else None) or "Unknown"
        valid_meetings.append({
            "date": m["_meeting_date"],
            "title": m.get("title", ""),
            "lead_id": lead_id or "",
            "lead_name": (lead_data.get("display_name") or lead_data.get("name", "Unknown")) if lead_data else "Unknown",
            "funnel": funnel,
        })

    log(f"   ✅ {len(valid_meetings)} valid first meetings [{elapsed_since(step_start)}]")

    # Build daily aggregations
    dates = [today + timedelta(days=i) for i in range(10)]
    daily_data = {}
    funnel_set = set()
    for d in dates:
        daily_data[d] = {"booked": 0, "capacity": CAPACITY[d.weekday()], "funnels": {}}
    for m in valid_meetings:
        d = m["date"]
        if d not in daily_data:
            continue
        daily_data[d]["booked"] += 1
        funnel = m["funnel"]
        funnel_set.add(funnel)
        daily_data[d]["funnels"][funnel] = daily_data[d]["funnels"].get(funnel, 0) + 1

    funnels_sorted = sorted(funnel_set)
    now_pacific = datetime.now(PACIFIC)

    log(f"   📡 Total API calls: {_api_call_count}")
    log(f"   ⏱ Pipeline time: {elapsed_since(pipeline_start)}")

    return {
        "dates": dates,
        "daily_data": daily_data,
        "funnels": funnels_sorted,
        "valid_meetings": valid_meetings,
        "last_updated": now_pacific.strftime("%I:%M %p %Z"),
        "last_updated_date": now_pacific.strftime("%A, %B %-d, %Y"),
        "today": today,
    }


# ─── HTML Generation (Light Cream Theme) ────────────────────────────────────

def generate_html(data):
    dates = data["dates"]
    daily = data["daily_data"]
    funnels = data["funnels"]
    last_updated = data["last_updated"]
    last_updated_date = data["last_updated_date"]
    today = data["today"]

    # Build date column headers
    date_headers = ""
    for d in dates:
        is_today = d == today
        day_label = "► TODAY" if is_today else d.strftime("%a").upper()
        date_str = d.strftime("%m/%d")
        cls = ' class="today-col"' if is_today else ""
        date_headers += f"<th{cls}>{day_label}<br>{date_str}</th>"

    # Capacity metrics rows
    cap_row = ""
    booked_row = ""
    avail_row = ""
    util_row = ""
    for d in dates:
        is_today = d == today
        cls = ' class="today-col"' if is_today else ""
        cap = daily[d]["capacity"]
        bk = daily[d]["booked"]
        avail = cap - bk if cap > 0 else "–"
        pct = round(bk / cap * 100, 2) if cap > 0 else None
        pct_str = f"{pct:.2f}%" if pct is not None else "N/A"

        if pct is not None:
            if pct >= 80:
                pct_class = "util-high"
            elif pct >= 50:
                pct_class = "util-mid"
            else:
                pct_class = "util-low"
        else:
            pct_class = ""

        cap_row += f"<td{cls}>{cap if cap > 0 else '–'}</td>"
        bk_class = "booked-val"
        booked_row += f'<td{cls} class="{bk_class}">{bk}</td>'
        avail_row += f"<td{cls}>{avail}</td>"
        util_row += f'<td{cls} class="{pct_class}">{pct_str}</td>'

    # Funnel breakdown rows
    funnel_rows_html = ""
    for funnel in funnels:
        cells = ""
        for d in dates:
            is_today = d == today
            cls = ' class="today-col"' if is_today else ""
            count = daily[d]["funnels"].get(funnel, 0)
            val = str(count) if count > 0 else "0"
            bold = " style='font-weight:700;color:#b45309;'" if count > 0 and is_today else (" style='font-weight:600;'" if count > 0 else "")
            cells += f"<td{cls}{bold}>{val}</td>"
        funnel_rows_html += f"""
        <tr>
            <td class="funnel-name">{funnel}</td>
            {cells}
        </tr>"""

    # Totals row for funnel section
    funnel_totals = ""
    for d in dates:
        is_today = d == today
        cls = ' class="today-col"' if is_today else ""
        t = daily[d]["booked"]
        funnel_totals += f'<td{cls} style="font-weight:700;">{t}</td>'

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Call Capacity Dashboard</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap');

  :root {{
    --bg: #faf8f5;
    --surface: #ffffff;
    --border: #e5e0d8;
    --border-light: #f0ece6;
    --text: #1a1a1a;
    --text-dim: #6b6560;
    --text-muted: #9a9490;
    --accent: #2d7a4f;
    --accent-bg: #e8f5ee;
    --green: #16a34a;
    --amber: #b45309;
    --red: #dc2626;
    --today-bg: #fef9ee;
    --today-border: #f0e4c8;
    --section-accent: #2d7a4f;
  }}

  * {{ margin: 0; padding: 0; box-sizing: border-box; }}

  body {{
    font-family: 'Inter', -apple-system, system-ui, sans-serif;
    background: var(--bg);
    color: var(--text);
    min-height: 100vh;
    padding: 1.5rem 2rem;
  }}

  .dashboard {{
    max-width: 1400px;
    margin: 0 auto;
  }}

  .header {{
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    margin-bottom: 2rem;
    padding-bottom: 1rem;
    border-bottom: 2px solid var(--border);
  }}
  .header-left h1 {{
    font-size: 1.5rem;
    font-weight: 700;
    color: var(--text);
    display: flex;
    align-items: center;
    gap: 0.5rem;
  }}
  .header-left .subtitle {{
    font-size: 0.8rem;
    color: var(--text-dim);
    margin-top: 0.25rem;
  }}
  .header-right {{
    text-align: right;
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.8rem;
  }}
  .header-right .date {{
    font-weight: 600;
    color: var(--text);
  }}
  .header-right .time {{
    color: var(--text-dim);
    font-size: 0.75rem;
  }}
  .status-dot {{
    display: inline-block;
    width: 7px; height: 7px;
    background: var(--green);
    border-radius: 50%;
    margin-right: 4px;
    vertical-align: middle;
  }}

  .section-label {{
    font-size: 0.7rem;
    font-weight: 700;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    color: var(--section-accent);
    padding: 0.75rem 1rem 0.4rem;
    background: var(--surface);
    border-left: 3px solid var(--section-accent);
  }}

  .table-wrap {{
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 8px;
    overflow-x: auto;
    margin-bottom: 1.5rem;
  }}

  table {{
    width: 100%;
    border-collapse: collapse;
    font-size: 0.82rem;
  }}
  th {{
    padding: 0.65rem 0.8rem;
    text-align: center;
    font-weight: 600;
    color: var(--text-dim);
    border-bottom: 2px solid var(--border);
    font-size: 0.75rem;
    white-space: nowrap;
    background: var(--surface);
  }}
  th:first-child {{
    text-align: left;
    min-width: 140px;
    padding-left: 1rem;
  }}
  td {{
    padding: 0.5rem 0.8rem;
    text-align: center;
    border-bottom: 1px solid var(--border-light);
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.8rem;
    font-weight: 500;
  }}
  td:first-child {{
    text-align: left;
    font-family: 'Inter', sans-serif;
    font-weight: 500;
    padding-left: 1rem;
    color: var(--text);
  }}

  .today-col {{
    background: var(--today-bg) !important;
    border-left: 1px solid var(--today-border);
    border-right: 1px solid var(--today-border);
  }}
  th.today-col {{
    background: var(--today-bg) !important;
    color: var(--amber);
    font-weight: 700;
  }}

  .metric-label {{
    font-weight: 600;
    color: var(--text);
  }}
  .booked-val {{
    color: var(--accent);
    font-weight: 600;
  }}
  .util-low {{ color: var(--green); font-weight: 600; }}
  .util-mid {{ color: var(--amber); font-weight: 600; }}
  .util-high {{ color: var(--red); font-weight: 600; }}

  .funnel-name {{
    font-size: 0.78rem;
    color: var(--text);
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    max-width: 180px;
  }}

  tr.totals-row td {{
    border-top: 2px solid var(--border);
    font-weight: 700;
    color: var(--text);
  }}

  @media (max-width: 900px) {{
    body {{ padding: 0.75rem; }}
  }}
</style>
</head>
<body>
<div class="dashboard">

  <div class="header">
    <div class="header-left">
      <h1>📞 Call Capacity Dashboard</h1>
      <div class="subtitle">10-Day Lookahead · First Meetings Only</div>
    </div>
    <div class="header-right">
      <div class="date"><span class="status-dot"></span>{last_updated_date}</div>
      <div class="time">Last updated: {last_updated}</div>
    </div>
  </div>

  <div class="table-wrap">
    <div class="section-label">CAPACITY METRICS</div>
    <table>
      <thead>
        <tr>
          <th></th>
          {date_headers}
        </tr>
      </thead>
      <tbody>
        <tr>
          <td class="metric-label">Capacity</td>
          {cap_row}
        </tr>
        <tr>
          <td class="metric-label">Booked</td>
          {booked_row}
        </tr>
        <tr>
          <td class="metric-label">Available</td>
          {avail_row}
        </tr>
        <tr>
          <td class="metric-label">Utilization %</td>
          {util_row}
        </tr>
      </tbody>
    </table>
  </div>

  <div class="table-wrap">
    <div class="section-label">FUNNEL BREAKDOWN</div>
    <table>
      <thead>
        <tr>
          <th>Funnel</th>
          {date_headers}
        </tr>
      </thead>
      <tbody>
        {funnel_rows_html}
        <tr class="totals-row">
          <td class="metric-label">TOTAL</td>
          {funnel_totals}
        </tr>
      </tbody>
    </table>
  </div>

</div>
</body>
</html>"""

    return html


# ─── Entry Point ─────────────────────────────────────────────────────────────

def main():
    if not CLOSE_API_KEY:
        log("❌ Error: CLOSE_API_KEY environment variable is not set.")
        sys.exit(1)

    start_time = time.time()
    log("🚀 Starting Call Capacity Dashboard update (v9 — SLT Rules)...")
    data = run_pipeline()

    log("📄 Generating HTML...")
    html = generate_html(data)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(html)

    elapsed = time.time() - start_time
    log(f"\n✅ Dashboard written to {OUTPUT_FILE}")
    log(f"   {len(data['valid_meetings'])} first meetings across {len(data['funnels'])} funnels")
    log(f"   ⏱ Total time: {elapsed:.1f}s")


if __name__ == "__main__":
    main()
