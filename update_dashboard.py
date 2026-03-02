#!/usr/bin/env python3
"""
Call Capacity Dashboard Generator (v10)

Changes from v9:
- Capacity updated: Mon=57, Tue-Fri=60, Sat=4, Sun=0
- Tightened classifier: dropped "Meet with", "Intro call", "Vendingpreneurs call"
  from ambiguous includes. Only core patterns + "Vending Consult" remain.
- Visual: closer match to original tiiny.site dashboard (dark header bar, warm theme)
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

# Updated capacity: Mon=57, Tue-Fri=60, Sat=4, Sun=0
CAPACITY = {0: 57, 1: 60, 2: 60, 3: 60, 4: 60, 5: 4, 6: 0}

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


# ─── Title Classification (SLT Doc) ─────────────────────────────────────────

# Step 3: Exclude patterns
EXCLUDE_FOLLOW_UP_RE = re.compile(
    r"follow[\s\-]?up|fallow\s+up|\bf/?u\b|next\s+steps|rescheduled|reschedule",
    re.IGNORECASE
)

# Step 5: Known first-call INCLUDE patterns (core)
INCLUDE_PATTERNS_RE = re.compile(
    r"vending\s+strategy\s+call"
    r"|vendingpren[eu]+rs?\s+consultation"
    r"|vendingpren[eu]+rs?\s+strategy\s+call"
    r"|new\s+vendingpreneur\s+strategy\s+call",
    re.IGNORECASE
)

# Step 6: Only "Vending Consult" kept (specific enough)
INCLUDE_AMBIGUOUS_RE = re.compile(
    r"vending\s+consult",
    re.IGNORECASE
)

# Step 6: Ambiguous-but-EXCLUDE patterns
EXCLUDE_AMBIGUOUS_RE = re.compile(
    r"\benrollment\b|silver\s+start\s+up|bronze\s+enrollment"
    r"|questions\s+on\s+enrollment",
    re.IGNORECASE
)


def classify_meeting_title(title):
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

    # Step 6 excludes (check before includes)
    if EXCLUDE_AMBIGUOUS_RE.search(title):
        return "exclude"

    # Step 5: Core first-call patterns → INCLUDE
    if INCLUDE_PATTERNS_RE.search(title):
        return "include"

    # Step 6: "Vending Consult" only → INCLUDE
    if INCLUDE_AMBIGUOUS_RE.search(title):
        return "include"

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

    # Step 2: Filter + classify
    step_start = time.time()
    log("🔍 Step 2/4: Filtering + classifying...")

    classified = {"include": 0, "exclude": 0, "exclude_other": 0, "user_excluded": 0, "out_of_range": 0}
    exclude_other_samples = []
    meetings_in_window = []

    for m in all_meetings:
        meeting_date = parse_meeting_date_pacific(m)
        if not meeting_date or meeting_date < today or meeting_date >= end_date:
            classified["out_of_range"] += 1
            continue

        user_id = m.get("user_id", "")
        users_list = m.get("users", [])
        if user_id in EXCLUDED_USER_IDS or any(u in EXCLUDED_USER_IDS for u in (users_list or [])):
            classified["user_excluded"] += 1
            continue

        title = m.get("title", "")
        classification = classify_meeting_title(title)
        classified[classification] += 1

        if classification == "include":
            m["_meeting_date"] = meeting_date
            meetings_in_window.append(m)
        elif classification == "exclude_other" and len(exclude_other_samples) < 10:
            exclude_other_samples.append(title)

    log(f"   Out of range: {classified['out_of_range']}")
    log(f"   User excluded: {classified['user_excluded']}")
    log(f"   INCLUDE: {classified['include']}")
    log(f"   EXCLUDE: {classified['exclude']}")
    log(f"   EXCLUDE_OTHER: {classified['exclude_other']}")
    if exclude_other_samples:
        log(f"   Sample exclude_other titles:")
        for t in exclude_other_samples:
            log(f"     - {t}")
    log(f"   ✓ {len(meetings_in_window)} first calls [{elapsed_since(step_start)}]")

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

    # Step 4: Build results
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
            "funnel": funnel,
        })

    log(f"   ✅ {len(valid_meetings)} valid first meetings [{elapsed_since(step_start)}]")

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


# ─── HTML Generation ────────────────────────────────────────────────────────

def generate_html(data):
    dates = data["dates"]
    daily = data["daily_data"]
    funnels = data["funnels"]
    last_updated = data["last_updated"]
    last_updated_date = data["last_updated_date"]
    today = data["today"]

    # Date headers
    date_headers = ""
    for d in dates:
        is_today = d == today
        day_label = "► TODAY" if is_today else d.strftime("%a").upper()
        date_str = d.strftime("%m/%d")
        cls = ' class="today-col"' if is_today else ""
        date_headers += f"<th{cls}>{day_label}<br>{date_str}</th>"

    # Capacity metrics
    def td(val, d, extra_class=""):
        is_today = d == today
        cls_parts = []
        if is_today:
            cls_parts.append("today-col")
        if extra_class:
            cls_parts.append(extra_class)
        cls_attr = f' class="{" ".join(cls_parts)}"' if cls_parts else ""
        return f"<td{cls_attr}>{val}</td>"

    cap_row = ""
    booked_row = ""
    avail_row = ""
    util_row = ""

    for d in dates:
        cap = daily[d]["capacity"]
        bk = daily[d]["booked"]

        # Capacity
        cap_row += td(cap if cap > 0 else "–", d)

        # Booked (green when > 0)
        bk_cls = "val-green" if bk > 0 else "val-zero"
        booked_row += td(bk, d, bk_cls)

        # Available
        if cap > 0:
            avail = cap - bk
            avail_row += td(avail, d)
        else:
            avail_row += td("–", d)

        # Utilization
        if cap > 0:
            pct = bk / cap * 100
            pct_str = f"{pct:.2f}%"
            if pct >= 80:
                u_cls = "util-high"
            elif pct >= 40:
                u_cls = "util-mid"
            else:
                u_cls = "util-low"
            util_row += td(pct_str, d, u_cls)
        else:
            util_row += td("N/A", d)

    # Funnel rows
    funnel_rows_html = ""
    for funnel in funnels:
        cells = ""
        for d in dates:
            is_today = d == today
            count = daily[d]["funnels"].get(funnel, 0)
            if count > 0:
                cls = "today-col val-green" if is_today else "val-green"
            else:
                cls = "today-col" if is_today else ""
            cls_attr = f' class="{cls}"' if cls else ""
            cells += f"<td{cls_attr}>{count}</td>"
        funnel_rows_html += f"""<tr>
            <td class="row-label">{funnel}</td>
            {cells}
        </tr>"""

    # Total row
    total_cells = ""
    for d in dates:
        is_today = d == today
        t = daily[d]["booked"]
        cls = "today-col total-val" if is_today else "total-val"
        total_cells += f'<td class="{cls}">{t}</td>'

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Call Capacity Dashboard</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600;700&display=swap');

  :root {{
    --bg: #f5f0e8;
    --surface: #fff;
    --header-bg: #1a2e1a;
    --header-text: #e8f0e8;
    --border: #d6cfc4;
    --border-light: #ebe6dd;
    --text: #2a2520;
    --text-dim: #6b6560;
    --green: #2d7a3a;
    --green-light: #16a34a;
    --amber: #b45309;
    --red: #c42b2b;
    --today-bg: #fef6e6;
    --today-header: #f0e0b8;
    --section-green: #2d6b3a;
  }}

  * {{ margin: 0; padding: 0; box-sizing: border-box; }}

  body {{
    font-family: 'Inter', -apple-system, system-ui, sans-serif;
    background: var(--bg);
    color: var(--text);
    min-height: 100vh;
  }}

  .header-bar {{
    background: var(--header-bg);
    padding: 1rem 2rem;
    display: flex;
    justify-content: space-between;
    align-items: center;
  }}
  .header-bar h1 {{
    font-size: 1.3rem;
    font-weight: 700;
    color: var(--header-text);
  }}
  .header-bar h1 span {{
    margin-right: 0.4rem;
  }}
  .header-bar .subtitle {{
    font-size: 0.75rem;
    color: #a0b8a0;
    margin-top: 0.15rem;
  }}
  .header-right {{
    text-align: right;
    font-family: 'JetBrains Mono', monospace;
    color: var(--header-text);
  }}
  .header-right .date {{
    font-size: 0.85rem;
    font-weight: 600;
  }}
  .header-right .time {{
    font-size: 0.7rem;
    color: #a0b8a0;
  }}
  .status-dot {{
    display: inline-block;
    width: 8px; height: 8px;
    background: #4ade80;
    border-radius: 50%;
    margin-right: 6px;
    vertical-align: middle;
  }}

  .content {{
    padding: 1.25rem 2rem 2rem;
    max-width: 1500px;
    margin: 0 auto;
  }}

  .section-label {{
    font-size: 0.68rem;
    font-weight: 800;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: var(--section-green);
    padding: 0.7rem 0.75rem 0.35rem;
    border-left: 3px solid var(--section-green);
    background: var(--surface);
    border-top: 1px solid var(--border);
    border-right: 1px solid var(--border);
  }}

  .table-wrap {{
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 6px;
    overflow-x: auto;
    margin-bottom: 1.25rem;
  }}

  table {{
    width: 100%;
    border-collapse: collapse;
  }}
  th {{
    padding: 0.6rem 0.7rem;
    text-align: center;
    font-weight: 700;
    font-size: 0.72rem;
    color: var(--text-dim);
    border-bottom: 2px solid var(--border);
    background: var(--surface);
    white-space: nowrap;
    line-height: 1.35;
  }}
  th:first-child {{
    text-align: left;
    padding-left: 0.75rem;
    min-width: 130px;
  }}
  td {{
    padding: 0.45rem 0.7rem;
    text-align: center;
    border-bottom: 1px solid var(--border-light);
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.8rem;
    font-weight: 500;
    color: var(--text);
  }}

  .row-label {{
    text-align: left !important;
    font-family: 'Inter', sans-serif !important;
    font-weight: 500;
    font-size: 0.78rem;
    padding-left: 0.75rem !important;
    color: var(--text);
  }}
  .metric-label {{
    text-align: left !important;
    font-family: 'Inter', sans-serif !important;
    font-weight: 600;
    font-size: 0.8rem;
    padding-left: 0.75rem !important;
    color: var(--text);
  }}

  .today-col {{
    background: var(--today-bg) !important;
  }}
  th.today-col {{
    background: var(--today-header) !important;
    color: var(--amber);
    font-weight: 800;
  }}

  .val-green {{ color: var(--green); font-weight: 700; }}
  .val-zero {{ color: var(--text-dim); }}
  .total-val {{ font-weight: 700; }}

  .util-low {{ color: var(--green-light); font-weight: 700; }}
  .util-mid {{ color: var(--amber); font-weight: 700; }}
  .util-high {{ color: var(--red); font-weight: 700; }}

  tr.totals-row td {{
    border-top: 2px solid var(--border);
    font-weight: 700;
  }}

  @media (max-width: 900px) {{
    .header-bar {{ padding: 0.75rem 1rem; }}
    .content {{ padding: 0.75rem 1rem; }}
  }}
</style>
</head>
<body>

<div class="header-bar">
  <div>
    <h1><span>📞</span>Call Capacity Dashboard</h1>
    <div class="subtitle">10-Day Lookahead · First Meetings Only</div>
  </div>
  <div class="header-right">
    <div class="date"><span class="status-dot"></span>{last_updated_date}</div>
    <div class="time">Last updated: {last_updated}</div>
  </div>
</div>

<div class="content">

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
        <tr><td class="metric-label">Capacity</td>{cap_row}</tr>
        <tr><td class="metric-label">Booked</td>{booked_row}</tr>
        <tr><td class="metric-label">Available</td>{avail_row}</tr>
        <tr><td class="metric-label">Utilization %</td>{util_row}</tr>
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
          {total_cells}
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
    log("🚀 Starting Call Capacity Dashboard update (v10)...")
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
