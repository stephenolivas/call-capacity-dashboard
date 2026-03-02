#!/usr/bin/env python3
"""
Call Capacity Dashboard Generator (v11)

UI: White/green/black theme matching original tiiny.site dashboard.
TODAY column highlighted through both tables. Excluded titles shown at bottom.
Link to MTD funnel reporting.
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

CAPACITY = {0: 57, 1: 60, 2: 60, 3: 60, 4: 60, 5: 4, 6: 0}

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

INCLUDE_AMBIGUOUS_RE = re.compile(
    r"vending\s+consult",
    re.IGNORECASE
)

EXCLUDE_AMBIGUOUS_RE = re.compile(
    r"\benrollment\b|silver\s+start\s+up|bronze\s+enrollment"
    r"|questions\s+on\s+enrollment",
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


# ─── Pipeline ────────────────────────────────────────────────────────────────

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
    while True:
        data = close_get("activity/meeting", {"_skip": skip, "_limit": 100})
        all_meetings.extend(data.get("data", []))
        if len(all_meetings) % 1000 == 0:
            log(f"   ... {len(all_meetings)} meetings fetched")
        if not data.get("has_more", False):
            break
        skip += 100
    log(f"   ✓ {len(all_meetings)} total [{elapsed_since(step_start)}]")

    # Step 2: Filter + classify
    step_start = time.time()
    log("🔍 Step 2/4: Filtering + classifying...")

    counts = {"include": 0, "exclude": 0, "exclude_other": 0, "user_excluded": 0, "out_of_range": 0}
    exclude_other_titles = []
    meetings_in_window = []

    for m in all_meetings:
        meeting_date = parse_meeting_date_pacific(m)
        if not meeting_date or meeting_date < today or meeting_date >= end_date:
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
            meetings_in_window.append(m)
        elif cls == "exclude_other":
            exclude_other_titles.append({"title": title, "date": meeting_date.isoformat()})

    log(f"   Out of range: {counts['out_of_range']}")
    log(f"   User excluded: {counts['user_excluded']}")
    log(f"   INCLUDE: {counts['include']}")
    log(f"   EXCLUDE: {counts['exclude']}")
    log(f"   EXCLUDE_OTHER: {counts['exclude_other']}")
    if exclude_other_titles[:10]:
        log("   Sample exclude_other:")
        for t in exclude_other_titles[:10]:
            log(f"     - {t['title']}")
    log(f"   ✓ {len(meetings_in_window)} first calls [{elapsed_since(step_start)}]")

    # Step 3: Fetch leads
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
    log(f"   ✓ Leads fetched [{elapsed_since(step_start)}]")

    # Step 4: Build results
    step_start = time.time()
    log("📊 Step 4/4: Building dashboard data...")
    valid_meetings = []
    for m in meetings_in_window:
        lead_id = m.get("lead_id")
        lead_data = lead_cache.get(lead_id) if lead_id else None
        funnel = (lead_data.get(FIELD_FUNNEL_NAME_DEAL) if lead_data else None) or "Unknown"
        valid_meetings.append({"date": m["_meeting_date"], "title": m.get("title", ""), "funnel": funnel})

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
        f = m["funnel"]
        funnel_set.add(f)
        daily_data[d]["funnels"][f] = daily_data[d]["funnels"].get(f, 0) + 1

    now_pacific = datetime.now(PACIFIC)
    log(f"   📡 Total API calls: {_api_call_count}")
    log(f"   ⏱ Pipeline: {elapsed_since(pipeline_start)}")

    return {
        "dates": dates,
        "daily_data": daily_data,
        "funnels": sorted(funnel_set),
        "valid_meetings": valid_meetings,
        "exclude_other_titles": exclude_other_titles,
        "counts": counts,
        "last_updated": now_pacific.strftime("%I:%M %p %Z"),
        "last_updated_date": now_pacific.strftime("%A, %B %-d, %Y"),
        "today": today,
    }


# ─── HTML ────────────────────────────────────────────────────────────────────

def generate_html(data):
    dates = data["dates"]
    daily = data["daily_data"]
    funnels = data["funnels"]
    today = data["today"]
    last_updated = data["last_updated"]
    last_updated_date = data["last_updated_date"]
    exclude_other = data["exclude_other_titles"]
    counts = data["counts"]

    def tc(d):
        return " today" if d == today else ""

    # --- Date headers ---
    date_headers = ""
    for d in dates:
        is_today = d == today
        label = "► TODAY" if is_today else d.strftime("%a").upper()
        ds = d.strftime("%m/%d")
        date_headers += f'<th class="col-date{tc(d)}">{label}<br>{ds}</th>'

    # --- Capacity metrics rows ---
    cap_r = booked_r = avail_r = util_r = ""
    for d in dates:
        c = daily[d]["capacity"]
        b = daily[d]["booked"]
        t = tc(d)

        cap_r += f'<td class="num{t}">{c if c > 0 else "–"}</td>'

        if b > 0:
            booked_r += f'<td class="num booked{t}">{b}</td>'
        else:
            booked_r += f'<td class="num zero{t}">{b}</td>'

        if c > 0:
            avail_r += f'<td class="num{t}">{c - b}</td>'
        else:
            avail_r += f'<td class="num{t}">–</td>'

        if c > 0:
            pct = b / c * 100
            ps = f"{pct:.2f}%"
            uc = " util-high" if pct >= 80 else (" util-mid" if pct >= 40 else " util-low")
            util_r += f'<td class="num{uc}{t}">{ps}</td>'
        else:
            util_r += f'<td class="num{t}">N/A</td>'

    # --- Funnel rows ---
    funnel_html = ""
    for fn in funnels:
        cells = ""
        for d in dates:
            cnt = daily[d]["funnels"].get(fn, 0)
            t = tc(d)
            if cnt > 0:
                cells += f'<td class="num booked{t}">{cnt}</td>'
            else:
                cells += f'<td class="num zero{t}">0</td>'
        funnel_html += f'<tr><td class="label">{fn}</td>{cells}</tr>\n'

    total_cells = ""
    for d in dates:
        t = tc(d)
        total_cells += f'<td class="num total-num{t}">{daily[d]["booked"]}</td>'

    # --- Excluded titles table ---
    excluded_rows = ""
    for item in exclude_other:
        title_esc = item["title"].replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        excluded_rows += f'<tr><td class="label">{title_esc}</td><td class="num">{item["date"]}</td></tr>\n'

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Call Capacity Dashboard</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600;700&display=swap');

* {{ margin:0; padding:0; box-sizing:border-box; }}

body {{
  font-family: 'Inter', -apple-system, system-ui, sans-serif;
  background: #ffffff;
  color: #1a1a1a;
}}

/* ── Header bar ── */
.header {{
  background: #1b2e1b;
  color: #fff;
  padding: 0.8rem 1.5rem;
  display: flex;
  justify-content: space-between;
  align-items: center;
}}
.header h1 {{
  font-size: 1.15rem;
  font-weight: 700;
}}
.header .sub {{
  font-size: 0.68rem;
  color: #a3c4a3;
  margin-top: 2px;
}}
.header .right {{
  text-align: right;
  font-family: 'JetBrains Mono', monospace;
}}
.header .right .date {{
  font-size: 0.78rem;
  font-weight: 600;
}}
.header .right .time {{
  font-size: 0.65rem;
  color: #a3c4a3;
}}
.dot {{
  display: inline-block;
  width: 7px; height: 7px;
  background: #4ade80;
  border-radius: 50%;
  margin-right: 5px;
}}

/* ── Content ── */
.wrap {{
  padding: 1rem 1.5rem 2rem;
  max-width: 1500px;
  margin: 0 auto;
}}

/* ── Section labels ── */
.sec {{
  font-size: 0.62rem;
  font-weight: 800;
  letter-spacing: 0.14em;
  text-transform: uppercase;
  color: #1b5e1b;
  padding: 0.55rem 0.6rem 0.3rem;
  border-left: 3px solid #1b5e1b;
  background: #f8faf8;
}}

/* ── Tables ── */
.card {{
  border: 1px solid #d4d4d4;
  border-radius: 4px;
  overflow-x: auto;
  margin-bottom: 1rem;
  background: #fff;
}}

table {{
  width: 100%;
  border-collapse: collapse;
}}

th {{
  padding: 0.5rem 0.6rem;
  font-size: 0.68rem;
  font-weight: 700;
  text-align: center;
  color: #555;
  border-bottom: 2px solid #d4d4d4;
  white-space: nowrap;
  background: #fafafa;
  line-height: 1.4;
}}
th:first-child {{
  text-align: left;
  padding-left: 0.6rem;
  min-width: 120px;
}}

td {{
  padding: 0.35rem 0.6rem;
  border-bottom: 1px solid #ececec;
  font-size: 0.78rem;
}}

td.num {{
  text-align: center;
  font-family: 'JetBrains Mono', monospace;
  font-size: 0.76rem;
  font-weight: 500;
  color: #333;
}}

td.label {{
  font-weight: 500;
  font-size: 0.76rem;
  padding-left: 0.6rem;
  color: #1a1a1a;
}}

.metric {{
  font-weight: 600;
  font-size: 0.78rem;
  padding-left: 0.6rem;
  color: #1a1a1a;
}}

/* ── TODAY column highlight ── */
th.col-date.today {{
  background: #fdf3e0 !important;
  color: #b45309;
}}
td.today {{
  background: #fdf8ee !important;
}}

/* ── Value styles ── */
.booked {{ color: #1b7a2e; font-weight: 700; }}
.zero {{ color: #aaa; }}
.total-num {{ font-weight: 700; color: #1a1a1a; }}

.util-low {{ color: #16a34a; font-weight: 700; }}
.util-mid {{ color: #b45309; font-weight: 700; }}
.util-high {{ color: #dc2626; font-weight: 700; }}

tr.total-row td {{
  border-top: 2px solid #bbb;
}}

/* ── Excluded section ── */
.excluded-section {{
  margin-top: 0.5rem;
}}
.excluded-section summary {{
  font-size: 0.72rem;
  font-weight: 600;
  color: #666;
  cursor: pointer;
  padding: 0.4rem 0;
}}
.excluded-section .exc-table {{
  margin-top: 0.3rem;
}}
.excluded-section .exc-table td {{
  font-size: 0.7rem;
  color: #666;
  padding: 0.2rem 0.6rem;
}}

/* ── Footer ── */
.footer {{
  margin-top: 1.5rem;
  padding-top: 0.75rem;
  border-top: 1px solid #e0e0e0;
  font-size: 0.72rem;
  color: #888;
  display: flex;
  justify-content: space-between;
  align-items: center;
}}
.footer a {{
  color: #1b7a2e;
  text-decoration: none;
  font-weight: 600;
}}
.footer a:hover {{
  text-decoration: underline;
}}

@media (max-width: 900px) {{
  .header {{ padding: 0.6rem 0.75rem; }}
  .wrap {{ padding: 0.5rem 0.75rem; }}
}}
</style>
</head>
<body>

<div class="header">
  <div>
    <h1>📞 Call Capacity Dashboard</h1>
    <div class="sub">10-Day Lookahead · First Meetings Only</div>
  </div>
  <div class="right">
    <div class="date"><span class="dot"></span>{last_updated_date}</div>
    <div class="time">Last updated: {last_updated}</div>
  </div>
</div>

<div class="wrap">

  <div class="card">
    <div class="sec">CAPACITY METRICS</div>
    <table>
      <thead><tr>
        <th></th>
        {date_headers}
      </tr></thead>
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
      <thead><tr>
        <th>Funnel</th>
        {date_headers}
      </tr></thead>
      <tbody>
        {funnel_html}
        <tr class="total-row">
          <td class="metric">TOTAL</td>
          {total_cells}
        </tr>
      </tbody>
    </table>
  </div>

  <div class="excluded-section">
    <details>
      <summary>📋 Excluded Titles ({len(exclude_other)} meetings not classified as first calls)</summary>
      <div class="card exc-table">
        <table>
          <thead><tr><th>Title</th><th>Date</th></tr></thead>
          <tbody>
            {excluded_rows if excluded_rows else '<tr><td class="label" colspan="2">None</td></tr>'}
          </tbody>
        </table>
      </div>
    </details>
  </div>

  <div class="footer">
    <span>Classification: {counts['include']} included · {counts['exclude']} excluded · {counts['exclude_other']} unclassified</span>
    <a href="https://anthony-funnel-reporting-mtd.tiiny.site/" target="_blank">📊 MTD Funnel Reporting →</a>
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
    log("🚀 Starting Call Capacity Dashboard update (v11)...")
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
