#!/usr/bin/env python3
"""
Call Capacity Dashboard Generator (v5 — Optimized Sequential)
Fetches first strategy call bookings from Close CRM for a 10-day rolling window,
applies exclusion rules, and generates a self-contained HTML dashboard.

Key optimization: Existing-customer check uses a single bulk query instead of
per-lead calls, cutting total API calls roughly in half vs v4.

Sequential with 0.7s throttle (~85 req/min, safely under Close's 100/min limit).
Typical runtime: 3-6 minutes.
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

EXCLUDED_USER_IDS = {
    "user_EmhqCmaHERTfgfWnPADiLGEqQw3ENvRYd3u1VEmblIp",  # Kristin Nelson
    "user_4sfuKGMbv0LQZ4hpS8ipASv406kKTSNP5Xx79jOwSqM",  # Spencer Reynolds
}

EXCLUDED_TITLE_PATTERNS = [
    r"\bfollow\b",
    r"\bf/u\b",
    r"\bfollow[\s\-]?up\b",
    r"\bnext\s+steps\b",
    r"\brescheduled\b",
    r"\banthony'?s?\s+q\s*&\s*a\b",
    r"^test$",
    r"\bcanceled\b",
]
EXCLUDED_TITLE_RE = re.compile("|".join(EXCLUDED_TITLE_PATTERNS), re.IGNORECASE)

FIELD_FIRST_CALL_BOOKED_DATE = "custom.cf_JsJZIVh7QDcFQBXr4cTRBxf1AkREpLdsKiZB4AEJ8Xh"
FIELD_FUNNEL_NAME_DEAL = "custom.cf_xqDQE8fkPsWa0RNEve7hcaxKblCe6489XeZGRDzyPdX"

EXISTING_CUSTOMER_CUTOFF = "2026-01-01"
OUTPUT_FILE = os.environ.get("OUTPUT_FILE", "index.html")

# 0.7s between calls = ~85 req/min, safely under Close's ~100/min limit
API_THROTTLE = 0.7

# ─── API Helpers ─────────────────────────────────────────────────────────────

session = requests.Session()
session.auth = (CLOSE_API_KEY, "")
session.headers.update({"Content-Type": "application/json"})

_api_call_count = 0


def close_get(endpoint, params=None):
    """Sequential GET with global throttle and retry on 429."""
    global _api_call_count
    time.sleep(API_THROTTLE)
    url = f"{CLOSE_API_BASE}/{endpoint}"
    for attempt in range(5):
        resp = session.get(url, params=params or {}, timeout=30)
        _api_call_count += 1
        if resp.status_code == 429:
            wait = float(resp.headers.get("Retry-After", 5))
            print(f"   ⏳ Rate limited (attempt {attempt+1}), waiting {wait}s...")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.json()
    resp.raise_for_status()


# ─── Data Fetching ───────────────────────────────────────────────────────────

def fetch_meetings_in_range(start_date, end_date):
    """Fetch all meetings in [start_date, end_date) with pagination."""
    all_meetings = []
    skip = 0
    limit = 100

    start_iso = datetime.combine(start_date, datetime.min.time(), tzinfo=PACIFIC).astimezone(UTC).isoformat()
    end_iso = datetime.combine(end_date, datetime.min.time(), tzinfo=PACIFIC).astimezone(UTC).isoformat()

    while True:
        data = close_get("activity/meeting", {
            "_skip": skip,
            "_limit": limit,
            "date_start__gte": start_iso,
            "date_start__lt": end_iso,
        })
        meetings = data.get("data", [])
        all_meetings.extend(meetings)
        if not data.get("has_more", False):
            break
        skip += limit

    return all_meetings


def fetch_leads_sequential(lead_ids):
    """Fetch leads one at a time with progress logging."""
    cache = {}
    total = len(lead_ids)
    for i, lid in enumerate(lead_ids, 1):
        if i % 20 == 0 or i == total:
            print(f"   ... {i}/{total} leads fetched")
        try:
            cache[lid] = close_get(f"lead/{lid}")
        except requests.HTTPError as e:
            print(f"  ⚠ Could not fetch lead {lid}: {e}", file=sys.stderr)
            cache[lid] = None
    return cache


def build_existing_customer_set_bulk(candidate_lead_ids):
    """
    BULK approach: Fetch ALL completed meetings before the cutoff in one
    paginated scan, then intersect with our candidate lead IDs.

    This replaces ~N individual API calls with ~5-15 paginated calls,
    regardless of how many leads we're checking.
    """
    cutoff_iso = datetime.combine(
        date.fromisoformat(EXISTING_CUSTOMER_CUTOFF),
        datetime.min.time(),
        tzinfo=UTC
    ).isoformat()

    candidate_set = set(candidate_lead_ids)
    existing = set()
    skip = 0
    limit = 100
    total_scanned = 0

    while True:
        data = close_get("activity/meeting", {
            "_skip": skip,
            "_limit": limit,
            "status": "completed",
            "date_start__lt": cutoff_iso,
        })
        meetings = data.get("data", [])
        total_scanned += len(meetings)

        for m in meetings:
            lid = m.get("lead_id")
            if lid and lid in candidate_set:
                existing.add(lid)

        if total_scanned % 500 == 0 or not data.get("has_more", False):
            print(f"   ... scanned {total_scanned} historical meetings, found {len(existing)} existing customers so far")

        if not data.get("has_more", False):
            break
        skip += limit

    return existing


# ─── Exclusion Logic ─────────────────────────────────────────────────────────

def is_excluded_by_title(title):
    if not title:
        return False
    return bool(EXCLUDED_TITLE_RE.search(title))


def is_excluded_by_user(user_id, users_list):
    if user_id in EXCLUDED_USER_IDS:
        return True
    if users_list:
        for uid in users_list:
            if uid in EXCLUDED_USER_IDS:
                return True
    return False


def get_meeting_date_pacific(meeting):
    starts_at = meeting.get("starts_at") or meeting.get("activity_at") or meeting.get("date_start")
    if not starts_at or not isinstance(starts_at, str):
        return None
    dt = datetime.fromisoformat(starts_at.replace("Z", "+00:00"))
    return dt.astimezone(PACIFIC).date()


def is_reschedule(lead_data, meeting_date):
    fcbd_raw = lead_data.get(FIELD_FIRST_CALL_BOOKED_DATE)
    if not fcbd_raw:
        return False
    try:
        fcbd = date.fromisoformat(str(fcbd_raw))
    except (ValueError, TypeError):
        return False
    return fcbd < meeting_date


# ─── Main Pipeline ───────────────────────────────────────────────────────────

def run_pipeline():
    global _api_call_count
    _api_call_count = 0

    today = datetime.now(PACIFIC).date()
    end_date = today + timedelta(days=10)
    print(f"📅 Date range: {today} to {end_date} (Pacific)")

    # Step 1: Fetch all meetings in range
    print("📥 Step 1/6: Fetching meetings...")
    raw_meetings = fetch_meetings_in_range(today, end_date)
    print(f"   Found {len(raw_meetings)} total meetings")

    # Step 2: Apply all local exclusions (no API calls)
    print("🔍 Step 2/6: Applying local exclusions...")
    meetings = []
    for m in raw_meetings:
        title = m.get("title", "")
        status = m.get("status", "")

        if is_excluded_by_title(title):
            continue
        if is_excluded_by_user(m.get("user_id", ""), m.get("users", [])):
            continue
        if status in ("canceled", "declined-by-org", "declined-by-lead"):
            continue
        if not m.get("lead_id"):
            continue
        meeting_date = get_meeting_date_pacific(m)
        if not meeting_date:
            continue

        m["_meeting_date"] = meeting_date
        meetings.append(m)

    print(f"   {len(raw_meetings)} → {len(meetings)} after local exclusions")

    # Step 3: Fetch unique leads sequentially (the main API cost)
    unique_lead_ids = list(set(m["lead_id"] for m in meetings))
    print(f"📥 Step 3/6: Fetching {len(unique_lead_ids)} unique leads...")
    lead_cache = fetch_leads_sequential(unique_lead_ids)

    # Step 4: Apply reschedule exclusion (no API calls, uses cached data)
    print("🔍 Step 4/6: Applying reschedule exclusion...")
    pre = len(meetings)
    meetings = [
        m for m in meetings
        if lead_cache.get(m["lead_id"]) and not is_reschedule(lead_cache[m["lead_id"]], m["_meeting_date"])
    ]
    print(f"   {pre} → {len(meetings)} (removed {pre - len(meetings)} reschedules)")

    # Step 5: Bulk existing-customer check (single paginated scan)
    remaining_lead_ids = list(set(m["lead_id"] for m in meetings))
    print(f"📥 Step 5/6: Bulk scanning for existing customers ({len(remaining_lead_ids)} candidates)...")
    existing_customers = build_existing_customer_set_bulk(remaining_lead_ids)
    print(f"   Found {len(existing_customers)} existing customers to exclude")

    # Step 6: Build final results
    print("📊 Step 6/6: Building dashboard data...")
    valid_meetings = []
    for m in meetings:
        lead_id = m["lead_id"]
        if lead_id in existing_customers:
            continue
        lead_data = lead_cache.get(lead_id)
        if not lead_data:
            continue

        funnel = lead_data.get(FIELD_FUNNEL_NAME_DEAL) or "Unknown"
        valid_meetings.append({
            "date": m["_meeting_date"],
            "title": m.get("title", ""),
            "lead_id": lead_id,
            "lead_name": lead_data.get("display_name") or lead_data.get("name", "Unknown"),
            "funnel": funnel,
        })

    print(f"   ✅ {len(valid_meetings)} valid first meetings")

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

    print(f"   📡 Total API calls made: {_api_call_count}")

    return {
        "dates": dates,
        "daily_data": daily_data,
        "funnels": funnels_sorted,
        "valid_meetings": valid_meetings,
        "last_updated": now_pacific.strftime("%b %d, %Y at %I:%M %p %Z"),
    }


# ─── HTML Generation ────────────────────────────────────────────────────────

def generate_html(data):
    dates = data["dates"]
    daily = data["daily_data"]
    funnels = data["funnels"]
    last_updated = data["last_updated"]

    labels_json = json.dumps([d.strftime("%a %m/%d") for d in dates])
    booked_json = json.dumps([daily[d]["booked"] for d in dates])
    capacity_json = json.dumps([daily[d]["capacity"] for d in dates])
    util_pcts = []
    for d in dates:
        cap = daily[d]["capacity"]
        bk = daily[d]["booked"]
        util_pcts.append(round(bk / cap * 100, 1) if cap > 0 else 0)
    util_json = json.dumps(util_pcts)

    funnel_rows_html = ""
    for funnel in funnels:
        cells = ""
        row_total = 0
        for d in dates:
            count = daily[d]["funnels"].get(funnel, 0)
            row_total += count
            cell_class = "cell-zero" if count == 0 else "cell-active"
            cells += f'<td class="{cell_class}">{count}</td>'
        funnel_rows_html += f"""
        <tr>
            <td class="funnel-name">{funnel}</td>
            {cells}
            <td class="cell-total">{row_total}</td>
        </tr>"""

    totals_cells = ""
    grand_total = 0
    for d in dates:
        t = daily[d]["booked"]
        grand_total += t
        totals_cells += f'<td class="cell-total">{t}</td>'
    totals_row = f"""
    <tr class="totals-row">
        <td class="funnel-name">TOTAL</td>
        {totals_cells}
        <td class="cell-total">{grand_total}</td>
    </tr>"""

    cap_cells = ""
    for d in dates:
        cap_cells += f'<td class="cell-cap">{daily[d]["capacity"]}</td>'
    cap_row = f"""
    <tr class="cap-row">
        <td class="funnel-name">Capacity</td>
        {cap_cells}
        <td class="cell-cap">—</td>
    </tr>"""

    util_cells = ""
    for d in dates:
        cap = daily[d]["capacity"]
        bk = daily[d]["booked"]
        pct = round(bk / cap * 100, 1) if cap > 0 else 0
        color_class = "util-low" if pct < 50 else ("util-mid" if pct < 80 else "util-high")
        util_cells += f'<td class="{color_class}">{pct}%</td>'
    util_row = f"""
    <tr class="util-row">
        <td class="funnel-name">Utilization</td>
        {util_cells}
        <td class="cell-cap">—</td>
    </tr>"""

    date_headers = ""
    for d in dates:
        day_label = d.strftime("%a<br>%m/%d")
        date_headers += f"<th>{day_label}</th>"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Call Capacity Dashboard</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.7/chart.umd.min.js"></script>
<style>
  @import url('https://fonts.googleapis.com/css2?family=DM+Sans:ital,opsz,wght@0,9..40,300;0,9..40,500;0,9..40,700;1,9..40,400&family=JetBrains+Mono:wght@400;500&display=swap');

  :root {{
    --bg: #0b0f1a;
    --surface: #121829;
    --surface-2: #1a2238;
    --border: #232d45;
    --text: #e2e8f0;
    --text-dim: #8494b2;
    --accent: #6366f1;
    --accent-light: #818cf8;
    --green: #22c55e;
    --amber: #f59e0b;
    --red: #ef4444;
    --cyan: #06b6d4;
  }}

  * {{ margin: 0; padding: 0; box-sizing: border-box; }}

  body {{
    font-family: 'DM Sans', -apple-system, sans-serif;
    background: var(--bg);
    color: var(--text);
    min-height: 100vh;
    padding: 2rem;
  }}

  .dashboard {{
    max-width: 1280px;
    margin: 0 auto;
  }}

  .header {{
    display: flex;
    justify-content: space-between;
    align-items: flex-end;
    margin-bottom: 2rem;
    padding-bottom: 1.5rem;
    border-bottom: 1px solid var(--border);
  }}
  .header h1 {{
    font-size: 1.75rem;
    font-weight: 700;
    letter-spacing: -0.025em;
    background: linear-gradient(135deg, var(--accent-light), var(--cyan));
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
  }}
  .header .subtitle {{
    font-size: 0.85rem;
    color: var(--text-dim);
    margin-top: 0.25rem;
  }}
  .updated {{
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.75rem;
    color: var(--text-dim);
    text-align: right;
  }}
  .updated .pulse {{
    display: inline-block;
    width: 6px; height: 6px;
    background: var(--green);
    border-radius: 50%;
    margin-right: 6px;
    animation: pulse 2s infinite;
  }}
  @keyframes pulse {{
    0%, 100% {{ opacity: 1; }}
    50% {{ opacity: 0.3; }}
  }}

  .summary-row {{
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 1rem;
    margin-bottom: 2rem;
  }}
  .card {{
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 1.25rem;
  }}
  .card .label {{
    font-size: 0.75rem;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: var(--text-dim);
    margin-bottom: 0.5rem;
  }}
  .card .value {{
    font-size: 1.75rem;
    font-weight: 700;
    font-family: 'JetBrains Mono', monospace;
  }}
  .card .value.green {{ color: var(--green); }}
  .card .value.amber {{ color: var(--amber); }}
  .card .value.red   {{ color: var(--red); }}
  .card .value.cyan  {{ color: var(--cyan); }}

  .chart-container {{
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 1.5rem;
    margin-bottom: 2rem;
  }}
  .chart-container h2 {{
    font-size: 1rem;
    font-weight: 500;
    margin-bottom: 1rem;
    color: var(--text-dim);
  }}
  .chart-wrap {{
    position: relative;
    height: 320px;
  }}

  .table-container {{
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 1.5rem;
    overflow-x: auto;
  }}
  .table-container h2 {{
    font-size: 1rem;
    font-weight: 500;
    margin-bottom: 1rem;
    color: var(--text-dim);
  }}
  table {{
    width: 100%;
    border-collapse: collapse;
    font-size: 0.85rem;
  }}
  th {{
    padding: 0.6rem 0.75rem;
    text-align: center;
    font-weight: 500;
    color: var(--text-dim);
    border-bottom: 2px solid var(--border);
    white-space: nowrap;
    font-size: 0.8rem;
  }}
  th:first-child {{
    text-align: left;
    min-width: 180px;
  }}
  td {{
    padding: 0.55rem 0.75rem;
    text-align: center;
    border-bottom: 1px solid var(--border);
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.8rem;
  }}
  .funnel-name {{
    text-align: left !important;
    font-family: 'DM Sans', sans-serif;
    font-weight: 500;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    max-width: 200px;
  }}
  .cell-zero {{ color: var(--text-dim); opacity: 0.4; }}
  .cell-active {{ color: var(--accent-light); font-weight: 500; }}
  .cell-total {{ font-weight: 700; color: var(--text); }}
  .cell-cap {{ color: var(--text-dim); }}
  .totals-row td {{
    border-top: 2px solid var(--accent);
    border-bottom: 1px solid var(--border);
    background: rgba(99, 102, 241, 0.06);
  }}
  .cap-row td {{ color: var(--text-dim); }}
  .util-row td {{ font-weight: 700; }}
  .util-low  {{ color: var(--green); }}
  .util-mid  {{ color: var(--amber); }}
  .util-high {{ color: var(--red); }}

  @media (max-width: 768px) {{
    body {{ padding: 1rem; }}
    .summary-row {{ grid-template-columns: repeat(2, 1fr); }}
    .header {{ flex-direction: column; align-items: flex-start; gap: 0.5rem; }}
  }}
</style>
</head>
<body>
<div class="dashboard">

  <div class="header">
    <div>
      <h1>Call Capacity Dashboard</h1>
      <div class="subtitle">10-Day Rolling Window — First Strategy Call Bookings vs. Capacity</div>
    </div>
    <div class="updated">
      <span class="pulse"></span> Last updated<br>
      <strong>{last_updated}</strong>
    </div>
  </div>

  <div class="summary-row" id="summaryCards"></div>

  <div class="chart-container">
    <h2>Daily Bookings vs. Capacity</h2>
    <div class="chart-wrap">
      <canvas id="capacityChart"></canvas>
    </div>
  </div>

  <div class="table-container">
    <h2>Funnel Breakdown</h2>
    <table>
      <thead>
        <tr>
          <th>Funnel</th>
          {date_headers}
          <th>Total</th>
        </tr>
      </thead>
      <tbody>
        {funnel_rows_html}
        {totals_row}
        {cap_row}
        {util_row}
      </tbody>
    </table>
  </div>

</div>

<script>
const labels   = {labels_json};
const booked   = {booked_json};
const capacity = {capacity_json};
const util     = {util_json};

const totalBooked   = booked.reduce((a,b) => a+b, 0);
const totalCapacity = capacity.reduce((a,b) => a+b, 0);
const avgUtil       = totalCapacity > 0 ? (totalBooked / totalCapacity * 100).toFixed(1) : 0;
const peakUtil      = Math.max(...util).toFixed(1);

const cards = [
  {{ label: 'Total Booked', value: totalBooked, cls: 'cyan' }},
  {{ label: 'Total Capacity', value: totalCapacity, cls: '' }},
  {{ label: 'Avg Utilization', value: avgUtil + '%', cls: parseFloat(avgUtil) >= 80 ? 'red' : parseFloat(avgUtil) >= 50 ? 'amber' : 'green' }},
  {{ label: 'Peak Utilization', value: peakUtil + '%', cls: parseFloat(peakUtil) >= 80 ? 'red' : parseFloat(peakUtil) >= 50 ? 'amber' : 'green' }},
];

const cardsContainer = document.getElementById('summaryCards');
cards.forEach(c => {{
  cardsContainer.innerHTML += `
    <div class="card">
      <div class="label">${{c.label}}</div>
      <div class="value ${{c.cls}}">${{c.value}}</div>
    </div>`;
}});

const ctx = document.getElementById('capacityChart').getContext('2d');
new Chart(ctx, {{
  type: 'bar',
  data: {{
    labels: labels,
    datasets: [
      {{
        label: 'Booked',
        data: booked,
        backgroundColor: booked.map((b, i) => {{
          const pct = capacity[i] > 0 ? b / capacity[i] : 0;
          return pct >= 0.8 ? 'rgba(239,68,68,0.75)' : pct >= 0.5 ? 'rgba(245,158,11,0.75)' : 'rgba(34,197,94,0.75)';
        }}),
        borderRadius: 6,
        barPercentage: 0.6,
        order: 2,
      }},
      {{
        label: 'Capacity',
        data: capacity,
        type: 'line',
        borderColor: 'rgba(99,102,241,0.6)',
        borderWidth: 2,
        borderDash: [6, 4],
        pointBackgroundColor: 'rgba(99,102,241,0.8)',
        pointRadius: 4,
        fill: false,
        order: 1,
      }}
    ]
  }},
  options: {{
    responsive: true,
    maintainAspectRatio: false,
    interaction: {{ mode: 'index', intersect: false }},
    plugins: {{
      legend: {{
        labels: {{ color: '#8494b2', font: {{ family: "'DM Sans', sans-serif" }} }}
      }},
      tooltip: {{
        callbacks: {{
          afterBody: function(context) {{
            const i = context[0].dataIndex;
            const pct = capacity[i] > 0 ? (booked[i] / capacity[i] * 100).toFixed(1) : 0;
            return 'Utilization: ' + pct + '%';
          }}
        }}
      }}
    }},
    scales: {{
      x: {{
        ticks: {{ color: '#8494b2', font: {{ family: "'DM Sans', sans-serif", size: 11 }} }},
        grid: {{ color: 'rgba(35,45,69,0.5)' }}
      }},
      y: {{
        beginAtZero: true,
        ticks: {{ color: '#8494b2', font: {{ family: "'JetBrains Mono', monospace", size: 11 }}, stepSize: 10 }},
        grid: {{ color: 'rgba(35,45,69,0.5)' }}
      }}
    }}
  }}
}});
</script>
</body>
</html>"""

    return html


# ─── Entry Point ─────────────────────────────────────────────────────────────

def main():
    if not CLOSE_API_KEY:
        print("❌ Error: CLOSE_API_KEY environment variable is not set.", file=sys.stderr)
        sys.exit(1)

    start_time = time.time()
    print("🚀 Starting Call Capacity Dashboard update (v5)...")
    data = run_pipeline()
    html = generate_html(data)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(html)

    elapsed = time.time() - start_time
    print(f"\n✅ Dashboard written to {OUTPUT_FILE}")
    print(f"   {len(data['valid_meetings'])} first meetings across {len(data['funnels'])} funnels")
    print(f"   ⏱ Completed in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
