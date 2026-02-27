#!/usr/bin/env python3
"""
Call Capacity Dashboard Generator
Fetches first strategy call bookings from Close CRM for a 10-day rolling window,
applies exclusion rules, and generates a self-contained HTML dashboard.
"""

import os
import sys
import json
import re
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
import requests

# ─── Configuration ───────────────────────────────────────────────────────────

CLOSE_API_KEY = os.environ.get("CLOSE_API_KEY", "")
CLOSE_API_BASE = "https://api.close.com/api/v1"
PACIFIC = ZoneInfo("America/Los_Angeles")
UTC = ZoneInfo("UTC")

# Capacity per day-of-week (Mon=0 ... Sun=6)
CAPACITY = {0: 44, 1: 47, 2: 47, 3: 47, 4: 47, 5: 4, 6: 0}

# User IDs to exclude (setter/confirmation calls)
EXCLUDED_USER_IDS = {
    "user_EmhqCmaHERTfgfWnPADiLGEqQw3ENvRYd3u1VEmblIp",  # Kristin Nelson
    "user_4sfuKGMbv0LQZ4hpS8ipASv406kKTSNP5Xx79jOwSqM",  # Spencer Reynolds
}

# Title patterns to exclude (case-insensitive)
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

# Close CRM custom field keys
FIELD_FIRST_CALL_BOOKED_DATE = "custom.cf_JsJZIVh7QDcFQBXr4cTRBxf1AkREpLdsKiZB4AEJ8Xh"
FIELD_FUNNEL_NAME_DEAL = "custom.cf_xqDQE8fkPsWa0RNEve7hcaxKblCe6489XeZGRDzyPdX"

# Existing-customer cutoff: completed meetings before this date = existing customer
EXISTING_CUSTOMER_CUTOFF = "2026-01-01"

OUTPUT_FILE = os.environ.get("OUTPUT_FILE", "index.html")


# ─── API Helpers ─────────────────────────────────────────────────────────────

def close_get(endpoint, params=None):
    """Make an authenticated GET request to the Close API."""
    url = f"{CLOSE_API_BASE}/{endpoint}"
    resp = requests.get(url, auth=(CLOSE_API_KEY, ""), params=params or {}, timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_meetings_in_range(start_date, end_date):
    """
    Fetch all meetings from Close whose start time falls within [start_date, end_date).
    Uses the activity/meeting endpoint with date filtering and pagination.
    """
    all_meetings = []
    skip = 0
    limit = 100

    # Convert dates to ISO datetime strings in UTC
    start_iso = datetime.combine(start_date, datetime.min.time(), tzinfo=PACIFIC).astimezone(UTC).isoformat()
    end_iso = datetime.combine(end_date, datetime.min.time(), tzinfo=PACIFIC).astimezone(UTC).isoformat()

    while True:
        params = {
            "_skip": skip,
            "_limit": limit,
            "date_start__gte": start_iso,
            "date_start__lt": end_iso,
        }
        data = close_get("activity/meeting", params)
        meetings = data.get("data", [])
        all_meetings.extend(meetings)

        if not data.get("has_more", False):
            break
        skip += limit

    return all_meetings


def fetch_lead(lead_id, cache):
    """Fetch a lead by ID, using a cache to avoid redundant calls."""
    if lead_id in cache:
        return cache[lead_id]
    try:
        lead = close_get(f"lead/{lead_id}")
        cache[lead_id] = lead
    except requests.HTTPError as e:
        print(f"  ⚠ Could not fetch lead {lead_id}: {e}", file=sys.stderr)
        cache[lead_id] = None
    return cache[lead_id]


def lead_has_completed_meetings_before(lead_id, cutoff_iso):
    """
    Check if a lead has any completed meetings before the cutoff date.
    Returns True if the lead is an existing customer.
    """
    params = {
        "lead_id": lead_id,
        "_limit": 1,
        "status": "completed",
        "date_start__lt": cutoff_iso,
    }
    try:
        data = close_get("activity/meeting", params)
        return len(data.get("data", [])) > 0
    except requests.HTTPError:
        return False


# ─── Exclusion Logic ─────────────────────────────────────────────────────────

def is_excluded_by_title(title):
    """Check if meeting title matches any exclusion pattern."""
    if not title:
        return False
    return bool(EXCLUDED_TITLE_RE.search(title))


def is_excluded_by_user(user_id, users_list):
    """Check if the meeting is assigned to an excluded user."""
    # Check primary user_id
    if user_id in EXCLUDED_USER_IDS:
        return True
    # Also check the users list (attendees)
    if users_list:
        for uid in users_list:
            if uid in EXCLUDED_USER_IDS:
                return True
    return False


def get_meeting_date_pacific(meeting):
    """Extract the meeting's date in Pacific time."""
    starts_at = meeting.get("starts_at") or meeting.get("activity_at") or meeting.get("date_start")
    if not starts_at:
        return None
    if isinstance(starts_at, str):
        dt = datetime.fromisoformat(starts_at.replace("Z", "+00:00"))
    else:
        return None
    return dt.astimezone(PACIFIC).date()


def is_reschedule(lead_data, meeting_date):
    """
    Exclude if the lead's First Call Booked Date is before the meeting date.
    This means the first call was originally booked for an earlier date,
    making this meeting a reschedule.
    """
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
    """Execute the full data pipeline and return structured dashboard data."""
    today = datetime.now(PACIFIC).date()
    end_date = today + timedelta(days=10)

    print(f"📅 Date range: {today} to {end_date} (Pacific)")

    # Step 1: Fetch all meetings in the 10-day window
    print("📥 Fetching meetings from Close CRM...")
    raw_meetings = fetch_meetings_in_range(today, end_date)
    print(f"   Found {len(raw_meetings)} total meetings")

    # Step 2: Apply title exclusions
    meetings = []
    for m in raw_meetings:
        title = m.get("title", "")
        if is_excluded_by_title(title):
            continue
        meetings.append(m)
    print(f"   After title exclusions: {len(meetings)}")

    # Step 3: Apply user exclusions (Kristin/Spencer setter calls)
    filtered = []
    for m in meetings:
        if is_excluded_by_user(m.get("user_id", ""), m.get("users", [])):
            continue
        filtered.append(m)
    meetings = filtered
    print(f"   After user exclusions: {len(meetings)}")

    # Step 4: Exclude canceled/declined meetings
    filtered = []
    for m in meetings:
        status = m.get("status", "")
        if status in ("canceled", "declined-by-org", "declined-by-lead"):
            continue
        filtered.append(m)
    meetings = filtered
    print(f"   After status exclusions: {len(meetings)}")

    # Step 5: Fetch lead data and apply lead-level exclusions
    lead_cache = {}
    cutoff_iso = datetime.combine(
        date.fromisoformat(EXISTING_CUSTOMER_CUTOFF),
        datetime.min.time(),
        tzinfo=UTC
    ).isoformat()

    # Pre-fetch unique leads
    unique_lead_ids = set(m.get("lead_id") for m in meetings if m.get("lead_id"))
    print(f"   Fetching data for {len(unique_lead_ids)} unique leads...")

    # Cache to track existing-customer status per lead
    existing_customer_cache = {}

    valid_meetings = []
    for m in meetings:
        lead_id = m.get("lead_id")
        if not lead_id:
            continue

        meeting_date = get_meeting_date_pacific(m)
        if not meeting_date:
            continue

        # Fetch lead
        lead_data = fetch_lead(lead_id, lead_cache)
        if not lead_data:
            continue

        # Exclusion: reschedule check (FCBD predates meeting date)
        if is_reschedule(lead_data, meeting_date):
            continue

        # Exclusion: existing customer (completed meetings before 2026)
        if lead_id not in existing_customer_cache:
            existing_customer_cache[lead_id] = lead_has_completed_meetings_before(lead_id, cutoff_iso)
        if existing_customer_cache[lead_id]:
            continue

        # Get funnel name
        funnel = lead_data.get(FIELD_FUNNEL_NAME_DEAL) or "Unknown"

        valid_meetings.append({
            "date": meeting_date,
            "title": m.get("title", ""),
            "lead_id": lead_id,
            "lead_name": lead_data.get("display_name") or lead_data.get("name", "Unknown"),
            "funnel": funnel,
            "user_id": m.get("user_id", ""),
        })

    print(f"   ✅ Valid first meetings: {len(valid_meetings)}")

    # Step 6: Build daily aggregations
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

    return {
        "dates": dates,
        "daily_data": daily_data,
        "funnels": funnels_sorted,
        "valid_meetings": valid_meetings,
        "last_updated": now_pacific.strftime("%b %d, %Y at %I:%M %p %Z"),
        "last_updated_iso": now_pacific.isoformat(),
    }


# ─── HTML Generation ────────────────────────────────────────────────────────

def generate_html(data):
    """Generate a self-contained HTML dashboard from pipeline data."""
    dates = data["dates"]
    daily = data["daily_data"]
    funnels = data["funnels"]
    last_updated = data["last_updated"]
    last_updated_iso = data["last_updated_iso"]

    # Prepare chart data
    labels_json = json.dumps([d.strftime("%a %m/%d") for d in dates])
    booked_json = json.dumps([daily[d]["booked"] for d in dates])
    capacity_json = json.dumps([daily[d]["capacity"] for d in dates])
    util_pcts = []
    for d in dates:
        cap = daily[d]["capacity"]
        bk = daily[d]["booked"]
        util_pcts.append(round(bk / cap * 100, 1) if cap > 0 else 0)
    util_json = json.dumps(util_pcts)

    # Build funnel breakdown table rows
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

    # Totals row
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

    # Capacity row
    cap_cells = ""
    for d in dates:
        cap_cells += f'<td class="cell-cap">{daily[d]["capacity"]}</td>'
    cap_row = f"""
    <tr class="cap-row">
        <td class="funnel-name">Capacity</td>
        {cap_cells}
        <td class="cell-cap">—</td>
    </tr>"""

    # Utilization row
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

    # Date headers for table
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

  /* ── Header ── */
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

  /* ── Summary Cards ── */
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

  /* ── Chart ── */
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

  /* ── Table ── */
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
  .cell-zero {{
    color: var(--text-dim);
    opacity: 0.4;
  }}
  .cell-active {{
    color: var(--accent-light);
    font-weight: 500;
  }}
  .cell-total {{
    font-weight: 700;
    color: var(--text);
  }}
  .cell-cap {{
    color: var(--text-dim);
  }}
  .totals-row td {{
    border-top: 2px solid var(--accent);
    border-bottom: 1px solid var(--border);
    background: rgba(99, 102, 241, 0.06);
  }}
  .cap-row td {{
    color: var(--text-dim);
  }}
  .util-row td {{
    font-weight: 700;
  }}
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

  <!-- Header -->
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

  <!-- Summary Cards -->
  <div class="summary-row" id="summaryCards"></div>

  <!-- Chart -->
  <div class="chart-container">
    <h2>Daily Bookings vs. Capacity</h2>
    <div class="chart-wrap">
      <canvas id="capacityChart"></canvas>
    </div>
  </div>

  <!-- Funnel Breakdown Table -->
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

// Summary cards
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

// Chart
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

    print("🚀 Starting Call Capacity Dashboard update...")
    data = run_pipeline()
    html = generate_html(data)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"✅ Dashboard written to {OUTPUT_FILE}")
    print(f"   {len(data['valid_meetings'])} first meetings across {len(data['funnels'])} funnels")


if __name__ == "__main__":
    main()
