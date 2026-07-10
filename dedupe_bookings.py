#!/usr/bin/env python3
"""
dedupe_bookings.py — Duplicate booking / duplicate lead detector (NOTIFY-ONLY v1)

Reconciliation sweep (Option A). Instead of racing at write-time with a webhook,
this reads *settled* Calendly state on a short cron, groups all upcoming bookings by
invitee email, and flags any invitee holding 2+ upcoming meetings. Because it reads
final state rather than intercepting writes, it needs no locking and is fully
idempotent — every run re-checks itself.

v1 is NOTIFY-ONLY: it sends an email alert (with one-click Calendly cancel links so a
human can act), and it does NOT cancel anything automatically. Flip AUTO_CANCEL to True
later once you trust the signal.

Keep policy: EARLIEST booking is kept; later bookings are flagged (matches the incident
where Rep A booked first).

Runs via GitHub Actions (.github/workflows/dedupe-bookings.yml) on cron + workflow_dispatch.
Reuses the dashboard repo's existing secrets — no new infra:
  CLOSE_API_KEY, CALENDLY_API_KEY, GMAIL_APP_PASSWORD, EMAIL_FROM, EMAIL_TO
"""

import os
import sys
import html
import smtplib
import datetime as dt
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from collections import defaultdict

try:
    from zoneinfo import ZoneInfo  # py3.9+
except ImportError:  # pragma: no cover
    ZoneInfo = None

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
LOOKAHEAD_DAYS = 14          # how far forward to scan for duplicate upcoming meetings
KEEP_POLICY = "earliest"     # keep earliest booked, flag later ones (team decision)
AUTO_CANCEL = False          # v1 = notify only. True => cancel flagged bookings via API
SEND_ALL_CLEAR = False       # if True, email even when no duplicates are found
ALERT_MIN_SEVERITY = "LOW"   # "LOW" = alert on every cluster; "HIGH" = only high-confidence dups
REQUEST_TIMEOUT = 30

CALENDLY_BASE = "https://api.calendly.com"
CLOSE_BASE = "https://api.close.com/api/v1"
CLOSE_LEAD_URL = "https://app.close.com/lead/{lead_id}/"

PT = ZoneInfo("America/Los_Angeles") if ZoneInfo else None

# Lead Owner custom field (stores Close user IDs) — from project context doc.
CF_LEAD_OWNER = "cf_gOfS9pFwext58oberEegLyix8hZzeHrxhCZOVh3P3rd"

# Friendly labels for the team's known event types (project context doc).
EVENT_TYPE_LABELS = {
    "https://api.calendly.com/event_types/3acb4582-147a-4652-ad6b-5effe4a1b755": "Vendingpreneurs Consultation",
    "https://api.calendly.com/event_types/f1a11c05-d0c0-41b7-aaec-b60bf5d96f39": "Vending Accelerator Call",
}

# Close user_id -> display name, for showing the lead owner in alerts (project context doc).
NAME_BY_USER_ID = {
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
    "user_vyiPzY0qxbLwnW5Ubwae8vY2MLviPuozSTIsEKcyrFE": "Zac Clover",
    "user_pKEujUcHJfsEyI5lM6L56aXM2s5nNOU994JRjRSlAdA": "Chris Wanke",
    "user_Bov31jjnHhENBy8uWNTTL8KKax8VX7o6DugLzBYOHBG": "Lyle Hubbard",
    "user_WquWudQN7dghZsAPiNY80eJUmg1EadQg2UCQdvgbif7": "Kelly Schrader",
    "user_I0cHZ04mBXXBvbFcnwmsc2KrcMsLsKxqjW8DtJ783Hr": "Elvis Ellis",
    "user_5pAfnzGONQLUVLKqFQVpQ3570YV1gurVCTp1MMgfCDL": "John Kirk",
    "user_UpJb11fzX2TuFHf7fFyWpfXr84lg2Ui7i7p5CtQkIaW": "Cameron Caswell",
    "user_MrBLkl5wCqTm7QxHxPo2ydNV5KxMllg6YZDVc12Aqzj": "Jason Aaron",
    "user_ulI4pdlkBQGJpFBjSfdf3U2deAXQATVPSAurnbL80T9": "Bryan Barcus",
    "user_L0aaUNmM45X52HE7rj3VPWkxhahpoYobhDVAXamQMMD": "Steven Starnes",
}


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------
def log(msg):
    print(msg, flush=True)


def env(name, required=True):
    val = os.environ.get(name, "").strip()
    if required and not val:
        log(f"ERROR: missing required environment variable {name}")
        sys.exit(1)
    return val


def parse_iso(ts):
    """Parse Calendly RFC3339 timestamps into aware UTC datetimes."""
    if not ts:
        return None
    return dt.datetime.fromisoformat(ts.replace("Z", "+00:00"))


def fmt_pt(ts):
    """Format a UTC ISO timestamp as Pacific time for humans."""
    d = parse_iso(ts)
    if not d:
        return "?"
    if PT:
        d = d.astimezone(PT)
        return d.strftime("%a %-m/%-d %-I:%M %p %Z")
    return d.strftime("%a %m/%d %H:%M UTC")


def event_label(event_type_uri):
    return EVENT_TYPE_LABELS.get(event_type_uri, "Other event type")


# ---------------------------------------------------------------------------
# Calendly
# ---------------------------------------------------------------------------
class Calendly:
    def __init__(self, token):
        self.s = requests.Session()
        self.s.headers.update({
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        })

    def _get(self, url, params=None):
        r = self.s.get(url, params=params, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        return r.json()

    def org_uri(self):
        me = self._get(f"{CALENDLY_BASE}/users/me")
        return me["resource"]["current_organization"]

    def scheduled_events(self, org_uri, min_time, max_time):
        """All active scheduled events for the org within the window (paginated)."""
        events = []
        params = {
            "organization": org_uri,
            "status": "active",
            "min_start_time": min_time,
            "max_start_time": max_time,
            "count": 100,
        }
        url = f"{CALENDLY_BASE}/scheduled_events"
        while True:
            data = self._get(url, params=params)
            events.extend(data.get("collection", []))
            nxt = (data.get("pagination") or {}).get("next_page")
            if not nxt:
                break
            url, params = nxt, None  # next_page is a fully-formed URL
        return events

    def invitees(self, event_uri):
        """Active invitees for a scheduled event (paginated)."""
        out = []
        params = {"count": 100, "status": "active"}
        url = f"{event_uri}/invitees"
        while True:
            data = self._get(url, params=params)
            out.extend(data.get("collection", []))
            nxt = (data.get("pagination") or {}).get("next_page")
            if not nxt:
                break
            url, params = nxt, None
        return out

    def host_name(self, event):
        """Best-effort host display name from event_memberships."""
        for m in event.get("event_memberships", []) or []:
            if m.get("user_name"):
                return m["user_name"]
            if m.get("user_email"):
                return m["user_email"]
        return "Unknown host"

    def cancel(self, event_uri, reason):
        """Cancel a scheduled event (only used if AUTO_CANCEL is True)."""
        r = self.s.post(
            f"{event_uri}/cancellation",
            json={"reason": reason},
            timeout=REQUEST_TIMEOUT,
        )
        r.raise_for_status()
        return r.json()


# ---------------------------------------------------------------------------
# Close (best-effort enrichment)
# ---------------------------------------------------------------------------
class Close:
    def __init__(self, api_key):
        self.s = requests.Session()
        self.s.auth = (api_key, "")  # Close uses API key as basic-auth username

    def lead_by_email(self, email):
        """Return (lead_id, display_name, owner_name) or (None, None, None)."""
        try:
            r = self.s.get(
                f"{CLOSE_BASE}/lead/",
                params={"query": f'email_address:"{email}"', "_limit": 1},
                timeout=REQUEST_TIMEOUT,
            )
            r.raise_for_status()
            data = r.json().get("data", [])
            if not data:
                return None, None, None
            lead = data[0]
            lead_id = lead.get("id")
            name = lead.get("display_name") or lead.get("name")
            owner = self._owner_name(lead)
            return lead_id, name, owner
        except Exception as e:  # enrichment must never break the alert
            log(f"  (Close lookup failed for {email}: {e})")
            return None, None, None

    @staticmethod
    def _owner_name(lead):
        raw = lead.get(f"custom.{CF_LEAD_OWNER}")
        if raw is None:
            raw = (lead.get("custom") or {}).get(CF_LEAD_OWNER)
        if isinstance(raw, list):
            raw = raw[0] if raw else None
        if not raw:
            return None
        return NAME_BY_USER_ID.get(raw, raw)


# ---------------------------------------------------------------------------
# Core detection
# ---------------------------------------------------------------------------
def collect_bookings(cal, org_uri):
    """Return {email: [booking, ...]} for all active upcoming invitees."""
    now = dt.datetime.now(dt.timezone.utc)
    min_time = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    max_time = (now + dt.timedelta(days=LOOKAHEAD_DAYS)).strftime("%Y-%m-%dT%H:%M:%SZ")

    events = cal.scheduled_events(org_uri, min_time, max_time)
    log(f"Fetched {len(events)} active upcoming event(s) in the next {LOOKAHEAD_DAYS} days.")

    by_email = defaultdict(list)
    for ev in events:
        ev_uri = ev.get("uri")
        host = cal.host_name(ev)
        for inv in cal.invitees(ev_uri):
            email = (inv.get("email") or "").strip().lower()
            if not email:
                continue
            by_email[email].append({
                "email": email,
                "name": inv.get("name") or "",
                "event_uri": ev_uri,
                "event_type": ev.get("event_type"),
                "event_label": event_label(ev.get("event_type")),
                "start_time": ev.get("start_time"),
                "host": host,
                "booked_at": inv.get("created_at"),   # when this person booked
                "cancel_url": inv.get("cancel_url"),
                "reschedule_url": inv.get("reschedule_url"),
            })
    return by_email


def build_clusters(by_email):
    """Turn the email->bookings map into duplicate clusters (2+ upcoming)."""
    clusters = []
    for email, bookings in by_email.items():
        if len(bookings) < 2:
            continue
        # Keep earliest booked; flag the rest.
        bookings.sort(key=lambda b: b.get("booked_at") or "")
        keep, flagged = bookings[0], bookings[1:]

        # Severity: HIGH if any flagged booking collides on the same event type
        # or the exact same start time as the kept one (near-certain duplicate);
        # LOW otherwise (could be a legit follow-up on a different calendar).
        severity = "LOW"
        for f in flagged:
            same_type = f["event_type"] == keep["event_type"]
            same_slot = f["start_time"] == keep["start_time"]
            if same_type or same_slot:
                severity = "HIGH"
                break

        clusters.append({
            "email": email,
            "invitee": keep["name"] or (flagged[0]["name"] if flagged else ""),
            "keep": keep,
            "flagged": flagged,
            "severity": severity,
        })

    # HIGH first, then by invitee name.
    clusters.sort(key=lambda c: (c["severity"] != "HIGH", c["invitee"].lower()))
    return clusters


def enrich_with_close(close, clusters):
    for c in clusters:
        lead_id, lead_name, owner = close.lead_by_email(c["email"])
        c["lead_id"] = lead_id
        c["lead_url"] = CLOSE_LEAD_URL.format(lead_id=lead_id) if lead_id else None
        c["lead_name"] = lead_name
        c["owner"] = owner


# ---------------------------------------------------------------------------
# Email
# ---------------------------------------------------------------------------
def render_email(clusters):
    rows = []
    for c in clusters:
        keep = c["keep"]
        badge = "#c0392b" if c["severity"] == "HIGH" else "#b8860b"
        header = (
            f'<tr><td colspan="5" style="padding:14px 8px 4px;border-top:2px solid #eee;">'
            f'<span style="background:{badge};color:#fff;border-radius:3px;'
            f'padding:2px 7px;font-size:11px;font-weight:700;">{c["severity"]}</span> '
            f'<b>{html.escape(c["invitee"] or c["email"])}</b> '
            f'&lt;{html.escape(c["email"])}&gt;'
        )
        if c.get("lead_url"):
            owner = f' · owner {html.escape(c["owner"])}' if c.get("owner") else ""
            header += (
                f' — <a href="{c["lead_url"]}">Close lead</a>{owner}'
            )
        header += "</td></tr>"
        rows.append(header)

        # column headers
        rows.append(
            '<tr style="font-size:11px;color:#888;text-align:left;">'
            '<td style="padding:2px 8px;">role</td>'
            '<td style="padding:2px 8px;">meeting time</td>'
            '<td style="padding:2px 8px;">event type</td>'
            '<td style="padding:2px 8px;">host</td>'
            '<td style="padding:2px 8px;">booked at · action</td></tr>'
        )

        def render_row(b, role):
            role_color = "#1b5e1b" if role == "KEEP" else "#c0392b"
            action = ""
            if role == "REVIEW" and b.get("cancel_url"):
                action = f' · <a href="{b["cancel_url"]}">cancel</a>'
            return (
                f'<tr style="font-size:13px;">'
                f'<td style="padding:3px 8px;color:{role_color};font-weight:700;">{role}</td>'
                f'<td style="padding:3px 8px;">{html.escape(fmt_pt(b["start_time"]))}</td>'
                f'<td style="padding:3px 8px;">{html.escape(b["event_label"])}</td>'
                f'<td style="padding:3px 8px;">{html.escape(b["host"])}</td>'
                f'<td style="padding:3px 8px;color:#666;">{html.escape(fmt_pt(b["booked_at"]))}{action}</td>'
                f'</tr>'
            )

        rows.append(render_row(keep, "KEEP"))
        for f in c["flagged"]:
            rows.append(render_row(f, "REVIEW"))

    body = (
        '<div style="font-family:-apple-system,Segoe UI,Roboto,Arial,sans-serif;color:#222;">'
        f'<h2 style="margin:0 0 4px;">Duplicate booking alert</h2>'
        f'<p style="margin:0 0 12px;color:#666;">'
        f'{len(clusters)} invitee(s) hold 2+ upcoming meetings. '
        f'<b>KEEP</b> = earliest booking (kept). <b>REVIEW</b> = later booking(s) — '
        f'click <i>cancel</i> to remove. Nothing was canceled automatically.</p>'
        '<table style="border-collapse:collapse;width:100%;">'
        + "".join(rows) +
        '</table>'
        '<p style="margin:16px 0 0;font-size:11px;color:#aaa;">'
        'Generated by dedupe_bookings.py · notify-only mode</p>'
        '</div>'
    )
    return body


def send_email(subject, html_body):
    email_from = env("EMAIL_FROM")
    email_to_raw = env("EMAIL_TO")
    app_pw = env("GMAIL_APP_PASSWORD")
    recipients = [a.strip() for a in email_to_raw.split(",") if a.strip()]

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = email_from
    msg["To"] = ", ".join(recipients)
    msg.attach(MIMEText(html_body, "html"))

    with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=REQUEST_TIMEOUT) as server:
        server.login(email_from, app_pw)
        server.sendmail(email_from, recipients, msg.as_string())
    log(f"Alert email sent to {', '.join(recipients)}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    close_key = env("CLOSE_API_KEY")
    calendly_key = env("CALENDLY_API_KEY")

    cal = Calendly(calendly_key)
    close = Close(close_key)

    org_uri = cal.org_uri()
    by_email = collect_bookings(cal, org_uri)
    clusters = build_clusters(by_email)

    # Apply severity threshold.
    if ALERT_MIN_SEVERITY.upper() == "HIGH":
        clusters = [c for c in clusters if c["severity"] == "HIGH"]

    if not clusters:
        log("No duplicate bookings found. All clear.")
        if SEND_ALL_CLEAR:
            send_email(
                f"Duplicate booking check — all clear ({dt.date.today().isoformat()})",
                '<p style="font-family:sans-serif;">No duplicate upcoming bookings found.</p>',
            )
        return

    enrich_with_close(close, clusters)

    for c in clusters:
        log(f"[{c['severity']}] {c['invitee']} <{c['email']}> — "
            f"{len(c['flagged'])} later booking(s) flagged")

    if AUTO_CANCEL:
        for c in clusters:
            for f in c["flagged"]:
                try:
                    cal.cancel(f["event_uri"], "Automatic duplicate-booking cleanup")
                    log(f"  canceled duplicate for {c['email']} ({f['event_label']})")
                except Exception as e:
                    log(f"  FAILED to cancel for {c['email']}: {e}")

    high = sum(1 for c in clusters if c["severity"] == "HIGH")
    subject = (
        f"⚠️ Duplicate booking alert — {len(clusters)} invitee(s)"
        f"{f', {high} high-confidence' if high else ''} "
        f"({dt.date.today().isoformat()})"
    )
    send_email(subject, render_email(clusters))


if __name__ == "__main__":
    main()
