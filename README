# Call Capacity Dashboard — Setup Guide

Automated 10-day rolling dashboard showing first strategy call bookings vs. capacity, broken down by funnel. Pulls live data from Close CRM every 15 minutes.

---

## Architecture

```
cron-job.org (every 15 min)
    │
    ▼  POST webhook (workflow_dispatch)
GitHub Actions
    │  1. Runs update_dashboard.py
    │  2. Fetches meetings from Close API
    │  3. Applies exclusion rules
    │  4. Generates index.html
    │  5. Commits & pushes to repo
    ▼
GitHub Pages serves index.html
```

---

## Step 1 — Create the GitHub Repo

1. Create a **new repository** on GitHub (e.g., `call-capacity-dashboard`)
   - Set it to **Public** (required for free GitHub Pages), or Private if you have GitHub Pro/Team
2. Clone it locally:
   ```bash
   git clone https://github.com/YOUR_USERNAME/call-capacity-dashboard.git
   cd call-capacity-dashboard
   ```
3. Copy the files into the repo:
   ```
   call-capacity-dashboard/
   ├── .github/
   │   └── workflows/
   │       └── update-dashboard.yml
   ├── update_dashboard.py
   └── index.html          ← (auto-generated on first run)
   ```
   > **Important**: The workflow file you received is saved as `dot-github/workflows/update-dashboard.yml`. Rename the folder to `.github` when placing it in your repo.

4. Push:
   ```bash
   git add -A
   git commit -m "Initial setup"
   git push origin main
   ```

---

## Step 2 — Add the Close API Key as a Secret

1. In your GitHub repo, go to **Settings → Secrets and variables → Actions**
2. Click **New repository secret**
3. Name: `CLOSE_API_KEY`
4. Value: Your Close API key (find it in Close under **Settings → API Keys**)
5. Click **Add secret**

---

## Step 3 — Enable GitHub Pages

1. Go to **Settings → Pages**
2. Under "Source", select **Deploy from a branch**
3. Branch: `main`, folder: `/ (root)`
4. Click **Save**
5. Your dashboard will be available at:
   ```
   https://YOUR_USERNAME.github.io/call-capacity-dashboard/
   ```

---

## Step 4 — Test the Workflow Manually

1. Go to the **Actions** tab in your repo
2. Click **"Update Call Capacity Dashboard"** in the left sidebar
3. Click **"Run workflow"** → **"Run workflow"**
4. Watch the run. Once it completes, `index.html` should be committed and your GitHub Pages URL should show the dashboard.

---

## Step 5 — Set Up cron-job.org for 15-Minute Updates

1. **Create a GitHub Personal Access Token (PAT)**:
   - Go to [github.com/settings/tokens](https://github.com/settings/tokens)
   - Click **"Generate new token (classic)"**
   - Scopes: check **`repo`** (full control of private repos) — or just **`public_repo`** if the repo is public
   - Copy the token

2. **Create the cron job** at [cron-job.org](https://cron-job.org):
   - **URL**:
     ```
     https://api.github.com/repos/YOUR_USERNAME/call-capacity-dashboard/actions/workflows/update-dashboard.yml/dispatches
     ```
   - **Schedule**: Every 15 minutes (`*/15 * * * *`)
   - **Request Method**: `POST`
   - **Headers** (add these under "Advanced"):
     ```
     Authorization: Bearer YOUR_GITHUB_PAT
     Accept: application/vnd.github.v3+json
     Content-Type: application/json
     ```
   - **Request Body**:
     ```json
     {"ref": "main"}
     ```
   - **Enable** the job

3. **Verify**: After 15 minutes, check the Actions tab in your repo to confirm a new run was triggered.

---

## What the Exclusion Rules Do

| Rule | Purpose |
|------|---------|
| Title contains "Follow", "F/U", "Follow-Up", "Next Steps", "Rescheduled" | Removes follow-up meetings |
| Kristin Nelson or Spencer Reynolds meetings | Removes setter/confirmation calls (Vending Quick Discovery) |
| Title contains "Anthony's Q&A" | Removes Q&A sessions |
| Title is "test" or "Canceled" | Removes test/canceled entries |
| Meeting status is canceled/declined | Removes canceled meetings |
| Lead's First Call Booked Date < meeting date | Removes reschedules (first call was originally booked earlier) |
| Lead has completed meetings before 2026 | Removes existing customers |

---

## Customization

### Change Capacity Numbers
In `update_dashboard.py`, edit the `CAPACITY` dict (Mon=0, Sun=6):
```python
CAPACITY = {0: 44, 1: 47, 2: 47, 3: 47, 4: 47, 5: 4, 6: 0}
```

### Change the Rolling Window
Search for `timedelta(days=10)` and `range(10)` in the script to adjust.

### Add/Remove Title Exclusions
Edit the `EXCLUDED_TITLE_PATTERNS` list. These are Python regex patterns (case-insensitive).

### Add/Remove User Exclusions
Edit the `EXCLUDED_USER_IDS` set with Close user IDs.

---

## Troubleshooting

| Issue | Fix |
|-------|-----|
| Workflow fails with auth error | Verify `CLOSE_API_KEY` secret is set correctly in repo settings |
| cron-job.org gets 404 | Double-check the workflow filename matches the URL path |
| cron-job.org gets 401 | Regenerate your GitHub PAT and update the cron job header |
| Dashboard shows 0 meetings | Check that meetings exist in the 10-day window and aren't all excluded |
| GitHub Pages not updating | Confirm the commit pushed to `main`; Pages can take 1-2 min to deploy |
| "Rate limit" errors from Close | The script makes ~1 API call per unique lead. If you have 200+ leads in 10 days, you may hit rate limits. Add `time.sleep(0.25)` in the `fetch_lead` function. |

---

## File Reference

| File | Purpose |
|------|---------|
| `update_dashboard.py` | Python script: fetches Close data, applies rules, generates HTML |
| `.github/workflows/update-dashboard.yml` | GitHub Actions: runs the script, commits result |
| `index.html` | Auto-generated dashboard (served by GitHub Pages) |
