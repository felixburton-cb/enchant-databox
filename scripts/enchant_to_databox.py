import os
import time
import math
from collections import defaultdict
from datetime import datetime, timezone

import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

# ========= ENV =========
ENCHANT_SITE  = os.environ["ENCHANT_SITE"]   # e.g. "carebit"
ENCHANT_TOKEN = os.environ["ENCHANT_TOKEN"]
DATABOX_TOKEN = os.environ["DATABOX_TOKEN"]

BASE = f"https://{ENCHANT_SITE}.enchant.com/api/v1"

# Exclude any tag names you don’t want counted in top-tags
EXCLUDED_TAGS = set([
    # "Internal",
    # "Core Support",
    # "SU",
])

# ========= HTTP SESSION WITH RETRIES =========
session = requests.Session()
session.headers.update({
    "Authorization": f"Bearer {ENCHANT_TOKEN}",
    "User-Agent": "enchant-databox-gh-actions/1.0",
    "Accept": "application/json",
})
retries = Retry(
    total=6,
    backoff_factor=1.2,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST"],
)
adapter = HTTPAdapter(max_retries=retries)
session.mount("https://", adapter)
session.mount("http://", adapter)

# ========= HELPERS =========
def parse_iso(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None

def iso_z(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def month_floor(dt: datetime | None) -> datetime | None:
    if not dt:
        return None
    return datetime(dt.year, dt.month, 1, tzinfo=timezone.utc)

def month_range_2025():
    m = datetime(2025, 1, 1, tzinfo=timezone.utc)
    # up to the current month
    end = datetime.utcnow().replace(day=1, hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc)
    months = []
    while m <= end:
        months.append(m)
        if m.month == 12:
            m = datetime(m.year + 1, 1, 1, tzinfo=timezone.utc)
        else:
            m = datetime(m.year, m.month + 1, 1, tzinfo=timezone.utc)
    return months

# ========= ENCHANT FETCH =========
def fetch_closed_tickets_since_2025():
    """
    Generator over closed tickets, starting Jan 1, 2025, paginated (per_page=100).
    We embed 'labels' and restrict fields to keep payload small.
    """
    since = "2025-01-01T00:00:00Z"
    page = 1
    per_page = 100

    while True:
        params = {
            "state": "closed",
            "per_page": per_page,
            "page": page,
            "since_updated_at": since,
            "embed": "labels",
            "fields": "id,created_at,updated_at,labels",
            # Adding count=true increases rate limit cost; avoid for large pulls.
        }
        r = session.get(f"{BASE}/tickets", params=params, timeout=45)
        if r.status_code == 429:
            retry_after = int(r.headers.get("Retry-After", "2"))
            time.sleep(retry_after)
            continue
        r.raise_for_status()

        items = r.json()
        n = len(items)
        print(f"fetch page {page}: {n} items")
        if n == 0:
            break

        for t in items:
            yield t

        page += 1
        # small pause to be gentle
        time.sleep(0.05)

# ========= MAIN =========
def main():
    # Collections
    m_counts_created  = defaultdict(int)   # monthly by created_at (recommended for your chart)
    m_counts_updated  = defaultdict(int)   # monthly by updated_at (activity month)
    m_res_minutes_sum = defaultdict(float) # for avg resolution by updated month
    m_res_minutes_n   = defaultdict(int)
    res_minutes_all   = []

    resolved_total = 0
    tag_counts = {}

    # Fetch tickets
    for t in fetch_closed_tickets_since_2025():
        created_dt = parse_iso(t.get("created_at") or t.get("createdAt"))
        updated_dt = parse_iso(t.get("updated_at") or t.get("updatedAt"))

        resolved_total += 1

        # Year-level average: minutes from created -> updated (close/update time)
        if created_dt and updated_dt:
            minutes = (updated_dt - created_dt).total_seconds() / 60.0
            res_minutes_all.append(minutes)

        # Monthly counts by created_at (only count those created during 2025)
        if created_dt and created_dt.year == 2025:
            mon_c = month_floor(created_dt)
            if mon_c:
                m_counts_created[mon_c] += 1

        # Monthly counts by updated_at (tickets updated/closed in 2025)
        if updated_dt and updated_dt.year == 2025:
            mon_u = month_floor(updated_dt)
            if mon_u:
                m_counts_updated[mon_u] += 1
                if created_dt:
                    m_res_minutes_sum[mon_u] += (updated_dt - created_dt).total_seconds() / 60.0
                    m_res_minutes_n[mon_u]   += 1

        # Top tags: use embedded labels
        labels = t.get("labels") or t.get("tags") or []
        for raw in labels:
            name = (raw.get("name") if isinstance(raw, dict) else str(raw)).strip()
            if not name or name in EXCLUDED_TAGS:
                continue
            tag_counts[name] = tag_counts.get(name, 0) + 1

    # Year averages
    avg_resolution_year = round(sum(res_minutes_all) / len(res_minutes_all), 2) if res_minutes_all else 0.0

    # Build Databox payload (v2)
    payload = [
        {"key": "enchant_resolved_2025_total",          "value": int(resolved_total)},
        {"key": "enchant_resolution_minutes_avg_2025",  "value": float(avg_resolution_year)},
    ]

    # Top 10 tags
    for name, cnt in sorted(tag_counts.items(), key=lambda kv: kv[1], reverse=True)[:10]:
        safe = (
            name.lower()
                .replace(" ", "_")
                .replace("/", "_")
                .replace("&", "and")
                .replace("__", "_")
        )
        payload.append({"key": f"enchant_tag_{safe}_2025_count", "value": int(cnt)})

    # Ensure every month is present
    for mon in month_range_2025():
        # Created-based monthly series (USE THIS IN YOUR CHART)
        payload.append({
            "key":  "enchant_resolved_monthly_created",
            "value": int(m_counts_created.get(mon, 0)),
            "date": iso_z(mon),
        })

        # Updated-based monthly series (activity month)
        payload.append({
            "key":  "enchant_resolved_monthly_updated",
            "value": int(m_counts_updated.get(mon, 0)),
            "date": iso_z(mon),
        })

        # Avg resolution minutes by updated-month
        if m_res_minutes_n.get(mon):
            avg_m = round(m_res_minutes_sum[mon] / m_res_minutes_n[mon], 2)
            payload.append({
                "key":  "enchant_resolution_minutes_avg_monthly",
                "value": float(avg_m),
                "date": iso_z(mon),
            })

    # Debug
    nonzero_created = sum(1 for v in m_counts_created.values() if v)
    nonzero_updated = sum(1 for v in m_counts_updated.values() if v)
    print("DEBUG — resolved_total:", resolved_total)
    print("DEBUG — avg_resolution_year:", avg_resolution_year)
    print("DEBUG — months with created data:", nonzero_created)
    print("DEBUG — months with updated data:", nonzero_updated)
    print("DEBUG — about to push first 6 rows:")
    for row in payload[:6]:
        print(row)

    # Push to Databox v2
    resp = session.post(
        "https://push.databox.com/data",
        auth=(DATABOX_TOKEN, ""),  # Databox uses Basic auth with token as username
        headers={"Accept": "application/vnd.databox.v2+json"},
        json=payload,
        timeout=45,
    )
    print(resp.status_code, resp.text)
    resp.raise_for_status()
    print("Pushed", len(payload), "items")

if __name__ == "__main__":
    main()
