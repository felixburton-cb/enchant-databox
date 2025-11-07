#!/usr/bin/env python3
import os, requests, datetime, time
from collections import defaultdict
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

# ---- ENV ----
ENCHANT_SITE   = os.environ["ENCHANT_SITE"]         # e.g. "carebit"
ENCHANT_TOKEN  = os.environ["ENCHANT_TOKEN"]
DATABOX_TOKEN  = os.environ["DATABOX_TOKEN"]

BASE = f"https://{ENCHANT_SITE}.enchant.com/api/v1"
YEAR = 2025
EXCLUDED_TAGS = {"SU", "Core Support"}               # adjust if needed

# ---- helpers ----
def iso_z(dt): return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
def parse_iso(z):
    if not z: return None
    try:
        return datetime.datetime.fromisoformat(str(z).replace("Z","+00:00")).replace(tzinfo=None)
    except Exception:
        return None

def month_floor(dt: datetime.datetime) -> datetime.datetime:
    return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

# ---- HTTP ----
session = requests.Session()
session.headers.update({
    "Authorization": f"Bearer {ENCHANT_TOKEN}",
    "User-Agent": "enchant-year-in-review/1.1"
})
retries = Retry(total=6, backoff_factor=1.2, status_forcelist=[429,500,502,503,504], allowed_methods=["GET","POST"])
adapter = HTTPAdapter(max_retries=retries)
session.mount("https://", adapter)
session.mount("http://", adapter)

start = datetime.datetime(YEAR, 1, 1)
now   = datetime.datetime.utcnow()

# ---- fetch all closed tickets in 2025 (one pass, paginated) ----
def iter_closed_2025():
    params = {"state": "closed", "since_updated_at": iso_z(start), "per_page": 100, "page": 1}
    while True:
        r = session.get(f"{BASE}/tickets", params=params, timeout=30)
        if r.status_code == 429:
            time.sleep(int(r.headers.get("Retry-After","2"))); continue
        r.raise_for_status()
        items = r.json()
        if not items: break
        for t in items:
            ca = parse_iso(t.get("closed_at") or t.get("closedAt"))
            if ca and start <= ca <= now:
                yield t
        params["page"] += 1
        time.sleep(0.2)

def minutes_between(a_iso, b_iso):
    a, b = parse_iso(a_iso), parse_iso(b_iso)
    if not a or not b: return None
    return max(0, (b - a).total_seconds() / 60.0)

# ---- aggregates ----
resolved_total = 0
res_minutes_all = []
tag_counts = {}

m_counts = defaultdict(int)
m_res_minutes_sum = defaultdict(float)
m_res_minutes_n   = defaultdict(int)

for t in iter_closed_2025():
    closed_at = t.get("closed_at") or t.get("closedAt")
    created_at = t.get("created_at") or t.get("createdAt")
    ca = parse_iso(closed_at)
    if not ca: continue

    resolved_total += 1
    m = minutes_between(created_at, closed_at)
    if m is not None:
        res_minutes_all.append(m)

    labels = t.get("labels") or t.get("tags") or []
    for raw in labels:
        name = raw.get("name") if isinstance(raw, dict) else str(raw)
        if not name or name in EXCLUDED_TAGS: continue
        tag_counts[name] = tag_counts.get(name, 0) + 1

    mon = month_floor(ca)
    m_counts[mon] += 1
    if m is not None:
        m_res_minutes_sum[mon] += m
        m_res_minutes_n[mon]   += 1

avg_resolution_year = round(sum(res_minutes_all)/len(res_minutes_all), 2) if res_minutes_all else 0.0

# ---- CSAT best-effort (skip if not available) ----
def try_fetch_csat_points_2025():
    try:
        r = session.get(f"{BASE}/happiness", params={"since": iso_z(start), "until": iso_z(now)}, timeout=30)
        if r.status_code in (401,403,404):  # not enabled
            return None, {}
        r.raise_for_status()
        data = r.json()

        vals_year = []
        per_month_vals = defaultdict(list)

        if isinstance(data, dict) and "ratings" in data:
            data = data["ratings"]
        if isinstance(data, dict) and "average" in data:
            return float(data["average"]), {}

        if isinstance(data, list):
            for row in data:
                d_s = row.get("date") or row.get("created_at") or row.get("createdAt")
                d = parse_iso(d_s) or parse_iso(str(d_s)+"T00:00:00Z")
                if not d or not (start <= d <= now): continue

                if "percent" in row:
                    val = float(row["percent"])
                elif "score" in row:
                    val = float(row["score"]) / 5.0 * 100.0
                else:
                    continue

                vals_year.append(val)
                per_month_vals[month_floor(d)].append(val)

            year_avg = round(sum(vals_year)/len(vals_year), 2) if vals_year else None
            month_avgs = {m: round(sum(v)/len(v), 2) for m, v in per_month_vals.items() if v}
            return year_avg, month_avgs

        return None, {}
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code in (401,403,404):
            return None, {}
        raise

csat_year, csat_monthly = try_fetch_csat_points_2025()

def slugify(s): return "".join(c.lower() if c.isalnum() else "_" for c in s).strip("_")

# ---- build payload for Databox v2 ----
payload = [
    {"key":"enchant_resolved_2025_total",         "value": int(resolved_total)},
    {"key":"enchant_resolution_minutes_avg_2025", "value": float(avg_resolution_year)},
]
if csat_year is not None:
    payload.append({"key":"enchant_csat_avg_2025", "value": float(csat_year)})

for name, cnt in sorted(tag_counts.items(), key=lambda kv: kv[1], reverse=True)[:10]:
    payload.append({"key": f"enchant_tag_{slugify(name)}_2025_count", "value": int(cnt)})

for mon in sorted(m_counts.keys()):
    payload.append({"key":"enchant_resolved_monthly","value": int(m_counts[mon]), "date": iso_z(mon)})

for mon in sorted(m_res_minutes_n.keys()):
    avg = round(m_res_minutes_sum[mon] / m_res_minutes_n[mon], 2)
    payload.append({"key":"enchant_resolution_minutes_avg_monthly","value": float(avg), "date": iso_z(mon)})

if csat_monthly:
    for mon, v in sorted(csat_monthly.items()):
        payload.append({"key":"enchant_csat_avg_monthly","value": float(v), "date": iso_z(mon)})
print("DEBUG – about to push:")
try:
    for row in payload:  # or whatever your list is called
        print(row)
except NameError:
    # If you used a dict called `metrics` instead of `payload`
    print(metrics)

# ---- push ----
resp = session.post(
    "https://push.databox.com/data",
    auth=(DATABOX_TOKEN, ""),
    headers={"Accept":"application/vnd.databox.v2+json"},
    json=payload,
    timeout=30
)
print(resp.status_code, resp.text)
resp.raise_for_status()
print("Pushed", len(payload), "items")
