#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import csv
import json
import time
import datetime
import collections
import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

# =========================
#  ENV
# =========================
ENCHANT_SITE  = os.environ["ENCHANT_SITE"]        # e.g. "carebit"
ENCHANT_TOKEN = os.environ["ENCHANT_TOKEN"]
DATABOX_TOKEN = os.environ["DATABOX_TOKEN"]       # Push Custom Data token
CSAT_CSV      = os.environ.get("CSAT_CSV", "").strip()  # optional path in repo, e.g. data/happiness_2025.csv

BASE = f"https://{ENCHANT_SITE}.enchant.com/api/v1"
YEAR_START_ISO = "2025-01-01T00:00:00Z"
PER_PAGE = 100

# Exclude any tags you don't want in "Top tags"
EXCLUDED_TAGS = {"SU", "Core Support"}

DEBUG = True  # set False to quiet logs


# =========================
#  HTTP session (robust)
# =========================
session = requests.Session()
session.headers.update({
    "Authorization": f"Bearer {ENCHANT_TOKEN}",
    "User-Agent": "enchant-yir-2025/1.0"
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


# =========================
#  Helpers
# =========================
def parse_iso_z(s: str) -> datetime.datetime | None:
    """Enchant docs: timestamps are ISO8601 UTC (…Z)."""
    if not s:
        return None
    try:
        return datetime.datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=datetime.timezone.utc)
    except Exception:
        return None

def iso_now_z() -> str:
    return datetime.datetime.utcnow().replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")

def month_floor_iso(dt: datetime.datetime) -> str:
    d0 = dt.astimezone(datetime.timezone.utc).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    return d0.strftime("%Y-%m-%dT%H:%M:%SZ")

def minutes_between(created_at: str, updated_at: str) -> float | None:
    c = parse_iso_z(created_at)
    u = parse_iso_z(updated_at)
    if not c or not u:
        return None
    return max(0.0, (u - c).total_seconds() / 60.0)

def slugify_tag(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")[:60] or "tag"


# =========================
#  Enchant pulls (per docs)
#  - Tickets endpoint
#  - state=closed
#  - since_updated_at = 2025-01-01T00:00:00Z
#  - embed=labels (to count tags)
# =========================
def iter_closed_2025():
    page = 1
    while True:
        params = {
            "state": "closed",
            "since_updated_at": YEAR_START_ISO,
            "embed": "labels",
            "per_page": PER_PAGE,
            "page": page,
            "sort": "updated_at",  # deterministic paging (oldest→newest); use "-updated_at" if you prefer newest first
        }
        r = session.get(f"{BASE}/tickets", params=params, timeout=30)
        if r.status_code == 429:
            # Respect Rate-Limit-Reset/Retry-After if present
            rl = r.headers.get("Rate-Limit-Reset") or r.headers.get("Retry-After")
            wait = int(rl) if (rl and rl.isdigit()) else 2
            if DEBUG: print(f"[429] sleeping {wait}s…")
            time.sleep(wait)
            r = session.get(f"{BASE}/tickets", params=params, timeout=30)
        r.raise_for_status()

        items = r.json() if isinstance(r.json(), list) else []
        if DEBUG: print(f"fetch page {page}: {len(items)} items")
        if not items:
            break
        for it in items:
            yield it
        page += 1


# =========================
#  Compute 2025 metrics
# =========================
resolved_total = 0
res_minutes_all: list[float] = []
tag_counts: dict[str, int] = {}

m_counts = collections.defaultdict(int)          # month -> count
m_res_sum = collections.defaultdict(float)       # month -> sum minutes
m_res_n   = collections.defaultdict(int)         # month -> n

year_start_dt = parse_iso_z(YEAR_START_ISO)
assert year_start_dt, "Invalid YEAR_START_ISO"

for t in iter_closed_2025():
    state = t.get("state")
    if state != "closed":
        continue

    created_at = t.get("created_at")
    updated_at = t.get("updated_at")
    u = parse_iso_z(updated_at)
    if not u or u < year_start_dt:
        continue

    # Totals
    resolved_total += 1

    # Resolution time (minutes)
    m = minutes_between(created_at, updated_at)
    if m is not None:
        res_minutes_all.append(m)

    # Tags/Labels
    labels = t.get("labels") or []
    for raw in labels:
        name = raw.get("name") if isinstance(raw, dict) else str(raw)
        if not name or name in EXCLUDED_TAGS:
            continue
        tag_counts[name] = tag_counts.get(name, 0) + 1

    # Monthly buckets (based on updated_at = close moment)
    mon_iso = month_floor_iso(u)
    m_counts[mon_iso] += 1
    if m is not None:
        m_res_sum[mon_iso] += m
        m_res_n[mon_iso]   += 1

avg_resolution_year = round(sum(res_minutes_all) / len(res_minutes_all), 2) if res_minutes_all else 0.0

if DEBUG:
    print("DEBUG — resolved_total:", resolved_total)
    print("DEBUG — avg_resolution_year:", avg_resolution_year)
    print("DEBUG — months found:", len(m_counts))


# =========================
#  Optional: CSAT from CSV
#  Expect CSV columns (adapt easily):
#    date, csat_percent
#  Where date is 'YYYY-MM' or 'YYYY-MM-DD'
# =========================
def push_csat_from_csv_if_present(payload: list[dict]):
    if not CSAT_CSV:
        return
    if not os.path.exists(CSAT_CSV):
        print(f"CSAT_CSV path not found: {CSAT_CSV} (skipping)")
        return

    def to_month_iso(s: str) -> str:
        s = s.strip()
        if len(s) >= 7:  # YYYY-MM or YYYY-MM-DD
            yyyy_mm = s[:7]
            return f"{yyyy_mm}-01T00:00:00Z"
        return "2025-01-01T00:00:00Z"

    csat_vals = []
    try:
        with open(CSAT_CSV, newline="") as f:
            rdr = csv.DictReader(f)
            for row in rdr:
                # Be tolerant on column names
                csat_pct = None
                for k in ("csat_percent", "csat", "csat_pct", "csat_percentage"):
                    if k in row and row[k] not in (None, "", "NaN"):
                        try:
                            csat_pct = float(row[k])
                            break
                        except Exception:
                            pass
                if csat_pct is None:
                    continue

                date_raw = row.get("date") or row.get("month") or ""
                month_iso = to_month_iso(str(date_raw))
                payload.append({
                    "key":  "enchant_csat_avg_monthly",
                    "value": round(csat_pct, 2),
                    "date": month_iso,
                })
                csat_vals.append(csat_pct)

        if csat_vals:
            payload.append({
                "key":  "enchant_csat_avg_2025",
                "value": round(sum(csat_vals)/len(csat_vals), 2),
            })
            if DEBUG:
                print("DEBUG — CSAT rows:", len(csat_vals))
    except Exception as e:
        print("WARN — CSAT CSV parse error:", e)


# =========================
#  Build Databox v2 payload
#  (raw array; header Accept: v2)
# =========================
payload: list[dict] = [
    {"key": "enchant_resolved_2025_total",          "value": int(resolved_total)},
    {"key": "enchant_resolution_minutes_avg_2025",  "value": float(avg_resolution_year)},
]

# Top 10 tags
for name, cnt in sorted(tag_counts.items(), key=lambda kv: kv[1], reverse=True)[:10]:
    payload.append({"key": f"enchant_tag_{slugify_tag(name)}_2025_count", "value": int(cnt)})

# Monthly series (with dates)
for mon in sorted(m_counts.keys()):
    payload.append({"key": "enchant_resolved_monthly", "value": int(m_counts[mon]), "date": mon})

for mon in sorted(m_res_n.keys()):
    avg_m = round(m_res_sum[mon] / m_res_n[mon], 2)
    payload.append({"key": "enchant_resolution_minutes_avg_monthly", "value": float(avg_m), "date": mon})

# Optional CSAT from CSV
push_csat_from_csv_if_present(payload)

if DEBUG:
    print("DEBUG — about to push first 6 rows:")
    for row in payload[:6]:
        print(row)
    print(f"DEBUG — total rows: {len(payload)}")

# =========================
#  Push to Databox
# =========================
resp = session.post(
    "https://push.databox.com/data",
    auth=(DATABOX_TOKEN, ""),
    headers={"Accept": "application/vnd.databox.v2+json", "Content-Type": "application/json"},
    json=payload,  # v2 raw array format
    timeout=45,
)
print(resp.status_code, resp.text)
resp.raise_for_status()
print("Pushed", len(payload), "items")
