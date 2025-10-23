import os, requests, datetime, time, random
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

ENCHANT_SITE   = os.environ["ENCHANT_SITE"]
ENCHANT_TOKEN  = os.environ["ENCHANT_TOKEN"]
DATABOX_TOKEN  = os.environ["DATABOX_TOKEN"]

BASE = f"https://{ENCHANT_SITE}.enchant.com/api/v1"

# Robust HTTP session with retry/backoff (handles 429 & transient errors)
session = requests.Session()
session.headers.update({
    "Authorization": f"Bearer {ENCHANT_TOKEN}",
    "User-Agent": "enchant-databox-gh-actions/1.0"
})
retries = Retry(
    total=6,                # up to 6 attempts
    backoff_factor=1.2,     # 1.2s, 2.4s, 3.6s, ...
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST"]
)
adapter = HTTPAdapter(max_retries=retries)
session.mount("https://", adapter)
session.mount("http://", adapter)

def count_tickets(params):
    p = {**params, "count": "true", "per_page": 1}
    # manual 429 respect for Retry-After (urllib3 handles some cases, we handle explicit too)
    for attempt in range(6):
        r = session.get(f"{BASE}/tickets", params=p, timeout=30)
        if r.status_code == 429:
            retry_after = r.headers.get("Retry-After")
            if retry_after:
                wait = int(retry_after)
            else:
                wait = min(15, (2 ** attempt)) + random.uniform(0, 0.5)
            time.sleep(wait)
            continue
        r.raise_for_status()
        return int(r.headers.get("Total-Count", "0"))
    # If we somehow get here, raise the last response as error
    r.raise_for_status()

# start of today in UTC (Actions cron uses UTC)
today_utc_iso = datetime.datetime.utcnow().replace(
    hour=0, minute=0, second=0, microsecond=0
).isoformat() + "Z"

# Small spacing between calls to be gentle on rate limits
metrics = {}
metrics["$enchant_open_tickets"] = count_tickets({"state": "open"});  time.sleep(1.0)
metrics["$enchant_hold_tickets"] = count_tickets({"state": "hold"});  time.sleep(1.0)
metrics["$enchant_new_today"]    = count_tickets({"since_created_at": today_utc_iso}); time.sleep(1.0)
metrics["$enchant_closed_today"] = count_tickets({"state": "closed", "since_updated_at": today_utc_iso})

# Push to Databox
resp = session.post(
    "https://push.databox.com",
    auth=(DATABOX_TOKEN, ""),
    json={"data": [metrics]},
    timeout=30,
)
resp.raise_for_status()
print("Pushed to Databox:", metrics)

