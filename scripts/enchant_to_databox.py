import os, requests, datetime, time, random
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

ENCHANT_SITE   = os.environ["ENCHANT_SITE"]      # e.g. carebit
ENCHANT_TOKEN  = os.environ["ENCHANT_TOKEN"]
DATABOX_TOKEN  = os.environ["DATABOX_TOKEN"]

BASE = f"https://{ENCHANT_SITE}.enchant.com/api/v1"

session = requests.Session()
session.headers.update({
    "Authorization": f"Bearer {ENCHANT_TOKEN}",
    "User-Agent": "enchant-databox-gh-actions/1.0"
})
retries = Retry(total=6, backoff_factor=1.2, status_forcelist=[429,500,502,503,504], allowed_methods=["GET","POST"])
adapter = HTTPAdapter(max_retries=retries)
session.mount("https://", adapter)
session.mount("http://", adapter)

def count_tickets(params):
    p = {**params, "count": "true", "per_page": 1}
    for attempt in range(6):
        r = session.get(f"{BASE}/tickets", params=p, timeout=30)
        if r.status_code == 429:
            retry_after = r.headers.get("Retry-After")
            wait = int(retry_after) if retry_after else min(15, (2 ** attempt)) + random.uniform(0, 0.5)
            time.sleep(wait); continue
        r.raise_for_status()
        return int(r.headers.get("Total-Count", "0"))
    r.raise_for_status()

today_utc_iso = datetime.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0).isoformat() + "Z"

metrics = {}
metrics["$enchant_open_tickets"]  = count_tickets({"state": "open"});                       time.sleep(1.0)
metrics["$enchant_hold_tickets"]  = count_tickets({"state": "hold"});                       time.sleep(1.0)
metrics["$enchant_new_today"]     = count_tickets({"since_created_at": today_utc_iso});     time.sleep(1.0)
metrics["$enchant_closed_today"]  = count_tickets({"state": "closed", "since_updated_at": today_utc_iso})

# --- v2 Databox push ---
push_data = [
    {"key": "enchant_open_tickets",  "value": metrics["$enchant_open_tickets"]},
    {"key": "enchant_hold_tickets",  "value": metrics["$enchant_hold_tickets"]},
    {"key": "enchant_new_today",     "value": metrics["$enchant_new_today"]},
    {"key": "enchant_closed_today",  "value": metrics["$enchant_closed_today"]},
]

resp = session.post(
    "https://push.databox.com/data",
    auth=(DATABOX_TOKEN, ""),
    headers={"Accept": "application/vnd.databox.v2+json"},
    json=push_data,
    timeout=30,
)
resp.raise_for_status()
print("Pushed to Databox:", push_data)
