import os, requests, datetime

ENCHANT_SITE   = os.environ["ENCHANT_SITE"]
ENCHANT_TOKEN  = os.environ["ENCHANT_TOKEN"]
DATABOX_TOKEN  = os.environ["DATABOX_TOKEN"]

BASE = f"https://{ENCHANT_SITE}.enchant.com/api/v1"
HEADERS = {"Authorization": f"Bearer {ENCHANT_TOKEN}"}

def count_tickets(params):
    p = {**params, "count": "true", "per_page": 1}
    r = requests.get(f"{BASE}/tickets", headers=HEADERS, params=p, timeout=20)
    r.raise_for_status()
    return int(r.headers.get("Total-Count", "0"))

# start of today in UTC (Actions cron uses UTC)
today_utc_iso = datetime.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0).isoformat() + "Z"

metrics = {
    "$enchant_open_tickets":  count_tickets({"state": "open"}),
    "$enchant_hold_tickets":  count_tickets({"state": "hold"}),
    "$enchant_new_today":     count_tickets({"since_created_at": today_utc_iso}),
    "$enchant_closed_today":  count_tickets({"state": "closed", "since_updated_at": today_utc_iso}),
}

resp = requests.post(
    "https://push.databox.com",
    auth=(DATABOX_TOKEN, ""),
    json={"data": [metrics]},
    timeout=20,
)
resp.raise_for_status()
print("Pushed to Databox:", metrics)
