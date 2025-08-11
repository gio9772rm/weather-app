import os, json
from datetime import datetime, timedelta
import requests
from dotenv import load_dotenv

load_dotenv()
APP = os.getenv("ECOWITT_APPLICATION_KEY","")
KEY = os.getenv("ECOWITT_API_KEY","")
MAC = os.getenv("ECOWITT_MAC","")

def fetch(url, params):
    r = requests.get(url, params=params, timeout=30)
    return r.status_code, r.reason, (r.json() if r.headers.get("content-type","").startswith("application/json") else r.text)

today = datetime.now()
yesterday = today - timedelta(days=1)

out = {}

# Real-time
status, reason, data = fetch("https://api.ecowitt.net/api/v3/device/real_time", {
    "application_key": APP, "api_key": KEY, "mac": MAC,
    "call_back": "outdoor,wind,pressure,rainfall"
})
out["real_time"] = {"status": status, "reason": reason, "sample": data if isinstance(data, dict) else str(data)[:500]}

# 1-day history (default resolution per API)
status, reason, data = fetch("https://api.ecowitt.net/api/v3/device/history", {
    "application_key": APP, "api_key": KEY, "mac": MAC,
    "start_date": yesterday.strftime("%Y-%m-%d %H:%M:%S"),
    "end_date": today.strftime("%Y-%m-%d %H:%M:%S"),
    "call_back": "outdoor,wind,pressure,rainfall"
})
out["history_1d"] = {"status": status, "reason": reason,
                     "keys": (list(data.keys()) if isinstance(data, dict) else None),
                     "sample": data if isinstance(data, dict) else str(data)[:500]}

# Try 30-min cycle (some tenants require cycle_type)
status, reason, data = fetch("https://api.ecowitt.net/api/v3/device/history", {
    "application_key": APP, "api_key": KEY, "mac": MAC,
    "start_date": yesterday.strftime("%Y-%m-%d %H:%M:%S"),
    "end_date": today.strftime("%Y-%m-%d %H:%M:%S"),
    "call_back": "outdoor,wind,pressure,rainfall",
    "cycle_type": "30min"
})
out["history_30min"] = {"status": status, "reason": reason,
                        "keys": (list(data.keys()) if isinstance(data, dict) else None),
                        "sample": data if isinstance(data, dict) else str(data)[:500]}

# Save to file
with open("ecowitt_probe_output.json","w",encoding="utf-8") as f:
    json.dump(out, f, ensure_ascii=False, indent=2, default=str)

print("Scritto ecowitt_probe_output.json")
