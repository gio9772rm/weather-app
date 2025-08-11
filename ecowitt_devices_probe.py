import os, json
import requests
from dotenv import load_dotenv

load_dotenv()
APP = os.getenv("ECOWITT_APPLICATION_KEY","")
KEY = os.getenv("ECOWITT_API_KEY","")

def call(endpoint, **params):
    u = f"https://api.ecowitt.net/api/v3/{endpoint}"
    r = requests.get(u, params=params, timeout=30)
    try:
        j = r.json()
    except Exception:
        j = {"status": r.status_code, "text": r.text[:400]}
    return r.status_code, r.reason, j

out = {}

# device/list
st, rs, j = call("device/list", application_key=APP, api_key=KEY, page=1, limit=20)
out["device_list"] = {"status": st, "reason": rs, "payload": j}

# If at least one device exists, try device/info for each to get canonical MAC
macs = []
if isinstance(j, dict):
    data = j.get("data") or []
    if isinstance(data, dict):
        # some tenants wrap list in 'list'
        data = data.get("list", [])
    for d in (data if isinstance(data, list) else []):
        mac = d.get("mac") or d.get("sn")
        if mac:
            macs.append(mac)

info = []
for mac in macs[:3]:
    st2, rs2, j2 = call("device/info", application_key=APP, api_key=KEY, mac=mac)
    info.append({"mac": mac, "status": st2, "reason": rs2, "payload": j2})

out["device_info"] = info
with open("ecowitt_devices.json","w",encoding="utf-8") as f:
    json.dump(out, f, ensure_ascii=False, indent=2)
print("Scritto ecowitt_devices.json")
