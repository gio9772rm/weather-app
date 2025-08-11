import sys, json, requests

IP = "192.168.1.31"

paths = ["/get_livedata_info", "/get_current_weather"]
out = {"ip": IP, "results": []}

for p in paths:
    url = f"http://{IP}{p}"
    try:
        r = requests.get(url, timeout=8)
        item = {
            "url": url,
            "status": r.status_code,
            "ok": r.ok,
            "content_type": r.headers.get("content-type",""),
        }
        try:
            j = r.json()
            item["json_keys"] = list(j.keys())[:20]
            item["sample"] = str(j)[:300]
        except Exception:
            item["text_head"] = r.text[:200]
        out["results"].append(item)
    except Exception as e:
        out["results"].append({"url": url, "error": str(e)})

with open("ecowitt_lan_probe_output.json","w",encoding="utf-8") as f:
    json.dump(out, f, ensure_ascii=False, indent=2)

print("Scritto ecowitt_lan_probe_output.json")