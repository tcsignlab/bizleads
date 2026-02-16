
import json, requests

WEBHOOK = "https://example.com/webhook"

def send_alert(biz):
    try:
        requests.post(WEBHOOK, json=biz, timeout=10)
    except Exception:
        pass

def check_new():
    data = json.load(open("data/businesses.json"))
    for biz in data[-10:]:
        send_alert(biz)

if __name__ == "__main__":
    check_new()
