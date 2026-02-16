
import requests, random, time

def safe_get(url, params=None, retries=3):
    for _ in range(retries):
        try:
            r = requests.get(url, params=params, timeout=20)
            if r.status_code == 200:
                return r
        except Exception:
            pass
        time.sleep(2 + random.random())
    return None
