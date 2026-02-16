import requests
from bs4 import BeautifulSoup
from ..utils import HEADERS

STATE_CODE = "MO"
SEARCH_URL = ""  # TODO: real endpoint

def scrape():
    results = []

    try:
        r = requests.get(SEARCH_URL, headers=HEADERS, timeout=20)
        if r.status_code != 200:
            print(f"[{STATE_CODE}] Non-200 status:", r.status_code)
            return []

        # Hardened: handle non-JSON responses safely
        content_type = r.headers.get("Content-Type", "")
        if "json" in content_type:
            data = r.json()
            # TODO: parse JSON
            return []
        else:
            soup = BeautifulSoup(r.text, "html.parser")
            # TODO: parse HTML
            return []

    except Exception as e:
        print(f"[{STATE_CODE}] Error:", e)
        return []
