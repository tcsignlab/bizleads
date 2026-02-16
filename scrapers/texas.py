import requests
import time
from ..utils import HEADERS

SEARCH_URL = "https://corp.sos.state.tx.us/acct/acct-search.asp"

def scrape():
    results = []
    offset = 0
    limit = 50

    while True:
        params = {
            "offset": offset,
            "limit": limit,
            "query": ""
        }

        r = requests.get(
            SEARCH_URL,
            headers=HEADERS,
            params=params,
            timeout=20
        )

        if r.status_code != 200:
            break

        data = r.json()

        entities = data.get("entities", [])
        if not entities:
            break

        for biz in entities:
            results.append({
                "name": biz.get("entityName", "").strip(),
                "state": "TX",
                "status": biz.get("status", ""),
                "filed_date": biz.get("fileDate", "")
            })

        offset += limit

        # Be nice to the API
        time.sleep(0.75)

        # Safety cap
        if offset > 1000:
            break

    return results
