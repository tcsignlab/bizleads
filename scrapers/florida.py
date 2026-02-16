import requests
from bs4 import BeautifulSoup
from ..utils import HEADERS

SEARCH_URL = "https://search.sunbiz.org/Inquiry/CorporationSearch/SearchResults"

def scrape():
    results = []
    page = 1

    while True:
        payload = {
            "SearchTerm": "",
            "SearchNameOrder": "StartsWith",
            "SearchType": "Keyword",
            "PageNumber": page
        }

        r = requests.post(SEARCH_URL, headers=HEADERS, data=payload, timeout=20)
        if r.status_code != 200:
            break

        soup = BeautifulSoup(r.text, "html.parser")
        rows = soup.select("tr.search-result")

        if not rows:
            break

        for row in rows:
            cols = row.find_all("td")
            if len(cols) < 2:
                continue

            name = cols[0].get_text(strip=True)
            status = cols[1].get_text(strip=True)

            results.append({
                "name": name,
                "state": "FL",
                "status": status
            })

        page += 1

        # Safety cap (avoid infinite loops / rate limits)
        if page > 20:
            break

    return results
