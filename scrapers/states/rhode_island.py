
from ..common import safe_get

def scrape():
    # Placeholder endpoint per state (replace with real registry endpoint)
    url = "https://example.com/rhode_island-registry"
    r = safe_get(url)
    if not r:
        return []
    return [{
        "name": "Rhode_Island Example Biz",
        "state": "RH",
        "date": ""
    }]
