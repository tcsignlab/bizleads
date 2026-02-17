
from ..common import safe_get

def scrape():
    # Placeholder endpoint per state (replace with real registry endpoint)
    url = "https://example.com/wyoming-registry"
    r = safe_get(url)
    if not r:
        return []
    return [{
        "name": "Wyoming Example Biz",
        "state": "WY",
        "date": ""
    }]
