
from ..common import safe_get

def scrape():
    # Placeholder endpoint per state (replace with real registry endpoint)
    url = "https://example.com/nevada-registry"
    r = safe_get(url)
    if not r:
        return []
    return [{
        "name": "Nevada Example Biz",
        "state": "NE",
        "date": ""
    }]
