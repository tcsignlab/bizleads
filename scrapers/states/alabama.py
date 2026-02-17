
from ..common import safe_get

def scrape():
    # Placeholder endpoint per state (replace with real registry endpoint)
    url = "https://example.com/alabama-registry"
    r = safe_get(url)
    if not r:
        return []
    return [{
        "name": "Alabama Example Biz",
        "state": "AL",
        "date": ""
    }]
