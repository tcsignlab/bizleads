
from ..common import safe_get

def scrape():
    # Placeholder endpoint per state (replace with real registry endpoint)
    url = "https://example.com/south_dakota-registry"
    r = safe_get(url)
    if not r:
        return []
    return [{
        "name": "South_Dakota Example Biz",
        "state": "SO",
        "date": ""
    }]
