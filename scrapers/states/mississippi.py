
from ..common import safe_get

def scrape():
    # Placeholder endpoint per state (replace with real registry endpoint)
    url = "https://example.com/mississippi-registry"
    r = safe_get(url)
    if not r:
        return []
    return [{
        "name": "Mississippi Example Biz",
        "state": "MI",
        "date": ""
    }]
