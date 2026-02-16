
import json, importlib, pathlib

STATES = ['alabama', 'alaska', 'arizona', 'arkansas', 'california', 'colorado', 'connecticut', 'delaware', 'florida', 'georgia', 'hawaii', 'idaho', 'illinois', 'indiana', 'iowa', 'kansas', 'kentucky', 'louisiana', 'maine', 'maryland', 'massachusetts', 'michigan', 'minnesota', 'mississippi', 'missouri', 'montana', 'nebraska', 'nevada', 'new_hampshire', 'new_jersey', 'new_mexico', 'new_york', 'north_carolina', 'north_dakota', 'ohio', 'oklahoma', 'oregon', 'pennsylvania', 'rhode_island', 'south_carolina', 'south_dakota', 'tennessee', 'texas', 'utah', 'vermont', 'virginia', 'washington', 'west_virginia', 'wisconsin', 'wyoming']

data_file = pathlib.Path("data/businesses.json")
existing = json.loads(data_file.read_text()) if data_file.exists() else []
seen = {(x.get("name"), x.get("state")) for x in existing}
new = []

for s in STATES:
    mod = importlib.import_module(f"scrapers.states.{s}")
    for biz in mod.scrape():
        k = (biz["name"], biz["state"])
        if k not in seen:
            seen.add(k)
            new.append(biz)

data_file.write_text(json.dumps(existing + new, indent=2))
print("Added", len(new))
