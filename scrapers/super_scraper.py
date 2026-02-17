"""
Super Scraper - Orchestrates all state business registration scrapers
Simplified version
"""
import json
import importlib
import pathlib
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

STATES = [
    'alabama', 'alaska', 'arizona', 'arkansas', 'california', 'colorado', 
    'connecticut', 'delaware', 'florida', 'georgia', 'hawaii', 'idaho', 
    'illinois', 'indiana', 'iowa', 'kansas', 'kentucky', 'louisiana', 
    'maine', 'maryland', 'massachusetts', 'michigan', 'minnesota', 
    'mississippi', 'missouri', 'montana', 'nebraska', 'nevada', 
    'new_hampshire', 'new_jersey', 'new_mexico', 'new_york', 
    'north_carolina', 'north_dakota', 'ohio', 'oklahoma', 'oregon', 
    'pennsylvania', 'rhode_island', 'south_carolina', 'south_dakota', 
    'tennessee', 'texas', 'utah', 'vermont', 'virginia', 'washington', 
    'west_virginia', 'wisconsin', 'wyoming'
]

def main():
    logger.info("STARTING SUPER SCRAPER")
    start_time = datetime.now()
    
    data_file = pathlib.Path("data/businesses.json")
    existing = json.loads(data_file.read_text()) if data_file.exists() else []
    seen = {(x.get("name"), x.get("state")) for x in existing}
    new = []
    successful = 0
    
    for state in STATES:
        try:
            logger.info(f"Scraping {state.upper()}...")
            mod = importlib.import_module(f"scrapers.states.{state}")
            businesses = mod.scrape()
            
            for biz in businesses:
                key = (biz.get("name"), biz.get("state"))
                if key not in seen:
                    seen.add(key)
                    new.append(biz)
            
            if businesses:
                logger.info(f"✓ {state.upper()}: {len(businesses)} businesses")
                successful += 1
            else:
                logger.info(f"○ {state.upper()}: No data")
        except Exception as e:
            logger.error(f"✗ {state.upper()}: {e}")
    
    data_file.parent.mkdir(parents=True, exist_ok=True)
    data_file.write_text(json.dumps(existing + new, indent=2))
    
    runtime = (datetime.now() - start_time).total_seconds()
    logger.info(f"\nCOMPLETE - Runtime: {runtime:.1f}s")
    logger.info(f"States successful: {successful}/{len(STATES)}")
    logger.info(f"Businesses: {len(existing)} existing + {len(new)} new = {len(existing)+len(new)} total")
    logger.info(f"Added {len(new)} new businesses")

if __name__ == "__main__":
    main()
