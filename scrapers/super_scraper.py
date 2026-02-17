"""
Super Scraper - Orchestrates all state business registration scrapers
Enhanced version with error handling, logging, and statistics
"""
import json
import importlib
import pathlib
import logging
from datetime import datetime
from typing import List, Dict

# Configure logging
log_file = pathlib.Path('scraper.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# All 50 US states
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

# Priority states (process these first)
PRIORITY_STATES = [
    'florida',      # Has FTP with daily files
    'california',   # Has API
    'delaware',     # High volume
    'texas',        # High volume
    'new_york',     # High volume
    'nevada',       # High volume
    'wyoming',      # High volume
]

def load_existing_data(data_file: pathlib.Path) -> tuple:
    """
    Load existing business data from file
    
    Args:
        data_file: Path to data file
    
    Returns:
        Tuple of (existing_data list, seen_set for deduplication)
    """
    if data_file.exists():
        try:
            existing = json.loads(data_file.read_text())
            seen = {(x.get("name"), x.get("state")) for x in existing}
            logger.info(f"Loaded {len(existing)} existing businesses")
            return existing, seen
        except Exception as e:
            logger.error(f"Error loading existing data: {e}")
            return [], set()
    return [], set()

def save_data(data_file: pathlib.Path, data: List[Dict]):
    """
    Save business data to file
    
    Args:
        data_file: Path to data file
        data: List of business dictionaries
    """
    try:
        # Ensure parent directory exists
        data_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Write data
        data_file.write_text(json.dumps(data, indent=2))
        logger.info(f"Saved {len(data)} businesses to {data_file}")
    except Exception as e:
        logger.error(f"Error saving data: {e}")

def scrape_state(state: str) -> List[Dict]:
    """
    Scrape a single state
    
    Args:
        state: State name (lowercase, underscores)
    
    Returns:
        List of business dictionaries
    """
    try:
        logger.info(f"=" * 60)
        logger.info(f"Scraping {state.upper()}")
        logger.info(f"=" * 60)
        
        # Import state module
        mod = importlib.import_module(f"scrapers.states.{state}")
        
        # Call scrape function
        businesses = mod.scrape()
        
        logger.info(f"✓ {state.upper()}: Found {len(businesses)} new businesses")
        return businesses
        
    except ModuleNotFoundError:
        logger.warning(f"⚠ {state.upper()}: Module not found (not implemented yet)")
        return []
    except AttributeError:
        logger.error(f"✗ {state.upper()}: Module has no scrape() function")
        return []
    except Exception as e:
        logger.error(f"✗ {state.upper()}: Error - {str(e)}")
        return []

def main():
    """
    Main orchestration function
    """
    logger.info("=" * 80)
    logger.info("STARTING SUPER SCRAPER")
    logger.info("=" * 80)
    
    start_time = datetime.now()
    
    # Data file
    data_file = pathlib.Path("data/businesses.json")
    
    # Load existing data
    existing, seen = load_existing_data(data_file)
    original_count = len(existing)
    
    # Track statistics
    stats = {
        'total_states': len(STATES),
        'successful_states': 0,
        'failed_states': 0,
        'skipped_states': 0,
        'new_businesses': 0,
        'by_state': {}
    }
    
    # New businesses found
    new = []
    
    # Process priority states first
    logger.info(f"\nProcessing {len(PRIORITY_STATES)} priority states first...")
    for state in PRIORITY_STATES:
        businesses = scrape_state(state)
        
        # Track state stats
        state_new = 0
        for biz in businesses:
            key = (biz["name"], biz["state"])
            if key not in seen:
                seen.add(key)
                new.append(biz)
                state_new += 1
        
        stats['by_state'][state] = {
            'found': len(businesses),
            'new': state_new
        }
        
        if len(businesses) > 0:
            stats['successful_states'] += 1
        elif len(businesses) == 0:
            stats['skipped_states'] += 1
    
    # Process remaining states
    remaining_states = [s for s in STATES if s not in PRIORITY_STATES]
    logger.info(f"\nProcessing {len(remaining_states)} remaining states...")
    
    for state in remaining_states:
        businesses = scrape_state(state)
        
        state_new = 0
        for biz in businesses:
            key = (biz["name"], biz["state"])
            if key not in seen:
                seen.add(key)
                new.append(biz)
                state_new += 1
        
        stats['by_state'][state] = {
            'found': len(businesses),
            'new': state_new
        }
        
        if len(businesses) > 0:
            stats['successful_states'] += 1
        elif len(businesses) == 0:
            stats['skipped_states'] += 1
    
    # Calculate final statistics
    stats['new_businesses'] = len(new)
    stats['failed_states'] = stats['total_states'] - stats['successful_states'] - stats['skipped_states']
    
    # Save combined data
    combined = existing + new
    save_data(data_file, combined)
    
    # Calculate runtime
    end_time = datetime.now()
    runtime = (end_time - start_time).total_seconds()
    
    # Print final report
    logger.info("\n" + "=" * 80)
    logger.info("SCRAPING COMPLETE")
    logger.info("=" * 80)
    logger.info(f"\nRuntime: {runtime:.2f} seconds ({runtime/60:.2f} minutes)")
    logger.info(f"\nTotal States: {stats['total_states']}")
    logger.info(f"  ✓ Successful: {stats['successful_states']}")
    logger.info(f"  ⚠ Skipped (not implemented): {stats['skipped_states']}")
    logger.info(f"  ✗ Failed: {stats['failed_states']}")
    logger.info(f"\nBusinesses:")
    logger.info(f"  Previous: {original_count}")
    logger.info(f"  New: {stats['new_businesses']}")
    logger.info(f"  Total: {len(combined)}")
    
    # Show top states by new registrations
    logger.info("\n" + "-" * 80)
    logger.info("TOP 10 STATES BY NEW REGISTRATIONS")
    logger.info("-" * 80)
    
    sorted_states = sorted(
        stats['by_state'].items(),
        key=lambda x: x[1]['new'],
        reverse=True
    )[:10]
    
    for state, state_stats in sorted_states:
        if state_stats['new'] > 0:
            logger.info(f"  {state.upper():20} - {state_stats['new']:4} new businesses")
    
    logger.info("\n" + "=" * 80)
    logger.info(f"Added {len(new)} new businesses")
    logger.info("=" * 80)

if __name__ == "__main__":
    main()
