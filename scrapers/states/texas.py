"""
Texas Business Registration Scraper
Texas Comptroller - Taxable Entity Search
"""
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import logging
from ..common import (create_business_entity, is_recent_registration,
                     safe_get, safe_post, get_date_range_last_30_days,
                     format_date_for_state, StateScraperError, rate_limit)

logger = logging.getLogger(__name__)

# Texas search URLs
SEARCH_URL = "https://mycpa.cpa.state.tx.us/coa/"
BASE_URL = "https://mycpa.cpa.state.tx.us"

@rate_limit(min_delay=2.0, max_delay=4.0)
def scrape():
    """
    Scrape new Texas business registrations from last 30 days
    
    Returns:
        List of business entity dictionaries
    """
    logger.info("Starting Texas scraper")
    businesses = []
    
    try:
        # Texas uses Comptroller's office for taxable entities
        # Search by entity type and date range
        
        entity_types = ['CORPORATION', 'LIMITED LIABILITY COMPANY', 'LIMITED PARTNERSHIP']
        
        for entity_type in entity_types:
            logger.info(f"Searching {entity_type} entities")
            
            try:
                entities = search_by_entity_type(entity_type)
                businesses.extend(entities)
                
            except Exception as e:
                logger.error(f"Error searching {entity_type}: {e}")
                continue
        
        logger.info(f"Texas scraper completed: {len(businesses)} businesses found")
        
    except Exception as e:
        logger.error(f"Texas scraper error: {e}")
        return []
    
    return businesses

def search_by_entity_type(entity_type):
    """
    Search Texas entities by type
    
    Args:
        entity_type: Type of entity to search
    
    Returns:
        List of business entities
    """
    businesses = []
    
    try:
        # Note: Texas search may require specific form navigation
        # This is a template that needs to be adjusted based on actual interface
        
        search_params = {
            'entityType': entity_type,
            'searchType': 'NAME',
        }
        
        response = safe_get(SEARCH_URL, params=search_params)
        if not response:
            return businesses
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Parse results (adjust selectors based on actual HTML)
        result_rows = soup.find_all('tr', class_='searchResultRow')
        
        for row in result_rows:
            try:
                entity = extract_entity_from_row(row)
                if entity and is_recent_registration(entity.get('registration_date', ''), days=30):
                    businesses.append(entity)
                    
            except Exception as e:
                logger.error(f"Error parsing row: {e}")
                continue
        
    except Exception as e:
        logger.error(f"Error in search_by_entity_type: {e}")
    
    return businesses

def extract_entity_from_row(row):
    """
    Extract entity data from HTML row
    
    Args:
        row: BeautifulSoup row element
    
    Returns:
        Business entity dictionary
    """
    try:
        cells = row.find_all('td')
        if len(cells) < 4:
            return None
        
        entity = create_business_entity(
            name=cells[0].text.strip(),
            state='TX',
            entity_number=cells[1].text.strip(),
            registration_date=cells[2].text.strip(),
            entity_type=cells[3].text.strip(),
            status='Active',  # Adjust based on actual data
        )
        
        return entity
        
    except Exception as e:
        logger.error(f"Error extracting entity: {e}")
        return None

if __name__ == "__main__":
    results = scrape()
    print(f"Found {len(results)} new Texas businesses")
    for biz in results[:5]:
        print(f"  - {biz['name']} ({biz['entity_type']}) - {biz['registration_date']}")
