"""
Delaware Business Registration Scraper
Delaware Division of Corporations - High volume state
"""
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import logging
import re
from ..common import (create_business_entity, is_recent_registration,
                     safe_get, safe_post, StateScraperError, rate_limit)

logger = logging.getLogger(__name__)

# Delaware search URLs
SEARCH_URL = "https://icis.corp.delaware.gov/Ecorp/EntitySearch/NameSearch.aspx"
BASE_URL = "https://icis.corp.delaware.gov"

@rate_limit(min_delay=2.0, max_delay=4.0)
def scrape():
    """
    Scrape new Delaware business registrations from last 30 days
    
    Returns:
        List of business entity dictionaries
    """
    logger.info("Starting Delaware scraper")
    businesses = []
    
    try:
        # Delaware doesn't have date search - need to search by recent file numbers
        # or use alternative approach
        
        # Get recent entity numbers (Delaware assigns sequential file numbers)
        # We can search for entities and check their formation date
        
        # Strategy: Search alphabetically and filter by date
        # This is less efficient but works without API
        
        letters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
        
        for letter in letters:
            logger.info(f"Searching entities starting with '{letter}'")
            
            try:
                entities = search_by_name_prefix(letter)
                businesses.extend(entities)
                
            except Exception as e:
                logger.error(f"Error searching letter '{letter}': {e}")
                continue
        
        logger.info(f"Delaware scraper completed: {len(businesses)} businesses found")
        
    except Exception as e:
        logger.error(f"Delaware scraper error: {e}")
        return []
    
    return businesses

def search_by_name_prefix(prefix):
    """
    Search Delaware entities by name prefix
    
    Args:
        prefix: Letter or prefix to search
    
    Returns:
        List of business entities registered in last 30 days
    """
    businesses = []
    
    try:
        # Get search page first to get ViewState and other form fields
        response = safe_get(SEARCH_URL)
        if not response:
            return businesses
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extract ASP.NET ViewState (required for form submission)
        viewstate = soup.find('input', {'id': '__VIEWSTATE'})
        viewstate_value = viewstate['value'] if viewstate else ''
        
        viewstate_generator = soup.find('input', {'id': '__VIEWSTATEGENERATOR'})
        viewstate_gen_value = viewstate_generator['value'] if viewstate_generator else ''
        
        event_validation = soup.find('input', {'id': '__EVENTVALIDATION'})
        event_val_value = event_validation['value'] if event_validation else ''
        
        # Prepare search form data
        data = {
            '__VIEWSTATE': viewstate_value,
            '__VIEWSTATEGENERATOR': viewstate_gen_value,
            '__EVENTVALIDATION': event_val_value,
            'ctl00$ContentPlaceHolder1$txtEntityName': prefix,
            'ctl00$ContentPlaceHolder1$btnSubmit': 'Search',
        }
        
        # Submit search
        response = safe_post(SEARCH_URL, data=data)
        if not response:
            return businesses
        
        # Parse results
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find result table
        result_table = soup.find('table', {'id': 'gvResults'})
        if not result_table:
            return businesses
        
        rows = result_table.find_all('tr')[1:]  # Skip header
        
        for row in rows:
            try:
                cells = row.find_all('td')
                if len(cells) < 5:
                    continue
                
                file_number = cells[0].text.strip()
                entity_name = cells[1].text.strip()
                entity_type = cells[2].text.strip()
                
                # Get entity details page
                detail_link = cells[0].find('a')
                if detail_link:
                    detail_url = BASE_URL + detail_link['href']
                    entity_details = get_entity_details(detail_url)
                    
                    if entity_details and is_recent_registration(entity_details.get('formation_date', ''), days=30):
                        entity = create_business_entity(
                            name=entity_name,
                            state='DE',
                            entity_number=file_number,
                            registration_date=entity_details.get('formation_date', ''),
                            entity_type=entity_type,
                            status=entity_details.get('status', ''),
                            registered_agent=entity_details.get('registered_agent', ''),
                            address=entity_details.get('address', ''),
                        )
                        businesses.append(entity)
                
            except Exception as e:
                logger.error(f"Error parsing row: {e}")
                continue
        
    except Exception as e:
        logger.error(f"Error in search_by_name_prefix: {e}")
    
    return businesses

def get_entity_details(detail_url):
    """
    Get detailed information for a Delaware entity
    
    Args:
        detail_url: URL to entity details page
    
    Returns:
        Dictionary with entity details
    """
    try:
        response = safe_get(detail_url)
        if not response:
            return None
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        details = {}
        
        # Extract formation date
        formation_elem = soup.find('span', {'id': 'lblFormationDate'})
        if formation_elem:
            details['formation_date'] = formation_elem.text.strip()
        
        # Extract status
        status_elem = soup.find('span', {'id': 'lblStatus'})
        if status_elem:
            details['status'] = status_elem.text.strip()
        
        # Extract registered agent
        agent_elem = soup.find('span', {'id': 'lblAgentName'})
        if agent_elem:
            details['registered_agent'] = agent_elem.text.strip()
        
        # Extract address
        address_elem = soup.find('span', {'id': 'lblAgentAddress'})
        if address_elem:
            details['address'] = address_elem.text.strip()
        
        return details
        
    except Exception as e:
        logger.error(f"Error getting entity details: {e}")
        return None

if __name__ == "__main__":
    results = scrape()
    print(f"Found {len(results)} new Delaware businesses")
    for biz in results[:5]:
        print(f"  - {biz['name']} ({biz['entity_type']}) - {biz['registration_date']}")
