"""
California Business Registration Scraper
Uses California SOS API for business entity data
"""
import os
from datetime import datetime, timedelta
import logging
from ..common import (create_business_entity, is_recent_registration, 
                     safe_get, safe_post, StateScraperError)

logger = logging.getLogger(__name__)

# California API endpoints
API_BASE_URL = "https://api.sos.ca.gov/business"
API_SEARCH_URL = f"{API_BASE_URL}/search"
API_ENTITY_URL = f"{API_BASE_URL}/entity"

# Note: API key should be stored in environment variable
# Register at: https://api.management.sos.ca.gov/
API_KEY = os.environ.get('CA_SOS_API_KEY', '')

def scrape():
    """
    Scrape new California business registrations from last 30 days
    
    Returns:
        List of business entity dictionaries
    """
    logger.info("Starting California scraper (API method)")
    
    if not API_KEY:
        logger.warning("California API key not found. Falling back to web scraper.")
        return scrape_by_search()
    
    businesses = []
    
    try:
        # Set up API headers
        headers = {
            'Ocp-Apim-Subscription-Key': API_KEY,
            'Content-Type': 'application/json'
        }
        
        # Get date range
        start_date = datetime.now() - timedelta(days=30)
        end_date = datetime.now()
        
        # Search for new entities
        # Note: Exact parameters may vary based on API documentation
        params = {
            'filingDateStart': start_date.strftime('%Y-%m-%d'),
            'filingDateEnd': end_date.strftime('%Y-%m-%d'),
            'pageSize': 150,  # Max results per page
            'page': 1
        }
        
        page = 1
        while True:
            params['page'] = page
            logger.info(f"Fetching page {page}")
            
            response = safe_get(API_SEARCH_URL, params=params, headers=headers)
            
            if not response:
                logger.error("Failed to get API response")
                break
            
            try:
                data = response.json()
            except ValueError:
                logger.error("Invalid JSON response")
                break
            
            entities = data.get('entities', [])
            if not entities:
                logger.info("No more entities found")
                break
            
            for entity_data in entities:
                entity = create_business_entity(
                    name=entity_data.get('entityName', ''),
                    state='CA',
                    entity_number=entity_data.get('entityNumber', '').replace('C', ''),  # Remove C prefix
                    registration_date=entity_data.get('filingDate', ''),
                    entity_type=entity_data.get('entityType', ''),
                    status=entity_data.get('status', ''),
                    registered_agent=entity_data.get('agentName', ''),
                    address=entity_data.get('address', ''),
                    jurisdiction=entity_data.get('jurisdiction', ''),
                )
                businesses.append(entity)
            
            logger.info(f"Page {page}: found {len(entities)} entities")
            
            # Check if there are more pages
            if len(entities) < params['pageSize']:
                break
            
            page += 1
        
        logger.info(f"California API scraper completed: {len(businesses)} businesses found")
        
    except Exception as e:
        logger.error(f"California API scraper error: {e}")
        raise StateScraperError(f"California API scraping failed: {e}")
    
    return businesses

def scrape_by_search():
    """
    Alternative method: Scrape California bizfileonline.sos.ca.gov search interface
    Use this as fallback if API method fails or no API key available
    
    Returns:
        List of business entity dictionaries
    """
    from bs4 import BeautifulSoup
    
    logger.info("Starting California scraper (web search method)")
    businesses = []
    
    try:
        # Base URL for California business search
        search_url = "https://bizfileonline.sos.ca.gov/search/business"
        
        # Get date range
        start_date = datetime.now() - timedelta(days=30)
        end_date = datetime.now()
        
        # California's search form parameters (may need adjustment based on actual form)
        search_data = {
            'SearchType': 'CORP',
            'SearchCriteria': '',
            'SearchSubType': 'Keyword',
        }
        
        response = safe_post(search_url, data=search_data)
        
        if not response:
            logger.error("Failed to access California search page")
            return []
        
        # Parse results
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Note: Actual parsing logic depends on California's HTML structure
        # This is a template that needs to be filled in with real selectors
        result_rows = soup.find_all('tr', class_='search-result')
        
        for row in result_rows:
            try:
                # Extract data from HTML (adjust selectors based on actual page)
                name = row.find('td', class_='entity-name').text.strip()
                entity_number = row.find('td', class_='entity-number').text.strip()
                reg_date = row.find('td', class_='filing-date').text.strip()
                entity_type = row.find('td', class_='entity-type').text.strip()
                status = row.find('td', class_='status').text.strip()
                
                if is_recent_registration(reg_date, days=30):
                    entity = create_business_entity(
                        name=name,
                        state='CA',
                        entity_number=entity_number,
                        registration_date=reg_date,
                        entity_type=entity_type,
                        status=status,
                    )
                    businesses.append(entity)
                    
            except Exception as e:
                logger.error(f"Error parsing row: {e}")
                continue
        
        logger.info(f"California web scraper completed: {len(businesses)} businesses found")
        
    except Exception as e:
        logger.error(f"California web scraper error: {e}")
        raise StateScraperError(f"California web scraping failed: {e}")
    
    return businesses

if __name__ == "__main__":
    results = scrape()
    print(f"Found {len(results)} new California businesses")
    for biz in results[:5]:
        print(f"  - {biz['name']} ({biz['entity_type']}) - {biz['registration_date']}")
