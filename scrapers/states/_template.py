"""
Template for Generic State Business Registration Scraper
Use this template for states with standard web search interfaces
"""
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import logging
from ..common import (create_business_entity, is_recent_registration,
                     safe_get, safe_post, get_date_range_last_30_days,
                     format_date_for_state, StateScraperError, rate_limit)

logger = logging.getLogger(__name__)

# State configuration (customize per state)
STATE_CONFIG = {
    'state_code': 'XX',  # 2-letter state code
    'state_name': 'State Name',
    'search_url': 'https://sos.state.xx.gov/business/search',
    'search_method': 'GET',  # or 'POST'
    'date_format': '%m/%d/%Y',  # Adjust based on state
    'has_pagination': True,
    'requires_js': False,  # Set True if needs Playwright
}

@rate_limit(min_delay=2.0, max_delay=4.0)
def scrape():
    """
    Scrape new business registrations from last 30 days
    
    Returns:
        List of business entity dictionaries
    """
    state_code = STATE_CONFIG['state_code']
    logger.info(f"Starting {STATE_CONFIG['state_name']} scraper")
    
    businesses = []
    
    try:
        # Get date range
        start_date, end_date = get_date_range_last_30_days()
        
        # Format dates for this state
        start_str = format_date_for_state(start_date, STATE_CONFIG['date_format'])
        end_str = format_date_for_state(end_date, STATE_CONFIG['date_format'])
        
        # Prepare search parameters
        if STATE_CONFIG['search_method'] == 'GET':
            params = {
                'startDate': start_str,
                'endDate': end_str,
                'searchType': 'filingDate',  # Adjust based on state
            }
            response = safe_get(STATE_CONFIG['search_url'], params=params)
        else:  # POST
            data = {
                'startDate': start_str,
                'endDate': end_str,
                'searchType': 'filingDate',
            }
            response = safe_post(STATE_CONFIG['search_url'], data=data)
        
        if not response:
            raise StateScraperError(f"Could not access {state_code} search page")
        
        # Parse results
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extract businesses from results
        # NOTE: Selectors must be customized per state
        result_rows = soup.find_all('tr', class_='result-row')  # Adjust selector
        
        for row in result_rows:
            try:
                entity = extract_entity_from_row(row, state_code)
                if entity and is_recent_registration(entity['registration_date'], days=30):
                    businesses.append(entity)
            except Exception as e:
                logger.error(f"Error parsing row: {e}")
                continue
        
        # Handle pagination if needed
        if STATE_CONFIG['has_pagination']:
            businesses.extend(scrape_additional_pages(soup, state_code))
        
        logger.info(f"{state_code} scraper completed: {len(businesses)} businesses found")
        
    except Exception as e:
        logger.error(f"{state_code} scraper error: {e}")
        # Don't raise - allow other states to continue
        return []
    
    return businesses

def extract_entity_from_row(row, state_code):
    """
    Extract business entity data from HTML row
    Customize selectors based on state's HTML structure
    
    Args:
        row: BeautifulSoup row element
        state_code: 2-letter state code
    
    Returns:
        Business entity dictionary
    """
    try:
        # Customize these selectors based on actual HTML
        name_elem = row.find('td', class_='entity-name')
        number_elem = row.find('td', class_='entity-number')
        date_elem = row.find('td', class_='filing-date')
        type_elem = row.find('td', class_='entity-type')
        status_elem = row.find('td', class_='status')
        
        # Alternative: use column indices
        # cells = row.find_all('td')
        # name = cells[0].text.strip()
        # number = cells[1].text.strip()
        # etc.
        
        entity = create_business_entity(
            name=name_elem.text.strip() if name_elem else '',
            state=state_code,
            entity_number=number_elem.text.strip() if number_elem else '',
            registration_date=date_elem.text.strip() if date_elem else '',
            entity_type=type_elem.text.strip() if type_elem else '',
            status=status_elem.text.strip() if status_elem else '',
        )
        
        return entity
        
    except Exception as e:
        logger.error(f"Error extracting entity: {e}")
        return None

def scrape_additional_pages(initial_soup, state_code):
    """
    Handle pagination to get all results
    
    Args:
        initial_soup: BeautifulSoup object of first page
        state_code: 2-letter state code
    
    Returns:
        List of business entities from additional pages
    """
    businesses = []
    
    try:
        # Find pagination links
        pagination = initial_soup.find('div', class_='pagination')
        if not pagination:
            return businesses
        
        # Get all page links
        page_links = pagination.find_all('a', class_='page-link')
        
        for link in page_links[1:]:  # Skip first page (already processed)
            page_url = link.get('href')
            if not page_url:
                continue
            
            # Handle relative URLs
            if not page_url.startswith('http'):
                base_url = '/'.join(STATE_CONFIG['search_url'].split('/')[:3])
                page_url = base_url + page_url
            
            response = safe_get(page_url)
            if not response:
                continue
            
            soup = BeautifulSoup(response.text, 'html.parser')
            result_rows = soup.find_all('tr', class_='result-row')
            
            for row in result_rows:
                try:
                    entity = extract_entity_from_row(row, state_code)
                    if entity and is_recent_registration(entity['registration_date'], days=30):
                        businesses.append(entity)
                except Exception as e:
                    logger.error(f"Error parsing pagination row: {e}")
                    continue
        
    except Exception as e:
        logger.error(f"Pagination error: {e}")
    
    return businesses

def scrape_with_playwright():
    """
    Alternative scraper using Playwright for JavaScript-heavy sites
    Use when STATE_CONFIG['requires_js'] is True
    
    Returns:
        List of business entity dictionaries
    """
    from playwright.sync_api import sync_playwright
    
    logger.info(f"Starting {STATE_CONFIG['state_name']} scraper (Playwright)")
    businesses = []
    
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            
            # Navigate to search page
            page.goto(STATE_CONFIG['search_url'])
            
            # Get date range
            start_date, end_date = get_date_range_last_30_days()
            start_str = format_date_for_state(start_date, STATE_CONFIG['date_format'])
            end_str = format_date_for_state(end_date, STATE_CONFIG['date_format'])
            
            # Fill in search form (customize selectors)
            page.fill('#startDate', start_str)
            page.fill('#endDate', end_str)
            page.click('button[type="submit"]')
            
            # Wait for results
            page.wait_for_selector('.result-row', timeout=10000)
            
            # Extract results
            html = page.content()
            soup = BeautifulSoup(html, 'html.parser')
            
            result_rows = soup.find_all('tr', class_='result-row')
            
            for row in result_rows:
                try:
                    entity = extract_entity_from_row(row, STATE_CONFIG['state_code'])
                    if entity and is_recent_registration(entity['registration_date'], days=30):
                        businesses.append(entity)
                except Exception as e:
                    logger.error(f"Error parsing row: {e}")
                    continue
            
            browser.close()
        
        logger.info(f"Playwright scraper completed: {len(businesses)} businesses found")
        
    except Exception as e:
        logger.error(f"Playwright scraper error: {e}")
        return []
    
    return businesses

# Export main scrape function
if __name__ == "__main__":
    if STATE_CONFIG['requires_js']:
        results = scrape_with_playwright()
    else:
        results = scrape()
    
    print(f"Found {len(results)} new {STATE_CONFIG['state_name']} businesses")
    for biz in results[:5]:
        print(f"  - {biz['name']} ({biz['entity_type']}) - {biz['registration_date']}")
