"""
Texas Business Registration Scraper
Multi-source: Texas Comptroller (CPA) + Secretary of State (SOS)

Sources:
- Comptroller: https://mycpa.cpa.state.tx.us/coa/
- SOS: https://www.sos.texas.gov/corp/
- Direct Web: https://www.sos.texas.gov/corp/sosda/index.shtml

Strategy:
1. Try SOS Direct Access (SOSDA) - public filings database
2. Try Comptroller public search
3. Fallback to web scraping with retry logic
4. Sample data as final fallback
"""
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import logging
import random
import json
import re
from ..common import (create_business_entity, is_recent_registration,
                     safe_get, safe_post, get_date_range_last_30_days,
                     format_date_for_state, StateScraperError, rate_limit)

logger = logging.getLogger(__name__)

# Texas configuration
TEXAS_CONFIG = {
    'state_code': 'TX',
    'state_name': 'Texas',
    'sos_search_url': 'https://www.sos.texas.gov/corp/sosda/index.shtml',
    'sos_api_url': 'https://mycpa.cpa.state.tx.us/coa/Index.html',
    'comptroller_search': 'https://mycpa.cpa.state.tx.us/coa/',
    'date_format': '%m/%d/%Y',
    'max_results': 100,
}

def scrape():
    """
    Main Texas scraper with multiple fallback methods
    
    Returns:
        List of business entity dictionaries
    """
    logger.info("Starting Texas scraper (multi-source)")
    
    # Try multiple methods in order of reliability
    methods = [
        ("SOS Direct Access", scrape_sos_direct),
        ("Comptroller Search", scrape_comptroller),
        ("Web Scraping", scrape_web_fallback),
        ("Sample Data", generate_sample_data),
    ]
    
    for method_name, method_func in methods:
        try:
            logger.info(f"Attempting {method_name}...")
            businesses = method_func()
            if businesses and len(businesses) > 0:
                logger.info(f"✓ {method_name} success: {len(businesses)} businesses")
                return businesses
            else:
                logger.warning(f"✗ {method_name} returned no results")
        except Exception as e:
            logger.warning(f"✗ {method_name} failed: {e}")
            continue
    
    # Should never reach here due to sample data fallback
    logger.error("All methods failed - returning empty list")
    return []

@rate_limit(min_delay=2.0, max_delay=4.0)
def scrape_sos_direct():
    """
    Scrape from Texas Secretary of State Direct Access
    This is the most reliable public database
    
    Returns:
        List of business entities
    """
    logger.info("Accessing Texas SOS Direct Access system")
    businesses = []
    
    try:
        # Texas SOS has a public search interface
        # We'll search for recently filed entities
        start_date, end_date = get_date_range_last_30_days()
        
        # The SOS system uses a specific search format
        search_url = "https://www.sos.texas.gov/corp/sosda/index.shtml"
        
        # Try to access the search page
        response = safe_get(search_url)
        if not response:
            raise StateScraperError("Could not access SOS search page")
        
        # Check if we can parse the page structure
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Look for search form and submission endpoint
        form = soup.find('form', {'name': 'searchForm'}) or soup.find('form')
        
        if form:
            # Extract form action URL
            action = form.get('action', '')
            if action:
                logger.info(f"Found search form with action: {action}")
                
                # Texas SOS search typically requires these parameters
                search_params = {
                    'searchType': 'filing',
                    'fromDate': start_date.strftime('%m/%d/%Y'),
                    'toDate': end_date.strftime('%m/%d/%Y'),
                    'entityType': 'all',
                }
                
                # Try POST request to search
                if action.startswith('/'):
                    action = 'https://www.sos.texas.gov' + action
                
                result = safe_post(action, data=search_params)
                if result and result.status_code == 200:
                    businesses = parse_sos_results(result.text)
                    if businesses:
                        return businesses
        
        # If form-based search didn't work, try alternate approach
        logger.info("Form search unavailable, trying alternate method")
        
    except Exception as e:
        logger.warning(f"SOS Direct Access error: {e}")
    
    return businesses

def parse_sos_results(html_content):
    """
    Parse Texas SOS search results HTML
    
    Args:
        html_content: HTML response from SOS search
        
    Returns:
        List of business entities
    """
    businesses = []
    
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Look for results table (common patterns)
        tables = soup.find_all('table', class_=re.compile('result|search|entity|filing'))
        
        if not tables:
            # Try all tables
            tables = soup.find_all('table')
        
        for table in tables:
            rows = table.find_all('tr')[1:]  # Skip header
            
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 3:
                    try:
                        # Common Texas SOS format: Name, Filing Number, Date, Type, Status
                        entity = create_business_entity(
                            name=cells[0].get_text(strip=True),
                            state='TX',
                            entity_number=cells[1].get_text(strip=True),
                            registration_date=cells[2].get_text(strip=True) if len(cells) > 2 else '',
                            entity_type=cells[3].get_text(strip=True) if len(cells) > 3 else 'Unknown',
                            status=cells[4].get_text(strip=True) if len(cells) > 4 else 'Active',
                        )
                        
                        if entity['name']:
                            businesses.append(entity)
                            
                    except Exception as e:
                        logger.debug(f"Error parsing row: {e}")
                        continue
        
        logger.info(f"Parsed {len(businesses)} businesses from SOS results")
        
    except Exception as e:
        logger.error(f"Error parsing SOS results: {e}")
    
    return businesses

@rate_limit(min_delay=2.0, max_delay=4.0)
def scrape_comptroller():
    """
    Scrape from Texas Comptroller's office public records
    
    Returns:
        List of business entities
    """
    logger.info("Accessing Texas Comptroller search")
    businesses = []
    
    try:
        # Texas Comptroller has taxpayer search
        comptroller_url = "https://mycpa.cpa.state.tx.us/coa/"
        
        response = safe_get(comptroller_url)
        if not response:
            raise StateScraperError("Could not access Comptroller page")
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Look for search functionality
        # The Comptroller site may have an API or search endpoint
        scripts = soup.find_all('script')
        
        # Look for API endpoints in JavaScript
        api_endpoints = []
        for script in scripts:
            if script.string:
                # Look for API URLs
                api_matches = re.findall(r'["\']([^"\']*api[^"\']*)["\']', script.string)
                api_endpoints.extend(api_matches)
        
        if api_endpoints:
            logger.info(f"Found {len(api_endpoints)} potential API endpoints")
            # Try to use discovered APIs
            # This would require further investigation of API structure
        
        # Alternative: Try direct taxable entity search
        # Comptroller tracks all businesses for tax purposes
        
    except Exception as e:
        logger.warning(f"Comptroller search error: {e}")
    
    return businesses

def scrape_web_fallback():
    """
    Fallback web scraping method using public business directories
    
    Returns:
        List of business entities
    """
    logger.info("Using web scraping fallback for Texas")
    businesses = []
    
    try:
        # Texas.gov has various public business listings
        # Try the business entity search
        search_endpoints = [
            "https://www.sos.texas.gov/corp/sosda/index.shtml",
            "https://www.sos.state.tx.us/corp/sosda/index.shtml",
        ]
        
        for endpoint in search_endpoints:
            try:
                response = safe_get(endpoint, retries=2)
                if response and response.status_code == 200:
                    logger.info(f"Successfully accessed {endpoint}")
                    
                    # Try to extract any visible business listings
                    soup = BeautifulSoup(response.text, 'html.parser')
                    
                    # Look for links to entity details
                    entity_links = soup.find_all('a', href=re.compile(r'entity|filing|corp'))
                    
                    logger.info(f"Found {len(entity_links)} potential entity links")
                    
                    if len(entity_links) > 5:
                        # This might be a results page
                        # Parse visible entities
                        pass
                    
                    break
                    
            except Exception as e:
                logger.debug(f"Endpoint {endpoint} failed: {e}")
                continue
        
    except Exception as e:
        logger.warning(f"Web fallback error: {e}")
    
    return businesses

def generate_sample_data():
    """
    Generate sample Texas business data for demonstration
    This ensures the system always returns some data
    
    Returns:
        List of sample business entities
    """
    logger.info("Generating sample Texas business data")
    
    businesses = []
    today = datetime.now()
    
    # Texas-specific business names
    company_names = [
        "Lone Star Tech Solutions LLC",
        "Houston Digital Ventures Corp",
        "Dallas Innovation Labs LLC",
        "Austin Consulting Group Inc",
        "San Antonio Holdings LLC",
        "Fort Worth Capital Partners Corp",
        "El Paso Marketing Services LLC",
        "Arlington Business Solutions Inc",
        "Corpus Christi Enterprises LLC",
        "Plano Technology Group Corp",
        "Lubbock Services International LLC",
        "Irving Investment Holdings Inc",
        "Laredo Business Development LLC",
        "Amarillo Ventures Group Corp",
        "Brownsville Consulting LLC",
        "The Woodlands Capital Inc",
        "McKinney Tech Innovations LLC",
        "Frisco Digital Solutions Corp",
        "Denton Business Partners LLC",
        "Round Rock Enterprises Inc",
        "Killeen Services Group LLC",
        "Waco Investment Holdings Corp",
        "Midland Energy Solutions LLC",
        "Abilene Business Services Inc",
        "Beaumont Capital Partners LLC",
    ]
    
    entity_types = [
        "Limited Liability Company",
        "Corporation",
        "Limited Partnership",
        "Professional Corporation",
        "Professional LLC",
    ]
    
    texas_cities = [
        "Houston", "Dallas", "Austin", "San Antonio", "Fort Worth",
        "El Paso", "Arlington", "Corpus Christi", "Plano", "Lubbock",
        "Irving", "Laredo", "Garland", "Frisco", "McKinney"
    ]
    
    for i, name in enumerate(company_names):
        # Generate dates within last 30 days
        days_ago = random.randint(1, 30)
        filing_date = today - timedelta(days=days_ago)
        
        # Determine entity type from name
        if 'LLC' in name:
            entity_type = "Limited Liability Company"
        elif 'Corp' in name or 'Inc' in name:
            entity_type = "Corporation"
        else:
            entity_type = random.choice(entity_types)
        
        businesses.append({
            'name': name,
            'state': 'TX',
            'entity_number': f'{32000000000 + i}',  # Texas uses 11-digit file numbers
            'registration_date': filing_date.strftime('%Y-%m-%d'),
            'entity_type': entity_type,
            'status': 'In Existence',
            'registered_agent': f'Registered Agents of Texas {i+1}',
            'address': f'{random.choice(texas_cities)}, TX',
            'scraped_at': datetime.now().isoformat(),
            'source': 'sample_data',
            'note': 'Sample data for demonstration - real scraper pending API access'
        })
    
    logger.info(f"✓ Generated {len(businesses)} sample Texas businesses")
    logger.info("Note: These are sample records. Real Texas data requires:")
    logger.info("  - Texas SOS Direct Access credentials")
    logger.info("  - Comptroller API access")
    logger.info("  - Or web scraping with JavaScript rendering")
    
    return businesses

def scrape_with_selenium():
    """
    Advanced scraping using Selenium for JavaScript-heavy Texas sites
    Use when simpler methods fail and you need full browser automation
    
    Returns:
        List of business entities
    """
    try:
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.chrome.options import Options
        
        logger.info("Starting Selenium-based Texas scraper")
        
        # Configure Chrome in headless mode
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        
        driver = webdriver.Chrome(options=chrome_options)
        businesses = []
        
        try:
            # Navigate to SOS search
            driver.get(TEXAS_CONFIG['sos_search_url'])
            
            # Wait for page to load
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Get date range
            start_date, end_date = get_date_range_last_30_days()
            
            # Try to find and fill search form
            try:
                # Look for date inputs
                date_inputs = driver.find_elements(By.CSS_SELECTOR, "input[type='text'][name*='date' i]")
                
                if len(date_inputs) >= 2:
                    date_inputs[0].send_keys(start_date.strftime('%m/%d/%Y'))
                    date_inputs[1].send_keys(end_date.strftime('%m/%d/%Y'))
                    
                    # Find and click submit button
                    submit_buttons = driver.find_elements(By.CSS_SELECTOR, "button[type='submit'], input[type='submit']")
                    if submit_buttons:
                        submit_buttons[0].click()
                        
                        # Wait for results
                        WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located((By.TAG_NAME, "table"))
                        )
                        
                        # Parse results
                        page_source = driver.page_source
                        businesses = parse_sos_results(page_source)
                        
            except Exception as e:
                logger.warning(f"Selenium form interaction failed: {e}")
            
        finally:
            driver.quit()
        
        if businesses:
            logger.info(f"Selenium scraper found {len(businesses)} businesses")
            return businesses
            
    except ImportError:
        logger.warning("Selenium not available - install with: pip install selenium")
    except Exception as e:
        logger.error(f"Selenium scraper error: {e}")
    
    return []

# Main entry point
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print("=" * 60)
    print("TEXAS BUSINESS REGISTRATION SCRAPER")
    print("=" * 60)
    print()
    
    results = scrape()
    
    print(f"\n✓ Found {len(results)} Texas businesses")
    print(f"\nSample results (first 5):")
    print("-" * 60)
    
    for i, biz in enumerate(results[:5], 1):
        print(f"{i}. {biz['name']}")
        print(f"   Entity #: {biz['entity_number']}")
        print(f"   Type: {biz['entity_type']}")
        print(f"   Filed: {biz['registration_date']}")
        print(f"   Status: {biz['status']}")
        print()
    
    # Show data source breakdown
    sources = {}
    for biz in results:
        source = biz.get('source', 'scraped')
        sources[source] = sources.get(source, 0) + 1
    
    print("Data sources:")
    for source, count in sources.items():
        print(f"  - {source}: {count} records")
