"""
Common utilities for state business registration scrapers
"""
import requests
import random
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# User agents for rotation
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
]

def get_random_user_agent() -> str:
    """Return a random user agent string"""
    return random.choice(USER_AGENTS)

def safe_get(url: str, params: Optional[Dict] = None, retries: int = 3, 
             headers: Optional[Dict] = None) -> Optional[requests.Response]:
    """
    Safely make GET request with retries and random delays
    
    Args:
        url: URL to request
        params: Query parameters
        retries: Number of retry attempts
        headers: Custom headers
    
    Returns:
        Response object or None if all retries fail
    """
    if headers is None:
        headers = {}
    
    headers['User-Agent'] = get_random_user_agent()
    
    for attempt in range(retries):
        try:
            logger.info(f"Requesting {url} (attempt {attempt + 1}/{retries})")
            response = requests.get(url, params=params, headers=headers, timeout=30)
            
            if response.status_code == 200:
                return response
            elif response.status_code == 429:  # Rate limited
                logger.warning(f"Rate limited. Waiting longer...")
                time.sleep(10 + random.random() * 5)
            else:
                logger.warning(f"Status code {response.status_code}")
                
        except requests.exceptions.Timeout:
            logger.warning(f"Timeout on attempt {attempt + 1}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error: {e}")
        
        # Random delay between retries
        if attempt < retries - 1:
            delay = 2 + random.random() * 2
            logger.info(f"Waiting {delay:.2f}s before retry...")
            time.sleep(delay)
    
    logger.error(f"All retries failed for {url}")
    return None

def safe_post(url: str, data: Optional[Dict] = None, json_data: Optional[Dict] = None,
              retries: int = 3, headers: Optional[Dict] = None) -> Optional[requests.Response]:
    """
    Safely make POST request with retries and random delays
    
    Args:
        url: URL to request
        data: Form data
        json_data: JSON data
        retries: Number of retry attempts
        headers: Custom headers
    
    Returns:
        Response object or None if all retries fail
    """
    if headers is None:
        headers = {}
    
    headers['User-Agent'] = get_random_user_agent()
    
    for attempt in range(retries):
        try:
            logger.info(f"POST to {url} (attempt {attempt + 1}/{retries})")
            response = requests.post(url, data=data, json=json_data, 
                                   headers=headers, timeout=30)
            
            if response.status_code == 200:
                return response
            elif response.status_code == 429:
                logger.warning(f"Rate limited. Waiting longer...")
                time.sleep(10 + random.random() * 5)
            else:
                logger.warning(f"Status code {response.status_code}")
                
        except requests.exceptions.Timeout:
            logger.warning(f"Timeout on attempt {attempt + 1}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error: {e}")
        
        if attempt < retries - 1:
            delay = 2 + random.random() * 2
            time.sleep(delay)
    
    logger.error(f"All retries failed for {url}")
    return None

def get_date_range_last_30_days() -> tuple:
    """
    Get start and end dates for last 30 days
    
    Returns:
        Tuple of (start_date, end_date) as datetime objects
    """
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    return start_date, end_date

def format_date_for_state(date: datetime, state_format: str) -> str:
    """
    Format date according to state requirements
    
    Args:
        date: datetime object
        state_format: Format string (e.g., '%m/%d/%Y', '%Y-%m-%d')
    
    Returns:
        Formatted date string
    """
    return date.strftime(state_format)

def normalize_business_entity(raw_data: Dict) -> Dict:
    """
    Normalize business entity data to standard format
    
    Args:
        raw_data: Raw scraped data with varying field names
    
    Returns:
        Standardized entity dictionary
    """
    # Map common field variations to standard names
    field_mapping = {
        'entity_name': ['name', 'business_name', 'entity_name', 'company_name'],
        'entity_number': ['number', 'file_number', 'entity_number', 'registration_number', 'filing_number'],
        'registration_date': ['date', 'registration_date', 'formation_date', 'filing_date', 'incorporation_date'],
        'entity_type': ['type', 'entity_type', 'business_type', 'classification'],
        'status': ['status', 'entity_status', 'business_status'],
        'registered_agent': ['agent', 'registered_agent', 'agent_name'],
        'address': ['address', 'principal_address', 'business_address', 'office_address'],
        'state': ['state', 'jurisdiction'],
    }
    
    normalized = {}
    
    for standard_field, variations in field_mapping.items():
        for variation in variations:
            if variation in raw_data:
                normalized[standard_field] = raw_data[variation]
                break
    
    return normalized

def create_business_entity(name: str, state: str, entity_number: str = "", 
                          registration_date: str = "", entity_type: str = "",
                          status: str = "", registered_agent: str = "",
                          address: str = "", **kwargs) -> Dict:
    """
    Create standardized business entity dictionary
    
    Args:
        name: Business name
        state: 2-letter state code
        entity_number: State filing number
        registration_date: Date of registration
        entity_type: Type of entity (LLC, Corp, etc.)
        status: Current status
        registered_agent: Registered agent name
        address: Business address
        **kwargs: Additional fields
    
    Returns:
        Standardized entity dictionary
    """
    entity = {
        'name': name,
        'state': state.upper(),
        'entity_number': entity_number,
        'registration_date': registration_date,
        'entity_type': entity_type,
        'status': status,
        'registered_agent': registered_agent,
        'address': address,
        'scraped_at': datetime.now().isoformat(),
    }
    
    # Add any additional fields
    entity.update(kwargs)
    
    return entity

def is_recent_registration(date_str: str, days: int = 30) -> bool:
    """
    Check if registration date is within the last N days
    
    Args:
        date_str: Date string in various formats
        days: Number of days to check
    
    Returns:
        True if within date range, False otherwise
    """
    # Try common date formats
    formats = [
        '%Y-%m-%d',
        '%m/%d/%Y',
        '%d/%m/%Y',
        '%Y/%m/%d',
        '%m-%d-%Y',
        '%B %d, %Y',
        '%b %d, %Y',
    ]
    
    for fmt in formats:
        try:
            reg_date = datetime.strptime(date_str, fmt)
            cutoff_date = datetime.now() - timedelta(days=days)
            return reg_date >= cutoff_date
        except ValueError:
            continue
    
    logger.warning(f"Could not parse date: {date_str}")
    return False

def rate_limit(min_delay: float = 1.0, max_delay: float = 3.0):
    """
    Decorator to add rate limiting to functions
    
    Args:
        min_delay: Minimum delay in seconds
        max_delay: Maximum delay in seconds
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            delay = random.uniform(min_delay, max_delay)
            logger.debug(f"Rate limiting: waiting {delay:.2f}s")
            time.sleep(delay)
            return result
        return wrapper
    return decorator

class StateScraperError(Exception):
    """Custom exception for scraper errors"""
    pass
