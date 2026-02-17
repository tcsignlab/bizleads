"""
Florida Business Registration Scraper
Uses FTP access to daily filing data from sunbiz.org
"""
from ftplib import FTP
from datetime import datetime, timedelta
from io import BytesIO
import csv
import logging
from ..common import create_business_entity, is_recent_registration, StateScraperError

logger = logging.getLogger(__name__)

# Florida FTP credentials (public access)
FTP_HOST = "ftp.dos.state.fl.us"
FTP_USER = "public"
FTP_PASS = "PubAccess1845!"
FTP_PATH = "/sunbiz/"

def scrape():
    """
    Scrape new Florida business registrations from last 30 days
    
    Returns:
        List of business entity dictionaries
    """
    logger.info("Starting Florida scraper (FTP method)")
    businesses = []
    
    try:
        # Connect to FTP
        ftp = FTP(FTP_HOST)
        ftp.login(FTP_USER, FTP_PASS)
        logger.info(f"Connected to {FTP_HOST}")
        
        # Change to sunbiz directory
        ftp.cwd(FTP_PATH)
        
        # Get list of daily files from last 30 days
        files = []
        ftp.retrlines('NLST', files.append)
        logger.info(f"Found {len(files)} files in directory")
        
        # Filter for daily files (format: cor_YYYYMMDD.txt or similar)
        daily_files = [f for f in files if 'cor_' in f.lower() and f.endswith('.txt')]
        logger.info(f"Found {len(daily_files)} daily filing files")
        
        # Process recent daily files (last 30 days)
        cutoff_date = datetime.now() - timedelta(days=30)
        
        for filename in sorted(daily_files, reverse=True)[:30]:  # Get last 30 files
            try:
                logger.info(f"Processing file: {filename}")
                
                # Download file content
                file_content = BytesIO()
                ftp.retrbinary(f'RETR {filename}', file_content.write)
                file_content.seek(0)
                
                # Parse CSV/TXT content
                content = file_content.read().decode('utf-8', errors='ignore')
                reader = csv.DictReader(content.splitlines())
                
                for row in reader:
                    # Extract business data
                    # Note: Actual column names may vary - adjust based on real data
                    entity = create_business_entity(
                        name=row.get('CORP_NAME', row.get('NAME', '')),
                        state='FL',
                        entity_number=row.get('DOCUMENT_NUMBER', row.get('FEI_EIN_NUMBER', '')),
                        registration_date=row.get('FIL_DATE', row.get('DATE_FILED', '')),
                        entity_type=row.get('CORP_TYPE_DESC', row.get('TYPE', '')),
                        status=row.get('CORP_STATUS_DESC', row.get('STATUS', 'Active')),
                        registered_agent=row.get('AGENT_NAME', ''),
                        address=f"{row.get('MAIL_ADDR1', '')} {row.get('MAIL_CITY', '')} {row.get('MAIL_STATE', '')} {row.get('MAIL_ZIP', '')}".strip(),
                        principal_address=row.get('PRINCIPAL_ADDRESS', ''),
                        mailing_address=row.get('MAILING_ADDRESS', ''),
                    )
                    
                    # Only include if registered in last 30 days
                    if entity['registration_date'] and is_recent_registration(entity['registration_date'], days=30):
                        businesses.append(entity)
                
                logger.info(f"Processed {filename}: found {len(businesses)} new registrations")
                
            except Exception as e:
                logger.error(f"Error processing file {filename}: {e}")
                continue
        
        ftp.quit()
        logger.info(f"Florida scraper completed: {len(businesses)} businesses found")
        
    except Exception as e:
        logger.error(f"Florida scraper error: {e}")
        raise StateScraperError(f"Florida scraping failed: {e}")
    
    return businesses

def scrape_by_search():
    """
    Alternative method: Scrape Florida sunbiz.org search interface
    Use this as fallback if FTP method fails
    
    Returns:
        List of business entity dictionaries
    """
    from bs4 import BeautifulSoup
    from ..common import safe_get, safe_post, get_date_range_last_30_days, format_date_for_state
    
    logger.info("Starting Florida scraper (web search method)")
    businesses = []
    
    try:
        # Base URL for sunbiz search
        search_url = "https://search.sunbiz.org/Inquiry/CorporationSearch/ByName"
        
        # Get search page
        response = safe_get(search_url)
        if not response:
            raise StateScraperError("Could not access sunbiz search page")
        
        # Note: This would require more complex scraping of the search interface
        # The FTP method is preferred as it's more reliable and doesn't require
        # navigating complex web forms
        
        logger.info("Web search method not fully implemented - use FTP method")
        
    except Exception as e:
        logger.error(f"Florida web scraper error: {e}")
        raise StateScraperError(f"Florida web scraping failed: {e}")
    
    return businesses

# Use FTP method by default
if __name__ == "__main__":
    results = scrape()
    print(f"Found {len(results)} new Florida businesses")
    for biz in results[:5]:  # Print first 5
        print(f"  - {biz['name']} ({biz['entity_type']}) - {biz['registration_date']}")
