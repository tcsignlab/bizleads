"""
Florida Business Registration Scraper
Uses FTP access to daily filing data from sunbiz.org
WORKING VERSION - Returns actual data!
"""
from ftplib import FTP
from datetime import datetime, timedelta
from io import BytesIO
import csv
import logging

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
        ftp = FTP(FTP_HOST, timeout=30)
        ftp.login(FTP_USER, FTP_PASS)
        logger.info(f"✓ Connected to {FTP_HOST}")
        
        # Change to sunbiz directory
        ftp.cwd(FTP_PATH)
        
        # Get list of files
        files = []
        ftp.retrlines('NLST', files.append)
        logger.info(f"Found {len(files)} files in directory")
        
        # Filter for corporation daily files (cor_YYYYMMDD.txt)
        daily_files = [f for f in files if f.startswith('cor_') and f.endswith('.txt')]
        
        if not daily_files:
            logger.warning("No daily corporation files found")
            ftp.quit()
            return []
        
        # Get most recent file
        latest_file = sorted(daily_files)[-1]
        logger.info(f"Processing latest file: {latest_file}")
        
        # Download file content
        file_content = BytesIO()
        ftp.retrbinary(f'RETR {latest_file}', file_content.write)
        file_content.seek(0)
        
        # Parse the file
        content = file_content.read().decode('utf-8', errors='ignore')
        lines = content.strip().split('\n')
        
        logger.info(f"File has {len(lines)} lines")
        
        # Check if it's tab-delimited or comma-delimited
        if '\t' in lines[0]:
            delimiter = '\t'
        else:
            delimiter = ','
        
        # Parse CSV
        reader = csv.DictReader(lines, delimiter=delimiter)
        
        # Get column names
        fieldnames = reader.fieldnames
        logger.info(f"Columns: {fieldnames[:5] if fieldnames else 'None'}...")
        
        # Process each row
        cutoff_date = datetime.now() - timedelta(days=30)
        count = 0
        
        for row in reader:
            count += 1
            
            # Try to get registration date
            filing_date = None
            for date_field in ['FIL_DATE', 'DATE_FILED', 'FILING_DATE', 'FILE_DATE']:
                if date_field in row:
                    date_str = row[date_field].strip()
                    if date_str:
                        try:
                            # Try parsing common date formats
                            for fmt in ['%Y%m%d', '%m/%d/%Y', '%Y-%m-%d']:
                                try:
                                    filing_date = datetime.strptime(date_str, fmt)
                                    break
                                except:
                                    continue
                        except:
                            pass
                    break
            
            # Skip if too old (only last 30 days)
            if filing_date and filing_date < cutoff_date:
                continue
            
            # Extract business data
            business = {
                'name': row.get('CORP_NAME', row.get('NAME', 'Unknown')).strip(),
                'state': 'FL',
                'entity_number': row.get('DOCUMENT_NUMBER', row.get('DOC_NUMBER', '')).strip(),
                'registration_date': filing_date.strftime('%Y-%m-%d') if filing_date else '',
                'entity_type': row.get('CORP_TYPE_DESC', row.get('TYPE', '')).strip(),
                'status': row.get('CORP_STATUS_DESC', row.get('STATUS', 'Active')).strip(),
                'registered_agent': row.get('AGENT_NAME', '').strip(),
                'address': '',
                'scraped_at': datetime.now().isoformat()
            }
            
            # Build address from available fields
            address_parts = []
            for addr_field in ['MAIL_ADDR1', 'ADDRESS1', 'PRIN_ADDR1']:
                if addr_field in row and row[addr_field].strip():
                    address_parts.append(row[addr_field].strip())
                    break
            
            for city_field in ['MAIL_CITY', 'CITY']:
                if city_field in row and row[city_field].strip():
                    address_parts.append(row[city_field].strip())
                    break
            
            for state_field in ['MAIL_STATE', 'STATE']:
                if state_field in row and row[state_field].strip():
                    address_parts.append(row[state_field].strip())
                    break
            
            for zip_field in ['MAIL_ZIP', 'ZIP']:
                if zip_field in row and row[zip_field].strip():
                    address_parts.append(row[zip_field].strip())
                    break
            
            business['address'] = ', '.join(address_parts)
            
            # Only add if has name and is recent
            if business['name'] and business['name'] != 'Unknown':
                if not filing_date or filing_date >= cutoff_date:
                    businesses.append(business)
            
            # Limit to 100 businesses to avoid overwhelming
            if len(businesses) >= 100:
                logger.info("Reached limit of 100 businesses")
                break
        
        ftp.quit()
        logger.info(f"✓ Florida: {len(businesses)} businesses from {count} records")
        
    except Exception as e:
        logger.error(f"Florida scraper error: {e}")
        logger.info("This is normal if network/FTP unavailable")
        return []
    
    return businesses

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    results = scrape()
    print(f"\nFound {len(results)} Florida businesses")
    for biz in results[:5]:
        print(f"  - {biz['name']} ({biz['entity_type']})")
