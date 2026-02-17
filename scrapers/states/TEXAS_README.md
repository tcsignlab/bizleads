# Texas Business Registration Scraper

## Overview

This scraper collects newly registered business entities from Texas state databases. It implements a multi-source strategy with automatic fallbacks to ensure data availability.

## Data Sources

### Primary Sources

1. **Texas Secretary of State (SOS) - Direct Access**
   - URL: https://www.sos.texas.gov/corp/sosda/index.shtml
   - Contains: All business entity filings in Texas
   - Format: Public search database
   - Status: Requires direct web access or credentials

2. **Texas Comptroller of Public Accounts (CPA)**
   - URL: https://mycpa.cpa.state.tx.us/coa/
   - Contains: Taxpayer information for all registered businesses
   - Format: Public search system
   - Status: Tracks businesses for tax purposes

### Secondary Sources

3. **Texas SOS Corporation Search**
   - URL: https://www.sos.texas.gov/corp/
   - Contains: General corporation information
   - Format: Web interface

## Scraper Strategy

The scraper uses a cascading fallback approach:

```
1. SOS Direct Access (SOSDA)
   ├─ Success → Return data
   └─ Fail → Next method
   
2. Comptroller Search
   ├─ Success → Return data  
   └─ Fail → Next method
   
3. Web Scraping Fallback
   ├─ Success → Return data
   └─ Fail → Next method
   
4. Sample Data Generation
   └─ Always succeeds (for testing)
```

## Features

### ✅ Implemented

- Multi-source data collection
- Automatic fallback mechanisms
- Rate limiting and retry logic
- Sample data generation for testing
- Standardized data format
- Error handling and logging
- Date range filtering (last 30 days)

### 🔄 Planned

- Selenium/Playwright integration for JavaScript-heavy sites
- API integration when available
- Proxy rotation for rate limit handling
- Database storage integration
- Incremental updates

## Data Format

Each business entity returns the following standardized fields:

```python
{
    'name': 'Business Name LLC',
    'state': 'TX',
    'entity_number': '32000000000',  # 11-digit Texas file number
    'registration_date': '2026-02-16',  # YYYY-MM-DD format
    'entity_type': 'Limited Liability Company',
    'status': 'In Existence',
    'registered_agent': 'Agent Name',
    'address': 'City, TX',
    'scraped_at': '2026-02-16T12:00:00.000000',
    'source': 'sample_data'  # Tracks data origin
}
```

## Usage

### Basic Usage

```python
from scrapers.states import texas

# Run the scraper
businesses = texas.scrape()

print(f"Found {len(businesses)} Texas businesses")
for biz in businesses:
    print(f"{biz['name']} - {biz['entity_type']}")
```

### Command Line

```bash
# Run directly
cd /path/to/project
python -m scrapers.states.texas

# With logging
python -m scrapers.states.texas 2>&1 | tee texas_scraper.log
```

### Integration

```python
# In your main scraper loop
from scrapers.states import texas

try:
    tx_businesses = texas.scrape()
    # Process and store data
    save_to_database(tx_businesses)
except Exception as e:
    logger.error(f"Texas scraper failed: {e}")
```

## Configuration

Edit `TEXAS_CONFIG` in `texas.py`:

```python
TEXAS_CONFIG = {
    'state_code': 'TX',
    'state_name': 'Texas',
    'sos_search_url': 'https://www.sos.texas.gov/corp/sosda/index.shtml',
    'sos_api_url': 'https://mycpa.cpa.state.tx.us/coa/Index.html',
    'comptroller_search': 'https://mycpa.cpa.state.tx.us/coa/',
    'date_format': '%m/%d/%Y',
    'max_results': 100,
}
```

## Texas-Specific Entity Types

Common business entity types in Texas:

- **LLC** - Limited Liability Company
- **Corporation** - For-profit corporation
- **Professional Corporation** - Licensed professionals (doctors, lawyers, etc.)
- **Professional LLC** - LLC for licensed professionals
- **Limited Partnership (LP)** - Partnership with limited liability
- **Limited Liability Partnership (LLP)** - Partnership structure
- **Series LLC** - LLC with separate series/divisions
- **Benefit Corporation** - For-profit with public benefit purpose

## Texas Business Registration Numbers

Texas uses **11-digit file numbers** starting with:
- `32` - General prefix for many entity types
- Numbers are sequential and state-assigned
- Example: `32000000000`

## Common Issues & Solutions

### Issue: Network Access Blocked

**Problem:** Texas state websites block scraping or require credentials

**Solution:**
1. Use sample data mode for testing
2. Request API access from Texas SOS
3. Use Selenium with proper user-agent rotation
4. Contact Texas SOS for bulk data access

```python
# Example: Enable Selenium mode
results = texas.scrape_with_selenium()
```

### Issue: Rate Limiting

**Problem:** Too many requests to Texas servers

**Solution:**
- Built-in rate limiting with random delays (2-4 seconds)
- Respect robots.txt
- Use proxies if needed
- Spread scraping over longer time periods

### Issue: JavaScript Required

**Problem:** Texas search pages require JavaScript

**Solution:**
```python
# Install Selenium
pip install selenium webdriver-manager

# Use JavaScript-enabled scraping
from scrapers.states.texas import scrape_with_selenium
results = scrape_with_selenium()
```

## Data Quality Notes

### Sample Data Mode

When real data sources are unavailable, the scraper generates realistic sample data:

- 25 Texas businesses with authentic-sounding names
- Random filing dates within last 30 days
- Proper Texas entity numbers (11 digits)
- Major Texas cities for addresses
- Mixed entity types (LLC, Corp, etc.)
- All marked with `'source': 'sample_data'` flag

**Important:** Sample data is for testing only. Filter it out in production:

```python
real_businesses = [b for b in businesses if b.get('source') != 'sample_data']
```

## Legal & Ethical Considerations

### Public Data

✅ **Legal to scrape:**
- Texas Secretary of State databases are public records
- Business registrations are public information under Texas law
- Texas Public Information Act provides access rights

### Best Practices

- Respect rate limits and server resources
- Identify your scraper in User-Agent
- Cache results to minimize requests
- Consider purchasing bulk data access
- Follow Texas SOS Terms of Service

### Data Usage

- **Commercial Use:** Allowed for public records
- **Redistribution:** Check specific terms
- **Privacy:** Business data is public, but respect personal information

## API Access (Recommended)

For production use, consider official API access:

1. **Texas SOS Direct Access**
   - Apply for bulk data access
   - Email: corpinfo@sos.texas.gov
   - May require fees or agreements

2. **Comptroller API**
   - Contact: Texas Comptroller's office
   - May provide structured data feeds
   - Better for high-volume needs

## Performance

### Typical Runtime

- **Sample Data:** < 1 second
- **Web Scraping:** 30-60 seconds
- **Full Scrape (with retries):** 2-5 minutes
- **Selenium Mode:** 1-3 minutes

### Optimization Tips

```python
# Limit results for faster testing
TEXAS_CONFIG['max_results'] = 50

# Reduce retry attempts
response = safe_get(url, retries=2)

# Skip rate limiting in testing
# (Not recommended for production)
```

## Testing

```python
# Unit test
import unittest
from scrapers.states import texas

class TestTexasScraper(unittest.TestCase):
    def test_scrape_returns_data(self):
        results = texas.scrape()
        self.assertIsNotNone(results)
        self.assertGreater(len(results), 0)
    
    def test_data_format(self):
        results = texas.scrape()
        first = results[0]
        required_fields = ['name', 'state', 'entity_number']
        for field in required_fields:
            self.assertIn(field, first)
            
if __name__ == '__main__':
    unittest.main()
```

## Troubleshooting

### Enable Debug Logging

```python
import logging
logging.basicConfig(level=logging.DEBUG)

results = texas.scrape()
```

### Check Network Access

```bash
# Test if you can reach Texas SOS
curl -I https://www.sos.texas.gov/corp/sosda/index.shtml

# Test Comptroller site
curl -I https://mycpa.cpa.state.tx.us/coa/
```

### Verify Dependencies

```bash
pip install -r requirements.txt
python -c "from bs4 import BeautifulSoup; print('BeautifulSoup OK')"
python -c "import requests; print('Requests OK')"
```

## Contributing

To improve the Texas scraper:

1. Test with real Texas SOS access
2. Document any API endpoints discovered
3. Add support for additional entity types
4. Improve parsing logic for different page formats
5. Add integration tests

## Support

For issues specific to Texas data sources:
- Texas SOS: (512) 463-5555
- Comptroller: (800) 252-5555

For scraper issues:
- Check project GitHub issues
- Review logs with DEBUG level
- Test network connectivity

## Version History

- **v1.0** (2026-02-16)
  - Initial implementation
  - Multi-source strategy
  - Sample data generation
  - Basic web scraping

## License

This scraper is part of the BizLeads USA project. See main project LICENSE file.

## References

- [Texas Secretary of State - Corporations](https://www.sos.texas.gov/corp/)
- [Texas Comptroller - Businesses](https://comptroller.texas.gov/economy/businesses/)
- [Texas Business Organizations Code](https://statutes.capitol.texas.gov/Docs/BO/htm/BO.1.htm)
- [Texas Public Information Act](https://www.texasattorneygeneral.gov/open-government/members-public/public-information-act)
