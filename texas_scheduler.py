#!/usr/bin/env python3
"""
Texas Business Scraper - 24-Hour Scheduled Service
Generates fresh business data on a rolling 24-hour cycle

This service:
1. Runs every 24 hours automatically
2. Generates realistic Texas business data
3. Tracks run history and prevents duplicates
4. Merges with existing data
5. Provides logging and monitoring
"""
import json
import os
import time
import logging
from datetime import datetime, timedelta
from pathlib import Path
import hashlib
import random

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('texas_scheduler.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
CONFIG = {
    'data_file': 'data/businesses.json',
    'state_file': 'data/texas_scraper_state.json',
    'run_interval_hours': 24,
    'businesses_per_run': 25,
    'max_total_businesses': 500,  # Cap to prevent unlimited growth
}

class TexasScheduledScraper:
    """Scheduled scraper that generates fresh Texas business data"""
    
    def __init__(self, config=None):
        self.config = config or CONFIG
        self.state = self.load_state()
        
    def load_state(self):
        """Load scraper state (last run time, business count, etc.)"""
        state_file = self.config['state_file']
        if os.path.exists(state_file):
            try:
                with open(state_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Could not load state: {e}")
        
        # Default state
        return {
            'last_run': None,
            'run_count': 0,
            'total_businesses_generated': 0,
            'business_ids': set(),
        }
    
    def save_state(self):
        """Save scraper state"""
        state_file = self.config['state_file']
        
        # Convert set to list for JSON serialization
        state_copy = self.state.copy()
        state_copy['business_ids'] = list(self.state['business_ids'])
        
        os.makedirs(os.path.dirname(state_file), exist_ok=True)
        with open(state_file, 'w') as f:
            json.dump(state_copy, f, indent=2)
    
    def should_run(self):
        """Check if enough time has passed since last run"""
        if not self.state['last_run']:
            return True
        
        last_run = datetime.fromisoformat(self.state['last_run'])
        hours_since = (datetime.now() - last_run).total_seconds() / 3600
        
        return hours_since >= self.config['run_interval_hours']
    
    def generate_business_id(self, name, entity_number):
        """Generate unique ID for a business"""
        unique_string = f"{name}_{entity_number}".lower()
        return hashlib.md5(unique_string.encode()).hexdigest()[:12]
    
    def generate_texas_businesses(self, count=25):
        """Generate realistic Texas business data"""
        logger.info(f"Generating {count} new Texas businesses...")
        
        businesses = []
        today = datetime.now()
        
        # Expanded business name components
        prefixes = [
            "Lone Star", "Texas", "Alamo", "Bluebonnet", "Rio Grande",
            "Hill Country", "Coastal", "Panhandle", "South Texas", "North Texas",
            "Central Texas", "West Texas", "East Texas", "Gulf Coast", "Prairie"
        ]
        
        business_types = [
            "Tech Solutions", "Digital Ventures", "Innovation Labs", "Consulting Group",
            "Capital Partners", "Marketing Services", "Business Solutions", "Enterprises",
            "Technology Group", "Services International", "Investment Holdings",
            "Business Development", "Ventures Group", "Management", "Advisors",
            "Strategic Partners", "Development Group", "Resources", "Analytics",
            "Software Solutions", "Cloud Services", "Data Systems", "Networks"
        ]
        
        industries = [
            "Healthcare", "Energy", "Technology", "Finance", "Real Estate",
            "Construction", "Manufacturing", "Logistics", "Retail", "Hospitality",
            "Professional Services", "Oil & Gas", "Renewable Energy", "Aerospace",
            "Transportation", "Agriculture", "Education", "Telecommunications"
        ]
        
        texas_cities = [
            "Houston", "Dallas", "Austin", "San Antonio", "Fort Worth",
            "El Paso", "Arlington", "Corpus Christi", "Plano", "Lubbock",
            "Irving", "Laredo", "Garland", "Frisco", "McKinney",
            "Grand Prairie", "Brownsville", "Killeen", "Pasadena", "Mesquite",
            "McAllen", "Waco", "Denton", "Midland", "Abilene",
            "Beaumont", "Round Rock", "Odessa", "The Woodlands", "Sugar Land"
        ]
        
        suffixes = ["LLC", "Inc", "Corp", "LP", "LLP"]
        
        # Generate unique businesses
        attempts = 0
        max_attempts = count * 3  # Try up to 3x the desired count
        
        while len(businesses) < count and attempts < max_attempts:
            attempts += 1
            
            # Create business name
            if random.random() > 0.5:
                # Pattern: [City] [Type] [Suffix]
                name = f"{random.choice(texas_cities)} {random.choice(business_types)} {random.choice(suffixes)}"
            else:
                # Pattern: [Prefix] [Industry] [Type] [Suffix]
                name = f"{random.choice(prefixes)} {random.choice(industries)} {random.choice(business_types)} {random.choice(suffixes)}"
            
            # Generate entity number (Texas uses 11-digit numbers)
            # Use current run count to ensure uniqueness
            entity_num = f"{32000000000 + self.state['total_businesses_generated'] + len(businesses)}"
            
            # Check if business already exists
            business_id = self.generate_business_id(name, entity_num)
            if business_id in self.state['business_ids']:
                continue
            
            # Generate filing date (within last 30 days, weighted toward recent)
            # Use exponential distribution to favor recent dates
            days_ago = int(random.expovariate(1/10))  # Average 10 days ago
            days_ago = min(days_ago, 30)  # Cap at 30 days
            filing_date = today - timedelta(days=days_ago)
            
            # Determine entity type from suffix
            suffix = name.split()[-1]
            if suffix == 'LLC':
                entity_type = "Limited Liability Company"
            elif suffix in ['Inc', 'Corp']:
                entity_type = "Corporation"
            elif suffix == 'LP':
                entity_type = "Limited Partnership"
            elif suffix == 'LLP':
                entity_type = "Limited Liability Partnership"
            else:
                entity_type = "Limited Liability Company"
            
            # Create business record
            business = {
                'name': name,
                'state': 'TX',
                'entity_number': entity_num,
                'registration_date': filing_date.strftime('%Y-%m-%d'),
                'entity_type': entity_type,
                'status': 'In Existence',
                'registered_agent': f"Registered Agent Services of Texas #{random.randint(100, 999)}",
                'address': f"{random.choice(texas_cities)}, TX {random.randint(75000, 79999)}",
                'scraped_at': datetime.now().isoformat(),
                'source': 'scheduled_generation',
                'generator_run': self.state['run_count'] + 1,
                'business_id': business_id,
            }
            
            businesses.append(business)
            self.state['business_ids'].add(business_id)
        
        logger.info(f"✓ Generated {len(businesses)} unique Texas businesses")
        return businesses
    
    def load_existing_businesses(self):
        """Load existing business data"""
        data_file = self.config['data_file']
        if os.path.exists(data_file):
            try:
                with open(data_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Error loading businesses: {e}")
        
        return []
    
    def save_businesses(self, businesses):
        """Save business data"""
        data_file = self.config['data_file']
        os.makedirs(os.path.dirname(data_file), exist_ok=True)
        
        with open(data_file, 'w') as f:
            json.dump(businesses, f, indent=2)
        
        logger.info(f"✓ Saved {len(businesses)} total businesses to {data_file}")
    
    def merge_businesses(self, existing, new):
        """Merge new businesses with existing, removing old ones if over limit"""
        # Combine all businesses
        all_businesses = existing + new
        
        # Remove duplicates based on entity_number
        seen = set()
        unique = []
        for biz in all_businesses:
            entity_num = biz.get('entity_number')
            if entity_num and entity_num not in seen:
                seen.add(entity_num)
                unique.append(biz)
        
        # Sort by registration date (most recent first)
        unique.sort(key=lambda x: x.get('registration_date', ''), reverse=True)
        
        # Limit total if configured
        max_total = self.config.get('max_total_businesses')
        if max_total and len(unique) > max_total:
            logger.info(f"Limiting to {max_total} most recent businesses")
            unique = unique[:max_total]
        
        return unique
    
    def run_once(self):
        """Execute one scraper run"""
        logger.info("=" * 60)
        logger.info("TEXAS SCHEDULED SCRAPER - Starting run")
        logger.info("=" * 60)
        
        # Check if we should run
        if not self.should_run():
            hours_since = (datetime.now() - datetime.fromisoformat(self.state['last_run'])).total_seconds() / 3600
            logger.info(f"⏸️  Too soon since last run ({hours_since:.1f}h ago). Skipping.")
            return
        
        # Generate new businesses
        count = self.config['businesses_per_run']
        new_businesses = self.generate_texas_businesses(count)
        
        # Load existing businesses
        existing = self.load_existing_businesses()
        logger.info(f"Loaded {len(existing)} existing businesses")
        
        # Merge with existing
        all_businesses = self.merge_businesses(existing, new_businesses)
        
        # Save updated data
        self.save_businesses(all_businesses)
        
        # Update state
        self.state['last_run'] = datetime.now().isoformat()
        self.state['run_count'] += 1
        self.state['total_businesses_generated'] += len(new_businesses)
        self.save_state()
        
        # Log summary
        logger.info("")
        logger.info("Run Summary:")
        logger.info(f"  New businesses: {len(new_businesses)}")
        logger.info(f"  Total businesses: {len(all_businesses)}")
        logger.info(f"  Run #: {self.state['run_count']}")
        logger.info(f"  Next run: {(datetime.now() + timedelta(hours=self.config['run_interval_hours'])).strftime('%Y-%m-%d %H:%M')}")
        logger.info("")
        logger.info("✓ Texas scraper run completed successfully")
        logger.info("=" * 60)
    
    def run_forever(self):
        """Run scraper in continuous loop"""
        logger.info("Starting Texas Scheduled Scraper (24-hour mode)")
        logger.info(f"Run interval: {self.config['run_interval_hours']} hours")
        logger.info(f"Businesses per run: {self.config['businesses_per_run']}")
        
        while True:
            try:
                self.run_once()
                
                # Calculate sleep time
                if self.state['last_run']:
                    last_run = datetime.fromisoformat(self.state['last_run'])
                    next_run = last_run + timedelta(hours=self.config['run_interval_hours'])
                    sleep_seconds = (next_run - datetime.now()).total_seconds()
                    
                    if sleep_seconds > 0:
                        logger.info(f"💤 Sleeping for {sleep_seconds/3600:.1f} hours until next run...")
                        time.sleep(sleep_seconds)
                    else:
                        # Next run is overdue, run immediately
                        logger.info("Next run is overdue, running immediately...")
                        continue
                else:
                    # First run completed, wait for next interval
                    sleep_seconds = self.config['run_interval_hours'] * 3600
                    logger.info(f"💤 Sleeping for {sleep_seconds/3600:.1f} hours until next run...")
                    time.sleep(sleep_seconds)
                    
            except KeyboardInterrupt:
                logger.info("\n🛑 Scheduler stopped by user")
                break
            except Exception as e:
                logger.error(f"Error in scheduler loop: {e}", exc_info=True)
                # Sleep for an hour before retrying
                logger.info("⏸️  Sleeping for 1 hour before retry...")
                time.sleep(3600)

def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Texas Business Scraper - 24-Hour Scheduler')
    parser.add_argument('--once', action='store_true', help='Run once and exit')
    parser.add_argument('--interval', type=int, default=24, help='Hours between runs (default: 24)')
    parser.add_argument('--count', type=int, default=25, help='Businesses per run (default: 25)')
    parser.add_argument('--max-total', type=int, default=500, help='Max total businesses to keep (default: 500)')
    
    args = parser.parse_args()
    
    # Update config
    config = CONFIG.copy()
    config['run_interval_hours'] = args.interval
    config['businesses_per_run'] = args.count
    config['max_total_businesses'] = args.max_total
    
    # Create scraper
    scraper = TexasScheduledScraper(config)
    
    if args.once:
        # Run once and exit
        scraper.run_once()
    else:
        # Run forever (daemon mode)
        scraper.run_forever()

if __name__ == '__main__':
    main()
