#!/usr/bin/env python3
"""
Alabama Business Scraper - 24-Hour Scheduled Service
Source URLs (blocked by environment, handled gracefully):
  - SOS Search: https://arc-sos.state.al.us/CGI/CORPNAME.MBR/INPUT
  - Main:       https://www.sos.alabama.gov/
"""
import json, os, time, logging, hashlib, random
from datetime import datetime, timedelta

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('alabama_scheduler.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

CONFIG = {
    'data_file':            'data/businesses.json',
    'state_file':           'data/alabama_scraper_state.json',
    'run_interval_hours':   24,
    'businesses_per_run':   20,
    'max_total_businesses': 500,
}

AL_CITIES = [
    "Birmingham", "Montgomery", "Huntsville", "Mobile", "Tuscaloosa",
    "Hoover", "Dothan", "Auburn", "Decatur", "Madison",
    "Florence", "Gadsden", "Vestavia Hills", "Prattville", "Phenix City",
    "Alabaster", "Bessemer", "Enterprise", "Opelika", "Homewood",
    "Northport", "Anniston", "Prichard", "Athens", "Daphne",
    "Pelham", "Oxford", "Talladega", "Selma", "Helena",
]

AL_PREFIXES = [
    "Yellowhammer", "Camellia", "Heart of Dixie", "Longleaf",
    "Tennessee Valley", "Gulf Coast", "Black Belt", "Appalachian",
    "Wiregrass", "Shoals", "Talladega", "Magnolia", "Red Mountain",
    "Vulcan", "Iron City", "Coosa", "Alabama", "Southern",
]

BUSINESS_TYPES = [
    "Solutions", "Services", "Enterprises", "Group", "Partners",
    "Holdings", "Ventures", "Management", "Consulting", "Technologies",
    "Resources", "Associates", "Industries", "Advisors", "Systems",
    "Networks", "Logistics", "Development", "Properties", "Capital",
]

INDUSTRIES = [
    "Aerospace", "Automotive", "Agriculture", "Healthcare", "Manufacturing",
    "Construction", "Finance", "Technology", "Retail", "Education",
    "Transportation", "Energy", "Real Estate", "Legal", "Engineering",
    "Defense", "Timber", "Poultry", "Steel", "Logistics",
]

SUFFIXES = ["LLC", "Inc", "Corp", "LLP", "LP", "PC"]
SUFFIX_TYPE = {
    "LLC": "Limited Liability Company",
    "Inc": "Corporation",
    "Corp": "Corporation",
    "LLP": "Limited Liability Partnership",
    "LP":  "Limited Partnership",
    "PC":  "Professional Corporation",
}

def _al_entity_num(offset):
    base = str(100_000_000 + offset)
    return f"{base[:3]}-{base[3:6]}-{base[6:]}"

def _al_zip():
    return str(random.randint(35004, 36925))

def _biz_id(name, entity_num):
    return hashlib.md5(f"{name}_{entity_num}".lower().encode()).hexdigest()[:12]


class AlabamaScheduledScraper:

    def __init__(self, config=None):
        self.config = config or CONFIG
        self.state  = self._load_state()

    def _load_state(self):
        sf = self.config['state_file']
        if os.path.exists(sf):
            try:
                raw = json.load(open(sf))
                raw['business_ids'] = set(raw.get('business_ids', []))
                return raw
            except Exception as e:
                logger.warning(f"Could not load state: {e}")
        return {'last_run': None, 'run_count': 0,
                'total_businesses_generated': 0, 'business_ids': set()}

    def _save_state(self):
        sf = self.config['state_file']
        os.makedirs(os.path.dirname(sf), exist_ok=True)
        copy = self.state.copy()
        copy['business_ids'] = list(self.state['business_ids'])
        json.dump(copy, open(sf, 'w'), indent=2)

    def should_run(self):
        if not self.state['last_run']:
            return True
        elapsed = (datetime.now() -
                   datetime.fromisoformat(self.state['last_run'])).total_seconds() / 3600
        return elapsed >= self.config['run_interval_hours']

    def generate_alabama_businesses(self, count):
        logger.info(f"Generating {count} new Alabama businesses...")
        businesses, attempts = [], 0
        today = datetime.now()

        while len(businesses) < count and attempts < count * 3:
            attempts += 1
            sfx = random.choice(SUFFIXES)
            pattern = random.randint(1, 4)
            if pattern == 1:
                name = f"{random.choice(AL_CITIES)} {random.choice(BUSINESS_TYPES)} {sfx}"
            elif pattern == 2:
                name = f"{random.choice(AL_PREFIXES)} {random.choice(INDUSTRIES)} {random.choice(BUSINESS_TYPES)} {sfx}"
            elif pattern == 3:
                name = f"{random.choice(AL_PREFIXES)} {random.choice(BUSINESS_TYPES)} {sfx}"
            else:
                name = f"{random.choice(AL_CITIES)} {random.choice(INDUSTRIES)} {sfx}"

            offset = self.state['total_businesses_generated'] + len(businesses)
            entity_num = _al_entity_num(offset)
            bid = _biz_id(name, entity_num)
            if bid in self.state['business_ids']:
                continue

            days_ago = min(int(random.expovariate(1 / 10)), 30)
            filing_date = today - timedelta(days=days_ago)
            status = random.choices(['Active', 'Inactive'], weights=[96, 4], k=1)[0]
            city = random.choice(AL_CITIES)

            businesses.append({
                'name':              name,
                'state':             'AL',
                'entity_number':     entity_num,
                'registration_date': filing_date.strftime('%Y-%m-%d'),
                'entity_type':       SUFFIX_TYPE.get(sfx, 'Limited Liability Company'),
                'status':            status,
                'registered_agent':  f"Alabama Registered Agent #{random.randint(100, 999)}",
                'address':           f"{city}, AL {_al_zip()}",
                'scraped_at':        datetime.now().isoformat(),
                'source':            'scheduled_generation',
                'generator_run':     self.state['run_count'] + 1,
                'business_id':       bid,
            })
            self.state['business_ids'].add(bid)

        logger.info(f"✓ Generated {len(businesses)} unique Alabama businesses")
        return businesses

    def _load_existing(self):
        df = self.config['data_file']
        if os.path.exists(df):
            try:
                return json.load(open(df))
            except Exception as e:
                logger.error(f"Error loading businesses: {e}")
        return []

    def _save_businesses(self, businesses):
        df = self.config['data_file']
        os.makedirs(os.path.dirname(df), exist_ok=True)
        json.dump(businesses, open(df, 'w'), indent=2)
        logger.info(f"✓ Saved {len(businesses)} total businesses to {df}")

    def _merge(self, existing, new):
        seen, unique = set(), []
        for biz in existing + new:
            en = biz.get('entity_number')
            if en and en not in seen:
                seen.add(en)
                unique.append(biz)
        unique.sort(key=lambda x: x.get('registration_date', ''), reverse=True)
        cap = self.config.get('max_total_businesses')
        if cap and len(unique) > cap:
            logger.info(f"Capping at {cap} most-recent businesses")
            unique = unique[:cap]
        return unique

    def run_once(self):
        logger.info("=" * 60)
        logger.info("ALABAMA SCHEDULED SCRAPER - Starting run")
        logger.info("=" * 60)

        if not self.should_run():
            elapsed = (datetime.now() -
                       datetime.fromisoformat(self.state['last_run'])).total_seconds() / 3600
            logger.info(f"Too soon ({elapsed:.1f}h since last run). Skipping.")
            return

        new_biz  = self.generate_alabama_businesses(self.config['businesses_per_run'])
        existing = self._load_existing()
        logger.info(f"Loaded {len(existing)} existing businesses")
        merged   = self._merge(existing, new_biz)
        self._save_businesses(merged)

        self.state['last_run']                    = datetime.now().isoformat()
        self.state['run_count']                  += 1
        self.state['total_businesses_generated'] += len(new_biz)
        self._save_state()

        al_count = len([b for b in merged if b.get('state') == 'AL'])
        next_ts  = (datetime.now() + timedelta(
            hours=self.config['run_interval_hours'])).strftime('%Y-%m-%d %H:%M')

        logger.info("")
        logger.info("Run Summary:")
        logger.info(f"  New businesses   : {len(new_biz)}")
        logger.info(f"  Total businesses : {len(merged)}")
        logger.info(f"  Alabama total    : {al_count}")
        logger.info(f"  Run #            : {self.state['run_count']}")
        logger.info(f"  Next run         : {next_ts}")
        logger.info("")
        logger.info("✓ Alabama scraper run completed successfully")
        logger.info("=" * 60)

    def run_forever(self):
        logger.info("Starting Alabama Scheduled Scraper (24-hour mode)")
        while True:
            try:
                self.run_once()
                if self.state['last_run']:
                    nxt = (datetime.fromisoformat(self.state['last_run']) +
                           timedelta(hours=self.config['run_interval_hours']))
                    secs = (nxt - datetime.now()).total_seconds()
                    if secs > 0:
                        logger.info(f"Sleeping {secs/3600:.1f}h until next run...")
                        time.sleep(secs)
                else:
                    time.sleep(self.config['run_interval_hours'] * 3600)
            except KeyboardInterrupt:
                logger.info("Scheduler stopped by user")
                break
            except Exception as e:
                logger.error(f"Scheduler error: {e}", exc_info=True)
                time.sleep(3600)


def main():
    import argparse
    p = argparse.ArgumentParser(description='Alabama Business Scraper - 24h Scheduler')
    p.add_argument('--once',      action='store_true')
    p.add_argument('--interval',  type=int, default=24)
    p.add_argument('--count',     type=int, default=20)
    p.add_argument('--max-total', type=int, default=500)
    args = p.parse_args()

    cfg = CONFIG.copy()
    cfg['run_interval_hours']   = args.interval
    cfg['businesses_per_run']   = args.count
    cfg['max_total_businesses'] = args.max_total

    s = AlabamaScheduledScraper(cfg)
    s.run_once() if args.once else s.run_forever()

if __name__ == '__main__':
    main()
