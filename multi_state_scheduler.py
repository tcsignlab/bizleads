#!/usr/bin/env python3
"""
Multi-State Business Scraper - 24-Hour Scheduled Service
Runs Texas and Florida scrapers together

This service:
1. Runs both state scrapers every 24 hours
2. Coordinates timing to avoid conflicts
3. Provides unified logging and monitoring
4. Handles errors independently per state
"""
import json
import os
import time
import logging
from datetime import datetime, timedelta
import subprocess
import sys

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('multi_state_scheduler.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
CONFIG = {
    'run_interval_hours': 24,
    'states': {
        'texas': {
            'script': 'texas_scheduler.py',
            'enabled': True,
            'delay_minutes': 0,   # Run immediately
        },
        'florida': {
            'script': 'florida_scheduler.py',
            'enabled': True,
            'delay_minutes': 5,   # Run 5 minutes after Texas
        },
        'alabama': {
            'script': 'alabama_scheduler.py',
            'enabled': True,
            'delay_minutes': 10,  # Run 10 minutes after Texas
        },
    }
}

class MultiStateScheduler:
    """Coordinates multiple state scrapers"""
    
    def __init__(self, config=None):
        self.config = config or CONFIG
        self.last_run = None
    
    def should_run(self):
        """Check if enough time has passed since last run"""
        if not self.last_run:
            return True
        
        hours_since = (datetime.now() - self.last_run).total_seconds() / 3600
        return hours_since >= self.config['run_interval_hours']
    
    def run_state_scraper(self, state_name, state_config):
        """Run a single state scraper"""
        if not state_config.get('enabled', True):
            logger.info(f"⏭️  {state_name.upper()} scraper is disabled, skipping")
            return True
        
        script = state_config['script']
        delay = state_config.get('delay_minutes', 0)
        
        # Apply delay if configured
        if delay > 0:
            logger.info(f"⏸️  Waiting {delay} minutes before running {state_name.upper()}...")
            time.sleep(delay * 60)
        
        logger.info(f"▶️  Running {state_name.upper()} scraper...")
        
        try:
            # Run the scraper script
            result = subprocess.run(
                [sys.executable, script, '--once'],
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout
            )
            
            if result.returncode == 0:
                logger.info(f"✅ {state_name.upper()} scraper completed successfully")
                # Log the output
                for line in result.stdout.split('\n'):
                    if line.strip() and ('INFO' in line or '✓' in line):
                        logger.info(f"  {state_name.upper()}: {line.strip()}")
                return True
            else:
                logger.error(f"❌ {state_name.upper()} scraper failed with code {result.returncode}")
                logger.error(f"  Error: {result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            logger.error(f"❌ {state_name.upper()} scraper timed out after 5 minutes")
            return False
        except Exception as e:
            logger.error(f"❌ {state_name.upper()} scraper error: {e}")
            return False
    
    def run_once(self):
        """Execute one complete run of all state scrapers"""
        logger.info("=" * 70)
        logger.info("MULTI-STATE SCHEDULER - Starting run")
        logger.info("=" * 70)
        
        # Check if we should run
        if not self.should_run():
            hours_since = (datetime.now() - self.last_run).total_seconds() / 3600
            logger.info(f"⏸️  Too soon since last run ({hours_since:.1f}h ago). Skipping.")
            return
        
        start_time = datetime.now()
        results = {}
        
        # Run each state scraper
        for state_name, state_config in self.config['states'].items():
            success = self.run_state_scraper(state_name, state_config)
            results[state_name] = success
        
        # Update last run time
        self.last_run = datetime.now()
        
        # Calculate summary
        total_states = len(results)
        successful = sum(1 for success in results.values() if success)
        failed = total_states - successful
        duration = (datetime.now() - start_time).total_seconds()
        
        # Log summary
        logger.info("")
        logger.info("=" * 70)
        logger.info("Run Summary:")
        logger.info(f"  Total states: {total_states}")
        logger.info(f"  Successful: {successful}")
        logger.info(f"  Failed: {failed}")
        logger.info(f"  Duration: {duration:.1f} seconds")
        
        for state_name, success in results.items():
            status = "✅ Success" if success else "❌ Failed"
            logger.info(f"    {state_name.upper()}: {status}")
        
        logger.info(f"  Next run: {(datetime.now() + timedelta(hours=self.config['run_interval_hours'])).strftime('%Y-%m-%d %H:%M')}")
        logger.info("")
        
        if failed == 0:
            logger.info("✅ All scrapers completed successfully!")
        else:
            logger.warning(f"⚠️  {failed} scraper(s) failed - check logs above")
        
        logger.info("=" * 70)
    
    def run_forever(self):
        """Run scheduler in continuous loop"""
        logger.info("🚀 Starting Multi-State Scheduled Scraper")
        logger.info(f"   Run interval: {self.config['run_interval_hours']} hours")
        logger.info(f"   Enabled states: {', '.join([s.upper() for s, c in self.config['states'].items() if c.get('enabled', True)])}")
        logger.info("")
        
        while True:
            try:
                self.run_once()
                
                # Calculate sleep time
                if self.last_run:
                    next_run = self.last_run + timedelta(hours=self.config['run_interval_hours'])
                    sleep_seconds = (next_run - datetime.now()).total_seconds()
                    
                    if sleep_seconds > 0:
                        logger.info(f"💤 Sleeping for {sleep_seconds/3600:.1f} hours until next run...")
                        logger.info(f"   Next run at: {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
                        logger.info("")
                        time.sleep(sleep_seconds)
                    else:
                        logger.info("⏭️  Next run is overdue, running immediately...")
                        continue
                else:
                    sleep_seconds = self.config['run_interval_hours'] * 3600
                    logger.info(f"💤 Sleeping for {sleep_seconds/3600:.1f} hours until next run...")
                    time.sleep(sleep_seconds)
                    
            except KeyboardInterrupt:
                logger.info("\n🛑 Multi-state scheduler stopped by user")
                break
            except Exception as e:
                logger.error(f"❌ Error in scheduler loop: {e}", exc_info=True)
                logger.info("⏸️  Sleeping for 1 hour before retry...")
                time.sleep(3600)

def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Multi-State Business Scraper - 24-Hour Scheduler')
    parser.add_argument('--once', action='store_true', help='Run once and exit')
    parser.add_argument('--interval', type=int, default=24, help='Hours between runs (default: 24)')
    parser.add_argument('--state', choices=['texas', 'florida', 'alabama', 'all'], default='all',
                       help='Which state(s) to run (default: all)')

    args = parser.parse_args()

    # Update config based on arguments
    config = CONFIG.copy()
    config['run_interval_hours'] = args.interval

    # Disable states based on argument
    if args.state != 'all':
        for s in config['states']:
            config['states'][s]['enabled'] = (s == args.state)
    
    # Create scheduler
    scheduler = MultiStateScheduler(config)
    
    if args.once:
        scheduler.run_once()
    else:
        scheduler.run_forever()

if __name__ == '__main__':
    main()
