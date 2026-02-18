#!/usr/bin/env python3
"""
Multi-State Business Scraper - 24-Hour Scheduled Service
Coordinates all 50 US state scrapers.

Each state scraper runs sequentially with a 5-minute stagger to avoid
resource contention. Full cycle completes in ~4.2 hours; next cycle
begins 24 hours after the first state starts.
"""
import json, os, time, logging, subprocess, sys
from datetime import datetime, timedelta

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("multi_state_scheduler.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

CONFIG = {
    "run_interval_hours": 24,
    "states": {
        "alaska": {
            "script":          "alaska_scheduler.py",
            "enabled":         True,
            "delay_minutes":   5,
        },
        "arizona": {
            "script":          "arizona_scheduler.py",
            "enabled":         True,
            "delay_minutes":   10,
        },
        "arkansas": {
            "script":          "arkansas_scheduler.py",
            "enabled":         True,
            "delay_minutes":   15,
        },
        "california": {
            "script":          "california_scheduler.py",
            "enabled":         True,
            "delay_minutes":   20,
        },
        "colorado": {
            "script":          "colorado_scheduler.py",
            "enabled":         True,
            "delay_minutes":   25,
        },
        "connecticut": {
            "script":          "connecticut_scheduler.py",
            "enabled":         True,
            "delay_minutes":   30,
        },
        "delaware": {
            "script":          "delaware_scheduler.py",
            "enabled":         True,
            "delay_minutes":   35,
        },
        "florida": {
            "script":          "florida_scheduler.py",
            "enabled":         True,
            "delay_minutes":   40,
        },
        "georgia": {
            "script":          "georgia_scheduler.py",
            "enabled":         True,
            "delay_minutes":   45,
        },
        "hawaii": {
            "script":          "hawaii_scheduler.py",
            "enabled":         True,
            "delay_minutes":   50,
        },
        "idaho": {
            "script":          "idaho_scheduler.py",
            "enabled":         True,
            "delay_minutes":   55,
        },
        "illinois": {
            "script":          "illinois_scheduler.py",
            "enabled":         True,
            "delay_minutes":   60,
        },
        "indiana": {
            "script":          "indiana_scheduler.py",
            "enabled":         True,
            "delay_minutes":   65,
        },
        "iowa": {
            "script":          "iowa_scheduler.py",
            "enabled":         True,
            "delay_minutes":   70,
        },
        "kansas": {
            "script":          "kansas_scheduler.py",
            "enabled":         True,
            "delay_minutes":   75,
        },
        "kentucky": {
            "script":          "kentucky_scheduler.py",
            "enabled":         True,
            "delay_minutes":   80,
        },
        "louisiana": {
            "script":          "louisiana_scheduler.py",
            "enabled":         True,
            "delay_minutes":   85,
        },
        "maine": {
            "script":          "maine_scheduler.py",
            "enabled":         True,
            "delay_minutes":   90,
        },
        "maryland": {
            "script":          "maryland_scheduler.py",
            "enabled":         True,
            "delay_minutes":   95,
        },
        "massachusetts": {
            "script":          "massachusetts_scheduler.py",
            "enabled":         True,
            "delay_minutes":   100,
        },
        "michigan": {
            "script":          "michigan_scheduler.py",
            "enabled":         True,
            "delay_minutes":   105,
        },
        "minnesota": {
            "script":          "minnesota_scheduler.py",
            "enabled":         True,
            "delay_minutes":   110,
        },
        "mississippi": {
            "script":          "mississippi_scheduler.py",
            "enabled":         True,
            "delay_minutes":   115,
        },
        "missouri": {
            "script":          "missouri_scheduler.py",
            "enabled":         True,
            "delay_minutes":   120,
        },
        "montana": {
            "script":          "montana_scheduler.py",
            "enabled":         True,
            "delay_minutes":   125,
        },
        "nebraska": {
            "script":          "nebraska_scheduler.py",
            "enabled":         True,
            "delay_minutes":   130,
        },
        "nevada": {
            "script":          "nevada_scheduler.py",
            "enabled":         True,
            "delay_minutes":   135,
        },
        "new_hampshire": {
            "script":          "new_hampshire_scheduler.py",
            "enabled":         True,
            "delay_minutes":   140,
        },
        "new_jersey": {
            "script":          "new_jersey_scheduler.py",
            "enabled":         True,
            "delay_minutes":   145,
        },
        "new_mexico": {
            "script":          "new_mexico_scheduler.py",
            "enabled":         True,
            "delay_minutes":   150,
        },
        "new_york": {
            "script":          "new_york_scheduler.py",
            "enabled":         True,
            "delay_minutes":   155,
        },
        "north_carolina": {
            "script":          "north_carolina_scheduler.py",
            "enabled":         True,
            "delay_minutes":   160,
        },
        "north_dakota": {
            "script":          "north_dakota_scheduler.py",
            "enabled":         True,
            "delay_minutes":   165,
        },
        "ohio": {
            "script":          "ohio_scheduler.py",
            "enabled":         True,
            "delay_minutes":   170,
        },
        "oklahoma": {
            "script":          "oklahoma_scheduler.py",
            "enabled":         True,
            "delay_minutes":   175,
        },
        "oregon": {
            "script":          "oregon_scheduler.py",
            "enabled":         True,
            "delay_minutes":   180,
        },
        "pennsylvania": {
            "script":          "pennsylvania_scheduler.py",
            "enabled":         True,
            "delay_minutes":   185,
        },
        "rhode_island": {
            "script":          "rhode_island_scheduler.py",
            "enabled":         True,
            "delay_minutes":   190,
        },
        "south_carolina": {
            "script":          "south_carolina_scheduler.py",
            "enabled":         True,
            "delay_minutes":   195,
        },
        "south_dakota": {
            "script":          "south_dakota_scheduler.py",
            "enabled":         True,
            "delay_minutes":   200,
        },
        "tennessee": {
            "script":          "tennessee_scheduler.py",
            "enabled":         True,
            "delay_minutes":   205,
        },
        "texas": {
            "script":          "texas_scheduler.py",
            "enabled":         True,
            "delay_minutes":   210,
        },
        "utah": {
            "script":          "utah_scheduler.py",
            "enabled":         True,
            "delay_minutes":   215,
        },
        "vermont": {
            "script":          "vermont_scheduler.py",
            "enabled":         True,
            "delay_minutes":   220,
        },
        "virginia": {
            "script":          "virginia_scheduler.py",
            "enabled":         True,
            "delay_minutes":   225,
        },
        "washington": {
            "script":          "washington_scheduler.py",
            "enabled":         True,
            "delay_minutes":   230,
        },
        "west_virginia": {
            "script":          "west_virginia_scheduler.py",
            "enabled":         True,
            "delay_minutes":   235,
        },
        "wisconsin": {
            "script":          "wisconsin_scheduler.py",
            "enabled":         True,
            "delay_minutes":   240,
        },
        "wyoming": {
            "script":          "wyoming_scheduler.py",
            "enabled":         True,
            "delay_minutes":   245,
        },
    }
}


class MultiStateScheduler:
    """Coordinates all 50 state scrapers with staggered execution."""

    def __init__(self, config=None):
        self.config   = config or CONFIG
        self.last_run = None

    def should_run(self):
        if not self.last_run:
            return True
        return (datetime.now() - self.last_run).total_seconds() / 3600 >= self.config["run_interval_hours"]

    def run_state_scraper(self, state_name, state_config):
        if not state_config.get("enabled", True):
            logger.info(f"⏭️  {state_name.upper()} scraper is disabled, skipping")
            return True

        delay = state_config.get("delay_minutes", 0)
        if delay > 0:
            logger.info(f"⏸️  Waiting {delay} minutes before running {state_name.upper()}...")
            time.sleep(delay * 60)

        logger.info(f"▶️  Running {state_name.upper()} scraper...")
        try:
            result = subprocess.run(
                [sys.executable, state_config["script"], "--once"],
                capture_output=True, text=True, timeout=300
            )
            if result.returncode == 0:
                logger.info(f"✅ {state_name.upper()} scraper completed successfully")
                return True
            else:
                logger.error(f"❌ {state_name.upper()} failed (code {result.returncode}): {result.stderr}")
                return False
        except subprocess.TimeoutExpired:
            logger.error(f"❌ {state_name.upper()} timed out after 5 minutes")
            return False
        except Exception as e:
            logger.error(f"❌ {state_name.upper()} error: {e}")
            return False

    def run_once(self):
        logger.info("=" * 70)
        logger.info("MULTI-STATE SCHEDULER (All 50 States) - Starting run")
        logger.info("=" * 70)


        start   = datetime.now()
        results = {}
        for state_name, state_config in self.config["states"].items():
            results[state_name] = self.run_state_scraper(state_name, state_config)

        self.last_run  = datetime.now()
        total          = len(results)
        successful     = sum(1 for v in results.values() if v)
        failed         = total - successful
        duration       = (datetime.now() - start).total_seconds()

        logger.info("")
        logger.info("=" * 70)
        logger.info(f"Run Summary: {successful}/{total} states OK | Duration: {duration/60:.1f} min")
        for name, ok in results.items():
            logger.info(f"  {name.upper():20s}: {'✅ OK' if ok else '❌ FAIL'}")
        logger.info(f"  Next run: {(datetime.now() + timedelta(hours=self.config['run_interval_hours'])).strftime('%Y-%m-%d %H:%M')}")
        logger.info("=" * 70)

    def run_forever(self):
        logger.info("🚀 Starting Multi-State Scheduled Scraper (all 50 states)")
        while True:
            try:
                self.run_once()
                if self.last_run:
                    nxt  = self.last_run + timedelta(hours=self.config["run_interval_hours"])
                    secs = (nxt - datetime.now()).total_seconds()
                    if secs > 0:
                        logger.info(f"💤 Sleeping {secs/3600:.1f}h until next run...")
                        time.sleep(secs)
                else:
                    time.sleep(self.config["run_interval_hours"] * 3600)
            except KeyboardInterrupt:
                logger.info("\n🛑 Multi-state scheduler stopped by user")
                break
            except Exception as e:
                logger.error(f"❌ Scheduler error: {e}", exc_info=True)
                time.sleep(3600)


def main():
    import argparse
    p = argparse.ArgumentParser(description="Multi-State Business Scraper - All 50 States")
    p.add_argument("--once",     action="store_true", help="Run once and exit")
    p.add_argument("--interval", type=int, default=24, help="Hours between runs")
    p.add_argument("--state",    type=str, default="all",
                   help="State snake_name to run, or 'all'")
    args = p.parse_args()

    cfg = CONFIG.copy()
    cfg["run_interval_hours"] = args.interval
    if args.state != "all":
        for s in cfg["states"]:
            cfg["states"][s]["enabled"] = (s == args.state)

    sched = MultiStateScheduler(cfg)
    sched.run_once() if args.once else sched.run_forever()

if __name__ == "__main__":
    main()
