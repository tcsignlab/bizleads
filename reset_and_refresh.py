#!/usr/bin/env python3
"""
Reset & Refresh Script
----------------------
1. Deletes all existing scraper state files and business data files
2. Runs all 50 state scrapers once to generate fresh 30-day data
3. Deduplicates any results within each state's data file

Usage:
    python3 reset_and_refresh.py            # full reset + fresh run
    python3 reset_and_refresh.py --dry-run  # preview what would be deleted
"""

import os
import sys
import json
import glob
import subprocess
import logging
import argparse
from datetime import datetime, timedelta

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("reset_and_refresh.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

# All 50 state schedulers (plus multi_state which we skip — we run each state directly)
STATE_SCHEDULERS = [
    "alabama_scheduler.py",
    "alaska_scheduler.py",
    "arizona_scheduler.py",
    "arkansas_scheduler.py",
    "california_scheduler.py",
    "colorado_scheduler.py",
    "connecticut_scheduler.py",
    "delaware_scheduler.py",
    "florida_scheduler.py",
    "georgia_scheduler.py",
    "hawaii_scheduler.py",
    "idaho_scheduler.py",
    "illinois_scheduler.py",
    "indiana_scheduler.py",
    "iowa_scheduler.py",
    "kansas_scheduler.py",
    "kentucky_scheduler.py",
    "louisiana_scheduler.py",
    "maine_scheduler.py",
    "maryland_scheduler.py",
    "massachusetts_scheduler.py",
    "michigan_scheduler.py",
    "minnesota_scheduler.py",
    "mississippi_scheduler.py",
    "missouri_scheduler.py",
    "montana_scheduler.py",
    "nebraska_scheduler.py",
    "nevada_scheduler.py",
    "new_hampshire_scheduler.py",
    "new_jersey_scheduler.py",
    "new_mexico_scheduler.py",
    "new_york_scheduler.py",
    "north_carolina_scheduler.py",
    "north_dakota_scheduler.py",
    "ohio_scheduler.py",
    "oklahoma_scheduler.py",
    "oregon_scheduler.py",
    "pennsylvania_scheduler.py",
    "rhode_island_scheduler.py",
    "south_carolina_scheduler.py",
    "south_dakota_scheduler.py",
    "tennessee_scheduler.py",
    "texas_scheduler.py",
    "utah_scheduler.py",
    "vermont_scheduler.py",
    "virginia_scheduler.py",
    "washington_scheduler.py",
    "west_virginia_scheduler.py",
    "wisconsin_scheduler.py",
    "wyoming_scheduler.py",
]


def find_data_files():
    """Find all state business data JSON files."""
    patterns = [
        os.path.join(DATA_DIR, "*_businesses.json"),
        os.path.join(BASE_DIR, "*_businesses.json"),  # fallback if stored elsewhere
    ]
    files = []
    for p in patterns:
        files.extend(glob.glob(p))
    return files


def find_state_files():
    """Find all scraper state JSON files."""
    patterns = [
        os.path.join(DATA_DIR, "*_state.json"),
        os.path.join(DATA_DIR, "*_scraper_state.json"),
        os.path.join(BASE_DIR, "*_state.json"),
        os.path.join(BASE_DIR, "*_scraper_state.json"),
    ]
    files = []
    seen = set()
    for p in patterns:
        for f in glob.glob(p):
            if f not in seen:
                seen.add(f)
                files.append(f)
    return files


def clear_all_data(dry_run=False):
    """Delete all state files and business data files (keeps businesses_sample.json)."""
    logger.info("=" * 60)
    logger.info("STEP 1: Clearing all existing scraper data")
    logger.info("=" * 60)

    state_files = find_state_files()
    data_files = [
        f for f in find_data_files()
        if "businesses_sample.json" not in f  # preserve sample
    ]

    # Also clear the main businesses.json so dashboard starts fresh
    main_biz = os.path.join(DATA_DIR, "businesses.json")
    if os.path.exists(main_biz):
        data_files.append(main_biz)

    all_to_delete = state_files + data_files
    all_to_delete = list(set(all_to_delete))  # deduplicate paths

    if not all_to_delete:
        logger.info("No existing data files found — already clean.")
        return

    for f in sorted(all_to_delete):
        rel = os.path.relpath(f, BASE_DIR)
        if dry_run:
            logger.info(f"  [DRY RUN] Would delete: {rel}")
        else:
            try:
                os.remove(f)
                logger.info(f"  ✓ Deleted: {rel}")
            except Exception as e:
                logger.error(f"  ✗ Could not delete {rel}: {e}")

    if not dry_run:
        logger.info(f"\nCleared {len(all_to_delete)} file(s). Starting fresh.\n")


def deduplicate_file(filepath):
    """
    Remove duplicate entries from a business data file.
    Deduplication key: entity_number (primary), then business_id.
    Keeps the most recently scraped entry when duplicates exist.
    """
    if not os.path.exists(filepath):
        return 0, 0

    try:
        with open(filepath) as f:
            businesses = json.load(f)
    except Exception as e:
        logger.error(f"  Could not read {filepath}: {e}")
        return 0, 0

    original_count = len(businesses)

    # Sort newest-scraped-first so we keep the freshest record on conflict
    businesses.sort(key=lambda x: x.get("scraped_at", ""), reverse=True)

    seen = set()
    unique = []
    for biz in businesses:
        key = biz.get("entity_number") or biz.get("business_id") or biz.get("name", "")
        if key and key not in seen:
            seen.add(key)
            unique.append(biz)

    # Restore chronological order (newest registration first)
    unique.sort(key=lambda x: x.get("registration_date", ""), reverse=True)

    removed = original_count - len(unique)
    if removed > 0:
        with open(filepath, "w") as f:
            json.dump(unique, f, indent=2)

    return original_count, len(unique)


def run_scraper_once(script_name, dry_run=False):
    """Run a single state scheduler with --once flag."""
    script_path = os.path.join(BASE_DIR, script_name)
    if not os.path.exists(script_path):
        logger.warning(f"  Script not found, skipping: {script_name}")
        return False

    state_label = script_name.replace("_scheduler.py", "").replace("_", " ").title()

    if dry_run:
        logger.info(f"  [DRY RUN] Would run: python3 {script_name} --once")
        return True

    try:
        logger.info(f"  Running {state_label}...")
        result = subprocess.run(
            [sys.executable, script_path, "--once"],
            cwd=BASE_DIR,
            capture_output=True,
            text=True,
            timeout=120,  # 2 minutes per state
        )
        if result.returncode == 0:
            logger.info(f"  ✓ {state_label} complete")
            return True
        else:
            logger.error(f"  ✗ {state_label} failed (exit {result.returncode})")
            if result.stderr:
                logger.error(f"    {result.stderr.strip()[:300]}")
            return False
    except subprocess.TimeoutExpired:
        logger.error(f"  ✗ {state_label} timed out after 2 minutes")
        return False
    except Exception as e:
        logger.error(f"  ✗ {state_label} error: {e}")
        return False


def run_all_scrapers(dry_run=False):
    """Run every state scraper once to generate fresh 30-day data."""
    logger.info("=" * 60)
    logger.info("STEP 2: Running all state scrapers (fresh 30-day pull)")
    logger.info("=" * 60)

    success, failed = 0, []
    total = len(STATE_SCHEDULERS)

    for i, script in enumerate(STATE_SCHEDULERS, 1):
        state_label = script.replace("_scheduler.py", "").replace("_", " ").title()
        logger.info(f"[{i}/{total}] {state_label}")
        ok = run_scraper_once(script, dry_run=dry_run)
        if ok:
            success += 1
        else:
            failed.append(state_label)

    logger.info("")
    logger.info(f"Scrapers complete: {success}/{total} succeeded")
    if failed:
        logger.warning(f"Failed states: {', '.join(failed)}")
    return success, failed


def deduplicate_all(dry_run=False):
    """Deduplicate all business data files after scraping."""
    logger.info("=" * 60)
    logger.info("STEP 3: Deduplicating all business data files")
    logger.info("=" * 60)

    data_files = [
        f for f in find_data_files()
        if "businesses_sample.json" not in f
    ]

    if not data_files:
        logger.info("No data files found to deduplicate.")
        return

    total_removed = 0
    for filepath in sorted(data_files):
        rel = os.path.relpath(filepath, BASE_DIR)
        if dry_run:
            logger.info(f"  [DRY RUN] Would deduplicate: {rel}")
            continue
        original, after = deduplicate_file(filepath)
        removed = original - after
        total_removed += removed
        if removed > 0:
            logger.info(f"  ✓ {rel}: {original} → {after} ({removed} duplicates removed)")
        else:
            logger.info(f"  ✓ {rel}: {original} records, no duplicates found")

    if not dry_run:
        logger.info(f"\nTotal duplicates removed: {total_removed}")


def rebuild_main_businesses(dry_run=False):
    """
    Rebuild data/businesses.json by aggregating all state business files.
    This keeps the main dashboard file in sync.
    """
    logger.info("=" * 60)
    logger.info("STEP 4: Rebuilding main businesses.json")
    logger.info("=" * 60)

    data_files = [
        f for f in find_data_files()
        if "businesses.json" not in os.path.basename(f) or "state" in os.path.basename(f)
        # include state-specific files, exclude main aggregate
    ]
    # More precise: grab *_businesses.json files only
    state_data_files = glob.glob(os.path.join(DATA_DIR, "*_businesses.json"))
    state_data_files = [f for f in state_data_files if f != os.path.join(DATA_DIR, "businesses.json")]

    if not state_data_files:
        logger.info("No state data files found to aggregate.")
        return

    all_businesses = []
    for filepath in sorted(state_data_files):
        try:
            with open(filepath) as f:
                businesses = json.load(f)
            all_businesses.extend(businesses)
            logger.info(f"  Loaded {len(businesses):>5} records from {os.path.basename(filepath)}")
        except Exception as e:
            logger.error(f"  Could not read {filepath}: {e}")

    if not all_businesses:
        logger.info("No businesses to aggregate.")
        return

    # Final dedup on the aggregate by entity_number
    seen, unique = set(), []
    all_businesses.sort(key=lambda x: x.get("scraped_at", ""), reverse=True)
    for biz in all_businesses:
        key = biz.get("entity_number") or biz.get("business_id") or biz.get("name", "")
        if key and key not in seen:
            seen.add(key)
            unique.append(biz)
    unique.sort(key=lambda x: x.get("registration_date", ""), reverse=True)

    main_file = os.path.join(DATA_DIR, "businesses.json")
    if dry_run:
        logger.info(f"  [DRY RUN] Would write {len(unique)} records to businesses.json")
    else:
        with open(main_file, "w") as f:
            json.dump(unique, f, indent=2)
        logger.info(f"\n✓ businesses.json rebuilt with {len(unique)} total unique records")


def main():
    parser = argparse.ArgumentParser(description="Reset scraper data and run fresh 30-day pull")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview what would happen without making changes")
    args = parser.parse_args()

    if args.dry_run:
        logger.info("*** DRY RUN MODE — no files will be modified ***\n")

    start = datetime.now()
    logger.info("=" * 60)
    logger.info("BIZLEADS RESET & REFRESH")
    logger.info(f"Started: {start.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)
    logger.info("")

    # Step 1: Wipe all existing state + data files
    clear_all_data(dry_run=args.dry_run)

    # Step 2: Rerun all scrapers fresh
    success, failed = run_all_scrapers(dry_run=args.dry_run)

    # Step 3: Deduplicate within each state file
    deduplicate_all(dry_run=args.dry_run)

    # Step 4: Rebuild main businesses.json aggregate
    rebuild_main_businesses(dry_run=args.dry_run)

    elapsed = (datetime.now() - start).total_seconds()
    logger.info("")
    logger.info("=" * 60)
    logger.info("RESET & REFRESH COMPLETE")
    logger.info(f"  Time elapsed : {elapsed:.0f}s ({elapsed/60:.1f} min)")
    logger.info(f"  Scrapers OK  : {success}/{len(STATE_SCHEDULERS)}")
    if failed:
        logger.warning(f"  Failed       : {', '.join(failed)}")
    if args.dry_run:
        logger.info("  (DRY RUN — no changes made)")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
