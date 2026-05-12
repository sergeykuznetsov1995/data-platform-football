"""
Manager-only backfill for iceberg.bronze.fbref_match_managers.

Reuses the FBref scraper's CF-bypass infrastructure (nodriver + cf-verify)
but invokes ONLY parse_match_managers for each match-page — does NOT
re-run the other 5 parsers (shot_events / match_events / lineups /
team_stats / player_stats), avoiding row duplication in those tables.

Usage:
    python scripts/backfill_fbref_match_managers.py \\
        --league "ENG-Premier League" --season 2025 --max-matches 3

Match-id source: ``silver.fbref_match_enriched`` (typed/dedup'd schedule
view). Matches whose (match_id, league, season) tuple already exists in
``bronze.fbref_match_managers`` are skipped — incremental by design so a
partial run can resume cleanly.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
import urllib3
from typing import List, Tuple

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("backfill_managers")

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# /opt/airflow/dags MUST come BEFORE /opt/airflow/scrapers in sys.path —
# both expose a top-level ``utils`` package (one in dags/utils, another
# under scrapers/utils) and we need the dags one for ``silver_tasks``.
sys.path.insert(0, "/opt/airflow")
sys.path.insert(0, "/opt/airflow/dags")  # last insert(0,...) wins → first

import importlib.util as _ilutil  # noqa: E402

_spec = _ilutil.spec_from_file_location(
    "_dags_silver_tasks", "/opt/airflow/dags/utils/silver_tasks.py"
)
_silver_tasks_mod = _ilutil.module_from_spec(_spec)
_spec.loader.exec_module(_silver_tasks_mod)  # noqa: E402


def _parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--league", required=True)
    p.add_argument("--season", required=True, type=int)
    p.add_argument("--max-matches", type=int, default=3,
                   help="Smoke cap. 0 = unlimited.")
    p.add_argument("--dry-run", action="store_true",
                   help="Only list candidates; do not start the scraper.")
    p.add_argument("--proxy-file", default="/opt/airflow/proxys.txt")
    p.add_argument("--nodriver-cf-wait", type=float, default=30.0)
    p.add_argument("--use-xvfb", action="store_true", default=True)
    p.add_argument("--headless", action="store_true", default=True)
    return p.parse_args()


def _fetch_existing_match_ids(league: str, season: int) -> set:
    """Match_ids already populated for (league, season) in bronze.fbref_match_managers.

    Empty set if the table doesn't exist yet (first run).
    """
    conn = _silver_tasks_mod._get_trino_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT table_name FROM iceberg.information_schema.tables
            WHERE table_schema = 'bronze' AND table_name = 'fbref_match_managers'
        """)
        if not cur.fetchall():
            logger.info("bronze.fbref_match_managers does not exist yet")
            return set()

        cur.execute(
            "SELECT DISTINCT match_id FROM iceberg.bronze.fbref_match_managers "
            "WHERE league = ? AND season = ?",
            [league, season],
        )
        ids = {r[0] for r in cur.fetchall()}
        logger.info("Found %d existing manager rows for (%s, %d)",
                    len(ids), league, season)
        return ids
    finally:
        cur.close()


def _candidate_matches(league: str, season: int) -> List[Tuple[str, str]]:
    """Return [(match_id, date_iso)] sorted ascending from silver enriched view."""
    conn = _silver_tasks_mod._get_trino_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT match_id, CAST(date AS VARCHAR)
            FROM iceberg.silver.fbref_match_enriched
            WHERE league = ? AND season = ?
              AND match_id IS NOT NULL
              AND match_id NOT LIKE 'fut_%'
            ORDER BY date ASC
        """, [league, season])
        return cur.fetchall()
    finally:
        cur.close()


BATCH_SAVE_INTERVAL = 50


def _save_batch(scraper, frames: list, label: str) -> None:
    if not frames:
        return
    df = pd.concat(frames, ignore_index=True)
    path = scraper.save_to_iceberg(
        df=df, table_name="fbref_match_managers",
        partition_cols=["league", "season"],
    )
    logger.info("Batch save %s: %d rows -> %s", label, len(df), path)
    frames.clear()


def _scrape_managers(scraper, match_ids: List[str], league: str, season: int) -> int:
    """Fetch each match-page, parse managers, save in batches of N matches.

    Returns the number of matches with at least one parsed row.
    """
    from scrapers.fbref.constants import BASE_URL
    from scrapers.fbref.html_parser import parse_match_managers
    from bs4 import BeautifulSoup

    frames: list = []
    parsed = 0
    for i, mid in enumerate(match_ids, 1):
        url = f"{BASE_URL}/en/matches/{mid}"
        logger.info("[%d/%d] Fetching %s", i, len(match_ids), url)
        t0 = time.time()
        try:
            html = scraper._fetch_page(url, use_cache=False, page_type="match")
        except Exception as e:
            logger.warning("[%d/%d] Fetch raised %s for %s", i, len(match_ids), e, mid)
            html = None
        dt = time.time() - t0
        if not html:
            logger.warning("[%d/%d] Fetch failed for %s after %.1fs",
                           i, len(match_ids), mid, dt)
        else:
            soup = BeautifulSoup(html, "html.parser")
            df = parse_match_managers(soup)
            if df is None or df.empty:
                logger.warning("[%d/%d] No manager block parsed for %s",
                               i, len(match_ids), mid)
            else:
                df = df.copy()
                df["match_id"] = mid
                df["league"] = league
                df["season"] = season
                df = scraper._add_metadata(df, "match_managers")
                frames.append(df)
                parsed += 1
                logger.info("[%d/%d] %s — %d manager row(s) in %.1fs",
                            i, len(match_ids), mid, len(df), dt)

        if i % BATCH_SAVE_INTERVAL == 0 and frames:
            _save_batch(scraper, frames, f"after {i}/{len(match_ids)}")

    if frames:
        _save_batch(scraper, frames, f"final {len(match_ids)}/{len(match_ids)}")

    return parsed


def main():
    args = _parse_args()
    logger.info("Backfill manager-only: league=%s season=%d max_matches=%d",
                args.league, args.season, args.max_matches)

    existing = _fetch_existing_match_ids(args.league, args.season)
    candidates = _candidate_matches(args.league, args.season)
    todo = [(mid, d) for mid, d in candidates if mid not in existing]
    logger.info("Candidates: %d total, %d already done, %d to fetch",
                len(candidates), len(candidates) - len(todo), len(todo))

    if args.max_matches > 0:
        todo = todo[:args.max_matches]
    if not todo:
        logger.info("Nothing to do — all matches covered")
        return

    logger.info("Will scrape %d match(es); first=%s last=%s",
                len(todo), todo[0], todo[-1])

    if args.dry_run:
        logger.info("--dry-run: stopping before scraper init")
        return

    # Lazy import — pulls ~1.5GB of selenium/nodriver/etc.
    from scrapers.fbref.scraper import FBrefScraper

    scraper = FBrefScraper(
        leagues=[args.league],
        seasons=[args.season],
        proxy_file=args.proxy_file,
        use_nodriver=True,
        headless=args.headless,
        use_xvfb=args.use_xvfb,
        nodriver_cloudflare_wait=args.nodriver_cf_wait,
    )

    try:
        parsed = _scrape_managers(scraper, [mid for mid, _ in todo],
                                  args.league, args.season)
        logger.info("Backfill complete: %d/%d matches parsed and saved",
                    parsed, len(todo))
    finally:
        scraper.close()


if __name__ == "__main__":
    main()
