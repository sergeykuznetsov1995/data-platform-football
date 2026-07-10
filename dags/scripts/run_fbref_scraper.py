#!/usr/bin/env python3
"""
FBref Scraper Runner Script
===========================

Standalone script to run FBref scraper.
Called from Airflow via BashOperator to avoid memory issues with PythonOperator.

Supports two scraper types:
1. nodriver (default) - Browser-based with Cloudflare Turnstile bypass.
   DEAD against the managed interstitial CF rolled out ~2026-07 (#877) —
   do not use for new scrapes.
2. selenium - FBrefScraper; with FBREF_TRANSPORT=camoufox this is the
   working Cloudflare path (#846/#853) used by the prod DAG for schedule
   and combined match-level data.

Usage:
    # Schedule via the working camoufox transport (#CF-2026-07, #877)
    FBREF_TRANSPORT=camoufox python run_fbref_scraper.py --scraper-type selenium \
        --mode match_data --match-data-type schedule --proxy-file /path/to/proxys.txt

NOTE: As of 2025-2026, FBref uses Cloudflare Turnstile CAPTCHA.
      The deprecated soccerdata/curl_cffi scraper was removed (Apr 2026) —
      it cannot execute JavaScript, so it never bypassed the Turnstile.

NOTE (#877): when wrapping this runner with a timeout from the host, put
      `timeout` INSIDE the container command:
          docker exec ... airflow-scheduler timeout -k 30 <secs> python dags/scripts/run_fbref_scraper.py ...
      A host-side `timeout N docker exec ...` only kills the docker client;
      the python process (and its ~1 GB camoufox/Firefox child) keeps running
      in the container. SIGTERM/SIGINT are handled: the runner exits non-zero
      and closes the browser child cleanly.
"""

import argparse
import json
import logging
import os
import signal
import sys
from collections import Counter
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

# Replace-partitions completeness guard (#513 → #583): refuse a single_stat save
# that would shrink a fbref_{category}_{stat} (league, season) partition below
# this share of its existing rows, so a partial/failed scrape can't wipe a good
# season (precedent #536 45-50x bloat the other way). COUNT(*) (full-state per
# (league, season) — no replace_guard_key needed). The guard lives inside
# scrape_single_stat_type; this runner threads force_replace down and maps the
# resulting ReplaceGuardError to exit 3. --force-replace bypasses it.
_MIN_REPLACE_RATIO = 0.9
REPLACE_GUARD_MARKER = 'FBREF_REPLACE_GUARD'

# Noisy third-party loggers to suppress to WARNING level
_NOISY_LOGGERS = [
    'nodriver', 'uc', 'urllib3', 'websockets', 'asyncio',
    'selenium', 'undetected_chromedriver', 'hpack', 'httpx',
]


def _configure_logging(verbose: bool = False) -> None:
    """Configure logging level and suppress noisy third-party loggers."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    if not verbose:
        for name in _NOISY_LOGGERS:
            logging.getLogger(name).setLevel(logging.WARNING)


def _handle_termination(signum, frame):
    """Translate SIGTERM/SIGINT into SystemExit so the ``with <Scraper>``
    block in main() unwinds and closes the browser child (camoufox/Firefox,
    ~1 GB RSS) instead of orphaning it inside the container (#877).

    SystemExit is a BaseException — the generic ``except Exception`` error
    handling below does not swallow it.
    """
    raise SystemExit(128 + signum)  # 143 for SIGTERM, 130 for SIGINT


def _install_signal_handlers() -> None:
    signal.signal(signal.SIGTERM, _handle_termination)
    signal.signal(signal.SIGINT, _handle_termination)


def _trino_connect():
    """Open a Trino dbapi connection from env. Returns None on import error.

    Same pattern as run_sofascore_scraper.py (#842 skip-existing probe):
    TRINO_PASSWORD set -> https:8443 BasicAuth, else plain http:8080.
    """
    try:
        import trino
        import trino.auth as trino_auth
    except ImportError as e:
        logger.error("trino client unavailable: %s", e)
        return None

    user = os.environ.get('TRINO_USER', 'airflow')
    password = os.environ.get('TRINO_PASSWORD')
    if password:
        return trino.dbapi.connect(
            host=os.environ.get('TRINO_HOST', 'trino'),
            port=int(os.environ.get('TRINO_PORT', 8443)),
            user=user,
            catalog='iceberg',
            http_scheme='https',
            auth=trino_auth.BasicAuthentication(user, password),
            verify=False,
        )
    return trino.dbapi.connect(
        host=os.environ.get('TRINO_HOST', 'trino'),
        port=int(os.environ.get('TRINO_PORT', 8080)),
        user=user,
        catalog='iceberg',
    )


def _completed_schedule_leagues(leagues: list, season: int) -> set:
    """Return the leagues whose ``bronze.fbref_schedule`` (league, season)
    partition already looks complete, i.e. has >= FBREF_SCHEDULE_MIN_ROWS
    rows (#877 --skip-existing probe).

    Floor default 270 covers every top-5 full season (GER=306, covid
    FRA-2019=279, others 380) while rejecting genuinely partial data.
    Partial partitions are not a concern: the schedule page is parsed
    atomically and saved with replace_partitions per (league, season).

    Fail-open: any Trino error returns an empty set (scrape everything) —
    a false "not complete" costs one page (~0.3 MB), a false "complete"
    would silently lose data.
    """
    floor = int(os.environ.get('FBREF_SCHEDULE_MIN_ROWS', '270'))
    conn = _trino_connect()
    if conn is None:
        return set()
    try:
        cur = conn.cursor()
        placeholders = ', '.join('?' for _ in leagues)
        sql = (
            "SELECT league, COUNT(*) "
            "FROM iceberg.bronze.fbref_schedule "
            f"WHERE season = ? AND league IN ({placeholders}) "
            "GROUP BY league"
        )
        cur.execute(sql, (season, *leagues))
        counts = {r[0]: r[1] for r in cur.fetchall() if r and r[0] is not None}
        done = {lg for lg, cnt in counts.items() if cnt >= floor}
        logger.info(
            "skip-existing probe season=%s floor=%s: counts=%s -> complete=%s",
            season, floor, counts, sorted(done),
        )
        return done
    except Exception as e:
        logger.warning(
            "skip-existing probe on bronze.fbref_schedule failed (%s) — "
            "scraping all requested leagues.", e,
        )
        return set()


def _write_noop_traffic_summary(
    label: str,
    mode: str,
    extra: Optional[dict] = None,
    explicit_path: Optional[str] = None,
) -> None:
    """Write a zeroed `/tmp/fbref_traffic_<label>.json` for a skip-existing
    no-op run (#877), so the Airflow `traffic_guard_<label>` task sees a
    valid file with 0.0 MB instead of a stale one from a previous run.
    """
    payload = {
        'mode': mode,
        'label': label,
        'real_proxy_mb': 0.0,
        'real_proxy_bytes': 0,
        'real_proxy_requests': 0,
        'http_mb_downloaded': 0.0,
        'pages_downloaded': 0,
        'cf_challenge_attempts': 0,
        'cf_challenges_passed': 0,
        'cf_challenges_failed': 0,
        'successes': 0,
        'failures': 0,
    }
    if extra:
        payload.update(extra)

    path = explicit_path or f'/tmp/fbref_traffic_{label}.json'
    try:
        with open(path, 'w') as fh:
            json.dump(payload, fh, indent=2)
        logger.info(f"TRAFFIC_SUMMARY_JSON={json.dumps(payload)}")
    except Exception as e:
        logger.warning(f"Could not write traffic summary JSON to {path}: {e}")


def _get_traffic_diagnostics(scraper) -> dict:
    """Extract traffic metrics from scraper _stats for JSON diagnostics.

    Reports both HTML-only bytes (legacy `bytes_downloaded`) and real
    proxy bytes (via CDP Network events) — the latter is what actually
    counts against proxy quota.

    Issue #44: also surfaces per-resource-type breakdown, CF challenge
    counters, and browser-restart reason counts so downstream tooling
    (traffic_guard, audit report) can attribute MB/run to specific
    code paths.
    """
    # Pull a fresh snapshot first — flushes mid-session counters into _stats.
    if hasattr(scraper, '_update_real_traffic_stats'):
        try:
            scraper._update_real_traffic_stats()
        except Exception:
            pass

    stats = scraper._stats
    html_bytes = stats.get('bytes_downloaded', 0)
    real_bytes = stats.get('real_bytes_downloaded', 0)
    real_requests = stats.get('real_requests_count', 0)
    overhead_ratio = round(real_bytes / html_bytes, 2) if html_bytes > 0 else None

    # Per-resource-type breakdown in MB (rounded) for readability; also
    # keep raw bytes for downstream math.
    bytes_by_rtype = dict(stats.get('real_bytes_by_resource_type', {}) or {})
    reqs_by_rtype = dict(stats.get('real_requests_by_resource_type', {}) or {})
    mb_by_rtype = {k: round(v / 1024 / 1024, 3) for k, v in bytes_by_rtype.items()}

    # Issue #124 — curl_cffi HTTP fast-path counters (bypass CDP, so not in
    # real_bytes_*). Aggregated with CDP into total_proxy_*_by_resource_type
    # to give one canonical proxy-attribution view across both fetch paths.
    http_bytes = stats.get('http_bytes_downloaded', 0)
    http_requests = stats.get('http_requests_count', 0)
    http_bytes_by_rtype = dict(stats.get('http_bytes_by_resource_type', {}) or {})
    http_reqs_by_rtype = dict(stats.get('http_requests_by_resource_type', {}) or {})
    http_mb_by_rtype = {
        k: round(v / 1024 / 1024, 3) for k, v in http_bytes_by_rtype.items()
    }

    total_bytes_by_rtype = dict(bytes_by_rtype)
    for k, v in http_bytes_by_rtype.items():
        total_bytes_by_rtype[k] = total_bytes_by_rtype.get(k, 0) + v
    total_reqs_by_rtype = dict(reqs_by_rtype)
    for k, v in http_reqs_by_rtype.items():
        total_reqs_by_rtype[k] = total_reqs_by_rtype.get(k, 0) + v
    total_mb_by_rtype = {
        k: round(v / 1024 / 1024, 3) for k, v in total_bytes_by_rtype.items()
    }

    # Issue #624 — HTTP fast-path fallback diagnostics. Each fallback to the
    # nodriver browser risks a fresh CF cold-start (~2 MB proxy). The dominant
    # fallback cause gates the fix: the summary aggregates ALL records by
    # reason / cf-mitigated, the raw list (capped) keeps recent samples.
    http_diag = list(stats.get('http_fetch_diag', []) or [])
    diag_by_reason = Counter(d.get('reason') for d in http_diag)
    diag_by_cf_mitigated = Counter(
        d.get('cf_mitigated') for d in http_diag if d.get('cf_mitigated')
    )
    # Issue #624: a fallback where the minted proxy drifted from the current
    # nodriver proxy is a proxy-mismatch (cf_clearance is IP-bound). Counted
    # only when both fields are present and differ — the dominant cause read.
    diag_proxy_mismatch = sum(
        1 for d in http_diag
        if d.get('proxy_minted') and d.get('proxy')
        and d['proxy_minted'] != d['proxy']
    )

    return {
        'bytes_downloaded': html_bytes,
        'pages_downloaded': stats.get('pages_downloaded', 0),
        'mb_downloaded': round(html_bytes / 1024 / 1024, 2),
        'bytes_by_page_type': dict(stats.get('bytes_by_page_type', {}) or {}),
        # Real proxy traffic (includes CSS/JS/images if not blocked)
        'real_proxy_bytes': real_bytes,
        'real_proxy_mb': round(real_bytes / 1024 / 1024, 2),
        'real_proxy_requests': real_requests,
        'overhead_ratio': overhead_ratio,  # real/html; 1.0 = perfect (HTML only)
        # Issue #44 — new fields.
        'real_proxy_bytes_by_resource_type': bytes_by_rtype,
        'real_proxy_mb_by_resource_type': mb_by_rtype,
        'real_proxy_requests_by_resource_type': reqs_by_rtype,
        # Issue #616 — per-URL audit: top consumers + first/third-party split.
        'top_traffic_urls': list(stats.get('top_traffic_urls', []) or []),
        'real_bytes_by_url': dict(stats.get('real_bytes_by_url', {}) or {}),
        'first_party_bytes': int(stats.get('first_party_bytes', 0) or 0),
        'third_party_bytes': int(stats.get('third_party_bytes', 0) or 0),
        'first_party_mb': stats.get('first_party_mb', 0.0),
        'third_party_mb': stats.get('third_party_mb', 0.0),
        'cf_challenge_attempts': int(stats.get('cf_challenge_attempts', 0) or 0),
        'cf_challenges_passed': int(stats.get('cf_challenges_passed', 0) or 0),
        'cf_challenges_failed': int(stats.get('cf_challenges_failed', 0) or 0),
        'restart_reasons': dict(stats.get('restart_reasons', {}) or {}),
        # Issue #116 — diagnostic: count of loadingFinished events with no
        # cached resource_type (should stay near 0 with the requestWillBeSent
        # subscription in place).
        'resource_type_cache_misses': int(
            stats.get('resource_type_cache_misses', 0) or 0
        ),
        # Issue #124 — curl_cffi HTTP fast-path audit + CDP+HTTP aggregate.
        'http_bytes_downloaded': http_bytes,
        'http_mb_downloaded': round(http_bytes / 1024 / 1024, 3),
        'http_requests_count': http_requests,
        'http_bytes_by_resource_type': http_bytes_by_rtype,
        'http_mb_by_resource_type': http_mb_by_rtype,
        'http_requests_by_resource_type': http_reqs_by_rtype,
        'total_proxy_bytes_by_resource_type': total_bytes_by_rtype,
        'total_proxy_mb_by_resource_type': total_mb_by_rtype,
        'total_proxy_requests_by_resource_type': total_reqs_by_rtype,
        # Issue #624 — HTTP fast-path reliability. A fallback (http_fetch_fallback)
        # is a nodriver fetch that can trigger a CF cold-start; keeping the
        # fast-path success rate high is the lever for per-match proxy traffic.
        'http_fetch_ok': int(stats.get('http_fetch_ok', 0) or 0),
        'http_fetch_fallback': int(stats.get('http_fetch_fallback', 0) or 0),
        'http_fetch_diag': http_diag[-50:],
        'http_fetch_diag_summary': {
            'by_reason': dict(diag_by_reason),
            'by_cf_mitigated': dict(diag_by_cf_mitigated),
            'proxy_mismatch': diag_proxy_mismatch,
        },
    }


def _write_traffic_summary(
    scraper,
    label: str,
    mode: str,
    extra: Optional[dict] = None,
    explicit_path: Optional[str] = None,
) -> None:
    """Write `/tmp/fbref_traffic_<label>.json` for ANY entity.

    Previously only `combined_match_data` produced this file. Issue #44
    extends coverage to schedule, single_stat (player/team/keeper) so the
    per-task `traffic_guard` can attribute MB to each step of the DAG.

    Args:
        scraper: The active scraper (used for _stats + traffic metrics).
        label: Short identifier baked into the filename (e.g. 'match_all_data',
            'match_schedule', 'player_stats', 'keeper_keeper').
        mode: Echoed into the JSON (matches the CLI --mode value).
        extra: Optional per-mode keys to merge in (e.g. matches_successes).
        explicit_path: Override the default `/tmp/fbref_traffic_<label>.json`.
    """
    import json as _json

    traffic = _get_traffic_diagnostics(scraper)
    payload = {
        'mode': mode,
        'label': label,
        'real_proxy_mb': traffic.get('real_proxy_mb', 0.0),
        'real_proxy_bytes': traffic.get('real_proxy_bytes', 0),
        'real_proxy_requests': traffic.get('real_proxy_requests', 0),
        'real_proxy_mb_by_resource_type': traffic.get(
            'real_proxy_mb_by_resource_type', {}
        ),
        'real_proxy_requests_by_resource_type': traffic.get(
            'real_proxy_requests_by_resource_type', {}
        ),
        # Issue #616 — per-URL audit for the blockable-XHR/SCRIPT analysis.
        'top_traffic_urls': traffic.get('top_traffic_urls', []),
        'first_party_mb': traffic.get('first_party_mb', 0.0),
        'third_party_mb': traffic.get('third_party_mb', 0.0),
        'cf_challenge_attempts': traffic.get('cf_challenge_attempts', 0),
        'cf_challenges_passed': traffic.get('cf_challenges_passed', 0),
        'cf_challenges_failed': traffic.get('cf_challenges_failed', 0),
        'restart_reasons': traffic.get('restart_reasons', {}),
        'resource_type_cache_misses': traffic.get('resource_type_cache_misses', 0),
        'html_mb_downloaded': traffic.get('mb_downloaded', 0.0),
        'pages_downloaded': traffic.get('pages_downloaded', 0),
        'overhead_ratio': traffic.get('overhead_ratio'),
        # Issue #124 — curl_cffi HTTP fast-path audit + CDP+HTTP aggregate.
        'http_mb_downloaded': traffic.get('http_mb_downloaded', 0.0),
        'http_requests_count': traffic.get('http_requests_count', 0),
        'http_mb_by_resource_type': traffic.get('http_mb_by_resource_type', {}),
        'http_requests_by_resource_type': traffic.get(
            'http_requests_by_resource_type', {}
        ),
        'total_proxy_mb_by_resource_type': traffic.get(
            'total_proxy_mb_by_resource_type', {}
        ),
        'total_proxy_requests_by_resource_type': traffic.get(
            'total_proxy_requests_by_resource_type', {}
        ),
        # Issue #624 — HTTP fast-path reliability + fallback diagnostics so the
        # per-task traffic JSON (read by traffic_guard) carries the cold-start
        # signal, not just totals.
        'http_fetch_ok': traffic.get('http_fetch_ok', 0),
        'http_fetch_fallback': traffic.get('http_fetch_fallback', 0),
        'http_fetch_diag': traffic.get('http_fetch_diag', []),
        'http_fetch_diag_summary': traffic.get('http_fetch_diag_summary', {}),
    }
    if extra:
        payload.update(extra)

    path = explicit_path or f'/tmp/fbref_traffic_{label}.json'
    try:
        with open(path, 'w') as fh:
            _json.dump(payload, fh, indent=2)
        # Single-line log mirrors the existing TRAFFIC_SUMMARY_JSON= grep target.
        logger.info(f"TRAFFIC_SUMMARY_JSON={_json.dumps(payload)}")
    except Exception as e:
        logger.warning(f"Could not write traffic summary JSON to {path}: {e}")


def main():
    # #877: must be first — a SIGTERM before handlers are installed would
    # kill the runner without unwinding the scraper context manager.
    _install_signal_handlers()

    parser = argparse.ArgumentParser(description='Run FBref scraper')

    # === Scraper type selection ===
    parser.add_argument(
        '--scraper-type',
        type=str,
        choices=['nodriver', 'selenium'],
        default='nodriver',
        help='Scraper type: nodriver (recommended, Cloudflare Turnstile bypass) '
             'or selenium (undetected-chromedriver, used for combined match data)'
    )

    # === Mode selection ===
    parser.add_argument(
        '--mode',
        type=str,
        choices=['full', 'single_stat', 'match_data', 'combined_match_data',
                 'combined_season_stats'],
        default='full',
        help='Scraping mode: full (all data), single_stat (one stat_type), '
             'match_data (one match data type), combined_match_data (all match data in one pass), '
             'combined_season_stats (all player/team/keeper season stats in one pass)'
    )
    parser.add_argument(
        '--stat-type',
        type=str,
        default=None,
        help='Stat type for single_stat mode (stats, shooting, playingtime, misc, keeper)'
    )
    parser.add_argument(
        '--data-category',
        type=str,
        choices=['player', 'team', 'keeper'],
        default='player',
        help='Data category for single_stat mode (player, team, keeper)'
    )
    parser.add_argument(
        '--match-data-type',
        type=str,
        choices=['schedule', 'shot_events', 'match_events', 'lineups'],
        default=None,
        help='Match data type for match_data mode'
    )

    # === Common arguments ===
    parser.add_argument(
        '--leagues',
        type=str,
        default='ENG-Premier League',
        help='Comma-separated list of leagues'
    )
    _now = datetime.now()
    _current_season = _now.year if _now.month >= 8 else _now.year - 1
    parser.add_argument(
        '--season',
        type=int,
        default=_current_season,
        help='Season year (default: current season)'
    )
    parser.add_argument(
        '--output',
        type=str,
        default='/tmp/fbref_result.json',
        help='Output file for results'
    )
    parser.add_argument(
        '--proxy-file',
        type=str,
        default=None,
        help='Path to file with proxies (format: host:port:user:pass)'
    )

    # === Selenium-specific arguments ===
    parser.add_argument(
        '--headless',
        action='store_true',
        default=True,
        help='Run browser in headless mode (selenium scraper)'
    )
    parser.add_argument(
        '--use-xvfb',
        action='store_true',
        default=True,
        help='Use xvfb for virtual display (selenium scraper)'
    )
    parser.add_argument(
        '--use-nodriver',
        action='store_true',
        help='Use nodriver instead of undetected-chromedriver (better Cloudflare bypass)'
    )
    parser.add_argument(
        '--nodriver-cloudflare-wait',
        type=float,
        default=30.0,
        help='Time to wait for Cloudflare challenge when using nodriver (seconds)'
    )
    parser.add_argument(
        '--cloudflare-wait',
        type=float,
        default=90.0,
        help='Time to wait for Cloudflare challenge (nodriver scraper)'
    )
    parser.add_argument(
        '--cf-verify-retries',
        type=int,
        default=12,
        help='Maximum retries for cf-verify plugin (nodriver scraper)'
    )
    parser.add_argument(
        '--max-retries',
        type=int,
        default=5,
        help='Maximum page load retries (nodriver scraper)'
    )
    parser.add_argument(
        '--content-timeout',
        type=float,
        default=45.0,
        help='Timeout for content extraction in seconds (default 45).'
    )

    # === Full mode specific arguments ===
    parser.add_argument(
        '--match-stats',
        action='store_true',
        help='[full mode] Collect per-match player stats (slow, selenium only)'
    )
    parser.add_argument(
        '--shot-events',
        action='store_true',
        default=True,
        help='[full mode] Collect shot events with xG data (selenium only)'
    )
    parser.add_argument(
        '--no-shot-events',
        action='store_true',
        help='[full mode] Disable shot events collection'
    )
    parser.add_argument(
        '--match-events',
        action='store_true',
        default=True,
        help='[full mode] Collect match events (goals, cards, substitutions, selenium only)'
    )
    parser.add_argument(
        '--no-match-events',
        action='store_true',
        help='[full mode] Disable match events collection'
    )
    parser.add_argument(
        '--lineups',
        action='store_true',
        default=True,
        help='[full mode] Collect team lineups (selenium only)'
    )
    parser.add_argument(
        '--no-lineups',
        action='store_true',
        help='[full mode] Disable lineups collection'
    )
    parser.add_argument(
        '--team-match-stats',
        action='store_true',
        help='[full mode] Collect team-level match statistics (slow, selenium only)'
    )
    parser.add_argument(
        '--max-matches',
        type=int,
        default=50,
        help='Maximum matches to scrape per league/season (0 = no limit, selenium only)'
    )
    parser.add_argument(
        '--skip-existing',
        action='store_true',
        help='[match_data schedule mode, #877] Probe bronze.fbref_schedule '
             'and skip leagues whose (league, season) partition already has '
             '>= FBREF_SCHEDULE_MIN_ROWS rows (default 270). Never skips the '
             'current season. Do not use for competitions with <270 matches '
             'per season (e.g. UCL, WC). Fail-open: probe errors scrape all.'
    )
    parser.add_argument(
        '--no-incremental',
        action='store_true',
        help='[combined_match_data mode] Re-scrape matches even if they '
             'already exist in fbref_match_player_stats. Required when '
             'a NEW Bronze table (e.g. fbref_match_managers) needs to be '
             'backfilled for already-scraped matches.'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable DEBUG logging (default: INFO with noisy loggers suppressed)'
    )
    parser.add_argument(
        '--traffic-output',
        type=str,
        default=None,
        help='[Issue #44] Override the auto-generated path '
             '/tmp/fbref_traffic_<label>.json where the per-task traffic '
             'summary (real_proxy_mb, per-resource-type, CF challenges, '
             'restart reasons) is written. Useful when one bash command '
             'should not clobber the file from a previous step.'
    )
    parser.add_argument(
        '--force-replace',
        action='store_true',
        help='Bypass the single_stat completeness guard — write even if the '
             'scraped frame shrinks the existing (league, season) partition. '
             'Use for a deliberate first backfill or a known legitimate shrink.'
    )
    args = parser.parse_args()

    # Configure logging AFTER parsing args so --verbose takes effect
    _configure_logging(verbose=args.verbose)

    leagues = [l.strip() for l in args.leagues.split(',')]

    # #920 bridge: single_year tournaments must never inherit the club-formula
    # season (July 2026 -> 2025). A 2025-labelled WC run both MISLABELS the
    # partition and — because match saves replace by match_id — WIPES the
    # correct 2026 partition. Substitute the active tournament year; a mixed
    # club+WC call cannot carry two seasons, so WC is dropped with a warning
    # (scrape it in a dedicated call), mirroring run_whoscored_scraper.
    if 'INT-World Cup' in leagues:
        from utils.medallion_config import get_active_season
        _wc_season = get_active_season('INT-World Cup')
        if len(leagues) > 1:
            logger.warning(
                "INT-World Cup dropped from mixed call (needs its own season; "
                f"leagues={leagues}). Scrape it with --leagues 'INT-World Cup'.")
            leagues = [l for l in leagues if l != 'INT-World Cup']
        elif _wc_season is None:
            logger.warning(
                "INT-World Cup is out of its tournament window "
                f"(competitions.yaml) — nothing to scrape; exiting 0.")
            with open(args.output, 'w') as f:
                json.dump({'mode': args.mode, 'tables': [], 'errors': [],
                           'skipped': 'out_of_window'}, f)
            return 0
        elif int(args.season) != int(_wc_season):
            logger.info(
                f"INT-World Cup: overriding --season {args.season} -> "
                f"{_wc_season} (active single_year season, #920 bridge).")
            args.season = _wc_season

    logger.info(f"Starting FBref scraper: scraper_type={args.scraper_type}, mode={args.mode}")
    logger.info(f"Leagues: {leagues}, Season: {args.season}")
    logger.info(f"Proxy file: {args.proxy_file}")

    results = {
        'mode': args.mode,
        'scraper_type': args.scraper_type,
        'tables': [],
        'errors': [],
        'diagnostics': {}
    }

    # #877: --skip-existing — don't re-fetch schedule seasons that are
    # already complete in bronze. Runs BEFORE the scraper is created, so a
    # full skip never starts Firefox/xvfb. Current season is never skipped
    # (it keeps growing until season end).
    if (
        args.skip_existing
        and args.mode == 'match_data'
        and args.match_data_type == 'schedule'
        and args.season < _current_season
    ):
        _done = _completed_schedule_leagues(leagues, args.season)
        _skipped = sorted(set(leagues) & _done)
        if _skipped:
            leagues = [lg for lg in leagues if lg not in _done]
            logger.info(
                f"skip-existing: schedule already complete for {_skipped} "
                f"season={args.season}; remaining leagues: {leagues}"
            )
            results['diagnostics']['skipped_leagues'] = _skipped
        if not leagues:
            # Full no-op: same sanctioned pattern as the combined_match_data
            # all-already-scraped path — populate tables so the
            # "no data and no errors -> exit 1" check passes.
            results['tables'] = ['iceberg.bronze.fbref_schedule']
            results['diagnostics']['all_already_scraped'] = True
            results['match_data_type'] = args.match_data_type
            _write_noop_traffic_summary(
                label=f"match_{args.match_data_type}",
                mode='match_data',
                extra={
                    'match_data_type': args.match_data_type,
                    'skip_existing': True,
                    'skipped_leagues': _skipped,
                },
                explicit_path=args.traffic_output,
            )
            with open(args.output, 'w') as f:
                json.dump(results, f, indent=2)
            logger.info(
                f"skip-existing: nothing to scrape "
                f"(all leagues complete for season={args.season}); exiting 0"
            )
            print(json.dumps(results, indent=2))
            return 0

    # max_matches=0 means no limit (None)
    max_matches = args.max_matches if args.max_matches > 0 else None

    # ==========================================================================
    # Nodriver scraper (recommended, Cloudflare Turnstile bypass)
    # ==========================================================================
    if args.scraper_type == 'nodriver':
        # Modes NOT in this set (e.g. combined_match_data) used to fall through
        # into the 'full' else-branch and silently run a full season scrape —
        # wrong tables and a lot of wasted proxy MB. Fail loudly instead.
        _NODRIVER_MODES = {'single_stat', 'match_data', 'full'}
        if args.mode not in _NODRIVER_MODES:
            error_msg = (
                f"Mode '{args.mode}' is not supported by the nodriver scraper "
                f"(supported: {sorted(_NODRIVER_MODES)}). "
                f"Use --scraper-type selenium for this mode."
            )
            logger.error(error_msg)
            results['errors'].append(error_msg)
            with open(args.output, 'w') as f:
                json.dump(results, f)
            sys.exit(2)

        logger.info("Using nodriver scraper (Cloudflare Turnstile bypass)")
        logger.info(f"Headless: {args.headless}, use_xvfb: {args.use_xvfb}")
        logger.info(f"Cloudflare wait: {args.cloudflare_wait}s, cf-verify retries: {args.cf_verify_retries}")
        logger.info(f"Content timeout: {args.content_timeout}s")
        logger.info(f"Proxy file: {args.proxy_file}")

        results['diagnostics']['nodriver_enabled'] = True
        results['diagnostics']['cloudflare_wait'] = args.cloudflare_wait
        results['diagnostics']['cf_verify_retries'] = args.cf_verify_retries
        results['diagnostics']['content_timeout'] = args.content_timeout

        try:
            from scrapers.base.base_scraper import ReplaceGuardError
            from scrapers.nodriver_fbref import NodriverFBrefScraper

            with NodriverFBrefScraper(
                leagues=leagues,
                seasons=[args.season],
                proxy_file=args.proxy_file,
                headless=args.headless,
                use_xvfb=args.use_xvfb,
                cloudflare_wait=args.cloudflare_wait,
                max_retries=args.max_retries,
                cf_verify_max_retries=args.cf_verify_retries,
                content_timeout=args.content_timeout,
            ) as scraper:

                # =============================================================
                # MODE: single_stat
                # =============================================================
                if args.mode == 'single_stat':
                    if not args.stat_type:
                        raise ValueError("--stat-type is required for single_stat mode")

                    logger.info(
                        f"Single stat mode: category={args.data_category}, "
                        f"stat_type={args.stat_type}"
                    )

                    scrape_result = scraper.scrape_single_stat_type(
                        stat_type=args.stat_type,
                        data_category=args.data_category,
                        force_replace=args.force_replace,
                    )

                    results['tables'] = list(scrape_result.values())
                    results['stat_type'] = args.stat_type
                    results['data_category'] = args.data_category
                    results['diagnostics']['scraper_stats'] = scraper.get_stats()
                    results['diagnostics']['traffic'] = _get_traffic_diagnostics(scraper)

                    # Issue #44: also emit traffic JSON from the nodriver-only
                    # code path (the selenium branch has its own equivalent).
                    _stat_label = f"{args.data_category}_{args.stat_type}"
                    _write_traffic_summary(
                        scraper,
                        label=_stat_label,
                        mode='single_stat',
                        extra={
                            'stat_type': args.stat_type,
                            'data_category': args.data_category,
                            'successes': scraper._stats.get('successes', 0),
                            'failures': scraper._stats.get('failures', 0),
                        },
                        explicit_path=args.traffic_output,
                    )

                    logger.info(f"Single stat scrape completed: {list(scrape_result.keys())}")

                    if not scrape_result:
                        error_msg = (
                            f"No data collected for {args.data_category}_{args.stat_type}. "
                            f"Stats: {scraper.get_stats()}"
                        )
                        logger.error(error_msg)
                        results['errors'].append(error_msg)

                # =============================================================
                # MODE: match_data (schedule)
                # =============================================================
                elif args.mode == 'match_data':
                    if not args.match_data_type:
                        raise ValueError("--match-data-type is required for match_data mode")

                    if args.match_data_type != 'schedule':
                        logger.warning(
                            f"Match data type '{args.match_data_type}' not yet supported by nodriver scraper. "
                            f"Use --scraper-type selenium for detailed match data."
                        )
                        results['tables'] = []
                        results['match_data_type'] = args.match_data_type
                    else:
                        logger.info("Scraping schedule...")
                        scrape_result = scraper.scrape_schedule()

                        results['tables'] = list(scrape_result.values())
                        results['match_data_type'] = args.match_data_type
                        results['diagnostics']['scraper_stats'] = scraper.get_stats()
                        results['diagnostics']['traffic'] = _get_traffic_diagnostics(scraper)

                        # Issue #44: emit traffic JSON for the schedule task
                        # (nodriver-only path — selenium path handles its own).
                        _write_traffic_summary(
                            scraper,
                            label=f"match_{args.match_data_type}",
                            mode='match_data',
                            extra={
                                'match_data_type': args.match_data_type,
                                'successes': scraper._stats.get('successes', 0),
                                'failures': scraper._stats.get('failures', 0),
                            },
                            explicit_path=args.traffic_output,
                        )

                        logger.info(f"Schedule scrape completed: {list(scrape_result.keys())}")

                        if not scrape_result:
                            error_msg = "No schedule data collected"
                            logger.error(error_msg)
                            results['errors'].append(error_msg)

                # =============================================================
                # MODE: full (not recommended for nodriver - use single_stat)
                # =============================================================
                elif args.mode == 'full':
                    logger.info(
                        "Full mode with nodriver: sequential collection "
                        "(schedule → player → team → keeper stats)"
                    )

                    scrape_results = scraper.scrape_all()

                    results['tables'] = list(scrape_results.values())
                    results['diagnostics']['scraper_stats'] = scraper.get_stats()
                    results['diagnostics']['traffic'] = _get_traffic_diagnostics(scraper)

                    logger.info(
                        f"Full scrape completed: {len(scrape_results)} tables saved to Iceberg"
                    )

        except ImportError as e:
            # #468: no silent fallback to selenium — it never worked (the
            # else-branch below belongs to an already-evaluated if), and a
            # live one would ignore nodriver args and write different table
            # names. A broken image must fail the DAG loudly.
            error_msg = f"Failed to import NodriverFBrefScraper: {e}"
            logger.error(error_msg)
            results['errors'].append(error_msg)
            with open(args.output, 'w') as f:
                json.dump(results, f)
            sys.exit(1)

        except ReplaceGuardError as e:
            # Guard refused: a partial single_stat scrape would shrink the
            # (league, season) partition — nothing written. Distinct exit 3 so
            # an operator can tell a refused guard from a hard failure (#583).
            msg = f"{REPLACE_GUARD_MARKER}: {e}"
            logger.error(msg)
            results['errors'].append(msg)
            with open(args.output, 'w') as f:
                json.dump(results, f)
            sys.exit(3)

        except Exception as e:
            logger.error(f"Nodriver scraper failed: {e}", exc_info=True)
            results['errors'].append(str(e))
            with open(args.output, 'w') as f:
                json.dump(results, f)
            sys.exit(1)

    # ==========================================================================
    # Selenium scraper (browser-based, Cloudflare bypass)
    # ==========================================================================
    else:  # scraper_type == 'selenium'
        logger.info("Using Selenium scraper (browser-based)")
        logger.info(f"Headless: {args.headless}, use_xvfb: {args.use_xvfb}")
        logger.info(f"Use nodriver: {args.use_nodriver}, cloudflare_wait: {args.nodriver_cloudflare_wait}s")

        # Add nodriver diagnostics
        if args.use_nodriver:
            results['diagnostics']['nodriver_enabled'] = True
            results['diagnostics']['nodriver_cloudflare_wait'] = args.nodriver_cloudflare_wait
            logger.info("Nodriver mode enabled - using advanced Cloudflare bypass")

        try:
            from scrapers.base.base_scraper import ReplaceGuardError
            from scrapers.fbref import FBrefScraper

            with FBrefScraper(
                leagues=leagues,
                seasons=[args.season],
                headless=args.headless,
                use_xvfb=args.use_xvfb,
                proxy_file=args.proxy_file,
                use_nodriver=args.use_nodriver,
                nodriver_cloudflare_wait=args.nodriver_cloudflare_wait,
            ) as scraper:

                # =============================================================
                # MODE: single_stat
                # =============================================================
                if args.mode == 'single_stat':
                    if not args.stat_type:
                        raise ValueError("--stat-type is required for single_stat mode")

                    logger.info(
                        f"Single stat mode: category={args.data_category}, "
                        f"stat_type={args.stat_type}"
                    )

                    scrape_results = scraper.scrape_single_stat_type(
                        stat_type=args.stat_type,
                        data_category=args.data_category,
                        force_replace=args.force_replace,
                    )

                    results['tables'] = list(scrape_results.values())
                    results['stat_type'] = args.stat_type
                    results['data_category'] = args.data_category

                    results['diagnostics']['scraper_stats'] = {
                        'successes': scraper._stats.get('successes', 0),
                        'failures': scraper._stats.get('failures', 0),
                    }
                    results['diagnostics']['traffic'] = _get_traffic_diagnostics(scraper)

                    # Issue #44: emit /tmp/fbref_traffic_<category>_<stat>.json
                    # so per-task traffic_guard can read it.
                    _stat_label = f"{args.data_category}_{args.stat_type}"
                    _write_traffic_summary(
                        scraper,
                        label=_stat_label,
                        mode='single_stat',
                        extra={
                            'stat_type': args.stat_type,
                            'data_category': args.data_category,
                            'successes': scraper._stats.get('successes', 0),
                            'failures': scraper._stats.get('failures', 0),
                        },
                        explicit_path=args.traffic_output,
                    )

                    logger.info(f"Single stat scrape completed: {list(scrape_results.keys())}")

                    if not scrape_results:
                        error_msg = (
                            f"No data collected for {args.data_category}_{args.stat_type}. "
                            f"Scraper stats: successes={scraper._stats.get('successes', 0)}, "
                            f"failures={scraper._stats.get('failures', 0)}"
                        )
                        logger.error(error_msg)
                        results['errors'].append(error_msg)

                # =============================================================
                # MODE: match_data
                # =============================================================
                elif args.mode == 'match_data':
                    if not args.match_data_type:
                        raise ValueError("--match-data-type is required for match_data mode")

                    logger.info(
                        f"Match data mode: type={args.match_data_type}, "
                        f"max_matches={max_matches}"
                    )

                    scrape_results = scraper.scrape_match_data(
                        data_type=args.match_data_type,
                        max_matches=max_matches,
                    )

                    results['tables'] = list(scrape_results.values())
                    results['match_data_type'] = args.match_data_type

                    results['diagnostics']['scraper_stats'] = {
                        'successes': scraper._stats.get('successes', 0),
                        'failures': scraper._stats.get('failures', 0),
                    }
                    results['diagnostics']['traffic'] = _get_traffic_diagnostics(scraper)

                    # Issue #44: emit /tmp/fbref_traffic_match_<type>.json
                    # for schedule / shot_events / etc. — currently only
                    # `schedule` is wired into the DAG, but the file pattern
                    # is uniform.
                    _write_traffic_summary(
                        scraper,
                        label=f"match_{args.match_data_type}",
                        mode='match_data',
                        extra={
                            'match_data_type': args.match_data_type,
                            'successes': scraper._stats.get('successes', 0),
                            'failures': scraper._stats.get('failures', 0),
                        },
                        explicit_path=args.traffic_output,
                    )

                    logger.info(f"Match data scrape completed: {list(scrape_results.keys())}")

                    if not scrape_results:
                        error_msg = (
                            f"No data collected for match_data type '{args.match_data_type}'. "
                            f"Scraper stats: successes={scraper._stats.get('successes', 0)}, "
                            f"failures={scraper._stats.get('failures', 0)}"
                        )
                        logger.error(error_msg)
                        results['errors'].append(error_msg)

                # =============================================================
                # MODE: combined_match_data (5x efficiency optimization)
                # =============================================================
                elif args.mode == 'combined_match_data':
                    logger.info(
                        f"Combined match data mode: max_matches={max_matches}. "
                        f"Collecting shot_events, match_events, lineups, "
                        f"match_team_stats, match_player_stats in one pass"
                    )

                    scrape_results = scraper.scrape_combined_match_data(
                        max_matches=max_matches,
                        incremental=not args.no_incremental,
                    )

                    results['tables'] = list(scrape_results.values())
                    results['mode'] = 'combined_match_data'

                    results['diagnostics']['scraper_stats'] = {
                        'successes': scraper._stats.get('successes', 0),
                        'failures': scraper._stats.get('failures', 0),
                    }
                    traffic = _get_traffic_diagnostics(scraper)
                    results['diagnostics']['traffic'] = traffic
                    results['diagnostics']['optimization'] = (
                        '5-way single-pass parse (shot_events + match_events + '
                        'lineups + match_team_stats + match_player_stats)'
                    )

                    # Issue #44: use shared helper so combined_match_data,
                    # single_stat and match_data emit the same schema. Filename
                    # stays `/tmp/fbref_traffic_match_all_data.json` for
                    # backward compatibility with existing traffic_guard.
                    _write_traffic_summary(
                        scraper,
                        label='match_all_data',
                        mode='combined_match_data',
                        extra={
                            'matches_successes': scraper._stats.get('successes', 0),
                            'matches_failures': scraper._stats.get('failures', 0),
                            'schedule_source': scraper._stats.get(
                                'schedule_source', 'unknown'
                            ),
                        },
                        explicit_path=args.traffic_output,
                    )

                    logger.info(
                        f"Combined match data scrape completed: {list(scrape_results.keys())}"
                    )

                    # Enhanced diagnostics for combined_match_data
                    stats = scraper._stats
                    failures = stats.get('failures', 0)
                    successes = stats.get('successes', 0)
                    skipped = stats.get('skipped_league_seasons', 0)
                    schedule_source = stats.get('schedule_source', 'unknown')
                    trino_available = stats.get('trino_available', None)

                    results['diagnostics']['schedule_source'] = schedule_source
                    results['diagnostics']['skipped_league_seasons'] = skipped
                    results['diagnostics']['trino_available'] = trino_available

                    if not scrape_results:
                        if trino_available is False and failures > 0:
                            error_msg = (
                                f"Trino unavailable: schedule could not be read from Iceberg. "
                                f"File fallback also failed. "
                                f"successes={successes}, failures={failures}, "
                                f"skipped_league_seasons={skipped}"
                            )
                            logger.error(error_msg)
                            results['errors'].append(error_msg)
                        elif failures > 0:
                            error_msg = (
                                f"No data collected for combined_match_data but had {failures} failures. "
                                f"schedule_source={schedule_source}, "
                                f"successes={successes}, failures={failures}, "
                                f"skipped_league_seasons={skipped}"
                            )
                            logger.error(error_msg)
                            results['errors'].append(error_msg)
                        elif successes == 0 and skipped == 0 and schedule_source in ('file', 'iceberg'):
                            # Schedule was found but no new matches — everything already scraped.
                            # This is NOT an error: incremental mode correctly detected all matches exist.
                            logger.info(
                                f"No new matches for combined_match_data — all matches already in Iceberg "
                                f"(schedule_source={schedule_source}, successes={successes}, "
                                f"failures={failures}, skipped={skipped})"
                            )
                            # Populate tables with known Iceberg paths so downstream checks pass
                            results['tables'] = [
                                'iceberg.bronze.fbref_match_events',
                                'iceberg.bronze.fbref_lineups',
                                'iceberg.bronze.fbref_match_team_stats',
                                'iceberg.bronze.fbref_match_player_stats',
                            ]
                            results['diagnostics']['all_already_scraped'] = True
                        elif successes == 0:
                            error_msg = (
                                f"No matches processed for combined_match_data. "
                                f"Schedule not found (schedule_source={schedule_source}). "
                                f"Ensure schedule_task completed and Trino is available. "
                                f"successes={successes}, failures={failures}, "
                                f"skipped_league_seasons={skipped}"
                            )
                            logger.error(error_msg)
                            results['errors'].append(error_msg)
                        else:
                            logger.info(
                                f"No new data for combined_match_data (all matches already scraped). "
                                f"successes={successes}, failures={failures}"
                            )

                # =============================================================
                # MODE: combined_season_stats (one fetch per season page,
                # player + team tables parsed from the same HTML)
                # =============================================================
                elif args.mode == 'combined_season_stats':
                    logger.info(
                        "Combined season stats mode: all player/team/keeper "
                        "stats in one pass (5 pages per league/season)"
                    )

                    scrape_results = scraper.scrape_combined_season_stats(
                        force_replace=args.force_replace,
                    )

                    results['tables'] = list(scrape_results['tables'].values())
                    results['errors'].extend(scrape_results['errors'])

                    results['diagnostics']['scraper_stats'] = {
                        'successes': scraper._stats.get('successes', 0),
                        'failures': scraper._stats.get('failures', 0),
                    }
                    results['diagnostics']['traffic'] = _get_traffic_diagnostics(scraper)

                    _write_traffic_summary(
                        scraper,
                        label='season_stats',
                        mode='combined_season_stats',
                        extra={
                            'successes': scraper._stats.get('successes', 0),
                            'failures': scraper._stats.get('failures', 0),
                            'guard_refusals': scrape_results['guard_refusals'],
                        },
                        explicit_path=args.traffic_output,
                    )

                    logger.info(
                        f"Combined season stats completed: "
                        f"{sorted(scrape_results['tables'].keys())}"
                    )

                    # #583 semantics: a refused guard is a distinct exit 3 so
                    # the operator can tell it from a hard failure. Tables that
                    # did save are idempotent (replace_partitions) — a re-run
                    # after fixing the guard just rewrites them.
                    if scrape_results['guard_refusals']:
                        for refusal in scrape_results['guard_refusals']:
                            msg = f"{REPLACE_GUARD_MARKER}: {refusal}"
                            logger.error(msg)
                            results['errors'].append(msg)
                        with open(args.output, 'w') as f:
                            json.dump(results, f)
                        sys.exit(3)

                # =============================================================
                # MODE: full
                # =============================================================
                else:  # mode == 'full'
                    include_shot_events = args.shot_events and not args.no_shot_events
                    include_match_events = args.match_events and not args.no_match_events
                    include_lineups = args.lineups and not args.no_lineups

                    logger.info(
                        f"Full mode (selenium): "
                        f"shot_events={include_shot_events}, match_events={include_match_events}, "
                        f"lineups={include_lineups}"
                    )
                    logger.info(f"Max matches per league: {max_matches if max_matches else 'unlimited'}")

                    scrape_results = scraper.scrape_all(
                        include_match_stats=args.match_stats,
                        include_shot_events=include_shot_events,
                        include_match_events=include_match_events,
                        include_lineups=include_lineups,
                        include_team_match_stats=args.team_match_stats,
                        max_matches_per_league=max_matches,
                    )

                    results['tables'] = list(scrape_results.values())

                    # Add row count placeholders for backwards compatibility
                    results['schedule_rows'] = 1 if 'schedule' in scrape_results else 0
                    results['team_stats_rows'] = 1 if 'team_stats' in scrape_results else 0
                    results['player_stats_rows'] = 1 if 'player_stats' in scrape_results else 0
                    results['match_stats_rows'] = 1 if 'player_match_stats' in scrape_results else 0
                    results['shot_events_rows'] = 1 if 'shot_events' in scrape_results else 0
                    results['match_events_rows'] = 1 if 'match_events' in scrape_results else 0
                    results['lineups_rows'] = 1 if 'lineups' in scrape_results else 0
                    results['team_match_stats_rows'] = 1 if 'team_match_stats' in scrape_results else 0

                    logger.info(f"Full scrape completed. Tables saved: {list(scrape_results.keys())}")

        except ReplaceGuardError as e:
            # Guard refused: a partial single_stat scrape would shrink the
            # (league, season) partition — nothing written. Distinct exit 3 so
            # an operator can tell a refused guard from a hard failure (#583).
            msg = f"{REPLACE_GUARD_MARKER}: {e}"
            logger.error(msg)
            results['errors'].append(msg)
            with open(args.output, 'w') as f:
                json.dump(results, f)
            sys.exit(3)

        except Exception as e:
            logger.error(f"Selenium scraper failed: {e}", exc_info=True)
            results['errors'].append(str(e))
            with open(args.output, 'w') as f:
                json.dump(results, f)
            sys.exit(1)

    # Write results
    with open(args.output, 'w') as f:
        json.dump(results, f, indent=2)

    total_tables = len(results['tables'])
    total_errors = len(results['errors'])

    traffic = results.get('diagnostics', {}).get('traffic', {})
    mb = traffic.get('mb_downloaded', 0)
    pages = traffic.get('pages_downloaded', 0)
    logger.info(
        f"Scraper complete: {total_tables} tables, {total_errors} errors, "
        f"{pages} pages, {mb:.1f} MB downloaded"
    )
    print(json.dumps(results, indent=2))

    # Exit with error if no data was collected
    if total_tables == 0:
        if total_errors > 0:
            logger.error(
                f"Scraper finished with no data and {total_errors} errors. "
                f"Errors: {results['errors']}"
            )
            return 1

        # For modes that MUST produce data, 0 tables is always a failure
        mode = results.get('mode', args.mode if hasattr(args, 'mode') else '')
        match_data_type = results.get('match_data_type', '')
        critical_modes = {'combined_match_data', 'combined_season_stats', 'schedule'}
        if mode in critical_modes or match_data_type == 'schedule':
            logger.error(
                f"Scraper finished with no data and no errors for mode='{mode}' "
                f"(match_data_type='{match_data_type}'). "
                f"This indicates a silent failure (e.g. Trino unavailable, schedule missing)."
            )
            return 1

        # Non-critical modes: no data with no errors may be expected
        logger.warning(
            f"Scraper finished with no data but no errors. "
            f"This may be expected for some stat types or leagues."
        )
        return 0

    return 0


if __name__ == '__main__':
    sys.exit(main())
