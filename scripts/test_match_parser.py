#!/usr/bin/env python3
"""E2E test for the raw-first FBref match parser path.

Online mode reuses one standalone ``FBrefFetcher`` clearance lease, commits
each exact response to Raw v2, and only then parses the committed bytes.
``--offline`` reads the latest committed target and never constructs a
transport. Optional Iceberg persistence uses ``FBrefTypedBronzeAdapter`` and
its fail-closed completion-marker ordering; no legacy scraper private APIs are
called.

Usage (inside Docker container):
    python /opt/airflow/scripts/test_match_parser.py \
        --match-id 643d26fd \
        --proxy-file /opt/airflow/proxys.txt

    # Pure replay from previously committed Raw v2:
    python /opt/airflow/scripts/test_match_parser.py \
        --match-id 643d26fd \
        --raw-store-uri file:///tmp/fbref-match-parser-raw \
        --offline

    # Multiple matches:
    python /opt/airflow/scripts/test_match_parser.py \
        --match-id 643d26fd e8724659 b0005978 \
        --proxy-file /opt/airflow/proxys.txt

    # Save raw HTML for debugging:
    python /opt/airflow/scripts/test_match_parser.py \
        --match-id 643d26fd \
        --proxy-file /opt/airflow/proxys.txt \
        --save-html

    # Write parsed data to Iceberg:
    python /opt/airflow/scripts/test_match_parser.py \
        --match-id 643d26fd \
        --proxy-file /opt/airflow/proxys.txt \
        --source-competition-id 9 \
        --source-season-id 2025-2026 \
        --save-to-iceberg
"""

import argparse
import logging
import os
import sys
import uuid
from pathlib import Path
from typing import Optional

import pandas as pd
from bs4 import BeautifulSoup

# Keep the tool runnable both from the repository and from /opt/airflow.
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from scrapers.fbref.constants import BASE_URL  # noqa: E402
from scrapers.fbref.parsers.table_parser import (  # noqa: E402
    extract_tables_from_comments,
)
from scrapers.fbref.parsers.finders import (  # noqa: E402
    parse_events_from_scorebox,
    parse_lineup_table,
    parse_shots_table,
    parse_team_match_stats_table,
    parse_player_match_stats_tables,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
)
logger = logging.getLogger('test_match_parser')
MAX_MATCHES_PER_RUN = 3

# ──────────────────────────────────────────────────────────────────
# Colours for terminal output
# ──────────────────────────────────────────────────────────────────
GREEN = '\033[92m'
RED = '\033[91m'
YELLOW = '\033[93m'
CYAN = '\033[96m'
BOLD = '\033[1m'
RESET = '\033[0m'


def PASS(msg: str) -> str:
    return f"  {GREEN}PASS{RESET}  {msg}"


def FAIL(msg: str) -> str:
    return f"  {RED}FAIL{RESET}  {msg}"


def WARN(msg: str) -> str:
    return f"  {YELLOW}WARN{RESET}  {msg}"


def INFO(msg: str) -> str:
    return f"  {CYAN}INFO{RESET}  {msg}"


# ──────────────────────────────────────────────────────────────────
# Fetch and commit exact raw bytes before parsing
# ──────────────────────────────────────────────────────────────────

def fetch_and_commit_match(
    fetcher,
    raw_store,
    match_id: str,
    *,
    run_id: str,
) -> bytes:
    """Fetch one match, commit its exact response, then reload parser input."""
    from scrapers.fbref.fetcher import FETCHER_VERSION
    from scrapers.fbref.raw_store import match_page_target

    target = match_page_target(match_id)
    logger.info("Fetching: %s", target.canonical_url)
    response = fetcher.fetch(target.canonical_url, page_kind="match")
    logical_refresh_id = f"{run_id}:match:{match_id}"
    raw_store.commit_fetch(
        target,
        response.body,
        logical_refresh_id=logical_refresh_id,
        http_status=response.status_code,
        headers=response.headers,
        wire_bytes=response.http_wire_bytes,
        provider_billed_bytes=response.provider_billed_bytes,
        latency_ms=response.latency_ms,
        http_requests=response.http_requests,
        http_status_history=response.http_status_history,
        browser_bootstrap_attempts=response.browser_bootstrap_attempts,
        browser_unobserved_bytes=response.browser_unobserved_bytes,
        fetcher_version=FETCHER_VERSION,
        transport_version=FETCHER_VERSION,
    )
    committed, _ = raw_store.load_fetch(logical_refresh_id)
    return committed


def load_committed_match(raw_store, match_id: str) -> bytes:
    """Load the latest Raw v2 content for an offline parser replay."""
    from scrapers.fbref.raw_store import match_page_target

    committed, _ = raw_store.load_latest_response(
        match_page_target(match_id)
    )
    return committed


# ──────────────────────────────────────────────────────────────────
# Validation checks
# ──────────────────────────────────────────────────────────────────

def validate_events(df: pd.DataFrame) -> list:
    """Validate match events DataFrame (BUG 4+5 checks)."""
    results = []
    if df is None or df.empty:
        results.append(FAIL("Events DataFrame is None or empty"))
        return results

    results.append(INFO(f"Events shape: {df.shape}"))

    # Check: minute is not empty
    empty_minutes = df['minute'].apply(lambda x: str(x).strip() == '').sum()
    total = len(df)
    if empty_minutes == 0:
        results.append(PASS(f"All {total} events have non-empty minute"))
    else:
        results.append(FAIL(f"{empty_minutes}/{total} events have empty minute"))

    # Check: no empty player strings (BUG 5 — ghost rows)
    empty_players = df['player'].apply(lambda x: str(x).strip() == '').sum()
    if empty_players == 0:
        results.append(PASS(f"No empty player strings ({total} events)"))
    else:
        results.append(FAIL(f"{empty_players}/{total} events have empty player"))

    # Check: team_side is filled (home/away)
    if 'team_side' in df.columns:
        valid_sides = df['team_side'].isin(['home', 'away']).sum()
        if valid_sides == total:
            results.append(PASS(f"All {total} events have team_side (home/away)"))
        else:
            results.append(FAIL(f"Only {valid_sides}/{total} events have valid team_side"))
    else:
        results.append(FAIL("Column 'team_side' missing from events"))

    # Check: team is not empty
    empty_teams = df['team'].apply(lambda x: str(x).strip() == '').sum()
    if empty_teams == 0:
        results.append(PASS(f"All {total} events have non-empty team name"))
    else:
        results.append(FAIL(f"{empty_teams}/{total} events have empty team"))

    # Check: secondary_player present in schema
    if 'secondary_player' in df.columns:
        results.append(PASS("Column 'secondary_player' exists"))
        # For goals, secondary_player should be the assister
        goals = df[df['event_type'].isin(['goal', 'penalty'])]
        if len(goals) > 0:
            assists = goals['secondary_player'].apply(lambda x: str(x).strip() != '').sum()
            results.append(INFO(f"Goals with assists (secondary_player): {assists}/{len(goals)}"))
    else:
        results.append(FAIL("Column 'secondary_player' missing"))

    if 'secondary_player_id' in df.columns:
        results.append(PASS("Column 'secondary_player_id' exists"))
    else:
        results.append(FAIL("Column 'secondary_player_id' missing"))

    # Check: description column removed
    if 'description' not in df.columns:
        results.append(PASS("Old column 'description' removed"))
    else:
        results.append(WARN("Old column 'description' still present"))

    # Check: event_type values
    etypes = df['event_type'].value_counts()
    results.append(INFO(f"Event types: {dict(etypes)}"))

    # Check: player_id extraction
    if 'player_id' in df.columns:
        has_id = df['player_id'].notna().sum()
        results.append(INFO(f"Player IDs: {has_id}/{total} events"))
    else:
        results.append(WARN("Column 'player_id' not in events"))

    return results


def validate_lineups(df: pd.DataFrame) -> list:
    """Validate lineups DataFrame (BUG 7 checks)."""
    results = []
    if df is None or df.empty:
        results.append(FAIL("Lineups DataFrame is None or empty"))
        return results

    results.append(INFO(f"Lineups shape: {df.shape}"))

    total = len(df)
    starters = df[df['is_starter'].eq(True)]
    bench = df[df['is_starter'].eq(False)]

    results.append(INFO(f"Starters: {len(starters)}, Bench: {len(bench)}"))

    # Check: position is filled for starters
    starter_positions = starters['position'].apply(lambda x: str(x).strip() != '').sum()
    if starter_positions == len(starters):
        results.append(PASS(f"All {len(starters)} starters have position"))
    elif starter_positions > 0:
        results.append(WARN(
            f"{starter_positions}/{len(starters)} starters have position "
            f"(positions come from stats_summary tables)"
        ))
    else:
        results.append(FAIL(f"No starters have position ({len(starters)} starters)"))

    # Check: position values are valid
    valid_positions = {'GK', 'DF', 'MF', 'FW'}
    if len(starters) > 0:
        actual_positions = set(starters['position'].dropna().unique())
        unexpected = actual_positions - valid_positions - {''}
        if unexpected:
            results.append(WARN(f"Unexpected positions: {unexpected}"))
        else:
            results.append(PASS(f"Positions are valid: {actual_positions & valid_positions}"))

    # Check: team does NOT contain formation (BUG 7)
    import re
    formation_pattern = re.compile(r'\(\d+-\d+')
    teams_with_formation = df['team'].apply(lambda x: bool(formation_pattern.search(str(x)))).sum()
    if teams_with_formation == 0:
        results.append(PASS("No team names contain formation (e.g., '(4-3-3)')"))
    else:
        results.append(FAIL(f"{teams_with_formation} rows have formation in team name"))
        # Show examples
        bad = df[df['team'].apply(lambda x: bool(formation_pattern.search(str(x))))]
        results.append(INFO(f"  Examples: {bad['team'].unique()[:3]}"))

    # Check: is_starter correctness (should be ~11 per team)
    for team_name in df['team'].unique():
        team_starters = df[
            (df['team'] == team_name) & df['is_starter'].eq(True)
        ]
        if len(team_starters) == 11:
            results.append(PASS(f"{team_name}: 11 starters"))
        else:
            results.append(WARN(f"{team_name}: {len(team_starters)} starters (expected 11)"))

    # Check: player_id extraction
    if 'player_id' in df.columns:
        has_id = df['player_id'].notna().sum()
        if has_id == total:
            results.append(PASS(f"All {total} players have player_id"))
        else:
            results.append(WARN(f"{has_id}/{total} players have player_id"))
    else:
        results.append(FAIL("Column 'player_id' missing"))

    # Check: number (jersey)
    if 'number' in df.columns:
        has_number = df['number'].apply(lambda x: str(x).strip() != '').sum()
        results.append(INFO(f"Players with jersey number: {has_number}/{total}"))
    else:
        results.append(WARN("Column 'number' not in lineups"))

    return results


def validate_team_stats(df: pd.DataFrame) -> list:
    """Validate team match stats DataFrame (new div-based parser)."""
    results = []
    if df is None or df.empty:
        results.append(FAIL("Team match stats DataFrame is None or empty"))
        return results

    results.append(INFO(f"Team stats shape: {df.shape}"))
    results.append(INFO(f"Columns: {list(df.columns)}"))

    # Check team names
    if 'home_team' in df.columns and df.iloc[0]['home_team']:
        results.append(PASS(f"home_team: {df.iloc[0]['home_team']}"))
    else:
        results.append(FAIL("home_team missing or empty"))

    if 'away_team' in df.columns and df.iloc[0]['away_team']:
        results.append(PASS(f"away_team: {df.iloc[0]['away_team']}"))
    else:
        results.append(FAIL("away_team missing or empty"))

    # Check key stat columns
    key_cols = [
        'home_possession', 'away_possession',
        'home_shots', 'away_shots',
        'home_sot', 'away_sot',
        'home_saves', 'away_saves',
    ]
    for col in key_cols:
        if col in df.columns:
            val = df.iloc[0][col]
            results.append(PASS(f"{col}: {val}"))
        else:
            results.append(WARN(f"Column '{col}' missing"))

    # Check extra stats
    extra_cols = ['home_fouls', 'home_corners', 'home_crosses',
                  'home_interceptions', 'home_offsides']
    found = [c for c in extra_cols if c in df.columns]
    if found:
        results.append(PASS(f"Extra stats available: {len(found)} columns"))
    else:
        results.append(WARN("No extra stats (div#team_stats_extra not found?)"))

    return results


def validate_player_match_stats(df: pd.DataFrame) -> list:
    """Validate player match stats DataFrame."""
    results = []
    if df is None or df.empty:
        results.append(FAIL("Player match stats DataFrame is None or empty"))
        return results

    results.append(INFO(f"Player match stats shape: {df.shape}"))

    # Check team_side column
    if 'team_side' in df.columns:
        sides = df['team_side'].unique().tolist()
        if 'home' in sides and 'away' in sides:
            results.append(PASS(f"team_side has both home/away: {sides}"))
        else:
            results.append(FAIL(f"team_side incomplete: {sides}"))
    else:
        results.append(FAIL("Column 'team_side' missing"))

    # Check team column
    if 'team' in df.columns:
        teams = df['team'].unique().tolist()
        results.append(PASS(f"Teams: {teams}"))
    else:
        results.append(FAIL("Column 'team' missing"))

    # Check no total rows leaked through
    if 'Player' in df.columns:
        import re
        total_re = re.compile(r'^\d+\s+Players?$', re.IGNORECASE)
        totals = df[df['Player'].astype(str).str.match(total_re, na=False)]
        if len(totals) == 0:
            results.append(PASS("No total/summary rows in data"))
        else:
            results.append(FAIL(f"{len(totals)} total rows leaked through"))

    # Check reasonable row count (should be ~22-30 players per match)
    if len(df) >= 20:
        results.append(PASS(f"Row count reasonable: {len(df)} players"))
    else:
        results.append(WARN(f"Low row count: {len(df)} (expected ~22-30)"))

    return results


def validate_shots(df: pd.DataFrame) -> list:
    """Validate shots DataFrame (BUG 6 / FBref data restrictions)."""
    results = []
    if df is None:
        results.append(WARN(
            "Shots table is None — FBref may restrict shot data (expected Feb 2026+)"
        ))
        return results

    if df.empty:
        results.append(WARN("Shots table found but empty — FBref data restrictions"))
        return results

    results.append(INFO(f"Shots shape: {df.shape}"))
    results.append(PASS(f"Shot data available: {len(df)} shots"))
    results.append(INFO(f"Columns: {list(df.columns)}"))

    return results


# ──────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────

def process_match(match_id: str, html: str, save_html: bool = False) -> dict:
    """Parse a match and return validation results."""
    print(f"\n{'='*70}")
    print(f"{BOLD}Match: {match_id}{RESET}")
    print(f"URL: {BASE_URL}/en/matches/{match_id}")
    print(f"{'='*70}")

    # Save raw HTML if requested
    if save_html:
        html_path = f"/tmp/fbref_match_{match_id}.html"
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(html)
        print(INFO(f"Raw HTML saved to {html_path} ({len(html)} bytes)"))

    # Parse HTML
    logger.info(f"Parsing HTML ({len(html)} bytes)...")
    soup = BeautifulSoup(html, 'html.parser')
    comment_tables = extract_tables_from_comments(soup)

    # Extract page title
    title = soup.title.string if soup.title else 'No title'
    print(INFO(f"Page title: {title}"))
    print(INFO(f"Comment tables: {len(comment_tables)} ({list(comment_tables.keys())[:10]})"))

    report = {}

    # ── Events (BUG 4+5) ──
    print(f"\n{BOLD}--- EVENTS (BUG 4+5) ---{RESET}")
    events_df = parse_events_from_scorebox(soup)
    if events_df is not None:
        print(f"\n{events_df.to_string()}\n")
    report['events'] = validate_events(events_df)
    for line in report['events']:
        print(line)

    # ── Lineups (BUG 7) ──
    print(f"\n{BOLD}--- LINEUPS (BUG 7) ---{RESET}")
    lineup_df = parse_lineup_table(soup, comment_tables=comment_tables)
    if lineup_df is not None:
        pd.set_option('display.max_rows', 60)
        pd.set_option('display.max_columns', 10)
        pd.set_option('display.width', 140)
        print(f"\n{lineup_df.to_string()}\n")
    report['lineups'] = validate_lineups(lineup_df)
    for line in report['lineups']:
        print(line)

    # ── Shots (BUG 6) ──
    print(f"\n{BOLD}--- SHOTS (BUG 6) ---{RESET}")
    shots_df = parse_shots_table(soup, comment_tables)
    if shots_df is not None and not shots_df.empty:
        print(f"\n{shots_df.head(10).to_string()}\n")
    report['shots'] = validate_shots(shots_df)
    for line in report['shots']:
        print(line)

    # ── Team Match Stats ──
    print(f"\n{BOLD}--- TEAM MATCH STATS ---{RESET}")
    team_stats_df = parse_team_match_stats_table(soup, comment_tables)
    if team_stats_df is not None and not team_stats_df.empty:
        print(f"\n{team_stats_df.to_string()}\n")
    report['team_stats'] = validate_team_stats(team_stats_df)
    for line in report['team_stats']:
        print(line)

    # ── Player Match Stats ──
    print(f"\n{BOLD}--- PLAYER MATCH STATS ---{RESET}")
    player_match_df = parse_player_match_stats_tables(soup, comment_tables)
    if player_match_df is not None and not player_match_df.empty:
        print(f"\n{player_match_df.head(10).to_string()}\n")
    report['player_match_stats'] = validate_player_match_stats(player_match_df)
    for line in report['player_match_stats']:
        print(line)

    # ── Schema check ──
    print(f"\n{BOLD}--- SCHEMA ---{RESET}")
    schema_results = []
    if events_df is not None:
        cols = set(events_df.columns)
        for expected in ['team_side', 'secondary_player', 'secondary_player_id']:
            if expected in cols:
                schema_results.append(PASS(f"Events has column '{expected}'"))
            else:
                schema_results.append(FAIL(f"Events missing column '{expected}'"))
        if 'description' in cols:
            schema_results.append(WARN("Events still has old 'description' column"))
        else:
            schema_results.append(PASS("Events does not have old 'description' column"))
    else:
        schema_results.append(FAIL("Cannot check schema — events_df is None"))
    report['schema'] = schema_results
    for line in report['schema']:
        print(line)

    # Cleanup
    soup.decompose()

    return {
        'events_df': events_df,
        'lineup_df': lineup_df,
        'shots_df': shots_df,
        'team_stats_df': team_stats_df,
        'player_match_df': player_match_df,
        'report': report,
    }


def print_summary(all_reports: dict):
    """Print final PASS/FAIL summary."""
    print(f"\n{'='*70}")
    print(f"{BOLD}SUMMARY{RESET}")
    print(f"{'='*70}")

    total_pass = 0
    total_fail = 0
    total_warn = 0

    for match_id, data in all_reports.items():
        report = data['report']
        for section_name, checks in report.items():
            for check in checks:
                if 'PASS' in check:
                    total_pass += 1
                elif 'FAIL' in check:
                    total_fail += 1
                elif 'WARN' in check:
                    total_warn += 1

    print(f"\n  {GREEN}PASS: {total_pass}{RESET}")
    print(f"  {RED}FAIL: {total_fail}{RESET}")
    print(f"  {YELLOW}WARN: {total_warn}{RESET}")

    if total_fail == 0:
        print(f"\n  {GREEN}{BOLD}ALL CHECKS PASSED{RESET}")
    else:
        print(f"\n  {RED}{BOLD}{total_fail} CHECKS FAILED — review output above{RESET}")

    return total_fail


def save_to_iceberg(
    match_ids: list[str],
    raw_pages: dict[str, bytes],
    *,
    source_competition_id: str,
    source_season_id: str,
    competition_name: Optional[str],
    compatibility_season: Optional[int],
    run_id: str,
    adapter=None,
) -> dict[str, dict[str, int]]:
    """Persist committed bytes through the production typed adapter.

    The adapter reparses the stored response and writes every available typed
    match dataset with ``match_player_stats`` last as the completion marker.
    Any parser or persistence failure propagates, so this utility cannot report
    a successful partial write.
    """
    from scrapers.fbref.typed_bronze import (
        FBrefTypedBronzeAdapter,
        TypedSourceContext,
    )

    typed_adapter = adapter or FBrefTypedBronzeAdapter()
    context = TypedSourceContext(
        source_competition_id=source_competition_id,
        source_season_id=source_season_id,
        competition_name=competition_name,
        compatibility_season=compatibility_season,
    )
    persisted: dict[str, dict[str, int]] = {}
    for match_id in match_ids:
        body = raw_pages[match_id]
        try:
            _, counts = typed_adapter.ingest_match_html(
                body,
                match_id=match_id,
                context=context,
                run_id=run_id,
                target_identity=f"manual-match:{match_id}",
            )
        except Exception as exc:
            print(FAIL(f"Typed Bronze write failed for {match_id}: {exc}"))
            raise
        persisted[match_id] = counts
        rendered = ", ".join(
            f"{dataset}={count}" for dataset, count in counts.items()
        )
        print(PASS(f"{match_id}: typed Bronze committed ({rendered})"))
    return persisted


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Raw-first E2E test for FBref match parsers"
    )
    parser.add_argument(
        "--match-id",
        nargs="+",
        required=True,
        help="FBref match ID(s) (8-char hex, e.g. 643d26fd)",
    )
    parser.add_argument(
        "--proxy-file",
        default="/opt/airflow/proxys.txt",
        help="Proxy file for online mode; pass an empty value for no proxy",
    )
    parser.add_argument(
        "--raw-store-uri",
        default=os.environ.get(
            "FBREF_MATCH_PARSER_RAW_URI",
            "file:///tmp/fbref-match-parser-raw",
        ),
        help="Raw v2 store used before parsing",
    )
    parser.add_argument(
        "--offline",
        action="store_true",
        help="Load latest committed Raw v2 pages; never construct a fetcher",
    )
    parser.add_argument(
        "--run-id",
        help="Stable logical run id; default is a fresh manual UUID",
    )
    parser.add_argument(
        "--source-competition-id",
        help="Source-native competition id for typed compatibility columns",
    )
    parser.add_argument(
        "--source-season-id",
        help="Source-native season id for typed compatibility columns",
    )
    parser.add_argument("--competition-name")
    parser.add_argument("--compatibility-season", type=int)
    parser.add_argument(
        "--save-html",
        action="store_true",
        help="Also save decoded committed HTML to /tmp for debugging",
    )
    parser.add_argument(
        "--save-to-iceberg",
        action="store_true",
        help="Persist committed bytes through FBrefTypedBronzeAdapter",
    )
    return parser


def main() -> None:
    from scrapers.fbref.raw_store import RawPageStore

    parser = _build_parser()
    args = parser.parse_args()
    if len(args.match_id) > MAX_MATCHES_PER_RUN:
        parser.error(
            f"at most {MAX_MATCHES_PER_RUN} match ids are allowed per run"
        )
    if args.save_to_iceberg and (
        not args.source_competition_id or not args.source_season_id
    ):
        parser.error(
            "--save-to-iceberg requires --source-competition-id and "
            "--source-season-id"
        )
    run_id = args.run_id or f"manual-match-parser-{uuid.uuid4().hex}"
    raw_store = RawPageStore.from_uri(args.raw_store_uri)

    print(f"\n{BOLD}FBref Match Parser E2E Test{RESET}")
    print(f"Match IDs: {args.match_id}")
    print(f"Raw store: {args.raw_store_uri}")
    print(f"Offline: {args.offline}")
    print(f"Run ID: {run_id}")
    print(f"Save HTML: {args.save_html}")
    print(f"Save to Iceberg: {args.save_to_iceberg}")

    raw_pages: dict[str, bytes] = {}
    if args.offline:
        for match_id in args.match_id:
            raw_pages[match_id] = load_committed_match(raw_store, match_id)
    else:
        from scrapers.fbref.fetcher import FBrefFetcher

        with FBrefFetcher(proxy_file=args.proxy_file or None) as fetcher:
            for match_id in args.match_id:
                raw_pages[match_id] = fetch_and_commit_match(
                    fetcher,
                    raw_store,
                    match_id,
                    run_id=run_id,
                )

    all_reports = {}
    for match_id, body in raw_pages.items():
        html = body.decode("utf-8")
        all_reports[match_id] = process_match(
            match_id,
            html,
            save_html=args.save_html,
        )

    if args.save_to_iceberg:
        print(f"\n{BOLD}--- SAVING TO TYPED BRONZE ---{RESET}")
        save_to_iceberg(
            args.match_id,
            raw_pages,
            source_competition_id=args.source_competition_id,
            source_season_id=args.source_season_id,
            competition_name=args.competition_name,
            compatibility_season=args.compatibility_season,
            run_id=run_id,
        )

    fail_count = print_summary(all_reports)
    raise SystemExit(1 if fail_count > 0 else 0)


if __name__ == '__main__':
    main()
