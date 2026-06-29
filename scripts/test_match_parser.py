#!/usr/bin/env python3
"""
E2E Test: Match Parser Verification
====================================

Ad-hoc script to verify BUG 4-7 fixes on real FBref match pages.
Fetches 1-3 match pages via nodriver, runs all parsers, and prints
a structured PASS/FAIL report.

Usage (inside Docker container):
    python /opt/airflow/scripts/test_match_parser.py \
        --match-id 643d26fd \
        --proxy-file /opt/airflow/proxys.txt

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
        --save-to-iceberg
"""

import argparse
import logging
import sys
import os

import pandas as pd
from bs4 import BeautifulSoup

# Ensure scrapers are importable
sys.path.insert(0, '/opt/airflow/scrapers')
sys.path.insert(0, '/opt/airflow/dags')
sys.path.insert(0, '/opt/airflow')

from scrapers.fbref.constants import BASE_URL
from scrapers.fbref.parsers.table_parser import extract_tables_from_comments
from scrapers.fbref.parsers.finders import (
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
# Fetch page via FBrefScraper (nodriver)
# ──────────────────────────────────────────────────────────────────

def fetch_match_html(match_id: str, proxy_file: str) -> str:
    """Fetch match page HTML using FBrefScraper's nodriver stack."""
    from scrapers.fbref.scraper import FBrefScraper

    url = f"{BASE_URL}/en/matches/{match_id}"
    logger.info(f"Fetching: {url}")

    scraper = FBrefScraper(
        leagues=['ENG-Premier League'],
        seasons=[2025],
        headless=True,
        use_xvfb=True,
        use_nodriver=True,
        proxy_file=proxy_file,
    )

    try:
        html = scraper._fetch_page(url, use_cache=False, page_type='match')
        if not html:
            logger.error(f"Failed to fetch match {match_id}")
            sys.exit(1)
        return html
    finally:
        scraper.close()


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
    starters = df[df['is_starter'] == True]
    bench = df[df['is_starter'] == False]

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
        team_starters = df[(df['team'] == team_name) & (df['is_starter'] == True)]
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


def main():
    parser = argparse.ArgumentParser(description='E2E test for FBref match parsers')
    parser.add_argument(
        '--match-id', nargs='+', required=True,
        help='FBref match ID(s) (8-char hex, e.g. 643d26fd)',
    )
    parser.add_argument(
        '--proxy-file', default='/opt/airflow/proxys.txt',
        help='Path to proxy file',
    )
    parser.add_argument(
        '--save-html', action='store_true',
        help='Save raw HTML to /tmp/ for debugging',
    )
    parser.add_argument(
        '--save-to-iceberg', action='store_true',
        help='Write parsed data to Iceberg tables',
    )
    args = parser.parse_args()

    print(f"\n{BOLD}FBref Match Parser E2E Test{RESET}")
    print(f"Match IDs: {args.match_id}")
    print(f"Proxy file: {args.proxy_file}")
    print(f"Save HTML: {args.save_html}")
    print(f"Save to Iceberg: {args.save_to_iceberg}")

    all_reports = {}

    for match_id in args.match_id:
        # Fetch page via nodriver
        html = fetch_match_html(match_id, args.proxy_file)

        # Parse and validate
        result = process_match(match_id, html, save_html=args.save_html)
        all_reports[match_id] = result

    # Optionally save to Iceberg
    if args.save_to_iceberg:
        print(f"\n{BOLD}--- SAVING TO ICEBERG ---{RESET}")
        save_to_iceberg(args.match_id, all_reports)

    # Summary
    fail_count = print_summary(all_reports)
    sys.exit(1 if fail_count > 0 else 0)


def save_to_iceberg(match_ids: list, all_reports: dict):
    """Write parsed DataFrames to Iceberg via FBrefScraper._write_to_iceberg."""
    from scrapers.fbref.scraper import FBrefScraper

    scraper = FBrefScraper(
        leagues=['ENG-Premier League'],
        seasons=[2025],
        headless=True,
        use_xvfb=False,
        use_nodriver=False,
    )

    league = 'ENG-Premier League'
    season = 2025

    try:
        for match_id in match_ids:
            data = all_reports.get(match_id, {})

            # Events
            events_df = data.get('events_df')
            if events_df is not None and not events_df.empty:
                events_df = events_df.copy()
                events_df['match_id'] = match_id
                events_df['league'] = league
                events_df['season'] = season
                try:
                    table = scraper.save_to_iceberg(events_df, 'fbref_match_events')
                    print(PASS(f"Events written to {table} ({len(events_df)} rows)"))
                except Exception as e:
                    print(FAIL(f"Events write failed: {e}"))

            # Lineups
            lineup_df = data.get('lineup_df')
            if lineup_df is not None and not lineup_df.empty:
                lineup_df = lineup_df.copy()
                lineup_df['match_id'] = match_id
                lineup_df['league'] = league
                lineup_df['season'] = season
                try:
                    table = scraper.save_to_iceberg(lineup_df, 'fbref_lineups')
                    print(PASS(f"Lineups written to {table} ({len(lineup_df)} rows)"))
                except Exception as e:
                    print(FAIL(f"Lineups write failed: {e}"))

            # Shots
            shots_df = data.get('shots_df')
            if shots_df is not None and not shots_df.empty:
                shots_df = shots_df.copy()
                shots_df['match_id'] = match_id
                shots_df['league'] = league
                shots_df['season'] = season
                try:
                    table = scraper.save_to_iceberg(shots_df, 'fbref_shot_events')
                    print(PASS(f"Shots written to {table} ({len(shots_df)} rows)"))
                except Exception as e:
                    print(FAIL(f"Shots write failed: {e}"))

            # Team match stats
            team_stats_df = data.get('team_stats_df')
            if team_stats_df is not None and not team_stats_df.empty:
                team_stats_df = team_stats_df.copy()
                team_stats_df['match_id'] = match_id
                team_stats_df['league'] = league
                team_stats_df['season'] = season
                try:
                    table = scraper.save_to_iceberg(team_stats_df, 'fbref_match_team_stats')
                    print(PASS(f"Team stats written to {table} ({len(team_stats_df)} rows)"))
                except Exception as e:
                    print(FAIL(f"Team stats write failed: {e}"))

            # Player match stats
            player_match_df = data.get('player_match_df')
            if player_match_df is not None and not player_match_df.empty:
                player_match_df = player_match_df.copy()
                player_match_df['match_id'] = match_id
                player_match_df['league'] = league
                player_match_df['season'] = season
                try:
                    table = scraper.save_to_iceberg(player_match_df, 'fbref_match_player_stats')
                    print(PASS(f"Player match stats written to {table} ({len(player_match_df)} rows)"))
                except Exception as e:
                    print(FAIL(f"Player match stats write failed: {e}"))
    finally:
        scraper.close()


if __name__ == '__main__':
    main()
