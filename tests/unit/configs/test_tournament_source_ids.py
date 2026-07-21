"""
Coherence guard: source-ID maps vs competitions.yaml (#920 Phase 3).

This test IS the onboarding checklist, executable. Source IDs live in each
source-owned registry (SofaScore uses configs/sofascore/tournaments.json) or
per-scraper map, plus configs/soccerdata/league_dict.json for soccerdata-based
sources. For every competition with configured seasons, every source it
declares must resolve; a new tournament added to competitions.yaml with
missing IDs fails here with a message naming exactly which source registry
lacks which key.

Also pins soccerdata 1.8.8 merge semantics: custom league_dict entries
REPLACE built-ins per key, so any fragment entry shadowing a built-in must
be a superset of it (adding ESPN must not drop FBref).
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import pytest
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[3]
for p in (str(PROJECT_ROOT),):
    if p not in sys.path:
        sys.path.insert(0, p)

COMPETITIONS = yaml.safe_load(
    (PROJECT_ROOT / 'configs' / 'medallion' / 'competitions.yaml')
    .read_text(encoding='utf-8')
)['competitions']
TEAMS = yaml.safe_load(
    (PROJECT_ROOT / 'configs' / 'medallion' / 'team_aliases.yaml')
    .read_text(encoding='utf-8')
)['teams']
LEAGUE_DICT_FRAGMENT = json.loads(
    (PROJECT_ROOT / 'configs' / 'soccerdata' / 'league_dict.json')
    .read_text(encoding='utf-8')
)

# Frozen copy of soccerdata 1.8.8's built-in INT-* entries (prod pins 1.8.8;
# the host venv has 1.9.0 — the test must encode the PINNED version's data,
# introspected in-container during T0 recon, not whatever is installed here).
SD_188_BUILTIN_INT = {
    'INT-World Cup': {
        'FBref': 'FIFA World Cup',
        'FotMob': 'INT-World Cup',
        'WhoScored': 'International - FIFA World Cup',
        'season_code': 'single-year',
    },
    'INT-European Championship': {
        'FBref': 'UEFA European Football Championship',
        'FotMob': 'INT-EURO',
        'Sofascore': 'EURO',
        'WhoScored': 'International - European Championship',
        'season_start': 'Jun',
        'season_end': 'Jul',
        'season_code': 'single-year',
    },
    "INT-Women's World Cup": {
        'FBref': "FIFA Women's World Cup",
        'WhoScored': "International - FIFA Women's World Cup",
        'season_code': 'single-year',
    },
}

# Effective soccerdata view after the ensure_league_dict install:
# {**builtin, **custom} with per-key FULL REPLACE (soccerdata _config.py).
EFFECTIVE_LEAGUE_DICT = {**SD_188_BUILTIN_INT, **LEAGUE_DICT_FRAGMENT}

# Club leagues resolve via soccerdata built-ins outside the INT-* namespace
# (not frozen here) — the per-source resolvers below only enforce what the
# repo actually owns. clubelo/matchhistory/understat resolve леагуе внутри
# своих скраперов и не участвуют в турнирном онбординге.


def _competitions_with_seasons():
    return [c for c in COMPETITIONS if c.get('seasons')]


def _resolve(source: str, comp_id: str):
    """Return a problem string or None if the source resolves for comp_id."""
    if source == 'fbref':
        from scrapers.fbref.constants import LEAGUE_IDS
        entry = LEAGUE_IDS.get(comp_id)
        if not entry or not entry.get('comp_id') or not entry.get('slug'):
            return "scrapers/fbref/constants.py LEAGUE_IDS: missing/incomplete entry"
        if not entry['comp_id'].isdigit():
            return f"fbref comp_id must be digits (got {entry['comp_id']!r})"
    elif source == 'fotmob':
        from scrapers.fotmob.constants import LEAGUE_IDS
        val = LEAGUE_IDS.get(comp_id)
        if not val or not str(val).isdigit():
            return "configs/fotmob/competitions.json: missing/non-numeric entry"
    elif source == 'sofascore':
        from scrapers.sofascore.scraper import (
            SOFASCORE_TOURNAMENT_MAP,
            SOFASCORE_TOURNAMENT_SLUG,
        )
        if not isinstance(SOFASCORE_TOURNAMENT_MAP.get(comp_id), int):
            return ("configs/sofascore/tournaments.json: missing/non-int "
                    "unique_tournament_id")
        slug = SOFASCORE_TOURNAMENT_SLUG.get(comp_id)
        if not slug or slug.count('/') < 2:
            return ("configs/sofascore/tournaments.json: "
                    "missing/malformed page_path")
    elif source == 'espn':
        entry = EFFECTIVE_LEAGUE_DICT.get(comp_id)
        if comp_id.startswith('INT-'):
            if not entry or not entry.get('ESPN'):
                return ("configs/soccerdata/league_dict.json: missing entry "
                        "or 'ESPN' key")
    elif source == 'whoscored':
        entry = EFFECTIVE_LEAGUE_DICT.get(comp_id)
        if comp_id.startswith('INT-'):
            if not entry or not entry.get('WhoScored'):
                return ("configs/soccerdata/league_dict.json: missing entry "
                        "or 'WhoScored' key")
            if not re.match(r'^[A-Z][A-Za-z ]* - .+', entry['WhoScored']):
                return (f"WhoScored value must be 'Region - Tournament' "
                        f"(got {entry['WhoScored']!r})")
    elif source == 'understat':
        from scrapers.understat.scraper import UnderstatScraper
        if comp_id not in UnderstatScraper.SUPPORTED_LEAGUES:
            return "understat SUPPORTED_LEAGUES: league not supported"
    elif source in ('clubelo', 'matchhistory'):
        # Club-only sources with their own internal league maps; never part
        # of tournament onboarding — declared in sources for documentation.
        return None
    else:
        return f"unknown source {source!r} — extend the resolver table"
    return None


@pytest.mark.unit
def test_every_declared_source_resolves_an_id():
    problems = []
    for comp in _competitions_with_seasons():
        cid = comp['id']
        for source in comp.get('sources', {}).get('primary') or []:
            problem = _resolve(source, cid)
            if problem:
                problems.append(f"{cid}: source '{source}' -> {problem}")
    assert not problems, (
        "Onboarding incomplete — fill these maps:\n" + "\n".join(problems)
    )


@pytest.mark.unit
def test_sofascore_ids_are_not_duplicated_in_legacy_league_metadata():
    """The discovery registry is the sole source for SofaScore ids."""
    document = yaml.safe_load(
        (PROJECT_ROOT / 'scrapers' / 'sources' / 'leagues.yaml')
        .read_text(encoding='utf-8')
    )
    duplicates = [
        league
        for league, metadata in (document.get('metadata') or {}).items()
        if 'sofascore_id' in (metadata or {})
    ]
    assert duplicates == []


@pytest.mark.unit
def test_single_year_iff_soccerdata_single_year_code():
    # season_format 'single_year' (yaml, underscore) must round-trip to
    # soccerdata's season_code 'single-year' (hyphen) for espn/whoscored.
    problems = []
    for comp in _competitions_with_seasons():
        cid = comp['id']
        uses_sd = {'espn', 'whoscored'} & set(
            comp.get('sources', {}).get('primary') or [])
        if not uses_sd or not cid.startswith('INT-'):
            continue
        yaml_single = any(
            s.get('season_format') == 'single_year' for s in comp['seasons'])
        sd_code = (EFFECTIVE_LEAGUE_DICT.get(cid) or {}).get('season_code')
        if yaml_single != (sd_code == 'single-year'):
            problems.append(
                f"{cid}: season_format single_year={yaml_single} but "
                f"league_dict season_code={sd_code!r}")
    assert not problems, "\n".join(problems)


@pytest.mark.unit
def test_fragment_entries_are_supersets_of_builtins():
    # Per-key FULL REPLACE: shadowing a built-in while dropping one of its
    # keys silently deletes that source's name for the league.
    problems = []
    for cid, entry in LEAGUE_DICT_FRAGMENT.items():
        builtin = SD_188_BUILTIN_INT.get(cid)
        if not builtin:
            continue
        for key, val in builtin.items():
            if entry.get(key) != val:
                problems.append(
                    f"{cid}: fragment lost/changed builtin key {key!r} "
                    f"(builtin {val!r}, fragment {entry.get(key)!r})")
    assert not problems, "\n".join(problems)


@pytest.mark.unit
def test_active_single_year_season_requires_dates():
    # A dateless single_year season is legal ONLY while inert (Copa América
    # 2028: host/dates TBA). Once in_scope flips, dates are mandatory —
    # otherwise get_active_season silently never opens the window.
    problems = []
    for comp in _competitions_with_seasons():
        if not comp.get('in_scope'):
            continue
        for s in comp['seasons']:
            if s.get('season_format') != 'single_year':
                continue
            if not s.get('start') or not s.get('end'):
                problems.append(
                    f"{comp['id']} season {s.get('id')}: in_scope single_year "
                    f"season without start/end")
    assert not problems, "\n".join(problems)


@pytest.mark.unit
def test_international_competitions_have_team_alias_coverage():
    # Forgetting team_aliases entirely is the silent-orphan class in xref.
    problems = []
    for comp in _competitions_with_seasons():
        cid = comp['id']
        if not comp.get('is_international'):
            continue
        covered = sum(
            1 for t in TEAMS
            if cid in (t.get('competition_scope') or [])
        )
        if covered < 5:
            problems.append(
                f"{cid}: only {covered} national teams carry it in "
                f"competition_scope (expected >= 5)")
    assert not problems, "\n".join(problems)


@pytest.mark.unit
def test_tournament_ids_are_int_prefixed_ascii():
    # config.py filters sofifa floors by l.startswith('INT-'); the id is a
    # Bronze partition value and an Airflow task_id fragment (ASCII only).
    for comp in COMPETITIONS:
        if comp.get('is_international'):
            assert comp['id'].startswith('INT-'), comp['id']
            assert comp['id'].isascii(), comp['id']
