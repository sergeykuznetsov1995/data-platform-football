"""Probe live FotMob API to diagnose 21 drift columns from 2026-05-14 audit.

Run inside airflow-webserver:
    docker compose exec -T airflow-webserver \
        python /opt/airflow/scripts/probe_fotmob_drift.py \
        > /tmp/fotmob_probe/verdicts.md 2>&1

Output:
  - /tmp/fotmob_probe/e{1..5}_*.json  -- raw payload dumps
  - stdout                            -- markdown decision table

No side effects: only GET requests, no Iceberg writes.
"""
from __future__ import annotations

import json
import re
import sys
import traceback
from pathlib import Path
from typing import Any

sys.path.insert(0, '/opt/airflow')

from scrapers.fotmob.scraper import FotMobScraper

DUMP_DIR = Path('/tmp/fotmob_probe')
DUMP_DIR.mkdir(parents=True, exist_ok=True)

LEAGUE = 'ENG-Premier League'
SEASON = 2025

# (col_id, endpoint_label, expected_path, notes)
# expected_path syntax: dotted; `[0]` indexes list; `<table_type>` is wildcard for
# tables.{all,home,away,form,xg}. `[]` means "iterate any element".
DRIFT_SPEC: list[tuple[str, str, str, str]] = [
    # ---------------- E1: /api/data/leagues ----------------
    ('team_stats.form',                 'E1', 'table[0].data.table.<tt>[].form',        'list-or-str joined; scraper writes via team.get("form")'),
    ('team_stats.qualification',        'E1', 'table[0].data.table.<tt>[].qualColor.name', '(team.qualColor or {}).name'),
    # ---------------- E2: data.fotmob.com/stats/... (TopLists) ----------------
    # Player-side leaderboards
    ('player_stats.player_id',          'E2p', '<legacy-mirror — scraper writes participant_id, not player_id>', 'dead-mirror: _flatten_top_list never sets player_id key'),
    ('player_stats.player_name',        'E2p', '<legacy-mirror — scraper writes participant_name>',              'dead-mirror'),
    ('player_stats.stat_category',      'E2p', '<legacy-mirror — scraper writes stat_category_header/group/name>','dead-mirror'),
    # Team-side leaderboards
    ('team_leaderboards.participant_id','E2t', 'TopLists[].StatList[].ParticipantId',    'team-context — key likely absent for teams'),
    ('team_leaderboards.positions',     'E2t', 'TopLists[].StatList[].Positions',        'team-context — positions field for players only'),
    ('team_leaderboards.team_name',     'E2t', 'TopLists[].StatList[].TeamName',         'team-context — likely no TeamName key (the team IS the row)'),
    # ---------------- E3: /api/data/transfers ----------------
    ('transfers.market_value',          'E3', 'transfers[].marketValue.value',           '(tr.marketValue or {}).value'),
    ('transfers.transfer_type',         'E3', 'transfers[].transferType',                'STRUCT row(localizationKey, text) in Bronze'),
    # ---------------- E4: /api/data/teams?id={tid} ----------------
    ('team_profile.country_code',           'E4', 'details.countryCode',                    ''),
    ('team_profile.founded',                'E4', 'details.founded',                        ''),
    ('team_profile.is_national',            'E4', 'details.isNational',                     ''),
    ('team_profile.sport_category',         'E4', 'details.sportCategory',                  ''),
    ('team_profile.venue',                  'E4', 'details.venue.name',                     '(details.venue or {}).name; fallback details.venue if str'),
    ('team_profile.website',                'E4', 'details.website',                        ''),
    ('team_profile.overview_table_position','E4', 'overview.overviewTablePosition',         'fallback overview.leaguePosition'),
    ('team_profile.history_seasons_count',  'E4', 'history.historicalTable[]',              'len(history.historicalTable)'),
    ('team_squad.injury_text',              'E4', 'squad.squad[].members[].injury.text',    'injury is dict; .text key'),
    ('team_squad.position_label',           'E4', 'squad.squad[].members[].role',           'Bronze schema is STRUCT row(fallback, key) — scraper writes plain str'),
    # ---------------- E5: /_next/data/<bid>/players/<id>.json ----------------
    ('player_details.international_duty_json','E5', 'pageProps.data.internationalDuty',     'wrapped in json.dumps(... or {}) → if None becomes literal {}'),
]


# ---------------------------- helpers ----------------------------

def pluck(obj: Any, path: str) -> tuple[str, Any]:
    """Walk a dotted path on a JSON object. Returns (verdict, value).

    Verdicts:
      - 'present'        value is not None / not empty
      - 'null_at_path'   path exists but value is None / empty list/dict / ""
      - 'path_missing'   key/index missing somewhere along the path
      - 'wrong_type'     a step expected dict but got list/scalar (or vice versa)
    """
    if path.startswith('<'):
        return ('legacy_mirror', None)

    cur: Any = obj
    parts = re.split(r'\.', path)
    for p in parts:
        # handle list indexing/iteration
        list_iter = False
        idx = None
        m = re.match(r'^(\w+)\[(\d*)\]$', p)
        if m:
            key, idx_str = m.group(1), m.group(2)
            if key:
                if not isinstance(cur, dict):
                    return ('wrong_type', f'expected dict at .{key}, got {type(cur).__name__}')
                if key not in cur:
                    return ('path_missing', f'key {key!r} missing')
                cur = cur[key]
            if not isinstance(cur, list):
                return ('wrong_type', f'expected list at .{key}[], got {type(cur).__name__}')
            if idx_str == '':
                list_iter = True
            else:
                idx = int(idx_str)
                if idx >= len(cur):
                    return ('path_missing', f'index {idx} out of range (len={len(cur)})')
                cur = cur[idx]
        elif p == '<tt>':
            if not isinstance(cur, dict):
                return ('wrong_type', f'expected dict at <tt>, got {type(cur).__name__}')
            # try 'all' first
            if 'all' not in cur:
                return ('path_missing', f'table type "all" missing; keys={list(cur.keys())[:5]}')
            cur = cur['all']
        else:
            if not isinstance(cur, dict):
                return ('wrong_type', f'expected dict at .{p}, got {type(cur).__name__}')
            if p not in cur:
                return ('path_missing', f'key {p!r} missing; keys={list(cur.keys())[:8]}')
            cur = cur[p]

        if list_iter:
            # iterate-and-collect remaining path
            rest = parts[parts.index(p) + 1:]
            if not rest:
                # leaf is list itself
                return ('present' if cur else 'null_at_path', f'<list of {len(cur)}>')
            # apply rest to first 3 items, collect verdicts
            sub_results = []
            for item in cur[:3]:
                sub_v, sub_val = pluck(item, '.'.join(rest))
                sub_results.append((sub_v, sub_val))
            # consolidate verdict
            if all(v == 'present' for v, _ in sub_results):
                return ('present', f'[{sub_results[0][1]!r}, ...] ({len(cur)} items)')
            if all(v == 'path_missing' for v, _ in sub_results):
                return ('path_missing', f'{sub_results[0][1]} in all {len(cur)} items')
            if any(v == 'present' for v, _ in sub_results):
                # mixed
                present_count = sum(1 for v, _ in sub_results if v == 'present')
                return ('mixed', f'{present_count}/{len(sub_results)} sample items had value; full list len={len(cur)}')
            return (sub_results[0][0], f'{sub_results[0][1]} (sample)')

    # leaf reached
    if cur is None:
        return ('null_at_path', 'None')
    if cur == '' or cur == [] or cur == {}:
        return ('null_at_path', f'empty {type(cur).__name__}')
    if isinstance(cur, (list, dict)):
        return ('present', f'<{type(cur).__name__} len={len(cur)}>')
    return ('present', repr(cur)[:80])


def dump_payload(name: str, payload: Any) -> None:
    path = DUMP_DIR / f'{name}.json'
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str))
    size_kb = path.stat().st_size / 1024
    print(f'    dumped {path} ({size_kb:.1f} KB)', file=sys.stderr)


# ---------------------------- probe ----------------------------

def main() -> None:
    print('# FotMob drift probe — live API verdicts', file=sys.stderr)
    print(f'## League={LEAGUE} season={SEASON}', file=sys.stderr)
    print('---', file=sys.stderr)

    s = FotMobScraper(leagues=[LEAGUE], seasons=[SEASON])

    # ---- E1: /api/data/leagues ----
    print('### E1 /api/data/leagues', file=sys.stderr)
    e1_payload = None
    try:
        e1_payload = s._get_league_data(LEAGUE, SEASON)
        if e1_payload is None:
            print('  ERROR: _get_league_data returned None', file=sys.stderr)
        else:
            dump_payload('e1_leagues', e1_payload)
    except Exception as e:
        print(f'  EXCEPTION: {e}\n{traceback.format_exc()}', file=sys.stderr)

    # ---- E2p: player-side TopLists ----
    print('### E2p /stats/47/season/<sid>/<cat>.json (player)', file=sys.stderr)
    e2p_payload = None
    e2p_url = None
    try:
        if e1_payload:
            cats = (e1_payload.get('stats') or {}).get('players') or []
            if cats:
                e2p_url = cats[0].get('fetchAllUrl')
                if e2p_url:
                    print(f'  fetching player cat[0]: {e2p_url}', file=sys.stderr)
                    e2p_payload = s._fetch_api_json(e2p_url)
                    if e2p_payload:
                        dump_payload('e2_player_leaderboard', e2p_payload)
    except Exception as e:
        print(f'  EXCEPTION: {e}', file=sys.stderr)

    # ---- E2t: team-side TopLists ----
    print('### E2t /stats/47/season/<sid>/<cat>.json (team)', file=sys.stderr)
    e2t_payload = None
    try:
        if e1_payload:
            cats = (e1_payload.get('stats') or {}).get('teams') or []
            if cats:
                e2t_url = cats[0].get('fetchAllUrl')
                if e2t_url:
                    print(f'  fetching team cat[0]: {e2t_url}', file=sys.stderr)
                    e2t_payload = s._fetch_api_json(e2t_url)
                    if e2t_payload:
                        dump_payload('e2_team_leaderboard', e2t_payload)
    except Exception as e:
        print(f'  EXCEPTION: {e}', file=sys.stderr)

    # ---- E3: /api/data/transfers?id=47 ----
    print('### E3 /api/data/transfers', file=sys.stderr)
    e3_payload = None
    try:
        e3_payload = s._fetch_api_json('transfers', params={'id': '47'})
        if e3_payload:
            dump_payload('e3_transfers', e3_payload)
    except Exception as e:
        print(f'  EXCEPTION: {e}', file=sys.stderr)

    # ---- E4: /api/data/teams?id={first_team_id} ----
    print('### E4 /api/data/teams', file=sys.stderr)
    e4_payload = None
    sample_team_id = None
    try:
        team_ids = s._team_ids_for_league(LEAGUE, SEASON)
        if team_ids:
            sample_team_id = team_ids[0]
            print(f'  fetching team_id={sample_team_id}', file=sys.stderr)
            e4_payload = s._get_team_data(sample_team_id)
            if e4_payload:
                dump_payload('e4_team', e4_payload)
    except Exception as e:
        print(f'  EXCEPTION: {e}', file=sys.stderr)

    # ---- E5: /_next/data/<bid>/players/<id>.json ----
    print('### E5 /_next/data/<bid>/players/<pid>.json', file=sys.stderr)
    e5_payload = None
    sample_player_id = None
    try:
        if e4_payload:
            squad_sections = (e4_payload.get('squad') or {}).get('squad') or []
            for sec in squad_sections:
                if isinstance(sec, dict):
                    for m in sec.get('members') or []:
                        if isinstance(m, dict) and m.get('id'):
                            sample_player_id = str(m['id'])
                            break
                if sample_player_id:
                    break
        if sample_player_id:
            print(f'  fetching player_id={sample_player_id}', file=sys.stderr)
            e5_payload = s._fetch_next_data_payload(f'/players/{sample_player_id}')
            if e5_payload:
                dump_payload('e5_player', e5_payload)
    except Exception as e:
        print(f'  EXCEPTION: {e}\n{traceback.format_exc()}', file=sys.stderr)

    # ---- evaluate verdicts ----
    print('### evaluating', file=sys.stderr)
    endpoints = {
        'E1':  e1_payload,
        'E2p': e2p_payload,
        'E2t': e2t_payload,
        'E3':  e3_payload,
        'E4':  e4_payload,
        'E5':  e5_payload,
    }

    # For E2 leaderboards, walk into TopLists[0].StatList[0]
    # For E4 squad, walk into squad.squad[0].members[0]
    # For E5, walk into pageProps.data
    # `pluck` handles paths starting at root of payload.
    # For paths like "TopLists[].StatList[].ParticipantId", we walk from payload root.

    # Print markdown table to stdout
    print('# FotMob Bronze drift — Phase 1 probe results')
    print(f'\n**League**: {LEAGUE}  **Season**: {SEASON}/{SEASON+1}')
    if sample_team_id:
        print(f'**Sample team_id**: {sample_team_id}')
    if sample_player_id:
        print(f'**Sample player_id**: {sample_player_id}')
    print()
    print('| # | Column | Endpoint | Expected path | Verdict | Value at path | Notes |')
    print('|---|---|---|---|---|---|---|')
    for i, (col, ep, path, notes) in enumerate(DRIFT_SPEC, 1):
        payload = endpoints.get(ep)
        if payload is None and not path.startswith('<'):
            verdict, val = 'endpoint_unreachable', f'{ep} payload was None'
        else:
            try:
                verdict, val = pluck(payload, path) if not path.startswith('<') else pluck(None, path)
            except Exception as e:
                verdict, val = 'probe_error', f'{type(e).__name__}: {e}'
        val_str = str(val).replace('|', '\\|')[:100]
        print(f'| {i} | `{col}` | {ep} | `{path}` | **{verdict}** | `{val_str}` | {notes} |')

    print()
    print('## Endpoints summary')
    for ep, p in endpoints.items():
        size = 'None' if p is None else f'{len(json.dumps(p, default=str))/1024:.1f} KB'
        print(f'- **{ep}**: {size}')


if __name__ == '__main__':
    main()
