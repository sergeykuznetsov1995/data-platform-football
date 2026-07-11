#!/usr/bin/env python3
"""Historical SofaScore backfill through the production capture engine only."""

from __future__ import annotations

import argparse
from pathlib import Path

from dags.scripts.run_sofascore_scraper import main as run_capture


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--league', default='ENG-Premier League')
    parser.add_argument('--seasons', required=True, help='Comma-separated canonical/start years')
    parser.add_argument('--output-dir', default='/tmp/sofascore-backfill')
    parser.add_argument('--offline-replay', action='store_true')
    parser.add_argument('--force-replace', action='store_true')
    parser.add_argument('--raw-store-uri')
    args = parser.parse_args(argv)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    for token in (value.strip() for value in args.seasons.split(',')):
        if not token:
            continue
        season = token
        safe_season = ''.join(
            character if character.isalnum() else '-'
            for character in season
        ).strip('-')
        if not safe_season:
            parser.error(f'invalid season token: {token!r}')
        common = [
            '--league', args.league,
            '--season', season,
            '--allow-inactive-season',
        ]
        if args.offline_replay:
            common.append('--offline-replay')
        if args.force_replace:
            common.append('--force-replace')
        if args.raw_store_uri:
            common.extend(['--raw-store-uri', args.raw_store_uri])

        # Historical backfill first materializes the full paginated schedule
        # through the same raw/manifest engine. Match capture then resolves only
        # finished event IDs from that Bronze partition; it never falls back to
        # a standalone schedule scraper.
        for entity, filename in (
            ('all', f'{safe_season}-season.json'),
            ('match_capture', f'{safe_season}-matches.json'),
        ):
            result = run_capture([
                '--entity', entity,
                *common,
                '--output', str(output_dir / filename),
            ])
            if result != 0:
                return int(result)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
