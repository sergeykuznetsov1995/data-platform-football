"""Shared helpers for ingest DAG files.

Consolidates byte-identical private helpers that had drifted into copies
across dag_ingest_capology / dag_ingest_sofascore / dag_ingest_transfermarkt /
dag_ingest_whoscored — a fix applied to one copy (e.g. encoding handling)
would silently miss the others.
"""

from typing import Any, Dict


def load_result(path: str, logger) -> Dict[str, Any]:
    """Load a runner JSON output. Missing file → empty dict (treated as failure)."""
    import json
    try:
        with open(path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error("Results file %s not found", path)
        return {}
    except json.JSONDecodeError as e:
        logger.error("Invalid JSON in %s: %s", path, e)
        return {}


def league_slug(league: str) -> str:
    """``'ENG-Premier League'`` → ``'eng_premier_league'`` (task-id / path safe)."""
    return league.lower().replace(' ', '_').replace('-', '_')
