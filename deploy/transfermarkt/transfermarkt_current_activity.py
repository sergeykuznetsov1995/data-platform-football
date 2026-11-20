"""Read the current writer's completion evidence for the host stall watch (#2092)."""

import json
import os
from datetime import datetime, timedelta
from pathlib import Path


def _time(value):
    stamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if stamp.utcoffset() is None:
        raise ValueError('current timestamp lacks a timezone')
    return stamp


def has_recent_completion(directory: Path, now: datetime) -> bool | None:
    """Return True for any proof in (now - 24h, now], False for verified silence.

    An unreadable source leaves the result unknown unless another source proves
    progress. A failed portion may still have completed independent editions.
    """
    since = now - timedelta(hours=24)
    incomplete = False
    try:
        index = json.loads((directory / 'cursor-v1.json').read_bytes())
        for scope_id, entry in index['scopes'].items():
            try:
                cursor = entry['cursor']
                if cursor is None:  # A grant can exist before the first roster.
                    continue
                if cursor['scope_id'] != scope_id:
                    raise ValueError('cursor identity differs from its index key')
                stamp = cursor.get('roster_completed_at')
                if stamp is not None and since < _time(stamp) <= now:
                    return True
            except (AttributeError, KeyError, TypeError, ValueError):
                incomplete = True
    except (OSError, ValueError, TypeError, KeyError, AttributeError):
        incomplete = True

    # The writer atomically publishes portion-*.json after setting finished_at.
    # Stat only excludes old publications. Touching a file cannot prove progress.
    try:
        with os.scandir(directory) as files:
            for entry in files:
                if not (entry.name.startswith('portion-') and entry.name.endswith('.json')):
                    continue
                try:
                    if entry.stat().st_mtime < since.timestamp():
                        continue
                    report = json.loads(Path(entry.path).read_bytes())
                    finished = _time(report['finished_at'])
                    if not since < finished <= now:
                        continue
                    completed = report['daily_complete_scope_ids']
                    if not isinstance(completed, list) or not all(isinstance(s, str) and s for s in completed):
                        raise ValueError('invalid daily_complete_scope_ids')
                    if completed:
                        return True
                except (OSError, ValueError, TypeError, KeyError, AttributeError):
                    incomplete = True
    except OSError:
        incomplete = True
    return None if incomplete else False
