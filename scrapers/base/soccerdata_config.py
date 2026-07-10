"""
Zero-manual-step install of the repo's soccerdata league_dict fragment
(#920 Phase 3).

ESPN/WhoScored resolve league -> source id through soccerdata's
``LEAGUE_DICT``, which merges ``~/soccerdata/config/league_dict.json`` at
import time. The built-in dict has NO ESPN key for INT-World Cup /
INT-European Championship and no AFCON / Copa América entries at all — the
World Cup ESPN ingest only worked in prod through a HAND-PATCHED,
unversioned file inside the ``soccerdata_cache`` docker volume. This module
makes the repo fragment (``configs/soccerdata/league_dict.json``) the
versioned source of those entries and installs it before soccerdata is
imported, in every environment (container, dev host, unit tests).

Merge semantics: the repo fragment is authoritative for ITS keys; foreign
keys in an existing file are preserved — the prod VM's hand-patched club
entries and the documented Understat RUS-Premier League extension path
(utils/config.py) survive the install. NOTE soccerdata itself does a
per-key FULL REPLACE over its built-ins, so every fragment entry must be
complete (restating the built-in FBref/FotMob/WhoScored names, not just
adding ESPN).
"""

import json
import logging
import os
import sys
import tempfile
from pathlib import Path
from typing import Iterable, Optional

logger = logging.getLogger(__name__)

# scrapers/base/soccerdata_config.py -> repo root (= /opt/airflow in the
# container, where compose mounts ./configs/soccerdata read-only).
FRAGMENT_PATH = (
    Path(__file__).resolve().parents[2] / 'configs' / 'soccerdata'
    / 'league_dict.json'
)


def soccerdata_config_dir() -> Path:
    """Mirror soccerdata._config's CONFIG_DIR resolution exactly."""
    base = Path(os.environ.get('SOCCERDATA_DIR', Path.home() / 'soccerdata'))
    return base / 'config'


def ensure_league_dict(required_leagues: Optional[Iterable[str]] = None) -> None:
    """Merge the repo league_dict fragment into soccerdata's config file.

    Called from SoccerdataScraper.__init__, i.e. BEFORE the lazy
    ``import soccerdata`` in every reader path (each scrape is a fresh
    subprocess, and all soccerdata imports in scrapers/ are function-local).

    - Idempotent: identical content -> no write (no mtime churn).
    - Atomic: temp file + os.replace, so concurrent runners never expose a
      partially-written file to a concurrent reader.
    - Corrupt target: renamed aside to ``league_dict.json.corrupt`` and
      rebuilt — loud warning, never a crash.
    - Missing fragment: logged as an error and skipped — club scrapers are
      unaffected; a tournament league then fails loudly in soccerdata's own
      league resolution instead of scraping the wrong thing.
    - ``required_leagues``: if soccerdata is ALREADY imported in this
      process (import-cache — the merged file can no longer take effect)
      and one of these leagues is in the fragment but absent from the live
      ``LEAGUE_DICT``, raise instead of letting the reader silently not
      know the league. Never fires in the standard one-scrape-per-process
      flow; it exists to make the impossible-to-fix-in-process case loud.
    """
    try:
        fragment = json.loads(FRAGMENT_PATH.read_text(encoding='utf-8'))
    except FileNotFoundError:
        logger.error(
            "soccerdata league_dict fragment missing: %s — tournament "
            "leagues will not resolve for ESPN/WhoScored.", FRAGMENT_PATH,
        )
        return
    except json.JSONDecodeError as e:
        logger.error(
            "soccerdata league_dict fragment unreadable (%s): %s — skipping "
            "install.", FRAGMENT_PATH, e,
        )
        return

    cfg_dir = soccerdata_config_dir()
    target = cfg_dir / 'league_dict.json'
    existing = {}
    if target.is_file():
        try:
            existing = json.loads(target.read_text(encoding='utf-8'))
            if not isinstance(existing, dict):
                raise ValueError(f"top-level {type(existing).__name__}, expected object")
        except (json.JSONDecodeError, ValueError) as e:
            backup = target.with_suffix('.json.corrupt')
            logger.warning(
                "existing %s is corrupt (%s) — moving aside to %s and "
                "rebuilding from the repo fragment.", target, e, backup,
            )
            os.replace(target, backup)
            existing = {}

    merged = {**existing, **fragment}
    if merged != existing:
        cfg_dir.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=str(cfg_dir), suffix='.tmp')
        try:
            with os.fdopen(fd, 'w', encoding='utf-8') as fh:
                json.dump(merged, fh, ensure_ascii=False, indent=1, sort_keys=True)
            os.replace(tmp, target)
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise
        logger.info(
            "soccerdata league_dict installed: %d repo entries merged into "
            "%s (%d total).", len(fragment), target, len(merged),
        )

    if required_leagues and 'soccerdata' in sys.modules:
        live = getattr(
            getattr(sys.modules['soccerdata'], '_config', None),
            'LEAGUE_DICT', None,
        )
        if isinstance(live, dict):
            stale = [
                lg for lg in required_leagues
                if lg in fragment and lg not in live
            ]
            if stale:
                raise RuntimeError(
                    f"soccerdata was imported before the league_dict install "
                    f"— leagues {stale} are not resolvable in this process; "
                    f"restart it (fresh runner subprocess) to pick up "
                    f"{target}."
                )
