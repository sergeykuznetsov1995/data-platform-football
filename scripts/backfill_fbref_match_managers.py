#!/usr/bin/env python3
"""Bounded offline remediation for ``bronze.fbref_match_managers``.

Only immutable match HTML from the required source control run is parsed.
There is no scraper, browser, URL construction, concurrency, or network path.
Each invocation processes at most 25 pages; successful completion manifests
make subsequent invocations advance through the source run incrementally.

Usage (inside the Airflow image):

    python /opt/airflow/scripts/backfill_fbref_match_managers.py \
        --source-control-run-id 12345678-1234-5678-1234-567812345678 \
        --max-pages 25
"""

from __future__ import annotations

import importlib.util
import logging
import sys
from pathlib import Path
from typing import Any, Optional, Sequence


_HELPER_MODULE_NAME = "_fbref_offline_match_remediation"
_HELPER_PATH = Path(__file__).with_name("rescrape_match_player_stats.py")
_HELPER_SPEC = importlib.util.spec_from_file_location(
    _HELPER_MODULE_NAME, _HELPER_PATH
)
if _HELPER_SPEC is None or _HELPER_SPEC.loader is None:  # pragma: no cover
    raise ImportError(f"Cannot load offline remediation helper: {_HELPER_PATH}")
_HELPER_MODULE = importlib.util.module_from_spec(_HELPER_SPEC)
sys.modules[_HELPER_MODULE_NAME] = _HELPER_MODULE
_HELPER_SPEC.loader.exec_module(_HELPER_MODULE)
remediation_cli = _HELPER_MODULE.remediation_cli


MANAGERS_DATASET = "match_managers"
MANAGERS_PARSER_VERSION = "fbref-remediation-match-managers-v1"


def main(
    argv: Optional[Sequence[str]] = None,
    *,
    control: Optional[Any] = None,
    raw_store: Optional[Any] = None,
    adapter: Optional[Any] = None,
) -> int:
    return remediation_cli(
        argv,
        description=__doc__ or "Offline FBref match manager remediation",
        target_dataset=MANAGERS_DATASET,
        parser_version=MANAGERS_PARSER_VERSION,
        control=control,
        raw_store=raw_store,
        adapter=adapter,
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    raise SystemExit(main())
