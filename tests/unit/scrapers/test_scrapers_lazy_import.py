from __future__ import annotations

import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]


def test_direct_sofascore_catalog_import_does_not_require_pandas_or_browser():
    script = """
import sys
sys.modules['pandas'] = None
sys.modules['selenium'] = None
import scrapers
import scrapers.sofascore.catalog
assert 'scrapers.base.base_scraper' not in sys.modules
assert 'scrapers.fbref' not in sys.modules
"""
    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr


def test_package_does_not_advertise_removed_fbref_scraper_apis():
    import scrapers

    assert "FBrefScraper" not in scrapers.__all__
    assert "NodriverFBrefScraper" not in scrapers.__all__
