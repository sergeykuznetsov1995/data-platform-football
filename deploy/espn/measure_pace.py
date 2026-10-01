#!/usr/bin/env python3
"""Host/checkout convenience wrapper; espn-live uses the mounted scrapers module."""
from pathlib import Path
import sys

# Direct script execution puts deploy/espn on sys.path; add this checkout only.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scrapers.espn.measure_pace import main

if __name__ == '__main__':
    raise SystemExit(main())
