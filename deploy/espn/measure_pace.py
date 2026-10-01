#!/usr/bin/env python3
"""Host/checkout convenience wrapper; espn-live uses the mounted scrapers module."""
from scrapers.espn.measure_pace import main

if __name__ == '__main__':
    raise SystemExit(main())
