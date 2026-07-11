"""
Integration tests for football data scrapers.

These tests make real HTTP requests to external sources and require:
- Network connectivity
- For some sources: Tor proxy running on port 9050
- For WhoScored: direct-first service coverage lives in focused unit tests

Run with: pytest tests/integration/scrapers/ -v -m integration
"""
