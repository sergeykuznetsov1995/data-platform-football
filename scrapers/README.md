# Football Data Scrapers

Scrapers for collecting football statistics from various data sources.

## Architecture

### Class Hierarchy

```
BaseScraper                     # Base class with rate limiting, retry, circuit breaker
├── SoccerdataScraper          # Wrapper for soccerdata library
├── SeleniumScraper            # Cloudflare bypass via Selenium/undetected-chromedriver
│   ├── FBrefScraper           # FBref statistics
│   ├── FotMobScraper          # FotMob match data
│   ├── WhoScoredScraper       # WhoScored events (SPADL format)
│   └── MatchHistoryScraper    # Football-data.co.uk
└── [Other scrapers]
    ├── UnderstatScraper       # Understat xG data
    ├── SofaScoreScraper       # SofaScore statistics
    ├── SoFIFAScraper          # FIFA ratings
    ├── ClubEloScraper         # ELO ratings
    └── ESPNScraper            # ESPN schedules
```

### Package Structure

```
scrapers/
├── __init__.py                 # Main exports
├── README.md                   # This file
│
├── base/                       # Base classes and utilities
│   ├── __init__.py
│   ├── base_scraper.py        # BaseScraper, SeleniumScraper
│   ├── iceberg_writer.py      # Apache Iceberg integration
│   ├── flaresolverr_client.py # FlareSolverr client
│   └── browser/               # Browser automation
│       ├── __init__.py
│       ├── cloudflare_bypass.py
│       ├── driver_factory.py
│       ├── proxy_extension.py
│       └── utils.py
│
├── fbref/                      # FBref scraper module
│   ├── __init__.py
│   ├── scraper.py             # FBrefScraper class
│   ├── constants.py           # LEAGUE_IDS, STAT_TYPES
│   ├── url_builder.py         # URL construction
│   └── html_parser.py         # HTML/comment parsing
│
├── whoscored/                  # WhoScored scraper module
│   ├── __init__.py
│   ├── scraper.py             # WhoScoredScraper class
│   ├── constants.py           # LEAGUE_CONFIG, EVENT_TYPE_MAPPING
│   ├── page_navigator.py      # Page navigation
│   └── spadl_converter.py     # SPADL format conversion
│
├── utils/                      # Shared utilities
│   ├── __init__.py
│   ├── rate_limiter.py        # Token bucket rate limiter
│   ├── retry_policy.py        # Retry with backoff
│   ├── circuit_breaker.py     # Circuit breaker pattern
│   └── proxy_manager.py       # Proxy rotation
│
├── schemas/                    # PyArrow schemas
│   └── *.py
│
├── sources/                    # Source configuration
│   ├── sources.yaml           # Data source configs
│   └── leagues.yaml           # League configurations
│
└── *_scraper.py               # Individual scrapers (compatibility modules)
```

## Usage

### Basic Usage

```python
from scrapers import FBrefScraper, WhoScoredScraper

# FBref scraper
scraper = FBrefScraper(
    leagues=['ENG-Premier League'],
    seasons=[2024],
    headless=True
)
result = scraper.scrape_all()

# WhoScored scraper
scraper = WhoScoredScraper(
    leagues=['ENG-Premier League'],
    seasons=[2024],
    headless=False  # Recommended for WhoScored
)
result = scraper.scrape_all()
```

### With Proxy Support

```python
from scrapers import FBrefScraper

scraper = FBrefScraper(
    leagues=['ENG-Premier League'],
    seasons=[2024],
    proxy_file='proxys.txt'  # Format: host:port:user:pass
)
```

### Using FlareSolverr

```python
from scrapers import WhoScoredScraper

scraper = WhoScoredScraper(
    leagues=['ENG-Premier League'],
    seasons=[2024],
    use_flaresolverr=True,
    flaresolverr_url='http://flaresolverr:8191'
)
```

## Data Sources

| Source | Data | Cloudflare | Selenium |
|--------|------|------------|----------|
| FBref | Stats, schedules, players | Yes | Yes |
| WhoScored | Events (SPADL), ratings | Yes | Yes |
| FotMob | Matches, lineups | No | Yes |
| MatchHistory | Results, odds | No | Optional |
| Understat | xG, shots | No | No |
| SofaScore | Stats, ratings | No | No |
| SoFIFA | FIFA attributes | No | No |
| ClubElo | ELO ratings | No | No |
| ESPN | Schedules | No | No |

## Configuration

### sources.yaml

Source-specific configuration including rate limits, retry policies, and circuit breakers:

```yaml
sources:
  fbref:
    rate_limit:
      requests_per_minute: 20
      burst_size: 5
    retry:
      max_attempts: 3
      max_delay: 60
    circuit_breaker:
      fail_max: 5
      reset_timeout: 300
```

### Environment Variables

- `CHROME_BIN` - Path to Chrome/Chromium binary
- `CHROMEDRIVER_PATH` - Path to ChromeDriver

## Testing

```bash
# Unit tests (no network)
pytest tests/unit/scrapers/ -v

# Integration tests (real HTTP requests)
pytest tests/integration/scrapers/ -v -m integration

# Skip slow/Cloudflare tests
pytest tests/unit/scrapers/ -v -m "not slow and not cloudflare"
```

## Output

All scrapers write data to Apache Iceberg tables via `IcebergWriter`:

```
/data/bronze/{source}/{entity}/league={league}/season={season}/
```

Example:
```
/data/bronze/fbref/schedule/league=ENG-Premier League/season=2024/
/data/bronze/whoscored/events_spadl/league=ENG-Premier League/season=2024/
```
