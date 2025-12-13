"""
League discovery utilities for FBref.com

This module provides functionality for automatically discovering all football leagues
from FBref's competitions page, extracting metadata, and caching results.

Features:
- Automatic league discovery from /en/comps/
- Filtering by tier (1st, 2nd, 3rd) and gender (M/W)
- Latest season URL determination
- JSON/CSV caching with configurable expiration (default: 7 days)
"""

import os
import json
import time
import logging
import re
import pandas as pd
from typing import List, Dict, Any, Optional
from bs4 import BeautifulSoup

from ..core.scraper import FBrefScraper
from ..constants import (
    FBREF_COMPETITIONS_URL,
    LEAGUE_TIER_HEADERS,
    METADATA_CACHE_DIR,
    METADATA_CACHE_FILE,
    METADATA_CACHE_MAX_AGE_HOURS
)

# Configure logging
logger = logging.getLogger(__name__)


def extract_all_leagues(tiers: List[str] = None, gender: str = 'M') -> List[Dict[str, Any]]:
    """
    Extract all leagues from FBref /en/comps/ page with tier and gender filtering

    Args:
        tiers: List of tier levels to include (['1st', '2nd', '3rd'])
                Default: all tiers
        gender: Gender filter - 'M' for men's leagues, 'W' for women's leagues
                Default: 'M'

    Returns:
        List of league metadata dictionaries:
        [{
            'league_id': '9',
            'name': 'Premier League',
            'country': 'England',
            'tier': '1st',
            'gender': 'M',
            'base_url': 'https://fbref.com/en/comps/9/Premier-League-Stats'
        }, ...]

    Raises:
        Exception: If unable to fetch or parse the competitions page
    """
    if tiers is None:
        tiers = ['1st', '2nd', '3rd']

    print(f"\n{'='*80}")
    print(f"🔍 ОБНАРУЖЕНИЕ ЛИГ")
    print(f"{'='*80}")
    print(f"Источник: {FBREF_COMPETITIONS_URL}")
    print(f"Фильтр по уровню: {', '.join(tiers)}")
    print(f"Фильтр по полу: {'Мужские' if gender == 'M' else 'Женские'}")

    try:
        # Fetch competitions page with rate limiting
        scraper = FBrefScraper()
        response = scraper.fetch_page(FBREF_COMPETITIONS_URL)
        soup = BeautifulSoup(response.content, 'html.parser')

        all_leagues = []

        # Process each tier
        for tier, header_text in LEAGUE_TIER_HEADERS.items():
            if tier not in tiers:
                continue

            print(f"\n{'─'*80}")
            print(f"📁 Обработка: {header_text}")
            print(f"{'─'*80}")

            # Find section by header text
            section_header = soup.find(['h2', 'h3'], string=lambda s: s and header_text in s)

            if not section_header:
                print(f"⚠️  Секция не найдена: {header_text}")
                continue

            # Find the table following this header
            table = section_header.find_next('table')

            if not table:
                print(f"⚠️  Таблица не найдена после секции: {header_text}")
                continue

            tbody = table.find('tbody')
            if not tbody:
                print(f"⚠️  Tbody не найден в таблице")
                continue

            # Extract league links from table
            tier_leagues = []
            for row in tbody.find_all('tr'):
                cells = row.find_all(['td', 'th'])
                if len(cells) < 2:
                    continue

                # Find league link (usually in first or second column)
                league_link = None
                country = None

                for cell in cells[:3]:
                    link = cell.find('a', href=lambda href: href and '/comps/' in href)
                    if link:
                        league_link = link
                        # Try to extract country from adjacent cells
                        if cell.find_previous_sibling():
                            country_cell = cell.find_previous_sibling(['td', 'th'])
                            if country_cell:
                                country = country_cell.get_text(strip=True)
                        break

                if not league_link or not league_link.get('href'):
                    continue

                league_name = league_link.get_text(strip=True)
                league_url = league_link['href']

                # Apply gender filter
                # Men's leagues typically have "(M)" or no marker
                # Women's leagues have "(W)" marker
                if gender == 'M':
                    # Skip if explicitly marked as women's
                    if '(W)' in league_name or 'Women' in league_name:
                        continue
                elif gender == 'W':
                    # Skip if not marked as women's
                    if not ('(W)' in league_name or 'Women' in league_name):
                        continue

                # Extract league ID from URL
                # URL format: /en/comps/9/Premier-League-Stats
                # IMPORTANT: Filter out invalid URLs like /en/comps/season/2026
                league_id = None
                if '/comps/' in league_url:
                    parts = league_url.split('/comps/')[1].split('/')
                    if parts:
                        # League ID must be numeric
                        potential_id = parts[0]
                        if potential_id.isdigit():
                            league_id = potential_id
                        else:
                            # Skip invalid URLs (e.g., /en/comps/season/...)
                            logger.warning(f"Skipping invalid league URL: {league_url}")
                            continue

                # Build full URL
                if not league_url.startswith('http'):
                    full_url = f"https://fbref.com{league_url}"
                else:
                    full_url = league_url

                # Clean league name (remove gender markers)
                clean_name = league_name.replace('(M)', '').replace('(W)', '').strip()

                league_data = {
                    'league_id': league_id,
                    'name': clean_name,
                    'country': country or 'Unknown',
                    'tier': tier,
                    'gender': gender,
                    'base_url': full_url
                }

                tier_leagues.append(league_data)

            print(f"✅ Найдено лиг в этой секции: {len(tier_leagues)}")
            for league in tier_leagues:
                print(f"   - {league['name']} ({league['country']})")

            all_leagues.extend(tier_leagues)

        print(f"\n{'='*80}")
        print(f"✅ ВСЕГО ОБНАРУЖЕНО ЛИГ: {len(all_leagues)}")
        print(f"{'='*80}")

        # Group by tier for summary
        tier_counts = {}
        for league in all_leagues:
            tier = league['tier']
            tier_counts[tier] = tier_counts.get(tier, 0) + 1

        for tier, count in sorted(tier_counts.items()):
            print(f"   {LEAGUE_TIER_HEADERS[tier]}: {count} лиг")

        return all_leagues

    except Exception as e:
        print(f"❌ Ошибка при извлечении списка лиг: {e}")
        logger.error(f"Error extracting leagues: {e}", exc_info=True)
        raise


def get_latest_season_url(league_base_url: str, league_name: str = None) -> str:
    """
    Determine the URL of the latest season for a given league

    Args:
        league_base_url: Base URL of the league (e.g., /en/comps/9/Premier-League-Stats)
        league_name: League name for logging (optional)

    Returns:
        URL of the latest season (e.g., /en/comps/9/2024-2025/Premier-League-Stats)
        If unable to determine, returns the base URL

    Implementation:
    1. Fetch league page
    2. Look for season dropdown or season links
    3. Extract first (most recent) season
    4. Return full URL
    """
    display_name = league_name or league_base_url.split('/')[-1]

    def is_valid_season_url(url: str, league_id: Optional[str]) -> bool:
        """Ensure URL has numeric league_id and a season segment (reject /comps/season/xxxx)."""
        if not url or '/comps/' not in url:
            return False
        parts = url.split('/comps/', 1)[1].split('/')
        if len(parts) < 2:
            return False
        league_part = parts[0]
        if not league_part.isdigit():
            return False
        if league_id and league_part != league_id:
            return False
        if parts[1] == 'season':
            return False
        season_part = '/'.join(parts[1:])
        return bool(re.search(r'20\d{2}', season_part))

    def latest_year(url: str) -> int:
        years = re.findall(r'20\d{2}', url or '')
        return max((int(y) for y in years), default=0)

    # Extract league_id from base URL for validation
    base_parts = league_base_url.split('/comps/')[1].split('/') if '/comps/' in league_base_url else []
    league_id_from_base = base_parts[0] if base_parts and base_parts[0].isdigit() else None

    try:
        scraper = FBrefScraper()
        response = scraper.fetch_page(league_base_url)
        soup = BeautifulSoup(response.content, 'html.parser')

        candidates: List[tuple[int, str, str]] = []  # (year, url, source)

        def add_candidate(raw_url: str, source: str):
            if not raw_url:
                return
            full_url = raw_url if raw_url.startswith('http') else f"https://fbref.com{raw_url}"
            if not is_valid_season_url(full_url, league_id_from_base):
                logger.warning(f"{display_name}: Discarding invalid season URL from {source}: {full_url}")
                return
            candidates.append((latest_year(full_url), full_url, source))

        # Strategy 1: Find season dropdown (most common)
        season_select = soup.find('select', {'id': 'seasons'})
        if season_select:
            options = season_select.find_all('option')
            if options:
                for opt in options:
                    add_candidate(opt.get('value'), 'dropdown')

        # Strategy 2: Look for season links in the page
        # IMPORTANT: Filter out invalid URLs like /en/comps/season/2026
        season_links = soup.find_all('a', href=lambda href: (
            href and '/comps/' in href and
            # league_id must be numeric (first segment after /comps/)
            len(href.split('/comps/')) > 1 and
            len(href.split('/comps/')[1].split('/')) > 0 and
            href.split('/comps/')[1].split('/')[0].isdigit() and
            any(c.isdigit() for c in href.split('/'))
        ))

        if season_links:
            for link in season_links:
                add_candidate(link.get('href', ''), 'season_links')

        # Pick the freshest valid season URL
        if candidates:
            year, url, source = max(candidates, key=lambda x: (x[0], x[1]))
            logger.info(f"{display_name}: Latest season URL ({source}): {url}")
            return url

        # Strategy 3: Check if base URL is already a season-specific URL
        if any(c.isdigit() for c in league_base_url.split('/')):
            logger.info(f"{display_name}: Base URL appears to be season-specific")
            return league_base_url

        # Fallback: Return base URL with validation
        if '/comps/' in league_base_url:
            try:
                parts = league_base_url.split('/comps/')[1].split('/')
                if parts and not parts[0].isdigit():
                    logger.error(f"{display_name}: Base URL has invalid format (league_id not numeric): {league_base_url}")
            except (IndexError, AttributeError):
                pass

        logger.warning(f"{display_name}: Unable to determine latest season, using base URL")
        return league_base_url

    except Exception as e:
        logger.warning(f"{display_name}: Error determining latest season: {e}, using base URL")
        return league_base_url


def cache_league_metadata(leagues: List[Dict], cache_path: str = None) -> None:
    """
    Save league metadata to JSON and CSV cache files

    Args:
        leagues: List of league metadata dictionaries
        cache_path: Path to cache file (default: METADATA_CACHE_FILE from constants)
    """
    if cache_path is None:
        cache_path = METADATA_CACHE_FILE

    # Ensure cache directory exists
    cache_dir = os.path.dirname(cache_path)
    os.makedirs(cache_dir, exist_ok=True)

    # Add cache timestamp
    cache_data = {
        'cached_at': time.time(),
        'cached_at_human': time.strftime('%Y-%m-%d %H:%M:%S'),
        'leagues': leagues
    }

    # Save as JSON
    with open(cache_path, 'w', encoding='utf-8') as f:
        json.dump(cache_data, f, indent=2, ensure_ascii=False)

    print(f"💾 Метаданные сохранены в JSON: {cache_path}")

    # Save as CSV for easy viewing
    csv_path = cache_path.replace('.json', '.csv')
    df = pd.DataFrame(leagues)
    df.to_csv(csv_path, index=False, encoding='utf-8')
    print(f"💾 Метаданные сохранены в CSV: {csv_path}")


def load_cached_metadata(cache_path: str = None, max_age_hours: int = None) -> Optional[List[Dict]]:
    """
    Load cached league metadata if cache is fresh

    Args:
        cache_path: Path to cache file (default: METADATA_CACHE_FILE from constants)
        max_age_hours: Maximum cache age in hours (default: METADATA_CACHE_MAX_AGE_HOURS)

    Returns:
        List of league metadata dictionaries if cache is fresh, None if cache is stale or missing
    """
    if cache_path is None:
        cache_path = METADATA_CACHE_FILE

    if max_age_hours is None:
        max_age_hours = METADATA_CACHE_MAX_AGE_HOURS

    if not os.path.exists(cache_path):
        logger.info(f"Cache file not found: {cache_path}")
        return None

    try:
        with open(cache_path, 'r', encoding='utf-8') as f:
            cache_data = json.load(f)

        cached_at = cache_data.get('cached_at')
        if not cached_at:
            logger.warning("Cache file missing timestamp")
            return None

        # Check cache age
        age_seconds = time.time() - cached_at
        age_hours = age_seconds / 3600

        if age_hours > max_age_hours:
            logger.info(f"Cache is stale ({age_hours:.1f}h old, max {max_age_hours}h). Will refresh.")
            return None

        leagues = cache_data.get('leagues', [])

        def _valid_cached_league(league: Dict) -> bool:
            url = league.get('season_url', '')
            if not url or '/comps/' not in url:
                return False
            parts = url.split('/comps/', 1)[1].split('/')
            if len(parts) < 2 or not parts[0].isdigit():
                return False
            if parts[1] == 'season':
                return False
            return True

        valid_leagues = [l for l in leagues if _valid_cached_league(l)]
        if len(valid_leagues) != len(leagues):
            logger.warning(f"Cached metadata contains {len(leagues) - len(valid_leagues)} записей с некорректными season_url — они будут пропущены и кеш пересоздастся при необходимости.")

        if not valid_leagues:
            print("⚠️  Кеш пуст или содержит только некорректные записи — требуется обновление.")
            return None

        print(f"📦 Загружено из кеша: {len(valid_leagues)} лиг (возраст: {age_hours:.1f}ч)")
        return valid_leagues

    except Exception as e:
        logger.warning(f"Error loading cache: {e}")
        return None


def discover_all_leagues(use_cache: bool = True, tiers: List[str] = None, gender: str = 'M') -> List[Dict]:
    """
    Main function for league discovery: attempts to load from cache, otherwise parses

    Args:
        use_cache: Whether to use cached metadata if available (default: True)
        tiers: List of tier levels to include (['1st', '2nd', '3rd'])
        gender: Gender filter - 'M' for men, 'W' for women

    Returns:
        List of league metadata dictionaries with season URLs

    Implementation:
    1. Try to load from cache (if use_cache=True)
    2. If cache is missing or stale:
       a. extract_all_leagues()
       b. get_latest_season_url() for each league
       c. cache_league_metadata()
    3. Return list of metadata
    """
    if tiers is None:
        tiers = ['1st', '2nd', '3rd']

    print(f"\n{'#'*80}")
    print(f"🔍 ОБНАРУЖЕНИЕ ВСЕХ ЛИГ FBREF")
    print(f"{'#'*80}")

    # Try to load from cache
    if use_cache:
        print(f"📦 Проверка кеша...")
        cached_leagues = load_cached_metadata()
        if cached_leagues:
            # Filter cached data by requested tiers and gender
            filtered = [
                l for l in cached_leagues
                if l.get('tier') in tiers and l.get('gender') == gender
            ]
            if filtered:
                # If cache is clearly incomplete, force refresh
                if len(filtered) < 50:
                    logger.warning(f"Cached leagues count is too small ({len(filtered)}). Forcing full refresh.")
                else:
                    print(f"✅ Используется кеш: {len(filtered)} лиг")
                    return filtered

    print(f"🌐 Кеш не найден или устарел. Парсинг со страницы...")

    # Extract all leagues
    leagues = extract_all_leagues(tiers=tiers, gender=gender)

    # Get latest season URL for each league
    print(f"\n{'─'*80}")
    print(f"🔗 Определение последних сезонов для {len(leagues)} лиг...")
    print(f"{'─'*80}")

    for i, league in enumerate(leagues, 1):
        print(f"\n[{i}/{len(leagues)}] {league['name']} ({league['country']})")
        season_url = get_latest_season_url(league['base_url'], league['name'])
        league['season_url'] = season_url
        print(f"   ✅ Сезон URL: {season_url}")

    # Cache the results
    print(f"\n{'─'*80}")
    print(f"💾 Кеширование результатов...")
    print(f"{'─'*80}")
    cache_league_metadata(leagues)

    print(f"\n{'#'*80}")
    print(f"✅ ОБНАРУЖЕНИЕ ЗАВЕРШЕНО: {len(leagues)} лиг")
    print(f"{'#'*80}")

    return leagues


# For local testing
if __name__ == "__main__":
    print("🧪 Тестирование модуля league_discovery")
    print("="*80)

    # Test: Discover all 1st tier leagues
    print("\nTEST 1: Обнаружение лиг 1st tier (только мужские)")
    print("="*80)

    try:
        leagues = discover_all_leagues(
            use_cache=False,  # Force fresh fetch for testing
            tiers=['1st'],
            gender='M'
        )

        print(f"\n✅ Успешно обнаружено {len(leagues)} лиг 1st tier")
        print("\nПример (первые 5 лиг):")
        for league in leagues[:5]:
            print(f"\n{league['name']} ({league['country']})")
            print(f"  ID: {league['league_id']}")
            print(f"  Tier: {league['tier']}")
            print(f"  Gender: {league['gender']}")
            print(f"  Season URL: {league['season_url']}")

    except Exception as e:
        print(f"\n❌ Ошибка при тестировании: {e}")
        import traceback
        traceback.print_exc()
