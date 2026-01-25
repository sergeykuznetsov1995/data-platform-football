"""
WhoScored Scraper
=================

Scraper for WhoScored event data with Selenium browser automation.
Converts event data to SPADL (Soccer Player Action Description Language) format.

Source: https://www.whoscored.com

NOTE: WhoScored requires browser automation due to Cloudflare protection.
This scraper should be run with headless=False for best results.
"""

import json
import logging
import re
import time
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from scrapers.base.base_scraper import SeleniumScraper
from scrapers.base.cloudflare_bypass import CloudflareBypass

logger = logging.getLogger(__name__)


class WhoScoredScraper(SeleniumScraper):
    """
    Scraper for WhoScored event data using Selenium.

    WhoScored provides:
    - Detailed match events (passes, shots, tackles, etc.)
    - Player ratings
    - Match statistics
    - Heat maps data

    Events are converted to SPADL format for standardization.

    IMPORTANT: WhoScored uses Cloudflare protection. Use headless=False
    for better success rate.

    Usage:
        scraper = WhoScoredScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
            headless=False  # Recommended
        )
        result = scraper.scrape_all()
    """

    SOURCE_NAME = 'whoscored'
    DEFAULT_RATE_LIMIT = 10  # Very conservative due to Cloudflare

    BASE_URL = 'https://www.whoscored.com'

    # SPADL pitch dimensions (in meters)
    SPADL_PITCH_LENGTH = 105.0
    SPADL_PITCH_WIDTH = 68.0

    # WhoScored uses 100x100 coordinate system
    WS_COORD_MAX = 100.0

    # Event type mapping to SPADL actions
    EVENT_TYPE_MAPPING = {
        'Pass': 'pass',
        'Cross': 'cross',
        'Throw-in': 'throw_in',
        'FreekickShort': 'freekick_short',
        'Corner': 'corner_crossed',
        'TakeOn': 'take_on',
        'Foul': 'foul',
        'Tackle': 'tackle',
        'Interception': 'interception',
        'Shot': 'shot',
        'ShotOnPost': 'shot',
        'MissedShots': 'shot',
        'SavedShot': 'shot',
        'Goal': 'shot',
        'Clearance': 'clearance',
        'BallTouch': 'dribble',
        'Aerial': 'non_action',
        'OffsidePass': 'pass',
    }

    # League URL slugs
    LEAGUE_SLUGS = {
        'ENG-Premier League': 'England-Premier-League',
        'ESP-La Liga': 'Spain-LaLiga',
        'GER-Bundesliga': 'Germany-Bundesliga',
        'ITA-Serie A': 'Italy-Serie-A',
        'FRA-Ligue 1': 'France-Ligue-1',
    }

    def __init__(
        self,
        leagues: Optional[List[str]] = None,
        seasons: Optional[List[int]] = None,
        headless: bool = False,  # Recommended False for WhoScored
        **kwargs
    ):
        super().__init__(
            leagues=leagues,
            seasons=seasons,
            headless=headless,
            **kwargs
        )
        self._match_cache: Dict[str, Dict] = {}

    def _get_browser(self) -> CloudflareBypass:
        """Get browser with WhoScored-specific configuration."""
        if self._browser is None:
            self._browser = CloudflareBypass(
                headless=self.headless,
                proxy=self.proxy,
                page_load_timeout=60,  # WhoScored can be slow
            )
        return self._browser

    def _navigate_to_match(self, match_url: str) -> bool:
        """
        Navigate to match page and wait for data to load.

        Args:
            match_url: Full match URL

        Returns:
            True if successful
        """
        browser = self._get_browser()

        try:
            html = browser.get_page(
                match_url,
                wait_for_selector='#player-table-statistics-body',
                wait_timeout=30,
                cloudflare_wait=10.0,
            )

            # Check if page loaded correctly
            if 'matchCentreData' in html or 'incidentEvents' in html:
                return True

            logger.warning(f"Match data not found on page: {match_url}")
            return False

        except Exception as e:
            logger.error(f"Error navigating to match: {e}")
            return False

    def _extract_match_data(self) -> Optional[Dict[str, Any]]:
        """
        Extract match data from page JavaScript.

        Returns:
            Match data dictionary
        """
        browser = self._get_browser()

        try:
            # Try to extract matchCentreData from page
            script = """
            if (typeof matchCentreData !== 'undefined') {
                return JSON.stringify(matchCentreData);
            }
            return null;
            """
            result = browser.execute_script(script)

            if result:
                return json.loads(result)

            # Alternative: extract from script tags
            page_source = browser.page_source
            pattern = r'matchCentreData\s*=\s*(\{.*?\});'
            match = re.search(pattern, page_source, re.DOTALL)

            if match:
                return json.loads(match.group(1))

            logger.warning("Could not extract matchCentreData")
            return None

        except Exception as e:
            logger.error(f"Error extracting match data: {e}")
            return None

    def _convert_coordinates(
        self,
        x: float,
        y: float
    ) -> Tuple[float, float]:
        """
        Convert WhoScored coordinates to SPADL format.

        WhoScored uses 0-100 scale, SPADL uses meters (105x68).

        Args:
            x: X coordinate (0-100)
            y: Y coordinate (0-100)

        Returns:
            Tuple of (x, y) in SPADL coordinates
        """
        spadl_x = (x / self.WS_COORD_MAX) * self.SPADL_PITCH_LENGTH
        spadl_y = (y / self.WS_COORD_MAX) * self.SPADL_PITCH_WIDTH

        return spadl_x, spadl_y

    def _event_to_spadl(
        self,
        event: Dict[str, Any],
        match_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Convert WhoScored event to SPADL format.

        Args:
            event: Raw WhoScored event
            match_info: Match metadata

        Returns:
            SPADL formatted event
        """
        event_type = event.get('type', {}).get('displayName', 'Unknown')
        qualifiers = {
            q.get('type', {}).get('displayName', ''): q.get('value')
            for q in event.get('qualifiers', [])
        }

        # Convert coordinates
        start_x, start_y = 0.0, 0.0
        end_x, end_y = 0.0, 0.0

        if 'x' in event and 'y' in event:
            start_x, start_y = self._convert_coordinates(
                event.get('x', 0), event.get('y', 0)
            )

        if 'endX' in event and 'endY' in event:
            end_x, end_y = self._convert_coordinates(
                event.get('endX', event.get('x', 0)),
                event.get('endY', event.get('y', 0))
            )
        else:
            end_x, end_y = start_x, start_y

        # Determine SPADL action type
        action_type = self.EVENT_TYPE_MAPPING.get(event_type, 'non_action')

        # Special cases
        if 'Penalty' in qualifiers:
            action_type = 'shot_penalty'
        elif 'FreekickTaken' in qualifiers and event_type == 'Shot':
            action_type = 'shot_freekick'
        elif 'CornerTaken' in qualifiers:
            if 'ShortCorner' in qualifiers:
                action_type = 'corner_short'
            else:
                action_type = 'corner_crossed'

        # Determine result
        outcome = event.get('outcomeType', {}).get('displayName', 'Unknown')
        if outcome == 'Successful':
            result = 'success'
        elif 'Goal' in event_type or event.get('isGoal'):
            result = 'success'
        elif 'OwnGoal' in qualifiers:
            result = 'owngoal'
        elif outcome == 'Unsuccessful':
            result = 'fail'
        else:
            result = 'fail'

        # Determine body part
        bodypart = 'foot'
        if 'Head' in qualifiers:
            bodypart = 'head'
        elif 'OtherBodyPart' in qualifiers:
            bodypart = 'other'

        # Calculate time in seconds
        minute = event.get('minute', 0)
        second = event.get('second', 0)
        period = event.get('period', {}).get('value', 1)

        if period == 1:
            time_seconds = minute * 60 + second
        else:
            time_seconds = 45 * 60 + (minute - 45) * 60 + second

        return {
            'league': match_info.get('league'),
            'season': match_info.get('season'),
            'game_id': match_info.get('match_id'),
            'match_date': match_info.get('match_date'),
            'home_team': match_info.get('home_team'),
            'away_team': match_info.get('away_team'),
            'home_team_id': match_info.get('home_team_id'),
            'away_team_id': match_info.get('away_team_id'),
            'event_id': event.get('id'),
            'period_id': period,
            'time_seconds': time_seconds,
            'team_id': event.get('teamId'),
            'team': match_info.get('home_team') if event.get('teamId') == match_info.get('home_team_id') else match_info.get('away_team'),
            'player_id': event.get('playerId'),
            'player': event.get('playerName', ''),
            'start_x': start_x,
            'start_y': start_y,
            'end_x': end_x,
            'end_y': end_y,
            'action_type': action_type,
            'result': result,
            'bodypart': bodypart,
            'original_event_type': event_type,
            'original_outcome_type': outcome,
            'is_goal': event.get('isGoal', False),
            'is_own_goal': 'OwnGoal' in qualifiers,
            'is_assist': 'IntentionalGoalAssist' in qualifiers or 'IntentionalAssist' in qualifiers,
            'is_key_pass': 'KeyPass' in qualifiers,
        }

    def read_match_events(
        self,
        match_url: str,
        league: str,
        season: int
    ) -> Optional[pd.DataFrame]:
        """
        Read and convert match events to SPADL format.

        Args:
            match_url: Full match URL
            league: League name
            season: Season year

        Returns:
            DataFrame with SPADL events
        """
        logger.info(f"Fetching WhoScored events: {match_url}")

        try:
            # Navigate to match page
            if not self._navigate_to_match(match_url):
                return None

            # Wait for dynamic content
            time.sleep(2)

            # Extract match data
            match_data = self._extract_match_data()
            if not match_data:
                return None

            # Extract match info
            home_team = match_data.get('home', {})
            away_team = match_data.get('away', {})

            match_info = {
                'league': league,
                'season': season,
                'match_id': match_data.get('matchId'),
                'match_date': match_data.get('startDate', '').split('T')[0],
                'home_team': home_team.get('name'),
                'away_team': away_team.get('name'),
                'home_team_id': home_team.get('teamId'),
                'away_team_id': away_team.get('teamId'),
            }

            # Convert events to SPADL
            events = match_data.get('events', [])
            spadl_events = []

            for event in events:
                try:
                    spadl_event = self._event_to_spadl(event, match_info)
                    spadl_events.append(spadl_event)
                except Exception as e:
                    logger.debug(f"Error converting event: {e}")
                    continue

            if spadl_events:
                df = pd.DataFrame(spadl_events)
                df = self._add_metadata(df, 'events_spadl')
                return df

            return None

        except Exception as e:
            logger.error(f"Error reading match events: {e}")
            return None

    def get_match_urls(
        self,
        league: str,
        season: int
    ) -> List[str]:
        """
        Get list of match URLs for a league and season.

        Args:
            league: League name
            season: Season year

        Returns:
            List of match URLs
        """
        # This would typically scrape the fixtures page
        # For now, return empty list as this requires additional implementation
        logger.warning(
            "get_match_urls not fully implemented - "
            "provide match URLs directly"
        )
        return []

    def scrape_match(
        self,
        match_url: str,
        league: str,
        season: int
    ) -> Dict[str, str]:
        """
        Scrape a single match.

        Args:
            match_url: Full match URL
            league: League name
            season: Season year

        Returns:
            Dictionary with table path
        """
        df = self.read_match_events(match_url, league, season)

        if df is not None and not df.empty:
            table_path = self.save_to_iceberg(
                df=df,
                table_name='whoscored_events_spadl',
                partition_cols=['league', 'season'],
            )
            return {'events': table_path}

        return {}

    def scrape_all(self) -> Dict[str, str]:
        """
        Scrape all WhoScored data.

        Note: This requires match URLs to be provided or
        get_match_urls to be implemented.

        Returns:
            Dictionary mapping data type to Iceberg table path
        """
        logger.info(
            f"Starting WhoScored scrape: leagues={self.leagues}, seasons={self.seasons}"
        )

        results = {}
        all_events = []

        for league in self.leagues:
            for season in self.seasons:
                match_urls = self.get_match_urls(league, season)

                for url in match_urls:
                    try:
                        df = self.read_match_events(url, league, season)
                        if df is not None and not df.empty:
                            all_events.append(df)

                        # Rate limiting between matches
                        time.sleep(5)

                    except Exception as e:
                        logger.error(f"Error scraping match {url}: {e}")
                        continue

        if all_events:
            combined_df = pd.concat(all_events, ignore_index=True)
            table_path = self.save_to_iceberg(
                df=combined_df,
                table_name='whoscored_events_spadl',
                partition_cols=['league', 'season'],
            )
            results['events'] = table_path

        logger.info(f"WhoScored scrape complete: {list(results.keys())}")
        return results


# SPADL action type definitions for reference
SPADL_ACTIONS = {
    'pass': 'Normal pass in open play',
    'cross': 'Cross into the box',
    'throw_in': 'Throw in',
    'freekick_crossed': 'Freekick crossed into the box',
    'freekick_short': 'Short freekick',
    'corner_crossed': 'Corner crossed into the box',
    'corner_short': 'Short corner',
    'take_on': 'Dribble past opponent',
    'foul': 'Foul',
    'tackle': 'Tackle',
    'interception': 'Interception',
    'shot': 'Shot from open play',
    'shot_penalty': 'Penalty kick',
    'shot_freekick': 'Direct freekick on goal',
    'keeper_save': 'Goalkeeper save',
    'keeper_claim': 'Goalkeeper catch',
    'keeper_punch': 'Goalkeeper punch',
    'keeper_pick_up': 'Goalkeeper picks up ball',
    'clearance': 'Clearance',
    'bad_touch': 'Bad touch / loss of possession',
    'non_action': 'Non-action (aerial duel, etc.)',
    'dribble': 'Dribble / carry',
    'goalkick': 'Goal kick',
}
