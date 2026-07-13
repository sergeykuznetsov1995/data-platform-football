"""
Tests for FBref finders — parse_events_from_scorebox, parse_shots_table,
parse_lineup_table and their helpers.

Covers BUG 4 (events only first player), BUG 5 (minute U+02BC),
BUG 6 (shots no logging), BUG 7 (lineup positions empty).
"""

import logging

import pytest
import pandas as pd
from bs4 import BeautifulSoup

from scrapers.fbref.parsers.finders import (
    parse_events_from_scorebox,
    parse_lineup_table,
    parse_shots_table,
    parse_team_match_stats_table,
    parse_player_match_stats_tables,
    _extract_team_names_from_scorebox,
    _detect_event_type,
    _enrich_lineup_positions,
    _safe_int,
    _parse_pct,
    _parse_of_stat,
)


# ---------------------------------------------------------------------------
# Fixtures — reusable HTML fragments
# ---------------------------------------------------------------------------

EVENTS_WRAP_HTML = """
<html><body>
<div class="scorebox">
  <div><strong><a href="/en/squads/18bb7c10/Arsenal">Arsenal</a></strong></div>
  <div><strong><a href="/en/squads/b8fd03ef/Manchester-City">Manchester City</a></strong></div>
</div>
<div id="events_wrap">
  <div class="event a">
    <div>
      <div>23\u02bc</div>
      <div class="goal">
        <a href="/en/players/b66315ae/Bukayo-Saka">Bukayo Saka</a>
        <small><a href="/en/players/d70ce98e/Martin-Odegaard">Martin \u00d8degaard</a></small>
      </div>
    </div>
  </div>
  <div class="event b">
    <div>
      <div>45+2\u02bc</div>
      <div class="goal">
        <a href="/en/players/abc12345/Erling-Haaland">Erling Haaland</a>
      </div>
    </div>
  </div>
  <div class="event a">
    <div>
      <div>67\u02bc</div>
      <div class="yellow_card">
        <a href="/en/players/cccccccc/Gabriel-Magalhaes">Gabriel</a>
      </div>
    </div>
  </div>
  <div class="event b">
    <div>
      <div>78\u02bc</div>
      <div class="substitute">
        <a href="/en/players/dddddddd/Phil-Foden">Phil Foden</a>
        <a href="/en/players/eeeeeeee/Jack-Grealish">Jack Grealish</a>
      </div>
    </div>
  </div>
</div>
</body></html>
"""

LINEUP_HTML = """
<html><body>
<div class="lineup" id="a">
  <table>
    <tbody>
      <tr><th colspan="2">Arsenal (4-3-3)</th></tr>
      <tr><td>1</td><td><a href="/en/players/aaa00001/GK-Name">Aaron Ramsdale</a></td></tr>
      <tr><td>4</td><td><a href="/en/players/aaa00002/DF-Name">Ben White</a></td></tr>
      <tr><th colspan="2">Bench</th></tr>
      <tr><td>15</td><td><a href="/en/players/aaa00003/Sub-Name">Jakub Kiwior</a></td></tr>
    </tbody>
  </table>
</div>
<div class="lineup" id="b">
  <table>
    <tbody>
      <tr><th colspan="2">Manchester City (4-2-3-1)</th></tr>
      <tr><td>31</td><td><a href="/en/players/bbb00001/GK-Name">Ederson</a></td></tr>
      <tr><th colspan="2">Bench</th></tr>
      <tr><td>18</td><td><a href="/en/players/bbb00002/Sub-Name">Stefan Ortega</a></td></tr>
    </tbody>
  </table>
</div>
</body></html>
"""

SUMMARY_TABLE_HTML = """
<table id="stats_18bb7c10_summary">
  <thead><tr><th>Player</th><th>Pos</th></tr></thead>
  <tbody>
    <tr>
      <td data-stat="player"><a href="/en/players/aaa00001/GK-Name">Aaron Ramsdale</a></td>
      <td data-stat="position">GK</td>
    </tr>
    <tr>
      <td data-stat="player"><a href="/en/players/aaa00002/DF-Name">Ben White</a></td>
      <td data-stat="position">DF</td>
    </tr>
    <tr>
      <td data-stat="player"><a href="/en/players/aaa00003/Sub-Name">Jakub Kiwior</a></td>
      <td data-stat="position">DF</td>
    </tr>
  </tbody>
</table>
"""


# ===========================================================================
# TestParseEventsFromScorebox
# ===========================================================================

class TestParseEventsFromScorebox:
    """Tests for parse_events_from_scorebox (BUG 4 + BUG 5)."""

    def _make_soup(self, html=EVENTS_WRAP_HTML):
        return BeautifulSoup(html, 'html.parser')

    def test_events_count(self):
        """All 4 events are captured (not just first player)."""
        df = parse_events_from_scorebox(self._make_soup())
        assert df is not None
        assert len(df) == 4

    def test_all_players_captured(self):
        """Every event div produces its own row — BUG 4 fix."""
        df = parse_events_from_scorebox(self._make_soup())
        players = df['player'].tolist()
        assert 'Bukayo Saka' in players
        assert 'Erling Haaland' in players
        assert 'Gabriel' in players
        assert 'Phil Foden' in players

    def test_minute_parsed_with_unicode_apostrophe(self):
        """Minute extracted despite U+02BC character — BUG 5 fix."""
        df = parse_events_from_scorebox(self._make_soup())
        minutes = df['minute'].tolist()
        assert '23' in minutes
        assert '67' in minutes

    def test_stoppage_time_parsed(self):
        """Stoppage time like 45+2 is correctly parsed."""
        df = parse_events_from_scorebox(self._make_soup())
        assert '45+2' in df['minute'].tolist()

    def test_event_types(self):
        """Event types detected from CSS classes."""
        df = parse_events_from_scorebox(self._make_soup())
        types = df['event_type'].tolist()
        assert 'goal' in types
        assert 'yellow_card' in types
        assert 'substitution' in types

    def test_team_sides(self):
        """Home/away team sides determined from div classes."""
        df = parse_events_from_scorebox(self._make_soup())
        sides = df['team_side'].tolist()
        assert 'home' in sides
        assert 'away' in sides
        # Saka event is home (class 'a')
        saka_row = df[df['player'] == 'Bukayo Saka'].iloc[0]
        assert saka_row['team_side'] == 'home'

    def test_team_names_from_scorebox(self):
        """Team names resolved from scorebox squad links."""
        df = parse_events_from_scorebox(self._make_soup())
        saka_row = df[df['player'] == 'Bukayo Saka'].iloc[0]
        assert saka_row['team'] == 'Arsenal'
        haaland_row = df[df['player'] == 'Erling Haaland'].iloc[0]
        assert haaland_row['team'] == 'Manchester City'

    def test_player_ids_extracted(self):
        """Player IDs extracted via PLAYER_ID_PATTERN."""
        df = parse_events_from_scorebox(self._make_soup())
        saka_row = df[df['player'] == 'Bukayo Saka'].iloc[0]
        assert saka_row['player_id'] == 'b66315ae'

    def test_secondary_player(self):
        """Secondary player (assist/sub) captured."""
        df = parse_events_from_scorebox(self._make_soup())
        # Saka goal has Odegaard as assister
        saka_row = df[df['player'] == 'Bukayo Saka'].iloc[0]
        assert 'degaard' in saka_row['secondary_player']  # Ødegaard
        assert saka_row['secondary_player_id'] == 'd70ce98e'

    def test_substitution_secondary_player(self):
        """Substitution event captures subbed-off player as secondary."""
        df = parse_events_from_scorebox(self._make_soup())
        sub_row = df[df['player'] == 'Phil Foden'].iloc[0]
        assert sub_row['secondary_player'] == 'Jack Grealish'
        assert sub_row['secondary_player_id'] == 'eeeeeeee'

    def test_no_events_returns_none(self):
        """Returns None when no event divs found."""
        html = '<html><body><div class="scorebox"></div></body></html>'
        assert parse_events_from_scorebox(BeautifulSoup(html, 'html.parser')) is None

    def test_no_scorebox_returns_none(self):
        """Returns None when neither events_wrap nor scorebox exist."""
        html = '<html><body><p>Nothing here</p></body></html>'
        assert parse_events_from_scorebox(BeautifulSoup(html, 'html.parser')) is None

    def test_ascii_apostrophe_fallback(self):
        """Minutes with plain ASCII apostrophe are also parsed."""
        html = """
        <html><body>
        <div class="scorebox">
          <div><a href="/en/squads/aaaaaaaa/Team-A">Team A</a></div>
          <div><a href="/en/squads/bbbbbbbb/Team-B">Team B</a></div>
        </div>
        <div id="events_wrap">
          <div class="event a">
            <div>
              <div>10'</div>
              <div class="goal">
                <a href="/en/players/11111111/Player-One">Player One</a>
              </div>
            </div>
          </div>
        </div>
        </body></html>
        """
        df = parse_events_from_scorebox(BeautifulSoup(html, 'html.parser'))
        assert df is not None
        assert df.iloc[0]['minute'] == '10'

    def test_right_single_quote_u2019(self):
        """Minutes with U+2019 RIGHT SINGLE QUOTATION MARK (real FBref Feb 2026)."""
        html = """
        <html><body>
        <div class="scorebox">
          <div><a href="/en/squads/aaaaaaaa/Team-A">Team A</a></div>
          <div><a href="/en/squads/bbbbbbbb/Team-B">Team B</a></div>
        </div>
        <div id="events_wrap">
          <div class="event a">
            <div>
              <div>13\u2019</div>
              <div class="goal">
                <a href="/en/players/11111111/Player-One">Player One</a>
              </div>
            </div>
          </div>
          <div class="event b">
            <div>
              <div>90+3\u2019</div>
              <div class="yellow_card">
                <a href="/en/players/22222222/Player-Two">Player Two</a>
              </div>
            </div>
          </div>
        </div>
        </body></html>
        """
        df = parse_events_from_scorebox(BeautifulSoup(html, 'html.parser'))
        assert df is not None
        assert len(df) == 2
        assert df.iloc[0]['minute'] == '13'
        assert df.iloc[1]['minute'] == '90+3'


# ===========================================================================
# TestParseShotsTable
# ===========================================================================

class TestParseShotsTable:
    """Tests for parse_shots_table (BUG 6 — diagnostic logging)."""

    def test_found_in_dom(self):
        """Shots table found directly in DOM."""
        html = """
        <html>
        <table id="shots_all">
            <thead><tr><th>Minute</th><th>Player</th><th>xG</th></tr></thead>
            <tbody>
                <tr><td>10</td><td>Saka</td><td>0.15</td></tr>
                <tr><td>35</td><td>Haaland</td><td>0.45</td></tr>
            </tbody>
        </table>
        </html>
        """
        soup = BeautifulSoup(html, 'html.parser')
        df = parse_shots_table(soup, {})
        assert df is not None
        assert len(df) == 2

    def test_empty_table_returns_none(self):
        """Found-but-empty shots table returns None."""
        html = """
        <html>
        <table id="shots_all">
            <thead><tr><th>Minute</th><th>Player</th><th>xG</th></tr></thead>
            <tbody></tbody>
        </table>
        </html>
        """
        soup = BeautifulSoup(html, 'html.parser')
        df = parse_shots_table(soup, {})
        assert df is None

    def test_not_found_returns_none(self):
        """No shots table at all returns None."""
        html = "<html><body><p>No tables</p></body></html>"
        soup = BeautifulSoup(html, 'html.parser')
        df = parse_shots_table(soup, {})
        assert df is None

    def test_logging_on_empty(self, caplog):
        """Empty shots table produces info-level log message."""
        html = """
        <html>
        <table id="shots_all">
            <thead><tr><th>Minute</th><th>Player</th></tr></thead>
            <tbody></tbody>
        </table>
        </html>
        """
        soup = BeautifulSoup(html, 'html.parser')
        with caplog.at_level(logging.INFO):
            parse_shots_table(soup, {})
        assert any('found but empty' in msg for msg in caplog.messages)

    def test_logging_on_not_found(self, caplog):
        """Missing shots table produces debug-level diagnostic."""
        html = '<html><body><table id="other_table"><tbody></tbody></table></body></html>'
        soup = BeautifulSoup(html, 'html.parser')
        with caplog.at_level(logging.DEBUG):
            parse_shots_table(soup, {})
        assert any('No shots table found' in msg for msg in caplog.messages)

    def test_found_in_comment_tables(self):
        """Shots table found inside comment_tables dict."""
        main_html = "<html><body></body></html>"
        soup = BeautifulSoup(main_html, 'html.parser')

        comment_html = """
        <table id="shots_all">
            <thead><tr><th>Minute</th><th>Player</th><th>xG</th></tr></thead>
            <tbody>
                <tr><td>55</td><td>Salah</td><td>0.30</td></tr>
            </tbody>
        </table>
        """
        comment_soup = BeautifulSoup(comment_html, 'html.parser')
        comment_table = comment_soup.find('table')

        df = parse_shots_table(soup, {'shots_all': comment_table})
        assert df is not None
        assert len(df) == 1


# ===========================================================================
# TestParseLineupTable
# ===========================================================================

class TestParseLineupTable:
    """Tests for parse_lineup_table (BUG 7 — positions + is_starter)."""

    def _make_soup(self, html=LINEUP_HTML):
        return BeautifulSoup(html, 'html.parser')

    def test_lineup_count(self):
        """All players from both teams are captured."""
        df = parse_lineup_table(self._make_soup())
        assert df is not None
        # 2 starters + 1 bench (Arsenal) + 1 starter + 1 bench (City) = 5
        assert len(df) == 5

    def test_team_names(self):
        """Team names extracted and formation stripped."""
        df = parse_lineup_table(self._make_soup())
        teams = df['team'].unique().tolist()
        assert 'Arsenal' in teams
        assert 'Manchester City' in teams

    def test_is_starter_bench_marker(self):
        """is_starter determined by Bench header, not index < 11."""
        df = parse_lineup_table(self._make_soup())
        # Arsenal: 2 starters, 1 bench
        arsenal = df[df['team'] == 'Arsenal']
        starters = arsenal[arsenal['is_starter'] == True]
        bench = arsenal[arsenal['is_starter'] == False]
        assert len(starters) == 2
        assert len(bench) == 1

    def test_player_ids(self):
        """Player IDs extracted via PLAYER_ID_PATTERN."""
        df = parse_lineup_table(self._make_soup())
        ramsdale = df[df['player'] == 'Aaron Ramsdale'].iloc[0]
        assert ramsdale['player_id'] == 'aaa00001'

    def test_jersey_numbers(self):
        """Jersey numbers parsed from first <td>."""
        df = parse_lineup_table(self._make_soup())
        ramsdale = df[df['player'] == 'Aaron Ramsdale'].iloc[0]
        assert ramsdale['number'] == '1'
        kiwior = df[df['player'] == 'Jakub Kiwior'].iloc[0]
        assert kiwior['number'] == '15'

    def test_position_without_comment_tables(self):
        """Without comment_tables, positions remain empty."""
        df = parse_lineup_table(self._make_soup())
        assert all(df['position'] == '')

    def test_position_with_comment_tables(self):
        """With summary table in comment_tables, positions are enriched."""
        soup = self._make_soup()
        comment_soup = BeautifulSoup(SUMMARY_TABLE_HTML, 'html.parser')
        comment_table = comment_soup.find('table')
        comment_tables = {'stats_18bb7c10_summary': comment_table}

        df = parse_lineup_table(soup, comment_tables=comment_tables)
        assert df is not None

        ramsdale = df[df['player'] == 'Aaron Ramsdale'].iloc[0]
        assert ramsdale['position'] == 'GK'
        white = df[df['player'] == 'Ben White'].iloc[0]
        assert white['position'] == 'DF'
        kiwior = df[df['player'] == 'Jakub Kiwior'].iloc[0]
        assert kiwior['position'] == 'DF'

    def test_no_lineup_returns_none(self):
        """Returns None when no lineup divs found."""
        html = '<html><body><p>No lineups</p></body></html>'
        df = parse_lineup_table(BeautifulSoup(html, 'html.parser'))
        assert df is None

    def test_backward_compatible_without_comment_tables(self):
        """Function works without comment_tables parameter (backward compat)."""
        df = parse_lineup_table(self._make_soup())
        assert df is not None
        assert len(df) > 0


# ===========================================================================
# TestHelpers
# ===========================================================================

class TestExtractTeamNamesFromScorebox:
    """Tests for _extract_team_names_from_scorebox helper."""

    def test_both_teams(self):
        html = """
        <div class="scorebox">
          <div><a href="/squads/aaaa1111/Home-Team">Home Team</a></div>
          <div><a href="/squads/bbbb2222/Away-Team">Away Team</a></div>
        </div>
        """
        soup = BeautifulSoup(html, 'html.parser')
        names = _extract_team_names_from_scorebox(soup)
        assert names['home'] == 'Home Team'
        assert names['away'] == 'Away Team'

    def test_no_scorebox(self):
        html = '<html><body></body></html>'
        soup = BeautifulSoup(html, 'html.parser')
        names = _extract_team_names_from_scorebox(soup)
        assert names['home'] == ''
        assert names['away'] == ''


class TestDetectEventType:
    """Tests for _detect_event_type helper."""

    def test_goal(self):
        html = '<div class="event"><div class="goal">Content</div></div>'
        div = BeautifulSoup(html, 'html.parser').find('div', class_='event')
        assert _detect_event_type(div) == 'goal'

    def test_own_goal(self):
        html = '<div class="event"><div class="own_goal">Content</div></div>'
        div = BeautifulSoup(html, 'html.parser').find('div', class_='event')
        assert _detect_event_type(div) == 'own_goal'

    def test_yellow_card(self):
        html = '<div class="event"><div class="yellow_card">Content</div></div>'
        div = BeautifulSoup(html, 'html.parser').find('div', class_='event')
        assert _detect_event_type(div) == 'yellow_card'

    def test_red_card(self):
        html = '<div class="event"><div class="red_card">Content</div></div>'
        div = BeautifulSoup(html, 'html.parser').find('div', class_='event')
        assert _detect_event_type(div) == 'red_card'

    def test_substitute(self):
        html = '<div class="event"><div class="substitute">Content</div></div>'
        div = BeautifulSoup(html, 'html.parser').find('div', class_='event')
        assert _detect_event_type(div) == 'substitution'

    def test_penalty_from_text(self):
        html = '<div class="event"><div class="goal">(Pen.) content</div></div>'
        div = BeautifulSoup(html, 'html.parser').find('div', class_='event')
        assert _detect_event_type(div) == 'penalty'

    def test_penalty_goal(self):
        # Scored penalty — FBref `penalty_goal` sprite. Must stay 'penalty'
        # (gold maps it to penalty_goal). Regression guard for #447: the new
        # penalty_miss branch must NOT swallow a scored penalty.
        html = '<div class="event"><div class="penalty_goal">Content</div></div>'
        div = BeautifulSoup(html, 'html.parser').find('div', class_='event')
        assert _detect_event_type(div) == 'penalty'

    def test_penalty_miss(self):
        # Missed penalty — FBref `penalty_miss` sprite. Must NOT collapse into a
        # scored penalty (which inflates the downstream running score). See #447.
        html = '<div class="event"><div class="penalty_miss">Content</div></div>'
        div = BeautifulSoup(html, 'html.parser').find('div', class_='event')
        assert _detect_event_type(div) == 'penalty_missed'

    def test_second_yellow_card(self):
        # Second yellow → sending off. FBref marks it with the `yellow_red_card`
        # sprite; must map to second_yellow_card, NOT yellow_card (dead branch
        # before #447 left the category empty across all seasons).
        html = '<div class="event"><div class="yellow_red_card">Content</div></div>'
        div = BeautifulSoup(html, 'html.parser').find('div', class_='event')
        assert _detect_event_type(div) == 'second_yellow_card'

    def test_unknown(self):
        html = '<div class="event"><div>Just text</div></div>'
        div = BeautifulSoup(html, 'html.parser').find('div', class_='event')
        assert _detect_event_type(div) == 'unknown'


class TestEnrichLineupPositions:
    """Tests for _enrich_lineup_positions helper."""

    def test_enriches_positions(self):
        lineup_df = pd.DataFrame({
            'player': ['Player A', 'Player B'],
            'player_id': ['aaa00001', 'aaa00002'],
            'position': ['', ''],
        })
        summary_html = """
        <table id="stats_xxx_summary">
          <tbody>
            <tr>
              <td data-stat="player"><a href="/players/aaa00001/A">A</a></td>
              <td data-stat="position">GK</td>
            </tr>
            <tr>
              <td data-stat="player"><a href="/players/aaa00002/B">B</a></td>
              <td data-stat="position">MF</td>
            </tr>
          </tbody>
        </table>
        """
        soup = BeautifulSoup('<html></html>', 'html.parser')
        comment_soup = BeautifulSoup(summary_html, 'html.parser')
        comment_tables = {
            'stats_xxx_summary': comment_soup.find('table'),
        }
        result = _enrich_lineup_positions(soup, comment_tables, lineup_df)
        assert result.iloc[0]['position'] == 'GK'
        assert result.iloc[1]['position'] == 'MF'

    def test_no_comment_tables(self):
        lineup_df = pd.DataFrame({
            'player': ['A'],
            'player_id': ['aaa00001'],
            'position': [''],
        })
        soup = BeautifulSoup('<html></html>', 'html.parser')
        result = _enrich_lineup_positions(soup, None, lineup_df)
        assert result.iloc[0]['position'] == ''

    def test_empty_dataframe(self):
        lineup_df = pd.DataFrame()
        soup = BeautifulSoup('<html></html>', 'html.parser')
        result = _enrich_lineup_positions(soup, {}, lineup_df)
        assert result.empty


# ===========================================================================
# HTML fixtures for team match stats
# ===========================================================================

TEAM_STATS_HTML = """
<html><body>
<div class="scorebox">
  <div><strong><a href="/en/squads/18bb7c10/Arsenal">Arsenal</a></strong></div>
  <div><strong><a href="/en/squads/b8fd03ef/Manchester-City">Manchester City</a></strong></div>
</div>
<div id="team_stats">
  <table>
    <tbody>
      <tr><th colspan="2">Possession</th></tr>
      <tr>
        <td style="width:61%"><strong>61%</strong></td>
        <td style="width:39%"><strong>39%</strong></td>
      </tr>
      <tr><th colspan="2">Shots on Target</th></tr>
      <tr>
        <td><strong>7 of 22 — 32%</strong></td>
        <td><strong>33% — 3 of 9</strong></td>
      </tr>
      <tr><th colspan="2">Saves</th></tr>
      <tr>
        <td><strong>2 of 3 — 67%</strong></td>
        <td><strong>68% — 15 of 22</strong></td>
      </tr>
      <tr><th colspan="2">Cards</th></tr>
      <tr>
        <td>
          <span class="yellow_card_sm">&nbsp;</span>
          <span class="yellow_card_sm">&nbsp;</span>
        </td>
        <td>
          <span class="yellow_card_sm">&nbsp;</span>
          <span class="red_card_sm">&nbsp;</span>
        </td>
      </tr>
    </tbody>
  </table>
</div>
<div id="team_stats_extra">
  <div>
    <div>12</div>
    <div>Fouls</div>
    <div>8</div>
  </div>
  <div>
    <div>6</div>
    <div>Corners</div>
    <div>4</div>
  </div>
  <div>
    <div>18</div>
    <div>Crosses</div>
    <div>11</div>
  </div>
  <div>
    <div>5</div>
    <div>Interceptions</div>
    <div>3</div>
  </div>
  <div>
    <div>2</div>
    <div>Offsides</div>
    <div>1</div>
  </div>
</div>
</body></html>
"""

PLAYER_SUMMARY_HTML = """
<html><body>
<div class="scorebox">
  <div><strong><a href="/en/squads/18bb7c10/Arsenal">Arsenal</a></strong></div>
  <div><strong><a href="/en/squads/b8fd03ef/Manchester-City">Manchester City</a></strong></div>
</div>
<table id="stats_18bb7c10_summary">
  <thead>
    <tr><th>Player</th><th>Pos</th><th>Age</th><th>Min</th><th>Gls</th><th>Ast</th></tr>
  </thead>
  <tbody>
    <tr>
      <td data-stat="player"><a href="/en/players/aaa00001/Saka">Bukayo Saka</a></td>
      <td>FW</td><td>23</td><td>90</td><td>1</td><td>0</td>
    </tr>
    <tr>
      <td data-stat="player"><a href="/en/players/aaa00002/Odegaard">Martin Odegaard</a></td>
      <td>MF</td><td>25</td><td>85</td><td>0</td><td>1</td>
    </tr>
  </tbody>
  <tfoot>
    <tr><td>2 Players</td><td></td><td></td><td>175</td><td>1</td><td>1</td></tr>
  </tfoot>
</table>
<table id="stats_b8fd03ef_summary">
  <thead>
    <tr><th>Player</th><th>Pos</th><th>Age</th><th>Min</th><th>Gls</th><th>Ast</th></tr>
  </thead>
  <tbody>
    <tr>
      <td data-stat="player"><a href="/en/players/bbb00001/Haaland">Erling Haaland</a></td>
      <td>FW</td><td>24</td><td>90</td><td>0</td><td>0</td>
    </tr>
    <tr>
      <td data-stat="player"><a href="/en/players/bbb00002/Foden">Phil Foden</a></td>
      <td>MF</td><td>24</td><td>70</td><td>0</td><td>0</td>
    </tr>
  </tbody>
  <tfoot>
    <tr><td>2 Players</td><td></td><td></td><td>160</td><td>0</td><td>0</td></tr>
  </tfoot>
</table>
</body></html>
"""


# ===========================================================================
# TestHelpers — _safe_int, _parse_pct, _parse_of_stat
# ===========================================================================

class TestTeamStatsHelpers:
    """Tests for team match stats helper functions."""

    def test_safe_int_normal(self):
        assert _safe_int('12') == 12

    def test_safe_int_with_noise(self):
        assert _safe_int(' 12 ') == 12

    def test_safe_int_empty(self):
        assert _safe_int('') == 0

    def test_safe_int_none(self):
        assert _safe_int(None) == 0

    def test_parse_pct(self):
        assert _parse_pct('61%') == 61

    def test_parse_pct_with_spaces(self):
        assert _parse_pct(' 39 % ') == 39

    def test_parse_pct_no_match(self):
        assert _parse_pct('no pct') == 0

    def test_parse_of_stat_home_format(self):
        """Home format: '7 of 22 — 32%'"""
        assert _parse_of_stat('7 of 22 — 32%') == (7, 22)

    def test_parse_of_stat_away_format(self):
        """Away format: '33% — 3 of 9'"""
        assert _parse_of_stat('33% — 3 of 9') == (3, 9)

    def test_parse_of_stat_no_match(self):
        assert _parse_of_stat('just text') == (0, 0)


# ===========================================================================
# TestParseTeamMatchStatsTable
# ===========================================================================

class TestParseTeamMatchStatsTable:
    """Tests for the rewritten parse_team_match_stats_table."""

    def _make_soup(self, html=TEAM_STATS_HTML):
        return BeautifulSoup(html, 'html.parser')

    def test_returns_dataframe(self):
        df = parse_team_match_stats_table(self._make_soup(), {})
        assert df is not None
        assert len(df) == 1

    def test_team_names(self):
        df = parse_team_match_stats_table(self._make_soup(), {})
        assert df.iloc[0]['home_team'] == 'Arsenal'
        assert df.iloc[0]['away_team'] == 'Manchester City'

    def test_possession(self):
        df = parse_team_match_stats_table(self._make_soup(), {})
        assert df.iloc[0]['home_possession'] == 61
        assert df.iloc[0]['away_possession'] == 39

    def test_shots_on_target(self):
        df = parse_team_match_stats_table(self._make_soup(), {})
        assert df.iloc[0]['home_sot'] == 7
        assert df.iloc[0]['home_shots'] == 22
        assert df.iloc[0]['away_sot'] == 3
        assert df.iloc[0]['away_shots'] == 9

    def test_saves(self):
        df = parse_team_match_stats_table(self._make_soup(), {})
        assert df.iloc[0]['home_saves'] == 2
        assert df.iloc[0]['away_saves'] == 15

    def test_cards(self):
        df = parse_team_match_stats_table(self._make_soup(), {})
        assert df.iloc[0]['home_yellow_cards'] == 2
        assert df.iloc[0]['home_red_cards'] == 0
        assert df.iloc[0]['away_yellow_cards'] == 1
        assert df.iloc[0]['away_red_cards'] == 1

    def test_extra_stats_fouls(self):
        df = parse_team_match_stats_table(self._make_soup(), {})
        assert df.iloc[0]['home_fouls'] == 12
        assert df.iloc[0]['away_fouls'] == 8

    def test_extra_stats_corners(self):
        df = parse_team_match_stats_table(self._make_soup(), {})
        assert df.iloc[0]['home_corners'] == 6
        assert df.iloc[0]['away_corners'] == 4

    def test_extra_stats_crosses(self):
        df = parse_team_match_stats_table(self._make_soup(), {})
        assert df.iloc[0]['home_crosses'] == 18
        assert df.iloc[0]['away_crosses'] == 11

    def test_extra_stats_interceptions(self):
        df = parse_team_match_stats_table(self._make_soup(), {})
        assert df.iloc[0]['home_interceptions'] == 5
        assert df.iloc[0]['away_interceptions'] == 3

    def test_extra_stats_offsides(self):
        df = parse_team_match_stats_table(self._make_soup(), {})
        assert df.iloc[0]['home_offsides'] == 2
        assert df.iloc[0]['away_offsides'] == 1

    def test_returns_none_when_no_div(self):
        html = '<html><body><p>No team stats</p></body></html>'
        df = parse_team_match_stats_table(
            BeautifulSoup(html, 'html.parser'), {}
        )
        assert df is None

    def test_returns_none_when_empty_div(self):
        html = """
        <html><body>
        <div class="scorebox">
          <div><a href="/squads/aaa/A">A</a></div>
          <div><a href="/squads/bbb/B">B</a></div>
        </div>
        <div id="team_stats"></div>
        </body></html>
        """
        df = parse_team_match_stats_table(
            BeautifulSoup(html, 'html.parser'), {}
        )
        assert df is None

    def test_no_extra_div_still_works(self):
        """Works with only div#team_stats (no team_stats_extra)."""
        html = """
        <html><body>
        <div class="scorebox">
          <div><a href="/squads/aaa/Home">Home</a></div>
          <div><a href="/squads/bbb/Away">Away</a></div>
        </div>
        <div id="team_stats">
          <table><tbody>
            <tr><th colspan="2">Possession</th></tr>
            <tr>
              <td><strong>55%</strong></td>
              <td><strong>45%</strong></td>
            </tr>
          </tbody></table>
        </div>
        </body></html>
        """
        df = parse_team_match_stats_table(
            BeautifulSoup(html, 'html.parser'), {}
        )
        assert df is not None
        assert df.iloc[0]['home_possession'] == 55
        assert 'home_fouls' not in df.columns


# ===========================================================================
# TestParsePlayerMatchStatsTables
# ===========================================================================

class TestParsePlayerMatchStatsTables:
    """Tests for parse_player_match_stats_tables."""

    def _make_soup(self, html=PLAYER_SUMMARY_HTML):
        return BeautifulSoup(html, 'html.parser')

    def test_combines_both_teams(self):
        df = parse_player_match_stats_tables(self._make_soup(), {})
        assert df is not None
        # 2 Arsenal + 2 City = 4 (total rows filtered out)
        assert len(df) == 4

    def test_team_side_assigned(self):
        df = parse_player_match_stats_tables(self._make_soup(), {})
        home = df[df['team_side'] == 'home']
        away = df[df['team_side'] == 'away']
        assert len(home) == 2
        assert len(away) == 2

    def test_team_names(self):
        df = parse_player_match_stats_tables(self._make_soup(), {})
        teams = df['team'].unique().tolist()
        assert 'Arsenal' in teams
        assert 'Manchester City' in teams

    def test_total_rows_filtered(self):
        """'2 Players' footer rows should be filtered out."""
        df = parse_player_match_stats_tables(self._make_soup(), {})
        if 'Player' in df.columns:
            assert not df['Player'].astype(str).str.contains(
                r'^\d+\s+Players?$', regex=True
            ).any()

    def test_returns_none_when_no_tables(self):
        html = '<html><body><p>No tables</p></body></html>'
        df = parse_player_match_stats_tables(
            BeautifulSoup(html, 'html.parser'), {}
        )
        assert df is None

    def test_works_with_comment_tables(self):
        """Summary tables in comment_tables dict are also found."""
        main_html = """
        <html><body>
        <div class="scorebox">
          <div><a href="/squads/aaaaaaaa/Home">Home</a></div>
          <div><a href="/squads/bbbbbbbb/Away">Away</a></div>
        </div>
        </body></html>
        """
        soup = BeautifulSoup(main_html, 'html.parser')

        comment_html = """
        <table id="stats_aaaaaaaa_summary">
          <thead><tr><th>Player</th><th>Min</th></tr></thead>
          <tbody>
            <tr><td>Player A</td><td>90</td></tr>
          </tbody>
        </table>
        <table id="stats_bbbbbbbb_summary">
          <thead><tr><th>Player</th><th>Min</th></tr></thead>
          <tbody>
            <tr><td>Player B</td><td>90</td></tr>
          </tbody>
        </table>
        """
        comment_soup = BeautifulSoup(comment_html, 'html.parser')
        comment_tables = {
            table['id']: table for table in comment_soup.find_all('table')
        }

        df = parse_player_match_stats_tables(
            soup, comment_tables
        )
        assert df is not None
        assert len(df) == 2
        assert df.iloc[0]['team_side'] == 'home'

    def test_one_team_summary_is_rejected(self):
        soup = self._make_soup()
        soup.find('table', id='stats_b8fd03ef_summary').decompose()

        assert parse_player_match_stats_tables(soup, {}) is None

    def test_summary_suffix_is_not_a_supported_dataset(self):
        soup = self._make_soup()
        soup.find(
            'table', id='stats_b8fd03ef_summary'
        )['id'] = 'stats_b8fd03ef_summary_extra'

        assert parse_player_match_stats_tables(soup, {}) is None

    def test_one_of_two_unparseable_summaries_is_rejected(self):
        soup = self._make_soup()
        away = soup.find('table', id='stats_b8fd03ef_summary')
        away.find('tbody').decompose()

        assert parse_player_match_stats_tables(soup, {}) is None


# ===========================================================================
# TestParseKeeperMatchStatsTables
# ===========================================================================

# Mirrors the real post-Apr-2026 keeper_stats_{team_id} structure: an
# over_header row ("Shot Stopping"), player name in a <th data-stat="player">
# with a /en/players/{id}/ link, basic GK columns only (no PSxG/launches).
# Home table lives in the DOM, away table inside an HTML comment — both
# discovery paths must work.
_KEEPER_TABLE_TMPL = """
<table id="keeper_stats_{tid}">
  <thead>
    <tr class="over_header">
      <th colspan="3"></th>
      <th colspan="4" data-stat="header_gk_shot_stopping">Shot Stopping</th>
    </tr>
    <tr>
      <th data-stat="player">Player</th>
      <th data-stat="age">Age</th>
      <th data-stat="minutes">Min</th>
      <th data-stat="gk_shots_on_target_against">SoTA</th>
      <th data-stat="gk_goals_against">GA</th>
      <th data-stat="gk_saves">Saves</th>
      <th data-stat="gk_save_pct">Save%</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th data-stat="player" data-append-csv="{pid}">
        <a href="/en/players/{pid}/">{name}</a></th>
      <td data-stat="age">23-127</td>
      <td data-stat="minutes">90</td>
      <td data-stat="gk_shots_on_target_against">{sota}</td>
      <td data-stat="gk_goals_against">{ga}</td>
      <td data-stat="gk_saves">{saves}</td>
      <td data-stat="gk_save_pct">{pct}</td>
    </tr>
  </tbody>
</table>
"""

KEEPER_MATCH_HTML = (
    '<html><body>'
    '<div class="scorebox">'
    '<div><strong><a href="/en/squads/8ef52968/Sunderland">Sunderland</a></strong></div>'
    '<div><strong><a href="/en/squads/cff3d9bb/Chelsea">Chelsea</a></strong></div>'
    '</div>'
    + _KEEPER_TABLE_TMPL.format(
        tid='8ef52968', pid='349fa918', name='Robin Roefs',
        sota='3', ga='1', saves='2', pct='66.7',
    )
    + '<!--'
    + _KEEPER_TABLE_TMPL.format(
        tid='cff3d9bb', pid='58b1b5b6', name='Robert Sanchez',
        sota='7', ga='2', saves='5', pct='71.4',
    )
    + '-->'
    '</body></html>'
)


class TestParseKeeperMatchStatsTables:
    """parse_keeper_match_stats_tables — per-match GK tables."""

    def _parse(self):
        from scrapers.fbref.parsers.finders import parse_keeper_match_stats_tables
        from scrapers.fbref.parsers.table_parser import extract_tables_from_comments

        soup = BeautifulSoup(KEEPER_MATCH_HTML, 'html.parser')
        comment_tables = extract_tables_from_comments(soup)
        return parse_keeper_match_stats_tables(soup, comment_tables)

    @pytest.mark.unit
    def test_both_tables_parsed_dom_and_comment(self):
        df = self._parse()
        assert df is not None
        assert len(df) == 2

    @pytest.mark.unit
    def test_team_side_assignment(self):
        df = self._parse()
        assert list(df['team_side']) == ['home', 'away']
        assert list(df['team']) == ['Sunderland', 'Chelsea']

    @pytest.mark.unit
    def test_player_ids_extracted(self):
        df = self._parse()
        assert set(df['player_id']) == {'349fa918', '58b1b5b6'}

    @pytest.mark.unit
    def test_basic_gk_values_present(self):
        df = self._parse()
        sota_cols = [c for c in df.columns if 'SoTA' in str(c)]
        saves_cols = [c for c in df.columns if 'Saves' in str(c)]
        assert sota_cols and saves_cols
        assert [str(v) for v in df[sota_cols[0]]] == ['3', '7']
        assert [str(v) for v in df[saves_cols[0]]] == ['2', '5']

    @pytest.mark.unit
    def test_no_keeper_tables_returns_none(self):
        from scrapers.fbref.parsers.finders import parse_keeper_match_stats_tables

        soup = BeautifulSoup('<html><body><p>no tables</p></body></html>',
                             'html.parser')
        assert parse_keeper_match_stats_tables(soup, {}) is None

    @pytest.mark.unit
    def test_one_keeper_side_is_rejected(self):
        from scrapers.fbref.parsers.finders import parse_keeper_match_stats_tables

        soup = BeautifulSoup(KEEPER_MATCH_HTML, 'html.parser')
        assert parse_keeper_match_stats_tables(soup, {}) is None


# ===========================================================================
# TestTeamSideById — home/away from the {team_id} in the table id (#A2)
# ===========================================================================

_SUMMARY_TABLE_TMPL = """
<table id="stats_{tid}_summary">
  <thead><tr><th>Player</th><th>Min</th></tr></thead>
  <tbody>
    <tr>
      <td data-stat="player"><a href="/en/players/{pid}/">{name}</a></td>
      <td data-stat="minutes">90</td>
    </tr>
  </tbody>
</table>
"""


class TestTeamSideById:
    """team_side must come from the {team_id} embedded in the table id, not
    from table order — order flips on partially-uncommented pages."""

    _SCOREBOX = (
        '<div class="scorebox">'
        '<div><strong><a href="/en/squads/18bb7c10/Arsenal">Arsenal</a></strong></div>'
        '<div><strong><a href="/en/squads/b8fd03ef/Manchester-City">'
        'Manchester City</a></strong></div>'
        '</div>'
    )

    @pytest.mark.unit
    def test_player_summary_sides_correct_when_away_table_first(self):
        html = (
            '<html><body>' + self._SCOREBOX
            # AWAY (City) summary table comes FIRST in the DOM
            + _SUMMARY_TABLE_TMPL.format(
                tid='b8fd03ef', pid='abc12345', name='Erling Haaland')
            + _SUMMARY_TABLE_TMPL.format(
                tid='18bb7c10', pid='b66315ae', name='Bukayo Saka')
            + '</body></html>'
        )
        soup = BeautifulSoup(html, 'html.parser')
        df = parse_player_match_stats_tables(soup, {})
        by_player = df.set_index('Player')
        assert by_player.loc['Erling Haaland', 'team_side'] == 'away'
        assert by_player.loc['Erling Haaland', 'team'] == 'Manchester City'
        assert by_player.loc['Bukayo Saka', 'team_side'] == 'home'
        assert by_player.loc['Bukayo Saka', 'team'] == 'Arsenal'

    @pytest.mark.unit
    def test_duplicate_crest_and_name_links_do_not_duplicate_team_id(self):
        scorebox = (
            '<div class="scorebox">'
            '<div><a href="/en/squads/18bb7c10/Arsenal"><img></a>'
            '<a href="/en/squads/18bb7c10/Arsenal">Arsenal</a></div>'
            '<div><a href="/en/squads/b8fd03ef/Manchester-City"><img></a>'
            '<a href="/en/squads/b8fd03ef/Manchester-City">'
            'Manchester City</a></div></div>'
        )
        html = (
            '<html><body>' + scorebox
            + _SUMMARY_TABLE_TMPL.format(
                tid='b8fd03ef', pid='abc12345', name='Erling Haaland')
            + _SUMMARY_TABLE_TMPL.format(
                tid='18bb7c10', pid='b66315ae', name='Bukayo Saka')
            + '</body></html>'
        )

        df = parse_player_match_stats_tables(
            BeautifulSoup(html, 'html.parser'), {}
        )

        assert df is not None
        by_player = df.set_index('Player')
        assert by_player.loc['Erling Haaland', 'team_side'] == 'away'
        assert by_player.loc['Erling Haaland', 'team'] == 'Manchester City'

    @pytest.mark.unit
    def test_keeper_sides_correct_when_away_table_first(self):
        from scrapers.fbref.parsers.finders import parse_keeper_match_stats_tables

        html = (
            '<html><body>'
            '<div class="scorebox">'
            '<div><strong><a href="/en/squads/8ef52968/Sunderland">'
            'Sunderland</a></strong></div>'
            '<div><strong><a href="/en/squads/cff3d9bb/Chelsea">'
            'Chelsea</a></strong></div>'
            '</div>'
            # AWAY (Chelsea) keeper table comes FIRST in the DOM
            + _KEEPER_TABLE_TMPL.format(
                tid='cff3d9bb', pid='58b1b5b6', name='Robert Sanchez',
                sota='7', ga='2', saves='5', pct='71.4')
            + _KEEPER_TABLE_TMPL.format(
                tid='8ef52968', pid='349fa918', name='Robin Roefs',
                sota='3', ga='1', saves='2', pct='66.7')
            + '</body></html>'
        )
        soup = BeautifulSoup(html, 'html.parser')
        df = parse_keeper_match_stats_tables(soup, {})
        assert list(df['team_side']) == ['away', 'home']
        assert list(df['team']) == ['Chelsea', 'Sunderland']

    @pytest.mark.unit
    def test_unknown_team_ids_fail_strict_player_contract(self):
        html = (
            '<html><body>' + self._SCOREBOX
            # Table id whose team_id matches neither scorebox squad
            + _SUMMARY_TABLE_TMPL.format(
                tid='deadbeef', pid='abc12345', name='Somebody')
            + _SUMMARY_TABLE_TMPL.format(
                tid='feedface', pid='def67890', name='Somebody Else')
            + '</body></html>'
        )
        soup = BeautifulSoup(html, 'html.parser')
        df = parse_player_match_stats_tables(soup, {})
        assert df is None


# ===========================================================================
# TestEventIconContract (#901 follow-up: Strasbourg-Lille 94889482)
# ===========================================================================

BECOMES_GK_EVENT_HTML = """
<html><body>
<div class="scorebox">
  <div><strong><a href="/en/squads/c0d3d0c8/Strasbourg">Strasbourg</a></strong></div>
  <div><strong><a href="/en/squads/cb188c0c/Lille">Lille</a></strong></div>
</div>
<div id="events_wrap">
  <div class="event a">
    <div>74ʼ<small><span>1:0</span></small></div>
    <div>
      <div class="event_icon goal"></div>
      <div><div><a href="/en/players/aaaaaaaa/Jonas-Martin">Jonas Martin</a></div></div>
      <div style="display: none;">&mdash; Goal</div>
    </div>
  </div>
  <div class="event b">
    <div>74ʼ<small><span>1:0</span></small></div>
    <div>
      <div class="event_icon becomes_gk"></div>
      <div><div><a href="/en/players/9260926b/Ibrahim-Amadou">Ibrahim Amadou</a></div></div>
      <div style="display: none;">&mdash; Goal</div>
    </div>
  </div>
  <div class="event b">
    <div>90ʼ<small><span>1:0</span></small></div>
    <div>
      <div class="event_icon brand_new_icon"></div>
      <div><div><a href="/en/players/bbbbbbbb/Some-Player">Some Player</a></div></div>
      <div style="display: none;">&mdash; Goal</div>
    </div>
  </div>
</div>
</body></html>
"""


class TestEventIconContract:
    """FBref hides a '— Goal' caption inside every event div, so the type must
    come from the icon: reading the markup instead credited the keeper-change
    of Strasbourg-Lille (94889482) as a Lille goal and broke the 3-0 score."""

    def _events(self):
        soup = BeautifulSoup(BECOMES_GK_EVENT_HTML, 'html.parser')
        return parse_events_from_scorebox(soup)

    def test_keeper_change_is_not_a_goal(self):
        df = self._events()
        amadou = df[df['player'] == 'Ibrahim Amadou'].iloc[0]
        assert amadou['event_type'] == 'becomes_gk'

    def test_real_goal_still_parses(self):
        df = self._events()
        martin = df[df['player'] == 'Jonas Martin'].iloc[0]
        assert martin['event_type'] == 'goal'
        assert martin['team_side'] == 'home'

    def test_unknown_icon_is_reported_not_guessed(self):
        df = self._events()
        unknown = df[df['player'] == 'Some Player'].iloc[0]
        assert unknown['event_type'] == 'unknown'
