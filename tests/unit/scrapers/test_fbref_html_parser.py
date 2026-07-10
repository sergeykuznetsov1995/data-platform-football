"""
Tests for FBref HTML parser functions.
"""

import pytest
import pandas as pd

from scrapers.fbref.html_parser import normalize_column_names, MULTIINDEX_PREFIXES


class TestNormalizeColumnNames:
    """Tests for normalize_column_names function."""

    def test_normalize_unnamed_prefix(self):
        """Test removing 'Unnamed: X_level_0_' prefix."""
        df = pd.DataFrame({
            'Unnamed: 0_level_0_Player': ['A', 'B'],
            'Unnamed: 1_level_0_Squad': ['Team1', 'Team2'],
        })

        result = normalize_column_names(df)

        assert 'Player' in result.columns
        assert 'Squad' in result.columns
        assert 'Unnamed: 0_level_0_Player' not in result.columns
        assert 'Unnamed: 1_level_0_Squad' not in result.columns

    def test_normalize_standard_prefix(self):
        """Test removing 'Standard_' prefix."""
        df = pd.DataFrame({
            'Standard_Gls': [10, 5],
            'Standard_Ast': [3, 2],
        })

        result = normalize_column_names(df)

        assert 'Gls' in result.columns
        assert 'Ast' in result.columns

    def test_normalize_shooting_prefix(self):
        """Test removing 'Shooting_' prefix."""
        df = pd.DataFrame({
            'Shooting_Sh': [50, 30],
            'Shooting_SoT': [20, 15],
            'Shooting_xG': [5.5, 3.2],
        })

        result = normalize_column_names(df)

        assert 'Sh' in result.columns
        assert 'SoT' in result.columns
        assert 'xG' in result.columns

    def test_normalize_passing_prefix(self):
        """Test removing 'Passing_' prefix."""
        df = pd.DataFrame({
            'Passing_Cmp': [500, 300],
            'Passing_Att': [600, 400],
            'Passing_Cmp%': [83.3, 75.0],
        })

        result = normalize_column_names(df)

        assert 'Cmp' in result.columns
        assert 'Att' in result.columns
        assert 'Cmp%' in result.columns

    def test_normalize_defensive_prefix(self):
        """Test removing 'Defensive Actions_' prefix."""
        df = pd.DataFrame({
            'Defensive Actions_Tkl': [30, 25],
            'Defensive Actions_Int': [15, 10],
        })

        result = normalize_column_names(df)

        assert 'Tkl' in result.columns
        assert 'Int' in result.columns

    def test_normalize_mixed_prefixes(self):
        """Test normalizing columns with different prefixes."""
        df = pd.DataFrame({
            'Unnamed: 0_level_0_Player': ['A'],
            'Standard_Squad': ['Team1'],
            'Shooting_Sh': [10],
            'Passing_Cmp': [100],
        })

        result = normalize_column_names(df)

        assert 'Player' in result.columns
        assert 'Squad' in result.columns
        assert 'Sh' in result.columns
        assert 'Cmp' in result.columns

    def test_normalize_already_clean_columns(self):
        """Test that already clean columns are not modified."""
        df = pd.DataFrame({
            'Player': ['A', 'B'],
            'Squad': ['Team1', 'Team2'],
            'Goals': [10, 5],
        })

        result = normalize_column_names(df)

        assert 'Player' in result.columns
        assert 'Squad' in result.columns
        assert 'Goals' in result.columns
        # Columns should be unchanged
        assert list(result.columns) == ['Player', 'Squad', 'Goals']

    def test_normalize_preserves_data(self):
        """Test that data is preserved after normalization."""
        df = pd.DataFrame({
            'Unnamed: 0_level_0_Player': ['Saka', 'Salah'],
            'Shooting_Sh': [50, 60],
        })

        result = normalize_column_names(df)

        assert result['Player'].tolist() == ['Saka', 'Salah']
        assert result['Sh'].tolist() == [50, 60]

    def test_normalize_empty_dataframe(self):
        """Test normalizing empty DataFrame."""
        df = pd.DataFrame()

        result = normalize_column_names(df)

        assert result.empty
        assert len(result.columns) == 0

    def test_all_known_prefixes_are_valid(self):
        """Test that all prefixes in MULTIINDEX_PREFIXES end with underscore."""
        for prefix in MULTIINDEX_PREFIXES:
            assert prefix.endswith('_'), f"Prefix '{prefix}' should end with '_'"


class TestNormalizeColumnNamesEdgeCases:
    """Edge case tests for normalize_column_names."""

    def test_similar_prefix_not_removed(self):
        """Test that similar but different prefixes are not removed."""
        df = pd.DataFrame({
            'StandardStats_Gls': [10],  # Not exactly 'Standard_'
        })

        result = normalize_column_names(df)

        # Should not be changed since 'StandardStats_' is not in our list
        assert 'StandardStats_Gls' in result.columns

    def test_only_first_matching_prefix_removed(self):
        """Test that only the first matching prefix is removed."""
        # This shouldn't happen in practice, but testing the behavior
        df = pd.DataFrame({
            'Unnamed: 0_level_0_Standard_Gls': [10],
        })

        result = normalize_column_names(df)

        # First matching prefix 'Unnamed: 0_level_0_' is removed
        assert 'Standard_Gls' in result.columns


class TestNormalizeColumnNamesIntegration:
    """Integration-like tests simulating real FBref data."""

    def test_realistic_stats_table(self):
        """Test with realistic FBref stats table columns."""
        df = pd.DataFrame({
            'Unnamed: 0_level_0_Player': ['Saka'],
            'Unnamed: 1_level_0_Nation': ['ENG'],
            'Unnamed: 2_level_0_Pos': ['FW'],
            'Unnamed: 3_level_0_Squad': ['Arsenal'],
            'Unnamed: 4_level_0_Age': [22],
            'Standard_Gls': [10],
            'Standard_Ast': [5],
            'Performance_xG': [8.5],
            'Expected_xA': [4.2],
        })

        result = normalize_column_names(df)

        expected_cols = [
            'Player', 'Nation', 'Pos', 'Squad', 'Age',
            'Gls', 'Ast', 'xG', 'xA'
        ]
        for col in expected_cols:
            assert col in result.columns, f"Expected column '{col}' not found"

    def test_realistic_shooting_table(self):
        """Test with realistic FBref shooting table columns."""
        df = pd.DataFrame({
            'Unnamed: 0_level_0_Player': ['Haaland'],
            'Standard_Squad': ['Man City'],
            'Shooting_Sh': [100],
            'Shooting_SoT': [50],
            'Shooting_SoT%': [50.0],
            'Expected_xG': [25.0],
            'Per 90 Minutes_Sh/90': [4.5],
        })

        result = normalize_column_names(df)

        assert 'Player' in result.columns
        assert 'Squad' in result.columns
        assert 'Sh' in result.columns
        assert 'SoT' in result.columns
        assert 'SoT%' in result.columns
        assert 'xG' in result.columns
        assert 'Sh/90' in result.columns


class TestNormalizeColumnNamesAdvanced:
    """Tests for advanced column normalization cases."""

    def test_normalize_level_x_patterns(self):
        """Test removing _level_X_ patterns from column names."""
        df = pd.DataFrame({
            'Performance_Gls_level_1': [10],
            'Something_level_0_Other': [5],
        })

        result = normalize_column_names(df)

        # Should remove the _level_X suffixes/prefixes
        assert 'Gls' in result.columns or 'Performance_Gls' in result.columns
        # The function should handle these edge cases

    def test_normalize_goalkeeper_prefixes(self):
        """Test removing goalkeeper-related prefixes."""
        df = pd.DataFrame({
            'Goalkeeping_GA': [20],
            'Keeper_CS': [10],
            'Penalty Kicks_PKsv': [3],
            'Goal Kicks_AvgLen': [45.5],
        })

        result = normalize_column_names(df)

        assert 'GA' in result.columns
        assert 'CS' in result.columns
        assert 'PKsv' in result.columns
        assert 'AvgLen' in result.columns

    def test_normalize_handles_duplicate_columns(self):
        """Test that duplicate columns get unique suffixes."""
        df = pd.DataFrame({
            'Standard_Gls': [10],
            'Expected_Gls': [8],  # Will become 'Gls' too after normalization
        })

        result = normalize_column_names(df)

        # After normalization, both would be 'Gls', so one should get a suffix
        assert len(result.columns) == 2
        # One should be 'Gls' and other 'Gls_1' or similar
        assert 'Gls' in result.columns

    def test_normalize_complex_multiindex_pattern(self):
        """Test normalizing complex nested MultiIndex patterns."""
        df = pd.DataFrame({
            'Unnamed: 5_level_0_Unnamed: 5_level_1_Player': ['Test'],
        })

        result = normalize_column_names(df)

        # Should be cleaned to just 'Player'
        assert 'Player' in result.columns or any('Player' in c for c in result.columns)

    def test_normalize_per90_prefix(self):
        """Test removing Per 90 Minutes_ and Per 90_ prefixes."""
        df = pd.DataFrame({
            'Per 90 Minutes_Gls': [0.5],
            'Per 90_Ast': [0.3],
        })

        result = normalize_column_names(df)

        assert 'Gls' in result.columns
        assert 'Ast' in result.columns

    def test_normalize_possession_subcategories(self):
        """Test removing possession subcategory prefixes."""
        df = pd.DataFrame({
            'Touches_Touches': [1500],
            'Take-Ons_Att': [50],
            'Carries_Carries': [800],
            'Receiving_Rec': [600],
        })

        result = normalize_column_names(df)

        assert 'Touches' in result.columns
        assert 'Att' in result.columns
        assert 'Carries' in result.columns
        assert 'Rec' in result.columns

    def test_normalize_gca_sca_prefixes(self):
        """Test removing GCA/SCA prefixes."""
        df = pd.DataFrame({
            'SCA Types_PassLive': [30],
            'GCA Types_PassLive': [10],
            'SCA_SCA': [50],
            'GCA_GCA': [15],
        })

        result = normalize_column_names(df)

        # At least the core stats should be extractable
        assert any('PassLive' in c for c in result.columns)

    def test_normalize_defensive_subcategories(self):
        """Test removing defensive subcategory prefixes."""
        df = pd.DataFrame({
            'Tackles_Tkl': [25],
            'Challenges_Att': [30],
            'Blocks_Blocks': [15],
            'Aerial Duels_Won': [40],
        })

        result = normalize_column_names(df)

        assert 'Tkl' in result.columns
        assert 'Blocks' in result.columns
        assert 'Won' in result.columns


class TestNormalizeColumnNamesLowercase:
    """Tests for case-insensitive column normalization."""

    def test_normalize_lowercase_with_spaces(self):
        """Test normalizing lowercase columns with spaces (from HTML)."""
        df = pd.DataFrame({
            'unnamed: 0_level_0_rk': [1],
            'playing time_mp': [10],
            'performance_gls': [5],
            'per 90 minutes_gls': [0.5],
        })

        result = normalize_column_names(df)

        assert 'Rk' in result.columns
        assert 'Mp' in result.columns
        assert 'Gls' in result.columns
        # per 90 minutes_gls should become Gls_1 due to duplicate
        assert 'Gls_1' in result.columns

    def test_normalize_mixed_case_prefixes(self):
        """Test normalizing columns with mixed case prefixes."""
        df = pd.DataFrame({
            'STANDARD_Gls': [10],
            'standard_ast': [5],
            'Shooting_sh': [20],
            'SHOOTING_SOT': [15],
        })

        result = normalize_column_names(df)

        assert 'Gls' in result.columns
        assert 'Ast' in result.columns
        assert 'Sh' in result.columns
        assert 'SOT' in result.columns

    def test_normalize_lowercase_unnamed_pattern(self):
        """Test normalizing lowercase 'unnamed:' pattern."""
        df = pd.DataFrame({
            'unnamed: 0_level_0_player': ['A'],
            'unnamed: 1_level_0_squad': ['Team1'],
        })

        result = normalize_column_names(df)

        assert 'Player' in result.columns
        assert 'Squad' in result.columns

    def test_normalize_lowercase_level_pattern(self):
        """Test normalizing lowercase '_level_X_' pattern."""
        df = pd.DataFrame({
            'something_level_0_player': ['A'],
            'OTHER_LEVEL_1_stat': [10],
        })

        result = normalize_column_names(df)

        assert 'Player' in result.columns
        assert 'Stat' in result.columns

    def test_capitalize_first_letter(self):
        """Test that first letter is capitalized after normalization."""
        df = pd.DataFrame({
            'performance_gls': [10],
            'standard_ast': [5],
            'shooting_shot': [3.5],
        })

        result = normalize_column_names(df)

        # First letter should be uppercase
        assert 'Gls' in result.columns
        assert 'Ast' in result.columns
        assert 'Shot' in result.columns

    def test_preserve_xg_pattern(self):
        """Test that xG-style names (lowercase+uppercase) are preserved."""
        df = pd.DataFrame({
            'performance_xG': [10],
            'standard_xA': [5],
            'expected_npxG': [3.5],
        })

        result = normalize_column_names(df)

        # xG, xA patterns should be preserved (not capitalized to XG, XA)
        assert 'xG' in result.columns
        assert 'xA' in result.columns
        # npxG should be capitalized to NpxG (lowercase first, next is lowercase)
        assert 'NpxG' in result.columns

    def test_preserve_already_capitalized(self):
        """Test that already capitalized names are preserved."""
        df = pd.DataFrame({
            'performance_Gls': [10],
            'standard_xG': [5],
        })

        result = normalize_column_names(df)

        assert 'Gls' in result.columns
        assert 'xG' in result.columns


class TestParserImports:
    """Test that new parser functions are importable."""

    def test_import_parse_shots_table(self):
        """Test that parse_shots_table can be imported."""
        from scrapers.fbref.html_parser import parse_shots_table
        assert callable(parse_shots_table)

    def test_import_parse_lineup_table(self):
        """Test that parse_lineup_table can be imported."""
        from scrapers.fbref.html_parser import parse_lineup_table
        assert callable(parse_lineup_table)

    def test_import_parse_events_from_scorebox(self):
        """Test that parse_events_from_scorebox can be imported."""
        from scrapers.fbref.html_parser import parse_events_from_scorebox
        assert callable(parse_events_from_scorebox)

    def test_import_parse_team_match_stats_table(self):
        """Test that parse_team_match_stats_table can be imported."""
        from scrapers.fbref.html_parser import parse_team_match_stats_table
        assert callable(parse_team_match_stats_table)

    def test_import_extract_player_ids_from_table(self):
        """Test that extract_player_ids_from_table can be imported."""
        from scrapers.fbref.html_parser import extract_player_ids_from_table
        assert callable(extract_player_ids_from_table)


class TestExtractPlayerIds:
    """Tests for extract_player_ids_from_table function."""

    def test_extract_player_ids_basic(self):
        """Test extracting player IDs from basic table structure."""
        from bs4 import BeautifulSoup
        from scrapers.fbref.html_parser import extract_player_ids_from_table

        html = """
        <table id="stats_standard">
            <tbody>
                <tr>
                    <td data-stat="player">
                        <a href="/en/players/b66315ae/Bukayo-Saka">Bukayo Saka</a>
                    </td>
                    <td data-stat="goals">10</td>
                </tr>
                <tr>
                    <td data-stat="player">
                        <a href="/players/d70ce98e/Mohamed-Salah">Mohamed Salah</a>
                    </td>
                    <td data-stat="goals">15</td>
                </tr>
            </tbody>
        </table>
        """

        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find('table')
        player_ids = extract_player_ids_from_table(table)

        assert len(player_ids) == 2
        assert player_ids[0] == 'b66315ae'
        assert player_ids[1] == 'd70ce98e'

    def test_extract_player_ids_skip_spacer_rows(self):
        """Test that spacer rows are skipped but thead rows are counted.

        pd.read_html behavior:
        - Skips empty spacer rows entirely
        - Includes thead class rows as data rows (they become rows with 'Player' in Player column)

        So our extraction must:
        - Skip spacer rows (don't count)
        - Count thead rows (to match pd.read_html indices) but don't extract player_id
        """
        from bs4 import BeautifulSoup
        from scrapers.fbref.html_parser import extract_player_ids_from_table

        html = """
        <table>
            <tbody>
                <tr class="spacer"><td></td></tr>
                <tr class="thead"><th>Header</th></tr>
                <tr>
                    <td data-stat="player">
                        <a href="/players/b66315ae/Player-Name">Player</a>
                    </td>
                </tr>
            </tbody>
        </table>
        """

        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find('table')
        player_ids = extract_player_ids_from_table(table)

        # Should only find 1 player_id
        # - spacer row: skipped (not counted)
        # - thead row: counted as index 0 (no player_id extracted)
        # - player row: counted as index 1 (player_id extracted)
        assert len(player_ids) == 1
        assert player_ids[1] == 'b66315ae'  # Index 1 - after thead row

    def test_extract_player_ids_empty_table(self):
        """Test extracting from empty table."""
        from bs4 import BeautifulSoup
        from scrapers.fbref.html_parser import extract_player_ids_from_table

        html = "<table><tbody></tbody></table>"
        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find('table')
        player_ids = extract_player_ids_from_table(table)

        assert len(player_ids) == 0

    def test_extract_player_ids_no_links(self):
        """Test extracting from table without player links."""
        from bs4 import BeautifulSoup
        from scrapers.fbref.html_parser import extract_player_ids_from_table

        html = """
        <table>
            <tbody>
                <tr><td>Player Name</td></tr>
                <tr><td>Another Player</td></tr>
            </tbody>
        </table>
        """

        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find('table')
        player_ids = extract_player_ids_from_table(table)

        assert len(player_ids) == 0

    def test_extract_player_ids_none_table(self):
        """Test extracting from None table."""
        from scrapers.fbref.html_parser import extract_player_ids_from_table

        player_ids = extract_player_ids_from_table(None)
        assert len(player_ids) == 0

    def test_extract_player_ids_fallback_to_any_player_link(self):
        """Test fallback to any /players/ link in row."""
        from bs4 import BeautifulSoup
        from scrapers.fbref.html_parser import extract_player_ids_from_table

        html = """
        <table>
            <tbody>
                <tr>
                    <td>
                        <a href="/en/players/abc12345/Some-Player">Link</a>
                    </td>
                </tr>
            </tbody>
        </table>
        """

        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find('table')
        player_ids = extract_player_ids_from_table(table)

        assert len(player_ids) == 1
        assert player_ids[0] == 'abc12345'

    def test_extract_player_ids_with_tfoot(self):
        """Test that player IDs are extracted from both tbody and tfoot.

        pd.read_html includes tfoot rows in the DataFrame, so
        extract_player_ids_from_table must also process tfoot.
        """
        from bs4 import BeautifulSoup
        from scrapers.fbref.html_parser import extract_player_ids_from_table

        html = """
        <table>
            <tbody>
                <tr>
                    <td data-stat="player">
                        <a href="/players/aaaaaaaa/Player-One">Player One</a>
                    </td>
                </tr>
                <tr>
                    <td data-stat="player">
                        <a href="/players/bbbbbbbb/Player-Two">Player Two</a>
                    </td>
                </tr>
            </tbody>
            <tfoot>
                <tr>
                    <td data-stat="player">
                        <a href="/players/cccccccc/Player-Three">Player Three</a>
                    </td>
                </tr>
            </tfoot>
        </table>
        """

        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find('table')
        player_ids = extract_player_ids_from_table(table)

        # Should find 3 players (2 from tbody + 1 from tfoot)
        assert len(player_ids) == 3
        assert player_ids[0] == 'aaaaaaaa'
        assert player_ids[1] == 'bbbbbbbb'
        assert player_ids[2] == 'cccccccc'

    def test_extract_player_ids_with_multiple_tbody(self):
        """Test that player IDs are extracted from multiple tbody elements.

        Some FBref tables have multiple <tbody> elements.
        pd.read_html includes all of them, so we must process all.
        """
        from bs4 import BeautifulSoup
        from scrapers.fbref.html_parser import extract_player_ids_from_table

        html = """
        <table>
            <tbody>
                <tr>
                    <td data-stat="player">
                        <a href="/players/aaaaaaaa/Player-One">Player One</a>
                    </td>
                </tr>
            </tbody>
            <tbody>
                <tr>
                    <td data-stat="player">
                        <a href="/players/bbbbbbbb/Player-Two">Player Two</a>
                    </td>
                </tr>
            </tbody>
            <tbody>
                <tr>
                    <td data-stat="player">
                        <a href="/players/cccccccc/Player-Three">Player Three</a>
                    </td>
                </tr>
            </tbody>
        </table>
        """

        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find('table')
        player_ids = extract_player_ids_from_table(table)

        # Should find 3 players from 3 tbody elements
        assert len(player_ids) == 3
        assert player_ids[0] == 'aaaaaaaa'
        assert player_ids[1] == 'bbbbbbbb'
        assert player_ids[2] == 'cccccccc'


class TestParseTableWithPlayerIds:
    """Tests for parse_table with extract_player_ids option."""

    def test_parse_table_with_player_ids(self):
        """Test parsing table with player ID extraction."""
        from bs4 import BeautifulSoup
        from scrapers.fbref.html_parser import parse_table

        html = """
        <html>
        <table id="test_table">
            <thead>
                <tr><th>Player</th><th>Goals</th></tr>
            </thead>
            <tbody>
                <tr>
                    <td data-stat="player">
                        <a href="/players/b66315ae/Bukayo-Saka">Bukayo Saka</a>
                    </td>
                    <td>10</td>
                </tr>
                <tr>
                    <td data-stat="player">
                        <a href="/players/d70ce98e/Mohamed-Salah">Mohamed Salah</a>
                    </td>
                    <td>15</td>
                </tr>
            </tbody>
        </table>
        </html>
        """

        soup = BeautifulSoup(html, 'html.parser')
        df = parse_table(soup, 'test_table', extract_player_ids=True)

        assert df is not None
        assert 'player_id' in df.columns
        assert len(df) == 2
        # Note: exact values depend on row indexing after pandas parsing
        assert df['player_id'].notna().sum() >= 1

    def test_parse_table_without_player_ids(self):
        """Test parsing table without player ID extraction."""
        from bs4 import BeautifulSoup
        from scrapers.fbref.html_parser import parse_table

        html = """
        <html>
        <table id="test_table">
            <thead>
                <tr><th>Player</th><th>Goals</th></tr>
            </thead>
            <tbody>
                <tr><td>Player A</td><td>10</td></tr>
                <tr><td>Player B</td><td>15</td></tr>
            </tbody>
        </table>
        </html>
        """

        soup = BeautifulSoup(html, 'html.parser')
        df = parse_table(soup, 'test_table', extract_player_ids=False)

        assert df is not None
        assert 'player_id' not in df.columns


class TestParseTableElement:
    """Tests for _parse_table_element helper function."""

    def test_parse_table_element_basic(self):
        """Test parsing table element directly."""
        from bs4 import BeautifulSoup
        from scrapers.fbref.html_parser import _parse_table_element

        html = """
        <table>
            <thead>
                <tr><th>Player</th><th>Goals</th></tr>
            </thead>
            <tbody>
                <tr><td>Player A</td><td>10</td></tr>
                <tr><td>Player B</td><td>15</td></tr>
            </tbody>
        </table>
        """

        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find('table')
        df = _parse_table_element(table)

        assert df is not None
        assert len(df) == 2
        assert 'Player' in df.columns
        assert 'Goals' in df.columns

    def test_parse_table_element_with_player_ids(self):
        """Test parsing table element with player ID extraction."""
        from bs4 import BeautifulSoup
        from scrapers.fbref.html_parser import _parse_table_element

        html = """
        <table>
            <thead>
                <tr><th>Player</th><th>Goals</th></tr>
            </thead>
            <tbody>
                <tr>
                    <td data-stat="player">
                        <a href="/players/abc12345/Test-Player">Test Player</a>
                    </td>
                    <td>10</td>
                </tr>
            </tbody>
        </table>
        """

        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find('table')
        df = _parse_table_element(table, extract_player_ids=True)

        assert df is not None
        assert 'player_id' in df.columns

    def test_parse_table_element_multiindex(self):
        """Test parsing table with MultiIndex headers."""
        from bs4 import BeautifulSoup
        from scrapers.fbref.html_parser import _parse_table_element

        html = """
        <table>
            <thead>
                <tr><th colspan="2">Standard</th></tr>
                <tr><th>Player</th><th>Goals</th></tr>
            </thead>
            <tbody>
                <tr><td>Player A</td><td>10</td></tr>
            </tbody>
        </table>
        """

        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find('table')
        df = _parse_table_element(table)

        assert df is not None
        assert len(df) >= 1


class TestTableHasPlayerHeader:
    """Tests for _table_has_player_header helper function."""

    def test_table_has_player_header_true(self):
        """Test detecting table with Player header."""
        from bs4 import BeautifulSoup
        from scrapers.fbref.html_parser import _table_has_player_header

        html = """
        <table>
            <thead>
                <tr><th>Player</th><th>Goals</th></tr>
            </thead>
            <tbody></tbody>
        </table>
        """

        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find('table')

        assert _table_has_player_header(table) is True

    def test_table_has_player_header_false(self):
        """Test detecting table without Player header."""
        from bs4 import BeautifulSoup
        from scrapers.fbref.html_parser import _table_has_player_header

        html = """
        <table>
            <thead>
                <tr><th>Team</th><th>Goals</th></tr>
            </thead>
            <tbody></tbody>
        </table>
        """

        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find('table')

        assert _table_has_player_header(table) is False

    def test_table_has_player_header_case_insensitive(self):
        """Test that Player header detection is case-insensitive."""
        from bs4 import BeautifulSoup
        from scrapers.fbref.html_parser import _table_has_player_header

        html = """
        <table>
            <thead>
                <tr><th>PLAYER</th><th>Goals</th></tr>
            </thead>
            <tbody></tbody>
        </table>
        """

        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find('table')

        assert _table_has_player_header(table) is True

    def test_table_has_player_header_no_thead(self):
        """Test table without thead element."""
        from bs4 import BeautifulSoup
        from scrapers.fbref.html_parser import _table_has_player_header

        html = """
        <table>
            <tbody>
                <tr><td>Player A</td><td>10</td></tr>
            </tbody>
        </table>
        """

        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find('table')

        assert _table_has_player_header(table) is False


class TestFindPlayerStatsTableFallback:
    """Tests for find_player_stats_table fallback logic."""

    def test_generic_class_table_is_not_returned(self):
        """A CSS class cannot prove which stat_type the table contains."""
        from bs4 import BeautifulSoup
        from scrapers.fbref.html_parser import find_player_stats_table

        html = """
        <html>
        <table class="stats_table">
            <thead>
                <tr><th>Player</th><th>Goals</th></tr>
            </thead>
            <tbody>
                <tr><td>Player A</td><td>10</td></tr>
            </tbody>
        </table>
        </html>
        """

        soup = BeautifulSoup(html, 'html.parser')
        df = find_player_stats_table(soup, {}, 'standard')

        assert df is None

    def test_bare_player_header_table_is_not_returned(self, caplog):
        """A bare table with only a Player header must NOT be returned.

        The old last-resort fallback ("any table with a Player header")
        silently filed the WRONG stat table into bronze when FBref changed
        layout — now the finder fails loudly with an ERROR instead.
        """
        import logging

        from bs4 import BeautifulSoup
        from scrapers.fbref.html_parser import find_player_stats_table

        html = """
        <html>
        <table>
            <thead>
                <tr><th>Player</th><th>Goals</th></tr>
            </thead>
            <tbody>
                <tr><td>Player A</td><td>10</td></tr>
            </tbody>
        </table>
        </html>
        """

        soup = BeautifulSoup(html, 'html.parser')
        with caplog.at_level(logging.ERROR):
            df = find_player_stats_table(soup, {}, 'standard')

        assert df is None
        assert any('No player stats table found' in r.message
                   for r in caplog.records)

    def test_find_in_comment_tables(self):
        """Test finding stats table from comment tables."""
        from bs4 import BeautifulSoup
        from scrapers.fbref.html_parser import find_player_stats_table

        # Main HTML without stats table
        main_html = "<html><body><p>No tables here</p></body></html>"
        soup = BeautifulSoup(main_html, 'html.parser')

        # Comment table with stats
        comment_html = """
        <table id="stats_shooting">
            <thead>
                <tr><th>Player</th><th>Shots</th></tr>
            </thead>
            <tbody>
                <tr><td>Player A</td><td>50</td></tr>
            </tbody>
        </table>
        """
        comment_soup = BeautifulSoup(comment_html, 'html.parser')
        comment_table = comment_soup.find('table')

        comment_tables = {'stats_shooting': comment_table}

        df = find_player_stats_table(soup, comment_tables, 'shooting')

        assert df is not None
        assert len(df) == 1


class TestExtractTeamIds:
    """Tests for extract_team_ids_from_table function."""

    def test_extract_team_ids_basic(self):
        """Test basic team ID extraction from table."""
        from bs4 import BeautifulSoup
        from scrapers.fbref.html_parser import extract_team_ids_from_table

        html = """
        <table>
            <tbody>
                <tr>
                    <td data-stat="squad">
                        <a href="/squads/18bb7c10/Arsenal-Stats">Arsenal</a>
                    </td>
                    <td>50</td>
                </tr>
                <tr>
                    <td data-stat="squad">
                        <a href="/squads/b8fd03ef/Manchester-City-Stats">Manchester City</a>
                    </td>
                    <td>60</td>
                </tr>
            </tbody>
        </table>
        """

        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find('table')
        team_ids = extract_team_ids_from_table(table)

        assert len(team_ids) == 2
        assert team_ids[0] == '18bb7c10'
        assert team_ids[1] == 'b8fd03ef'

    def test_extract_team_ids_with_team_stat(self):
        """Test team ID extraction with data-stat='team'."""
        from bs4 import BeautifulSoup
        from scrapers.fbref.html_parser import extract_team_ids_from_table

        html = """
        <table>
            <tbody>
                <tr>
                    <td data-stat="team">
                        <a href="/en/squads/cff3d9bb/Chelsea-Stats">Chelsea</a>
                    </td>
                </tr>
            </tbody>
        </table>
        """

        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find('table')
        team_ids = extract_team_ids_from_table(table)

        assert len(team_ids) == 1
        assert team_ids[0] == 'cff3d9bb'

    def test_extract_team_ids_skip_spacer_rows(self):
        """Test that spacer rows are skipped for team ID extraction."""
        from bs4 import BeautifulSoup
        from scrapers.fbref.html_parser import extract_team_ids_from_table

        html = """
        <table>
            <tbody>
                <tr class="spacer"><td></td></tr>
                <tr class="thead"><th>Squad</th></tr>
                <tr>
                    <td data-stat="squad">
                        <a href="/squads/18bb7c10/Arsenal">Arsenal</a>
                    </td>
                </tr>
            </tbody>
        </table>
        """

        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find('table')
        team_ids = extract_team_ids_from_table(table)

        # spacer skipped, thead counted but no ID, data row at index 1
        assert len(team_ids) == 1
        assert team_ids[1] == '18bb7c10'

    def test_extract_team_ids_fallback_to_any_link(self):
        """Test fallback to any /squads/ link in row."""
        from bs4 import BeautifulSoup
        from scrapers.fbref.html_parser import extract_team_ids_from_table

        html = """
        <table>
            <tbody>
                <tr>
                    <td>Some text</td>
                    <td><a href="/squads/361ca564/Tottenham-Stats">Spurs</a></td>
                </tr>
            </tbody>
        </table>
        """

        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find('table')
        team_ids = extract_team_ids_from_table(table)

        assert len(team_ids) == 1
        assert team_ids[0] == '361ca564'

    def test_extract_team_ids_empty_table(self):
        """Test extracting from empty table."""
        from bs4 import BeautifulSoup
        from scrapers.fbref.html_parser import extract_team_ids_from_table

        html = "<table><tbody></tbody></table>"
        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find('table')
        team_ids = extract_team_ids_from_table(table)

        assert len(team_ids) == 0

    def test_extract_team_ids_none_table(self):
        """Test extracting from None table."""
        from scrapers.fbref.html_parser import extract_team_ids_from_table

        team_ids = extract_team_ids_from_table(None)
        assert len(team_ids) == 0


class TestParseTableWithTeamIds:
    """Tests for parse_table with extract_team_ids option."""

    def test_parse_table_with_team_ids(self):
        """Test parsing table with team ID extraction."""
        from bs4 import BeautifulSoup
        from scrapers.fbref.html_parser import parse_table

        html = """
        <html>
        <table id="stats_squads_standard_for">
            <thead>
                <tr><th>Squad</th><th>Goals</th></tr>
            </thead>
            <tbody>
                <tr>
                    <td data-stat="squad">
                        <a href="/squads/18bb7c10/Arsenal">Arsenal</a>
                    </td>
                    <td>50</td>
                </tr>
                <tr>
                    <td data-stat="squad">
                        <a href="/squads/b8fd03ef/Man-City">Man City</a>
                    </td>
                    <td>60</td>
                </tr>
            </tbody>
        </table>
        </html>
        """

        soup = BeautifulSoup(html, 'html.parser')
        df = parse_table(soup, 'stats_squads_standard_for', extract_team_ids=True)

        assert df is not None
        assert 'team_id' in df.columns
        assert len(df) == 2
        assert df['team_id'].notna().sum() == 2


class TestParseScheduleRepeatedHeaders:
    """Repeated 'Home'/'Away' header rows must not leak into Bronze (issue #189)."""

    SCHEDULE_HTML = """
    <html>
    <table id="sched_2025-2026_9_1">
        <thead>
            <tr><th>Wk</th><th>Home</th><th>Score</th><th>Away</th></tr>
        </thead>
        <tbody>
            <tr><td>1</td><td>Arsenal</td><td>2–1</td><td>Chelsea</td></tr>
            <!-- FBref repeats the header row mid-table for in-progress seasons -->
            <tr><td>Wk</td><td>Home</td><td>Score</td><td>Away</td></tr>
            <tr><td>2</td><td>Liverpool</td><td>0–0</td><td>Everton</td></tr>
        </tbody>
    </table>
    </html>
    """

    def test_parse_table_drops_home_header_rows(self):
        from bs4 import BeautifulSoup
        from scrapers.fbref.html_parser import parse_table

        soup = BeautifulSoup(self.SCHEDULE_HTML, 'html.parser')
        df = parse_table(soup, 'sched_2025-2026_9_1')

        assert df is not None
        # The repeated header row (Home='Home', Away='Away') must be removed.
        assert 'Home' not in df['Home'].values
        assert 'Away' not in df['Away'].values
        assert len(df) == 2
        assert set(df['Home']) == {'Arsenal', 'Liverpool'}

    def test_parse_table_element_drops_home_header_rows(self):
        from bs4 import BeautifulSoup
        from scrapers.fbref.html_parser import _parse_table_element

        soup = BeautifulSoup(self.SCHEDULE_HTML, 'html.parser')
        table = soup.find('table')
        df = _parse_table_element(table)

        assert df is not None
        assert 'Home' not in df['Home'].values
        assert len(df) == 2


class TestParseScheduleBlankRows:
    """FBref's blank separator rows must not reach Bronze (issue #892).

    Before the fix they landed in bronze.fbref_schedule as 2146 all-NULL rows
    (~10% of the table). A *future* fixture — no score, no match report — is
    not blank and must survive.
    """

    SCHEDULE_HTML = """
    <html>
    <table id="sched_2025-2026_9_1">
        <thead>
            <tr><th>Wk</th><th>Home</th><th>Score</th><th>Away</th></tr>
        </thead>
        <tbody>
            <tr><td>1</td><td>Arsenal</td><td>2–1</td><td>Chelsea</td></tr>
            <!-- FBref's blank separator row between gameweeks -->
            <tr><td></td><td></td><td></td><td></td></tr>
            <tr><td>2</td><td>Liverpool</td><td></td><td>Everton</td></tr>
        </tbody>
    </table>
    </html>
    """

    def test_parse_table_drops_blank_rows_but_keeps_future_fixture(self):
        from bs4 import BeautifulSoup
        from scrapers.fbref.html_parser import parse_table

        soup = BeautifulSoup(self.SCHEDULE_HTML, 'html.parser')
        df = parse_table(soup, 'sched_2025-2026_9_1')

        assert df is not None
        assert len(df) == 2
        # The unplayed Liverpool–Everton fixture has no score, but it is not blank.
        assert set(df['Home']) == {'Arsenal', 'Liverpool'}

    def test_parse_table_element_drops_blank_rows(self):
        from bs4 import BeautifulSoup
        from scrapers.fbref.html_parser import _parse_table_element

        soup = BeautifulSoup(self.SCHEDULE_HTML, 'html.parser')
        table = soup.find('table')
        df = _parse_table_element(table)

        assert df is not None
        assert len(df) == 2

    def test_drop_blank_rows_is_a_noop_on_clean_frames(self):
        from scrapers.fbref.html_parser import drop_blank_rows

        df = pd.DataFrame({'Home': ['Arsenal'], 'Score': [None]})
        out = drop_blank_rows(df)

        assert len(out) == 1


class TestExtractMatchUrlsFromSchedule:
    """match_url must stay aligned with the right fixture even when the
    schedule HTML interleaves spacer rows, repeated header rows, and future
    (link-less) rows. Misalignment is the root cause of issue #241 — it made
    FBref emit the same fixture under an alternate match-page hex.
    """

    # tbody order: 2 played matches, a spacer row, a repeated header row,
    # another played match, and a future match (Head-to-Head link, NOT a
    # match-report). pd.read_html keeps every tbody row (spacer/header as
    # NaN/text), so the extractor must count them all to stay aligned.
    SCHEDULE_HTML = """
    <html>
    <table id="sched_all">
        <thead>
            <tr><th>Wk</th><th>Home</th><th>Score</th><th>Away</th><th>Match Report</th></tr>
        </thead>
        <tbody>
            <tr><td>1</td><td>Arsenal</td><td>2-1</td><td>Chelsea</td>
                <td><a href="/en/matches/aaaaaaaa/Arsenal-Chelsea">Match Report</a></td></tr>
            <tr><td>1</td><td>Spurs</td><td>0-0</td><td>Everton</td>
                <td><a href="/en/matches/bbbbbbbb/Spurs-Everton">Match Report</a></td></tr>
            <tr class="spacer"><td colspan="5"></td></tr>
            <tr><td>Wk</td><td>Home</td><td>Score</td><td>Away</td><td>Match Report</td></tr>
            <tr><td>2</td><td>Liverpool</td><td>3-1</td><td>Leeds</td>
                <td><a href="/en/matches/cccccccc/Liverpool-Leeds">Match Report</a></td></tr>
            <tr><td>3</td><td>City</td><td></td><td>United</td>
                <td><a href="/en/matches/2026-05-01">Head-to-Head</a></td></tr>
        </tbody>
    </table>
    </html>
    """

    def _table(self):
        from bs4 import BeautifulSoup
        return BeautifulSoup(self.SCHEDULE_HTML, 'html.parser').find('table')

    def test_indices_align_with_pandas_rows(self):
        """data_row_idx must count spacer/header rows (pd.read_html keeps them)."""
        from scrapers.fbref.html_parser import extract_match_urls_from_schedule

        match_urls = extract_match_urls_from_schedule(self._table())

        # row 0 Arsenal, row 1 Spurs, row 2 spacer (none), row 3 header (none),
        # row 4 Liverpool, row 5 future Head-to-Head (none).
        assert match_urls == {
            0: '/en/matches/aaaaaaaa/Arsenal-Chelsea',
            1: '/en/matches/bbbbbbbb/Spurs-Everton',
            4: '/en/matches/cccccccc/Liverpool-Leeds',
        }

    def test_none_table(self):
        from scrapers.fbref.html_parser import extract_match_urls_from_schedule

        assert extract_match_urls_from_schedule(None) == {}

    def test_parse_table_maps_url_to_correct_fixture(self):
        """Regression guard for #241: each surviving row keeps ITS OWN url."""
        from bs4 import BeautifulSoup
        from scrapers.fbref.html_parser import parse_table

        soup = BeautifulSoup(self.SCHEDULE_HTML, 'html.parser')
        df = parse_table(soup, 'sched_all', extract_match_urls=True)

        assert df is not None
        assert 'match_url' in df.columns
        url_by_home = dict(zip(df['Home'], df['match_url']))
        assert url_by_home['Arsenal'] == '/en/matches/aaaaaaaa/Arsenal-Chelsea'
        assert url_by_home['Spurs'] == '/en/matches/bbbbbbbb/Spurs-Everton'
        assert url_by_home['Liverpool'] == '/en/matches/cccccccc/Liverpool-Leeds'
        # Future match (Head-to-Head, no match report) must NOT receive a hex url.
        assert pd.isna(url_by_home['City'])
        # Repeated header row dropped; no skeleton/duplicate hex leaked in.
        assert 'Home' not in df['Home'].values

    def test_find_schedule_table_populates_match_url(self):
        """End-to-end: find_schedule_table wires extract_match_urls=True."""
        from bs4 import BeautifulSoup
        from scrapers.fbref.parsers.finders import find_schedule_table

        soup = BeautifulSoup(self.SCHEDULE_HTML, 'html.parser')
        df = find_schedule_table(soup, {}, '2025-2026', '9')

        assert df is not None
        assert 'match_url' in df.columns
        url_by_home = dict(zip(df['Home'], df['match_url']))
        assert url_by_home['Liverpool'] == '/en/matches/cccccccc/Liverpool-Leeds'
        assert pd.isna(url_by_home['City'])
