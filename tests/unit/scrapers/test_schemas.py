"""
Tests for schema definitions.
"""

import pytest
import pyarrow as pa


class TestCommonSchemas:
    """Tests for common schema definitions."""

    def test_metadata_fields(self):
        """Test metadata fields are defined."""
        from scrapers.schemas.common import METADATA_FIELDS

        field_names = [f.name for f in METADATA_FIELDS]

        assert '_source' in field_names
        assert '_ingested_at' in field_names
        assert '_batch_id' in field_names

    def test_create_schema_with_metadata(self):
        """Test creating schema with metadata."""
        from scrapers.schemas.common import create_schema_with_metadata

        fields = [
            pa.field('col1', pa.int32()),
            pa.field('col2', pa.string()),
        ]

        schema = create_schema_with_metadata(fields)

        field_names = [f.name for f in schema]
        assert 'col1' in field_names
        assert 'col2' in field_names
        assert '_source' in field_names

    def test_normalize_league_name(self):
        """Test league name normalization."""
        from scrapers.schemas.common import normalize_league_name

        assert normalize_league_name('Premier League') == 'ENG-Premier League'
        assert normalize_league_name('La Liga') == 'ESP-La Liga'
        assert normalize_league_name('Unknown League') == 'Unknown League'

    def test_format_season(self):
        """Test season formatting."""
        from scrapers.schemas.common import format_season

        assert format_season(2023) == '2023-24'
        assert format_season(2024) == '2024-25'

    def test_parse_season(self):
        """Test season parsing."""
        from scrapers.schemas.common import parse_season

        assert parse_season('2023-24') == 2023
        assert parse_season(2024) == 2024


class TestFBrefSchemas:
    """Tests for FBref schema definitions."""

    def test_schedule_schema(self):
        """Test FBref schedule schema."""
        from scrapers.schemas.fbref import FBREF_SCHEDULE_SCHEMA

        field_names = [f.name for f in FBREF_SCHEDULE_SCHEMA]

        assert 'league' in field_names
        assert 'season' in field_names
        assert 'home_team' in field_names
        assert 'away_team' in field_names
        assert 'home_goals' in field_names
        assert 'home_xg' in field_names

    def test_player_stats_schema(self):
        """Test FBref player stats schema."""
        from scrapers.schemas.fbref import FBREF_PLAYER_STATS_SCHEMA

        field_names = [f.name for f in FBREF_PLAYER_STATS_SCHEMA]

        assert 'player' in field_names
        assert 'team' in field_names
        assert 'goals' in field_names
        assert 'assists' in field_names
        assert 'xg' in field_names
        assert 'xa' in field_names

    def test_team_stats_schema(self):
        """Test FBref team stats schema."""
        from scrapers.schemas.fbref import FBREF_TEAM_STATS_SCHEMA

        field_names = [f.name for f in FBREF_TEAM_STATS_SCHEMA]

        assert 'team' in field_names
        assert 'wins' in field_names
        assert 'losses' in field_names
        assert 'points' in field_names


class TestUnderstatSchemas:
    """Tests for Understat schema definitions."""

    def test_shots_schema(self):
        """Test Understat shots schema."""
        from scrapers.schemas.understat import UNDERSTAT_SHOTS_SCHEMA

        field_names = [f.name for f in UNDERSTAT_SHOTS_SCHEMA]

        assert 'x' in field_names
        assert 'y' in field_names
        assert 'xg' in field_names
        assert 'result' in field_names
        assert 'situation' in field_names
        assert 'shot_type' in field_names

    def test_players_schema(self):
        """Test Understat players schema."""
        from scrapers.schemas.understat import UNDERSTAT_PLAYERS_SCHEMA

        field_names = [f.name for f in UNDERSTAT_PLAYERS_SCHEMA]

        assert 'player' in field_names
        assert 'xg' in field_names
        assert 'xa' in field_names
        assert 'xg_chain' in field_names


class TestWhoScoredSchemas:
    """Tests for WhoScored schema definitions."""

    def test_spadl_events_schema(self):
        """Test WhoScored SPADL events schema."""
        from scrapers.schemas.whoscored import WHOSCORED_EVENTS_SPADL_SCHEMA

        field_names = [f.name for f in WHOSCORED_EVENTS_SPADL_SCHEMA]

        assert 'game_id' in field_names
        assert 'period_id' in field_names
        assert 'time_seconds' in field_names
        assert 'start_x' in field_names
        assert 'end_x' in field_names
        assert 'action_type' in field_names
        assert 'result' in field_names
        assert 'bodypart' in field_names

    def test_spadl_action_types(self):
        """Test SPADL action types are defined."""
        from scrapers.schemas.whoscored import SPADL_ACTION_TYPES

        assert 'pass' in SPADL_ACTION_TYPES
        assert 'shot' in SPADL_ACTION_TYPES
        assert 'tackle' in SPADL_ACTION_TYPES
        assert 'dribble' in SPADL_ACTION_TYPES

    def test_spadl_result_types(self):
        """Test SPADL result types are defined."""
        from scrapers.schemas.whoscored import SPADL_RESULT_TYPES

        assert 'success' in SPADL_RESULT_TYPES
        assert 'fail' in SPADL_RESULT_TYPES

    def test_spadl_bodyparts(self):
        """Test SPADL body parts are defined."""
        from scrapers.schemas.whoscored import SPADL_BODYPARTS

        assert 'foot' in SPADL_BODYPARTS
        assert 'head' in SPADL_BODYPARTS
