from scrapers.fbref.discovery import (
    CalendarType,
    CompetitionFormat,
    CompetitionGender,
    CompetitionRef,
    ParticipantType,
    SeasonRef,
    parse_competition_html,
    parse_competition_index_html,
    parse_schedule_html,
    parse_season_html,
)
from scrapers.fbref.match_parser import DatasetStatus


def _competition(
    *,
    comp_id="9",
    competition_format=CompetitionFormat.LEAGUE,
    participants=ParticipantType.CLUB,
):
    return CompetitionRef(
        comp_id=comp_id,
        name="Test Competition",
        format=competition_format,
        participants=participants,
        gender=CompetitionGender.MALE,
        source_section="Domestic Leagues - 1st Tier",
        country="eng ENG",
        governing_body=None,
        tier="1st",
        first_season="2020-2021",
        last_season="2025-2026",
        history_url=(
            f"https://fbref.com/en/comps/{comp_id}/history/Test-Seasons"
        ),
    )


def _season(comp_id="9", season_id="2025-2026"):
    return SeasonRef(
        comp_id=comp_id,
        season_id=season_id,
        label="2025-2026",
        calendar_type=CalendarType.SPLIT_YEAR,
        season_url=(
            f"https://fbref.com/en/comps/{comp_id}/{season_id}/Test-Stats"
        ),
    )


def test_competition_index_classifies_categories_and_category_beats_popular():
    html = """
    <h2>Popular Competitions</h2>
    <table><tbody><tr>
      <td data-stat="comp_name"><a href="/en/comps/9/history/Popular-Name-Seasons">Popular Name</a></td>
      <td data-stat="gender">M</td>
    </tr></tbody></table>

    <h2>Domestic Leagues - 1st Tier</h2>
    <table id="domestic_leagues"><tbody><tr>
      <td data-stat="comp_name"><a href="/en/comps/9/history/Premier-League-Seasons">Premier League</a></td>
      <td data-stat="gender">M</td><td data-stat="country">eng ENG</td>
      <td data-stat="first_season">1888-1889</td>
      <td data-stat="last_season">2025-2026</td><td data-stat="tier">1st</td>
    </tr><tr>
      <td data-stat="comp"><a href="/en/comps/189/history/Womens-Super-League-Seasons">Women's Super League</a></td>
      <td data-stat="gender">F</td><td data-stat="country">eng ENG</td>
      <td data-stat="minseason">2017</td><td data-stat="maxseason">2025-2026</td>
    </tr></tbody></table>

    <h2>Club International Cups</h2>
    <table><tbody><tr>
      <td data-stat="comp_name"><a href="https://www.fbref.com/en/comps/8/history/Champions-League-Seasons">Champions League</a></td>
      <td data-stat="gender">M</td><td data-stat="governing_body">UEFA</td>
    </tr></tbody></table>

    <h2>National Team Competitions</h2>
    <table><tbody><tr>
      <td data-stat="comp_name"><a href="/en/comps/1/history/World-Cup-Seasons">World Cup</a></td>
      <td data-stat="gender">M</td><td data-stat="governing_body">FIFA</td>
    </tr></tbody></table>
    """

    result = parse_competition_index_html(html)

    assert result.status == DatasetStatus.AVAILABLE
    assert not result.has_errors
    records = {
        item.comp_id: item for item in result.datasets["competitions"].records
    }
    assert set(records) == {"1", "8", "9", "189"}
    assert records["9"].name == "Premier League"
    assert records["9"].source_section == "Domestic Leagues - 1st Tier"
    assert records["9"].format == CompetitionFormat.LEAGUE
    assert records["9"].participants == ParticipantType.CLUB
    assert records["9"].country == "eng ENG"
    assert records["9"].first_season == "1888-1889"
    assert records["189"].gender == CompetitionGender.FEMALE
    assert records["8"].format == CompetitionFormat.CUP
    assert records["8"].participants == ParticipantType.CLUB
    assert records["8"].governing_body == "UEFA"
    assert records["1"].format == CompetitionFormat.OTHER
    assert records["1"].participants == ParticipantType.NATIONAL_TEAM
    assert records["8"].history_url == (
        "https://fbref.com/en/comps/8/history/Champions-League-Seasons"
    )


def test_competition_index_reads_a_table_hidden_in_an_html_comment():
    html = """
    <h1>Competitions</h1>
    <!--
      <h2>Domestic Cups</h2>
      <table><tr>
        <td data-stat="comp_name"><a href="/en/comps/123/history/Test-Cup-Seasons">Test Cup</a></td>
        <td data-stat="gender">F</td><td data-stat="country">xx TST</td>
      </tr></table>
    -->
    """

    result = parse_competition_index_html(html)
    record = result.datasets["competitions"].records[0]

    assert record.comp_id == "123"
    assert record.format == CompetitionFormat.CUP
    assert record.participants == ParticipantType.CLUB
    assert record.gender == CompetitionGender.FEMALE


def test_competition_history_keeps_exact_urls_and_source_native_season_ids():
    html = """
    <table id="seasons"><tbody>
      <tr><th data-stat="year_id"><a href="/en/comps/9/2025-2026/2025-2026-Premier-League-Stats">2025-2026</a></th></tr>
      <tr><th data-stat="year_id"><a href="/en/comps/9/2025/2025-Test-League-Stats">2025</a></th></tr>
      <tr><th data-stat="year_id"><a href="/en/comps/9/edition-42/source-owned-slug">2024-2025</a></th></tr>
      <tr><th data-stat="year_id"><a href="/en/comps/9/Premier-League-Stats">2026-2027</a></th></tr>
    </tbody></table>
    """

    result = parse_competition_html(html, _competition())

    assert result.status == DatasetStatus.AVAILABLE
    seasons = result.datasets["seasons"].records
    assert [item.season_id for item in seasons] == [
        "2025-2026",
        "2025",
        "edition-42",
        "2026-2027",
    ]
    assert [item.calendar_type for item in seasons] == [
        CalendarType.SPLIT_YEAR,
        CalendarType.SINGLE_YEAR,
        CalendarType.SPLIT_YEAR,
        CalendarType.SPLIT_YEAR,
    ]
    assert seasons[2].season_url == (
        "https://fbref.com/en/comps/9/edition-42/source-owned-slug"
    )


def test_cup_and_national_team_seasons_are_tournaments_even_with_opaque_labels():
    html = """
    <table id="seasons"><tbody><tr>
      <th data-stat="season"><a href="/en/comps/1/edition-42/source-slug">Edition XLII</a></th>
    </tr></tbody></table>
    """
    cup = _competition(
        comp_id="1",
        competition_format=CompetitionFormat.CUP,
        participants=ParticipantType.NATIONAL_TEAM,
    )

    result = parse_competition_html(html, cup)
    season = result.datasets["seasons"].records[0]

    assert result.status == DatasetStatus.AVAILABLE
    assert season.season_id == "edition-42"
    assert season.calendar_type == CalendarType.TOURNAMENT


def test_unknown_league_season_label_is_an_explicit_error():
    html = """
    <table id="seasons"><tr>
      <th data-stat="season"><a href="/en/comps/9/spring/source-slug">Spring Edition</a></th>
    </tr></table>
    """

    result = parse_competition_html(html, _competition())
    dataset = result.datasets["seasons"]

    assert result.has_errors
    assert dataset.status == DatasetStatus.ERROR
    assert dataset.reason == "season_row_parse_failed"
    assert dataset.error_type == "SeasonDiscoveryError"
    assert "Unsupported season label" in dataset.error_message


def test_season_parser_uses_the_exact_discovered_schedule_href():
    html = """
    <nav>
      <a href="/en/comps/8/2024-2025/stats/noise">Stats</a>
      <a href="/en/comps/999/2024-2025/schedule/wrong-comp">Wrong</a>
      <a href="/en/comps/8/opaque-edition/schedule/source-owned-fixtures-slug?view=all#scores">Scores &amp; Fixtures</a>
    </nav>
    """
    season = SeasonRef(
        comp_id="8",
        season_id="opaque-edition",
        label="2024-2025",
        calendar_type=CalendarType.TOURNAMENT,
        season_url="https://fbref.com/en/comps/8/opaque-edition/source-stats",
    )

    result = parse_season_html(html, season)
    schedule = result.datasets["schedules"].records[0]

    assert result.status == DatasetStatus.AVAILABLE
    assert schedule.schedule_url == (
        "https://fbref.com/en/comps/8/opaque-edition/schedule/"
        "source-owned-fixtures-slug"
    )


def test_schedule_prefers_sched_all_and_keeps_future_rows():
    html = """
    <table id="sched_stage_noise"><tbody><tr>
      <th data-stat="date">2025-01-01</th><td data-stat="home_team">Noise</td>
      <td data-stat="away_team">Noise</td>
      <td data-stat="match_report"><a href="/en/matches/ffffffff/noise">Match Report</a></td>
    </tr></tbody></table>
    <table id="sched_all"><thead><tr>
      <th data-stat="date">Date</th><th data-stat="home_team">Home</th>
      <th data-stat="score">Score</th><th data-stat="away_team">Away</th>
      <th data-stat="match_report">Match Report</th>
    </tr></thead><tbody>
      <tr><th data-stat="date">2025-08-01</th><td data-stat="home_team">Alpha</td>
        <td data-stat="score">2-1</td><td data-stat="away_team">Beta</td>
        <td data-stat="match_report"><a href="/en/matches/aaaaaaaa/source-slug">Match Report</a></td></tr>
      <tr><th data-stat="date">2025-08-08</th><td data-stat="home_team">Gamma</td>
        <td data-stat="score"></td><td data-stat="away_team">Delta</td>
        <td data-stat="match_report"><a href="/en/matches/2025-08-08">Head-to-Head</a></td></tr>
    </tbody></table>
    """

    result = parse_schedule_html(html, _season())
    rows = result.datasets["schedule_rows"].records
    matches = result.datasets["matches"].records

    assert result.status == DatasetStatus.AVAILABLE
    assert len(rows) == 2
    assert rows[0]["table_id"] == "sched_all"
    assert rows[0]["match_url"] == "https://fbref.com/en/matches/aaaaaaaa"
    assert rows[1]["home_team"] == "Gamma"
    assert rows[1]["match_url"] is None
    assert [item.match_id for item in matches] == ["aaaaaaaa"]


def test_schedule_combines_dom_and_comment_blocks_and_deduplicates_matches():
    html = """
    <table id="sched_group"><tbody>
      <tr><th data-stat="round">Group</th><td data-stat="home_team">A</td>
        <td data-stat="away_team">B</td><td data-stat="match_report">
        <a href="/en/matches/aaaaaaaa/group-match">Match Report</a></td></tr>
    </tbody></table>
    <!--
      <table id="sched_knockout"><tbody>
        <tr><th data-stat="round">Final</th><td data-stat="home_team">A</td>
          <td data-stat="away_team">C</td><td data-stat="match_report">
          <a href="https://www.fbref.com/en/matches/bbbbbbbb/final">Match Report</a></td></tr>
        <tr><th data-stat="round">Replay</th><td data-stat="home_team">A</td>
          <td data-stat="away_team">B</td><td data-stat="match_report">
          <a href="/en/matches/aaaaaaaa/duplicate">Match Report</a></td></tr>
        <tr><th data-stat="round">Next</th><td data-stat="home_team">D</td>
          <td data-stat="away_team">E</td><td data-stat="match_report"></td></tr>
      </tbody></table>
    -->
    """

    result = parse_schedule_html(html, _season())
    rows = result.datasets["schedule_rows"].records
    matches = result.datasets["matches"].records

    assert [row["table_id"] for row in rows] == [
        "sched_group",
        "sched_knockout",
        "sched_knockout",
        "sched_knockout",
    ]
    assert [match.match_id for match in matches] == ["aaaaaaaa", "bbbbbbbb"]
    assert matches[1].canonical_url == "https://fbref.com/en/matches/bbbbbbbb"


def test_schedule_with_only_future_rows_has_empty_non_error_matches():
    html = """
    <table id="sched_all"><tbody><tr>
      <th data-stat="date">2026-01-01</th>
      <td data-stat="home_team">Future A</td><td data-stat="score"></td>
      <td data-stat="away_team">Future B</td>
      <td data-stat="match_report"><a href="/en/matches/2026-01-01">Head-to-Head</a></td>
    </tr></tbody></table>
    """

    result = parse_schedule_html(html, _season())

    assert result.status == DatasetStatus.AVAILABLE
    assert result.datasets["schedule_rows"].row_count == 1
    assert result.datasets["matches"].status == DatasetStatus.EMPTY
    assert result.datasets["matches"].reason == "no_match_report_links"
    assert not result.has_errors
