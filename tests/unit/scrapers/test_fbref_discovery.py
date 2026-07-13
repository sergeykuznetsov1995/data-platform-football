from scrapers.fbref.discovery import (
    CalendarType,
    CompetitionFormat,
    CompetitionGender,
    CompetitionRef,
    CompetitionEligibility,
    ParticipantType,
    SeasonRef,
    parse_competition_html,
    parse_competition_index_html,
    parse_schedule_html,
    parse_season_html,
    discover_page_links,
    partition_competitions,
    sentinel_coverage,
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


def test_unknown_league_season_label_keeps_source_opaque_identity():
    html = """
    <table id="seasons"><tr>
      <th data-stat="season"><a href="/en/comps/9/spring/source-slug">Spring Edition</a></th>
    </tr></table>
    """

    result = parse_competition_html(html, _competition())
    dataset = result.datasets["seasons"]

    assert not result.has_errors
    assert dataset.status == DatasetStatus.AVAILABLE
    assert dataset.row_count == 1
    assert dataset.records[0].season_id == "spring"
    assert dataset.records[0].calendar_type == CalendarType.OPAQUE


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


def test_gender_partition_is_decided_before_child_targets():
    male = _competition(comp_id="1")
    female = CompetitionRef(
        **{
            **male.__dict__,
            "comp_id": "2",
            "gender": CompetitionGender.FEMALE,
        }
    )
    unknown = CompetitionRef(
        **{
            **male.__dict__,
            "comp_id": "3",
            "gender": CompetitionGender.UNKNOWN,
        }
    )

    partitioned = partition_competitions([male, female, unknown])

    assert [item.comp_id for item in partitioned[CompetitionEligibility.ELIGIBLE]] == ["1"]
    assert [item.comp_id for item in partitioned[CompetitionEligibility.SKIPPED_FEMALE]] == ["2"]
    assert [item.comp_id for item in partitioned[CompetitionEligibility.QUARANTINED_UNKNOWN]] == ["3"]


def test_source_link_inventory_covers_graph_and_comments_without_url_synthesis():
    html = """
    <a href="/en/comps/8/history/Champions-League-Seasons">History</a>
    <a href="/en/comps/8/2025-2026/Champions-League-Stats">Overview</a>
    <a href="/en/comps/8/2025-2026/schedule/source-schedule">Schedule</a>
    <a href="/en/squads/abcd1234/Team-Stats">Squad</a>
    <a href="/en/players/1234abcd/Player">Player</a>
    <!-- <a href="/en/players/1234abcd/matchlogs/2025/summary/Player-Match-Logs">Logs</a>
         <a href="/en/matches/aaaaaaaa/source-slug">Match</a> -->
    """

    links = discover_page_links(html)
    by_kind = {link.page_kind: link for link in links}

    assert set(by_kind) == {
        "competition", "season", "schedule", "squad", "player", "matchlog", "match"
    }
    assert by_kind["match"].canonical_url == "https://fbref.com/en/matches/aaaaaaaa"
    assert by_kind["season"].source_ids == {
        "competition_id": "8", "season_id": "2025-2026"
    }
    assert by_kind["matchlog"].source_ids == {
        "player_id": "1234abcd",
        "matchlog_season_id": "2025",
        "matchlog_discriminator": "2025/summary",
    }


def test_matchlog_inventory_requires_structural_discriminator():
    html = """
    <a href="/en/players/1234abcd/matchlogs/">Root</a>
    <a href="/en/players/1234abcd/matchlogs/2025">Season nav</a>
    <a href="/en/players/1234abcd/matchlogs/2025/summary">Type nav</a>
    <a href="/en/players/1234abcd/matchlogs/2025/summary/Player-Logs">Data</a>
    """

    matchlogs = [
        link for link in discover_page_links(html)
        if link.page_kind == "matchlog"
    ]

    assert len(matchlogs) == 1
    assert matchlogs[0].source_ids["matchlog_discriminator"] == "2025/summary"


def test_source_id_allowlists_remove_cross_context_inheritance():
    html = """
    <a href="/en/players/1234abcd/Player">Player</a>
    <a href="/en/squads/abcd1234/Team">Squad</a>
    <a href="/en/comps/8/2025-2026/schedule/Source">Schedule</a>
    """

    links = discover_page_links(
        html,
        parent_source_ids={
            "competition_id": "wrong-comp",
            "season_id": "wrong-season",
            "player_id": "wrong-player",
            "squad_id": "wrong-squad",
        },
    )
    by_kind = {link.page_kind: link for link in links}

    assert by_kind["player"].source_ids == {"player_id": "1234abcd"}
    assert by_kind["squad"].source_ids["squad_id"] == "abcd1234"
    assert len(by_kind["squad"].source_ids["squad_discriminator"]) == 20
    assert not {
        "competition_id", "season_id", "player_id"
    } & set(by_kind["squad"].source_ids)
    assert by_kind["schedule"].source_ids == {
        "competition_id": "8",
        "season_id": "2025-2026",
    }


def test_stat_links_stay_distinct_and_known_empty_routes_are_skipped():
    html = """
    <a href="/en/comps/9/2025-2026/Premier-League-Stats">Overview</a>
    <a href="/en/comps/9/2025-2026/shooting/source-shooting">Shooting</a>
    <a href="/en/comps/9/2025-2026/misc/source-misc">Misc</a>
    <a href="/en/comps/9/2025-2026/passing/source-passing">Passing</a>
    <a href="/en/comps/9/2025-2026/keepersadv/source-keepers">Advanced</a>
    """

    links = discover_page_links(html)

    assert [link.page_kind for link in links].count("season") == 1
    assert {
        link.source_ids.get("stat_route")
        for link in links
        if link.page_kind == "season_stats"
    } == {"shooting", "misc"}
    assert not any(
        route in link.canonical_url
        for link in links
        for route in ("/passing/", "/keepersadv/")
    )


SEASON_LESS_NAV_HTML = """
<a href="/en/comps/8/Champions-League-Stats">Champions League</a>
<a href="/en/comps/20/Bundesliga-Stats">Bundesliga</a>
<a href="/en/comps/9/schedule/Premier-League-Scores-and-Fixtures">Fixtures</a>
<a href="/en/comps/9/keepers/Premier-League-Stats">Keepers</a>
<a href="/en/comps/season/2027">2027 seasons</a>
"""


def test_season_less_comps_links_on_a_match_page_are_not_season_targets():
    # FBref's navigation addresses every competition's *current* season without
    # a season segment; a 2016-2017 match page must not mint targets that claim
    # its own season for them.
    links = discover_page_links(
        SEASON_LESS_NAV_HTML,
        parent_source_ids={
            "competition_id": "20",
            "season_id": "2016-2017",
            "match_id": "5492b4b4",
        },
        parent_url="https://fbref.com/en/matches/5492b4b4",
    )

    assert links == []


def test_current_season_page_lends_its_season_to_its_own_subpages_only():
    links = discover_page_links(
        SEASON_LESS_NAV_HTML,
        parent_source_ids={"competition_id": "9", "season_id": "2026"},
        parent_url="https://fbref.com/en/comps/9/Premier-League-Stats",
    )
    by_kind = {link.page_kind: link for link in links}

    assert set(by_kind) == {"schedule", "season_stats"}
    assert by_kind["schedule"].source_ids == {
        "competition_id": "9",
        "season_id": "2026",
    }
    assert by_kind["season_stats"].source_ids == {
        "competition_id": "9",
        "season_id": "2026",
        "stat_route": "keepers",
    }
    assert not any(
        link.source_ids.get("competition_id") in {"8", "20", "season"}
        for link in links
    )


def test_historical_season_page_does_not_lend_its_season_to_current_links():
    links = discover_page_links(
        """
        <a href="/en/comps/9/Premier-League-Stats">Premier League</a>
        <a href="/en/comps/9/2016-2017/keepers/2016-2017-Premier-League-Stats">
          Keepers
        </a>
        """,
        parent_source_ids={"competition_id": "9", "season_id": "2016-2017"},
        parent_url=(
            "https://fbref.com/en/comps/9/2016-2017/2016-2017-Premier-League-Stats"
        ),
    )

    assert [link.page_kind for link in links] == ["season_stats"]
    assert links[0].source_ids["season_id"] == "2016-2017"


def test_sentinel_coverage_observes_but_does_not_filter_arbitrary_male_rows():
    premier = CompetitionRef(
        **{**_competition(comp_id="9").__dict__, "name": "Premier League"}
    )
    arbitrary = CompetitionRef(
        **{**_competition(comp_id="999").__dict__, "name": "Small Test League"}
    )

    report = sentinel_coverage(
        [premier, arbitrary], ["Premier League", "Champions League"]
    )
    partitioned = partition_competitions([premier, arbitrary])

    assert report["Premier League"]["published"] is True
    assert report["Champions League"]["published"] is False
    assert {item.comp_id for item in partitioned[CompetitionEligibility.ELIGIBLE]} == {
        "9", "999"
    }


def test_sentinel_coverage_matches_source_governing_body_prefixes():
    champions = CompetitionRef(
        **{
            **_competition(comp_id="8").__dict__,
            "name": "UEFA Champions League",
        }
    )
    world_cup = CompetitionRef(
        **{
            **_competition(comp_id="1").__dict__,
            "name": "FIFA World Cup",
        }
    )

    report = sentinel_coverage(
        [champions, world_cup], ["Champions League", "World Cup"]
    )

    assert report["Champions League"]["competition_id"] == "8"
    assert report["World Cup"]["competition_id"] == "1"


TOURNAMENT_CARD_HISTORY_HTML = """
<div id="content">
  <div class="content_grid">
    <div>
      <h2><a href="/en/comps/255/2026/2026-Play-offs-Stats">2026 Play-offs</a></h2>
      <p>Top Scorer: <a href="/en/players/2e67fc18/Moises-Paniagua">Moises Paniagua</a></p>
      <p><a href="/en/comps/1/World-Cup-Stats">2026 World Cup Qualifiers</a>:
         <a href="/en/squads/9be9f315/Congo-DR-Men-Stats">Congo DR</a></p>
    </div>
    <div>
      <h2><a href="/en/comps/255/2022/2022-Play-offs-Stats">2022 Play-offs</a></h2>
    </div>
  </div>
</div>
"""


def test_history_without_a_seasons_table_reads_the_tournament_card_grid():
    """Competitions whose editions are standalone tournaments (comp 255, the
    World Cup inter-confederation play-offs) publish no table#seasons at all —
    their history is a card grid. Rejecting the page would drop a men's
    competition from the frontier."""
    cup = _competition(
        comp_id="255",
        competition_format=CompetitionFormat.CUP,
        participants=ParticipantType.NATIONAL_TEAM,
    )

    result = parse_competition_html(TOURNAMENT_CARD_HISTORY_HTML, cup)
    seasons = result.datasets["seasons"].records

    assert not result.has_errors
    assert [item.season_id for item in seasons] == ["2026", "2022"]
    assert seasons[0].season_url == (
        "https://fbref.com/en/comps/255/2026/2026-Play-offs-Stats"
    )
    assert seasons[0].calendar_type == CalendarType.TOURNAMENT


def test_card_grid_ignores_links_to_other_competitions_and_squads():
    """The grid also links to the parent competition, squads and players; only
    season routes of *this* competition may become season targets."""
    cup = _competition(comp_id="255", competition_format=CompetitionFormat.CUP)

    result = parse_competition_html(TOURNAMENT_CARD_HISTORY_HTML, cup)

    urls = [item.season_url for item in result.datasets["seasons"].records]
    assert all("/comps/255/" in url for url in urls)


def test_a_history_page_with_no_seasons_at_all_still_fails_closed():
    """No table, no cards — the page contract is broken and must not pass as an
    empty-but-fine competition."""
    result = parse_competition_html(
        "<div id='content'><p>nothing here</p></div>", _competition()
    )
    dataset = result.datasets["seasons"]

    assert result.has_errors
    assert dataset.status == DatasetStatus.ERROR
    assert dataset.reason == "season_history_table_missing"
