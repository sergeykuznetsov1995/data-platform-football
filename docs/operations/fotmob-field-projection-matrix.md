# FotMob: матрица проекций

Генерация: `python scripts/research/audit_fotmob_field_projection.py --check --format markdown`.

`typed` имеет конкретную колонку. Для остальных путей указана причина хранения raw или исключения.

| Цель | Путь | Статус | Колонка / причина |
|---|---|---|---|
| all_leagues | (root) | raw_only | JSON document root |
| all_leagues | countries.* | raw_only | catalog grouping/container metadata |
| all_leagues | countries[].ccode | typed | fotmob_competitions.country_code |
| all_leagues | countries[].leagues[].id | typed | fotmob_competitions.competition_id |
| all_leagues | countries[].leagues[].isPopular | raw_only | unprojected fields retained raw; explicit child projections are typed |
| all_leagues | countries[].leagues[].localizedNameId | raw_only | localization token retained raw |
| all_leagues | countries[].leagues[].name | typed | fotmob_competitions.name |
| all_leagues | countries[].leagues[].pageUrl | typed | fotmob_competitions.page_url |
| all_leagues | countries[].name | typed | fotmob_competitions.country_name |
| all_leagues | international.* | raw_only | variant index grouping retained raw |
| all_leagues | popular.* | raw_only | duplicate presentation list; numeric ids deduplicated against countries |
| all_leagues | popularLeagues.* | raw_only | duplicate presentation list; numeric ids deduplicated against countries |
| competition_profile | (root) | raw_only | JSON document root |
| competition_profile | .* | raw_only | season and presentation data is retained raw for later fan-out |
| competition_profile | details.* | raw_only | unprojected fields retained raw; explicit child projections are typed |
| competition_profile | details.ageGroup | typed | fotmob_competition_scope_observations.source_age_group |
| competition_profile | details.gender | typed | fotmob_competition_scope_observations.source_gender |
| competition_profile | details.id | typed | fotmob_competition_scope_observations.competition_id |
| competition_profile | details.name | typed | fotmob_competition_scope_observations.profile_name |
| competition_profile | details.type | typed | fotmob_competition_scope_observations.source_type |
| leaderboard | (root) | raw_only | JSON document root |
| leaderboard | LeagueName | raw_only | display label; competition identity comes from the requesting scope |
| leaderboard | TopLists.* | raw_only | leaderboard list/container metadata |
| leaderboard | TopLists[].Category | typed | fotmob_leaderboards.stat_category_group |
| leaderboard | TopLists[].StatList[].* | raw_only | unprojected fields retained raw; explicit child projections are typed |
| leaderboard | TopLists[].StatList[].MatchesPlayed | typed | fotmob_leaderboards.matches_played |
| leaderboard | TopLists[].StatList[].MinutesPlayed | typed | fotmob_leaderboards.minutes_played |
| leaderboard | TopLists[].StatList[].ParticipantCountryCode | typed | fotmob_leaderboards.country_code |
| leaderboard | TopLists[].StatList[].ParticipantId | typed | fotmob_leaderboards.participant_id |
| leaderboard | TopLists[].StatList[].ParticipantName | typed | fotmob_leaderboards.participant_name |
| leaderboard | TopLists[].StatList[].Rank | typed | fotmob_leaderboards.rank |
| leaderboard | TopLists[].StatList[].StatValue | typed | fotmob_leaderboards.stat_value |
| leaderboard | TopLists[].StatList[].StatValueCount | typed | fotmob_leaderboards.stat_value_count |
| leaderboard | TopLists[].StatList[].SubStatValue | typed | fotmob_leaderboards.sub_stat_value |
| leaderboard | TopLists[].StatList[].TeamColor | typed | fotmob_leaderboards.team_color |
| leaderboard | TopLists[].StatList[].TeamId | typed | fotmob_leaderboards.team_id |
| leaderboard | TopLists[].StatList[].TeamName | typed | fotmob_leaderboards.team_name |
| leaderboard | TopLists[].StatName | typed | fotmob_leaderboards.stat_name |
| leaderboard | TopLists[].Subtitle | raw_only | presentation text |
| leaderboard | TopLists[].Title | typed | fotmob_leaderboards.stat_category_header |
| league_season | (root) | raw_only | JSON document root |
| league_season | QAData.* | raw_only | source QA/debug payload retained verbatim |
| league_season | allAvailableSeasons | raw_only | unprojected fields retained raw; explicit child projections are typed |
| league_season | allAvailableSeasons[] | typed | fotmob_competition_seasons.source_season_key |
| league_season | details.* | raw_only | unprojected fields retained raw; explicit child projections are typed |
| league_season | details.id | typed | fotmob_competition_seasons.competition_id |
| league_season | details.latestSeason | typed | fotmob_competition_seasons.is_latest |
| league_season | details.selectedSeason | typed | fotmob_competition_seasons.source_season_key |
| league_season | fixtures.* | raw_only | unprojected fields retained raw; explicit child projections are typed |
| league_season | fixtures.allMatches[].away.id | typed | fotmob_matches.away_team_id |
| league_season | fixtures.allMatches[].away.name | typed | fotmob_matches.away_team_name |
| league_season | fixtures.allMatches[].group | typed | fotmob_matches.group_name |
| league_season | fixtures.allMatches[].home.id | typed | fotmob_matches.home_team_id |
| league_season | fixtures.allMatches[].home.name | typed | fotmob_matches.home_team_name |
| league_season | fixtures.allMatches[].id | typed | fotmob_matches.match_id |
| league_season | fixtures.allMatches[].matchId | typed | fotmob_matches.match_id |
| league_season | fixtures.allMatches[].pageUrl | typed | fotmob_matches.page_url |
| league_season | fixtures.allMatches[].round | typed | fotmob_matches.round_id |
| league_season | fixtures.allMatches[].roundName | typed | fotmob_matches.round_name |
| league_season | fixtures.allMatches[].stage | typed | fotmob_matches.stage_id |
| league_season | fixtures.allMatches[].status.awarded | typed | fotmob_matches.awarded |
| league_season | fixtures.allMatches[].status.cancelled | typed | fotmob_matches.cancelled |
| league_season | fixtures.allMatches[].status.finished | typed | fotmob_matches.finished |
| league_season | fixtures.allMatches[].status.reason.long | typed | fotmob_matches.status_reason_long |
| league_season | fixtures.allMatches[].status.reason.short | typed | fotmob_matches.status_reason_short |
| league_season | fixtures.allMatches[].status.scoreStr | typed | fotmob_matches.score_text |
| league_season | fixtures.allMatches[].status.started | typed | fotmob_matches.started |
| league_season | fixtures.allMatches[].status.utcTime | typed | fotmob_matches.utc_time |
| league_season | overview.* | raw_only | presentation widgets preserved raw |
| league_season | playoff.* | raw_only | unprojected fields retained raw; explicit child projections are typed |
| league_season | seasons.* | raw_only | secondary source season representation |
| league_season | seasons[].loser.* | typed | fotmob_competition_season_history.season_history_json |
| league_season | seasons[].seasonName | typed | fotmob_competition_season_history.history_season_label |
| league_season | seasons[].winner.* | typed | fotmob_competition_season_history.season_history_json |
| league_season | seostr | typed | fotmob_competition_seasons.source_slug |
| league_season | stats | raw_only | statistics container |
| league_season | stats.players.* | raw_only | unprojected fields retained raw; explicit child projections are typed |
| league_season | stats.players[].category | typed | fotmob_leaderboard_categories.category |
| league_season | stats.players[].fetchAllUrl | typed | fotmob_leaderboard_categories.fetch_all_url |
| league_season | stats.players[].header | typed | fotmob_leaderboard_categories.header |
| league_season | stats.players[].localizedTitleId | typed | fotmob_leaderboard_categories.localized_title_id |
| league_season | stats.players[].name | typed | fotmob_leaderboard_categories.name |
| league_season | stats.seasonStatLinks.* | raw_only | stat links may span phase seasons; no guaranteed season-row projection, retained raw |
| league_season | stats.seasonsWithLinks.* | raw_only | unprojected fields retained raw; explicit child projections are typed |
| league_season | stats.teams.* | raw_only | unprojected fields retained raw; explicit child projections are typed |
| league_season | stats.teams[].category | typed | fotmob_leaderboard_categories.category |
| league_season | stats.teams[].fetchAllUrl | typed | fotmob_leaderboard_categories.fetch_all_url |
| league_season | stats.teams[].header | typed | fotmob_leaderboard_categories.header |
| league_season | stats.teams[].localizedTitleId | typed | fotmob_leaderboard_categories.localized_title_id |
| league_season | stats.teams[].name | typed | fotmob_leaderboard_categories.name |
| league_season | table.* | raw_only | unprojected fields retained raw; explicit child projections are typed |
| league_season | tabs.* | raw_only | UI navigation metadata |
| league_season | transfers.* | raw_only | league payload descriptor; events come from paginated endpoint |
| match | (root) | raw_only | JSON document root |
| match | .* | raw_only | new matchDetails fields retained in raw and payload_json; specific rules take precedence |
| match | content | raw_only | match content container |
| match | content.attackingZones.* | raw_only | attacking-zones widget retained raw pending explicit modelling |
| match | content.buzz.* | excluded | ephemeral UI engagement widget; retained in raw object |
| match | content.h2h.* | raw_only | head-to-head context retained as JSON |
| match | content.hasPlayoff | raw_only | UI flag mirroring competition playoff structure |
| match | content.heatmapUrl | raw_only | external heatmap asset link |
| match | content.highlightStories.* | raw_only | video highlight stories widget; assets with viewing restrictions, not match data |
| match | content.lineup.* | typed | fotmob_match_payloads.lineup_json |
| match | content.liveticker.* | raw_only | live-ticker widget config; matches are ingested finished |
| match | content.matchFacts.* | typed | fotmob_match_payloads.match_facts_json |
| match | content.momentum.* | typed | fotmob_match_payloads.momentum_json |
| match | content.playerStats.* | typed | fotmob_match_payloads.player_stats_json |
| match | content.shotmap.* | typed | fotmob_match_payloads.shotmap_json |
| match | content.stats.* | typed | fotmob_match_payloads.stats_json |
| match | content.superlive.* | raw_only | SuperLive stream widget config |
| match | content.table.* | raw_only | embedded league table; duplicate of competition standings ingestion |
| match | content.weather.* | raw_only | kickoff weather widget retained raw pending explicit modelling |
| match | general.* | raw_only | unprojected fields retained raw; explicit child projections are typed |
| match | general.matchId | typed | fotmob_match_payloads.match_id |
| match | hasPendingVAR | raw_only | live VAR UI flag; matches are ingested finished |
| match | header.* | raw_only | unprojected fields retained raw; explicit child projections are typed |
| match | nav.* | raw_only | UI navigation metadata |
| match | ongoing | raw_only | live-state UI flag; matches are ingested finished |
| match | seo.* | raw_only | SEO/JSON-LD page markup |
| player | (root) | raw_only | JSON document root |
| player | __N_SSP | raw_only | Next.js framework flag |
| player | buildId | raw_only | Next.js build identity |
| player | context.* | raw_only | Next.js request context (locale/path/user) |
| player | gsp | raw_only | Next.js framework flag |
| player | isFallback | raw_only | Next.js framework flag |
| player | pageProps.* | raw_only | unprojected fields retained raw; explicit child projections are typed |
| player | pageProps.data.birthDate | typed | fotmob_player_snapshots.birth_date |
| player | pageProps.data.careerHistory.* | typed | fotmob_player_snapshots.career_history_json |
| player | pageProps.data.coachStats.* | typed | fotmob_player_snapshots.coach_stats_json |
| player | pageProps.data.firstSeasonStats.* | typed | fotmob_player_snapshots.first_season_stats_json |
| player | pageProps.data.gender | typed | fotmob_player_snapshots.gender |
| player | pageProps.data.id | typed | fotmob_player_snapshots.player_id |
| player | pageProps.data.injuryInformation.* | typed | fotmob_player_snapshots.injury_information_json |
| player | pageProps.data.isCaptain | typed | fotmob_player_snapshots.is_captain |
| player | pageProps.data.isCoach | typed | fotmob_player_snapshots.is_coach |
| player | pageProps.data.mainLeague.leagueId | typed | fotmob_player_snapshots.main_league_id |
| player | pageProps.data.mainLeague.leagueName | typed | fotmob_player_snapshots.main_league_name |
| player | pageProps.data.marketValues.* | typed | fotmob_player_snapshots.market_values_json |
| player | pageProps.data.meta.* | typed | fotmob_player_snapshots.meta_json |
| player | pageProps.data.name | typed | fotmob_player_snapshots.name |
| player | pageProps.data.nextMatch.* | typed | fotmob_player_snapshots.next_match_json |
| player | pageProps.data.playerInformation.* | typed | fotmob_player_snapshots.player_information_json |
| player | pageProps.data.primaryTeam.teamId | typed | fotmob_player_snapshots.primary_team_id |
| player | pageProps.data.primaryTeam.teamName | typed | fotmob_player_snapshots.primary_team_name |
| player | pageProps.data.recentMatches.* | typed | fotmob_player_snapshots.recent_matches_json |
| player | pageProps.data.statSeasons.* | typed | fotmob_player_snapshots.stat_seasons_json |
| player | pageProps.data.traits.* | typed | fotmob_player_snapshots.traits_json |
| player | pageProps.data.trophies.* | typed | fotmob_player_snapshots.trophies_json |
| player | props.* | raw_only | Next.js props container metadata |
| player | props.pageProps.* | raw_only | unprojected fields retained raw; explicit child projections are typed |
| player | props.pageProps.data.birthDate | typed | fotmob_player_snapshots.birth_date |
| player | props.pageProps.data.careerHistory.* | typed | fotmob_player_snapshots.career_history_json |
| player | props.pageProps.data.coachStats.* | typed | fotmob_player_snapshots.coach_stats_json |
| player | props.pageProps.data.firstSeasonStats.* | typed | fotmob_player_snapshots.first_season_stats_json |
| player | props.pageProps.data.gender | typed | fotmob_player_snapshots.gender |
| player | props.pageProps.data.id | typed | fotmob_player_snapshots.player_id |
| player | props.pageProps.data.injuryInformation.* | typed | fotmob_player_snapshots.injury_information_json |
| player | props.pageProps.data.isCaptain | typed | fotmob_player_snapshots.is_captain |
| player | props.pageProps.data.isCoach | typed | fotmob_player_snapshots.is_coach |
| player | props.pageProps.data.mainLeague.leagueId | typed | fotmob_player_snapshots.main_league_id |
| player | props.pageProps.data.mainLeague.leagueName | typed | fotmob_player_snapshots.main_league_name |
| player | props.pageProps.data.marketValues.* | typed | fotmob_player_snapshots.market_values_json |
| player | props.pageProps.data.meta.* | typed | fotmob_player_snapshots.meta_json |
| player | props.pageProps.data.name | typed | fotmob_player_snapshots.name |
| player | props.pageProps.data.nextMatch.* | typed | fotmob_player_snapshots.next_match_json |
| player | props.pageProps.data.playerInformation.* | typed | fotmob_player_snapshots.player_information_json |
| player | props.pageProps.data.primaryTeam.teamId | typed | fotmob_player_snapshots.primary_team_id |
| player | props.pageProps.data.primaryTeam.teamName | typed | fotmob_player_snapshots.primary_team_name |
| player | props.pageProps.data.recentMatches.* | typed | fotmob_player_snapshots.recent_matches_json |
| player | props.pageProps.data.statSeasons.* | typed | fotmob_player_snapshots.stat_seasons_json |
| player | props.pageProps.data.traits.* | typed | fotmob_player_snapshots.traits_json |
| player | props.pageProps.data.trophies.* | typed | fotmob_player_snapshots.trophies_json |
| player | toggles.* | raw_only | feature-flag payload |
| player | translations.* | raw_only | localization string bundle |
| player | url | raw_only | page route echo |
| team | (root) | raw_only | JSON document root |
| team | QAData.* | raw_only | source QA/debug payload retained verbatim |
| team | allAvailableSeasons.* | raw_only | team-page season list; duplicate of competition season ingestion |
| team | details.* | typed | fotmob_team_snapshots.details_json |
| team | fixtures.* | excluded | duplicate of competition fixture ingestion |
| team | history.* | raw_only | team history retained as JSON |
| team | overview.* | typed | fotmob_team_snapshots.overview_json |
| team | seostr | raw_only | source presentation slug |
| team | squad.* | raw_only | unprojected fields retained raw; explicit child projections are typed |
| team | squad.squad[].members[].* | typed | fotmob_squad_snapshots.member_json |
| team | squad.squad[].role | typed | fotmob_squad_snapshots.section |
| team | squad.squad[].title | typed | fotmob_squad_snapshots.section |
| team | stats.* | raw_only | team-page leaderboard widgets; duplicate of competition leaderboard ingestion |
| team | table.* | raw_only | embedded league table; duplicate of competition standings ingestion |
| team | tabs.* | raw_only | UI navigation metadata |
| team | transfers.* | raw_only | team-page transfer widget; events come from the paginated endpoint |
| transfers | (root) | raw_only | JSON document root |
| transfers | hits | raw_only | unprojected fields retained raw; explicit child projections are typed |
| transfers | maxFee | raw_only | fee filter bound; page chrome around the transfer stream |
| transfers | maxMarketValue | raw_only | market value filter bound; retained in the raw transfer page |
| transfers | minAge | raw_only | minimum age filter bound; retained in the raw transfer page |
| transfers | minDate | raw_only | minimum date filter bound; retained in the raw transfer page |
| transfers | page | raw_only | unprojected fields retained raw; explicit child projections are typed |
| transfers | pageSize | raw_only | unprojected fields retained raw; explicit child projections are typed |
| transfers | transfers.* | raw_only | transfer list/container metadata |
| transfers | transfers[].* | raw_only | unprojected fields retained raw; explicit child projections are typed |
| transfers | transfers[].feeText | typed | fotmob_transfer_events.fee_text |
| transfers | transfers[].fromClub.id | typed | fotmob_transfer_events.from_club_id |
| transfers | transfers[].localizedFeeText | typed | fotmob_transfer_events.localized_fee_text |
| transfers | transfers[].name | typed | fotmob_transfer_events.player_name |
| transfers | transfers[].onLoan | typed | fotmob_transfer_events.on_loan |
| transfers | transfers[].playerId | typed | fotmob_transfer_events.player_id |
| transfers | transfers[].position.key | typed | fotmob_transfer_events.position_key |
| transfers | transfers[].position.label | typed | fotmob_transfer_events.position_label |
| transfers | transfers[].toClub.id | typed | fotmob_transfer_events.to_club_id |
| transfers | transfers[].transferDate | typed | fotmob_transfer_events.transfer_date |
| transfers | transfers[].transferType.localizationKey | typed | fotmob_transfer_events.transfer_type_key |
| transfers | transfers[].transferType.text | typed | fotmob_transfer_events.transfer_type_text |
| transfers | transfers[].value | typed | fotmob_transfer_events.fee_value |
