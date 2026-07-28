"""Production runner tests for one exact native Understat scope."""

from __future__ import annotations

from argparse import Namespace
from unittest.mock import MagicMock

import pandas as pd
import pytest

from dags.scripts import run_understat_scraper as runner
from scrapers.base.base_scraper import ReplaceGuardError
from scrapers.understat.manifest import (
    UNDERSTAT_ENTITIES,
    ManifestStatus,
    ScopeAttempt,
)
from scrapers.understat.quality import REQUIRED_COLUMNS, TEAM_BREAKDOWN_DIMENSIONS


LEAGUE = "ENG-Premier League"


def _value(column: str, *, source_year: int = 2025):
    values = {
        "league": LEAGUE,
        "season": f"{source_year % 100:02d}{(source_year + 1) % 100:02d}",
        "source_season_id": source_year,
        "game_id": 100,
        "shot_id": 1000,
        "player_id": 10,
        "team_id": 20,
        "home_team_id": 20,
        "away_team_id": 21,
        "date": "2026-01-10T15:00:00Z",
        "is_result": True,
        "has_data": True,
        "forecast_home_win": 0.5,
        "forecast_draw": 0.25,
        "forecast_away_win": 0.25,
        "xg": 0.2,
        "last_action": "Pass",
        "situation": "Open Play",
        "assist_player_id": 10,
        "player": "Player Ten",
        "source_team_title": "Club",
        "is_multi_team": False,
        "matches": 1,
        "minutes": 90,
        "home_ppda_att": 10,
        "home_ppda_def": 5,
        "home_ppda_allowed_att": 11,
        "home_ppda_allowed_def": 4,
        "away_ppda_att": 11,
        "away_ppda_def": 4,
        "away_ppda_allowed_att": 10,
        "away_ppda_allowed_def": 5,
        "team_side": "h",
        "roster_entry_ids": "501",
        "roster_in": 0,
        "roster_out": 0,
        "dimension": "situation",
        "category": "OpenPlay",
    }
    return values.get(column, 1)


def _complete_frames(*, source_year: int = 2025) -> dict[str, pd.DataFrame]:
    frames = {}
    for entity in UNDERSTAT_ENTITIES:
        row = {
            column: _value(column, source_year=source_year)
            for column in REQUIRED_COLUMNS[entity]
        }
        frames[entity] = pd.DataFrame([row])
    breakdown = frames["understat_team_season_breakdowns"].iloc[0]
    rows = []
    for team_id in (20, 21):
        for dimension in sorted(TEAM_BREAKDOWN_DIMENSIONS):
            row = breakdown.copy()
            row["team_id"] = team_id
            row["dimension"] = dimension
            rows.append(row)
    frames["understat_team_season_breakdowns"] = pd.DataFrame(rows)
    roster = frames["understat_player_match_stats"].iloc[0]
    away_roster = roster.copy()
    away_roster["team_id"] = 21
    away_roster["team_side"] = "a"
    away_roster["player_id"] = 10
    away_roster["player"] = "Player Ten"
    frames["understat_player_match_stats"] = pd.DataFrame(
        [roster, away_roster]
    )
    player_team = frames["understat_player_team_season_stats"].iloc[0]
    away_player_team = player_team.copy()
    away_player_team["team_id"] = 21
    frames["understat_player_team_season_stats"] = pd.DataFrame(
        [player_team, away_player_team]
    )
    return frames


def _empty_frames() -> dict[str, pd.DataFrame]:
    return {
        entity: pd.DataFrame(columns=REQUIRED_COLUMNS[entity])
        for entity in UNDERSTAT_ENTITIES
    }


def _pending_frames() -> dict[str, pd.DataFrame]:
    frames = _empty_frames()
    row = {
        column: _value(column, source_year=2026)
        for column in REQUIRED_COLUMNS["understat_schedule"]
    }
    row.update(is_result=False, has_data=False)
    frames["understat_schedule"] = pd.DataFrame([row])
    return frames


def _args(
    *,
    source_year: int = 2025,
    mode: str = "current",
    force_replace: bool = False,
    source_discovered: bool = True,
) -> Namespace:
    return Namespace(
        mode=mode,
        league=LEAGUE,
        season_slug=f"{source_year % 100:02d}{(source_year + 1) % 100:02d}",
        source_season_id=source_year,
        source_discovered="true" if source_discovered else "false",
        output="/tmp/not-used.json",
        run_id="test-run",
        reparse=False,
        force_replace=force_replace,
    )


class _Repository:
    def __init__(
        self,
        *,
        physical=True,
        previous=None,
        physical_error=None,
        legacy_schedule_rows=0,
    ):
        self.physical = physical
        self.physical_error = physical_error
        self.legacy_schedule_rows = legacy_schedule_rows
        self.previous = previous
        self.appended: list[ScopeAttempt] = []
        self.ensured = False
        self.verified: list[ScopeAttempt] = []

    def ensure_table(self):
        self.ensured = True

    def latest_attempt(self, *_args, **_kwargs):
        return self.appended[-1] if self.appended else None

    def latest_complete(self, *_args, **_kwargs):
        return self.previous

    def latest_data_attempt(self, *_args, **_kwargs):
        for attempt in reversed(self.appended):
            if attempt.status in {
                ManifestStatus.COMPLETE,
                ManifestStatus.UPSTREAM_PENDING,
            }:
                return attempt
        return self.previous

    def verify_physical_batch(self, attempt):
        self.verified.append(attempt)
        if self.physical_error is not None:
            raise self.physical_error
        return self.physical

    def physical_scope_row_count(self, entity, _scope):
        assert entity == "understat_schedule"
        return self.legacy_schedule_rows

    def append_attempt(self, attempt):
        self.appended.append(attempt)
        return "iceberg.ops.understat_ingest_manifest_v1"


def _scraper_factory(frames, *, save_error=None):
    scraper = MagicMock()
    scraper.scrape_scope.return_value = frames
    scraper.save_to_iceberg.side_effect = save_error or (
        lambda **kwargs: f"iceberg.bronze.{kwargs['table_name']}"
    )
    scraper.__enter__.return_value = scraper
    scraper.__exit__.return_value = False
    return MagicMock(return_value=scraper), scraper


def test_complete_scope_writes_exactly_seven_tables_with_one_batch_and_manifest():
    factory, scraper = _scraper_factory(_complete_frames())
    repository = _Repository()

    payload, exit_code = runner.run_scope(
        _args(), scraper_factory=factory, repository=repository
    )

    assert exit_code == 0
    assert payload["status"] == "complete"
    assert payload["errors"] == []
    assert set(payload["tables"]) == set(UNDERSTAT_ENTITIES)
    assert scraper.save_to_iceberg.call_count == 7
    batch_ids = {call.kwargs["batch_id"] for call in scraper.save_to_iceberg.call_args_list}
    assert batch_ids == {payload["batch_id"]}
    assert all(
        call.kwargs["replace_partitions"] == ["league", "season"]
        and call.kwargs["min_replace_ratio"] == 0.9
        for call in scraper.save_to_iceberg.call_args_list
    )
    assert [attempt.status for attempt in repository.appended] == [
        ManifestStatus.IN_PROGRESS,
        ManifestStatus.COMPLETE,
    ]
    assert len(repository.verified) == 1
    assert repository.verified[0].attempt_id == repository.appended[-1].attempt_id
    assert repository.verified[0].batch_id == repository.appended[-1].batch_id
    assert repository.verified[0].completed_at <= repository.appended[-1].completed_at
    assert payload["scope_attempt"] == repository.appended[-1].to_dict()
    assert repository.appended[0].quality["phase"] == "bronze_write_started"


def test_unpublished_future_scope_is_terminal_without_bronze_replacement():
    factory, scraper = _scraper_factory(_empty_frames())
    repository = _Repository()

    payload, exit_code = runner.run_scope(
        _args(source_year=2026, source_discovered=False),
        scraper_factory=factory,
        repository=repository,
    )

    assert exit_code == 0
    assert payload["status"] == "not_published"
    scraper.save_to_iceberg.assert_not_called()
    assert repository.verified == []
    assert repository.appended[0].status is ManifestStatus.NOT_PUBLISHED


def test_discovered_current_scope_cannot_treat_empty_response_as_not_published():
    factory, scraper = _scraper_factory(_empty_frames())
    repository = _Repository()

    payload, exit_code = runner.run_scope(
        _args(source_year=2026, source_discovered=True),
        scraper_factory=factory,
        repository=repository,
    )

    assert exit_code == 1
    assert payload["status"] == "contract_failure"
    assert any("source-discovered" in error for error in payload["errors"])
    scraper.save_to_iceberg.assert_not_called()
    assert repository.appended == []


@pytest.mark.parametrize("source_discovered", [False, True])
def test_empty_probe_never_cuts_over_or_hides_pre_v2_legacy_schedule(
    source_discovered,
):
    repository = _Repository(legacy_schedule_rows=380)

    for _attempt in range(2):
        factory, scraper = _scraper_factory(_empty_frames())
        payload, exit_code = runner.run_scope(
            _args(source_year=2026, source_discovered=source_discovered),
            scraper_factory=factory,
            repository=repository,
        )

        assert exit_code == 1
        assert payload["status"] == "contract_failure"
        assert any("legacy physical scope" in error for error in payload["errors"])
        # No v2 attempt means downstream LEFT JOINs still observe
        # ``m.league IS NULL`` and retain the legacy partition.
        assert repository.latest_attempt(None) is None
        assert repository.appended == []
        scraper.save_to_iceberg.assert_not_called()


def test_active_schedule_only_scope_writes_only_schedule_and_is_not_published():
    factory, scraper = _scraper_factory(_pending_frames())
    repository = _Repository()

    payload, exit_code = runner.run_scope(
        _args(source_year=2026),
        scraper_factory=factory,
        repository=repository,
    )

    assert exit_code == 0
    assert payload["status"] == "upstream_pending"
    assert list(payload["tables"]) == ["understat_schedule"]
    assert scraper.save_to_iceberg.call_count == 1
    assert scraper.save_to_iceberg.call_args.kwargs["table_name"] == (
        "understat_schedule"
    )
    assert repository.verified == []


def test_active_schedule_cannot_regress_to_not_published_after_empty_response():
    repository = _Repository()
    pending_factory, _ = _scraper_factory(_pending_frames())
    first, first_exit = runner.run_scope(
        _args(source_year=2026),
        scraper_factory=pending_factory,
        repository=repository,
    )
    assert first_exit == 0 and first["status"] == "upstream_pending"

    empty_factory, empty_scraper = _scraper_factory(_empty_frames())
    second, second_exit = runner.run_scope(
        _args(source_year=2026),
        scraper_factory=empty_factory,
        repository=repository,
    )

    assert second_exit == 1
    assert second["status"] == "contract_failure"
    assert any("source-discovered" in message for message in second["errors"])
    empty_scraper.save_to_iceberg.assert_not_called()
    assert [attempt.status for attempt in repository.appended] == [
        ManifestStatus.IN_PROGRESS,
        ManifestStatus.UPSTREAM_PENDING,
    ]


def test_backfill_never_accepts_a_non_complete_scope():
    factory, _scraper = _scraper_factory(_empty_frames())
    repository = _Repository()

    payload, exit_code = runner.run_scope(
        _args(source_year=2025, mode="backfill"),
        scraper_factory=factory,
        repository=repository,
    )

    assert exit_code == 1
    assert payload["status"] == "contract_failure"
    assert payload["errors"]
    assert repository.appended == []


def test_first_v2_schema_failure_before_write_preserves_legacy_scope():
    frames = _complete_frames()
    frames["understat_shots"] = frames["understat_shots"].drop(columns=["xg"])
    factory, scraper = _scraper_factory(frames)
    repository = _Repository(legacy_schedule_rows=380)

    payload, exit_code = runner.run_scope(
        _args(), scraper_factory=factory, repository=repository
    )

    assert exit_code == 1
    assert payload["status"] == "schema_drift"
    scraper.save_to_iceberg.assert_not_called()
    assert repository.appended == []
    assert repository.latest_attempt(None) is None


def test_replace_guard_is_exit_three_and_invalidates_older_complete_marker():
    factory, scraper = _scraper_factory(
        _complete_frames(),
        save_error=ReplaceGuardError("new rows would shrink the partition"),
    )
    repository = _Repository(legacy_schedule_rows=380)

    payload, exit_code = runner.run_scope(
        _args(), scraper_factory=factory, repository=repository
    )

    assert exit_code == 3
    assert payload["status"] == "contract_failure"
    assert runner.REPLACE_GUARD_MARKER in payload["errors"][0]
    assert scraper.save_to_iceberg.call_count == 1
    assert repository.appended[-1].status is ManifestStatus.CONTRACT_FAILURE


def test_force_replace_only_disarms_size_guard():
    factory, scraper = _scraper_factory(_complete_frames())
    repository = _Repository()

    payload, exit_code = runner.run_scope(
        _args(force_replace=True),
        scraper_factory=factory,
        repository=repository,
    )

    assert exit_code == 0 and payload["status"] == "complete"
    assert all(
        call.kwargs["min_replace_ratio"] is None
        for call in scraper.save_to_iceberg.call_args_list
    )
    assert repository.verified


def test_physical_batch_mismatch_refuses_complete_manifest():
    factory, _scraper = _scraper_factory(_complete_frames())
    repository = _Repository(physical=False)

    payload, exit_code = runner.run_scope(
        _args(), scraper_factory=factory, repository=repository
    )

    assert exit_code == 1
    assert payload["status"] == "contract_failure"
    assert runner.PHYSICAL_FENCE_MARKER in payload["errors"][0]
    assert [attempt.status for attempt in repository.appended] == [
        ManifestStatus.IN_PROGRESS,
        ManifestStatus.CONTRACT_FAILURE,
    ]


def test_first_v2_transport_failure_preserves_legacy_and_has_seven_entity_evidence():
    factory, scraper = _scraper_factory(_complete_frames())
    scraper.scrape_scope.side_effect = TimeoutError("source timed out")
    repository = _Repository(legacy_schedule_rows=380)

    payload, exit_code = runner.run_scope(
        _args(), scraper_factory=factory, repository=repository
    )

    assert exit_code == 1
    assert payload["status"] == "retryable_failure"
    assert set(payload["entity_statuses"]) == set(UNDERSTAT_ENTITIES)
    assert repository.appended == []
    assert repository.latest_attempt(None) is None


def test_pre_write_transport_failure_preserves_last_complete_publication():
    repository = _Repository()
    complete_factory, _ = _scraper_factory(_complete_frames())
    published, published_exit = runner.run_scope(
        _args(), scraper_factory=complete_factory, repository=repository
    )
    assert published_exit == 0
    published_attempt = repository.latest_attempt(None)

    failing_factory, failing_scraper = _scraper_factory(_complete_frames())
    failing_scraper.scrape_scope.side_effect = TimeoutError("source timed out")
    failed, failed_exit = runner.run_scope(
        _args(), scraper_factory=failing_factory, repository=repository
    )

    assert failed_exit == 1
    assert failed["status"] == "retryable_failure"
    assert repository.latest_attempt(None) is published_attempt
    assert repository.latest_attempt(None).batch_id == published["batch_id"]
    assert [attempt.status for attempt in repository.appended] == [
        ManifestStatus.IN_PROGRESS,
        ManifestStatus.COMPLETE,
    ]


def test_pre_write_dq_failure_preserves_last_complete_publication():
    repository = _Repository()
    complete_factory, _ = _scraper_factory(_complete_frames())
    published, published_exit = runner.run_scope(
        _args(), scraper_factory=complete_factory, repository=repository
    )
    assert published_exit == 0
    published_attempt = repository.latest_attempt(None)

    invalid_frames = _complete_frames()
    invalid_frames["understat_shots"] = invalid_frames["understat_shots"].drop(
        columns=["xg"]
    )
    failing_factory, failing_scraper = _scraper_factory(invalid_frames)
    failed, failed_exit = runner.run_scope(
        _args(), scraper_factory=failing_factory, repository=repository
    )

    assert failed_exit == 1
    assert failed["status"] == "schema_drift"
    failing_scraper.save_to_iceberg.assert_not_called()
    assert repository.latest_attempt(None) is published_attempt
    assert repository.latest_attempt(None).batch_id == published["batch_id"]
    assert [attempt.status for attempt in repository.appended] == [
        ManifestStatus.IN_PROGRESS,
        ManifestStatus.COMPLETE,
    ]


def test_reparse_and_backfill_are_translated_to_native_service_contract():
    factory, scraper = _scraper_factory(_complete_frames())
    repository = _Repository()
    args = _args(mode="backfill")
    args.reparse = True

    payload, exit_code = runner.run_scope(
        args, scraper_factory=factory, repository=repository
    )

    assert exit_code == 0 and payload["status"] == "complete"
    scraper.scrape_scope.assert_called_once_with(
        LEAGUE,
        "2526",
        2025,
        mode="history",
        force_refresh=True,
    )


def test_failed_backfill_attempt_forces_source_refresh_on_retry():
    repository = _Repository()
    failed_factory, _ = _scraper_factory(_complete_frames())
    failed_scraper = failed_factory.return_value
    failed_scraper.scrape_scope.side_effect = TimeoutError("partial payload")
    first, first_exit = runner.run_scope(
        _args(mode="backfill"),
        scraper_factory=failed_factory,
        repository=repository,
    )
    assert first_exit == 1
    assert first["status"] == "retryable_failure"

    retry_factory, retry_scraper = _scraper_factory(_complete_frames())
    second, second_exit = runner.run_scope(
        _args(mode="backfill"),
        scraper_factory=retry_factory,
        repository=repository,
    )

    assert second_exit == 0
    retry_scraper.scrape_scope.assert_called_once_with(
        LEAGUE,
        "2526",
        2025,
        mode="history",
        force_refresh=True,
    )


def test_backfill_skips_scope_completed_while_waiting_for_shared_pool():
    repository = _Repository()
    initial_factory, _ = _scraper_factory(_complete_frames())
    first, first_exit = runner.run_scope(
        _args(), scraper_factory=initial_factory, repository=repository
    )
    assert first_exit == 0
    appended_before = list(repository.appended)

    stale_history_factory = MagicMock()
    second, second_exit = runner.run_scope(
        _args(mode="backfill"),
        scraper_factory=stale_history_factory,
        repository=repository,
    )

    assert second_exit == 0
    assert second["status"] == "complete"
    assert second["batch_id"] == first["batch_id"]
    assert repository.appended == appended_before
    stale_history_factory.assert_not_called()


def test_backfill_verification_outage_does_not_hide_existing_complete_batch():
    repository = _Repository()
    initial_factory, _ = _scraper_factory(_complete_frames())
    first, first_exit = runner.run_scope(
        _args(), scraper_factory=initial_factory, repository=repository
    )
    assert first_exit == 0
    appended_before = list(repository.appended)
    repository.physical_error = TimeoutError("catalog unavailable")

    stale_history_factory = MagicMock()
    second, second_exit = runner.run_scope(
        _args(mode="backfill"),
        scraper_factory=stale_history_factory,
        repository=repository,
    )

    assert second_exit == 1
    assert second["status"] == "complete"
    assert second["batch_id"] == first["batch_id"]
    assert second["errors"]
    assert repository.appended == appended_before
    stale_history_factory.assert_not_called()


def test_cli_requires_explicit_exact_scope_and_rejects_legacy_multi_scope_flags():
    parser = runner._argument_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(["--leagues", LEAGUE, "--season", "2025"])

    parsed = parser.parse_args(
        [
            "--mode",
            "current",
            "--league",
            LEAGUE,
            "--season-slug",
            "2526",
            "--source-season-id",
            "2025",
            "--source-discovered",
            "true",
            "--output",
            "/tmp/result.json",
        ]
    )
    assert parsed.league == LEAGUE and parsed.source_season_id == 2025
    assert parsed.source_discovered == "true"
