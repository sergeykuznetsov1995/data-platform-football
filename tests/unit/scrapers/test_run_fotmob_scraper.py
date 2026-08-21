"""Unit tests for the source-native FotMob runner."""

from __future__ import annotations

import importlib
import json
import sys
from contextlib import contextmanager, nullcontext
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest


PUBLICATION_SHA = "a" * 40


def _publication_cli(monkeypatch):
    from utils import fotmob_publication as publication

    monkeypatch.setenv(publication.FOTMOB_RUNTIME_FINGERPRINT_ENV, PUBLICATION_SHA)
    binding = publication.make_publication_binding(
        owner="isolated",
        data_interval_start="2026-07-20T14:00:00+00:00",
        data_interval_end="2026-07-21T14:00:00+00:00",
        fingerprint=PUBLICATION_SHA,
    )
    generation_id = publication.make_generation_id(binding)
    arguments = ["--publication-generation-id", generation_id]
    for field, option in (
        ("schema", "--publication-schema"),
        ("source", "--publication-source"),
        ("owner", "--publication-owner"),
        ("data_interval_start", "--publication-data-interval-start"),
        ("data_interval_end", "--publication-data-interval-end"),
        ("runtime_fingerprint", "--publication-runtime-fingerprint"),
    ):
        arguments.extend((option, binding[field]))
    return arguments, {"generation_id": generation_id, "binding": binding}


def _daily_cli(monkeypatch):
    """Point the exact container contract at the same repo bytes in host tests."""

    from utils import fotmob_publication as publication

    scope_file = (
        Path(__file__).resolve().parents[3]
        / "configs"
        / "fotmob"
        / "issue-930-scopes.txt"
    )
    monkeypatch.setattr(
        publication,
        "FOTMOB_DAILY_SCOPE_FILE",
        str(scope_file),
    )
    return [
        "--daily-contract",
        publication.FOTMOB_DAILY_CONTRACT_SCHEMA,
        "--competition-scope-file",
        str(scope_file),
        "--competition-scope-sha256",
        publication.FOTMOB_DAILY_SCOPE_SHA256,
        "--competition-ids-sha256",
        publication.FOTMOB_DAILY_COMPETITION_IDS_SHA256,
        "--entities",
        ",".join(publication.FOTMOB_DAILY_ENTITIES),
        "--max-requests",
        str(publication.FOTMOB_DAILY_MAX_REQUESTS),
        "--max-direct-mib",
        str(publication.FOTMOB_DAILY_MAX_DIRECT_MIB),
        "--requests-per-minute",
        str(publication.FOTMOB_DAILY_REQUESTS_PER_MINUTE),
    ]


def _source_refresh_cli():
    from scrapers.fotmob.source_refresh import (
        PLAYER_SOURCE_REFRESH_MAX_DIRECT_MIB,
        PLAYER_SOURCE_REFRESH_MAX_REQUESTS,
        PLAYER_SOURCE_REFRESH_PROFILE,
        PLAYER_SOURCE_REFRESH_SHA256,
    )

    return [
        "--source-refresh-profile",
        PLAYER_SOURCE_REFRESH_PROFILE,
        "--source-refresh-targets-sha256",
        PLAYER_SOURCE_REFRESH_SHA256,
        "--entities",
        "players",
        "--max-requests",
        str(PLAYER_SOURCE_REFRESH_MAX_REQUESTS),
        "--max-direct-mib",
        str(PLAYER_SOURCE_REFRESH_MAX_DIRECT_MIB),
    ]


def _player_collector_cli():
    from scrapers.fotmob.player_collector import (
        PLAYER_COLLECTOR_MAX_DIRECT_MIB,
        PLAYER_COLLECTOR_MAX_REQUESTS,
        PLAYER_COLLECTOR_PLAYER_LIMIT,
        PLAYER_COLLECTOR_REQUESTS_PER_MINUTE,
    )

    return [
        "--entities",
        "players",
        "--max-requests",
        str(PLAYER_COLLECTOR_MAX_REQUESTS),
        "--max-direct-mib",
        str(PLAYER_COLLECTOR_MAX_DIRECT_MIB),
        "--player-limit",
        str(PLAYER_COLLECTOR_PLAYER_LIMIT),
        "--requests-per-minute",
        str(PLAYER_COLLECTOR_REQUESTS_PER_MINUTE),
    ]


def _run_native_admitted(mod, args, **kwargs):
    """Exercise native planning under an explicit test-only active identity."""

    generation_id = "11111111-1111-4111-8111-111111111111"
    assert mod._ACTIVE_PUBLICATION_GENERATION is None
    args.publication_generation_id = generation_id
    mod._ACTIVE_PUBLICATION_GENERATION = generation_id
    try:
        return mod._run_native(args, **kwargs)
    finally:
        mod._ACTIVE_PUBLICATION_GENERATION = None


@pytest.fixture(autouse=True)
def _writer_lock_disabled(monkeypatch):
    """У юнит-тестов нет Postgres, а замок писателя (B7) молча не обходится.

    Поэтому в сьюте он выключается явно — ровно тем же выключателем, что и в
    офлайн-реплее. Тесты самого замка ставят переменную сами.
    """

    from dags.scripts.run_fotmob_scraper import WRITER_LOCK_ENV

    monkeypatch.setenv(WRITER_LOCK_ENV, "0")


class TestFotmobNativeRunner:
    """Source-native mode is explicit and preserves exact source identities."""

    @staticmethod
    def _module():
        sys.modules.pop("dags.scripts.run_fotmob_scraper", None)
        return importlib.import_module("dags.scripts.run_fotmob_scraper")

    @pytest.mark.unit
    def test_retired_legacy_scraper_is_not_a_package_export(self):
        import scrapers
        import scrapers.fotmob

        assert "FotMobScraper" not in scrapers.__all__
        assert not hasattr(scrapers.fotmob, "FotMobScraper")

    @pytest.mark.unit
    def test_scope_parser_requires_numeric_id_and_preserves_exact_season(self):
        mod = self._module()

        assert mod._parse_scopes(
            ["47=2025/2026,230=2025 Apertura", "47=2025/2026"]
        ) == (
            (47, "2025/2026"),
            (230, "2025 Apertura"),
        )
        with pytest.raises(ValueError, match="ASCII decimal"):
            mod._parse_scopes(["ENG-Premier League=2025/2026"])
        with pytest.raises(ValueError, match="source season key"):
            mod._parse_scopes(["47="])

    @pytest.mark.unit
    def test_scope_parser_normalizes_only_the_optional_empty_scope_sentinel(self):
        mod = self._module()

        assert mod._parse_scopes([""]) == ()
        with pytest.raises(ValueError, match="empty scope fragment"):
            mod._parse_scopes(["230=2025,"])

    @pytest.mark.unit
    def test_runner_and_daily_evidence_share_ordered_deduplicated_scopes(self):
        mod = self._module()
        # This focused scraper suite does not collect tests/unit/dags, whose
        # conftest normally installs the host-side Airflow API stubs.
        from tests.unit.dags.conftest import _install_airflow_stubs

        _install_airflow_stubs()
        daily = importlib.import_module("dag_ingest_fotmob")
        expected_pairs = ((230, "2025 Apertura"), (47, "2024/2025"))
        expected_tokens = ("230=2025 Apertura", "47=2024/2025")
        raw_groups = ["230=2025 Apertura,47=2024/2025", "230=2025 Apertura"]

        assert mod._parse_scopes(raw_groups) == expected_pairs
        assert daily.validate_scope_tokens(
            [*expected_tokens, expected_tokens[0]]
        ) == expected_tokens
        assert daily._validated_exact_scope_evidence(list(expected_tokens)) == (
            expected_tokens
        )

    @pytest.mark.unit
    def test_max_buffered_rows_defaults_high_and_rejects_non_positive(self):
        # The repository's 20k default flushed every ~4 matches once
        # field-inventory rows piled up, defeating --commit-batch-size (#930).
        mod = self._module()

        parser = mod._argument_parser()
        assert parser.parse_args(["--mode", "daily"]).max_buffered_rows == 100_000
        with pytest.raises(SystemExit):
            mod._validate_args(
                parser,
                parser.parse_args(["--mode", "daily", "--max-buffered-rows", "0"]),
            )
        with pytest.raises(SystemExit):
            mod._validate_args(
                parser,
                parser.parse_args(["--mode", "daily", "--commit-batch-size", "0"]),
            )

    @pytest.mark.unit
    def test_cli_requires_native_mode_and_rejects_removed_legacy_flags(self):
        mod = self._module()
        parser = mod._argument_parser()

        with pytest.raises(SystemExit):
            parser.parse_args([])
        for flag, value in (
            ("--leagues", "ENG-Premier League"),
            ("--season", "2025"),
            ("--force-replace", None),
            ("--full-players", None),
        ):
            argv = ["--mode", "daily", flag]
            if value is not None:
                argv.append(value)
            with pytest.raises(SystemExit):
                parser.parse_args(argv)

    @pytest.mark.unit
    def test_automatic_catalog_profile_never_loads_issue930_scope_file(
        self, monkeypatch
    ):
        mod = self._module()
        parser = mod._argument_parser()
        from utils import fotmob_publication as publication

        monkeypatch.setattr(
            publication,
            "load_fotmob_daily_competition_contract",
            lambda *_args, **_kwargs: pytest.fail("legacy scope file was read"),
        )
        args = parser.parse_args(
            [
                "--mode",
                "refresh",
                "--catalog-contract",
                "fotmob-catalog-v1",
                "--deadline",
                "2026-08-08T12:00:00Z",
            ]
        )

        assert mod._validate_args(parser, args) is None
        assert args.catalog_contract == "fotmob-catalog-v1"
        assert args.deadline_at == datetime(2026, 8, 8, 12)

    @pytest.mark.unit
    def test_automatic_catalog_profile_rejects_legacy_contract_fields(self):
        mod = self._module()
        parser = mod._argument_parser()
        with pytest.raises(SystemExit):
            mod._validate_args(
                parser,
                parser.parse_args(
                    [
                        "--mode",
                        "refresh",
                        "--catalog-contract",
                        "fotmob-catalog-v1",
                        "--daily-contract",
                        "fotmob-daily-v1",
                    ]
                ),
            )

    @pytest.mark.unit
    def test_automatic_catalog_discover_fails_before_service_construction(
        self, monkeypatch
    ):
        from utils import fotmob_publication as publication

        mod = self._module()
        monkeypatch.delenv(
            publication.FOTMOB_DEPLOYMENT_REPORT_PATH_ENV, raising=False
        )
        monkeypatch.delenv(
            publication.FOTMOB_SHARED_DEPLOYMENT_REPORT_PATH_ENV, raising=False
        )
        build_service = MagicMock()
        monkeypatch.setattr(mod, "_build_native_service", build_service)
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "run_fotmob_scraper.py",
                "--mode",
                "discover",
                "--catalog-contract",
                "fotmob-catalog-v1",
            ],
        )

        with pytest.raises(SystemExit):
            mod.main()

        build_service.assert_not_called()

    @pytest.mark.unit
    def test_automatic_runner_emits_classifier_bound_contract_and_attempts(self):
        from scrapers.fotmob.catalog_contract import catalog_contract_from_dict
        from scrapers.fotmob.transport import canonicalize_target
        from tests.unit.scrapers.test_fotmob_service import _league_payload, _service

        mod = self._module()
        responses = {
            canonicalize_target("allLeagues").canonical_url: {
                "countries": [{"leagues": [{"id": 47, "name": "Premier League"}]}]
            },
            canonicalize_target("leagues", {"id": 47}).canonical_url: _league_payload(),
        }
        service, _, _ = _service(responses)
        args = mod._argument_parser().parse_args(
            [
                "--mode",
                "refresh",
                "--catalog-contract",
                "fotmob-catalog-v1",
                "--entities",
                "season",
            ]
        )

        rc, report = _run_native_admitted(mod, args, service=service)

        assert rc == 0, report["errors"]
        selection = report["selection"]
        contract = catalog_contract_from_dict(selection["catalog_contract"])
        assert contract.classifier_version == "fotmob-men-v1"
        assert contract.included_ids == (47,)
        assert contract.scopes == ("47=2025/2026",)
        assert selection["scope_lane"] == "current"
        assert selection["catalog_ids"] == [47]
        assert selection["catalog_decisions"][0]["decision"] == "included"
        assert selection["scope_attempts"][0]["outcome"] == "success"

    @pytest.mark.unit
    def test_automatic_runner_removes_now_female_cached_inclusion_before_fanout(
        self,
    ):
        from scrapers.fotmob.catalog_contract import catalog_contract_from_dict
        from scrapers.fotmob.planner import RunMode, TransportBudget
        from scrapers.fotmob.repository import MemoryFotMobRepository
        from scrapers.fotmob.service import FotMobIngestService, OperationResult
        from scrapers.fotmob.transport import canonicalize_target
        from tests.unit.scrapers.test_fotmob_service import (
            StubTransport,
            _competition_payload,
        )

        mod = self._module()
        competition_id = 47
        all_leagues = {
            "countries": [
                {
                    "leagues": [
                        {"id": competition_id, "name": "Premier League"}
                    ]
                }
            ]
        }
        catalog_url = canonicalize_target("allLeagues").canonical_url
        profile_url = canonicalize_target(
            "leagues", {"id": competition_id}
        ).canonical_url
        repository = MemoryFotMobRepository()
        seed_service = FotMobIngestService(
            transport=StubTransport(
                {
                    catalog_url: all_leagues,
                    profile_url: _competition_payload(
                        competition_id, "Premier League", gender="male"
                    ),
                }
            ),
            repository=repository,
            mode=RunMode.DAILY,
            budget=TransportBudget(
                max_requests=100, max_direct_bytes=10_000_000
            ),
            run_id="seed-male-evidence",
        )
        seed_service.discover_catalog()

        service = FotMobIngestService(
            transport=StubTransport(
                {
                    catalog_url: all_leagues,
                    profile_url: _competition_payload(
                        competition_id, "Premier League", gender="female"
                    ),
                }
            ),
            repository=repository,
            mode=RunMode.DAILY,
            budget=TransportBudget(
                max_requests=100, max_direct_bytes=10_000_000
            ),
            run_id="fresh-female-evidence",
        )
        service.sync_transfers = MagicMock(
            return_value=OperationResult(
                "transfer_events",
                attempted=1,
                succeeded=1,
                counts={"events": 0},
                metadata={"source_hits": 0},
            )
        )
        args = mod._argument_parser().parse_args(
            [
                "--mode",
                "refresh",
                "--catalog-contract",
                "fotmob-catalog-v1",
                "--entities",
                "season,transfers",
            ]
        )

        rc, report = _run_native_admitted(mod, args, service=service)

        contract = catalog_contract_from_dict(
            report["selection"]["catalog_contract"]
        )
        assert rc == 0, report["errors"]
        assert contract.included_ids == ()
        assert contract.scopes == ()
        assert report["selection"]["planned_scopes"] == []
        assert report["selection"]["catalog_decisions"][0]["decision"] == "excluded"
        assert report["selection"]["catalog_decisions"][0]["source_gender"] == "female"
        service.sync_transfers.assert_not_called()

    @pytest.mark.unit
    @pytest.mark.parametrize("automatic_catalog", [True, False])
    def test_transfer_waits_for_fresh_profile_validation(
        self, automatic_catalog
    ):
        from scrapers.fotmob.planner import RunMode, TransportBudget
        from scrapers.fotmob.repository import MemoryFotMobRepository
        from scrapers.fotmob.service import FotMobIngestService, OperationResult
        from scrapers.fotmob.transport import canonicalize_target
        from tests.unit.scrapers.test_fotmob_service import (
            StubTransport,
            _competition_payload,
        )

        mod = self._module()
        competition_ids = (47, 48)
        all_leagues = {
            "countries": [
                {
                    "leagues": [
                        {"id": competition_id, "name": f"League {competition_id}"}
                        for competition_id in competition_ids
                    ]
                }
            ]
        }
        catalog_url = canonicalize_target("allLeagues").canonical_url

        def profile(competition_id: int, *, gender: str = "male"):
            payload = _competition_payload(
                competition_id,
                f"League {competition_id}",
                gender=gender,
            )
            payload["allAvailableSeasons"] = []
            return payload

        repository = MemoryFotMobRepository()
        seed_service = FotMobIngestService(
            transport=StubTransport(
                {
                    catalog_url: all_leagues,
                    **{
                        canonicalize_target(
                            "leagues", {"id": competition_id}
                        ).canonical_url: profile(competition_id)
                        for competition_id in competition_ids
                    },
                }
            ),
            repository=repository,
            mode=RunMode.BACKFILL,
            budget=TransportBudget(
                max_requests=100, max_direct_bytes=10_000_000
            ),
            run_id="seed-two-male-profiles",
        )
        seed_service.discover_catalog()

        deferred_profile_url = canonicalize_target(
            "leagues", {"id": 48}
        ).canonical_url
        current_transport = StubTransport(
            {
                catalog_url: all_leagues,
                canonicalize_target(
                    "leagues", {"id": 47}
                ).canonical_url: profile(47),
                deferred_profile_url: profile(48, gender="female"),
            }
        )
        service = FotMobIngestService(
            transport=current_transport,
            repository=repository,
            mode=RunMode.BACKFILL,
            budget=TransportBudget(
                max_requests=100, max_direct_bytes=10_000_000
            ),
            run_id="bounded-profile-validation",
        )
        repository.completed_competition_ids = MagicMock(return_value={47})
        service.sync_transfers = MagicMock(
            return_value=OperationResult(
                "transfer_events",
                attempted=1,
                succeeded=1,
                counts={"events": 0},
                metadata={"source_hits": 0},
            )
        )
        argv = [
            "--mode",
            "backfill",
            "--entities",
            "transfers",
            "--competition-limit",
            "1",
        ]
        if automatic_catalog:
            argv.extend(("--catalog-contract", "fotmob-catalog-v1"))
        args = mod._argument_parser().parse_args(argv)

        rc, report = _run_native_admitted(mod, args, service=service)

        assert rc == 0, report["errors"]
        service.sync_transfers.assert_not_called()
        assert not any(
            url == deferred_profile_url for url, _replay in current_transport.calls
        )
        if automatic_catalog:
            transfer_deferrals = [
                item
                for item in report["selection"]["deferrals"]
                if item["target_type"] == "transfer"
            ]
            assert [item["targets"] for item in transfer_deferrals] == [[48]]
        transfer_plan = next(
            item
            for item in report["operations"]
            if item["entity"] == "transfer_work_plan"
        )
        assert transfer_plan["metadata"][
            "profile_validation_deferred_competition_ids"
        ] == [48]

    @pytest.mark.unit
    def test_automatic_deadline_deferral_is_partial_success_with_evidence(self):
        from scrapers.fotmob.transport import canonicalize_target
        from tests.unit.scrapers.test_fotmob_service import _league_payload, _service

        mod = self._module()
        responses = {
            canonicalize_target("allLeagues").canonical_url: {
                "countries": [{"leagues": [{"id": 47, "name": "Premier League"}]}]
            },
            canonicalize_target("leagues", {"id": 47}).canonical_url: _league_payload(),
        }
        service, _, _ = _service(responses)
        service.sync_season = MagicMock(wraps=service.sync_season)
        args = mod._argument_parser().parse_args(
            [
                "--mode",
                "refresh",
                "--catalog-contract",
                "fotmob-catalog-v1",
                "--entities",
                "season",
            ]
        )
        args.deadline_at = datetime(2020, 1, 1)

        rc, report = _run_native_admitted(mod, args, service=service)

        assert rc == 0
        assert report["status"] == "partial_success"
        assert report["complete"] is False
        assert report["selection"]["scope_attempts"][0]["outcome"] == "deferred"
        service.sync_season.assert_not_called()

    @pytest.mark.unit
    def test_automatic_schema_failure_remains_hard(self):
        from scrapers.fotmob.service import OperationResult
        from scrapers.fotmob.transport import canonicalize_target
        from tests.unit.scrapers.test_fotmob_service import _league_payload, _service

        mod = self._module()
        responses = {
            canonicalize_target("allLeagues").canonical_url: {
                "countries": [{"leagues": [{"id": 47, "name": "Premier League"}]}]
            },
            canonicalize_target("leagues", {"id": 47}).canonical_url: _league_payload(),
        }
        service, _, _ = _service(responses)
        service.sync_season = MagicMock(
            return_value=(
                OperationResult(
                    "season_bundle",
                    attempted=1,
                    errors=["schema drift: missing required match identity"],
                ),
                None,
            )
        )
        args = mod._argument_parser().parse_args(
            [
                "--mode",
                "refresh",
                "--catalog-contract",
                "fotmob-catalog-v1",
                "--entities",
                "season",
            ]
        )

        rc, report = _run_native_admitted(mod, args, service=service)

        assert rc == 1
        assert report["status"] == "incomplete"
        assert report["selection"]["scope_attempts"][0]["outcome"] == "terminal"

    @pytest.mark.unit
    def test_automatic_http_retry_remains_incomplete_and_nonzero(self):
        from scrapers.fotmob.service import OperationResult
        from scrapers.fotmob.transport import canonicalize_target
        from tests.unit.scrapers.test_fotmob_service import _league_payload, _service

        mod = self._module()
        responses = {
            canonicalize_target("allLeagues").canonical_url: {
                "countries": [{"leagues": [{"id": 47, "name": "Premier League"}]}]
            },
            canonicalize_target("leagues", {"id": 47}).canonical_url: _league_payload(),
        }
        service, _, _ = _service(responses)
        service.sync_season = MagicMock(
            return_value=(
                OperationResult(
                    "season_bundle",
                    attempted=1,
                    retryable=["HTTP 503 from FotMob"],
                ),
                None,
            )
        )
        args = mod._argument_parser().parse_args(
            [
                "--mode",
                "refresh",
                "--catalog-contract",
                "fotmob-catalog-v1",
                "--entities",
                "season",
            ]
        )

        rc, report = _run_native_admitted(mod, args, service=service)

        assert rc == 1
        assert report["status"] == "incomplete"
        assert report["complete"] is False
        assert report["selection"]["scope_attempts"][0]["outcome"] == "retryable"

    @pytest.mark.unit
    def test_automatic_source_gap_requires_two_distinct_missing_match_runs(
        self, monkeypatch
    ):
        from scrapers.fotmob import planner
        from scrapers.fotmob.planner import RunMode, TransportBudget
        from scrapers.fotmob.repository import MemoryFotMobRepository
        from scrapers.fotmob.service import FotMobIngestService
        from scrapers.fotmob.transport import canonicalize_target
        from tests.unit.scrapers.test_fotmob_service import StubTransport, _league_payload

        mod = self._module()
        missing_match = {"error": True, "message": "Data not found", "matchId": "100"}
        responses = {
            canonicalize_target("allLeagues").canonical_url: {
                "countries": [{"leagues": [{"id": 47, "name": "Premier League"}]}]
            },
            canonicalize_target("leagues", {"id": 47}).canonical_url: _league_payload(),
            canonicalize_target("matchDetails", {"matchId": "100"}).canonical_url: missing_match,
        }
        repository = MemoryFotMobRepository()

        def make_service(run_id):
            return FotMobIngestService(
                transport=StubTransport(dict(responses)),
                repository=repository,
                mode=RunMode.DAILY,
                budget=TransportBudget(max_requests=100, max_direct_bytes=10_000_000),
                run_id=run_id,
                max_workers=2,
            )

        args = mod._argument_parser().parse_args(
            [
                "--mode",
                "refresh",
                "--catalog-contract",
                "fotmob-catalog-v1",
                "--entities",
                "season,matches",
                "--run-id",
                "missing-match-1",
            ]
        )
        first_rc, first_report = _run_native_admitted(
            mod, args, service=make_service("missing-match-1")
        )

        assert first_rc == 1
        assert first_report["status"] == "incomplete"
        assert first_report["selection"]["scope_attempts"][0]["outcome"] == "retryable"

        real_datetime = datetime

        class FutureDatetime(datetime):
            @classmethod
            def now(cls, tz=None):
                value = real_datetime.now(tz)
                return value + timedelta(hours=1)

        monkeypatch.setattr(planner, "datetime", FutureDatetime)
        args.run_id = "missing-match-2"
        second_rc, second_report = _run_native_admitted(
            mod, args, service=make_service("missing-match-2")
        )

        assert second_rc == 0, second_report["errors"]
        assert second_report["status"] == "success"
        attempt = second_report["selection"]["scope_attempts"][0]
        assert attempt["outcome"] == "source_gap"
        assert attempt["attempt_count"] == 2
        assert len(attempt["attempt_identities"]) == 2
        assert len(set(attempt["attempt_identities"])) == 2

    @pytest.mark.unit
    def test_automatic_run_with_progress_defers_retry_without_failing(self):
        """Отложенный повтор рядом с закрытым скоупом — жёлтый ран, не красный.

        Непрерывная полоса обходит ~450 скоупов под временным бюджетом и не может
        закрыть их все за одно окно. Пока ран продвигается (хотя бы один скоуп
        закрыт), скоуп, поставленный на повтор, — штатное промежуточное
        состояние: rc=0 и partial_success, иначе silver никогда не триггерится.
        Парный инвариант «ран без единого закрытого скоупа остаётся красным»
        держит test_automatic_http_retry_remains_incomplete_and_nonzero.
        """

        from scrapers.fotmob.planner import RunMode, TransportBudget
        from scrapers.fotmob.repository import MemoryFotMobRepository
        from scrapers.fotmob.service import FotMobIngestService
        from scrapers.fotmob.transport import canonicalize_target
        from tests.unit.scrapers.test_fotmob_service import (
            StubTransport,
            _competition_payload,
        )

        mod = self._module()
        healthy = _competition_payload(48, "Second League")
        healthy["fixtures"]["allMatches"][0]["id"] = 200
        healthy["fixtures"]["allMatches"][0]["pageUrl"] = "/matches/alpha-vs-beta/x#200"
        responses = {
            canonicalize_target("allLeagues").canonical_url: {
                "countries": [
                    {
                        "leagues": [
                            {"id": 47, "name": "Premier League"},
                            {"id": 48, "name": "Second League"},
                        ]
                    }
                ]
            },
            canonicalize_target("leagues", {"id": 47}).canonical_url: (
                _competition_payload(47, "Premier League")
            ),
            canonicalize_target("leagues", {"id": 48}).canonical_url: healthy,
            canonicalize_target(
                "matchDetails", {"matchId": "100"}
            ).canonical_url: {
                "error": True,
                "message": "Data not found",
                "matchId": "100",
            },
            canonicalize_target(
                "matchDetails", {"matchId": "200"}
            ).canonical_url: {
                "content": {"matchFacts": {"events": []}, "stats": {"x": 1}}
            },
        }
        service = FotMobIngestService(
            transport=StubTransport(dict(responses)),
            repository=MemoryFotMobRepository(),
            mode=RunMode.DAILY,
            budget=TransportBudget(max_requests=100, max_direct_bytes=10_000_000),
            run_id="mixed-1",
            max_workers=2,
        )
        args = mod._argument_parser().parse_args(
            [
                "--mode",
                "refresh",
                "--catalog-contract",
                "fotmob-catalog-v1",
                "--entities",
                "season,matches",
                "--run-id",
                "mixed-1",
            ]
        )

        rc, report = _run_native_admitted(mod, args, service=service)

        outcomes = {
            attempt["competition_id"]: attempt["outcome"]
            for attempt in report["selection"]["scope_attempts"]
        }
        assert outcomes[47] == "retryable"
        assert outcomes[48] == "success"
        assert rc == 0, report["errors"]
        assert report["status"] == "partial_success"
        assert report["complete"] is False
        assert report["selection"]["scope_outcome_counts"] == {
            "retryable": 1,
            "success": 1,
        }
        assert report["selection"]["planned_scope_count"] == 2

        # Гейт раннера и приёмка отчёта — два независимых барьера на пути к
        # silver: покрасить может любой. Поэтому НАСТОЯЩИЙ отчёт этого рана
        # прогоняется через тот же валидатор, что зовёт validate_data.
        from scripts.fotmob_catalog_acceptance import validate_report

        assert validate_report(report, require_full_completion=True).ok is False
        continuous = validate_report(report, require_full_completion=False)
        assert continuous.ok is True, continuous.errors

    @pytest.mark.unit
    def test_schedule_cooldown_returns_after_the_next_kickoff_not_in_two_days(self):
        """Главный случай — матч, который на момент обхода ЕЩЁ БУДУЩИЙ.

        Обход утром видит вечерний матч не начавшимся; при фиксированных 48 часах
        никто не пришёл бы за его флагом двое суток, а без флага не качается
        карточка (#1193). Срок обязан приземлиться вскоре ПОСЛЕ матча.
        """

        from datetime import datetime, timedelta

        mod = self._module()
        now = datetime(2026, 8, 20, 9, 0)

        vecherniy = [{"utc_time": "2026-08-20T18:00:00Z"}]
        assert mod._schedule_cooldown(vecherniy, now) == timedelta(hours=12)

        # Матч уже идёт — обязательство висит прямо сейчас.
        idet = [{"utc_time": "2026-08-20T08:00:00Z"}]
        assert mod._schedule_cooldown(idet, now) == timedelta(hours=2)

        # Ближайший матч так далеко, что срок упирается в общий потолок.
        daleko = [{"utc_time": "2026-08-30T18:00:00Z"}]
        assert mod._schedule_cooldown(daleko, now) == timedelta(hours=48)

        # Матч через полчаса: срок = до него плюс запас на доигрывание.
        skoro = [{"utc_time": "2026-08-20T09:30:00Z"}]
        assert mod._schedule_cooldown(skoro, now) == timedelta(hours=3, minutes=30)

        # Считается БЛИЖАЙШИЙ будущий, а не первый по списку.
        vperemeshku = [
            {"utc_time": "2026-08-25T18:00:00Z"},
            {"utc_time": "2026-08-20T18:00:00Z"},
        ]
        assert mod._schedule_cooldown(vperemeshku, now) == timedelta(hours=12)

    @pytest.mark.unit
    def test_schedule_cooldown_ignores_settled_and_undated_matches(self):
        """Закрытый матч обязательством не делает, мусорная дата — тоже.

        `postponed` и `awarded` парсер сохраняет наравне с `finished`/`cancelled`
        (`scrapers/fotmob/parsers.py:143-146`). Без них прошедший перенесённый или
        присуждённый матч висел бы обязательством вечно, и турнир опрашивался бы
        каждые два часа бесконечно.
        """

        from datetime import datetime, timedelta

        mod = self._module()
        now = datetime(2026, 8, 20, 12, 0)
        proshedshiy = "2026-08-20T09:00:00Z"

        assert mod._schedule_cooldown([], now) == timedelta(hours=48)
        for terminal in ("finished", "cancelled", "postponed", "awarded"):
            assert mod._schedule_cooldown(
                [{"utc_time": proshedshiy, terminal: True}], now
            ) == timedelta(hours=48), terminal
        assert mod._schedule_cooldown([{"utc_time": None}], now) == timedelta(hours=48)
        assert mod._schedule_cooldown([{"utc_time": "не дата"}], now) == timedelta(
            hours=48
        )
        assert mod._schedule_cooldown([{}], now) == timedelta(hours=48)

    @pytest.mark.unit
    def test_closed_current_scope_comes_back_for_the_evening_match(self):
        """Тот же переход «будущий → сыгранный», но через настоящий ран.

        Один и тот же турнир закрывается дважды: в расписании либо только
        давно сыгранный матч (возвращаться незачем — 48 ч), либо ещё и матч
        сегодня вечером (вернуться надо вскоре после него).
        """

        import copy
        from datetime import datetime, timedelta, timezone

        from scrapers.fotmob.planner import RunMode, TransportBudget
        from scrapers.fotmob.repository import MemoryFotMobRepository
        from scrapers.fotmob.service import FotMobIngestService
        from scrapers.fotmob.transport import canonicalize_target
        from tests.unit.scrapers.test_fotmob_service import (
            StubTransport,
            _competition_payload,
        )

        mod = self._module()

        def _attempt_for(*, evening_kickoff):
            payload = _competition_payload(47, "Premier League")
            if evening_kickoff is not None:
                extra = copy.deepcopy(payload["fixtures"]["allMatches"][0])
                extra["id"] = 300
                extra["pageUrl"] = "/matches/alpha-vs-beta/x#300"
                extra["status"]["finished"] = False
                extra["status"]["utcTime"] = evening_kickoff
                payload["fixtures"]["allMatches"].append(extra)
            responses = {
                canonicalize_target("allLeagues").canonical_url: {
                    "countries": [{"leagues": [{"id": 47, "name": "Premier League"}]}]
                },
                canonicalize_target("leagues", {"id": 47}).canonical_url: payload,
                canonicalize_target(
                    "matchDetails", {"matchId": "100"}
                ).canonical_url: {
                    "content": {"matchFacts": {"events": []}, "stats": {"x": 1}}
                },
            }
            service = FotMobIngestService(
                transport=StubTransport(responses),
                repository=MemoryFotMobRepository(),
                mode=RunMode.DAILY,
                budget=TransportBudget(max_requests=100, max_direct_bytes=10_000_000),
                run_id="cooldown-1",
                max_workers=1,
            )
            args = mod._argument_parser().parse_args(
                [
                    "--mode",
                    "refresh",
                    "--catalog-contract",
                    "fotmob-catalog-v1",
                    "--entities",
                    "season,matches",
                    "--run-id",
                    "cooldown-1",
                ]
            )
            _rc, report = _run_native_admitted(mod, args, service=service)
            return {
                attempt["competition_id"]: attempt
                for attempt in report["selection"]["scope_attempts"]
            }[47]

        def _cooldown(attempt):
            # Срок отсчитывается от начала работы над скоупом, а метка попытки
            # ставится в её конце, поэтому номинал недостижим на миллисекунды.
            started = datetime.fromisoformat(attempt["last_attempt_at"])
            due = datetime.fromisoformat(attempt["next_retry_at"])
            return due - started

        # Матч через 9 часов от «сейчас» — срок обязан лечь примерно на +12 ч.
        soon = (datetime.now(timezone.utc) + timedelta(hours=9)).strftime(
            "%Y-%m-%dT%H:%M:%S.000Z"
        )
        settled = _attempt_for(evening_kickoff=None)
        owing = _attempt_for(evening_kickoff=soon)

        assert settled["outcome"] == "success"
        assert owing["outcome"] == "success"
        assert timedelta(hours=47) < _cooldown(settled) <= timedelta(hours=48)
        assert timedelta(hours=11) < _cooldown(owing) <= timedelta(hours=12)

    @pytest.mark.unit
    def test_terminal_scope_is_counted_in_the_run_report(self):
        """Терминальный скоуп обязан быть виден числом, а не только цветом рана.

        Тихое терминальное выпадение турнира (B5) распознаётся по отчёту: если
        terminal только красит ран, но не попадает в scope_outcome_counts,
        мониторинг не отличит «один скоуп выпал» от «полоса не доехала».
        """

        from scrapers.fotmob.planner import RunMode, TransportBudget
        from scrapers.fotmob.repository import MemoryFotMobRepository
        from scrapers.fotmob.service import FotMobIngestService
        from scrapers.fotmob.transport import canonicalize_target
        from tests.unit.scrapers.test_fotmob_service import (
            StubTransport,
            _competition_payload,
        )

        mod = self._module()
        healthy = _competition_payload(48, "Second League")
        healthy["fixtures"]["allMatches"][0]["id"] = 200
        healthy["fixtures"]["allMatches"][0]["pageUrl"] = "/matches/alpha-vs-beta/x#200"
        responses = {
            canonicalize_target("allLeagues").canonical_url: {
                "countries": [
                    {
                        "leagues": [
                            {"id": 47, "name": "Premier League"},
                            {"id": 48, "name": "Second League"},
                        ]
                    }
                ]
            },
            canonicalize_target("leagues", {"id": 47}).canonical_url: (
                _competition_payload(47, "Premier League")
            ),
            canonicalize_target("leagues", {"id": 48}).canonical_url: healthy,
            canonicalize_target("matchDetails", {"matchId": "100"}).canonical_url: {
                "content": {"matchFacts": {"events": []}, "stats": {"x": 1}}
            },
            canonicalize_target("matchDetails", {"matchId": "200"}).canonical_url: {
                "content": {"matchFacts": {"events": []}, "stats": {"x": 1}}
            },
        }

        class BrokenCompletionService(FotMobIngestService):
            """Коммит завершения скоупа 47 срывается — ровно так рождается terminal."""

            def record_scope_completion(self, competition_id, *args, **kwargs):
                if int(competition_id) == 47:
                    raise RuntimeError("commit conflict")
                return super().record_scope_completion(
                    competition_id, *args, **kwargs
                )

        service = BrokenCompletionService(
            transport=StubTransport(dict(responses)),
            repository=MemoryFotMobRepository(),
            mode=RunMode.DAILY,
            budget=TransportBudget(max_requests=100, max_direct_bytes=10_000_000),
            run_id="terminal-1",
            max_workers=2,
        )
        args = mod._argument_parser().parse_args(
            [
                "--mode",
                "refresh",
                "--catalog-contract",
                "fotmob-catalog-v1",
                "--entities",
                "season,matches",
                "--run-id",
                "terminal-1",
            ]
        )

        rc, report = _run_native_admitted(mod, args, service=service)

        outcomes = {
            attempt["competition_id"]: attempt["outcome"]
            for attempt in report["selection"]["scope_attempts"]
        }
        assert outcomes[47] == "terminal"
        assert outcomes[48] == "success"
        assert report["selection"]["scope_outcome_counts"] == {
            "success": 1,
            "terminal": 1,
        }
        # Терминальный исход по-прежнему красит ран — счётчик его не смягчает.
        assert rc == 1
        assert report["status"] == "incomplete"

    @pytest.mark.unit
    def test_automatic_transport_absence_defers_scope_instead_of_killing_it(self):
        """404 по одной цели — повтор, а не смерть скоупа и цвета рана.

        Транспортное 204/404 не доказывает отсутствия сущности: tombstone
        ставит только entity-aware парсер по scoped-доказательству. Значит
        такой исход не может ни красить продвинувшийся ран, ни закрывать
        скоуп терминально с next_retry_at=None. 12.08 ровно так 134 × 404 по
        игрокам унесли скоуп 59=2026 и весь ран целиком (#1169).
        """

        from scrapers.fotmob.planner import RunMode, TransportBudget
        from scrapers.fotmob.repository import MemoryFotMobRepository
        from scrapers.fotmob.service import FotMobIngestService
        from scrapers.fotmob.transport import FetchOutcome, FetchResult, canonicalize_target
        from tests.unit.scrapers.test_fotmob_service import (
            StubTransport,
            _competition_payload,
        )

        mod = self._module()
        healthy = _competition_payload(48, "Second League")
        healthy["fixtures"]["allMatches"][0]["id"] = 200
        healthy["fixtures"]["allMatches"][0]["pageUrl"] = "/matches/alpha-vs-beta/x#200"
        absent_target = canonicalize_target("matchDetails", {"matchId": "100"})
        absent_match = FetchResult(
            outcome=FetchOutcome.NOT_AVAILABLE,
            target_key=absent_target.target_key,
            url=absent_target.canonical_url,
            http_status=404,
            json_data=None,
            body=None,
            attempts=1,
            retries=0,
            cache_hit=False,
            stale=False,
            terminal=True,
            etag=None,
            last_modified=None,
            raw_uri=None,
            content_hash=None,
            fetched_at=None,
            encoded_bytes=0,
            decoded_bytes=0,
            direct_bytes=0,
            proxy_bytes=0,
            error="FotMob returned 404",
        )
        responses = {
            canonicalize_target("allLeagues").canonical_url: {
                "countries": [
                    {
                        "leagues": [
                            {"id": 47, "name": "Premier League"},
                            {"id": 48, "name": "Second League"},
                        ]
                    }
                ]
            },
            canonicalize_target("leagues", {"id": 47}).canonical_url: (
                _competition_payload(47, "Premier League")
            ),
            canonicalize_target("leagues", {"id": 48}).canonical_url: healthy,
            absent_target.canonical_url: absent_match,
            canonicalize_target(
                "matchDetails", {"matchId": "200"}
            ).canonical_url: {
                "content": {"matchFacts": {"events": []}, "stats": {"x": 1}}
            },
        }
        service = FotMobIngestService(
            transport=StubTransport(dict(responses)),
            repository=MemoryFotMobRepository(),
            mode=RunMode.DAILY,
            budget=TransportBudget(max_requests=100, max_direct_bytes=10_000_000),
            run_id="absence-1",
            max_workers=2,
        )
        args = mod._argument_parser().parse_args(
            [
                "--mode",
                "refresh",
                "--catalog-contract",
                "fotmob-catalog-v1",
                "--entities",
                "season,matches",
                "--run-id",
                "absence-1",
            ]
        )

        rc, report = _run_native_admitted(mod, args, service=service)

        attempts = {
            attempt["competition_id"]: attempt
            for attempt in report["selection"]["scope_attempts"]
        }
        assert attempts[47]["outcome"] == "retryable"
        assert attempts[47]["next_retry_at"] is not None
        assert attempts[48]["outcome"] == "success"
        assert rc == 0, report["errors"]
        assert report["status"] == "partial_success"
        assert any(
            "unscoped transport absence (404)" in error for error in report["errors"]
        )
        assert not any(error.startswith("terminal:") for error in report["errors"])

    @pytest.mark.unit
    def test_automatic_competition_budget_rotates_after_repeated_failed_attempts(self):
        from scrapers.fotmob.planner import RunMode, TransportBudget
        from scrapers.fotmob.repository import MemoryFotMobRepository
        from scrapers.fotmob.service import FotMobIngestService
        from scrapers.fotmob.transport import canonicalize_target
        from tests.unit.scrapers.test_fotmob_service import (
            StubTransport,
            _competition_payload,
        )

        mod = self._module()
        payloads = {
            competition_id: _competition_payload(
                competition_id, f"Competition {competition_id}"
            )
            for competition_id in (47, 48, 49)
        }
        payloads[47]["allAvailableSeasons"] = "invalid"
        responses = {
            canonicalize_target("allLeagues").canonical_url: {
                "countries": [
                    {
                        "leagues": [
                            {"id": competition_id, "name": f"Competition {competition_id}"}
                            for competition_id in (47, 48, 49)
                        ]
                    }
                ]
            },
            **{
                canonicalize_target("leagues", {"id": competition_id}).canonical_url: payload
                for competition_id, payload in payloads.items()
            },
        }
        repository = MemoryFotMobRepository()
        attempted_prefixes = []

        def run_once(index, max_requests):
            service = FotMobIngestService(
                transport=StubTransport(dict(responses)),
                repository=repository,
                mode=RunMode.DAILY,
                budget=TransportBudget(
                    max_requests=max_requests,
                    max_direct_bytes=10_000_000,
                ),
                run_id=f"fair-{index}",
                max_workers=3,
            )
            discover = service.discover_competitions

            def capture(candidates, **kwargs):
                attempted_prefixes.append(
                    [item.competition.competition_id for item in candidates]
                )
                return discover(candidates, **kwargs)

            service.discover_competitions = MagicMock(side_effect=capture)
            args = mod._argument_parser().parse_args(
                [
                    "--mode",
                    "refresh",
                    "--catalog-contract",
                    "fotmob-catalog-v1",
                    "--entities",
                    "season",
                    "--run-id",
                    f"fair-{index}",
                ]
            )
            return _run_native_admitted(mod, args, service=service)

        results = [run_once(1, 5), *(run_once(index, 2) for index in range(2, 6))]

        assert attempted_prefixes == [[47], [48], [49], [47], [48]]
        assert results[0][0] == 1  # malformed low-ID root is still a hard failure
        assert repository.latest_entity_attempt("competition_seasons", 47)[
            "status"
        ] == "schema_drift"

    @pytest.mark.unit
    def test_attempt_journal_survives_a_catalog_composition_change(self, monkeypatch):
        """История попыток обязана пережить появление нового турнира в каталоге.

        Контрактная подпись хеширует состав каталога, поэтому в августе, когда
        источник открывает сезоны пачками, она менялась почти каждый ран. Пока
        журнал жил под ней, второй попытки не наступало никогда: дыра источника
        не подтверждалась, скоуп оставался вечно retryable, а обход каждый раз
        начинал с головы очереди.
        """

        from scrapers.fotmob import planner
        from scrapers.fotmob.catalog_contract import catalog_contract_from_dict
        from scrapers.fotmob.planner import RunMode, TransportBudget
        from scrapers.fotmob.repository import MemoryFotMobRepository
        from scrapers.fotmob.service import FotMobIngestService
        from scrapers.fotmob.transport import canonicalize_target
        from tests.unit.scrapers.test_fotmob_service import (
            StubTransport,
            _competition_payload,
            _league_payload,
        )

        mod = self._module()
        missing_match = {"error": True, "message": "Data not found", "matchId": "100"}
        first_catalog = {
            "countries": [{"leagues": [{"id": 47, "name": "Premier League"}]}]
        }
        second_catalog = {
            "countries": [
                {
                    "leagues": [
                        {"id": 47, "name": "Premier League"},
                        {"id": 48, "name": "Championship"},
                    ]
                }
            ]
        }
        base_responses = {
            canonicalize_target("leagues", {"id": 47}).canonical_url: _league_payload(),
            canonicalize_target("leagues", {"id": 48}).canonical_url: (
                _competition_payload(48, "Championship")
            ),
            canonicalize_target(
                "matchDetails", {"matchId": "100"}
            ).canonical_url: missing_match,
        }
        repository = MemoryFotMobRepository()

        def run(catalog, run_id):
            responses = dict(base_responses)
            responses[canonicalize_target("allLeagues").canonical_url] = catalog
            service = FotMobIngestService(
                transport=StubTransport(responses),
                repository=repository,
                mode=RunMode.DAILY,
                budget=TransportBudget(max_requests=200, max_direct_bytes=10_000_000),
                run_id=run_id,
                max_workers=2,
            )
            args = mod._argument_parser().parse_args(
                [
                    "--mode",
                    "refresh",
                    "--catalog-contract",
                    "fotmob-catalog-v1",
                    "--entities",
                    "season,matches",
                    "--run-id",
                    run_id,
                ]
            )
            return _run_native_admitted(mod, args, service=service)

        first_rc, first_report = run(first_catalog, "composition-1")

        assert first_rc == 1
        assert first_report["selection"]["scope_attempts"][0]["outcome"] == "retryable"

        real_datetime = datetime

        class FutureDatetime(datetime):
            @classmethod
            def now(cls, tz=None):
                return real_datetime.now(tz) + timedelta(hours=1)

        monkeypatch.setattr(planner, "datetime", FutureDatetime)
        second_rc, second_report = run(second_catalog, "composition-2")

        first_contract = catalog_contract_from_dict(
            first_report["selection"]["catalog_contract"]
        )
        second_contract = catalog_contract_from_dict(
            second_report["selection"]["catalog_contract"]
        )
        assert first_contract.plan_signature != second_contract.plan_signature

        attempts = {
            (attempt["competition_id"], attempt["source_season_key"]): attempt
            for attempt in second_report["selection"]["scope_attempts"]
        }
        gap = attempts[(47, "2025/2026")]
        assert gap["outcome"] == "source_gap"
        assert gap["attempt_count"] == 2
        # Наружу отчёт отдаёт контрактную подпись, под которой стартовал ран,
        # даже когда состояние прочитано из журнала прошлого рана.
        assert gap["plan_signature"] == second_contract.plan_signature
        assert second_rc == 0, second_report["errors"]

    @pytest.mark.unit
    def test_scope_attempts_evidence_is_bounded_by_the_current_contract(self):
        """Скоуп, выпавший из каталога, не тащит свою историю в отчёт.

        Журнал теперь общий для всех ранов полосы, а отчёт отвечает ровно за то
        обязательство, под которым ран стартовал: чужой скоуп в selection —
        это «scope attempt вне контракта» и красная приёмка.
        """

        from scrapers.fotmob.planner import RunMode, TransportBudget
        from scrapers.fotmob.repository import MemoryFotMobRepository
        from scrapers.fotmob.service import FotMobIngestService
        from scrapers.fotmob.transport import canonicalize_target
        from tests.unit.scrapers.test_fotmob_service import (
            StubTransport,
            _competition_payload,
            _league_payload,
        )

        mod = self._module()
        base_responses = {
            canonicalize_target("leagues", {"id": 47}).canonical_url: _league_payload(),
            canonicalize_target("leagues", {"id": 48}).canonical_url: (
                _competition_payload(48, "Championship")
            ),
        }
        repository = MemoryFotMobRepository()

        def run(leagues, run_id):
            responses = dict(base_responses)
            responses[canonicalize_target("allLeagues").canonical_url] = {
                "countries": [{"leagues": leagues}]
            }
            service = FotMobIngestService(
                transport=StubTransport(responses),
                repository=repository,
                mode=RunMode.DAILY,
                budget=TransportBudget(max_requests=200, max_direct_bytes=10_000_000),
                run_id=run_id,
                max_workers=2,
            )
            args = mod._argument_parser().parse_args(
                [
                    "--mode",
                    "refresh",
                    "--catalog-contract",
                    "fotmob-catalog-v1",
                    "--entities",
                    "season",
                    "--run-id",
                    run_id,
                ]
            )
            return _run_native_admitted(mod, args, service=service)

        first_rc, first_report = run(
            [{"id": 47, "name": "Premier League"}], "bounded-1"
        )
        assert first_rc == 0, first_report["errors"]
        assert [
            attempt["competition_id"]
            for attempt in first_report["selection"]["scope_attempts"]
        ] == [47]

        second_rc, second_report = run(
            [{"id": 48, "name": "Championship"}], "bounded-2"
        )

        assert second_rc == 0, second_report["errors"]
        assert [
            attempt["competition_id"]
            for attempt in second_report["selection"]["scope_attempts"]
        ] == [48]
        assert second_report["selection"]["deferrals"] == []

    @pytest.mark.unit
    def test_journal_signature_separates_the_two_automatic_lanes(self):
        """У полос `current` и `history` разные журналы, и это не случайность."""

        from scrapers.fotmob.catalog_contract import catalog_contract_from_dict
        from scrapers.fotmob.planner import (
            RunMode,
            TransportBudget,
            deterministic_plan_signature,
        )
        from scrapers.fotmob.repository import MemoryFotMobRepository
        from scrapers.fotmob.service import FotMobIngestService
        from scrapers.fotmob.transport import canonicalize_target
        from tests.unit.scrapers.test_fotmob_service import StubTransport, _league_payload

        mod = self._module()
        responses = {
            canonicalize_target("allLeagues").canonical_url: {
                "countries": [{"leagues": [{"id": 47, "name": "Premier League"}]}]
            },
            canonicalize_target("leagues", {"id": 47}).canonical_url: _league_payload(),
            canonicalize_target(
                "leagues", {"id": 47, "season": "2024/2025"}
            ).canonical_url: _league_payload("2024/2025"),
        }
        repository = MemoryFotMobRepository()

        def run(mode, run_id):
            service = FotMobIngestService(
                transport=StubTransport(dict(responses)),
                repository=repository,
                mode=RunMode.DAILY if mode == "refresh" else RunMode.BACKFILL,
                budget=TransportBudget(max_requests=200, max_direct_bytes=10_000_000),
                run_id=run_id,
                max_workers=2,
            )
            args = mod._argument_parser().parse_args(
                [
                    "--mode",
                    mode,
                    "--catalog-contract",
                    "fotmob-catalog-v1",
                    "--entities",
                    "season",
                    "--run-id",
                    run_id,
                ]
            )
            return _run_native_admitted(mod, args, service=service)

        current_rc, current_report = run("refresh", "lane-current")
        history_rc, history_report = run("backfill", "lane-history")

        assert current_rc == 0, current_report["errors"]
        assert history_rc == 0, history_report["errors"]
        assert current_report["selection"]["scope_lane"] == "current"
        assert history_report["selection"]["scope_lane"] == "history"

        contract = catalog_contract_from_dict(
            current_report["selection"]["catalog_contract"]
        )
        journals = {
            lane: deterministic_plan_signature(
                contract.entities,
                policy={**contract.entity_policy, "scope_lane": lane},
            )
            for lane in ("current", "history")
        }
        assert journals["current"] != journals["history"]
        # Сезон, закрытый текущей полосой, историческая не видит как закрытый:
        # у полос раздельная память, иначе после смены сезона история молча
        # пропускала бы то, что собиралось текущей.
        assert repository.completed_scope_keys(journals["current"]) == {
            (47, "2025/2026")
        }
        assert repository.completed_scope_keys(journals["history"]) == {
            (47, "2024/2025")
        }

    @pytest.mark.unit
    def test_stale_deferrals_do_not_leak_into_the_next_runs_evidence(self):
        """Отсрочка прошлого окна не должна становиться доказательством этого.

        Журнал переживает раны, поэтому скоуп, отложенный по бюджету вчера,
        так и лежит в карте состояний со статусом `deferred`. Если отчёт
        возьмёт его из карты, приёмка справедливо ответит «deferred scope
        lacks explicit budget/deadline evidence»: в этом ране такой отсрочки
        не было, обосновать её нечем.
        """

        from scripts.fotmob_catalog_acceptance import validate_report
        from scrapers.fotmob.planner import RunMode, TransportBudget
        from scrapers.fotmob.repository import MemoryFotMobRepository
        from scrapers.fotmob.service import FotMobIngestService
        from scrapers.fotmob.transport import canonicalize_target
        from tests.unit.scrapers.test_fotmob_service import (
            StubTransport,
            _competition_payload,
            _league_payload,
        )

        mod = self._module()
        responses = {
            canonicalize_target("allLeagues").canonical_url: {
                "countries": [
                    {
                        "leagues": [
                            {"id": 47, "name": "Premier League"},
                            {"id": 48, "name": "Championship"},
                        ]
                    }
                ]
            },
            canonicalize_target("leagues", {"id": 47}).canonical_url: _league_payload(),
            canonicalize_target("leagues", {"id": 48}).canonical_url: (
                _competition_payload(48, "Championship")
            ),
        }
        repository = MemoryFotMobRepository()

        def run(run_id, season_limit):
            service = FotMobIngestService(
                transport=StubTransport(dict(responses)),
                repository=repository,
                mode=RunMode.DAILY,
                budget=TransportBudget(max_requests=200, max_direct_bytes=10_000_000),
                run_id=run_id,
                max_workers=2,
            )
            args = mod._argument_parser().parse_args(
                [
                    "--mode",
                    "refresh",
                    "--catalog-contract",
                    "fotmob-catalog-v1",
                    "--entities",
                    "season",
                    "--season-limit",
                    str(season_limit),
                    "--run-id",
                    run_id,
                ]
            )
            return _run_native_admitted(mod, args, service=service)

        first_rc, first_report = run("stale-deferral-1", 1)
        assert first_rc == 0, first_report["errors"]
        first_outcomes = {
            attempt["competition_id"]: attempt["outcome"]
            for attempt in first_report["selection"]["scope_attempts"]
        }
        assert sorted(first_outcomes.values()) == ["deferred", "success"]

        # Второй ран стартует раньше, чем наступил повтор отложенного скоупа:
        # планировать ему нечего, и своих отсрочек у него нет.
        second_rc, second_report = run("stale-deferral-2", 0)

        assert second_rc == 0, second_report["errors"]
        assert second_report["selection"]["deferrals"] == []
        assert second_report["selection"]["scope_attempts"] == []
        accepted = validate_report(second_report, require_full_completion=False)
        assert accepted.ok, accepted.errors

    @pytest.mark.unit
    def test_drained_transfer_lane_reports_durable_coverage(self):
        """Дренированный бэкфил обязан доказать покрытие уже закрытых потоков.

        Под стабильной подписью бэкфил больше не перезабирает завершённые
        трансферы, поэтому список этого рана перестал совпадать с included_ids.
        Приёмка спрашивает именно про обязательство: без накопительного
        доказательства зелёный ран дренированной полосы краснел бы на
        «transfer completion evidence is incomplete».
        """

        from scrapers.fotmob.planner import RunMode, TransportBudget
        from scrapers.fotmob.repository import MemoryFotMobRepository
        from scrapers.fotmob.service import FotMobIngestService
        from scrapers.fotmob.transport import canonicalize_target
        from tests.unit.scrapers.test_fotmob_service import StubTransport, _league_payload

        mod = self._module()
        responses = {
            canonicalize_target("allLeagues").canonical_url: {
                "countries": [{"leagues": [{"id": 47, "name": "Premier League"}]}]
            },
            canonicalize_target("leagues", {"id": 47}).canonical_url: _league_payload(),
            canonicalize_target(
                "leagues", {"id": 47, "season": "2024/2025"}
            ).canonical_url: _league_payload("2024/2025"),
            canonicalize_target(
                "transfers", {"leagueIds": "47", "page": 1}
            ).canonical_url: {"hits": 0, "page": 1, "transfers": []},
        }
        repository = MemoryFotMobRepository()

        def run(run_id):
            service = FotMobIngestService(
                transport=StubTransport(dict(responses)),
                repository=repository,
                mode=RunMode.BACKFILL,
                budget=TransportBudget(max_requests=200, max_direct_bytes=10_000_000),
                run_id=run_id,
                max_workers=2,
            )
            args = mod._argument_parser().parse_args(
                [
                    "--mode",
                    "backfill",
                    "--catalog-contract",
                    "fotmob-catalog-v1",
                    "--entities",
                    "season,transfers",
                    "--run-id",
                    run_id,
                ]
            )
            return _run_native_admitted(mod, args, service=service)

        first_rc, first_report = run("drained-1")
        assert first_rc == 0, first_report["errors"]
        assert first_report["selection"]["completed_transfer_competition_ids"] == [47]

        second_rc, second_report = run("drained-2")

        assert second_rc == 0, second_report["errors"]
        assert second_report["selection"]["completed_transfer_competition_ids"] == [47]

    @pytest.mark.unit
    def test_automatic_transfer_completion_uses_catalog_contract_signature(self):
        from scrapers.fotmob.catalog_contract import catalog_contract_from_dict
        from scrapers.fotmob.planner import deterministic_plan_signature
        from scrapers.fotmob.transport import canonicalize_target
        from tests.unit.scrapers.test_fotmob_service import _league_payload, _service

        mod = self._module()
        responses = {
            canonicalize_target("allLeagues").canonical_url: {
                "countries": [{"leagues": [{"id": 47, "name": "Premier League"}]}]
            },
            canonicalize_target("leagues", {"id": 47}).canonical_url: _league_payload(),
            canonicalize_target(
                "transfers", {"leagueIds": "47", "page": 1, "last": "1year"}
            ).canonical_url: {"hits": 0, "page": 1, "transfers": []},
        }
        service, _, repository = _service(responses)
        args = mod._argument_parser().parse_args(
            [
                "--mode",
                "refresh",
                "--catalog-contract",
                "fotmob-catalog-v1",
                "--entities",
                "season,transfers",
            ]
        )

        rc, report = _run_native_admitted(mod, args, service=service)

        assert rc == 0, report["errors"]
        selection = report["selection"]
        contract = catalog_contract_from_dict(selection["catalog_contract"])
        assert contract.entities == ("season", "transfers")
        assert contract.entity_policy["transfer_policy"] == {
            "window": "1year",
            "pagination": "unique_hits",
            "completion_scope": "included_ids",
            "completion_signature": "catalog_contract",
        }
        assert selection["transfer_plan_signature"] == contract.plan_signature
        assert selection["completed_transfer_competition_ids"] == [47]
        # Отчёт доказывает контракт, а журнал живёт под стабильной подписью:
        # контентная менялась бы при каждом изменении состава каталога и
        # обнуляла бы историю завершений.
        journal_signature = deterministic_plan_signature(
            contract.entities,
            policy={**contract.entity_policy, "scope_lane": "current"},
        )
        assert journal_signature != contract.plan_signature
        assert repository.completed_competition_ids(journal_signature) == {47}
        assert repository.completed_competition_ids(contract.plan_signature) == set()

    @pytest.mark.unit
    def test_direct_cli_requires_exact_publication_and_matching_run_id(
        self, monkeypatch
    ):
        mod = self._module()
        parser = mod._argument_parser()

        with pytest.raises(SystemExit):
            mod._validate_args(parser, parser.parse_args(["--mode", "daily"]))

        publication_args, publication = _publication_cli(monkeypatch)
        daily_args = _daily_cli(monkeypatch)
        args = parser.parse_args(["--mode", "daily", *daily_args, *publication_args])
        assert mod._validate_args(parser, args) == publication

        args = parser.parse_args(
            [
                "--mode",
                "daily",
                *daily_args,
                *publication_args,
                "--run-id",
                "11111111-1111-4111-8111-111111111111",
            ]
        )
        with pytest.raises(SystemExit):
            mod._validate_args(parser, args)

    @pytest.mark.unit
    def test_daily_contract_path_drift_fails_before_writer_or_service(
        self, monkeypatch, tmp_path
    ):
        mod = self._module()
        publication_args, _publication = _publication_cli(monkeypatch)
        daily_args = _daily_cli(monkeypatch)
        alternate = tmp_path / "same-bytes.txt"
        alternate.write_bytes(
            Path(
                daily_args[daily_args.index("--competition-scope-file") + 1]
            ).read_bytes()
        )
        daily_args[daily_args.index("--competition-scope-file") + 1] = str(alternate)
        writer = MagicMock()
        service = MagicMock()
        monkeypatch.setattr(mod, "_native_writer_fence", writer)
        monkeypatch.setattr(mod, "_build_native_service", service)
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "run_fotmob_scraper.py",
                "--mode",
                "daily",
                *daily_args,
                *publication_args,
            ],
        )

        with pytest.raises(SystemExit):
            mod.main()

        writer.assert_not_called()
        service.assert_not_called()

    @pytest.mark.unit
    def test_source_refresh_cli_rejects_profile_or_scope_widening(self, monkeypatch):
        mod = self._module()
        publication_args, publication = _publication_cli(monkeypatch)
        parser = mod._argument_parser()
        args = parser.parse_args(
            ["--mode", "backfill", *_source_refresh_cli(), *publication_args]
        )

        assert mod._validate_args(parser, args) == publication
        assert args.source_refresh_contract["target_count"] == 7
        assert args.source_refresh_contract["player_ids"] == [
            302783,
            798654,
            863822,
            1025603,
            1074750,
            1292100,
            1334842,
        ]

        rejected = (
            ["--source-refresh-profile", "$(touch /tmp/not-allowed)"],
            ["--scope", "47=2026/2027"],
            ["--entities", "players,teams"],
            ["--player-limit", "7"],
            ["--max-requests", "63"],
            ["--next-build-id", "operator-build"],
        )
        base = _source_refresh_cli()
        for replacement in rejected:
            option = replacement[0]
            mutated = list(base)
            if option in mutated:
                mutated[mutated.index(option) + 1] = replacement[1]
            else:
                mutated.extend(replacement)
            with pytest.raises(SystemExit):
                candidate = parser.parse_args(
                    ["--mode", "backfill", *mutated, *publication_args]
                )
                mod._validate_args(parser, candidate)

    @pytest.mark.unit
    def test_source_refresh_runs_only_fixed_players_and_forces_retry_observation(
        self, monkeypatch
    ):
        from scrapers.fotmob.planner import RunMode
        from scrapers.fotmob.service import OperationResult
        from tests.unit.scrapers.test_fotmob_service import _service

        mod = self._module()
        publication_args, _publication = _publication_cli(monkeypatch)
        parser = mod._argument_parser()
        args = parser.parse_args(
            ["--mode", "backfill", *_source_refresh_cli(), *publication_args]
        )
        mod._validate_args(parser, args)
        service, _transport, repository = _service({}, mode=RunMode.BACKFILL)
        repository.ensure_current_views = MagicMock(return_value=[])
        service.discover_catalog = MagicMock(
            side_effect=AssertionError("source refresh must not discover catalog")
        )
        outcomes = [
            {"player_id": player_id, "status": "success"}
            for player_id in args.source_refresh_contract["player_ids"]
        ]
        service.sync_player_snapshots = MagicMock(
            return_value=OperationResult(
                "player_snapshots",
                attempted=7,
                succeeded=7,
                metadata={"terminal_outcomes": outcomes},
            )
        )

        rc, report = _run_native_admitted(mod, args, service=service)

        assert rc == 0, report["errors"]
        assert report["selection"]["entities"] == ["players"]
        assert report["selection"]["explicit_scopes"] == []
        assert len(report["selection"]["target_outcomes"]) == 7
        service.sync_player_snapshots.assert_called_once_with(
            args.source_refresh_contract["player_ids"],
            force_refresh=True,
            capture_terminal_outcomes=True,
        )
        service.discover_catalog.assert_not_called()

    @pytest.mark.unit
    def test_player_collector_cli_is_exact_and_rejects_widening(self, monkeypatch):
        from scrapers.fotmob.player_collector import PLAYER_COLLECTOR_MODE

        mod = self._module()
        publication_args, publication = _publication_cli(monkeypatch)
        parser = mod._argument_parser()
        base = _player_collector_cli()
        args = parser.parse_args(
            ["--mode", PLAYER_COLLECTOR_MODE, *base, *publication_args]
        )

        assert mod._validate_args(parser, args) == publication

        rejected = (
            ["--scope", "47=2026/2027"],
            ["--catalog-contract", "fotmob-catalog-v1"],
            ["--entities", "players,teams"],
            ["--competition-limit", "1"],
            ["--season-limit", "1"],
            ["--match-limit", "1"],
            ["--team-limit", "1"],
            ["--player-limit", "99"],
            ["--max-requests", "499"],
            ["--max-direct-mib", "65"],
            ["--requests-per-minute", "31"],
            ["--max-attempts", "3"],
            ["--workers", "5"],
            ["--next-build-id", "operator-build"],
            ["--deadline", "2026-08-22T00:00:00Z"],
            ["--daily-contract", "fotmob-daily-v1"],
        )
        for replacement in rejected:
            option = replacement[0]
            mutated = list(base)
            if option in mutated:
                mutated[mutated.index(option) + 1] = replacement[1]
            else:
                mutated.extend(replacement)
            with pytest.raises(SystemExit):
                candidate = parser.parse_args(
                    [
                        "--mode",
                        PLAYER_COLLECTOR_MODE,
                        *mutated,
                        *publication_args,
                    ]
                )
                mod._validate_args(parser, candidate)

    @pytest.mark.unit
    def test_player_collector_fetches_only_selected_missing_players(self, monkeypatch):
        from scrapers.fotmob.player_collector import (
            PLAYER_COLLECTOR_MODE,
            PLAYER_COLLECTOR_PROFILE,
            player_collector_ids_sha256,
            player_collector_plan_signature,
        )
        from scrapers.fotmob.planner import RunMode
        from scrapers.fotmob.service import OperationResult
        from tests.unit.scrapers.test_fotmob_service import _service

        mod = self._module()
        publication_args, _publication = _publication_cli(monkeypatch)
        parser = mod._argument_parser()
        args = parser.parse_args(
            [
                "--mode",
                PLAYER_COLLECTOR_MODE,
                *_player_collector_cli(),
                *publication_args,
            ]
        )
        mod._validate_args(parser, args)
        service, _transport, repository = _service({}, mode=RunMode.DAILY)
        repository.missing_current_squad_player_ids = MagicMock(return_value=[21, 40])
        repository.ensure_current_views = MagicMock(return_value=[])
        service.discover_catalog = MagicMock(
            side_effect=AssertionError("player collector must not discover catalog")
        )
        outcomes = [
            {"player_id": 21, "status": "success"},
            {"player_id": 40, "status": "not_available"},
        ]
        service.sync_player_snapshots = MagicMock(
            return_value=OperationResult(
                "player_snapshots",
                attempted=2,
                succeeded=1,
                not_available=1,
                metadata={
                    "terminal_outcomes": outcomes,
                    "intentional_not_available": 1,
                    "typed_snapshot_writes": 1,
                },
            )
        )

        rc, report = _run_native_admitted(mod, args, service=service)

        assert rc == 0, report["errors"]
        assert report["mode"] == PLAYER_COLLECTOR_MODE
        assert report["selection"] == {
            "profile": PLAYER_COLLECTOR_PROFILE,
            "entities": ["players"],
            "explicit_scopes": [],
            "competition_limit": 0,
            "season_limit": 0,
            "planned_scopes": [],
            "completed_scopes": [],
            "completed_transfer_competition_ids": [],
            "requests_per_minute": args.requests_per_minute,
            "scope_plan_signature": player_collector_plan_signature([21, 40]),
            "player_collector": {
                "profile": PLAYER_COLLECTOR_PROFILE,
                "player_ids": [21, 40],
                "player_count": 2,
                "player_ids_sha256": player_collector_ids_sha256([21, 40]),
                "player_limit": args.player_limit,
            },
            "target_outcomes": outcomes,
        }
        repository.missing_current_squad_player_ids.assert_called_once_with(
            args.player_limit
        )
        service.sync_player_snapshots.assert_called_once_with(
            [21, 40],
            force_refresh=True,
            repair_missing_snapshot=True,
            capture_terminal_outcomes=True,
        )
        service.discover_catalog.assert_not_called()

    @pytest.mark.unit
    def test_player_collector_empty_backlog_is_green_without_network(self, monkeypatch):
        from scrapers.fotmob.player_collector import PLAYER_COLLECTOR_MODE
        from scrapers.fotmob.planner import RunMode
        from tests.unit.scrapers.test_fotmob_service import _service

        mod = self._module()
        publication_args, _publication = _publication_cli(monkeypatch)
        parser = mod._argument_parser()
        args = parser.parse_args(
            [
                "--mode",
                PLAYER_COLLECTOR_MODE,
                *_player_collector_cli(),
                *publication_args,
            ]
        )
        mod._validate_args(parser, args)
        service, transport, repository = _service({}, mode=RunMode.DAILY)
        repository.missing_current_squad_player_ids = MagicMock(return_value=[])
        repository.ensure_current_views = MagicMock(return_value=[])
        service.discover_catalog = MagicMock(
            side_effect=AssertionError("player collector must not discover catalog")
        )

        rc, report = _run_native_admitted(mod, args, service=service)

        assert rc == 0, report["errors"]
        player_operation = next(
            operation
            for operation in report["operations"]
            if operation["entity"] == "player_snapshots"
        )
        assert player_operation["attempted"] == 0
        assert report["selection"]["player_collector"]["player_ids"] == []
        assert transport.calls == []
        service.discover_catalog.assert_not_called()

    @pytest.mark.unit
    @pytest.mark.parametrize("inject_service", [False, True])
    def test_native_service_cannot_run_outside_writer_guard(
        self, monkeypatch, inject_service
    ):
        mod = self._module()
        publication_args, publication = _publication_cli(monkeypatch)
        daily_args = _daily_cli(monkeypatch)
        args = mod._argument_parser().parse_args(
            ["--mode", "daily", *daily_args, *publication_args]
        )
        args.publication_generation_id = publication["generation_id"]
        build = MagicMock()
        service = MagicMock() if inject_service else None
        monkeypatch.setattr(mod, "_build_native_service", build)

        with pytest.raises(RuntimeError, match="exact active publication"):
            mod._run_native(args, service=service)

        build.assert_not_called()
        if service is not None:
            assert service.mock_calls == []

    @pytest.mark.unit
    def test_native_writer_guard_verifies_exact_active_control_state(self, monkeypatch):
        mod = self._module()
        publication_args, publication = _publication_cli(monkeypatch)
        daily_args = _daily_cli(monkeypatch)
        args = mod._argument_parser().parse_args(
            ["--mode", "daily", *daily_args, *publication_args]
        )
        assert mod._validate_args(mod._argument_parser(), args) == publication
        events = []

        @contextmanager
        def guard(run_id, *, source):
            events.append(("enter", run_id, source))
            yield {
                "generation_id": publication["generation_id"],
                "source": "fotmob",
                "binding": publication["binding"],
                "status": "running",
                "phase": "writing",
                "active": True,
            }
            events.append(("exit", run_id, source))

        from scrapers.fbref.control import ControlStore

        monkeypatch.setattr(
            ControlStore,
            "from_env",
            lambda: SimpleNamespace(guard_publication_writer=guard),
        )

        with mod._native_writer_fence(publication):
            assert mod._ACTIVE_PUBLICATION_GENERATION == publication["generation_id"]
            events.append(("write",))

        assert mod._ACTIVE_PUBLICATION_GENERATION is None
        assert events == [
            ("enter", publication["generation_id"], "fotmob"),
            ("write",),
            ("exit", publication["generation_id"], "fotmob"),
        ]

    @pytest.mark.unit
    @pytest.mark.parametrize(
        ("violation", "message"),
        [
            ("source", "source mismatch"),
            ("runtime", "binding mismatch"),
            ("inactive", "publication lock is inactive"),
        ],
    )
    def test_native_writer_guard_rejects_wrong_control_state(
        self, monkeypatch, violation, message
    ):
        mod = self._module()
        _publication_args, publication = _publication_cli(monkeypatch)
        state = {
            "generation_id": publication["generation_id"],
            "source": "fotmob",
            "binding": publication["binding"],
            "status": "running",
            "phase": "writing",
            "active": True,
        }
        if violation == "source":
            state["source"] = "fbref"
        elif violation == "runtime":
            state["binding"] = {
                **publication["binding"],
                "runtime_fingerprint": "b" * 40,
            }
        else:
            state["active"] = False

        @contextmanager
        def guard(_run_id, *, source):
            assert source == "fotmob"
            yield state

        from scrapers.fbref.control import ControlStore

        monkeypatch.setattr(
            ControlStore,
            "from_env",
            lambda: SimpleNamespace(guard_publication_writer=guard),
        )

        with pytest.raises(RuntimeError, match=message):
            with mod._native_writer_fence(publication):
                pytest.fail("mismatched publication reached native writer")

    @pytest.mark.unit
    def test_salvage_flush_stays_inside_writer_guard(self, monkeypatch):
        mod = self._module()
        _publication_args, publication = _publication_cli(monkeypatch)
        events = []

        @contextmanager
        def fence(_publication):
            events.append("guard_enter")
            yield {}
            events.append("guard_exit")

        def fail(_args):
            events.append("native_write")
            raise RuntimeError("write failed")

        monkeypatch.setattr(mod, "_native_writer_fence", fence)
        monkeypatch.setattr(
            mod,
            "_attest_native_runtime",
            lambda *_args: events.append("runtime_attestation"),
        )
        monkeypatch.setattr(mod, "_run_native", fail)
        monkeypatch.setattr(mod, "_salvage_flush", lambda: events.append("salvage"))

        rc, payload = mod._run_native_under_fence(
            SimpleNamespace(run_id=publication["generation_id"], mode="daily"),
            publication,
        )

        assert rc == 1
        assert payload["complete"] is False
        assert events == [
            "runtime_attestation",
            "guard_enter",
            "native_write",
            "salvage",
            "runtime_attestation",
            "guard_exit",
        ]

    @pytest.mark.unit
    def test_shared_owner_attests_runtime_before_writer_guard(self, monkeypatch):
        mod = self._module()
        from utils import fotmob_publication

        evidence = {"deployment_id": "e" * 32, "owner": "shared"}
        attest = MagicMock(return_value=evidence)
        monkeypatch.setattr(
            fotmob_publication,
            "attest_fotmob_shared_runtime",
            attest,
        )

        assert (
            mod._attest_native_runtime(
                SimpleNamespace(),
                {"binding": {"owner": "shared"}},
            )
            == evidence
        )
        attest.assert_called_once_with(require_scheduled_owner=False)

    @pytest.mark.unit
    def test_post_operation_runtime_drift_fails_before_guard_release(self, monkeypatch):
        mod = self._module()
        _publication_args, publication = _publication_cli(monkeypatch)
        events = []

        @contextmanager
        def fence(_publication):
            events.append("guard_enter")
            try:
                yield {}
            finally:
                events.append("guard_exit")

        def attest(*_args):
            events.append("runtime_attestation")
            if events.count("runtime_attestation") == 2:
                raise RuntimeError("post-operation runtime drift")
            return {}

        def run(_args):
            events.append("native_write")
            return 0, {"status": "success", "complete": True}

        monkeypatch.setattr(mod, "_native_writer_fence", fence)
        monkeypatch.setattr(mod, "_attest_native_runtime", attest)
        monkeypatch.setattr(mod, "_run_native", run)

        with pytest.raises(RuntimeError, match="post-operation runtime drift"):
            mod._run_native_under_fence(
                SimpleNamespace(run_id=publication["generation_id"], mode="daily"),
                publication,
            )

        assert events == [
            "runtime_attestation",
            "guard_enter",
            "native_write",
            "runtime_attestation",
            "guard_exit",
        ]

    @pytest.mark.unit
    def test_rejected_writer_guard_never_runs_or_salvages(self, monkeypatch):
        mod = self._module()
        _publication_args, publication = _publication_cli(monkeypatch)

        @contextmanager
        def rejected(_publication):
            raise RuntimeError("rejected before write")
            yield  # pragma: no cover

        run = MagicMock()
        salvage = MagicMock()
        monkeypatch.setattr(mod, "_native_writer_fence", rejected)
        monkeypatch.setattr(mod, "_attest_native_runtime", lambda *_args: {})
        monkeypatch.setattr(mod, "_run_native", run)
        monkeypatch.setattr(mod, "_salvage_flush", salvage)

        with pytest.raises(RuntimeError, match="rejected before write"):
            mod._run_native_under_fence(
                SimpleNamespace(run_id=publication["generation_id"], mode="daily"),
                publication,
            )

        run.assert_not_called()
        salvage.assert_not_called()

    @pytest.mark.unit
    def test_players_entity_automatically_includes_team_squad_discovery(self):
        mod = self._module()

        assert mod._parse_native_entities("players") == frozenset({"teams", "players"})

    @pytest.mark.unit
    def test_unexplained_not_available_remains_outstanding(self):
        from scrapers.fotmob.service import OperationResult

        mod = self._module()
        unexplained = OperationResult("match_payloads", attempted=1, not_available=1)
        intentional = OperationResult(
            "leaderboards",
            attempted=1,
            not_available=1,
            metadata={"intentional_not_available": 1},
        )

        assert mod._outstanding_targets(unexplained) == 1
        assert mod._outstanding_targets(intentional) == 0

    @pytest.mark.unit
    def test_native_startup_failure_writes_incomplete_report_and_exits_nonzero(
        self, monkeypatch, tmp_path
    ):
        mod = self._module()
        output = tmp_path / "native-report.json"
        publication_args, publication = _publication_cli(monkeypatch)
        daily_args = _daily_cli(monkeypatch)
        sys.argv = [
            "run_fotmob_scraper.py",
            "--mode",
            "daily",
            *daily_args,
            "--output",
            str(output),
            *publication_args,
        ]

        @contextmanager
        def admitted(_publication):
            mod._ACTIVE_PUBLICATION_GENERATION = publication["generation_id"]
            try:
                yield {}
            finally:
                mod._ACTIVE_PUBLICATION_GENERATION = None

        monkeypatch.setattr(mod, "_native_writer_fence", admitted)
        monkeypatch.setattr(mod, "_attest_native_runtime", lambda *_args: {})

        with patch.object(
            mod,
            "_build_native_service",
            side_effect=RuntimeError("raw store unavailable"),
        ):
            rc = mod.main()

        assert rc == 1
        payload = json.loads(output.read_text(encoding="utf-8"))
        assert payload["status"] == "incomplete"
        assert payload["complete"] is False
        assert payload["errors"] == ["RuntimeError: raw store unavailable"]
        assert not list(tmp_path.glob(".native-report.json.*.tmp"))

    @pytest.mark.unit
    def test_daily_native_discovers_and_syncs_exact_scope(self):
        from scrapers.fotmob.transport import canonicalize_target
        from tests.unit.scrapers.test_fotmob_service import (
            _league_payload,
            _service,
        )

        mod = self._module()
        all_leagues = {
            "countries": [
                {
                    "ccode": "ENG",
                    "name": "England",
                    "leagues": [{"id": 47, "name": "Premier League"}],
                }
            ]
        }
        leaderboard = {
            "TopLists": [
                {
                    "Title": "Goals",
                    "StatName": "goals",
                    "StatList": [],
                }
            ]
        }
        responses = {
            canonicalize_target("allLeagues").canonical_url: all_leagues,
            canonicalize_target("leagues", {"id": 47}).canonical_url: _league_payload(),
            canonicalize_target(
                "leagues", {"id": 47, "season": "2025/2026"}
            ).canonical_url: _league_payload(),
            "https://data.fotmob.com/stats/47/season/goals.json": leaderboard,
        }
        service, transport, repository = _service(responses)
        repository.ensure_current_views = MagicMock(return_value=[])
        args = mod._argument_parser().parse_args(
            [
                "--mode",
                "daily",
                "--scope",
                "47=2025/2026",
                "--entities",
                "season,leaderboards",
            ]
        )

        rc, report = _run_native_admitted(mod, args, service=service)

        assert rc == 0
        assert report["status"] == "success"
        assert report["complete"] is True
        assert mod._ACTIVE_NATIVE_SERVICE is None
        assert report["selection"]["explicit_scopes"] == ["47=2025/2026"]
        assert report["transport"]["proxy_bytes"] == 0
        # The no-season discovery response selected this exact scope, so the
        # season ingest reuses its committed raw payload instead of issuing a
        # duplicate leagues?id=47&season=... request.
        assert any(url.endswith("leagues?id=47") for url, _ in transport.calls)
        assert not any("season=" in url for url, _ in transport.calls)
        repository.ensure_current_views.assert_called_once_with()

    @pytest.mark.unit
    def test_backfill_skips_scope_completed_by_any_earlier_run(self):
        from scrapers.fotmob.transport import canonicalize_target
        from tests.unit.scrapers.test_fotmob_service import (
            _league_payload,
            _service,
        )

        mod = self._module()
        responses = {
            canonicalize_target("allLeagues").canonical_url: {
                "countries": [{"leagues": [{"id": 47, "name": "Premier League"}]}]
            },
            canonicalize_target("leagues", {"id": 47}).canonical_url: _league_payload(),
        }
        service, transport, repository = _service(responses)
        repository.completed_scope_keys = MagicMock(return_value={(47, "2025/2026")})
        args = mod._argument_parser().parse_args(
            ["--mode", "backfill", "--scope", "47=2025/2026"]
        )

        rc, report = _run_native_admitted(mod, args, service=service)

        assert rc == 0
        assert report["status"] == "success"
        # No run_id: the drain must resume where the previous DagRun stopped.
        repository.completed_scope_keys.assert_called_once_with(
            report["selection"]["scope_plan_signature"],
        )
        assert not any("season=2025%2F2026" in url for url, _ in transport.calls)

    @pytest.mark.unit
    def test_new_backfill_generation_resumes_after_prior_generation(self):
        from scrapers.fotmob.planner import (
            RunMode,
            deterministic_plan_signature,
        )
        from scrapers.fotmob.repository import ManifestStatus, TargetCommit
        from scrapers.fotmob.transport import canonicalize_target
        from tests.unit.scrapers.test_fotmob_service import (
            _league_payload,
            _service,
        )

        mod = self._module()
        responses = {
            canonicalize_target("allLeagues").canonical_url: {
                "countries": [{"leagues": [{"id": 47, "name": "Premier League"}]}]
            },
            canonicalize_target("leagues", {"id": 47}).canonical_url: (
                _league_payload()
            ),
            canonicalize_target(
                "leagues", {"id": 47, "season": "2025/2026"}
            ).canonical_url: _league_payload(),
        }
        service, transport, repository = _service(responses, mode=RunMode.BACKFILL)
        signature = deterministic_plan_signature(
            {"season"},
            policy={
                "match_policy": "finished_only",
                "leaderboard_policy": "all_advertised",
                "team_policy": "global_observed_snapshot",
                "player_policy": "global_observed_snapshot",
            },
        )
        repository.record(
            TargetCommit(
                run_id="prior-generation",
                target_type="scope_completion",
                target_key="a" * 64,
                status=ManifestStatus.SUCCESS,
                competition_id="47",
                source_season_key="2025/2026",
                entity_id=signature,
                content_hash="b" * 64,
            )
        )
        args = mod._argument_parser().parse_args(
            [
                "--mode",
                "backfill",
                "--scope",
                "47=2025/2026",
                "--entities",
                "season",
            ]
        )

        rc, report = _run_native_admitted(mod, args, service=service)

        # The publication generation_id is a fresh uuid on every DagRun, so
        # scoping the resume set to it made the @continuous drain replan the
        # same first chunk forever. A scope committed by an earlier generation
        # is done: this run plans nothing and spends no season request.
        assert rc == 0, report["errors"]
        assert report["selection"]["planned_scopes"] == []
        assert report["selection"]["completed_scopes"] == []
        assert repository.completed_scope_keys(signature) == {(47, "2025/2026")}
        assert not any("season=2025%2F2026" in url for url, _ in transport.calls)

    @pytest.mark.unit
    def test_transfer_competition_limit_applies_after_completion_filter(self):
        from scrapers.fotmob.transport import canonicalize_target
        from tests.unit.scrapers.test_fotmob_service import (
            _competition_payload,
            _league_payload,
            _service,
        )

        mod = self._module()
        responses = {
            canonicalize_target("allLeagues").canonical_url: {
                "countries": [
                    {
                        "leagues": [
                            {"id": 47, "name": "Premier League"},
                            {"id": 48, "name": "Competition 48"},
                        ]
                    }
                ]
            },
            canonicalize_target("leagues", {"id": 47}).canonical_url: _league_payload(),
            canonicalize_target("leagues", {"id": 48}).canonical_url: (
                _competition_payload(48, "Competition 48")
            ),
            canonicalize_target(
                "transfers", {"leagueIds": "48", "page": 1}
            ).canonical_url: {"hits": 0, "page": 1, "transfers": []},
        }
        service, transport, repository = _service(responses, mode="backfill")
        repository.completed_scope_keys = MagicMock(
            return_value={(47, "2025/2026"), (47, "2024/2025")}
        )
        repository.completed_competition_ids = MagicMock(return_value={47})
        repository.ensure_current_views = MagicMock(return_value=[])
        args = mod._argument_parser().parse_args(
            [
                "--mode",
                "backfill",
                "--entities",
                "transfers",
                "--competition-limit",
                "1",
            ]
        )

        rc, report = _run_native_admitted(mod, args, service=service)

        assert rc == 0, report["errors"]
        transfer_calls = [url for url, _ in transport.calls if "/transfers?" in url]
        assert transfer_calls == [
            canonicalize_target(
                "transfers", {"leagueIds": "48", "page": 1}
            ).canonical_url
        ]
        assert repository.completed_scope_keys.call_args.kwargs == {}
        assert repository.completed_competition_ids.call_args.kwargs == {}

    @pytest.mark.unit
    def test_players_receive_deduplicated_ids_from_team_snapshots(self):
        from scrapers.fotmob.service import OperationResult
        from scrapers.fotmob.transport import canonicalize_target
        from tests.unit.scrapers.test_fotmob_service import (
            _league_payload,
            _service,
        )

        mod = self._module()
        responses = {
            canonicalize_target("allLeagues").canonical_url: {
                "countries": [{"leagues": [{"id": 47, "name": "Premier League"}]}]
            },
            canonicalize_target("leagues", {"id": 47}).canonical_url: _league_payload(),
            canonicalize_target(
                "leagues", {"id": 47, "season": "2025/2026"}
            ).canonical_url: _league_payload(),
        }
        service, _, _ = _service(responses)
        service.sync_team_snapshots = MagicMock(
            return_value=(OperationResult("team_snapshots", succeeded=2), {10, 11})
        )
        service.sync_player_snapshots = MagicMock(
            return_value=OperationResult("player_snapshots", succeeded=2)
        )
        args = mod._argument_parser().parse_args(
            [
                "--mode",
                "daily",
                "--scope",
                "47=2025/2026",
                "--entities",
                "players",
                "--next-build-id",
                "build-1",
            ]
        )

        rc, report = _run_native_admitted(mod, args, service=service)

        assert rc == 0
        assert report["selection"]["entities"] == ["players", "teams"]
        service.sync_player_snapshots.assert_called_once_with(
            {10, 11}, build_id="build-1", limit=2
        )

    @pytest.mark.unit
    def test_replay_player_batch_is_not_truncated_by_network_request_budget(self):
        from scrapers.fotmob.planner import BudgetLedger, RunMode, TransportBudget
        from scrapers.fotmob.service import OperationResult
        from scrapers.fotmob.transport import canonicalize_target
        from tests.unit.scrapers.test_fotmob_service import (
            _league_payload,
            _service,
        )

        mod = self._module()
        responses = {
            canonicalize_target("allLeagues").canonical_url: {
                "countries": [{"leagues": [{"id": 47, "name": "Premier League"}]}]
            },
            canonicalize_target("leagues", {"id": 47}).canonical_url: (
                _league_payload()
            ),
            canonicalize_target(
                "leagues", {"id": 47, "season": "2025/2026"}
            ).canonical_url: _league_payload(),
        }
        service, _, _ = _service(responses, mode=RunMode.REPLAY)
        service.ledger = BudgetLedger(
            TransportBudget(max_requests=1, max_direct_bytes=10_000_000)
        )
        player_ids = set(range(10_000, 10_250))
        service.sync_team_snapshots = MagicMock(
            return_value=(
                OperationResult("team_snapshots", attempted=1, succeeded=1),
                player_ids,
            )
        )
        service.sync_player_snapshots = MagicMock(
            return_value=OperationResult(
                "player_snapshots",
                attempted=len(player_ids),
                succeeded=len(player_ids),
            )
        )
        args = mod._argument_parser().parse_args(
            [
                "--mode",
                "replay",
                "--scope",
                "47=2025/2026",
                "--entities",
                "players",
                "--max-requests",
                "1",
            ]
        )
        raw_store = SimpleNamespace(has_target=MagicMock(return_value=True))

        rc, report = _run_native_admitted(
            mod, args, service=service, raw_store=raw_store
        )

        assert rc == 0, report["errors"]
        service.sync_player_snapshots.assert_called_once_with(
            player_ids,
            build_id=None,
            limit=len(player_ids),
        )

    @pytest.mark.unit
    def test_backfill_resumes_children_from_raw_before_marking_scope_complete(self):
        from scrapers.fotmob.planner import RunMode, TransportBudget
        from scrapers.fotmob.repository import MemoryFotMobRepository
        from scrapers.fotmob.service import FotMobIngestService
        from scrapers.fotmob.transport import canonicalize_target
        from tests.unit.scrapers.test_fotmob_service import (
            StubTransport,
            _league_payload,
        )

        mod = self._module()
        root = _league_payload()
        historical = _league_payload("2024/2025")
        responses = {
            canonicalize_target("allLeagues").canonical_url: {
                "countries": [{"leagues": [{"id": 47, "name": "Premier League"}]}]
            },
            canonicalize_target("leagues", {"id": 47}).canonical_url: root,
            canonicalize_target(
                "leagues", {"id": 47, "season": "2024/2025"}
            ).canonical_url: historical,
            canonicalize_target("matchDetails", {"matchId": "100"}).canonical_url: {
                "content": {"matchFacts": {"events": []}}
            },
        }
        repository = MemoryFotMobRepository()

        def make_service(max_requests, run_id):
            transport = StubTransport(responses)
            service = FotMobIngestService(
                transport=transport,
                repository=repository,
                mode=RunMode.BACKFILL,
                budget=TransportBudget(
                    max_requests=max_requests,
                    max_direct_bytes=10_000_000,
                ),
                run_id=run_id,
                max_workers=2,
            )
            return service, transport

        args = mod._argument_parser().parse_args(
            [
                "--mode",
                "backfill",
                "--scope",
                "47=2024/2025",
                "--entities",
                "season,matches",
            ]
        )
        first_service, _ = make_service(3, "backfill-1")

        first_rc, first_report = _run_native_admitted(mod, args, service=first_service)

        assert first_rc == 1
        assert first_report["complete"] is False
        signature = first_report["selection"]["scope_plan_signature"]
        assert repository.completed_scope_keys(signature) == set()

        second_service, second_transport = make_service(3, "backfill-2")
        second_rc, second_report = _run_native_admitted(
            mod, args, service=second_service
        )

        assert second_rc == 0
        assert second_report["complete"] is True
        assert repository.completed_scope_keys(signature) == {(47, "2024/2025")}
        historical_calls = [
            replay
            for url, replay in second_transport.calls
            if "season=2024%2F2025" in url
        ]
        assert historical_calls == [True]

    @pytest.mark.unit
    def test_daily_orders_scopes_by_oldest_completion_for_fair_progress(self):
        from scrapers.fotmob.transport import canonicalize_target
        from tests.unit.scrapers.test_fotmob_service import (
            _league_payload,
            _service,
        )

        mod = self._module()
        payload_47 = _league_payload()
        payload_48 = json.loads(json.dumps(payload_47))
        payload_48["details"]["id"] = 48
        payload_48["details"]["name"] = "Competition 48"
        responses = {
            canonicalize_target("allLeagues").canonical_url: {
                "countries": [
                    {
                        "leagues": [
                            {"id": 47, "name": "Premier League"},
                            {"id": 48, "name": "Competition 48"},
                        ]
                    }
                ]
            },
            canonicalize_target("leagues", {"id": 47}).canonical_url: payload_47,
            canonicalize_target("leagues", {"id": 48}).canonical_url: payload_48,
        }
        service, _, repository = _service(responses)
        repository.scope_completion_times = MagicMock(
            return_value={
                (47, "2025/2026"): datetime(2026, 7, 11, 12),
                (48, "2025/2026"): datetime(2026, 7, 10, 12),
            }
        )
        original_sync = service.sync_season
        service.sync_season = MagicMock(side_effect=original_sync)
        args = mod._argument_parser().parse_args(
            [
                "--mode",
                "daily",
                "--scope",
                "47=2025/2026,48=2025/2026",
                "--entities",
                "season",
            ]
        )

        rc, report = _run_native_admitted(mod, args, service=service)

        assert rc == 0 and report["complete"] is True
        assert [call.args[0] for call in service.sync_season.call_args_list] == [
            48,
            47,
        ]

    @pytest.mark.unit
    def test_daily_contract_filters_to_dynamic_current_issue_930_cohort(self):
        from scrapers.fotmob.domain import (
            CompetitionRef,
            ScopeClassification,
            ScopeDecision,
            ScopeRef,
            SeasonBundle,
            SeasonRef,
        )
        from scrapers.fotmob.planner import (
            MANDATORY_COMPETITION_IDS,
            BudgetLedger,
            RunMode,
            TransportBudget,
            plan_seasons,
        )
        from scrapers.fotmob.service import (
            CatalogResult,
            CompetitionDiscoveryResult,
            OperationResult,
            RunReport,
        )
        from utils import fotmob_publication as publication

        mod = self._module()
        cohort = publication.FOTMOB_DAILY_COMPETITION_IDS
        competitions = [
            CompetitionRef(value, f"Competition {value}") for value in (*cohort, 999999)
        ]
        classifications = tuple(
            ScopeClassification(
                competition=item,
                decision=ScopeDecision.INCLUDED,
                reason="test",
                policy_rule="test",
            )
            for item in competitions
        )
        seasons = {
            item.competition_id: SeasonRef(
                item.competition_id,
                f"dynamic-{item.competition_id}",
                is_selected=True,
                is_latest=True,
            )
            for item in competitions
        }
        repository = SimpleNamespace(
            scope_completion_times=MagicMock(return_value={}),
            competition_completion_times=MagicMock(return_value={}),
            flush=MagicMock(return_value=[]),
            ensure_current_views=MagicMock(return_value=[]),
        )
        service = SimpleNamespace(
            transport=SimpleNamespace(max_attempts=1),
            repository=repository,
            ledger=BudgetLedger(
                TransportBudget(
                    max_requests=publication.FOTMOB_DAILY_MAX_REQUESTS,
                    max_direct_bytes=(
                        publication.FOTMOB_DAILY_MAX_DIRECT_MIB * 1024 * 1024
                    ),
                )
            ),
            discover_catalog=MagicMock(
                return_value=CatalogResult(
                    OperationResult(
                        "competition_catalog",
                        attempted=1,
                        succeeded=len(competitions),
                        counts={"competitions": len(competitions)},
                    ),
                    discovery=object(),
                    classifications=classifications,
                )
            ),
        )
        discovered_ids = []

        def discover_competitions(candidates, **_kwargs):
            discovered_ids.extend(
                item.competition.competition_id for item in candidates
            )
            return [
                CompetitionDiscoveryResult(
                    item.competition,
                    item,
                    OperationResult("competition_seasons", attempted=1, succeeded=1),
                    seasons=(seasons[item.competition.competition_id],),
                )
                for item in candidates
            ]

        def sync_season(competition_id, source_season_key, **_kwargs):
            return OperationResult("season_bundle", attempted=1, succeeded=1), (
                SeasonBundle(
                    scope=ScopeRef(competition_id, source_season_key),
                    details={},
                    capabilities={},
                )
            )

        service.discover_competitions = MagicMock(side_effect=discover_competitions)
        service.sync_season = MagicMock(side_effect=sync_season)
        service.sync_leaderboards = MagicMock(
            side_effect=lambda _bundle: OperationResult("leaderboards")
        )
        service.sync_match_payloads = MagicMock(
            side_effect=lambda _bundle, **_kwargs: OperationResult("match_payloads")
        )
        service.sync_team_snapshots = MagicMock(
            side_effect=lambda _bundle, **_kwargs: (
                OperationResult("team_snapshots"),
                set(),
            )
        )
        service.sync_player_snapshots = MagicMock(
            side_effect=lambda _ids, **_kwargs: OperationResult("player_snapshots")
        )
        service.record_scope_completion = MagicMock(return_value=[])
        service.sync_transfers = MagicMock(
            side_effect=lambda competition_id, **_kwargs: OperationResult(
                "transfer_events",
                attempted=1,
                succeeded=1,
                counts={"events": 0},
                metadata={"competition_id": competition_id, "source_hits": 0},
            )
        )
        service.record_competition_completion = MagicMock(return_value=[])
        service.report = MagicMock(
            side_effect=lambda operations, started_at: RunReport(
                run_id="daily-generation",
                mode="daily",
                started_at=started_at,
                completed_at=started_at,
                operations=list(operations),
                budget=service.ledger.as_dict(),
                transport={
                    "attempts": 0,
                    "direct_bytes": 0,
                    "proxy_bytes": 0,
                },
            )
        )
        args = mod._argument_parser().parse_args(
            [
                "--mode",
                "daily",
                "--entities",
                ",".join(publication.FOTMOB_DAILY_ENTITIES),
                "--max-requests",
                str(publication.FOTMOB_DAILY_MAX_REQUESTS),
                "--max-direct-mib",
                str(publication.FOTMOB_DAILY_MAX_DIRECT_MIB),
                "--requests-per-minute",
                str(publication.FOTMOB_DAILY_REQUESTS_PER_MINUTE),
            ]
        )
        args.daily_competition_ids = cohort
        args.daily_competition_contract = {
            "schema": publication.FOTMOB_DAILY_CONTRACT_SCHEMA,
            "competition_ids": list(cohort),
        }

        rc, report = _run_native_admitted(mod, args, service=service)

        assert rc == 0, report["errors"]
        expected_discovery_order = sorted(
            cohort,
            key=lambda value: (
                value not in MANDATORY_COMPETITION_IDS,
                value,
            ),
        )
        assert discovered_ids == expected_discovery_order
        expected_scope_order = [
            item.competition_id
            for item in plan_seasons(
                classifications,
                seasons.values(),
                mode=RunMode.DAILY,
            )
            if item.competition_id in cohort
        ]
        assert report["selection"]["planned_scopes"] == [
            f"{value}=dynamic-{value}" for value in expected_scope_order
        ]
        assert (
            report["selection"]["completed_scopes"]
            == report["selection"]["planned_scopes"]
        )
        assert report["selection"]["completed_transfer_competition_ids"] == list(cohort)
        assert 999999 not in discovered_ids

    @pytest.mark.unit
    def test_unadvertised_exact_scope_is_incomplete(self):
        from scrapers.fotmob.transport import canonicalize_target
        from tests.unit.scrapers.test_fotmob_service import (
            _league_payload,
            _service,
        )

        mod = self._module()
        responses = {
            canonicalize_target("allLeagues").canonical_url: {
                "countries": [{"leagues": [{"id": 47, "name": "Premier League"}]}]
            },
            canonicalize_target("leagues", {"id": 47}).canonical_url: _league_payload(),
        }
        service, _, _ = _service(responses)
        args = mod._argument_parser().parse_args(
            ["--mode", "backfill", "--scope", "47=1900/1901"]
        )

        rc, report = _run_native_admitted(mod, args, service=service)

        assert rc == 1
        assert report["status"] == "incomplete"
        assert any("not advertised" in error for error in report["errors"])

    @pytest.mark.unit
    def test_partially_unadvertised_scopes_are_skipped_not_failed(self):
        # A backfill chunk mixes seasons FotMob advertises with a pair it never
        # ran (competition 42 has no 2026/2027). The absent pair must not paint
        # the whole chunk red — exactly what killed the drain run on 03.08,
        # where five of six scopes had been collected.
        import copy

        from scrapers.fotmob.transport import canonicalize_target
        from tests.unit.scrapers.test_fotmob_service import (
            _league_payload,
            _service,
        )

        mod = self._module()
        other = copy.deepcopy(_league_payload())
        other["details"]["id"] = 42
        other["details"]["name"] = "Competition 42"
        responses = {
            canonicalize_target("allLeagues").canonical_url: {
                "countries": [
                    {
                        "leagues": [
                            {"id": 47, "name": "Premier League"},
                            {"id": 42, "name": "Competition 42"},
                        ]
                    }
                ]
            },
            canonicalize_target("leagues", {"id": 47}).canonical_url: _league_payload(),
            canonicalize_target("leagues", {"id": 42}).canonical_url: other,
        }
        service, _, _ = _service(responses)
        args = mod._argument_parser().parse_args(
            [
                "--mode",
                "backfill",
                "--scope",
                "47=2025/2026,42=2026/2027",
                "--entities",
                "season",
            ]
        )

        rc, report = _run_native_admitted(mod, args, service=service)

        assert rc == 0, report["errors"]
        assert report["status"] == "success"
        assert report["errors"] == []
        assert report["selection"]["completed_scopes"] == ["47=2025/2026"]
        validation = next(
            item
            for item in report["operations"]
            if item["entity"] == "scope_validation"
        )
        assert validation["status"] == "success"
        assert validation["skipped"] == 1
        assert validation["metadata"]["unadvertised_scopes"] == ["42=2026/2027"]

    @pytest.mark.unit
    def test_every_exit_path_flushes_buffered_commits(self):
        # Batched commits live in memory until flushed. finish() is the single
        # exit of _run_native, so a missing flush there silently drops the
        # last targets of every run — including budget-cut runs.
        from scrapers.fotmob.transport import canonicalize_target
        from tests.unit.scrapers.test_fotmob_service import (
            _league_payload,
            _service,
        )

        mod = self._module()
        responses = {
            canonicalize_target("allLeagues").canonical_url: {
                "countries": [{"leagues": [{"id": 47, "name": "Premier League"}]}]
            },
            canonicalize_target("leagues", {"id": 47}).canonical_url: _league_payload(),
            canonicalize_target(
                "leagues", {"id": 47, "season": "2025/2026"}
            ).canonical_url: _league_payload(),
        }
        service, _, repository = _service(responses)
        repository.ensure_current_views = MagicMock(return_value=[])
        repository.flush = MagicMock(return_value=["iceberg.bronze.fotmob_matches"])
        args = mod._argument_parser().parse_args(
            ["--mode", "daily", "--scope", "47=2025/2026", "--entities", "season"]
        )

        rc, report = _run_native_admitted(mod, args, service=service)

        assert rc == 0
        assert repository.flush.call_count == 2

    @pytest.mark.unit
    def test_failed_flush_turns_the_run_red_instead_of_losing_targets(self):
        from scrapers.fotmob.transport import canonicalize_target
        from tests.unit.scrapers.test_fotmob_service import (
            _league_payload,
            _service,
        )

        mod = self._module()
        responses = {
            canonicalize_target("allLeagues").canonical_url: {
                "countries": [{"leagues": [{"id": 47, "name": "Premier League"}]}]
            },
            canonicalize_target("leagues", {"id": 47}).canonical_url: _league_payload(),
            canonicalize_target(
                "leagues", {"id": 47, "season": "2025/2026"}
            ).canonical_url: _league_payload(),
        }
        service, _, repository = _service(responses)
        repository.ensure_current_views = MagicMock(return_value=[])
        repository.flush = MagicMock(side_effect=RuntimeError("catalog down"))
        args = mod._argument_parser().parse_args(
            ["--mode", "daily", "--scope", "47=2025/2026", "--entities", "season"]
        )

        rc, report = _run_native_admitted(mod, args, service=service)

        assert rc == 1
        assert any("commit flush" in error for error in report["errors"])

    @pytest.mark.unit
    def test_escaped_failure_salvages_buffered_commits(self, monkeypatch, tmp_path):
        # finish() covers control-flow exits, but an exception escaping
        # _run_native mid-scope (or SIGTERM converted by the handler) used to
        # drop up to batch_size-1 already-paid-for targets. main() must
        # salvage-flush through the module holder and still write the report.
        mod = self._module()
        service = MagicMock()
        service.repository.flush = MagicMock(
            return_value=["iceberg.bronze.fotmob_matches"]
        )

        def fake_run_native(args, **kwargs):
            mod._ACTIVE_NATIVE_SERVICE = service
            raise RuntimeError("mid-scope Trino failure")

        monkeypatch.setattr(mod, "_run_native", fake_run_native)
        publication_args, _publication = _publication_cli(monkeypatch)
        daily_args = _daily_cli(monkeypatch)
        monkeypatch.setattr(
            mod,
            "_native_writer_fence",
            lambda _publication: nullcontext({}),
        )
        monkeypatch.setattr(mod, "_attest_native_runtime", lambda *_args: {})
        out = tmp_path / "report.json"
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "run_fotmob_scraper.py",
                "--mode",
                "daily",
                *daily_args,
                "--output",
                str(out),
                *publication_args,
            ],
        )

        rc = mod.main()

        assert rc == 1
        service.repository.flush.assert_called_once_with()
        assert mod._ACTIVE_NATIVE_SERVICE is None
        assert json.loads(out.read_text())["complete"] is False

    @pytest.mark.unit
    def test_sigterm_routes_through_failure_report(self, monkeypatch, tmp_path):
        # The driver's unit timeout sends TERM then KILL(+30s): the handler
        # must convert TERM into the ordinary failure path so the unit leaves
        # a report (no more NO_REPORT kills) and flushes its buffer.
        import signal as signal_module

        mod = self._module()
        service = MagicMock()
        service.repository.flush = MagicMock(return_value=[])

        def fake_run_native(args, **kwargs):
            mod._ACTIVE_NATIVE_SERVICE = service
            handler = signal_module.getsignal(signal_module.SIGTERM)
            handler(signal_module.SIGTERM, None)

        monkeypatch.setattr(mod, "_run_native", fake_run_native)
        publication_args, _publication = _publication_cli(monkeypatch)
        daily_args = _daily_cli(monkeypatch)
        monkeypatch.setattr(
            mod,
            "_native_writer_fence",
            lambda _publication: nullcontext({}),
        )
        monkeypatch.setattr(mod, "_attest_native_runtime", lambda *_args: {})
        out = tmp_path / "report.json"
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "run_fotmob_scraper.py",
                "--mode",
                "daily",
                *daily_args,
                "--output",
                str(out),
                *publication_args,
            ],
        )

        try:
            rc = mod.main()
        finally:
            signal_module.signal(signal_module.SIGTERM, signal_module.SIG_DFL)

        assert rc == 1
        service.cancel.assert_called_once_with()
        service.repository.flush.assert_called_once_with()
        assert mod._ACTIVE_NATIVE_SERVICE is None
        payload = json.loads(out.read_text())
        assert any("terminated by signal" in e for e in payload["errors"])

    @pytest.mark.unit
    def test_ceremony_free_cli_accepts_absent_publication_and_rejects_partial(
        self, monkeypatch
    ):
        from utils import fotmob_publication as publication

        mod = self._module()
        parser = mod._argument_parser()
        monkeypatch.delenv(
            publication.FOTMOB_DEPLOYMENT_REPORT_PATH_ENV, raising=False
        )
        monkeypatch.delenv(
            publication.FOTMOB_SHARED_DEPLOYMENT_REPORT_PATH_ENV, raising=False
        )

        args = parser.parse_args(["--mode", "discover"])
        assert mod._validate_args(parser, args) is None
        assert args.run_id == ""

        with pytest.raises(SystemExit):
            mod._validate_args(
                parser,
                parser.parse_args(
                    [
                        "--mode",
                        "discover",
                        "--publication-generation-id",
                        "11111111-1111-4111-8111-111111111111",
                    ]
                ),
            )

    @pytest.mark.unit
    def test_ceremony_free_cli_accepts_owner_stub_and_deployed_env_rejects_it(
        self, monkeypatch
    ):
        from utils import fotmob_publication as publication

        mod = self._module()
        parser = mod._argument_parser()
        monkeypatch.delenv(
            publication.FOTMOB_DEPLOYMENT_REPORT_PATH_ENV, raising=False
        )
        monkeypatch.delenv(
            publication.FOTMOB_SHARED_DEPLOYMENT_REPORT_PATH_ENV, raising=False
        )
        stub_args = [
            "--mode",
            "discover",
            "--publication-generation-id",
            "11111111-1111-5111-8111-111111111111",
            "--publication-schema",
            publication.FOTMOB_PUBLICATION_DISABLED_SCHEMA,
            "--publication-source",
            "fotmob",
            "--publication-owner",
            "isolated",
            "--publication-data-interval-start",
            "2026-07-20T14:00:00+00:00",
            "--publication-data-interval-end",
            "2026-07-21T14:00:00+00:00",
            "--publication-runtime-fingerprint",
            "ceremony-disabled",
        ]
        assert mod._validate_args(parser, parser.parse_args(stub_args)) is None

        monkeypatch.setenv(
            publication.FOTMOB_DEPLOYMENT_REPORT_PATH_ENV, "/deployment-report.json"
        )
        with pytest.raises(SystemExit):
            mod._validate_args(parser, parser.parse_args(stub_args))
        with pytest.raises(SystemExit):
            mod._validate_args(parser, parser.parse_args(["--mode", "discover"]))

    @pytest.mark.unit
    def test_ceremony_free_main_runs_without_fence_or_attestation(
        self, monkeypatch, tmp_path
    ):
        from utils import fotmob_publication as publication

        mod = self._module()
        monkeypatch.delenv(
            publication.FOTMOB_DEPLOYMENT_REPORT_PATH_ENV, raising=False
        )
        monkeypatch.delenv(
            publication.FOTMOB_SHARED_DEPLOYMENT_REPORT_PATH_ENV, raising=False
        )
        fence = MagicMock()
        attest = MagicMock()
        monkeypatch.setattr(mod, "_native_writer_fence", fence)
        monkeypatch.setattr(mod, "_attest_native_runtime", attest)
        monkeypatch.setattr(
            mod,
            "_run_native",
            lambda _args: (0, {"status": "success", "complete": True}),
        )
        out = tmp_path / "report.json"
        monkeypatch.setattr(
            sys,
            "argv",
            ["run_fotmob_scraper.py", "--mode", "discover", "--output", str(out)],
        )

        assert mod.main() == 0

        fence.assert_not_called()
        attest.assert_not_called()
        assert json.loads(out.read_text(encoding="utf-8"))["status"] == "success"

    @pytest.mark.unit
    def test_ceremony_free_stub_never_demands_an_active_writer_guard(
        self, monkeypatch, tmp_path
    ):
        """The owner stub names the run; it is not a writer generation.

        The stub reaches the runner on argv, but no fence is acquired for it, so
        ``_ACTIVE_PUBLICATION_GENERATION`` stays ``None``. Leaving the stub id on
        ``args`` made ``_run_native`` refuse to build the service — the first
        real daily wave after the ceremony-free rotation died on
        "requires its exact active publication writer guard" with the whole
        catalog untouched. The existing ceremony-free coverage stubs out
        ``_run_native`` wholesale and cannot see that.
        """
        from utils import fotmob_publication as publication

        mod = self._module()
        monkeypatch.delenv(
            publication.FOTMOB_DEPLOYMENT_REPORT_PATH_ENV, raising=False
        )
        monkeypatch.delenv(
            publication.FOTMOB_SHARED_DEPLOYMENT_REPORT_PATH_ENV, raising=False
        )
        stub_id = "cf43bcd4-8ee0-5069-b615-48f7b347edbc"
        seen: dict[str, Any] = {}

        def run_native(args, **_kwargs):
            seen["publication_generation_id"] = args.publication_generation_id
            seen["run_id"] = args.run_id
            # The invariant `_run_native` itself enforces, verbatim.
            assert mod._ACTIVE_PUBLICATION_GENERATION == (
                getattr(args, "publication_generation_id", None) or None
            )
            return 0, {"status": "success", "complete": True}

        monkeypatch.setattr(mod, "_run_native", run_native)
        out = tmp_path / "report.json"
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "run_fotmob_scraper.py",
                "--mode",
                "discover",
                "--output",
                str(out),
                "--publication-generation-id",
                stub_id,
                "--publication-schema",
                publication.FOTMOB_PUBLICATION_DISABLED_SCHEMA,
                "--publication-source",
                "fotmob",
                "--publication-owner",
                "isolated",
                "--publication-data-interval-start",
                "2026-07-27T14:00:00+00:00",
                "--publication-data-interval-end",
                "2026-07-28T14:00:00+00:00",
                "--publication-runtime-fingerprint",
                "ceremony-disabled",
            ],
        )

        assert mod.main() == 0
        assert seen["publication_generation_id"] is None
        # The stub still gives the run a retry-stable identity.
        assert seen["run_id"] == stub_id


class _FakeCursor:
    def __init__(self, acquired, executed):
        self._acquired = acquired
        self._executed = executed

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def execute(self, sql, params=None):
        self._executed.append((sql, params))

    def fetchone(self):
        return (self._acquired,)


class _FakeConnection:
    def __init__(self, acquired, executed):
        self.autocommit = False
        self.closed = False
        self._acquired = acquired
        self._executed = executed

    def cursor(self):
        return _FakeCursor(self._acquired, self._executed)

    def close(self):
        self.closed = True


def _fake_psycopg2(monkeypatch, mod, *, acquired, executed, connections):
    def connect(dsn):
        connections.append(dsn)
        return _FakeConnection(acquired, executed)

    monkeypatch.setitem(
        sys.modules, "psycopg2", SimpleNamespace(connect=connect)
    )
    monkeypatch.setattr(
        "scrapers.fbref.control.store.resolve_control_db_uri",
        lambda env=None: "postgresql://airflow@metadb:5432/airflow",
    )
    return mod


class TestFotmobWriterLock:
    """B7: право записи в bronze захватывается атомарно и без ожидания.

    max_active_runs=1 сериализует только DagRun'ы одного дага; ручной добор и
    осиротевший скрапер писали в те же таблицы мимо неё, а ретрая на конфликт
    коммита у FotMob нет.
    """

    @staticmethod
    def _module():
        sys.modules.pop("dags.scripts.run_fotmob_scraper", None)
        return importlib.import_module("dags.scripts.run_fotmob_scraper")

    @pytest.mark.unit
    def test_lock_is_taken_without_waiting_and_released_by_closing(
        self, monkeypatch
    ):
        mod = self._module()
        monkeypatch.setenv(mod.WRITER_LOCK_ENV, "1")
        executed: list[tuple[str, Any]] = []
        connections: list[str] = []
        _fake_psycopg2(
            monkeypatch, mod, acquired=True, executed=executed, connections=connections
        )
        held = None

        with mod._writer_lock() as acquired:
            held = acquired
            statements = list(executed)

        assert held is True
        # Именно НЕблокирующий вариант: pg_advisory_lock ждал бы второго
        # писателя вместо мгновенного отказа.
        assert statements == [
            ("SELECT pg_try_advisory_lock(%s)", (mod._WRITER_LOCK_KEY,))
        ]
        assert connections == ["postgresql://airflow@metadb:5432/airflow"]

    @pytest.mark.unit
    def test_second_writer_is_refused_immediately(self, monkeypatch):
        mod = self._module()
        monkeypatch.setenv(mod.WRITER_LOCK_ENV, "1")
        _fake_psycopg2(
            monkeypatch, mod, acquired=False, executed=[], connections=[]
        )

        with pytest.raises(mod.WriterLockBusy) as excinfo:
            with mod._writer_lock():
                raise AssertionError("тело не должно выполняться")

        assert "bronze writer lock" in str(excinfo.value)

    @pytest.mark.unit
    def test_busy_lock_fails_the_run_without_touching_the_scraper(
        self, monkeypatch, tmp_path
    ):
        """Отказ обязан быть виден отчётом, а не тихим пропуском работы."""

        from utils import fotmob_publication as publication

        mod = self._module()
        monkeypatch.setenv(mod.WRITER_LOCK_ENV, "1")
        monkeypatch.delenv(
            publication.FOTMOB_DEPLOYMENT_REPORT_PATH_ENV, raising=False
        )
        monkeypatch.delenv(
            publication.FOTMOB_SHARED_DEPLOYMENT_REPORT_PATH_ENV, raising=False
        )
        _fake_psycopg2(
            monkeypatch, mod, acquired=False, executed=[], connections=[]
        )
        started = MagicMock()
        monkeypatch.setattr(mod, "_run_native", started)
        out = tmp_path / "report.json"
        monkeypatch.setattr(
            sys,
            "argv",
            ["run_fotmob_scraper.py", "--mode", "discover", "--output", str(out)],
        )

        assert mod.main() == 1

        started.assert_not_called()
        report = json.loads(out.read_text(encoding="utf-8"))
        assert report["status"] == "incomplete"
        assert report["complete"] is False
        assert any("writer lock" in str(error) for error in report["errors"])

    @pytest.mark.unit
    def test_lock_can_be_disabled_only_explicitly(self, monkeypatch):
        mod = self._module()
        monkeypatch.setenv(mod.WRITER_LOCK_ENV, "0")

        def refuse(_dsn):
            raise AssertionError("выключенный замок не должен ходить в базу")

        monkeypatch.setitem(sys.modules, "psycopg2", SimpleNamespace(connect=refuse))

        with mod._writer_lock() as acquired:
            assert acquired is False
