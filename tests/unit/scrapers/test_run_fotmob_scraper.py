"""Unit tests for the source-native FotMob runner."""

from __future__ import annotations

import importlib
import json
import sys
from contextlib import contextmanager, nullcontext
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
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

        assert mod._parse_scopes(["47=2025/2026,289=2019", "47=2025/2026"]) == (
            (47, "2025/2026"),
            (289, "2019"),
        )
        with pytest.raises(ValueError, match="numeric ID"):
            mod._parse_scopes(["ENG-Premier League=2025/2026"])
        with pytest.raises(ValueError, match="exact source key"):
            mod._parse_scopes(["47="])

    @pytest.mark.unit
    def test_historical_scope_detection_handles_old_selected_competitions(self):
        mod = self._module()

        assert mod._scope_is_historical("2025", reference_year=2026)
        assert mod._scope_is_historical("2024/2025", reference_year=2026)
        assert not mod._scope_is_historical("2025/2026", reference_year=2026)
        assert not mod._scope_is_historical("2026/2027", reference_year=2026)
        assert not mod._scope_is_historical("current", reference_year=2026)

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
    def test_direct_cli_requires_exact_publication_and_matching_run_id(
        self, monkeypatch
    ):
        mod = self._module()
        parser = mod._argument_parser()

        with pytest.raises(SystemExit):
            mod._validate_args(parser, parser.parse_args(["--mode", "daily"]))

        publication_args, publication = _publication_cli(monkeypatch)
        daily_args = _daily_cli(monkeypatch)
        args = parser.parse_args(
            ["--mode", "daily", *daily_args, *publication_args]
        )
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
            Path(daily_args[daily_args.index("--competition-scope-file") + 1])
            .read_bytes()
        )
        daily_args[daily_args.index("--competition-scope-file") + 1] = str(
            alternate
        )
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
    def test_native_writer_guard_verifies_exact_active_control_state(
        self, monkeypatch
    ):
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
            assert (
                mod._ACTIVE_PUBLICATION_GENERATION
                == publication["generation_id"]
            )
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
            "guard_exit",
        ]

    @pytest.mark.unit
    def test_shared_owner_does_not_require_isolated_runtime_report(self):
        mod = self._module()

        assert mod._attest_native_runtime(
            SimpleNamespace(),
            {"binding": {"owner": "shared"}},
        ) == {
            "owner": "shared",
            "isolated_attestation": "not_applicable",
        }

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
    def test_backfill_skips_only_fully_completed_scope_plan(self):
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
        repository.completed_scope_keys.assert_called_once_with(
            report["selection"]["scope_plan_signature"]
        )
        assert not any("season=2025%2F2026" in url for url, _ in transport.calls)

    @pytest.mark.unit
    def test_transfer_competition_limit_applies_after_completion_filter(self):
        from scrapers.fotmob.transport import canonicalize_target
        from tests.unit.scrapers.test_fotmob_service import _league_payload, _service

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

        first_rc, first_report = _run_native_admitted(
            mod, args, service=first_service
        )

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
            CompetitionRef(value, f"Competition {value}")
            for value in (*cohort, 999999)
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

        def discover_competitions(candidates):
            discovered_ids.extend(
                item.competition.competition_id for item in candidates
            )
            return [
                CompetitionDiscoveryResult(
                    item.competition,
                    item,
                    OperationResult(
                        "competition_seasons", attempted=1, succeeded=1
                    ),
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

        service.discover_competitions = MagicMock(
            side_effect=discover_competitions
        )
        service.sync_season = MagicMock(side_effect=sync_season)
        service.sync_leaderboards = MagicMock(
            side_effect=lambda _bundle: OperationResult("leaderboards")
        )
        service.sync_match_payloads = MagicMock(
            side_effect=lambda _bundle, **_kwargs: OperationResult(
                "match_payloads"
            )
        )
        service.sync_team_snapshots = MagicMock(
            side_effect=lambda _bundle, **_kwargs: (
                OperationResult("team_snapshots"),
                set(),
            )
        )
        service.sync_player_snapshots = MagicMock(
            side_effect=lambda _ids, **_kwargs: OperationResult(
                "player_snapshots"
            )
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
        assert report["selection"]["completed_scopes"] == report[
            "selection"
        ]["planned_scopes"]
        assert report["selection"][
            "completed_transfer_competition_ids"
        ] == list(cohort)
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
        repository.flush.assert_called_once_with()

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
