"""
Unit tests for the completeness-guard wiring in
``dags/scripts/run_fotmob_scraper.py`` (#583).

The guard arithmetic lives in ``BaseScraper.save_to_iceberg`` (covered by
``test_base_scraper.py``); here we cover the runner's *handling* — arm the guard
on the normal path, map ``ReplaceGuardError`` to exit 3, and let
``--force-replace`` disarm it. We restrict the run to a single entity
(``--entities schedule``) so exactly one save is exercised.

The runner does ``from scrapers.fotmob import FotMobScraper`` lazily inside
``main()``; we install a stub via ``patch.dict('sys.modules', ...)`` (mirrors
``test_run_espn_scraper.py``).
"""

from __future__ import annotations

import importlib
import json
import os
import sys
import tempfile
from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _build_guard_scraper(*, guard_blocks: bool = False):
    """Stub FotMobScraper whose ``read_schedule`` returns a non-empty frame so the
    runner reaches ``save_to_iceberg``. With ``guard_blocks=True`` the
    BaseScraper-level completeness guard is simulated by raising
    ``ReplaceGuardError`` — the runner must catch it and exit 3 (#583).
    """
    from scrapers.base.base_scraper import ReplaceGuardError

    df = pd.DataFrame(
        {
            "league": ["ENG-Premier League"] * 10,
            "season": [2025] * 10,
            "match_id": [str(i) for i in range(10)],
        }
    )
    scraper = MagicMock()
    scraper.read_schedule.return_value = df
    if guard_blocks:
        scraper.save_to_iceberg.side_effect = ReplaceGuardError(
            "new=3 rows < 90% of existing=380 for bronze.fotmob_schedule "
            "— refusing replace_partitions save (would shrink the partition)"
        )
    else:
        scraper.save_to_iceberg.return_value = "iceberg.bronze.fotmob_schedule"
    scraper.__enter__ = MagicMock(return_value=scraper)
    scraper.__exit__ = MagicMock(return_value=False)
    return scraper


def _run_main(args: list, scraper_cls) -> int:
    """Execute ``run_fotmob_scraper.main()`` with a stubbed scraper."""
    stub_pkg = MagicMock()
    stub_pkg.FotMobScraper = scraper_cls

    sys.argv = ["run_fotmob_scraper.py"] + args

    with patch.dict(
        sys.modules,
        {"scrapers.fotmob": stub_pkg},
    ):
        sys.modules.pop("dags.scripts.run_fotmob_scraper", None)
        mod = importlib.import_module("dags.scripts.run_fotmob_scraper")
        importlib.reload(mod)
        return mod.main()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
class TestFotmobReplaceGuard:
    """#583: completeness-guard wiring in the FotMob runner (single entity)."""

    @pytest.fixture
    def temp_output(self):
        fd, path = tempfile.mkstemp(suffix=".json", prefix="fotmob_")
        os.close(fd)
        yield path
        if os.path.exists(path):
            os.unlink(path)

    @pytest.mark.unit
    def test_guard_refusal_exits_3(self, temp_output):
        """save_to_iceberg raises ReplaceGuardError → exit 3 +
        FOTMOB_REPLACE_GUARD marker (distinct from a hard failure)."""
        scraper = _build_guard_scraper(guard_blocks=True)

        rc = _run_main(
            [
                "--leagues",
                "ENG-Premier League",
                "--season",
                "2025",
                "--entities",
                "schedule",
                "--output",
                temp_output,
            ],
            MagicMock(return_value=scraper),
        )

        assert rc == 3
        scraper.save_to_iceberg.assert_called_once()
        with open(temp_output) as f:
            payload = json.load(f)
        assert any("FOTMOB_REPLACE_GUARD" in e for e in payload["errors"])

    @pytest.mark.unit
    def test_normal_path_arms_guard_exits_0(self, temp_output):
        """Non-force run passes min_replace_ratio=0.9 (raw COUNT(*), no key)."""
        scraper = _build_guard_scraper()

        rc = _run_main(
            [
                "--leagues",
                "ENG-Premier League",
                "--season",
                "2025",
                "--entities",
                "schedule",
                "--output",
                temp_output,
            ],
            MagicMock(return_value=scraper),
        )

        assert rc == 0
        kwargs = scraper.save_to_iceberg.call_args.kwargs
        assert kwargs["min_replace_ratio"] == 0.9
        assert kwargs["replace_partitions"] == ["league", "season"]
        # one row per match → raw COUNT(*), no replace_guard_key
        assert "replace_guard_key" not in kwargs

    @pytest.mark.unit
    def test_full_players_flag_passed_to_scraper(self, temp_output):
        """--full-players → FotMobScraper(full_players=True); default False."""
        scraper = _build_guard_scraper()
        cls = MagicMock(return_value=scraper)

        rc = _run_main(
            [
                "--leagues",
                "ENG-Premier League",
                "--season",
                "2025",
                "--entities",
                "schedule",
                "--full-players",
                "--output",
                temp_output,
            ],
            cls,
        )

        assert rc == 0
        assert cls.call_args.kwargs["full_players"] is True

    @pytest.mark.unit
    def test_full_players_defaults_false(self, temp_output):
        scraper = _build_guard_scraper()
        cls = MagicMock(return_value=scraper)

        rc = _run_main(
            [
                "--leagues",
                "ENG-Premier League",
                "--season",
                "2025",
                "--entities",
                "schedule",
                "--output",
                temp_output,
            ],
            cls,
        )

        assert rc == 0
        assert cls.call_args.kwargs["full_players"] is False

    @pytest.mark.unit
    def test_force_replace_disarms_guard(self, temp_output):
        """--force-replace must pass min_replace_ratio=None to the save."""
        scraper = _build_guard_scraper()

        rc = _run_main(
            [
                "--leagues",
                "ENG-Premier League",
                "--season",
                "2025",
                "--entities",
                "schedule",
                "--force-replace",
                "--output",
                temp_output,
            ],
            MagicMock(return_value=scraper),
        )

        assert rc == 0
        kwargs = scraper.save_to_iceberg.call_args.kwargs
        assert kwargs["min_replace_ratio"] is None


class TestFotmobNativeRunner:
    """Source-native mode is explicit and preserves exact source identities."""

    @staticmethod
    def _module():
        sys.modules.pop("dags.scripts.run_fotmob_scraper", None)
        return importlib.import_module("dags.scripts.run_fotmob_scraper")

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
        self, tmp_path
    ):
        mod = self._module()
        output = tmp_path / "native-report.json"
        sys.argv = [
            "run_fotmob_scraper.py",
            "--mode",
            "daily",
            "--output",
            str(output),
        ]

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

        rc, report = mod._run_native(args, service=service)

        assert rc == 0
        assert report["status"] == "success"
        assert report["complete"] is True
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

        rc, report = mod._run_native(args, service=service)

        assert rc == 0
        assert report["status"] == "success"
        repository.completed_scope_keys.assert_called_once_with(
            report["selection"]["scope_plan_signature"]
        )
        assert not any("season=2025%2F2026" in url for url, _ in transport.calls)

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

        rc, report = mod._run_native(args, service=service)

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

        first_rc, first_report = mod._run_native(args, service=first_service)

        assert first_rc == 1
        assert first_report["complete"] is False
        signature = first_report["selection"]["scope_plan_signature"]
        assert repository.completed_scope_keys(signature) == set()

        second_service, second_transport = make_service(3, "backfill-2")
        second_rc, second_report = mod._run_native(args, service=second_service)

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

        rc, report = mod._run_native(args, service=service)

        assert rc == 0 and report["complete"] is True
        assert [call.args[0] for call in service.sync_season.call_args_list] == [
            48,
            47,
        ]

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

        rc, report = mod._run_native(args, service=service)

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

        rc, report = mod._run_native(args, service=service)

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

        rc, report = mod._run_native(args, service=service)

        assert rc == 1
        assert any("commit flush" in error for error in report["errors"])
