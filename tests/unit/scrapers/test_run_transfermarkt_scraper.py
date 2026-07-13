"""
Unit tests for ``dags/scripts/run_transfermarkt_scraper.py``.

The replace-partitions completeness guard (#484/#486) was generalised into
``BaseScraper.save_to_iceberg`` in #513: the runner now passes
``min_replace_ratio`` + ``replace_guard_key='player_id'`` and the guard raises
``ReplaceGuardError`` when the scraped frame would shrink the existing bronze
partition below 90%. These tests cover the runner's *handling* of that error
(map to exit 3 + ``TM_REPLACE_GUARD`` marker) and the ``--dry-run`` /
``--force-replace`` flags — the guard arithmetic itself is unit-tested in
``test_base_scraper.py``.

The runner lazily imports THREE modules inside each ``_run_*``:
``scrapers.base.base_scraper`` (``ReplaceGuardError`` — the real class, NOT
stubbed), ``scrapers.transfermarkt`` (the class) and
``scrapers.transfermarkt.scraper`` (``R0_2B_FALLBACK_MARKER``) — the latter two
are stubbed via ``patch.dict(sys.modules)`` following the understat-runner test
pattern. ``R0_2B_FALLBACK_MARKER`` must be the real string: it is f-stringed
into ``results['errors']`` and JSON-dumped.
"""

from __future__ import annotations

import importlib
import inspect
import json
import os
import sys
import tempfile
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


REAL_TM_REGISTRY = importlib.import_module('scrapers.transfermarkt.registry')


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _players_df(n: int) -> pd.DataFrame:
    return pd.DataFrame({'player_id': [str(i) for i in range(n)]})


def _build_scraper(*, df: pd.DataFrame, guard_blocks: bool = False):
    """Stub TransfermarktScraper context-manager.

    Every ``read_*`` returns ``df``. With ``guard_blocks=True`` the (now
    BaseScraper-level) completeness guard is simulated by making
    ``save_to_iceberg`` raise ``ReplaceGuardError`` — the runner must catch it
    and exit 3 (#513).
    """
    from scrapers.base.base_scraper import ReplaceGuardError

    scraper = MagicMock()
    scraper._last_endpoint_error = None
    scraper.get_traffic_stats.return_value = {
        'decoded_response_body_bytes': 0,
        'decoded_response_body_mb': 0.0,
        'network_fetches': 0,
    }
    scraper.get_stats.return_value = {'requests': 0}
    scraper.read_players.return_value = df
    scraper.read_market_value_history.return_value = df
    scraper.read_transfers.return_value = df
    if guard_blocks:
        scraper.save_to_iceberg.side_effect = ReplaceGuardError(
            'new=3 distinct player_id < 90% of existing=600 for '
            'bronze.transfermarkt_players — refusing replace_partitions save'
        )
    else:
        scraper.save_to_iceberg.return_value = 'iceberg.bronze.stub_table'
    scraper.__enter__ = MagicMock(return_value=scraper)
    scraper.__exit__ = MagicMock(return_value=False)
    return scraper


def _run_main(args: list, scraper) -> int:
    """Execute ``run_transfermarkt_scraper.main()`` with stubbed scraper."""
    args = list(args)
    if '--expected-reader-revision' not in args:
        args.extend(['--expected-reader-revision', '0'])
    stub_pkg = MagicMock()
    stub_pkg.TransfermarktScraper = MagicMock(return_value=scraper)

    stub_scraper_mod = MagicMock()
    stub_scraper_mod.R0_2B_FALLBACK_MARKER = 'TM_FALLBACK'

    sys.argv = ["run_transfermarkt_scraper.py"] + args

    with patch.dict(
        sys.modules,
        {
            "scrapers.transfermarkt": stub_pkg,
            "scrapers.transfermarkt.scraper": stub_scraper_mod,
            "scrapers.transfermarkt.registry": REAL_TM_REGISTRY,
        },
    ):
        sys.modules.pop("dags.scripts.run_transfermarkt_scraper", None)
        mod = importlib.import_module("dags.scripts.run_transfermarkt_scraper")
        importlib.reload(mod)
        traffic_mod = importlib.import_module("utils.proxy_traffic")
        with tempfile.TemporaryDirectory() as budget_dir:
            with (
                patch.dict(
                    os.environ,
                    {'TM_CYCLE_BUDGET_DIR': budget_dir},
                ),
                patch.object(
                    traffic_mod, 'record_traffic_run', return_value=True,
                ),
                patch.object(
                    mod,
                    '_authorize_write_mode',
                    return_value={
                        'write_mode': 'test',
                        'expected_revision': 0,
                        'state_exists': False,
                    },
                ),
                patch.object(
                    mod,
                    '_persist_native_write_manifest',
                    return_value={
                        'status': 'success',
                        'write_mode': 'native-only',
                        'rows': [],
                    },
                ),
            ):
                return mod.main()


@pytest.fixture
def temp_output():
    fd, path = tempfile.mkstemp(suffix=".json", prefix="transfermarkt_")
    os.close(fd)
    yield path
    if os.path.exists(path):
        os.unlink(path)


def _load_results(path: str) -> dict:
    with open(path) as f:
        return json.load(f)


def _import_runner():
    """Fresh import of the runner module (module-level has no heavy imports,
    so no scraper stubs needed — used to exercise pure helpers like
    ``_window_offset``)."""
    sys.modules.pop("dags.scripts.run_transfermarkt_scraper", None)
    return importlib.import_module("dags.scripts.run_transfermarkt_scraper")


# ---------------------------------------------------------------------------
# Production traffic ceilings
# ---------------------------------------------------------------------------

class TestProductionTrafficCeilings:
    def test_metered_runner_pins_retry_budget_into_scraper_call(
        self, temp_output, monkeypatch,
    ):
        mod = _import_runner()
        monkeypatch.setenv('TM_REQUIRE_METERED_PROXY', 'true')
        monkeypatch.setenv('TM_RETRY_BUDGET', '1')
        monkeypatch.setattr(sys, 'argv', [
            'run_transfermarkt_scraper.py',
            '--entity', 'players',
            '--competition-id', 'GB1',
            '--edition-id', '2025',
            '--output', temp_output,
            '--write-mode', 'dual',
            '--expected-reader-revision', '7',
            '--retry-budget', '1',
        ])
        run_entity = MagicMock(return_value=0)
        monkeypatch.setattr(mod, '_run_entity', run_entity)

        assert mod.main() == 0
        assert run_entity.call_args.kwargs['retry_budget'] == 1

    def test_metered_runner_rejects_retry_budget_env_drift_before_run(
        self, temp_output, monkeypatch,
    ):
        mod = _import_runner()
        monkeypatch.setenv('TM_REQUIRE_METERED_PROXY', 'true')
        monkeypatch.setenv('TM_RETRY_BUDGET', '1')
        monkeypatch.setattr(sys, 'argv', [
            'run_transfermarkt_scraper.py',
            '--entity', 'players',
            '--competition-id', 'GB1',
            '--edition-id', '2025',
            '--output', temp_output,
            '--write-mode', 'dual',
            '--expected-reader-revision', '7',
            '--retry-budget', '2',
        ])
        run_entity = MagicMock(return_value=0)
        monkeypatch.setattr(mod, '_run_entity', run_entity)

        assert mod.main() == 1
        run_entity.assert_not_called()
        assert '--retry-budget differs from TM_RETRY_BUDGET' in (
            _load_results(temp_output)['errors'][0]
        )

    @pytest.mark.parametrize(
        'state,mode,revision,allowed',
        [
            (SimpleNamespace(exists=False, revision=0), 'dual', 0, False),
            (SimpleNamespace(
                exists=True, revision=4, active_version='v2',
                legacy_writers_disabled_at=None, cleanup_completed_at=None,
            ), 'dual', 4, True),
            (SimpleNamespace(
                exists=True, revision=5, active_version='v2',
                legacy_writers_disabled_at='now', cleanup_completed_at=None,
            ), 'dual', 5, False),
            (SimpleNamespace(
                exists=True, revision=5, active_version='v2',
                legacy_writers_disabled_at='now', cleanup_completed_at=None,
            ), 'native-only', 5, True),
            (SimpleNamespace(
                exists=True, revision=5, active_version='v2',
                legacy_writers_disabled_at=None, cleanup_completed_at=None,
            ), 'native-only', 5, False),
            (SimpleNamespace(
                exists=True, revision=3, active_version='legacy',
                legacy_writers_disabled_at=None, cleanup_completed_at=None,
            ), 'legacy-only', 3, True),
        ],
    )
    def test_persisted_state_authorizes_exact_writer_lifecycle(
        self, state, mode, revision, allowed,
    ):
        mod = _import_runner()

        error = mod._writer_mode_state_error(state, mode, revision)

        assert (error is None) is allowed

    def test_writer_state_rejection_precedes_budget_and_proxy(self):
        mod = _import_runner()
        stub_pkg = MagicMock()
        stub_scraper_mod = MagicMock(R0_2B_FALLBACK_MARKER='TM_FALLBACK')

        with (
            patch.dict(sys.modules, {
                'scrapers.transfermarkt': stub_pkg,
                'scrapers.transfermarkt.scraper': stub_scraper_mod,
                'scrapers.transfermarkt.registry': REAL_TM_REGISTRY,
            }),
            patch.object(
                mod, '_authorize_write_mode',
                side_effect=RuntimeError('persisted state rejected'),
            ),
            patch.object(mod, '_prepare_cycle_budget') as reserve,
            patch.object(mod, '_write_results'),
        ):
            rc = mod._run_entity(
                mod.ENTITY_SPECS['players'],
                ['ENG-Premier League'], 2025, None, '/tmp/ignored.json',
                write_mode='dual', expected_reader_revision=7,
                cycle_budget_bytes=15 * 1024 * 1024,
            )

        assert rc == 1
        reserve.assert_not_called()
        stub_pkg.TransfermarktScraper.assert_not_called()

    @pytest.mark.parametrize(
        'extra,expected',
        [
            (['--decoded-body-budget-mb', '16.0001'],
             '--decoded-body-budget-mb cannot exceed 16.0'),
            (['--request-budget', '151'],
             '--request-budget cannot exceed 150'),
            (['--cycle-budget-bytes', str(15 * 1024 * 1024 + 1)],
             '--cycle-budget-bytes cannot exceed production cap'),
        ],
    )
    def test_cli_cannot_raise_paid_traffic_caps(
        self, temp_output, extra, expected,
    ):
        scraper = _build_scraper(df=_players_df(3))
        rc = _run_main(
            ['--entity', 'players', '--output', temp_output, *extra], scraper,
        )
        assert rc == 1
        assert expected in _load_results(temp_output)['errors'][0]
        scraper.read_squad_data.assert_not_called()


# ---------------------------------------------------------------------------
# Completeness guard handling (#484 / #486, generalised #513)
# ---------------------------------------------------------------------------

class TestReplaceGuard:
    def test_guard_error_exits_3(self, temp_output):
        # save_to_iceberg raises ReplaceGuardError → runner maps it to exit 3.
        scraper = _build_scraper(df=_players_df(3), guard_blocks=True)
        rc = _run_main(
            ['--entity', 'players', '--output', temp_output],
            scraper,
        )
        assert rc == 3
        scraper.save_to_iceberg.assert_called_once()
        results = _load_results(temp_output)
        assert any('TM_REPLACE_GUARD' in e for e in results['errors'])
        # what was rejected is visible to validate_data / Telegram
        assert results['rows'] == 3
        assert results['players_with_rows'] == 3

    def test_guard_passes_exits_0(self, temp_output):
        scraper = _build_scraper(df=_players_df(3))
        rc = _run_main(
            ['--entity', 'players', '--output', temp_output], scraper,
        )
        assert rc == 0
        scraper.save_to_iceberg.assert_called_once()

    def test_guard_params_passed_to_save(self, temp_output):
        # Non-force path must arm the guard: ratio 0.9, distinct player_id.
        scraper = _build_scraper(df=_players_df(3))
        rc = _run_main(
            ['--entity', 'players', '--output', temp_output], scraper,
        )
        assert rc == 0
        kwargs = scraper.save_to_iceberg.call_args.kwargs
        assert kwargs['min_replace_ratio'] == 0.9
        assert kwargs['replace_guard_key'] == 'player_id'
        assert kwargs['replace_partitions'] == ['league', 'season']

    @pytest.mark.parametrize('entity', ['market_value_history', 'transfers'])
    def test_guard_error_exits_3_dependent_entities(self, temp_output, entity):
        scraper = _build_scraper(df=_players_df(3), guard_blocks=True)
        rc = _run_main(
            ['--entity', entity, '--limit', '3', '--output', temp_output],
            scraper,
        )
        assert rc == 3
        scraper.save_to_iceberg.assert_called_once()
        results = _load_results(temp_output)
        assert any('TM_REPLACE_GUARD' in e for e in results['errors'])


# ---------------------------------------------------------------------------
# Rotating window + per-player upsert (issue #620)
# ---------------------------------------------------------------------------

class TestRosterRotationUpsert:
    @pytest.mark.parametrize('entity', ['market_value_history', 'transfers'])
    def test_dependent_entities_upsert_by_player_id(self, temp_output, entity):
        # transfers / mv_history must delete+reinsert ONLY the scraped window's
        # players so previous windows accumulate (#620). That means player_id
        # joins the replace_partitions key.
        scraper = _build_scraper(df=_players_df(3))
        rc = _run_main(
            ['--entity', entity, '--limit', '100', '--output', temp_output],
            scraper,
        )
        assert rc == 0
        kwargs = scraper.save_to_iceberg.call_args.kwargs
        assert kwargs['replace_partitions'] == ['league', 'season', 'player_id']
        assert kwargs['replace_guard_key'] == 'player_id'

    def test_players_partition_unchanged(self, temp_output):
        # Anchor entity stays a whole-partition replace (full crawl).
        scraper = _build_scraper(df=_players_df(3))
        rc = _run_main(
            ['--entity', 'players', '--output', temp_output], scraper,
        )
        assert rc == 0
        kwargs = scraper.save_to_iceberg.call_args.kwargs
        assert kwargs['replace_partitions'] == ['league', 'season']

    def test_window_offset_helper_increments_per_run(self):
        mod = _import_runner()
        # Two dates 7 days apart (one weekly DAG run) → offset differs by 1.
        assert mod._window_offset('2026-06-29') - mod._window_offset('2026-06-22') == 1
        # No date → today-based int (callable standalone).
        assert isinstance(mod._window_offset(None), int)

    @pytest.mark.parametrize(
        'entity,reader',
        [
            ('market_value_history', 'read_market_value_history'),
            ('transfers', 'read_transfers'),
        ],
    )
    def test_as_of_date_forwards_window_offset(self, temp_output, entity, reader):
        scraper = _build_scraper(df=_players_df(3))
        rc = _run_main(
            ['--entity', entity, '--limit', '100',
             '--as-of-date', '2026-06-22', '--output', temp_output],
            scraper,
        )
        assert rc == 0
        expected = _import_runner()._window_offset('2026-06-22')
        assert getattr(scraper, reader).call_args.kwargs['window_offset'] == expected


# ---------------------------------------------------------------------------
# --dry-run / --force-replace
# ---------------------------------------------------------------------------

class TestRunnerFlags:
    def test_write_mode_contract_filters_legacy_after_retention(self):
        mod = _import_runner()
        spec = mod.ENTITY_SPECS['players']

        assert [
            output.key for output in mod._spec_for_write_mode(spec, 'dual').outputs
        ] == [
            'memberships', 'attribute_observations',
            'contract_observations', 'legacy_players',
        ]
        assert [
            output.key
            for output in mod._spec_for_write_mode(spec, 'native-only').outputs
        ] == [
            'memberships', 'attribute_observations', 'contract_observations',
        ]
        assert [
            output.key
            for output in mod._spec_for_write_mode(spec, 'legacy-only').outputs
        ] == ['legacy_players']

    def test_dry_run_skips_save(self, temp_output):
        scraper = _build_scraper(df=_players_df(3), guard_blocks=True)
        rc = _run_main(
            ['--entity', 'players', '--limit', '3', '--dry-run',
             '--output', temp_output],
            scraper,
        )
        assert rc == 0
        scraper.save_to_iceberg.assert_not_called()
        results = _load_results(temp_output)
        assert results['dry_run'] is True
        assert results['rows'] == 3

    def test_force_replace_disables_guard(self, temp_output):
        scraper = _build_scraper(df=_players_df(3))
        rc = _run_main(
            ['--entity', 'players', '--force-replace',
             '--output', temp_output],
            scraper,
        )
        assert rc == 0
        scraper.save_to_iceberg.assert_called_once()
        # --force-replace must turn the guard off at the save call.
        assert scraper.save_to_iceberg.call_args.kwargs['min_replace_ratio'] is None

    def test_native_dual_write_reads_squad_once_in_dry_run(self, temp_output):
        memberships = pd.DataFrame({
            'league': ['ENG-Premier League'], 'season': ['2526'],
            'club_id': ['1'], 'player_id': ['7'],
        })
        observations = memberships.assign(name='Player', observed_at='2026-07-10')
        legacy = pd.DataFrame({
            'player_id': ['7'], 'current_club_id': ['1'],
            'league': ['ENG-Premier League'], 'season': ['2526'],
        })

        class NativeScraper:
            _batch_id = 'batch-1'
            _last_endpoint_error = None

            def __init__(self):
                self.read_calls = 0
                self.save_to_iceberg = MagicMock()

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def read_squad_data(self, **kwargs):
                self.read_calls += 1
                return {
                    'memberships': memberships,
                    'attribute_observations': observations,
                    'legacy_players': legacy,
                }

            def get_traffic_stats(self):
                return {
                    'decoded_response_body_bytes': 104858,
                    'decoded_response_body_mb': 0.1,
                    'requests': 2,
                }

            def get_stats(self):
                return {'requests': 2}

            def get_fetch_outcomes(self):
                return {}

        scraper = NativeScraper()
        rc = _run_main(
            ['--entity', 'players', '--dry-run', '--output', temp_output],
            scraper,
        )

        assert rc == 0
        assert scraper.read_calls == 1
        scraper.save_to_iceberg.assert_not_called()
        results = _load_results(temp_output)
        assert results['native_dual_write'] is True
        assert results['outputs']['memberships']['rows'] == 1
        assert results['outputs']['legacy_players']['rows'] == 1
        assert results['network_fetches'] == 2
        assert results['dual_write_complete'] is False

    def test_scope_dq_blocks_strict_partial_capture_before_any_write(
        self, temp_output, monkeypatch,
    ):
        memberships = pd.DataFrame({
            'competition_id': ['CL'], 'edition_id': ['2025'],
            'league': ['TM-CL'], 'season': ['2526'],
            'club_id': ['1'], 'player_id': ['7'],
        })
        observations = memberships.assign(name='Player', observed_at='2026-07-11')
        contracts = memberships.rename(columns={'club_id': 'team_id'}).assign(
            applicability_status='ok', observed_at='2026-07-11',
        )
        legacy = pd.DataFrame({
            'player_id': ['7'], 'current_club_id': ['1'],
            'league': ['TM-CL'], 'season': ['2526'],
        })

        class StrictScraper:
            _batch_id = 'strict-cycle'
            _last_endpoint_error = None

            def __init__(self):
                self.save_to_iceberg = MagicMock()

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def read_squad_data(self, **kwargs):
                return {
                    'memberships': memberships,
                    'attribute_observations': observations,
                    'contract_observations': contracts,
                    'legacy_players': legacy,
                }

            def get_scope_capture(self):
                from scrapers.transfermarkt.registry import deterministic_scope_id
                return {
                    'scope_id': deterministic_scope_id('CL', '2025'),
                    'competition_id': 'CL',
                    'edition_id': '2025',
                    'competition_type': 'continental_club',
                    'gender': 'men',
                    'team_type': 'club',
                    'age_category': 'senior',
                    'listing_status': 'ok',
                    'listing_source_url': 'https://example.test/CL/2025',
                    'listing_source_body_hash': 'listing-response-hash',
                    'expected_team_ids': ['1', '2'],
                    'observed_team_ids': ['1'],
                    'endpoint_status_by_team': {
                        '1': 'ok', '2': 'retry_exhausted',
                    },
                    'fetched_at': '2026-07-11T00:00:00+00:00',
                }

            def get_traffic_stats(self):
                return {
                    'decoded_response_body_bytes': 10,
                    'decoded_response_body_mb': 10 / 1024 / 1024,
                    'network_fetches': 3,
                }

            def get_stats(self):
                return {'requests': 3}

            def get_fetch_outcomes(self):
                return {}

        scraper = StrictScraper()
        monkeypatch.setenv('TM_SCOPE_DQ_REQUIRED', 'true')
        monkeypatch.setenv('TM_EDITION_CURRENT', 'true')
        rc = _run_main([
            '--entity', 'players', '--competition-id', 'CL',
            '--edition-id', '2025', '--output', temp_output,
        ], scraper)

        assert rc == 1
        scraper.save_to_iceberg.assert_not_called()
        assert 'participant mismatch' in _load_results(temp_output)['errors'][0]

    def test_native_only_mode_never_materializes_or_writes_legacy(self, temp_output):
        memberships = pd.DataFrame({
            'league': ['ENG-Premier League'], 'season': ['2526'],
            'club_id': ['1'], 'player_id': ['7'],
        })
        observations = memberships.assign(
            name='Player', observed_at='2026-07-10',
        )

        class NativeOnlyScraper:
            _batch_id = 'native-cycle'
            _last_endpoint_error = None

            def __init__(self):
                self.save_to_iceberg = MagicMock(
                    side_effect=lambda **kw: (
                        'iceberg.bronze.' + kw['table_name']
                    ),
                )
                self.materialize_legacy_players = MagicMock(
                    side_effect=AssertionError(
                        'legacy materialization is forbidden after cleanup'
                    ),
                )

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def read_squad_data(self, **kwargs):
                return {
                    'memberships': memberships,
                    'attribute_observations': observations,
                }

            def get_traffic_stats(self):
                return {
                    'decoded_response_body_bytes': 10,
                    'decoded_response_body_mb': 10 / 1024 / 1024,
                    'network_fetches': 1,
                }

            def get_stats(self):
                return {'requests': 1}

            def get_fetch_outcomes(self):
                return {}

        scraper = NativeOnlyScraper()
        rc = _run_main([
            '--entity', 'players', '--write-mode', 'native-only',
            '--output', temp_output,
        ], scraper)

        assert rc == 0
        assert scraper.save_to_iceberg.call_count == 2
        assert {
            call.kwargs['table_name']
            for call in scraper.save_to_iceberg.call_args_list
        } == {
            'transfermarkt_squad_memberships',
            'transfermarkt_player_attribute_observations',
        }
        scraper.materialize_legacy_players.assert_not_called()
        results = _load_results(temp_output)
        assert results['write_mode'] == 'native-only'
        assert results['native_write'] is True
        assert results['legacy_write'] is False
        assert results['native_write_complete'] is True
        assert results['native_dual_write'] is False
        assert results['dual_write_complete'] is False
        assert 'legacy_players' not in results['outputs']
        assert 'batch_manifest' not in results

    def test_native_only_mode_forbids_legacy_reader_fallback(self, temp_output):
        scraper = _build_scraper(df=_players_df(3))

        rc = _run_main([
            '--entity', 'players', '--write-mode', 'native-only',
            '--output', temp_output,
        ], scraper)

        assert rc == 1
        scraper.read_players.assert_not_called()
        scraper.save_to_iceberg.assert_not_called()
        results = _load_results(temp_output)
        assert results['write_mode'] == 'native-only'
        assert any(
            'legacy fallback is forbidden after cleanup' in error
            for error in results['errors']
        )


# ---------------------------------------------------------------------------
# Pre-existing fallback path stays intact
# ---------------------------------------------------------------------------

class TestFallbackPath:
    def test_empty_frame_exits_2_without_save(self, temp_output):
        # No endpoint error recorded → genuine empty_payload → soft exit 2.
        scraper = _build_scraper(df=pd.DataFrame())
        rc = _run_main(
            ['--entity', 'players', '--output', temp_output], scraper,
        )
        assert rc == 2
        scraper.save_to_iceberg.assert_not_called()
        results = _load_results(temp_output)
        assert results['fallback'] is True

    def test_empty_frame_http_block_exits_1_red(self, temp_output):
        """#790: an empty frame caused by an http block (403) is a real failure
        → exit 1 (red), NOT the soft exit 2 of a genuine empty payload."""
        scraper = _build_scraper(df=pd.DataFrame())
        scraper._last_endpoint_error = {'status': 403}
        rc = _run_main(
            ['--entity', 'players', '--output', temp_output], scraper,
        )
        assert rc == 1
        scraper.save_to_iceberg.assert_not_called()
        results = _load_results(temp_output)
        assert results['fallback'] is True
        assert results['fallback_reason'] == 'http_403'

    def test_exception_still_captures_traffic_in_finally(self, temp_output):
        scraper = _build_scraper(df=_players_df(1))
        scraper.read_players.side_effect = RuntimeError('parser exploded')
        scraper.get_traffic_stats.return_value = {
            'decoded_response_body_mb': 0.25,
            'network_fetches': 3,
            'failed_attempts': 1,
        }
        scraper.get_stats.return_value = {'requests': 3, 'failures': 1}

        rc = _run_main(
            ['--entity', 'players', '--output', temp_output], scraper,
        )

        assert rc == 1
        results = _load_results(temp_output)
        assert results['decoded_response_body_mb'] == pytest.approx(0.25)
        assert results['network_fetches'] == 3
        assert results['failed_attempts'] == 1
        assert 'parser exploded' in results['errors']


# ---------------------------------------------------------------------------
# argparse hard-fail (#512)
# ---------------------------------------------------------------------------

class TestArgparseHardFail:
    """A CLI parse error must exit 1 (hard failure), NOT argparse's default 2.

    Exit 2 is the ``TM_FALLBACK`` soft-success code the DAG bash wrapper maps
    to ``exit 0`` — so a flag typo at exit 2 would silently no-op the task.
    """

    def test_unknown_flag_returns_1_not_2(self, temp_output):
        scraper = _build_scraper(df=_players_df(3))
        rc = _run_main(
            ['--entity', 'players', '--bogus-flag', 'x', '--output', temp_output],
            scraper,
        )
        assert rc == 1
        scraper.read_players.assert_not_called()
        scraper.save_to_iceberg.assert_not_called()

    def test_bad_typed_season_returns_1(self, temp_output):
        # --season is type=int; a non-int must hard-fail, not soft-fallback.
        scraper = _build_scraper(df=_players_df(3))
        rc = _run_main(
            ['--entity', 'players', '--season', 'notanumber', '--output', temp_output],
            scraper,
        )
        assert rc == 1
        scraper.read_players.assert_not_called()

    @pytest.mark.parametrize('limit', ['0', '101'])
    def test_paid_window_must_be_bounded_1_to_100(self, temp_output, limit):
        scraper = _build_scraper(df=_players_df(3))
        rc = _run_main(
            ['--entity', 'transfers', '--limit', limit, '--output', temp_output],
            scraper,
        )
        assert rc == 1
        scraper.read_transfers.assert_not_called()

    def test_players_limit_is_dry_run_only(self, temp_output):
        scraper = _build_scraper(df=_players_df(3))
        rc = _run_main(
            ['--entity', 'players', '--limit', '3', '--output', temp_output],
            scraper,
        )
        assert rc == 1
        scraper.read_players.assert_not_called()


class TestRunnerInternals:
    def test_execute_cursor_drains_trino_result_for_ddl(self):
        mod = _import_runner()
        cursor = MagicMock()
        conn = MagicMock()
        conn.cursor.return_value = cursor

        assert mod._execute_cursor(conn, 'CREATE SCHEMA x') is None

        cursor.execute.assert_called_once_with('CREATE SCHEMA x', ())
        cursor.fetchall.assert_called_once_with()
        cursor.close.assert_called_once_with()

    def test_compatibility_hash_is_order_and_duplicate_invariant(self):
        mod = _import_runner()
        left = pd.DataFrame({
            'player_id': ['2', '1', '1'],
            'mv_date': ['2025-01-01', '2024-01-01', '2024-01-01'],
        })
        right = left.iloc[[1, 0]].copy()

        assert mod._compatibility_fingerprint(
            left, ('player_id', 'mv_date'),
        ) == mod._compatibility_fingerprint(
            right, ('player_id', 'mv_date'),
        )

    def test_manifest_requires_compatible_committed_pairs(self):
        mod = _import_runner()
        native_base = {
            'league': ['ENG-Premier League'], 'season': ['2526'],
            'club_id': ['1'], 'club_name': ['Club'], 'player_id': ['7'],
            'player_slug': ['player'], 'player_name': ['Player'],
            '_batch_id': ['batch-1'],
        }
        frames = {
            'memberships': pd.DataFrame(native_base),
            'attribute_observations': pd.DataFrame({
                **native_base, 'name': ['Player'], 'position': ['Forward'],
                'dob': ['2000-01-01'], 'age': [26], 'height_cm': [180],
                'foot': ['right'], 'nationality': ['England'],
                'contract_until': ['2028-06-30'],
                'market_value_eur': [10_000_000],
                'observed_at': ['2026-07-10'],
            }),
            'contract_observations': pd.DataFrame({
                'competition_id': ['GB1'], 'edition_id': ['2025'],
                'team_id': ['1'], 'team_name': ['Club'],
                'player_id': ['7'], 'contract_until': ['2028-06-30'],
                'observed_at': ['2026-07-10'],
                '_batch_id': ['batch-1'],
            }),
            'legacy_players': pd.DataFrame({
                'league': ['ENG-Premier League'], 'season': ['2526'],
                'current_club_id': ['1'], 'current_club_name': ['Club'],
                'player_id': ['7'], 'player_slug': ['player'],
                'name': ['Player'], 'position': ['Forward'],
                'dob': ['2000-01-01'], 'age': [26], 'height_cm': [180],
                'foot': ['right'], 'nationality': ['England'],
                'contract_until': ['2028-06-30'],
                'market_value_eur': [10_000_000],
                '_batch_id': ['batch-1'],
            }),
        }
        cursor = MagicMock()
        cursor.fetchall.return_value = []
        conn = MagicMock()
        conn.cursor.return_value = cursor
        scraper = MagicMock()
        scraper._bronze_connection.return_value = conn
        results = {
            'outputs': {
                key: {'table': f'iceberg.bronze.{key}'} for key in frames
            },
        }

        manifest = mod._persist_dual_write_manifest(
            scraper, mod.ENTITY_SPECS['players'], frames, results, 'cycle-1',
            'ENG-Premier League', 2025,
        )

        assert manifest['status'] == 'success'
        assert manifest['league'] == 'ENG-Premier League'
        assert manifest['season'] == 2025
        assert {row['entity'] for row in manifest['rows']} == {
            'squad_memberships', 'player_attribute_observations',
            'player_contract_observations',
        }
        executed = '\n'.join(
            call.args[0] for call in cursor.execute.call_args_list
        )
        assert 'cycle_id varchar' in executed
        assert 'league varchar' in executed
        assert 'season integer' in executed
        assert "'cycle-1', 'ENG-Premier League', 2025" in executed
        assert 'legacy_batch_id varchar' in executed
        assert "'success'" in executed
        assert cursor.fetchall.call_count == cursor.execute.call_count

        # Identical keys with a corrupted business measure must not be
        # promotable merely because key-level parity still holds.
        frames['attribute_observations'] = (
            frames['attribute_observations'].copy()
        )
        frames['attribute_observations'].loc[0, 'market_value_eur'] = 1
        mismatch = mod._persist_dual_write_manifest(
            scraper, mod.ENTITY_SPECS['players'], frames, results, 'cycle-2',
            'ENG-Premier League', 2025,
        )
        assert mismatch['status'] == 'parity_mismatch'
        assert next(
            row for row in mismatch['rows']
            if row['entity'] == 'player_attribute_observations'
        )['status'] == 'parity_mismatch'

    def test_native_only_manifest_records_exact_business_batch_and_revision(self):
        mod = _import_runner()
        native_base = {
            'league': ['ENG-Premier League'], 'season': ['2526'],
            'club_id': ['1'], 'club_name': ['Club'], 'player_id': ['7'],
            'player_slug': ['player'], 'player_name': ['Player'],
            '_batch_id': ['native-batch'],
        }
        frames = {
            'memberships': pd.DataFrame(native_base),
            'attribute_observations': pd.DataFrame({
                **native_base, 'name': ['Player'], 'position': ['Forward'],
                'dob': ['2000-01-01'], 'age': [26], 'height_cm': [180],
                'foot': ['right'], 'nationality': ['England'],
                'contract_until': ['2028-06-30'],
                'market_value_eur': [10_000_000],
                'observed_at': ['2026-07-10'],
            }),
            'contract_observations': pd.DataFrame({
                'competition_id': ['GB1'], 'edition_id': ['2025'],
                'team_id': ['1'], 'team_name': ['Club'],
                'player_id': ['7'], 'contract_until': ['2028-06-30'],
                'observed_at': ['2026-07-10'],
                '_batch_id': ['native-batch'],
            }),
        }
        cursor = MagicMock()
        cursor.fetchall.return_value = []
        conn = MagicMock()
        conn.cursor.return_value = cursor
        scraper = MagicMock()
        scraper._bronze_connection.return_value = conn
        results = {'outputs': {
            key: {'table': f'iceberg.bronze.{key}'} for key in frames
        }}

        manifest = mod._persist_native_write_manifest(
            scraper, mod.ENTITY_SPECS['players'], frames, results,
            'native-cycle-1', 'ENG-Premier League', 2025, 9,
        )

        assert manifest['status'] == 'success'
        assert manifest['writer_revision'] == 9
        assert manifest['write_mode'] == 'native-only'
        assert {row['entity'] for row in manifest['rows']} == {
            'squad_memberships', 'player_attribute_observations',
            'player_contract_observations',
        }
        assert all(row['native_rows'] == 1 for row in manifest['rows'])
        assert all(row['native_hash'] for row in manifest['rows'])
        executed = '\n'.join(
            call.args[0] for call in cursor.execute.call_args_list
        )
        assert mod.NATIVE_WRITE_MANIFEST_TABLE in executed
        assert 'writer_revision bigint' in executed
        assert 'write_mode varchar' in executed
        assert "'native-only'" in executed
        assert "'native-cycle-1', 'ENG-Premier League', 2025" in executed

    def test_not_applicable_contract_is_explicit_empty_in_dual_and_native_manifests(
        self,
    ):
        mod = _import_runner()

        def one_row(columns):
            return pd.DataFrame({column: ['x'] for column in columns} | {
                '_batch_id': ['batch-national'],
            })

        membership = mod._MANIFEST_COMPATIBILITY['memberships']
        attributes = mod._MANIFEST_COMPATIBILITY['attribute_observations']
        contract = mod._MANIFEST_COMPATIBILITY['contract_observations']
        legacy_columns = tuple(dict.fromkeys(
            membership['legacy'] + attributes['legacy'] + contract['legacy']
        ))
        contracts = pd.DataFrame(columns=contract['native'] + ('_batch_id',))
        contracts.attrs['fetch_status'] = 'not_applicable'
        frames = {
            'memberships': one_row(membership['native']),
            'attribute_observations': one_row(attributes['native']),
            'contract_observations': contracts,
            'legacy_players': one_row(legacy_columns),
        }
        cursor = MagicMock()
        cursor.fetchall.return_value = []
        connection = MagicMock()
        connection.cursor.return_value = cursor
        scraper = MagicMock()
        scraper._bronze_connection.return_value = connection
        results = {'outputs': {
            key: {'table': f'iceberg.bronze.{key}'} for key in frames
        }}

        dual = mod._persist_dual_write_manifest(
            scraper, mod.ENTITY_SPECS['players'], frames, results,
            'national-dual', 'TM-FIWC', 2026,
        )
        native = mod._persist_native_write_manifest(
            scraper, mod.ENTITY_SPECS['players'], frames, {
                'outputs': {
                    key: {'table': f'iceberg.bronze.{key}'}
                    for key in frames if key != 'legacy_players'
                },
            }, 'national-native', 'TM-FIWC', 2026, 7,
        )

        assert dual['status'] == 'success'
        assert native['status'] == 'success'
        for manifest in (dual, native):
            row = next(
                item for item in manifest['rows']
                if item['entity'] == 'player_contract_observations'
            )
            assert row['native_rows'] == 0
            assert row['applicability_status'] == 'not_applicable'
            assert row['status'] == 'success'

    def test_not_applicable_empty_output_has_target_without_fake_write(self):
        mod = _import_runner()
        contracts = pd.DataFrame()
        contracts.attrs['fetch_status'] = 'not_applicable'
        scraper = MagicMock()
        results = {
            'outputs': {
                'contract_observations': mod._frame_output_summary(contracts),
            },
            'tables': [],
        }

        mod._save_frames(
            scraper,
            mod.ENTITY_SPECS['players'],
            {'contract_observations': contracts},
            False,
            results,
        )

        assert results['outputs']['contract_observations'] == {
            'rows': 0,
            'table': (
                'iceberg.bronze.'
                'transfermarkt_player_contract_observations'
            ),
            'applicability_status': 'not_applicable',
        }
        scraper.save_to_iceberg.assert_not_called()

    def test_historical_state_error_fails_closed(self):
        mod = _import_runner()

        class BrokenState:
            def _bronze_connection(self):
                raise RuntimeError('trino unavailable')

        with pytest.raises(RuntimeError, match='refusing historical proxy refetch'):
            mod._load_fetch_state(
                BrokenState(), 'transfer_events', strict=True,
            )

    def test_global_career_valid_empty_deletes_all_legacy_partitions(self):
        mod = _import_runner()
        cursor = MagicMock()
        cursor.fetchall.return_value = []
        conn = MagicMock()
        conn.cursor.return_value = cursor
        scraper = MagicMock()
        scraper._bronze_connection.return_value = conn

        mod._delete_valid_empty_rows(
            scraper,
            mod.ENTITY_SPECS['market_value_history'],
            ['7'],
            'ENG-Premier League',
            2025,
        )

        statements = [call.args[0] for call in cursor.execute.call_args_list]
        legacy_delete = next(
            sql for sql in statements
            if 'transfermarkt_market_value_history' in sql
        )
        assert "player_id IN ('7')" in legacy_delete
        assert 'league =' not in legacy_delete
        assert 'season =' not in legacy_delete

        cursor.execute.reset_mock()
        mod._delete_valid_empty_rows(
            scraper,
            mod.ENTITY_SPECS['coaches'],
            ['1'],
            'ENG-Premier League',
            2025,
            source_key='club_id',
        )
        coach_statements = [
            call.args[0] for call in cursor.execute.call_args_list
        ]
        legacy_coach_delete = next(
            sql for sql in coach_statements if 'transfermarkt_coaches' in sql
        )
        assert "current_club_id IN ('1')" in legacy_coach_delete
        assert "league = 'ENG-Premier League'" in legacy_coach_delete
        assert "season = '2526'" in legacy_coach_delete

    def test_coach_manifest_ignores_out_of_season_native_history(self):
        mod = _import_runner()
        frames = {
            'profiles': pd.DataFrame({
                'coach_id': ['10', '99'], 'coach_slug': ['coach', 'old'],
                'name': ['Coach', 'Old Coach'],
                'dob': ['1970-01-01', '1960-01-01'],
                'nationality': ['Spain', 'France'],
                '_batch_id': ['batch-1', 'batch-1'],
            }),
            'stints': pd.DataFrame({
                'club_id': ['1', '1'], 'coach_id': ['10', '99'],
                'club_name': ['Club', 'Club'],
                'coach_slug': ['coach', 'old'],
                'name': ['Coach', 'Old Coach'],
                'role': ['Manager', 'Manager'],
                '_batch_id': ['batch-1', 'batch-1'],
            }),
            'legacy_coaches': pd.DataFrame({
                'current_club_id': ['1'], 'coach_id': ['10'],
                'current_club_name': ['Club'], 'coach_slug': ['coach'],
                'name': ['Coach'], 'dob': ['1970-01-01'],
                'nationality': ['Spain'], 'role': ['Manager'],
                '_batch_id': ['batch-1'],
            }),
        }
        cursor = MagicMock()
        cursor.fetchall.return_value = []
        conn = MagicMock()
        conn.cursor.return_value = cursor
        scraper = MagicMock()
        scraper._bronze_connection.return_value = conn
        results = {'outputs': {
            key: {'table': f'iceberg.bronze.{key}'} for key in frames
        }}

        manifest = mod._persist_dual_write_manifest(
            scraper, mod.ENTITY_SPECS['coaches'], frames, results, 'cycle-1',
            'ENG-Premier League', 2025,
        )

        assert manifest['status'] == 'success'
        assert {row['entity'] for row in manifest['rows']} == {
            'coach_profiles', 'coach_stints',
        }

    def test_bootstrap_cache_requires_native_legacy_key_and_batch_parity(self):
        mod = _import_runner()
        cursor = MagicMock()
        cursor.fetchall.side_effect = [
            [],  # no runtime manifest-derived rows
            [
                ('1', '2025-01-01', '2026-07-01', 'bootstrap-batch'),
                ('2', '2025-01-01', '2026-07-01', 'partial-native-batch'),
            ],
            [
                (
                    '1', 'ENG-Premier League', '2526', '2025-01-01',
                    'bootstrap-batch',
                ),
                (
                    '2', 'ENG-Premier League', '2526', '2025-01-01',
                    'old-legacy-batch',
                ),
            ],
        ]
        conn = MagicMock()
        conn.cursor.return_value = cursor
        scraper = MagicMock()
        scraper._bronze_connection.return_value = conn

        derived = mod._load_data_derived_state(
            scraper,
            'market_value_points',
            'ENG-Premier League',
            2025,
            ['1', '2'],
        )

        assert set(derived) == {'1'}
        assert derived['1']['derived'] == 'bootstrap'
        # Native-only partial commit must not suppress the repair fetch.
        assert '2' not in derived

    def test_bootstrap_complete_other_partition_hydrates_requested_season(self):
        mod = _import_runner()
        cursor = MagicMock()
        cursor.fetchall.side_effect = [
            [],  # no runtime manifest-derived rows
            [('1', '2020-01-01', '2026-07-01', 'bootstrap-batch')],
            [(
                '1', 'ESP-La Liga', '2021', '2020-01-01',
                'bootstrap-batch',
            )],
        ]
        conn = MagicMock()
        conn.cursor.return_value = cursor
        scraper = MagicMock()
        scraper._bronze_connection.return_value = conn

        derived = mod._load_data_derived_state(
            scraper,
            'market_value_points',
            'ENG-Premier League',
            2025,
            ['1'],
        )

        assert derived['1']['derived'] == 'bootstrap'
        assert derived['1']['legacy_partition_ready'] is False
        assert derived['1']['needs_legacy_materialization'] is True

    def test_historical_selection_seeds_parity_proven_bootstrap_keys(self):
        mod = _import_runner()

        class RosterScraper:
            def _resolve_player_ids_from_bronze(self, *args, **kwargs):
                return ['1', '2']

        with (
            patch.object(mod, '_load_fetch_state', return_value={}),
            patch.object(mod, '_load_pending_checkpoint', return_value=({}, None)),
            patch.object(mod, '_load_data_derived_state', return_value={
                '1': {
                    'status': 'success', 'run_key': 'bootstrap-parity',
                    'last_success_at': '2026-01-01', 'row_count': 4,
                },
            }),
            patch.object(mod, '_persist_fetch_state', return_value=True) as persist,
        ):
            selected, cache_hits, seeded, hydrate_ids, _coverage = mod._select_player_ids(
                RosterScraper(),
                mod.ENTITY_SPECS['market_value_history'],
                'ENG-Premier League',
                2025,
                100,
                0,
                'historical',
                'run-1',
                allow_state_writes=True,
            )

        assert selected == ['2']
        assert cache_hits == 1
        assert seeded == 1
        assert hydrate_ids == []
        assert persist.call_args.args[2] == ['1']

    def test_historical_manifest_cache_materializes_missing_legacy_without_http(
        self, temp_output,
    ):
        mod = _import_runner()
        points = pd.DataFrame({
            'player_id': ['1'], 'mv_date': ['2020-01-01'],
            'value_eur': [1_000_000], '_batch_id': ['old-batch'],
        })

        class CacheScraper:
            _batch_id = 'local-batch'
            _last_endpoint_error = None

            def __init__(self):
                self.read_calls = 0

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def read_market_value_points(self, **kwargs):
                self.read_calls += 1
                raise AssertionError('paid reader must not run')

            def materialize_legacy_market_value_history(
                self, native, league, season, season_format,
            ):
                assert str(season_format) == 'SeasonFormat.SPLIT_YEAR'
                assert season == 2025
                return native.assign(league=league, season='2526')

            def get_traffic_stats(self):
                return {
                    'network_fetches': 0,
                    'decoded_response_body_bytes': 0,
                    'decoded_response_body_mb': 0.0,
                }

            def get_stats(self):
                return {'requests': 0}

        scraper = CacheScraper()
        stub_pkg = MagicMock()
        stub_pkg.TransfermarktScraper = MagicMock(return_value=scraper)
        stub_scraper_mod = MagicMock(R0_2B_FALLBACK_MARKER='TM_FALLBACK')

        def committed_outputs(_scraper, spec, frames, _force, results):
            for output in spec.outputs:
                results['outputs'][output.key] = {
                    'rows': len(frames[output.key]),
                    'table': f'iceberg.bronze.{output.table_name}',
                }

        with (
            patch.dict(sys.modules, {
                'scrapers.transfermarkt': stub_pkg,
                'scrapers.transfermarkt.scraper': stub_scraper_mod,
                'scrapers.transfermarkt.registry': REAL_TM_REGISTRY,
            }),
            patch.object(mod, '_select_player_ids', return_value=(
                [], 1, 0, ['1'],
                {'roster_size': 1, 'selected': 0, 'pending': 0},
            )),
            patch.object(mod, '_load_cached_career_frames', return_value={
                'market_value_points': points,
            }),
            patch.object(mod, '_save_frames', side_effect=committed_outputs),
            patch.object(mod, '_persist_dual_write_manifest', return_value={
                'status': 'success', 'rows': [],
            }) as manifest,
            patch.object(
                mod, '_commit_checkpoint_or_pending', return_value='success',
            ) as checkpoint,
        ):
            rc = mod._run_entity(
                mod.ENTITY_SPECS['market_value_history'],
                ['ENG-Premier League'], 2025, 100, temp_output,
                refresh_mode='historical', run_key='run-local-cache',
            )

        assert rc == 0
        assert scraper.read_calls == 0
        assert manifest.call_count == 1
        assert checkpoint.call_args.args[2] == ['1']
        results = _load_results(temp_output)
        assert results['cache_only_materialization'] is True
        assert results['career_cache_materialized_keys'] == 1
        assert results['network_fetches'] == 0
        assert results['checkpoint_status'] == 'success'

    def test_historical_selection_hydrates_only_manifest_proven_native(self):
        mod = _import_runner()

        class RosterScraper:
            def _resolve_player_ids_from_bronze(self, *args, **kwargs):
                return ['1', '2']

        state = {
            '1': {'status': 'success', 'last_success_at': '2026-01-01'},
            '2': {'status': 'success', 'last_success_at': '2026-01-01'},
        }
        with (
            patch.object(mod, '_load_fetch_state', return_value=state),
            patch.object(mod, '_load_pending_checkpoint', return_value=({}, None)),
            patch.object(mod, '_load_data_derived_state', return_value={
                '1': {
                    'status': 'success', 'derived': 'manifest',
                    'row_count': 3, 'needs_legacy_materialization': True,
                },
            }),
            patch.object(mod, '_persist_fetch_state') as persist,
        ):
            selected, cache_hits, seeded, hydrate_ids, _coverage = mod._select_player_ids(
                RosterScraper(),
                mod.ENTITY_SPECS['market_value_history'],
                'ENG-Premier League', 2025, 100, 0, 'historical', 'run-1',
                allow_state_writes=True,
            )

        assert selected == ['2']
        assert hydrate_ids == ['1']
        assert cache_hits == 1
        assert seeded == 0
        persist.assert_not_called()

    def test_post_cleanup_historical_selection_trusts_committed_native_state(self):
        mod = _import_runner()

        class RosterScraper:
            def _resolve_player_ids_from_bronze(self, *args, **kwargs):
                return ['1', '2']

        state = {
            '1': {'status': 'success', 'last_success_at': '2026-07-01'},
            '2': {'status': 'success', 'last_success_at': '2026-07-02'},
        }
        with (
            patch.object(mod, '_load_fetch_state', return_value=state),
            patch.object(mod, '_load_pending_checkpoint', return_value=({}, None)),
            patch.object(mod, '_load_data_derived_state', return_value={}),
        ):
            selected, cache_hits, seeded, hydrate, _coverage = mod._select_player_ids(
                RosterScraper(), mod.ENTITY_SPECS['transfers'],
                'ENG-Premier League', 2025, 100, 0, 'historical',
                'native-only-cycle', allow_state_writes=True,
                legacy_materialization_required=False,
            )

        assert selected == []
        assert cache_hits == 2
        assert seeded == 0
        assert hydrate == []

    def test_committed_checkpoint_outage_uses_recoverable_pending_journal(
        self, tmp_path, monkeypatch,
    ):
        mod = _import_runner()
        durable_dir = tmp_path / 'nested' / 'transfermarkt-checkpoints'
        monkeypatch.setenv('TM_PENDING_CHECKPOINT_DIR', str(durable_dir))
        spec = mod.ENTITY_SPECS['transfers']
        rows = [
            ('success', 2, 'hash-1', None),
            ('valid_empty', 0, 'hash-2', None),
        ]
        with patch.object(mod, '_persist_fetch_state', return_value=False):
            status = mod._commit_checkpoint_or_pending(
                MagicMock(), spec, ['1', '2'], rows, 'run-1',
                'ENG-Premier League', 2025,
            )

        assert status == 'committed_checkpoint_pending'
        assert durable_dir.is_dir()
        state, payload = mod._load_pending_checkpoint(
            'transfer_events', 'ENG-Premier League', 2025,
        )
        assert payload is not None
        assert state['1']['status'] == 'success'
        assert state['2']['status'] == 'valid_empty'
        cross_season_state, _ = mod._load_pending_checkpoint(
            'transfer_events', 'ESP-La Liga', 2018,
        )
        assert cross_season_state['2']['status'] == 'valid_empty'
        source = inspect.getsource(mod._run_entity)
        assert source.index('_persist_dual_write_manifest') < source.index(
            '_commit_checkpoint_or_pending'
        )

    def test_pending_checkpoint_load_prefers_newest_fallback_file(
        self, tmp_path, monkeypatch,
    ):
        mod = _import_runner()
        preferred_root = tmp_path / 'durable'
        preferred_root.mkdir()
        monkeypatch.setenv('TM_PENDING_CHECKPOINT_DIR', str(preferred_root))
        endpoint = f'test-endpoint-{tmp_path.name}'
        league = 'ENG-Premier League'
        season = 2025
        preferred_path = mod._pending_checkpoint_path(
            endpoint, league, season, root_override=str(preferred_root),
        )
        fallback_path = mod._pending_checkpoint_path(
            endpoint, league, season, root_override='/tmp',
        )
        base = {
            'endpoint': endpoint,
            'league': league,
            'season': season,
            'parser_version': mod.PARSER_VERSION,
            'schema_version': mod.SCHEMA_VERSION,
        }
        old_payload = {
            **base,
            'run_key': 'old-run',
            'created_at': (
                pd.Timestamp.now(tz='UTC') - pd.Timedelta(days=1)
            ).isoformat(),
            'rows': [{
                'source_id': '1', 'status': 'success', 'row_count': 1,
                'run_key': 'old-run',
            }],
        }
        new_payload = {
            **base,
            'run_key': 'new-run',
            'created_at': pd.Timestamp.now(tz='UTC').isoformat(),
            'rows': [{
                'source_id': '2', 'status': 'valid_empty', 'row_count': 0,
                'run_key': 'new-run',
            }],
        }
        try:
            with open(preferred_path, 'w') as fh:
                json.dump(old_payload, fh)
            with open(fallback_path, 'w') as fh:
                json.dump(new_payload, fh)

            state, payload = mod._load_pending_checkpoint(
                endpoint, league, season,
            )
        finally:
            try:
                os.unlink(fallback_path)
            except FileNotFoundError:
                pass

        assert payload['run_key'] == 'new-run'
        assert set(state) == {'2'}
        assert state['2']['status'] == 'valid_empty'

    def test_coach_history_ttl_selects_only_missing_or_stale_clubs(self):
        mod = _import_runner()
        memberships = pd.DataFrame({
            'club_id': ['1', '2', '3'],
            'club_slug': ['one', 'two', 'three'],
            'club_name': ['One', 'Two', 'Three'],
        })
        now = pd.Timestamp.now(tz='UTC')
        state = {
            '1': {'status': 'success', 'last_success_at': now},
            '2': {
                'status': 'success',
                'last_success_at': now - pd.Timedelta(days=40),
            },
        }
        with (
            patch.object(mod, '_load_fetch_state', return_value=state),
            patch.object(mod, '_load_pending_checkpoint', return_value=({}, None)),
            patch.object(mod, '_load_data_derived_state', return_value={}),
        ):
            selected_frame, selected, cached, cache_hits, seeded = (
                mod._select_coach_memberships(
                    MagicMock(), memberships, 'ENG-Premier League', 2025,
                    'current', 'run-1', 28, allow_state_writes=False,
                )
            )

        assert selected == ['2', '3']
        assert cached == ['1']
        assert set(selected_frame['club_id']) == {'2', '3'}
        assert cache_hits == 1
        assert seeded == 0

    def test_coach_partial_refresh_replaces_legacy_by_club(self):
        mod = _import_runner()
        legacy = next(
            output for output in mod.ENTITY_SPECS['coaches'].outputs
            if output.is_legacy
        )
        assert legacy.replace_keys == (
            'league', 'season', 'current_club_id',
        )

    def test_coach_cache_materializes_season_without_network_reader(self):
        mod = _import_runner()
        cached = {
            'profiles': pd.DataFrame({
                'coach_id': ['10'], 'name': ['Coach'],
            }),
            'stints': pd.DataFrame({
                'club_id': ['1'], 'coach_id': ['10'],
                'appointed_date': ['2025-01-01'], 'left_date': [None],
            }),
        }
        scraper = MagicMock()
        scraper.materialize_legacy_coaches.return_value = pd.DataFrame({
            'coach_id': ['10'], 'current_club_id': ['1'],
        })

        frames = mod._merge_coach_cache_frames(
            scraper, {}, cached, 'ENG-Premier League', 2025,
        )

        assert list(frames['profiles']['coach_id']) == ['10']
        assert list(frames['stints']['club_id']) == ['1']
        assert list(frames['legacy_coaches']['coach_id']) == ['10']
        scraper.materialize_legacy_coaches.assert_called_once()

    def test_coach_cache_empty_season_reconciles_legacy_without_http(
        self, temp_output,
    ):
        mod = _import_runner()
        memberships = pd.DataFrame({
            'club_id': ['1'], 'club_slug': ['one'], 'club_name': ['One'],
        })
        cached = {
            'profiles': pd.DataFrame({
                'coach_id': ['10'], 'coach_slug': ['coach'], 'name': ['Coach'],
                '_batch_id': ['old-batch'],
            }),
            'stints': pd.DataFrame({
                'club_id': ['1'], 'coach_id': ['10'],
                'appointed_date': ['2010-01-01'], 'left_date': ['2011-01-01'],
                '_batch_id': ['old-batch'],
            }),
        }

        class CacheScraper:
            _batch_id = 'local-batch'
            _last_endpoint_error = None

            def __init__(self):
                self.read_calls = 0

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def read_coach_data(self, **kwargs):
                self.read_calls += 1
                raise AssertionError('paid reader must not run')

            def materialize_legacy_coaches(self, *args):
                return pd.DataFrame(columns=['coach_id', 'current_club_id'])

            def get_traffic_stats(self):
                return {
                    'network_fetches': 0,
                    'decoded_response_body_bytes': 0,
                    'decoded_response_body_mb': 0.0,
                }

            def get_stats(self):
                return {'requests': 0}

        scraper = CacheScraper()
        stub_pkg = MagicMock()
        stub_pkg.TransfermarktScraper = MagicMock(return_value=scraper)
        stub_scraper_mod = MagicMock(R0_2B_FALLBACK_MARKER='TM_FALLBACK')

        def committed_outputs(_scraper, spec, frames, _force, results):
            for output in spec.outputs:
                if frames[output.key].empty:
                    continue
                results['outputs'][output.key] = {
                    'rows': len(frames[output.key]),
                    'table': f'iceberg.bronze.{output.table_name}',
                }

        with (
            patch.dict(sys.modules, {
                'scrapers.transfermarkt': stub_pkg,
                'scrapers.transfermarkt.scraper': stub_scraper_mod,
                'scrapers.transfermarkt.registry': REAL_TM_REGISTRY,
            }),
            patch.object(mod, '_load_coach_memberships', return_value=memberships),
            patch.object(mod, '_select_coach_memberships', return_value=(
                memberships.iloc[0:0], [], ['1'], 1, 0,
            )),
            patch.object(mod, '_load_cached_coach_data', return_value=cached),
            patch.object(mod, '_save_frames', side_effect=committed_outputs),
            patch.object(mod, '_delete_empty_season_coach_rows') as clear_season,
            patch.object(mod, '_delete_removed_coach_clubs'),
            patch.object(mod, '_persist_dual_write_manifest', return_value={
                'status': 'success', 'rows': [],
            }) as manifest,
        ):
            rc = mod._run_entity(
                mod.ENTITY_SPECS['coaches'],
                ['ENG-Premier League'], 2025, None, temp_output,
                refresh_mode='historical', run_key='run-empty-season',
            )

        assert rc == 0
        assert scraper.read_calls == 0
        clear_season.assert_called_once()
        assert clear_season.call_args.args[2].empty
        manifest.assert_called_once()
        results = _load_results(temp_output)
        assert results['rows'] == 0
        assert results['cache_only_materialization'] is True
        assert results['dual_write_complete'] is True
        assert results['checkpoint_status'] == 'cache_complete'
        assert results['network_fetches'] == 0


class TestCredentialRedaction:
    def test_redaction_supports_http_and_socks_proxy_urls(self):
        mod = _import_runner()

        assert mod._redact_sensitive(
            'http://user:pass@proxy.invalid:1'
        ) == 'http://****:****@proxy.invalid:1'
        assert mod._redact_sensitive(
            'socks5://user:pass@proxy.invalid:2'
        ) == 'socks5://****:****@proxy.invalid:2'

    def test_write_results_redacts_nested_payload(self, temp_output):
        mod = _import_runner()
        payload = {
            'entity': 'players',
            'errors': ['failed http://private:secret@proxy.invalid:1'],
            'traffic': {
                'telemetry_available': False,
                'telemetry_error': 'socks5://other:hidden@proxy.invalid:2',
            },
        }

        mod._write_results(temp_output, payload)

        with open(temp_output) as fh:
            raw = fh.read()
        assert 'private' not in raw
        assert 'secret' not in raw
        assert 'other' not in raw
        assert 'hidden' not in raw
        assert 'http://****:****@proxy.invalid:1' in raw
        assert 'socks5://****:****@proxy.invalid:2' in raw

    def test_hard_scrape_exception_is_redacted_in_result(self, temp_output):
        scraper = _build_scraper(df=_players_df(1))
        scraper.read_players.side_effect = RuntimeError(
            'TLS failed via http://private:secret@proxy.invalid:1'
        )
        scraper.get_traffic_stats.return_value = {
            'decoded_response_body_mb': 0.0,
            'network_fetches': 0,
        }
        scraper.get_stats.return_value = {'requests': 0}

        rc = _run_main(
            ['--entity', 'players', '--output', temp_output], scraper,
        )

        assert rc == 1
        with open(temp_output) as fh:
            raw = fh.read()
        assert 'private' not in raw
        assert 'secret' not in raw
        assert 'http://****:****@proxy.invalid:1' in raw


class TestNativeV2RecoverySafety:
    def test_mb_only_telemetry_fails_shared_raw_byte_guard(self, temp_output):
        scraper = _build_scraper(df=_players_df(3))
        scraper.get_traffic_stats.return_value = {
            'decoded_response_body_mb': 0.25,
            'network_fetches': 1,
        }

        rc = _run_main(
            ['--entity', 'players', '--dry-run', '--limit', '3',
             '--output', temp_output],
            scraper,
        )

        assert rc == 1
        results = _load_results(temp_output)
        assert results['traffic']['telemetry_available'] is False
        assert 'decoded_response_body_bytes' not in results['traffic']
        assert any(
            'raw decoded_response_body_bytes telemetry unavailable' in error
            for error in results['errors']
        )
        assert results['cycle_budget']['remaining_after_bytes'] == 0
        assert (
            results['cycle_budget']['telemetry_unknown_reservation_bytes']
            == 15 * 1024 * 1024
        )

    def test_telemetry_exception_exhausts_cycle_after_successful_read(
        self, temp_output,
    ):
        scraper = _build_scraper(df=_players_df(3))
        scraper.get_traffic_stats.side_effect = RuntimeError('counter offline')

        rc = _run_main(
            ['--entity', 'players', '--dry-run', '--limit', '3',
             '--output', temp_output],
            scraper,
        )

        assert rc == 1
        results = _load_results(temp_output)
        assert results['rows'] == 3
        assert results['cycle_budget']['remaining_after_bytes'] == 0
        assert results['traffic']['telemetry_available'] is False
        assert any('raw decoded_response_body_bytes' in e for e in results['errors'])

    def test_negative_raw_counter_is_unknown_not_a_cycle_credit(
        self, temp_output,
    ):
        scraper = _build_scraper(df=_players_df(3))
        scraper.get_traffic_stats.return_value = {
            'decoded_response_body_bytes': -1,
            'network_fetches': 1,
        }
        rc = _run_main(
            ['--entity', 'players', '--dry-run', '--limit', '3',
             '--output', temp_output],
            scraper,
        )
        assert rc == 1
        result = _load_results(temp_output)
        assert result['cycle_budget']['remaining_after_bytes'] == 0
        assert result['traffic']['telemetry_available'] is False

    def test_shared_cycle_budget_is_raw_byte_exact(
        self, tmp_path, monkeypatch,
    ):
        mod = _import_runner()
        monkeypatch.setenv('TM_CYCLE_BUDGET_DIR', str(tmp_path))
        limit = 15 * 1024 * 1024

        before = mod._prepare_cycle_budget('cycle-1', limit)
        assert before['consumed_before_bytes'] == 0
        after = mod._record_cycle_traffic(
            'cycle-1', limit, 'players', limit + 1,
        )

        assert after['consumed_after_bytes'] == limit + 1
        assert after['remaining_after_bytes'] == -1
        assert after['exhausted'] is True
        with pytest.raises(RuntimeError, match='budget exhausted'):
            mod._prepare_cycle_budget('cycle-1', limit)

    def test_cycle_reservations_bound_parallel_and_crashed_workers(
        self, tmp_path, monkeypatch,
    ):
        mod = _import_runner()
        monkeypatch.setenv('TM_CYCLE_BUDGET_DIR', str(tmp_path))

        first = mod._prepare_cycle_budget(
            'parallel-cycle', 100, entity='players', reserve_bytes=80,
        )
        second = mod._prepare_cycle_budget(
            'parallel-cycle', 100, entity='transfers', reserve_bytes=80,
        )
        assert first['reserved_bytes'] == 80
        assert second['reserved_bytes'] == 20
        with pytest.raises(RuntimeError, match='budget exhausted'):
            mod._prepare_cycle_budget(
                'parallel-cycle', 100, entity='coaches', reserve_bytes=1,
            )

        # The second reservation simulates SIGKILL and stays fully charged.
        settled = mod._record_cycle_traffic(
            'parallel-cycle', 100, 'players', 30,
            reservation_id=first['reservation_id'],
        )
        assert settled['accounted_after_bytes'] == 50
        third = mod._prepare_cycle_budget(
            'parallel-cycle', 100, entity='coaches', reserve_bytes=80,
        )
        assert third['reserved_bytes'] == 50

    def test_cycle_budget_identity_mismatch_fails_closed(
        self, tmp_path, monkeypatch,
    ):
        mod = _import_runner()
        monkeypatch.setenv('TM_CYCLE_BUDGET_DIR', str(tmp_path))
        mod._prepare_cycle_budget('cycle-2', 100)
        with pytest.raises(RuntimeError, match='identity/budget mismatch'):
            mod._prepare_cycle_budget('cycle-2', 101)

    def test_not_found_is_never_authoritative_empty(self):
        mod = _import_runner()

        class Scraper:
            def get_fetch_outcomes(self):
                return {'market_value_points': {'1': {'status': 'not_found'}}}

        spec = mod.ENTITY_SPECS['market_value_history']
        rows = mod._state_rows(
            Scraper(), spec, ['1'], pd.DataFrame(columns=['player_id']),
        )

        assert rows[0][0] == 'not_found'
        assert mod._valid_empty_ids(Scraper(), spec, ['1']) == []

    def test_pending_success_overrides_older_failed_ops_state(self):
        mod = _import_runner()

        class RosterScraper:
            def _resolve_player_ids_from_bronze(self, *_args, **_kwargs):
                return ['1']

        pending = {
            '1': {
                'status': 'success', 'run_key': 'run-1',
                'last_success_at': pd.Timestamp.now(tz='UTC'),
                'pending': True,
            },
        }
        payload = {'rows': [{'source_id': '1'}]}
        with (
            patch.object(mod, '_load_fetch_state', return_value={
                '1': {'status': 'failed', 'run_key': 'older-run'},
            }),
            patch.object(
                mod, '_load_pending_checkpoint', return_value=(pending, payload),
            ),
            patch.object(mod, '_flush_pending_checkpoint', return_value=True),
            patch.object(mod, '_clear_pending_checkpoint'),
            patch.object(mod, '_load_data_derived_state', return_value={}),
        ):
            selected, cache_hits, _seeded, _hydrate, _coverage = mod._select_player_ids(
                RosterScraper(), mod.ENTITY_SPECS['market_value_history'],
                'ENG-Premier League', 2025, 100, 0, 'current', 'run-1',
                allow_state_writes=True,
            )

        assert selected == []
        assert cache_hits == 1

    def test_pending_merge_preserves_per_row_committed_at(
        self, tmp_path, monkeypatch,
    ):
        mod = _import_runner()
        monkeypatch.setenv('TM_PENDING_CHECKPOINT_DIR', str(tmp_path))
        spec = mod.ENTITY_SPECS['transfers']
        assert mod._write_pending_checkpoint(
            spec, ['1'], [('success', 1, 'h1', None)], 'run-1',
            'ENG-Premier League', 2025,
        )
        path = mod._pending_checkpoint_path(
            'transfer_events', 'ENG-Premier League', 2025,
            root_override=str(tmp_path),
        )
        with open(path) as fh:
            first = json.load(fh)
        original = (
            pd.Timestamp.now(tz='UTC') - pd.Timedelta(days=10)
        ).isoformat()
        first['rows'][0]['committed_at'] = original
        with open(path, 'w') as fh:
            json.dump(first, fh)

        assert mod._write_pending_checkpoint(
            spec, ['2'], [('success', 1, 'h2', None)], 'run-2',
            'ENG-Premier League', 2025,
        )
        with open(path) as fh:
            merged = json.load(fh)
        rows = {row['source_id']: row for row in merged['rows']}
        assert rows['1']['committed_at'] == original
        assert rows['2']['committed_at'] != original

    def test_read_only_result_does_not_persist_ops_traffic(self, temp_output):
        mod = _import_runner()
        traffic_mod = importlib.import_module('utils.proxy_traffic')
        payload = {
            'entity': 'players', 'run_key': 'dry-run',
            'traffic': {
                'telemetry_available': True,
                'decoded_response_body_bytes': 0,
                'decoded_response_body_mb': 0.0,
            },
        }
        with patch.object(traffic_mod, 'record_traffic_run') as persist:
            mod._write_results(
                temp_output, payload, persist_traffic=False,
            )
        persist.assert_not_called()

    def test_dead_compatibility_wrappers_are_removed(self):
        mod = _import_runner()
        for name in ('_run_players', '_run_mv_history', '_run_transfers', '_run_coaches'):
            assert not hasattr(mod, name)


def test_cli_keeps_child_run_key_but_uses_explicit_parent_cycle_ledger(
    monkeypatch,
):
    mod = _import_runner()
    captured = {}

    def fake_run(*args, **kwargs):
        captured.update(kwargs)
        return 0

    monkeypatch.delenv('TM_COMPETITION_RECORDS_JSON', raising=False)
    monkeypatch.setattr(mod, '_run_entity', fake_run)
    monkeypatch.setattr(sys, 'argv', [
        'run_transfermarkt_scraper.py',
        '--entity', 'players',
        '--competition-id', 'GB1',
        '--edition-id', '2025',
        '--run-key', 'tm-child-exact',
        '--cycle-ledger-key', 'scheduled__parent',
        '--expected-reader-revision', '3',
    ])

    assert mod.main() == 0
    assert captured['run_key'] == 'tm-child-exact'
    assert captured['cycle_ledger_key'] == 'scheduled__parent'


class TestCoachCacheMergeSeason:
    def test_the_cache_merge_dates_the_season_the_scraper_dated(
        self, monkeypatch,
    ):
        """The scraper projects the season, then this merge reprojects it from
        the union with bronze.  Letting the merge default the season format
        turned a calendar season into a split one, and the two projections
        disagreed key for key — which is exactly what the parity gate reads.
        """
        import pandas as pd
        from scrapers.transfermarkt.registry import (
            CompetitionType, SeasonFormat, TeamType, _bootstrap_record,
        )

        mod = _import_runner()
        record = _bootstrap_record(
            competition_id='2DVB',
            slug='2-division-b',
            name='Second League Division B',
            country='Russia',
            confederation='UEFA',
            competition_type=CompetitionType.DOMESTIC_LEAGUE,
            team_type=TeamType.CLUB,
            season_format=SeasonFormat.SINGLE_YEAR,
            source_url=(
                'https://www.transfermarkt.com/2-division-b/startseite/'
                'wettbewerb/2DVB'
            ),
            canonical_competition_id='TM-2DVB',
        )
        monkeypatch.setattr(mod, '_competition_record', lambda value: record)
        monkeypatch.setenv('TM_CANONICAL_SEASON', '2024')
        seen = {}

        class _Scraper:
            def materialize_legacy_coaches(
                self, profiles, stints, league, season, season_format,
            ):
                seen.update(season=season, season_format=season_format)
                return pd.DataFrame([{'coach_id': '10'}])

        frames = mod._merge_coach_cache_frames(
            _Scraper(),
            {},
            {
                'profiles': pd.DataFrame([{'coach_id': '10'}]),
                'stints': pd.DataFrame([{
                    'club_id': '1', 'coach_id': '10',
                    'appointed_date': None, 'left_date': None,
                }]),
            },
            'TM-2DVB',
            2023,
        )

        assert seen['season'] == 2024
        assert seen['season_format'] is SeasonFormat.SINGLE_YEAR
        assert list(frames['legacy_coaches']['coach_id']) == ['10']
