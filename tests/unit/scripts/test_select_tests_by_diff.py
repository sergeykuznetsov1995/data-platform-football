"""
Unit tests for scripts/select_tests_by_diff.py (#2244).

Strategy
--------
The selector is a top-level script (not a package), loaded via
``importlib.util`` from its absolute path — same pattern as
``test_audit_silver_charter.py``. ``select_tests``/``classify``/``derive_stem``
are pure functions over string lists, so mapping behaviour is tested by
injection (no git, no repo). CLI behaviour is tested through ``main()`` with
``scan_available_tests``/``REPO_ROOT`` monkeypatched; real-git integration
cases build a throwaway repository under ``tmp_path``.

Coverage map (issue #2244 test plan):
- groups 1-8: mapping table, FULL escalations, stem boundaries,
  no-test-mapping, short stems, unmapped paths, deleted files, multi-file.
- group 9: CLI contract (json / pytest-args / exit codes / determinism).
- group 10: real-git integration against throwaway tmp_path repositories
  (default merge-base base, --worktree staged+unstaged, --no-renames vs
  diff.renames config, bad --base ref).
"""

from __future__ import annotations

import importlib.util
import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
_SCRIPT_PATH = REPO_ROOT / 'scripts' / 'select_tests_by_diff.py'


def _load_module():
    spec = importlib.util.spec_from_file_location('select_tests_by_diff', _SCRIPT_PATH)
    assert spec is not None and spec.loader is not None, f'cannot load {_SCRIPT_PATH}'
    mod = importlib.util.module_from_spec(spec)
    sys.modules['select_tests_by_diff'] = mod
    spec.loader.exec_module(mod)
    return mod


mod = _load_module()

AVAILABLE = [
    'tests/unit/dags/test_dag_ingest_fbref.py',
    'tests/unit/dags/test_transfermarkt_registry_publish.py',
    'tests/unit/scrapers/test_base_scraper.py',
    'tests/unit/scrapers/test_fbref_html_parser.py',
    'tests/unit/scrapers/test_run_sofascore_scraper.py',
    'tests/unit/scripts/test_audit_silver_charter.py',
    'tests/unit/sql/test_dim_competition_render.py',
    'tests/unit/sql/test_espn_match_silver.py',
    'tests/unit/sql/test_espn_match_events_silver.py',
    'tests/unit/utils/test_xref_player_resolver.py',
    'tests/unit/utils/test_xref_player_resolver_dob.py',
    'tests/unit/utils/test_xref_player_resolver_v2.py',
]


# ---------------------------------------------------------------------------
# Group 1: code -> test mapping (stem search, cross-directory, suffixes)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ('changed', 'expected'),
    [
        # source-prefix case: module parsers under fbref -> fbref_html_parser
        ('scrapers/fbref/html_parser.py',
         ['tests/unit/scrapers/test_fbref_html_parser.py']),
        # cross-directory: dags/scripts runner lives in tests/unit/scrapers
        ('dags/scripts/run_sofascore_scraper.py',
         ['tests/unit/scrapers/test_run_sofascore_scraper.py']),
        # cross-directory: dags/utils module tested under tests/unit/dags
        ('dags/utils/transfermarkt_registry_publish.py',
         ['tests/unit/dags/test_transfermarkt_registry_publish.py']),
        # dag file maps to its dag test
        ('dags/dag_ingest_fbref.py',
         ['tests/unit/dags/test_dag_ingest_fbref.py']),
        # plain scripts/ mapping
        ('scripts/audit_silver_charter.py',
         ['tests/unit/scripts/test_audit_silver_charter.py']),
        # sql template mapping (render tests)
        ('dags/sql/gold/dim_competition.sql.j2',
         ['tests/unit/sql/test_dim_competition_render.py']),
        # suffix variants are all selected
        ('dags/utils/xref_player_resolver.py',
         ['tests/unit/utils/test_xref_player_resolver.py',
          'tests/unit/utils/test_xref_player_resolver_dob.py',
          'tests/unit/utils/test_xref_player_resolver_v2.py']),
        # over-selection by word boundary is accepted (events test too)
        ('dags/sql/silver/espn_match.sql',
         ['tests/unit/sql/test_espn_match_events_silver.py',
          'tests/unit/sql/test_espn_match_silver.py']),
        # a changed unit test selects itself
        ('tests/unit/scripts/test_audit_silver_charter.py',
         ['tests/unit/scripts/test_audit_silver_charter.py']),
    ],
)
def test_mapping_selects_expected_tests(changed, expected):
    verdict, payload = mod.select_tests([changed], AVAILABLE)
    assert verdict == 'list'
    assert payload == expected


# ---------------------------------------------------------------------------
# Group 2: FULL escalations (path -> reason)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ('changed', 'reason'),
    [
        ('tests/unit/dags/conftest.py', 'conftest'),
        ('tests/unit/scrapers/conftest.py', 'conftest'),
        ('conftest.py', 'conftest'),
        ('pyproject.toml', 'pytest-config'),
        ('requirements-ci.txt', 'requirements'),
        ('requirements/test/fbref-py312.lock', 'requirements'),
        ('requirements/test/x.in', 'requirements'),
        ('scrapers/base/scraper.py', 'scraper-base'),
        ('scrapers/utils/proxy_manager.py', 'scraper-shared-utils'),
        ('scrapers/fotmob/__init__.py', 'package-init'),
        ('dags/utils/__init__.py', 'package-init'),
        ('tests/unit/configs/__init__.py', 'package-init'),
        ('tests/unit/fixtures/raw_payload.json', 'fixtures'),
        ('tests/unit/dags/fixtures/mini.html', 'fixtures'),
        ('configs/medallion/leagues.yaml', 'configs'),
        ('configs/transfermarkt/registry_policy.json', 'configs'),
        ('dags/plugins/whoscored_bootstrap_timetable.py', 'dag-plugins'),
        ('tests/integration/test_real_requests.py', 'tests-non-unit'),
        ('tests/integration/scrapers/conftest.py', 'conftest'),
        ('deploy/fotmob/docker-compose.yml', 'unmapped-path'),
        ('docker/images/airflow/Dockerfile', 'unmapped-path'),
        ('compose.yaml', 'unmapped-path'),
        ('Makefile', 'unmapped-path'),
        ('.env.example', 'unmapped-path'),
    ],
)
def test_full_escalations(changed, reason):
    verdict, payload = mod.select_tests([changed], AVAILABLE)
    assert verdict == 'full'
    assert payload == [(changed, reason)]


# ---------------------------------------------------------------------------
# Group 3: stem word boundaries
# ---------------------------------------------------------------------------


def test_short_stem_word_boundary_no_false_match():
    # `base` must NOT match `database_x` (no word boundary before "base")
    verdict, payload = mod.select_tests(
        ['scrapers/espn/base.py'],
        ['tests/unit/scrapers/test_database_x.py'],
    )
    assert verdict == 'full'
    assert payload == [('scrapers/espn/base.py', 'no-test-mapping')]


def test_scraper_base_full_regardless_of_matches():
    verdict, payload = mod.select_tests(
        ['scrapers/base/anything.py'],
        ['tests/unit/scrapers/test_base_scraper.py'],
    )
    assert verdict == 'full'
    assert payload == [('scrapers/base/anything.py', 'scraper-base')]


# ---------------------------------------------------------------------------
# Group 4: no-test-mapping and docs-only diffs
# ---------------------------------------------------------------------------


def test_code_without_tests_escalates():
    verdict, payload = mod.select_tests(['scrapers/espn/new_module.py'], AVAILABLE)
    assert verdict == 'full'
    assert payload == [('scrapers/espn/new_module.py', 'no-test-mapping')]


def test_docs_and_github_contribute_nothing():
    verdict, payload = mod.select_tests(
        ['README.md', 'docs/design/x.md', '.github/workflows/nightly-unit-suite.yml'],
        AVAILABLE,
    )
    assert verdict == 'list'
    assert payload == []


# ---------------------------------------------------------------------------
# Group 5: short stems
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    'changed',
    ['scrapers/espn/abc.py', 'dags/sql/silver/x.py', 'scripts/ab.py'],
)
def test_short_stem_escalates(changed):
    verdict, payload = mod.select_tests([changed], AVAILABLE)
    assert verdict == 'full'
    assert payload == [(changed, 'short-stem')]


def test_short_stem_escalates_even_with_matching_test():
    # a short stem escalates unconditionally, even when a name-matching
    # test exists ("abc" would match tests/unit/scrapers/test_abc.py)
    verdict, payload = mod.select_tests(
        ['scrapers/espn/abc.py'], ['tests/unit/scrapers/test_abc.py']
    )
    assert verdict == 'full'
    assert payload == [('scrapers/espn/abc.py', 'short-stem')]


# ---------------------------------------------------------------------------
# Group 6: unmapped roots
# ---------------------------------------------------------------------------


def test_unmapped_roots_escalate():
    for path in ('deploy/x.yml', 'compose.yaml', 'Makefile'):
        verdict, payload = mod.select_tests([path], AVAILABLE)
        assert verdict == 'full'
        assert payload == [(path, 'unmapped-path')]


def test_dags_sql_unknown_extension_does_not_map():
    # not .sql/.sql.j2 -> not code for the stem search; no crash, plain FULL
    verdict, payload = mod.select_tests(['dags/sql/gold/data.yaml'], AVAILABLE)
    assert verdict == 'full'
    assert payload == [('dags/sql/gold/data.yaml', 'unmapped-path')]


# ---------------------------------------------------------------------------
# Group 7: deleted files
# ---------------------------------------------------------------------------


def test_deleted_source_maps_by_stem():
    # path need not exist on disk; stem search still applies
    verdict, payload = mod.select_tests(
        ['scrapers/fbref/html_parser.py'], AVAILABLE
    )
    assert verdict == 'list'
    assert 'tests/unit/scrapers/test_fbref_html_parser.py' in payload


def test_deleted_test_is_dropped():
    # a changed test file missing from available (deleted) contributes nothing
    verdict, payload = mod.select_tests(
        ['tests/unit/scrapers/test_gone.py'], AVAILABLE
    )
    assert verdict == 'list'
    assert payload == []


# ---------------------------------------------------------------------------
# Group 8: multi-file behaviour
# ---------------------------------------------------------------------------


def test_union_dedup_sorted_and_full_wins():
    verdict, payload = mod.select_tests(
        [
            'scrapers/fbref/html_parser.py',
            'scripts/audit_silver_charter.py',
            'dags/utils/xref_player_resolver.py',
            'dags/dag_ingest_fbref.py',
            'scripts/audit_silver_charter.py',
        ],
        AVAILABLE,
    )
    assert verdict == 'list'
    assert payload == sorted(set(payload))
    assert 'tests/unit/scrapers/test_fbref_html_parser.py' in payload
    assert 'tests/unit/utils/test_xref_player_resolver_v2.py' in payload


def test_multiple_full_triggers_all_reported_sorted_by_path():
    verdict, payload = mod.select_tests(
        ['pyproject.toml', 'tests/unit/dags/conftest.py'], AVAILABLE
    )
    assert verdict == 'full'
    # sorted() over (path, reason) tuples: 'p' < 't', so pyproject.toml first
    assert payload == [
        ('pyproject.toml', 'pytest-config'),
        ('tests/unit/dags/conftest.py', 'conftest'),
    ]


# ---------------------------------------------------------------------------
# Group 9: CLI contract (git-free paths via --files and monkeypatched scan)
# ---------------------------------------------------------------------------


def _run_main(capsys, monkeypatch, argv, available=AVAILABLE):
    monkeypatch.setattr(mod, 'REPO_ROOT', Path('/nonexistent-repo'))
    monkeypatch.setattr(
        mod, 'scan_available_tests', lambda root: list(available)
    )
    code = mod.main(argv)
    out = capsys.readouterr().out
    return code, out


def test_cli_files_list_output(capsys, monkeypatch):
    code, out = _run_main(
        capsys,
        monkeypatch,
        ['--files', 'scrapers/fbref/html_parser.py'],
    )
    assert code == 0
    assert out.strip() == 'tests/unit/scrapers/test_fbref_html_parser.py'


def test_cli_files_empty_output(capsys, monkeypatch):
    code, out = _run_main(capsys, monkeypatch, ['--files', 'README.md'])
    assert code == 0
    assert out == ''


def test_cli_full_line_and_exit_zero(capsys, monkeypatch):
    code, out = _run_main(capsys, monkeypatch, ['--files', 'pyproject.toml'])
    assert code == 0
    assert out.startswith('FULL pytest-config: pyproject.toml')


def test_cli_pytest_args_full_prints_tests_unit(capsys, monkeypatch):
    code, out = _run_main(
        capsys, monkeypatch, ['--files', 'pyproject.toml', '--pytest-args']
    )
    assert code == 0
    assert out.strip() == 'tests/unit'


def test_cli_json_schema(capsys, monkeypatch):
    code, out = _run_main(
        capsys,
        monkeypatch,
        ['--files', 'dags/utils/xref_player_resolver.py', '--json'],
    )
    assert code == 0
    data = json.loads(out)
    assert data['status'] == 'list'
    assert data['tests'] == [
        'tests/unit/utils/test_xref_player_resolver.py',
        'tests/unit/utils/test_xref_player_resolver_dob.py',
        'tests/unit/utils/test_xref_player_resolver_v2.py',
    ]
    assert data['full_triggers'] == []
    assert data['base'] is None and data['head'] is None


def test_cli_json_full_triggers(capsys, monkeypatch):
    code, out = _run_main(
        capsys,
        monkeypatch,
        ['--files', 'pyproject.toml', 'README.md', '--json'],
    )
    assert code == 0
    data = json.loads(out)
    assert data['status'] == 'full'
    assert data['tests'] == []
    assert data['full_triggers'] == [
        {'path': 'pyproject.toml', 'reason': 'pytest-config'}
    ]


def test_cli_git_failure_exit_three(monkeypatch, capsys):
    def boom(*args, **kwargs):
        raise RuntimeError('not a git repository')

    monkeypatch.setattr(mod, 'resolve_diff', boom)
    code = mod.main([])
    assert code == 3


def test_cli_determinism(capsys, monkeypatch):
    outs = []
    for _ in range(2):
        code, out = _run_main(
            capsys, monkeypatch, ['--files', 'dags/dag_ingest_fbref.py', '--json']
        )
        assert code == 0
        outs.append(out)
    assert outs[0] == outs[1]


# ---------------------------------------------------------------------------
# Group 10: real-git integration (offline tmp_path repos; git required)
# ---------------------------------------------------------------------------

GIT = shutil.which('git')

pytestmark_git = pytest.mark.skipif(GIT is None, reason='git not available')


def _init_repo(tmp_path: Path) -> Path:
    repo = tmp_path / 'repo'
    repo.mkdir()
    subprocess.run(
        ['git', 'init', '-q', '-b', 'master', str(repo)], check=True
    )
    subprocess.run(
        ['git', '-C', str(repo), 'config', 'user.email', 't@example.com'],
        check=True,
    )
    subprocess.run(
        ['git', '-C', str(repo), 'config', 'user.name', 'T'], check=True
    )
    return repo


def _commit(repo: Path, message: str) -> None:
    subprocess.run(['git', '-C', str(repo), 'add', '-A'], check=True)
    subprocess.run(
        ['git', '-C', str(repo), 'commit', '-q', '-m', message], check=True
    )


def _write(repo: Path, rel: str, text: str) -> None:
    path = repo / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding='utf-8')


@pytestmark_git
def test_real_git_merge_base_excludes_advanced_master(tmp_path, monkeypatch, capsys):
    """Branch diff must NOT include master-only changes (three-dot diff).

    master advanced past the branch point with a FULL-triggering edit
    (scrapers/base/scraper.py); the branch carries its own mappable edit.
    The default base (merge-base HEAD origin/master) must diff against the
    branch point, so the verdict stays focused on the branch change.
    """
    repo = _init_repo(tmp_path)
    _write(repo, 'scrapers/base/scraper.py', 'BASE = 1\n')
    _write(repo, 'scrapers/fbref/html_parser.py', 'def parse():\n    return 1\n')
    _write(
        repo,
        'tests/unit/scrapers/test_fbref_html_parser.py',
        'def test_parse():\n    assert True\n',
    )
    _commit(repo, 'initial')

    subprocess.run(
        ['git', '-C', str(repo), 'checkout', '-q', '-b', 'feature'], check=True
    )

    # master-only edit AFTER the branch point (would give FULL scraper-base)
    subprocess.run(
        ['git', '-C', str(repo), 'checkout', '-q', 'master'], check=True
    )
    _write(repo, 'scrapers/base/scraper.py', 'BASE = 2\n')
    _commit(repo, 'master-only scraper/base edit')
    master_sha = subprocess.run(
        ['git', '-C', str(repo), 'rev-parse', 'master'],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    subprocess.run(
        [
            'git',
            '-C',
            str(repo),
            'update-ref',
            'refs/remotes/origin/master',
            master_sha,
        ],
        check=True,
    )

    # branch-only mappable edit
    subprocess.run(
        ['git', '-C', str(repo), 'checkout', '-q', 'feature'], check=True
    )
    _write(repo, 'scrapers/fbref/html_parser.py', 'def parse():\n    return 2\n')
    _commit(repo, 'branch-only html_parser edit')

    monkeypatch.setattr(mod, 'REPO_ROOT', repo)
    code = mod.main([])
    out = capsys.readouterr().out
    assert code == 0
    # focused on the branch change; master-only path never enters the diff
    assert out.strip() == 'tests/unit/scrapers/test_fbref_html_parser.py'


@pytestmark_git
def test_real_git_worktree_sees_staged_and_unstaged(tmp_path, monkeypatch, capsys):
    """--worktree must see both staged and unstaged changes."""
    repo = _init_repo(tmp_path)
    _write(repo, 'scrapers/fbref/html_parser.py', 'v1\n')
    _write(repo, 'scrapers/espn/match_feed.py', 'v1\n')
    _write(
        repo,
        'tests/unit/scrapers/test_fbref_html_parser.py',
        'def test_parse():\n    assert True\n',
    )
    _write(
        repo,
        'tests/unit/scrapers/test_match_feed.py',
        'def test_feed():\n    assert True\n',
    )
    _commit(repo, 'initial')

    _write(repo, 'scrapers/fbref/html_parser.py', 'v2 staged\n')
    subprocess.run(
        ['git', '-C', str(repo), 'add', 'scrapers/fbref/html_parser.py'],
        check=True,
    )
    _write(repo, 'scrapers/espn/match_feed.py', 'v2 unstaged\n')

    monkeypatch.setattr(mod, 'REPO_ROOT', repo)
    code = mod.main(['--worktree', '--base', 'HEAD'])
    out = capsys.readouterr().out
    assert code == 0
    assert out.splitlines() == [
        'tests/unit/scrapers/test_fbref_html_parser.py',
        'tests/unit/scrapers/test_match_feed.py',
    ]


@pytestmark_git
def test_real_git_no_renames_keeps_old_full_path(tmp_path, monkeypatch, capsys):
    """git mv out of a FULL directory must still trigger on the old path.

    diff.renames is enabled in the repo config; the selector's --no-renames
    must override it so the deleted old path (scrapers/base/x.py) reaches
    classify and escalates to FULL scraper-base.
    """
    repo = _init_repo(tmp_path)
    _write(repo, 'scrapers/base/x.py', 'v1\n')
    _commit(repo, 'initial')

    subprocess.run(
        ['git', '-C', str(repo), 'config', 'diff.renames', 'true'], check=True
    )
    (repo / 'scrapers' / 'fotmob').mkdir(parents=True, exist_ok=True)
    subprocess.run(
        ['git', '-C', str(repo), 'mv', 'scrapers/base/x.py', 'scrapers/fotmob/x.py'],
        check=True,
    )

    monkeypatch.setattr(mod, 'REPO_ROOT', repo)
    code = mod.main(['--worktree', '--base', 'HEAD'])
    out = capsys.readouterr().out
    assert code == 0
    assert out.startswith('FULL scraper-base: scrapers/base/x.py')


@pytestmark_git
def test_real_git_bad_ref_exit_three(tmp_path, monkeypatch, capsys):
    """A bad --base ref must exit 3 without stdout output."""
    repo = _init_repo(tmp_path)
    _write(repo, 'README.md', 'x\n')
    _commit(repo, 'initial')

    monkeypatch.setattr(mod, 'REPO_ROOT', repo)
    code = mod.main(['--base', 'deadbeef'])
    captured = capsys.readouterr()
    assert code == 3
    assert captured.out == ''
    assert captured.err.strip() != ''
