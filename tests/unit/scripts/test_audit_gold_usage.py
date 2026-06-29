"""
Unit tests for scripts/audit_gold_usage.py (R0.5 usage tracker).

Spec: docs/research/R0.5_usage_tracker.md

Strategy
--------
``audit_gold_usage`` is a top-level script (not a package), so we load it via
``importlib.util`` from its absolute path. All external services (Trino,
Superset, OpenMetadata) are mocked or short-circuited via the script's own
graceful-skip branches — these tests never hit the network.

What we cover
-------------
1. ``scan_dag_sql_files`` — walks a tmp_path repo with ``dags/sql/**/*.sql``:
   - matches ``iceberg.gold.<tbl>`` and bare ``gold.<tbl>``
   - per-file dedup (multiple refs in one file = 1)
   - cross-file aggregation (two files = 2)
   - strips ``-- ...`` line comments and ``/* ... */`` block comments
   - case-insensitive matching
   - walks subdirectories under ``dags/sql/``
2. ``verdict`` — truth table over the (dag_refs, charts, downstream) triplet.
3. ``superset_login`` — graceful skip when password is None (no HTTP call).
4. ``openmetadata_downstream_count`` — graceful skip when jwt_token is falsy.
5. ``list_gold_tables`` — fallback to ``dags/sql/gold/*.sql`` filenames when
   the Trino cursor raises.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Module loader — script is not a package
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parents[3]
_SCRIPT_PATH = REPO_ROOT / 'scripts' / 'audit_gold_usage.py'


def _load_module():
    spec = importlib.util.spec_from_file_location('audit_gold_usage', _SCRIPT_PATH)
    assert spec is not None and spec.loader is not None, f'cannot load {_SCRIPT_PATH}'
    mod = importlib.util.module_from_spec(spec)
    sys.modules['audit_gold_usage'] = mod
    spec.loader.exec_module(mod)
    return mod


mod = _load_module()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_repo(tmp_path: Path) -> Path:
    """Create a tmp repo skeleton with dags/sql/{silver,gold}/ subdirs."""
    (tmp_path / 'dags' / 'sql' / 'silver').mkdir(parents=True)
    (tmp_path / 'dags' / 'sql' / 'gold').mkdir(parents=True)
    return tmp_path


def _write_sql(repo: Path, rel_path: str, body: str) -> Path:
    p = repo / rel_path
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(body, encoding='utf-8')
    return p


# ---------------------------------------------------------------------------
# 1) scan_dag_sql_files
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestScanDagSqlFiles:

    def test_simple_ref(self, tmp_path):
        repo = _make_repo(tmp_path)
        _write_sql(repo, 'dags/sql/gold/q.sql', 'SELECT * FROM iceberg.gold.fct_match;')
        assert mod.scan_dag_sql_files(repo) == {'fct_match': 1}

    def test_no_iceberg_prefix(self, tmp_path):
        """Bare ``gold.<tbl>`` (no ``iceberg.`` prefix) must also match."""
        repo = _make_repo(tmp_path)
        _write_sql(repo, 'dags/sql/silver/q.sql', 'SELECT * FROM gold.dim_team')
        assert mod.scan_dag_sql_files(repo) == {'dim_team': 1}

    def test_two_files_same_table(self, tmp_path):
        repo = _make_repo(tmp_path)
        _write_sql(repo, 'dags/sql/gold/a.sql', 'SELECT 1 FROM iceberg.gold.fct_match;')
        _write_sql(repo, 'dags/sql/gold/b.sql', 'SELECT 2 FROM iceberg.gold.fct_match;')
        assert mod.scan_dag_sql_files(repo) == {'fct_match': 2}

    def test_one_file_multiple_refs(self, tmp_path):
        """One file with N refs to the same table counts as 1 (per-file dedup)."""
        repo = _make_repo(tmp_path)
        sql = (
            "SELECT a.x, b.y\n"
            "FROM iceberg.gold.fct_match a\n"
            "JOIN iceberg.gold.fct_match b ON a.id = b.id\n"
        )
        _write_sql(repo, 'dags/sql/gold/join.sql', sql)
        assert mod.scan_dag_sql_files(repo) == {'fct_match': 1}

    def test_ignores_line_comment(self, tmp_path):
        repo = _make_repo(tmp_path)
        _write_sql(
            repo,
            'dags/sql/gold/cmt.sql',
            '-- DROP TABLE iceberg.gold.fct_match;\nSELECT 1;\n',
        )
        assert mod.scan_dag_sql_files(repo) == {}

    def test_ignores_block_comment(self, tmp_path):
        repo = _make_repo(tmp_path)
        _write_sql(
            repo,
            'dags/sql/gold/blk.sql',
            '/* SELECT * FROM iceberg.gold.fct_match */ SELECT 1;\n',
        )
        assert mod.scan_dag_sql_files(repo) == {}

    def test_handles_mixed_case(self, tmp_path):
        """Regex is case-insensitive — ``Iceberg.Gold.fct_match`` is counted."""
        repo = _make_repo(tmp_path)
        _write_sql(repo, 'dags/sql/gold/lower.sql', 'SELECT * FROM iceberg.gold.fct_match')
        _write_sql(repo, 'dags/sql/gold/upper.sql', 'SELECT * FROM Iceberg.Gold.fct_match')
        # group(1) is .lower()'d in script, so both files contribute to fct_match.
        assert mod.scan_dag_sql_files(repo) == {'fct_match': 2}

    def test_walks_subdirs(self, tmp_path):
        """rglob must descend into both silver/ and gold/ subdirs."""
        repo = _make_repo(tmp_path)
        _write_sql(repo, 'dags/sql/silver/foo.sql', 'SELECT * FROM iceberg.gold.dim_team')
        _write_sql(repo, 'dags/sql/gold/bar.sql', 'SELECT * FROM iceberg.gold.fct_match')
        assert mod.scan_dag_sql_files(repo) == {'dim_team': 1, 'fct_match': 1}

    def test_string_literal_acceptable_false_positive(self, tmp_path):
        """Documented behaviour: a string literal containing ``iceberg.gold.<tbl>``
        is matched too (regex is intentionally simple). This is acceptable for
        MVP — actual DAG SQL never embeds such literals.
        """
        repo = _make_repo(tmp_path)
        _write_sql(
            repo,
            'dags/sql/gold/lit.sql',
            "SELECT 'iceberg.gold.fct_match' AS tbl;",
        )
        # Document the false positive — change this assertion if regex is
        # tightened in a future iteration.
        assert mod.scan_dag_sql_files(repo) == {'fct_match': 1}

    def test_empty_dir_returns_empty(self, tmp_path):
        repo = _make_repo(tmp_path)
        assert mod.scan_dag_sql_files(repo) == {}

    def test_missing_sql_root_returns_empty(self, tmp_path):
        # No dags/sql/ at all
        assert mod.scan_dag_sql_files(tmp_path) == {}


# ---------------------------------------------------------------------------
# 2) verdict
# ---------------------------------------------------------------------------

@pytest.mark.unit
@pytest.mark.parametrize(
    'dag_refs, charts, downstream, expected',
    [
        (3, 0, 0, 'active'),   # dag_refs only
        (0, 5, 0, 'active'),   # charts only
        (7, 5, 9, 'active'),   # all signals
        (0, 0, 1, 'stale'),    # only downstream consumers
        (0, 0, 0, 'unused'),   # nothing
    ],
    ids=['dag_only', 'charts_only', 'all', 'downstream_only', 'none'],
)
def test_verdict_truth_table(dag_refs, charts, downstream, expected):
    assert mod.verdict(dag_refs, charts, downstream) == expected


# ---------------------------------------------------------------------------
# 3) superset_login graceful skip
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_superset_login_no_password_returns_none_without_http():
    with patch.object(mod.requests, 'post') as post_mock:
        result = mod.superset_login('http://localhost:8088', 'admin', None)
    assert result is None
    post_mock.assert_not_called()


@pytest.mark.unit
def test_superset_login_empty_password_returns_none_without_http():
    """Empty string is also falsy and should short-circuit before HTTP."""
    with patch.object(mod.requests, 'post') as post_mock:
        result = mod.superset_login('http://localhost:8088', 'admin', '')
    assert result is None
    post_mock.assert_not_called()


# ---------------------------------------------------------------------------
# 4) openmetadata_downstream_count graceful skip
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_openmetadata_downstream_count_no_token_returns_zero_without_http():
    with patch.object(mod.requests, 'get') as get_mock:
        result = mod.openmetadata_downstream_count(
            'http://localhost:8585', None, 'trino_iceberg.iceberg.gold.fct_match'
        )
    assert result == 0
    get_mock.assert_not_called()


@pytest.mark.unit
def test_openmetadata_downstream_count_empty_token_returns_zero_without_http():
    with patch.object(mod.requests, 'get') as get_mock:
        result = mod.openmetadata_downstream_count(
            'http://localhost:8585', '', 'trino_iceberg.iceberg.gold.fct_match'
        )
    assert result == 0
    get_mock.assert_not_called()


# ---------------------------------------------------------------------------
# 5) list_gold_tables — fallback to filenames on Trino error
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestListGoldTablesFallback:

    def _fake_repo_with_gold_sql(self, tmp_path: Path, names: list[str]) -> Path:
        gold = tmp_path / 'dags' / 'sql' / 'gold'
        gold.mkdir(parents=True)
        for n in names:
            (gold / f'{n}.sql').write_text('-- stub\n', encoding='utf-8')
        return tmp_path

    def test_fallback_when_cursor_raises(self, tmp_path, monkeypatch):
        """Trino SHOW TABLES raises -> fall back to filenames in dags/sql/gold/.

        The fallback inside the script uses ``Path(__file__).resolve().parent.parent``
        which points to the real repo's ``scripts/`` parent. To isolate the
        fallback to a tmp tree, we monkeypatch ``mod.Path`` so that the
        ``__file__`` path used inside ``list_gold_tables`` resolves to our
        tmp_path's ``scripts/audit_gold_usage.py``.

        Simpler approach: monkeypatch ``mod.__file__`` so ``Path(__file__)``
        inside the function resolves to a tmp scripts/ dir.
        """
        # Build tmp repo: tmp_path/scripts/audit_gold_usage.py + dags/sql/gold/*.sql
        scripts_dir = tmp_path / 'scripts'
        scripts_dir.mkdir()
        fake_script = scripts_dir / 'audit_gold_usage.py'
        fake_script.write_text('# stub\n', encoding='utf-8')
        self._fake_repo_with_gold_sql(tmp_path, ['fct_match', 'dim_team', 'feat_team_form'])

        monkeypatch.setattr(mod, '__file__', str(fake_script))

        # Mock trino connection whose cursor.execute raises
        conn = MagicMock()
        cur = MagicMock()
        cur.execute.side_effect = Exception('schema not found')
        conn.cursor.return_value = cur

        tables, method = mod.list_gold_tables(conn)
        assert method == 'filenames_fallback'
        assert tables == ['dim_team', 'fct_match', 'feat_team_form']

    def test_fallback_when_show_tables_returns_empty(self, tmp_path, monkeypatch):
        """SHOW TABLES returns 0 rows -> fall back to filenames."""
        scripts_dir = tmp_path / 'scripts'
        scripts_dir.mkdir()
        fake_script = scripts_dir / 'audit_gold_usage.py'
        fake_script.write_text('# stub\n', encoding='utf-8')
        self._fake_repo_with_gold_sql(tmp_path, ['only_one'])

        monkeypatch.setattr(mod, '__file__', str(fake_script))

        conn = MagicMock()
        cur = MagicMock()
        cur.execute.return_value = None
        cur.fetchall.return_value = []  # zero rows
        conn.cursor.return_value = cur

        tables, method = mod.list_gold_tables(conn)
        assert method == 'filenames_fallback'
        assert tables == ['only_one']

    def test_uses_show_tables_when_available(self):
        """Sanity-check the happy path: cursor returns rows -> method='shows_tables'."""
        conn = MagicMock()
        cur = MagicMock()
        cur.execute.return_value = None
        cur.fetchall.return_value = [('fct_match',), ('dim_team',), ('fct_match',)]  # dup ok
        conn.cursor.return_value = cur

        tables, method = mod.list_gold_tables(conn)
        assert method == 'shows_tables'
        assert tables == ['dim_team', 'fct_match']  # sorted + deduped

    def test_no_conn_falls_back(self, tmp_path, monkeypatch):
        """trino_conn=None must still try the filename fallback."""
        scripts_dir = tmp_path / 'scripts'
        scripts_dir.mkdir()
        fake_script = scripts_dir / 'audit_gold_usage.py'
        fake_script.write_text('# stub\n', encoding='utf-8')
        self._fake_repo_with_gold_sql(tmp_path, ['fct_match'])

        monkeypatch.setattr(mod, '__file__', str(fake_script))

        tables, method = mod.list_gold_tables(None)
        assert method == 'filenames_fallback'
        assert tables == ['fct_match']
