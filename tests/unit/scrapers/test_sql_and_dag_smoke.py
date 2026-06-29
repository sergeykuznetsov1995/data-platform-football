"""
Smoke tests for SQL files and DAG modules.

These do NOT require a live Trino / Airflow runtime — they protect against:
  * Accidental revert of the pseudo-match_id fix in fbref_match_enriched.sql.
  * Syntax errors in DAG modules (caught at import time).

DAG import strategy: importing a DAG module triggers Airflow's `DAG` constructor
which needs Airflow installed. Where Airflow isn't on the host, we fall back to
``ast.parse`` to at least catch SyntaxErrors. That keeps the smoke check useful
without forcing every contributor to install the entire Airflow stack.
"""

import ast
import importlib.util
import re
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_SILVER = PROJECT_ROOT / 'dags' / 'sql' / 'silver'
DAGS_DIR = PROJECT_ROOT / 'dags'


class TestSilverMatchEnrichedSQL:
    """fbref_match_enriched.sql must keep generating pseudo-match_ids for
    future fixtures. Without this, future matches silently drop out of
    Silver/Gold."""

    @pytest.fixture
    def sql_text(self) -> str:
        path = SQL_SILVER / 'fbref_match_enriched.sql'
        assert path.exists(), f"missing SQL file: {path}"
        return path.read_text(encoding='utf-8')

    def test_pseudo_match_id_pattern_present(self, sql_text):
        """COALESCE(REGEXP_EXTRACT(...), 'fut_' || ... XXHASH64(...)) — exact
        shape of the future-fixture fix. Tolerates whitespace/newlines but
        requires all three building blocks in order."""
        # Normalise whitespace so the regex doesn't break on formatting tweaks
        compact = re.sub(r'\s+', ' ', sql_text)
        pattern = re.compile(
            r"COALESCE\s*\(.*?REGEXP_EXTRACT\s*\(\s*match_url.*?'fut_'\s*\|\|.*?XXHASH64",
            re.IGNORECASE,
        )
        assert pattern.search(compact), (
            "pseudo-match_id COALESCE+REGEXP_EXTRACT+'fut_'+XXHASH64 fix "
            "has been removed from fbref_match_enriched.sql — future fixtures "
            "will silently drop out of Silver. See dags/sql/silver/fbref_match_enriched.sql."
        )

    def test_pseudo_match_id_aliased_as_match_id(self, sql_text):
        """The COALESCE expression must produce a column called match_id."""
        compact = re.sub(r'\s+', ' ', sql_text)
        # ... XXHASH64(...) ))) AS match_id
        assert re.search(r'XXHASH64.*?\)\s*\)\s*AS\s+match_id', compact, re.IGNORECASE), (
            "pseudo-match_id expression must be aliased AS match_id"
        )


def _has_airflow() -> bool:
    """True iff a real Airflow install (with airflow.DAG) is importable.

    A namespace-package stub (just `airflow/` without __init__.py) makes
    find_spec succeed but airflow.DAG raises ImportError. Skip in that case
    — DAG syntax is still covered by the ast.parse test."""
    try:
        from airflow import DAG  # noqa: F401
        return True
    except Exception:
        return False


# DAG modules covered by the smoke test. Adding a new DAG? Add it here too.
SMOKE_DAG_FILES = [
    'dag_transform_fbref_silver.py',
    'dag_transform_fbref_gold.py',
]


@pytest.mark.parametrize('dag_filename', SMOKE_DAG_FILES)
class TestDagSmoke:
    """Cheap protection against syntax / import regressions in DAG modules."""

    def test_dag_file_exists(self, dag_filename):
        assert (DAGS_DIR / dag_filename).exists(), f"missing {dag_filename}"

    def test_dag_file_parses_as_python(self, dag_filename):
        """Always run: pure ast.parse — catches SyntaxError without needing
        any heavy deps (airflow / scrapers)."""
        path = DAGS_DIR / dag_filename
        try:
            ast.parse(path.read_text(encoding='utf-8'), filename=str(path))
        except SyntaxError as e:
            pytest.fail(f"{dag_filename} has a SyntaxError: {e}")

    @pytest.mark.skipif(not _has_airflow(), reason="airflow not installed on host")
    def test_dag_imports_with_airflow(self, dag_filename):
        """Full import — only when Airflow is available. Skips on bare host."""
        # Make sure dags/ is on sys.path so 'utils.*' resolves the same way
        # the Airflow scheduler resolves it.
        if str(DAGS_DIR) not in sys.path:
            sys.path.insert(0, str(DAGS_DIR))

        spec = importlib.util.spec_from_file_location(
            dag_filename[:-3], DAGS_DIR / dag_filename,
        )
        module = importlib.util.module_from_spec(spec)
        try:
            spec.loader.exec_module(module)
        except Exception as e:
            pytest.fail(f"{dag_filename} failed to import: {e}")
