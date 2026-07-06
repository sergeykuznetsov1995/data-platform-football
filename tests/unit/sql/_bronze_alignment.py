"""Shared helper: extract Bronze column references from Silver SQL via sqlglot.

Used by:
  - ``tests/unit/sql/test_silver_bronze_column_alignment.py`` to assert that
    every referenced Bronze column exists in the schema snapshot.
  - ``scripts/snapshot_bronze_schemas.py`` to enumerate which Bronze tables
    are touched by Silver SQL and therefore need a fixture entry.

Approach
--------
Walk the parsed SQL tree scope-by-scope (sqlglot.optimizer.scope). For each
scope, record which alias maps to a ``iceberg.bronze.<table>`` source. A
"passthrough" CTE — one whose SELECT uses ``*`` / ``alias.*`` over a single
bronze source — propagates its bronze identity to the outer scope, with any
SELECT-introduced aliases (e.g. ``ROW_NUMBER() AS rn``) tracked so they are
not mistaken for bronze columns when later filtered in WHERE / projected by
the outer SELECT.

The walker deliberately does NOT validate columns referenced from non-bronze
sources (silver / gold) — those would require silver / gold schema snapshots
which are out of scope for issue #71.
"""
from __future__ import annotations

import importlib.util
import os
from pathlib import Path
from typing import Dict, Set, Tuple

import sqlglot
from sqlglot import exp
from sqlglot.optimizer.scope import Scope, traverse_scope

def _find_repo_root() -> Path:
    """Locate the data platform repo root.

    Resolution order:
      1. ``DATA_PLATFORM_ROOT`` env var (explicit override; useful when this
         helper is copied outside its normal tree, e.g. into a container's
         ``/tmp``).
      2. Walk up from this file until a directory containing
         ``dags/sql/silver`` is found.
      3. ``/opt/airflow`` (canonical container path).
    """
    env = os.environ.get("DATA_PLATFORM_ROOT")
    if env:
        return Path(env)
    here = Path(__file__).resolve()
    for parent in [here, *here.parents]:
        if (parent / "dags" / "sql" / "silver").is_dir():
            return parent
    return Path("/opt/airflow")


_REPO_ROOT = _find_repo_root()
_MEDALLION_CONFIG_PATH = _REPO_ROOT / "dags" / "utils" / "medallion_config.py"
_TEMPLATE_DIR = _REPO_ROOT / "dags" / "sql" / "silver"


def _load_medallion_config():
    """Import dags/utils/medallion_config.py without touching dags/utils/__init__.py.

    The package ``dags.utils`` imports ``utils.config`` at import-time which
    only resolves inside the Airflow container's PYTHONPATH. We bypass that
    by loading medallion_config as a standalone module. ``MEDALLION_CONFIG_DIR``
    is set BEFORE import so the module's load-time constant picks it up.
    """
    os.environ.setdefault(
        "MEDALLION_CONFIG_DIR", str(_REPO_ROOT / "configs" / "medallion")
    )
    spec = importlib.util.spec_from_file_location(
        "_medallion_config_test_shim", str(_MEDALLION_CONFIG_PATH)
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def load_silver_sql(sql_path: Path) -> str:
    """Read a Silver SQL file, rendering Jinja-style ``.sql.j2`` templates."""
    if sql_path.suffix == ".j2":
        mc = _load_medallion_config()
        # Pass every known alias-VALUES context; render_sql_template ignores
        # unused keys, so one call covers team / referee / manager templates alike.
        clubelo_leagues = ", ".join(
            f"'{mc._escape_sql_string(lg)}'"
            for lg in mc.get_in_scope_competitions()
        )
        return mc.render_sql_template(
            str(sql_path),
            team_aliases_values_sql=mc.get_team_alias_sql_values(),
            referee_aliases_values_sql=mc.get_referee_alias_sql_values(),
            manager_aliases_values_sql=mc.get_manager_alias_sql_values(),
            clubelo_in_scope_leagues=clubelo_leagues,
        )
    return sql_path.read_text()


def list_silver_sql_paths() -> list[Path]:
    """All Silver SQL files (``.sql`` + ``.sql.j2``), sorted."""
    return sorted(
        list(_TEMPLATE_DIR.glob("*.sql")) + list(_TEMPLATE_DIR.glob("*.sql.j2"))
    )


def _select_has_star(expr: exp.Expression) -> bool:
    if not isinstance(expr, exp.Select):
        return False
    for e in expr.expressions:
        if isinstance(e, exp.Star):
            return True
        if isinstance(e, exp.Column) and isinstance(e.this, exp.Star):
            return True
    return False


def _select_introduced_aliases(expr: exp.Expression) -> Set[str]:
    if not isinstance(expr, exp.Select):
        return set()
    return {p.alias for p in expr.expressions if isinstance(p, exp.Alias)}


def collect_bronze_refs(sql: str) -> Dict[str, Set[str]]:
    """Return ``{bronze_table_name: {referenced_column_names}}`` for one SQL.

    Column refs are collected when:
      * Qualified against an alias mapped (directly or via passthrough CTE)
        to ``iceberg.bronze.<table>``.
      * Unqualified inside a scope whose single FROM/JOIN source is bronze.

    Introduced aliases (e.g. ``ROW_NUMBER() OVER (...) AS rn``) propagated
    through passthrough CTEs are excluded — they are not bronze columns.
    """
    tree = sqlglot.parse_one(sql, read="trino")

    # Per-scope: {alias_in_scope: (bronze_table_name, introduced_aliases_set)}
    scope_meta: Dict[int, Dict[str, Tuple[str, Set[str]]]] = {}
    refs: Dict[str, Set[str]] = {}

    for scope in traverse_scope(tree):  # innermost first
        # Only consider sources EXPLICITLY referenced in this scope's FROM/JOIN;
        # ignore CTE visibility (sqlglot lists every visible CTE in scope.sources).
        actual_aliases: Set[str] = set()
        # Columns introduced by UNNEST(...) AS t(v) — exclude them from bronze
        # attribution (they originate from JSON arrays, not the bronze table).
        unnest_introduced: Set[str] = set()
        for t in scope.tables:
            actual_aliases.add(t.alias_or_name)
        for d in scope.derived_tables:
            actual_aliases.add(d.alias if d.alias else "")
        for unnest in scope.expression.find_all(exp.Unnest):
            alias_node = unnest.args.get("alias")
            if alias_node is None:
                continue
            if alias_node.name:
                actual_aliases.add(alias_node.name)
            for col in getattr(alias_node, "columns", []):
                unnest_introduced.add(col.name)

        scope_aliases: Dict[str, Tuple[str, Set[str]]] = {}
        for alias in actual_aliases:
            source = scope.sources.get(alias)
            if source is None:
                continue
            if isinstance(source, exp.Table):
                if source.catalog == "iceberg" and source.db == "bronze":
                    scope_aliases[alias] = (source.name, set())
            elif isinstance(source, Scope):
                child_meta = scope_meta.get(id(source), {})
                child_bronze = [
                    (a, v) for a, v in child_meta.items() if v[0] is not None
                ]
                if len(child_bronze) == 1 and _select_has_star(source.expression):
                    _, (bt, inner_introduced) = child_bronze[0]
                    scope_aliases[alias] = (
                        bt,
                        inner_introduced | _select_introduced_aliases(source.expression),
                    )

        scope_meta[id(scope)] = scope_aliases

        bronze_in_scope = {a: v for a, v in scope_aliases.items() if v[0] is not None}
        non_bronze_count = len(actual_aliases) - len(bronze_in_scope)

        for col in scope.columns:
            ctab = col.table
            if ctab and ctab in bronze_in_scope:
                bt, intro = bronze_in_scope[ctab]
                if col.name not in intro and col.name not in unnest_introduced:
                    refs.setdefault(bt, set()).add(col.name)
            elif not ctab and len(bronze_in_scope) == 1 and non_bronze_count == 0:
                bt, intro = next(iter(bronze_in_scope.values()))
                if col.name not in intro and col.name not in unnest_introduced:
                    refs.setdefault(bt, set()).add(col.name)

    return refs


def collect_all_bronze_tables() -> Set[str]:
    """Union of bronze tables referenced across all Silver SQL files."""
    out: Set[str] = set()
    for path in list_silver_sql_paths():
        try:
            sql = load_silver_sql(path)
            out.update(collect_bronze_refs(sql).keys())
        except Exception as e:  # pragma: no cover - defensive
            raise RuntimeError(f"Failed to parse {path}: {e}") from e
    return out
