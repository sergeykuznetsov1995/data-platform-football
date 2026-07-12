#!/usr/bin/env python3
"""Inventory and guard runtime consumers of legacy Transfermarkt relations.

Legacy physical tables remain available during the rollback retention window,
but only source writers/bootstrap-parity code and explicitly versioned rollback
SQL may reference them. Canonical/native-v2 consumers must use native semantic
relations. The audit is static and read-only; it performs no network or Trino
I/O and exits non-zero for every unclassified legacy reader.
"""

from __future__ import annotations

import argparse
import ast
import io
import json
import re
import tokenize
from functools import lru_cache
from pathlib import Path
from typing import Iterable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RUNTIME_ROOTS = ('dags', 'scripts', 'scrapers', 'configs/superset')
RUNTIME_SUFFIXES = ('.py', '.sql', '.j2')

LEGACY_TABLES = (
    'transfermarkt_players',
    'transfermarkt_market_value_history',
    'transfermarkt_transfers',
    'transfermarkt_coaches',
)
VERSIONED_SILVER_TABLES = LEGACY_TABLES + (
    'xref_player',
    'xref_team',
    'xref_manager',
)
VERSIONED_GOLD_TABLES = (
    'dim_player',
    'dim_team',
    'dim_player_attributes',
    'dim_manager',
    'fct_transfer',
    'fct_player_market_value',
    'fct_team_season_stats',
    'transfermarkt_team_season_market_value',
)
LEGACY_RELATION_RE = re.compile(
    r'(?<![A-Za-z0-9_])(?:iceberg\.)?(?:'
    r'(?P<bronze_layer>bronze)\.'
    r'(?P<bronze_table>' + '|'.join(map(re.escape, LEGACY_TABLES)) + r')'
    r'|(?P<silver_layer>silver)\.'
    r'(?P<silver_table>'
    + '|'.join(map(re.escape, VERSIONED_SILVER_TABLES))
    + r')_legacy'
    r'|(?P<gold_layer>gold)\.'
    r'(?P<gold_table>'
    + '|'.join(map(re.escape, VERSIONED_GOLD_TABLES))
    + r')_legacy(?P<gold_legacy_suffix>_source)?'
    r')(?![A-Za-z0-9_])',
    re.IGNORECASE,
)

# These files either populate/verify the compatibility tables or bootstrap
# native rows from them. Their legacy reads are required until cleanup.
LEGACY_WRITER_ALLOWLIST = frozenset({
    'dags/dag_ingest_transfermarkt.py',
    'dags/dag_transform_transfermarkt_silver.py',
    'dags/scripts/run_transfermarkt_scope_cycle.py',
    'dags/scripts/run_transfermarkt_scraper.py',
    'dags/utils/transfermarkt_native_v2.py',
    'scrapers/transfermarkt/scraper.py',
    'scripts/transfermarkt_native_v2.py',
})

# Physical legacy transforms are retained as the rollback implementation. They
# are never the native-v2 branch and may be removed only by retention-gated
# cleanup. Keeping the list exact prevents a new SQL file being silently
# blessed merely because it lives in dags/sql.
LEGACY_ROLLBACK_ALLOWLIST = frozenset({
    'dags/sql/silver/transfermarkt_players.sql',
    'dags/sql/silver/transfermarkt_market_value_history.sql',
    'dags/sql/silver/transfermarkt_transfers.sql.j2',
    'dags/sql/silver/transfermarkt_coaches.sql.j2',
    'dags/sql/gold/dim_manager.sql',
    'dags/sql/gold/fct_player_market_value.sql',
    'dags/sql/gold/fct_transfer.sql',
})

# State-selected serving/runtime consumers. These paths may read unversioned
# canonical relations or pre-Silver native Bronze contracts, but never a
# versioned physical ``_legacy`` or ``_v2``/``_v2_a``/``_v2_b`` serving table.
STABLE_RUNTIME_CONSUMERS = frozenset({
    'configs/superset/dashboards/league_overview.py',
    'dags/sql/silver/xref_manager.sql.j2',
    'dags/sql/silver/xref_team.sql.j2',
    'dags/sql/gold/dim_player.sql.j2',
    'dags/sql/gold/dim_player_attributes.sql',
    'dags/sql/gold/fct_team_season_stats.sql.j2',
    'dags/utils/xref_dq.py',
    'dags/utils/xref_player_resolver.py',
})

# Physical shadow builders/readiness inputs are the only consumer SQL allowed
# to mention ``_v2`` relations. They are still forbidden from reading a
# physical legacy branch.
SHADOW_V2_CONSUMERS = frozenset({
    'dags/sql/gold/dim_manager_v2.sql',
    'dags/sql/gold/fct_player_market_value_v2.sql',
    'dags/sql/gold/fct_transfer_v2.sql',
    'dags/sql/gold/transfermarkt_team_season_market_value_v2.sql',
    'dags/sql/silver/transfermarkt_coach_profiles_v2.sql',
    'dags/sql/silver/transfermarkt_coach_stints_v2.sql',
    'dags/sql/silver/transfermarkt_market_value_points_v2.sql',
    'dags/sql/silver/transfermarkt_player_attribute_observations_v2.sql',
    'dags/sql/silver/transfermarkt_player_contract_observations_v2.sql',
    'dags/sql/silver/transfermarkt_player_attributes_v2.sql',
    'dags/sql/silver/transfermarkt_player_team_season_assignment_v2.sql',
    'dags/sql/silver/transfermarkt_player_xref_global_v2.sql',
    'dags/sql/silver/transfermarkt_squad_memberships_v2.sql',
    'dags/sql/silver/transfermarkt_transfer_events_v2.sql.j2',
})
CANONICAL_V2_CONSUMERS = STABLE_RUNTIME_CONSUMERS | SHADOW_V2_CONSUMERS

PHYSICAL_V2_RELATION_RE = re.compile(
    r'(?<![A-Za-z0-9_])(?:iceberg\.)?(?:silver|gold)\.'
    r'[a-zA-Z0-9_]+_v2(?:_[ab])?(?![A-Za-z0-9_])',
    re.IGNORECASE,
)
PHYSICAL_V2_CONTROL_ALLOWLIST = frozenset({
    'dags/dag_transform_transfermarkt_silver.py',
    'dags/utils/transfermarkt_native_v2.py',
    'dags/utils/transfermarkt_registry_publish.py',
    'dags/utils/transfermarkt_scope_planner.py',
    'scripts/transfermarkt_native_v2.py',
})

CANONICAL_RELATION_RE = re.compile(
    r'(?<![A-Za-z0-9_])(?:iceberg\.)?(?:'
    r'(?P<canonical_silver_layer>silver)\.'
    r'(?P<canonical_silver_table>'
    + '|'.join(map(re.escape, VERSIONED_SILVER_TABLES))
    + r')'
    r'|(?P<canonical_gold_layer>gold)\.'
    r'(?P<canonical_gold_table>'
    + '|'.join(map(re.escape, VERSIONED_GOLD_TABLES))
    + r')'
    r')\b',
    re.IGNORECASE,
)


def _mask(lines: list[str], start: tuple[int, int], end: tuple[int, int]) -> None:
    """Replace a source span with spaces while preserving line positions."""
    start_line, start_col = start
    end_line, end_col = end
    for line_no in range(start_line, end_line + 1):
        index = line_no - 1
        if index >= len(lines):
            break
        left = start_col if line_no == start_line else 0
        right = end_col if line_no == end_line else len(lines[index])
        lines[index] = (
            lines[index][:left]
            + ' ' * max(0, right - left)
            + lines[index][right:]
        )


def _python_runtime_text(text: str) -> str:
    """Remove Python comments and true docstrings, retaining SQL strings."""
    lines = text.splitlines(keepends=True)
    try:
        tree = ast.parse(text)
    except SyntaxError:
        tree = None
    if tree is not None:
        for node in ast.walk(tree):
            body = getattr(node, 'body', None)
            if not isinstance(body, list) or not body:
                continue
            first = body[0]
            if (
                isinstance(first, ast.Expr)
                and isinstance(first.value, ast.Constant)
                and isinstance(first.value.value, str)
                and hasattr(first, 'end_lineno')
            ):
                _mask(
                    lines,
                    (first.lineno, first.col_offset),
                    (first.end_lineno, first.end_col_offset),
                )
    try:
        tokens = tokenize.generate_tokens(io.StringIO(''.join(lines)).readline)
        comments = [token for token in tokens if token.type == tokenize.COMMENT]
    except (IndentationError, tokenize.TokenError):
        comments = []
    for token in comments:
        _mask(lines, token.start, token.end)
    return ''.join(lines)


def _sql_runtime_text(text: str) -> str:
    """Remove SQL/Jinja comments so documentation does not enter inventory."""
    without_blocks = re.sub(r'/\*.*?\*/', ' ', text, flags=re.DOTALL)
    return '\n'.join(line.split('--', 1)[0] for line in without_blocks.splitlines())


@lru_cache(maxsize=None)
def _runtime_text(path: Path) -> str:
    text = path.read_text(encoding='utf-8')
    return _python_runtime_text(text) if path.suffix == '.py' else _sql_runtime_text(text)


def iter_runtime_files(root: Path) -> Iterable[Path]:
    for relative_root in RUNTIME_ROOTS:
        base = root / relative_root
        if not base.exists():
            continue
        for path in base.rglob('*'):
            if path.is_file() and path.name.endswith(RUNTIME_SUFFIXES):
                yield path


def scan_legacy_consumers(root: Path = PROJECT_ROOT) -> dict[str, list[str]]:
    """Return ``relative_path -> referenced legacy physical relations``."""
    findings: dict[str, list[str]] = {}
    for path in iter_runtime_files(root):
        matches = set()
        for match in LEGACY_RELATION_RE.finditer(_runtime_text(path)):
            if match.group('bronze_table'):
                matches.add(
                    f"bronze.{match.group('bronze_table').lower()}"
                )
            elif match.group('silver_table'):
                matches.add(
                    f"silver.{match.group('silver_table').lower()}_legacy"
                )
            else:
                matches.add(
                    f"gold.{match.group('gold_table').lower()}_legacy"
                    f"{(match.group('gold_legacy_suffix') or '').lower()}"
                )
        if matches:
            findings[path.relative_to(root).as_posix()] = sorted(matches)
    return dict(sorted(findings.items()))


def scan_physical_v2_consumers(
    root: Path = PROJECT_ROOT,
) -> dict[str, list[str]]:
    """Return every runtime file that directly reads a physical v2 relation."""
    findings: dict[str, list[str]] = {}
    for path in iter_runtime_files(root):
        matches = sorted({
            match.group(0).lower().removeprefix('iceberg.')
            for match in PHYSICAL_V2_RELATION_RE.finditer(_runtime_text(path))
        })
        if matches:
            findings[path.relative_to(root).as_posix()] = matches
    return dict(sorted(findings.items()))


def scan_canonical_consumers(root: Path = PROJECT_ROOT) -> dict[str, list[str]]:
    """Inventory unversioned state-selected Silver/Gold reader relations."""
    findings: dict[str, list[str]] = {}
    for path in iter_runtime_files(root):
        matches = set()
        for match in CANONICAL_RELATION_RE.finditer(_runtime_text(path)):
            if match.group('canonical_silver_table'):
                matches.add(
                    f"silver.{match.group('canonical_silver_table').lower()}"
                )
            else:
                matches.add(
                    f"gold.{match.group('canonical_gold_table').lower()}"
                )
        if matches:
            findings[path.relative_to(root).as_posix()] = sorted(matches)
    return dict(sorted(findings.items()))


def audit_consumers(root: Path = PROJECT_ROOT) -> dict:
    findings = scan_legacy_consumers(root)
    physical_v2 = scan_physical_v2_consumers(root)
    canonical = scan_canonical_consumers(root)
    allowed = LEGACY_WRITER_ALLOWLIST | LEGACY_ROLLBACK_ALLOWLIST
    violations = []
    for path, relations in findings.items():
        if path in CANONICAL_V2_CONSUMERS:
            reason = 'canonical_v2_consumer_reads_legacy'
        elif path not in allowed:
            reason = 'unclassified_legacy_consumer'
        else:
            continue
        violations.append({
            'path': path,
            'relations': relations,
            'reason': reason,
        })

    physical_v2_allowlist = (
        SHADOW_V2_CONSUMERS | PHYSICAL_V2_CONTROL_ALLOWLIST
    )
    for relative_path, versioned in physical_v2.items():
        if relative_path not in physical_v2_allowlist:
            reason = (
                'stable_runtime_consumer_reads_physical_v2'
                if relative_path in STABLE_RUNTIME_CONSUMERS
                else 'unclassified_physical_v2_consumer'
            )
            violations.append({
                'path': relative_path,
                'relations': versioned,
                'reason': reason,
            })

    return {
        'passed': not violations,
        'legacy_consumers': findings,
        'physical_v2_consumers': physical_v2,
        'canonical_consumers': canonical,
        'writer_allowlist': sorted(LEGACY_WRITER_ALLOWLIST),
        'rollback_allowlist': sorted(LEGACY_ROLLBACK_ALLOWLIST),
        'stable_runtime_consumers': sorted(STABLE_RUNTIME_CONSUMERS),
        'shadow_v2_consumers': sorted(SHADOW_V2_CONSUMERS),
        'physical_v2_control_allowlist': sorted(
            PHYSICAL_V2_CONTROL_ALLOWLIST
        ),
        'violations': violations,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=PROJECT_ROOT)
    args = parser.parse_args(argv)
    report = audit_consumers(args.root.resolve())
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
