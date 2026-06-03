"""
Dim loaders — config-driven Gold dim renderers
==============================================

E2 (master-data dims) helper. Two of the five master-data dims do NOT come
from a Bronze/Silver source — they are projections of YAML / Python config:

  * ``dim_competition``   <- ``scrapers/sources/leagues.yaml:metadata``
  * ``dim_season``        <- a fixed 5-season window anchored at
                            ``utils.config.CURRENT_SEASON``

Both are stored as Jinja-style templates (``dags/sql/gold/dim_*.sql.j2``)
with a single ``{{ rows }}`` placeholder for the VALUES tuples. The renderer
substitutes the placeholder with one quoted SQL row per config entry, writes
the rendered SQL to a tempfile, and hands it off to the existing
``run_gold_transform`` CTAS engine — same DROP+CTAS+partitioning contract as
every other Gold task.

This module is intentionally thin: the renderers are pure functions, the
Airflow-callable shim ``run_inline_ctas`` only adds tempfile lifecycle +
delegation. No Trino client lives here.

Why a separate module (not a method on gold_tasks.py)?
  * gold_tasks.py reads from ``iceberg.silver.*`` only (universal contract).
  * dim_loaders renders config -> VALUES literals -> CTAS, a different shape.
  * Keeping rendering isolated also makes it trivially unit-testable without
    pulling in Trino / Airflow.
"""

from __future__ import annotations

import os
import re
import tempfile
import unicodedata
from datetime import date
from pathlib import Path
from typing import Callable, List, Optional

import yaml

# ---------------------------------------------------------------------------
# Config sources
# ---------------------------------------------------------------------------
# leagues.yaml lives in scrapers/sources/. In the Airflow container the
# scrapers package is mounted at /opt/airflow/scrapers. On the host the
# project root is wherever this repo is checked out — overridable via the
# ``LEAGUES_YAML`` env var so unit tests can point it elsewhere.
LEAGUES_YAML = Path(
    os.environ.get('LEAGUES_YAML', '/opt/airflow/scrapers/sources/leagues.yaml')
)

# Number of historical seasons (incl. current) to materialize in dim_season.
# Must match the row-count contract in gold_tasks.validate_gold_row_counts:
#   CHECK.row_count('gold.dim_season', min_rows=5, max_rows=5)
# Bump here AND there together; otherwise the DQ check will fail.
N_SEASONS_WINDOW = 5


def _slug(s: str) -> str:
    """Strip diacritics + lowercase + non-alnum -> '_' + collapse runs + strip edges.

    Matches the convention used by xref orphan IDs ('ss_<slug>') so
    canonical_id formatting stays consistent across Gold tables.

    NFD decomposes accented chars ("ü" -> "u" + combining mark); we drop the
    combining marks (Unicode category 'Mn') before the non-alnum collapse, so a
    competition name with/without accents maps to one slug (issue #215). This is
    the Python mirror of the NORMALIZE(NFD) + `\\p{Mn}+` idiom in the SQL
    normalizers (xref_team.sql.j2, xref_referee.sql).
    """
    s = ''.join(c for c in unicodedata.normalize('NFD', s)
                if unicodedata.category(c) != 'Mn')
    return re.sub(r'[^a-z0-9]+', '_', s.lower()).strip('_')


_ROWS_PLACEHOLDER_RE = re.compile(r'^[ \t]*\{\{ rows \}\}[ \t]*$', re.MULTILINE)


def _substitute_rows(template: str, body: str) -> str:
    """Substitute the ACTIVE ``{{ rows }}`` placeholder inside the VALUES
    block, leaving any references inside ``-- comments`` untouched.

    The .sql.j2 templates document the placeholder in their header comment
    (``-- The {{ rows }} placeholder is substituted ...``); a naive
    ``str.replace('{{ rows }}', ...)`` would inject the multi-line VALUES
    body INTO that comment line, breaking SQL because the second and later
    rows would no longer be inside ``--`` -> Trino parser error.

    The match is anchored to a line that is whitespace-only-then-placeholder
    (i.e. the standalone form used in the VALUES block), guaranteeing we only
    rewrite the real CTAS payload site.
    """
    return _ROWS_PLACEHOLDER_RE.sub(lambda _: '    ' + body, template, count=1)


def _seasons_window(current_season: int, n: int = N_SEASONS_WINDOW) -> List[int]:
    """Return the last `n` season-start years ending at (and including)
    ``current_season``. e.g. current=2025, n=5 -> [2021, 2022, 2023, 2024, 2025].

    Centralised here so unit tests can monkey-patch the seed without touching
    utils.config.
    """
    return list(range(current_season - n + 1, current_season + 1))


# ---------------------------------------------------------------------------
# Renderers
# ---------------------------------------------------------------------------

def render_dim_competition_sql(template_path: str, out_path: str) -> str:
    """Render ``dim_competition.sql.j2`` to ``out_path``.

    Reads ``leagues.yaml:metadata``, emits one VALUES row per league with
    the columns declared in the template's ``AS t(...)`` clause:
        competition_id, competition_name, country, competition_level,
        n_teams, matches_per_season, fbref_id, whoscored_id, sofascore_id,
        espn_id, competition_canonical, competition_source, competition_version

    The template's ``{{ rows }}`` literal is replaced with the joined tuples.
    Returns ``out_path`` for the convenience of pipeline glue code.
    """
    meta = yaml.safe_load(LEAGUES_YAML.read_text())['metadata']

    rows: List[str] = []
    for name, m in meta.items():
        cid = _slug(name)
        rows.append(
            f"('{cid}', '{name}', '{m['country']}', {m['level']}, "
            f"{m['teams']}, {m['matches_per_season']}, "
            f"{m['fbref_id']}, {m['whoscored_id']}, {m['sofascore_id']}, "
            f"'{m['espn_id']}', '{name}', 'config', 'v1')"
        )

    body = ',\n    '.join(rows)
    template = Path(template_path).read_text()
    rendered = _substitute_rows(template, body)
    Path(out_path).write_text(rendered)
    return out_path


def render_dim_season_sql(template_path: str, out_path: str) -> str:
    """Render ``dim_season.sql.j2`` to ``out_path``.

    Builds a 5-season window ending at ``CURRENT_SEASON`` (e.g. 2025 today
    -> 2021..2025). For each year ``y`` emits a row keyed at ``f"{y}-{(y+1)%100:02d}"``
    with August 1 / July 31 anchors and ``is_current`` marking the season
    that contains today's date.

    Columns must match the template's ``AS t(...)`` clause:
        season_id, season_start_year, season_end_year, season_label,
        valid_from, valid_to, is_current,
        season_canonical, season_source, season_version
    """
    # Lazy import: utils.config pulls in datetime + a tiny bit of state but
    # avoids a hard cycle at module-import time when running under pytest
    # (where /opt/airflow may not be on sys.path).
    from utils.config import CURRENT_SEASON

    today = date.today()
    rows: List[str] = []

    for y in _seasons_window(CURRENT_SEASON):
        sid = f"{y}-{str(y + 1)[-2:]}"
        label = f"{y}/{str(y + 1)[-2:]}"
        valid_from, valid_to = date(y, 8, 1), date(y + 1, 7, 31)
        is_current = 'true' if valid_from <= today <= valid_to else 'false'
        rows.append(
            f"('{sid}', {y}, {y + 1}, '{label}', "
            f"DATE '{valid_from}', DATE '{valid_to}', {is_current}, "
            f"'{sid}', 'config', 'v1')"
        )

    body = ',\n    '.join(rows)
    template = Path(template_path).read_text()
    rendered = _substitute_rows(template, body)
    Path(out_path).write_text(rendered)
    return out_path


def render_dim_venue_sql(template_path: str, out_path: str) -> str:
    """Render ``dim_venue.sql.j2`` to ``out_path`` (issue #145).

    Unlike the two renderers above (which build VALUES rows from leagues.yaml /
    a season window and use the ``{{ rows }}`` placeholder), dim_venue embeds
    the curated venue-alias dictionary. It reuses ``medallion_config`` — the
    same loader/VALUES/template machinery behind ``xref_referee.sql.j2`` — to
    fill the single ``{{ venue_aliases_values_sql }}`` placeholder.

    Returns ``out_path`` for the convenience of pipeline glue code.
    """
    # Lazy import: keeps the module top import-light (no PyYAML pull for DAGs
    # that don't render config-driven dims).
    from utils.medallion_config import (
        get_venue_alias_sql_values,
        render_sql_template,
    )

    rendered = render_sql_template(
        Path(template_path),
        venue_aliases_values_sql=get_venue_alias_sql_values(),
    )
    Path(out_path).write_text(rendered)
    return out_path


# ---------------------------------------------------------------------------
# Airflow-callable shim
# ---------------------------------------------------------------------------

def run_inline_ctas(
    renderer: Callable[[str, str], str],
    template_sql: str,
    table_name: str,
    partition_cols: Optional[List[str]] = None,
    **_ctx,
) -> dict:
    """Render a Jinja-style template to a tempfile and CTAS it into Gold.

    Designed as a ``PythonOperator.python_callable``. Mirrors the shape of
    ``dag_transform_fbref_gold._run_transform`` so the Airflow UI shows the
    same kwargs / XCom layout for inline-rendered dims as for file-based dims.

    Args:
        renderer: One of ``render_dim_competition_sql`` /
            ``render_dim_season_sql`` (or any compatible
            ``(template_path, out_path) -> out_path`` callable).
        template_sql: Path to the ``.sql.j2`` template. Resolved relative to
            ``/opt/airflow`` if not absolute (matches container layout).
        table_name: Target table name in ``iceberg.gold.``.
        partition_cols: Optional list of partition columns. Master-data dims
            are tiny (≤20 rows) so this is normally ``None``.

    Returns:
        The dict from ``run_gold_transform`` (status, row_count, etc.).
    """
    # Lazy import: avoids loading scrapers/__init__.py at DAG-parse time.
    from utils.gold_tasks import run_gold_transform

    template_path = Path(template_sql)
    if not template_path.is_absolute():
        template_path = Path('/opt/airflow') / template_path

    # NamedTemporaryFile with delete=False: we hand the path to Trino via
    # the CTAS engine, then let the OS clean /tmp at its leisure. The file
    # is cheap (a few KB of VALUES literals).
    with tempfile.NamedTemporaryFile(
        mode='w', suffix=f'_{table_name}.sql', delete=False
    ) as f:
        out_path = f.name

    renderer(str(template_path), out_path)

    return run_gold_transform(
        sql_file=out_path,
        table_name=table_name,
        partition_columns=partition_cols,
        add_timestamp=True,
    )
