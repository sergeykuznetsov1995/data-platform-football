"""
Dim loaders — config-driven Gold dim renderers
==============================================

E2 (master-data dims) helper. Two of the config-driven dims do NOT come
from a Bronze/Silver source — they are projections of the medallion config
(single source of truth, issue #425):

  * ``dim_competition``   <- ``configs/medallion/competitions.yaml``
  * ``dim_season``        <- union of ``seasons`` across in-scope
                            competitions in the same YAML

Both are stored as Jinja-style templates (``dags/sql/gold/dim_*.sql.j2``)
with a single ``{{ rows }}`` placeholder for the VALUES tuples. The renderer
substitutes the placeholder with one quoted SQL row per config entry, writes
the rendered SQL to a tempfile, and hands it off to the existing
``run_gold_transform`` CTAS engine — same CREATE-OR-REPLACE CTAS contract as
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

import re
import tempfile
from datetime import date
from pathlib import Path
from typing import Callable, List, Optional

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


# ---------------------------------------------------------------------------
# Renderers
# ---------------------------------------------------------------------------

def render_dim_competition_sql(template_path: str, out_path: str) -> str:
    """Render ``dim_competition.sql.j2`` to ``out_path`` (issue #425).

    Reads ``configs/medallion/competitions.yaml`` (the medallion source of
    truth) and emits one VALUES row per competition — stubs included, the
    dim is a dictionary — with the columns declared in the template's
    ``AS t(...)`` clause:
        league, competition_name, country, tier

    PK ``league`` is the competition slug ('ENG-Premier League'), identical
    to the ``league`` value carried by every Bronze/Silver/Gold row, so
    fact-to-dim JOINs need no mapping table.

    The template's ``{{ rows }}`` literal is replaced with the joined tuples.
    Returns ``out_path`` for the convenience of pipeline glue code.
    """
    # Lazy import: keeps the module top import-light (no PyYAML pull for DAGs
    # that don't render config-driven dims).
    from utils.medallion_config import _escape_sql_string, load_competitions

    rows: List[str] = []
    for c in load_competitions()['competitions']:
        rows.append(
            f"('{_escape_sql_string(c['id'])}', "
            f"'{_escape_sql_string(c['name'])}', "
            f"'{_escape_sql_string(c['country'])}', "
            f"{int(c['tier'])})"
        )

    body = ',\n    '.join(rows)
    template = Path(template_path).read_text()
    rendered = _substitute_rows(template, body)
    Path(out_path).write_text(rendered)
    return out_path


def render_dim_season_sql(template_path: str, out_path: str) -> str:
    """Render ``dim_season.sql.j2`` to ``out_path`` (issue #425).

    Unions ``seasons`` across all in-scope competitions in
    ``configs/medallion/competitions.yaml``, dedupes by season slug
    (earliest start / latest end win, so two leagues sharing slug '2425'
    produce one covering row), and emits the columns declared in the
    template's ``AS t(...)`` clause:
        season, season_name, start_date, end_date, is_current

    PK ``season`` is the 4-char slug ('2425') — the same value carried by
    every Bronze/Silver/Gold row after #404.

    ``is_current``: the LATEST season already started (max slug with
    start_date <= today). A plain BETWEEN would flag zero seasons during
    the summer gap between end_date and the next start_date.
    """
    from utils.medallion_config import load_competitions

    seasons: dict = {}  # slug -> {'start': date, 'end': date}
    for c in load_competitions()['competitions']:
        if not c.get('in_scope'):
            continue
        for s in c.get('seasons') or []:
            # YAML season id is an int (2425) — format as 4-digit slug so a
            # hypothetical '0203' season keeps its leading zero.
            slug = f"{int(s['id']):04d}"
            start = date.fromisoformat(str(s['start']))
            end = date.fromisoformat(str(s['end']))
            if slug in seasons:
                seasons[slug]['start'] = min(seasons[slug]['start'], start)
                seasons[slug]['end'] = max(seasons[slug]['end'], end)
            else:
                seasons[slug] = {'start': start, 'end': end}

    today = date.today()
    started = [slug for slug, w in seasons.items() if w['start'] <= today]
    current_slug = max(started) if started else None

    rows: List[str] = []
    for slug in sorted(seasons):
        w = seasons[slug]
        name = f"20{slug[:2]}-{slug[2:]}"
        is_current = 'true' if slug == current_slug else 'false'
        rows.append(
            f"('{slug}', '{name}', "
            f"DATE '{w['start']}', DATE '{w['end']}', {is_current})"
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
        # include_capacity=True (issue #434): dim_venue projects capacity;
        # dim_match reuses the same dict WITHOUT capacity (6-col tuple).
        venue_aliases_values_sql=get_venue_alias_sql_values(include_capacity=True),
    )
    Path(out_path).write_text(rendered)
    return out_path


def render_dim_team_sql(template_path: str, out_path: str) -> str:
    """Render ``dim_team.sql.j2`` to ``out_path`` (issue #425).

    Fills the single ``{{ team_meta_values_sql }}`` placeholder with
    ``(team_id, team_name, country, short_name)`` tuples from
    ``team_aliases.yaml`` — the club-attribute side of the dim; the row
    spine still comes from ``silver.xref_team`` inside the template.
    """
    from utils.medallion_config import (
        get_team_meta_sql_values,
        render_sql_template,
    )

    rendered = render_sql_template(
        Path(template_path),
        team_meta_values_sql=get_team_meta_sql_values(),
    )
    Path(out_path).write_text(rendered)
    return out_path


def render_dim_player_sql(template_path: str, out_path: str) -> str:
    """Render ``dim_player.sql.j2`` to ``out_path`` (issues #435, #585).

    Fills two placeholders from ``country_codes.yaml``:
    - ``{{ country_map_values_sql }}`` — ``(fifa_code, country_name)`` tuples,
      the FBref FIFA-code -> full-name map used in the nationality COALESCE;
    - ``{{ nationality_alias_values_sql }}`` (#585) — ``(variant, canonical)``
      tuples that canonicalize inconsistent source spellings before that map.
    The row spine still comes from ``silver.xref_player`` inside the template.
    """
    from utils.medallion_config import (
        get_country_alias_sql_values,
        get_country_map_sql_values,
        render_sql_template,
    )

    rendered = render_sql_template(
        Path(template_path),
        country_map_values_sql=get_country_map_sql_values(),
        nationality_alias_values_sql=get_country_alias_sql_values(),
    )
    Path(out_path).write_text(rendered)
    return out_path


def render_dim_match_sql(template_path: str, out_path: str) -> str:
    """Render ``dim_match.sql.j2`` to ``out_path`` (issue #425).

    Fills ``{{ venue_aliases_values_sql }}`` with the SAME tuples as
    ``render_dim_venue_sql`` so the inline venue_id resolution in dim_match
    is byte-identical to dim_venue's PK (a JOIN on gold.dim_venue is not an
    option: the slim dim no longer carries raw alias spellings).
    """
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
