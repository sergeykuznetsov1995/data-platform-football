"""
Silver xref Transformation DAG  (Medallion E1 / T4)
====================================================

Materialises the five Silver-layer cross-reference tables that are the
source-of-truth for canonical entity identity across the platform:

    iceberg.silver.xref_team      — 8-source team alias map (pure SQL CTAS)
    iceberg.silver.xref_match     — match-id spine (FBref-only at MVP)
    iceberg.silver.xref_referee   — referee names from FBref + MatchHistory
    iceberg.silver.xref_manager   — empty STUB (schema-only; populated in 1.5)
    iceberg.silver.xref_player    — fuzzy resolver across FBref/Understat/WhoScored

Topology
--------
    start_marker
        |
        v
    TaskGroup: xref_transforms  (4 sequential pure-SQL CTAS)
        ├── xref_team       — render Jinja .sql.j2 + run_silver_transform
        ├── xref_match      — run_silver_transform
        ├── xref_referee    — run_silver_transform
        └── xref_manager    — run_silver_transform   (zero-row STUB OK)
        |
        v
    xref_player             — Python: utils.xref_player_resolver.run_resolver()
        |
        v
    validate_xref           — DQ row counts + PK uniqueness
        |
        v
    end_marker

Trigger model
-------------
``schedule=None`` — the DAG is triggered after Bronze ingestion finishes
(see ``dag_master_pipeline`` integration). xref tables are intentionally
INDEPENDENT of FBref Silver: re-running this DAG does not require the
existing fbref Silver to be fresh.

Notes for maintainers
---------------------
* xref_team is a Jinja template (``.sql.j2``); team-alias VALUES are
  injected by ``utils.medallion_config.render_sql_template`` at task time.
  The rendered SQL is written to a tempfile and passed to
  ``run_silver_transform`` (which already supports absolute paths).
* xref_player is **NOT** a Trino CTAS — it is a Python pipeline (rapidfuzz
  + unidecode) materialised via INSERT VALUES. ``run_resolver`` does its
  own DROP+CREATE+INSERT inside one Trino connection.
* ``xref_manager`` is intentionally zero-row at E1 (R0.2c FALLBACK
  pending). Materialising the empty table protects downstream JOINs.
"""

from __future__ import annotations

import logging
import os
import tempfile
from datetime import datetime
from typing import Any, Dict

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from utils.default_args import SILVER_ARGS

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pure-SQL xref transforms (xref_team handled separately due to Jinja)
# ---------------------------------------------------------------------------
# (task_id, sql_file relative to /opt/airflow/, target table name)
PURE_SQL_XREF_TRANSFORMS = [
    (
        'xref_match',
        'dags/sql/silver/xref_match.sql',
        'xref_match',
    ),
    # xref_referee moved to a dedicated Jinja-template callable (_run_xref_referee,
    # issue #143) — it now embeds referee_aliases.yaml like xref_team.
    # xref_manager moved to a dedicated Jinja-template callable
    # (_run_xref_manager, xref-improvements) — it now embeds
    # manager_aliases.yaml and bridges Transfermarkt coaches.
]


# ---------------------------------------------------------------------------
# Task callables — imports are inside callables so DAG parse stays cheap
# ---------------------------------------------------------------------------

def _run_xref_team(**context) -> Dict[str, Any]:
    """Render the xref_team Jinja template and run it as a Silver CTAS.

    The template embeds team-alias VALUES via
    ``medallion_config.render_sql_template`` — we render to a tempfile so
    we can re-use the existing ``run_silver_transform`` (which accepts
    absolute paths and handles DROP+CREATE+SCHEMA bookkeeping).

    The tempfile is removed in the ``finally`` block regardless of CTAS
    success — leaking temp files in /tmp would slowly fill the scheduler.
    """
    from pathlib import Path

    from utils.medallion_config import (
        get_team_alias_sql_values,
        render_sql_template,
    )
    from utils.silver_tasks import run_silver_transform

    template_path = Path('/opt/airflow/dags/sql/silver/xref_team.sql.j2')
    if not template_path.exists():
        raise FileNotFoundError(f"xref_team template not found: {template_path}")

    rendered_sql = render_sql_template(
        template_path,
        team_aliases_values_sql=get_team_alias_sql_values(
            with_canonical_id=True, with_league=True
        ),
    )
    logger.info(
        "Rendered xref_team.sql.j2 — %d chars (template embeds %d alias pairs)",
        len(rendered_sql),
        rendered_sql.count("),\n"),  # rough alias-row counter for log only
    )

    with tempfile.NamedTemporaryFile(
        mode='w',
        suffix='_xref_team.sql',
        delete=False,
        encoding='utf-8',
    ) as tmp:
        tmp.write(rendered_sql)
        tmp_path = tmp.name

    try:
        result = run_silver_transform(
            sql_file=tmp_path,
            table_name='xref_team',
            schema='silver',
        )
        logger.info(
            "xref_team CTAS complete: %d rows in %s",
            result.get('rows', 0),
            result.get('table'),
        )
        return result
    finally:
        try:
            os.unlink(tmp_path)
        except OSError as e:
            logger.warning("Failed to cleanup temp file %s: %s", tmp_path, e)


def _run_xref_referee(**context) -> Dict[str, Any]:
    """Render the xref_referee Jinja template and run it as a Silver CTAS.

    Mirror of :func:`_run_xref_team` (issue #143): embeds the curated
    referee_aliases.yaml as inline VALUES so FBref "Michael Oliver" and
    MatchHistory "M Oliver" resolve to one ``ref_<slug>`` canonical_id without
    fuzzy matching. The tempfile is removed in ``finally`` regardless of CTAS
    outcome to avoid leaking files in the scheduler's /tmp.
    """
    from pathlib import Path

    from utils.medallion_config import (
        get_referee_alias_sql_values,
        render_sql_template,
    )
    from utils.silver_tasks import run_silver_transform

    template_path = Path('/opt/airflow/dags/sql/silver/xref_referee.sql.j2')
    if not template_path.exists():
        raise FileNotFoundError(f"xref_referee template not found: {template_path}")

    rendered_sql = render_sql_template(
        template_path,
        referee_aliases_values_sql=get_referee_alias_sql_values(
            with_canonical_id=True, with_league=True
        ),
    )
    logger.info("Rendered xref_referee.sql.j2 — %d chars", len(rendered_sql))

    with tempfile.NamedTemporaryFile(
        mode='w',
        suffix='_xref_referee.sql',
        delete=False,
        encoding='utf-8',
    ) as tmp:
        tmp.write(rendered_sql)
        tmp_path = tmp.name

    try:
        result = run_silver_transform(
            sql_file=tmp_path,
            table_name='xref_referee',
            schema='silver',
        )
        logger.info(
            "xref_referee CTAS complete: %d rows in %s",
            result.get('rows', 0),
            result.get('table'),
        )
        return result
    finally:
        try:
            os.unlink(tmp_path)
        except OSError as e:
            logger.warning("Failed to cleanup temp file %s: %s", tmp_path, e)


def _run_xref_manager(**context) -> Dict[str, Any]:
    """Render the xref_manager Jinja template and run it as a Silver CTAS.

    Mirror of :func:`_run_xref_referee`: embeds the curated
    manager_aliases.yaml as inline VALUES so mis-normalising spellings
    (surname-first TM forms etc.) resolve onto the FBref-spine canonical_id.
    ``source=None`` merges the ``_generic`` bucket with every per-source
    bucket — one alias map serves TM and FotMob alike (a TM-only raw form
    never matches a FotMob name, safe no-op). The tempfile is removed in
    ``finally`` regardless of CTAS outcome.
    """
    from pathlib import Path

    from utils.medallion_config import (
        get_manager_alias_sql_values,
        render_sql_template,
    )
    from utils.silver_tasks import run_silver_transform

    template_path = Path('/opt/airflow/dags/sql/silver/xref_manager.sql.j2')
    if not template_path.exists():
        raise FileNotFoundError(f"xref_manager template not found: {template_path}")

    rendered_sql = render_sql_template(
        template_path,
        manager_aliases_values_sql=get_manager_alias_sql_values(source=None),
    )
    logger.info("Rendered xref_manager.sql.j2 — %d chars", len(rendered_sql))

    with tempfile.NamedTemporaryFile(
        mode='w',
        suffix='_xref_manager.sql',
        delete=False,
        encoding='utf-8',
    ) as tmp:
        tmp.write(rendered_sql)
        tmp_path = tmp.name

    try:
        result = run_silver_transform(
            sql_file=tmp_path,
            table_name='xref_manager',
            schema='silver',
        )
        logger.info(
            "xref_manager CTAS complete: %d rows in %s",
            result.get('rows', 0),
            result.get('table'),
        )
        return result
    finally:
        try:
            os.unlink(tmp_path)
        except OSError as e:
            logger.warning("Failed to cleanup temp file %s: %s", tmp_path, e)


def _run_pure_sql_xref(sql_file: str, table_name: str, **context) -> Dict[str, Any]:
    """Generic CTAS runner for xref_match (pure-SELECT, no Jinja).

    Kept list-driven for any future pure-SQL xref additions; xref_referee and
    xref_manager graduated to dedicated Jinja callables.
    """
    from utils.silver_tasks import run_silver_transform

    result = run_silver_transform(
        sql_file=sql_file,
        table_name=table_name,
        schema='silver',
    )
    logger.info(
        "%s CTAS complete: %d rows in %s",
        table_name,
        result.get('rows', 0),
        result.get('table'),
    )
    return result


def _run_xref_player(**context) -> Dict[str, Any]:
    """Run the Python xref_player resolver (T3 deliverable).

    Materialises ``iceberg.silver.xref_player`` from FBref + Understat +
    WhoScored Bronze data. ``run_resolver`` raises ``ResolverError`` if
    the known-pair regression check (10/10 APL anchors) drops below 8/10
    — that aborts the DAG before partial data lands.

    Summary dict is pushed to XCom for the downstream DQ check (T6)
    to consume.
    """
    from utils.xref_player_resolver import run_resolver

    summary = run_resolver(
        target_table='iceberg.silver.xref_player',
        league='ENG-Premier League',
        seasons=None,                # all configured seasons (E1: APL only)
        chunk_size=500,
        drop_before_insert=True,
    )

    logger.info("xref_player resolver summary: %s", summary)
    context['ti'].xcom_push(key='resolver_summary', value=summary)
    return summary


def _validate_xref(**context) -> Dict[str, Any]:
    """Run extended DQ for the 5 xref tables (T6).

    Phase 1 — standard DQ via :mod:`utils.xref_dq.build_all_xref_checks`
    Phase 2 — orphan-rate per source for xref_team / xref_player / xref_referee
              / xref_manager (synthetic CheckResults appended to the report)

    Severity model:
      * Phase 1 ERROR-checks raise ``AirflowException``.
      * Phase 2 orphan-rate ERROR raises (team/player >25%; referee >35%;
        manager >60%).

    XCom outputs:
      * key=``orphan_rates``    — dict {team, player, referee, manager →
                                  per-source verdicts}
      * key=``orphan_teams``    — dict {total_orphans, per_source, rows} (issue #141)
    """
    from airflow.exceptions import AirflowException

    from utils.alerts import telegram_dq_summary
    from utils.data_quality import CheckResult, run_checks
    from utils.xref_dq import (
        build_all_xref_checks,
        evaluate_bronze_xref_freshness_gap,
        evaluate_dob_conflicts,
        evaluate_manager_dob_collisions,
        evaluate_orphan_rate_per_source,
        report_orphan_teams,
    )

    # ------------------------------------------------------------------
    # Phase 1 — standard DQ on all 5 xref tables (16+ checks)
    # ------------------------------------------------------------------
    report = run_checks(build_all_xref_checks(), raise_on_error=False)
    logger.info("Phase 1 — xref DQ: %s", report.summary())

    # ------------------------------------------------------------------
    # Phase 2 — orphan-rate per source (team + player + referee + manager)
    # Per-entity (warn, err) bands: referee feeds (#143) and manager FotMob
    # coaches (#144) are noisier than team/player, so they get looser bands —
    # surfaced in the report, but won't fail the DAG below the error threshold.
    # ------------------------------------------------------------------
    # #803: team/player orphan-rate is measured on the CURRENT season only.
    # After the historical backfill these tables span ~10 seasons whose thin
    # old FBref spine leaves most rows legitimately orphan (#788) — table-wide
    # that turns into a false ERROR (fotmob 28.8%, xref_team TM 86.9%), while
    # the current season is the real resolver-health signal (1.3%, 0%).
    # Referee/manager stay table-wide: looser bands, not season-historized the
    # same way, and not breaching — left untouched pending their own evidence.
    orphan_rates: Dict[str, Any] = {}
    for entity, table, warn_t, err_t, current_only in (
        ('team', 'iceberg.silver.xref_team', 10.0, 25.0, True),
        ('player', 'iceberg.silver.xref_player', 10.0, 25.0, True),
        # Referee feeds are noisier (initial-only MatchHistory forms) and have
        # no DOB disambiguator → looser band (issue #143).
        ('referee', 'iceberg.silver.xref_referee', 15.0, 35.0, False),
        # manager (#144): FotMob orphans expected at worldwide scale → soft band.
        ('manager', 'iceberg.silver.xref_manager', 25.0, 60.0, False),
    ):
        try:
            res = evaluate_orphan_rate_per_source(
                table=table,
                warning_threshold=warn_t,
                error_threshold=err_t,
                current_season_only=current_only,
            )
        except Exception as e:
            logger.exception("orphan_rate evaluation failed for %s", table)
            report.results.append(CheckResult(
                name=f"orphan_rate[{table}]",
                kind='coverage',
                severity='WARNING',
                passed=False,
                error=str(e),
            ))
            orphan_rates[entity] = {'error': str(e)}
            continue

        verdict = res['verdict']
        passed = verdict == 'OK'
        # Map verdict to CheckResult severity. 'OK' falls through to WARNING
        # because severity is ignored downstream when passed=True.
        if verdict == 'ERROR':
            severity = 'ERROR'
        else:
            severity = 'WARNING'
        report.results.append(CheckResult(
            name=f"orphan_rate[{table}]",
            kind='coverage',
            severity=severity,
            passed=passed,
            details=(
                f"verdict={verdict}, overall={res['overall_pct']}%, "
                f"breaches={res['breaches']}"
            ),
            value=res['overall_pct'],
        ))
        orphan_rates[entity] = res

    context['ti'].xcom_push(key='orphan_rates', value=orphan_rates)

    # ------------------------------------------------------------------
    # Phase 2.6 — orphan team report (issue #141, stage 4)
    # Informational: lists the actual un-glued raw team-names so a maintainer
    # can extend team_aliases.yaml for what is really broken. Never escalates.
    # ------------------------------------------------------------------
    try:
        orphan_teams = report_orphan_teams(table='iceberg.silver.xref_team')
        logger.info(
            "Phase 2.6 — orphan teams: %d distinct (per_source=%s)",
            orphan_teams['total_orphans'],
            orphan_teams['per_source'],
        )
        for row in orphan_teams['rows']:
            logger.info(
                "  orphan team: source=%s league=%s season=%s name=%r",
                row['source'], row['league'], row['season'], row['source_id'],
            )
        context['ti'].xcom_push(key='orphan_teams', value=orphan_teams)
    except Exception:
        logger.exception("orphan team report failed (non-fatal)")

    # ------------------------------------------------------------------
    # Phase 2.7 — cross-source DOB conflicts (companion to name_team_dob)
    # A canonical whose linked sources disagree on birth date by >1 day is
    # a suspected false merge; the resolver excludes such canonicals from
    # its own DOB map, so this WARNING report is where they surface.
    # ------------------------------------------------------------------
    try:
        dob_conflicts = evaluate_dob_conflicts()
        verdict = dob_conflicts['verdict']
        report.results.append(CheckResult(
            name='dob_conflicts[xref_player]',
            kind='coverage',
            severity='WARNING',
            passed=verdict == 'OK',
            details=(
                f"verdict={verdict}, conflicts={dob_conflicts['conflicts']}, "
                f"rows={dob_conflicts['rows'][:10]}"
            ),
            value=float(dob_conflicts['conflicts']),
        ))
        context['ti'].xcom_push(key='dob_conflicts', value=dob_conflicts)
    except Exception as e:
        logger.exception("dob-conflict evaluation failed (non-fatal)")
        report.results.append(CheckResult(
            name='dob_conflicts[xref_player]',
            kind='coverage',
            severity='WARNING',
            passed=False,
            error=str(e),
        ))

    # Manager DOB corroboration: FotMob-vs-TM disagreement per canonical is a
    # suspected false merge (strongest signal for name_initial-tier rows).
    try:
        mgr_dob = evaluate_manager_dob_collisions()
        report.results.append(CheckResult(
            name='dob_collisions[xref_manager]',
            kind='coverage',
            severity='WARNING',
            passed=mgr_dob['verdict'] == 'OK',
            details=(
                f"verdict={mgr_dob['verdict']}, "
                f"collisions={mgr_dob['collisions']}, "
                f"rows={mgr_dob['rows'][:10]}"
            ),
            value=float(mgr_dob['collisions']),
        ))
        context['ti'].xcom_push(key='manager_dob_collisions', value=mgr_dob)
    except Exception as e:
        logger.exception("manager dob-collision evaluation failed (non-fatal)")
        report.results.append(CheckResult(
            name='dob_collisions[xref_manager]',
            kind='coverage',
            severity='WARNING',
            passed=False,
            error=str(e),
        ))

    # ------------------------------------------------------------------
    # Phase 2.5 — Bronze-vs-xref freshness gap (Issue #15 regression guard)
    # ------------------------------------------------------------------
    try:
        freshness = evaluate_bronze_xref_freshness_gap()
        verdict = freshness['verdict']
        passed = verdict == 'OK'
        severity = 'ERROR' if verdict == 'ERROR' else 'WARNING'
        report.results.append(CheckResult(
            name='bronze_xref_freshness[xref_player]',
            kind='freshness',
            severity=severity,
            passed=passed,
            details=(
                f"verdict={verdict}, xref_committed={freshness['xref_max_committed_at']}, "
                f"breaches={freshness['breaches']}"
            ),
        ))
        context['ti'].xcom_push(key='bronze_xref_freshness', value=freshness)
    except Exception as e:
        logger.exception("bronze-xref freshness evaluation failed")
        report.results.append(CheckResult(
            name='bronze_xref_freshness[xref_player]',
            kind='freshness',
            severity='WARNING',
            passed=False,
            error=str(e),
        ))

    # ------------------------------------------------------------------
    # Telegram summary (Phase 1 + Phase 2 + Phase 2.5 combined)
    # ------------------------------------------------------------------
    telegram_dq_summary(report, header="Silver xref DQ (E1)")

    # ------------------------------------------------------------------
    # Phase 1 + 2 + 2.5 ERROR escalation
    # ------------------------------------------------------------------
    if report.errors:
        raise AirflowException(
            f"xref DQ failed: {len(report.errors)} error(s). "
            + "; ".join(
                f"{r.name}: {r.details or r.error}"
                for r in report.errors[:5]
            )
        )

    return {
        'passed': len(report.passed),
        'total': len(report.results),
        'errors': [r.name for r in report.errors],
        'warnings': [r.name for r in report.warnings],
        'orphan_rates': orphan_rates,
    }


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id='dag_transform_xref',
    default_args=SILVER_ARGS,
    description=(
        'Materialise Silver xref_* tables (team/match/referee/manager/player) '
        'from Bronze. E1 medallion redesign — runs before fbref Silver.'
    ),
    schedule=None,           # Triggered (see master pipeline integration)
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=['silver', 'xref', 'medallion-e1', 'transform'],
    max_active_runs=1,
    max_active_tasks=1,      # Sequential — same OOM-safety reasoning as fbref Silver
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id='start_marker')

    # =========================================================================
    # TaskGroup: xref CTAS transforms (sequential pure-SQL — max_active_tasks=1)
    # =========================================================================
    with TaskGroup(group_id='xref_transforms') as transforms_group:
        # xref_team is a Jinja template (.sql.j2) — handled by dedicated callable
        xref_team_task = PythonOperator(
            task_id='xref_team',
            python_callable=_run_xref_team,
        )

        # xref_referee — Jinja template (.sql.j2), embeds referee_aliases.yaml
        xref_referee_task = PythonOperator(
            task_id='xref_referee',
            python_callable=_run_xref_referee,
        )
        xref_team_task >> xref_referee_task

        # xref_match — plain SELECT file
        prev = xref_referee_task
        for task_id, sql_file, table_name in PURE_SQL_XREF_TRANSFORMS:
            t = PythonOperator(
                task_id=task_id,
                python_callable=_run_pure_sql_xref,
                op_kwargs={
                    'sql_file': sql_file,
                    'table_name': table_name,
                },
            )
            prev >> t
            prev = t

        # xref_manager — Jinja template (.sql.j2), embeds manager_aliases.yaml
        # and bridges Transfermarkt coach_ids onto the FBref name-spine.
        xref_manager_task = PythonOperator(
            task_id='xref_manager',
            python_callable=_run_xref_manager,
        )
        prev >> xref_manager_task

    # =========================================================================
    # xref_player — Python resolver (rapidfuzz / unidecode, NOT a Trino CTAS)
    # =========================================================================
    xref_player_task = PythonOperator(
        task_id='xref_player',
        python_callable=_run_xref_player,
    )

    # =========================================================================
    # Validation — DQ row counts + PK uniqueness
    # =========================================================================
    validate_task = PythonOperator(
        task_id='validate_xref',
        python_callable=_validate_xref,
        trigger_rule='all_success',  # Skip validation if any xref task failed
    )

    end = EmptyOperator(task_id='end_marker')

    # =========================================================================
    # Dependencies
    # =========================================================================
    start >> transforms_group >> xref_player_task >> validate_task >> end
