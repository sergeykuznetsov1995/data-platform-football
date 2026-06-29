# E7 — BI + Catalog: Scope

> Plan: `/root/.claude/plans/root-data-platform-docs-medallion-redes-replicated-rainbow.md`
> Branch: `feature/medallion-e7-bi-catalog` (от `feature/medallion-e6-features-ml-v2`)
> Reference postmortems: `docs/decisions/E0-postmortem.md` … `docs/decisions/E6-postmortem.md`

## Goal

Закрыть BI + Catalog гэп Medallion roadmap'а (`docs/MEDALLION_REDESIGN_ROADMAP.md` L384-392):

1. **3 mart-таблицы** в `iceberg.gold.*`: `mart_scouting_radar`, `mart_referee_dashboard`, `mart_event_heatmap`.
2. **Superset infra**: `import_dashboards.py` + 18 новых datasets + 3 декларативных дашборда + Makefile цели.
3. **OpenMetadata infra**: `apply_descriptions.py` + 20 YAML descriptions (+ relationships) + Makefile цели.

Аддитивно: existing E1-E6 объекты (dashboards, descriptions, dims/facts/features) НЕ трогаем.

## Out of scope

- **Manager profile mart** — деферрится в Phase 1.5 вслед за `dim_manager` (R0.2c FALLBACK, см. `docs/decisions/E2-postmortem.md`). `silver.xref_manager` сейчас zero-row STUB; mart без dim бессмыслен.
- E8 (advanced ML, GNN, real-time serving) — после ≥3 дней green DQ gate.

## Subtasks

| # | Описание | Owner agent |
|---|----------|-------------|
| T1 | Bootstrap ветки + scope-doc (этот файл) | `github-data-platform-manager` |
| T2 | 3 mart SQL CTAS + empty-stubs в `dags/sql/gold/` | `trino-specialist` |
| T3 | Wire mart-таски в `dag_transform_fbref_gold` + `dag_master_pipeline` | `airflow-expert` |
| T4 | Superset `import_dashboards.py` + 18 datasets + 3 dashboards + Makefile | `superset-dashboard-builder` |
| T5 | OpenMetadata `apply_descriptions.py` + 20 YAML + Makefile | `data-platform-architect` |
| T6 | Unit + integration тесты (mart SQL, apply scripts, DAG imports) | `testing-agent` |
| T7 | DQ extension в `gold_tasks.validate_gold_quality()` для 3 mart'ов | `data-quality-agent` |
| T8 | Postmortem + roadmap update + memory note (вручную после ≥3d green) | пользователь |

## DoD

- 3 mart-таблицы материализованы в `iceberg.gold.*`, row counts ≥ thresholds (scouting ≥800, referee ≥40, heatmap ≥80), PK uniqueness=1.0.
- 3 дашборда рендерятся в http://localhost:8088 без rate-limit.
- 20 OpenMetadata YAML примены: description + columns + tags + relationships видны в UI; ER-diagram содержит mart→fact→dim рёбра.
- DQ ERROR=0 ≥3 дня (стандартный roadmap gate перед E8).

## Critical files

См. секцию "Критические файлы" в плане (L148-165): создаваемые / редактируемые поверхности декомпозированы там пофайлово.

## Sequencing

```
T1 → (T2 ∥ T4 ∥ T5) → T3 → (T6 ∥ T7) → T8
```

T2/T4/T5 параллелятся (разные файловые поверхности). T3 ждёт T2 (mart SQL должен существовать перед wiring). T6/T7 ждут T2+T3+T4+T5 готовности скелетов. T8 — после ≥3d green DQ.

## Rollback

1. `git revert <merge-commit>` E7 PR.
2. `DROP TABLE iceberg.gold.mart_scouting_radar`, `mart_referee_dashboard`, `mart_event_heatmap` через `make shell-trino`.
3. Existing E1-E6 объекты не затронуты — всё E7 аддитивно (новые файлы, новые таски, новые datasets/descriptions без модификации старых).
