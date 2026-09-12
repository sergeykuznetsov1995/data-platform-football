# FBref Bronze — архивный проект плана

**Статус: вход для последующих ревью, не разрешение на реализацию.** Это сокращённое изложение проекта от12.09.2026 с исправленным scope. Пользователь остановил подготовку реализации; далее несколько ревью и только затем новые дочерние задачи. Порядок, интерфейсы и критерии ниже остаются предложениями.

Цель: raw, lossless generic и typed Bronze для всех опубликованных мужских турниров и всей доступной истории. Current≤24h и устойчивая параллельная история определяют первый GO; вся история к этому моменту может ещё загружаться. [Согласованный scope](agreed-scope.md) исключает Silver/Gold и proxy budget из задач/GO. 7-дневный soak согласован и **не начат**.

| Предлагавшаяся область | Предмет следующего ревью |
|---|---|
| 1. Correctness/recovery | Три подготовленных source fixes, точный release, Atlas replay, equal-count duplicates, аудит возможного wrong-perspective cohort и10 retained stages. |
| 2. Deadline | Единая абсолютная граница admission→recovery→fetch/parse/write→finalization; durable partial outcome и resume. |
| 3. Current lifecycle/freshness | Source-backed судьба comps68/76/79, точные freshness intersections, index identity, current/historical policy и advertised seasons. |
| 4. Параллельность | Writer ownership/fencing, snapshot consistency, резерв current в Airflow/executor, реальный overlap и независимый Bronze completion/release. |
| 5. Current capacity | Cadence и end-to-end latency в пределах24h, measured throughput, moved-target backoff, durable counters при failed/cancelled runs. |
| 6. Source inventory | Все male competitions/seasons и dataset/perspective denominator, включая полностью неоткрытые поддеревья и match-backed supercups. |
| 7. Availability | Evidence/TTL для restricted routes, различие empty/missing/error, oversized discovery gaps и адресный recovery. |
| 8. Dataset fidelity | Generic/typed parity, for/against, profile facts, Bronze score/events DQ и полная source season identity. |
| 9. Непрерывная история | Durable checkpoint, один controller, bounded retry, no-progress detection и restart/resume без потери данных. |
| 10. Приёмка | Точный release, offline replay, current/history canaries, overlap/crash tests, coverage и согласованный7-day soak. |

Предлагавшаяся зависимость: recovery → deadline/current lifecycle → concurrency → capacity/acceptance; inventory поддерживает availability/datasets и history controller. Новые интерфейсы ещё не утверждены.

Инварианты для ревью: completed manifests не переписываются; replay адресный и сохраняет lineage; display slug не заменяет полный source season ID; source gaps отличаются от parser errors; отсутствие canonical ID не удаляет source row. Сохранение текущей корректности учёта не означает отдельную задачу выбора proxy budget.

До остановки: три code fixes + test fix; **1770 passed,1 skipped** до audit utility; отдельная exact-row test-fixture archival. Deployment, source recovery, stage recovery, overlap и GO не выполнены. Первичный план предлагал canary/replay и предварительную серию scheduled current перед soak; эта схема ещё подлежит ревью, запусков не подтверждает. Silver child не входит в Bronze acceptance.

Историческая [карта23 issues](board-audit.md) сохраняет происхождение требований. Она не предписывает статусы новых карточек. [Downstream остаток](downstream-backlog.md) учитывается отдельно.
