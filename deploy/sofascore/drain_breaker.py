"""Ломатель тупика шага drain (#1245): припаркованный повтор скоупа истории — в failed.

Зовётся из deploy.sh как `docker exec -i <scheduler> python - <dag_id> <run_id> <pool>`
(текст идёт по stdin — однострочник без кавычек здесь уже не помещается).

Зачем: пул истории осушён на время выката, а run_historical_scope упавшего скоупа уходит
в up_for_retry и через retry_delay встаёт `scheduled` в осушённый пул. Слота не будет до
конца выката — прогон висит running навсегда, и шаг drain ждёт сам себя (ночь 05.09:
92 минуты и rc=4). Deadlock-детектор Airflow для этого DAG отключён (max_active_tis_per_dag=1
у run_historical_scope), сам он такой прогон не закроет.

Только `failed`, только припаркованные скоупы, только пул выката. Батч (#1248 ступень 1)
разбирается поскоупно: гасим ровно то множество, которое считает пятое число deploy.sh —
`scheduled` или `up_for_retry` при ЛЮБОМ try_number. Меньшее множество означало бы, что
прогон не закроется и выкат всё равно упрётся в потолок ожидания; большее — что мы гасим
работающий или уже терминальный скоуп. Работающие, поставленные в очередь исполнителю и
терминальные скоупы не трогаем: их учёт закроют validate и finalize.

Сначала блокировка строки прогона — ту же берут планировщик (dagrun.py) и мини-планировщик
(taskinstance.py), поэтому после нашего commit протокол «DagRun-lock → свежий список
готовых TI → schedule_tis» увидит failed и не вернёт задачу в scheduled. `failed` даёт
планировщику доработать хвост: validate своего скоупа не найдёт результата и упадёт,
finalize запишет отказ в failures.json (count+1 — то же, что сделал бы упавший повтор, на
90 минут раньше) и оставит соседние скоупы батча в покое.
"""

import sys

from airflow import settings
from airflow.models import DagRun, TaskInstance
from airflow.utils.sqlalchemy import with_row_locks

DAG_ID, RUN_ID, POOL = sys.argv[1], sys.argv[2], sys.argv[3]

session = settings.Session()
try:
    run = with_row_locks(
        session.query(DagRun).filter(DagRun.dag_id == DAG_ID, DagRun.run_id == RUN_ID),
        session=session,
        of=DagRun,
    ).one_or_none()
    if run is None:
        print("drain_breaker: отказ — прогона %s нет" % RUN_ID)
    else:
        # Свежее чтение ПОСЛЕ блокировки: между подсчётом в deploy.sh и этой строкой
        # состояние могло измениться, и тогда менять его — гадать.
        scopes = (
            session.query(TaskInstance)
            .filter(
                TaskInstance.dag_id == DAG_ID,
                TaskInstance.run_id == RUN_ID,
                TaskInstance.task_id == "run_historical_scope",
                TaskInstance.map_index >= 0,
            )
            .all()
        )
        if not scopes:
            # Карта ещё не раскрыта: есть только NULL-плейсхолдер map_index=-1,
            # и он не скоуп — трогать его нельзя.
            print("drain_breaker: отказ — раскрытых скоупов нет (mapped-скоупов 0)")
        for ti in scopes:
            # Колонка state — String, из базы приходит обычной строкой (не enum).
            state = ti.state
            if ti.pool != POOL:
                print("drain_breaker: отказ — скоуп в пуле %r, drain осушил %r" % (ti.pool, POOL))
            elif state in ("up_for_retry", "scheduled"):
                ti.set_state("failed", session=session)
                print("drain_breaker: map_index=%d %s -> failed" % (ti.map_index, state))
            else:
                print(
                    "drain_breaker: отказ — состояние вне протокола (state=%r, try_number=%r)"
                    % (state, ti.try_number)
                )
    session.commit()
finally:
    session.close()
