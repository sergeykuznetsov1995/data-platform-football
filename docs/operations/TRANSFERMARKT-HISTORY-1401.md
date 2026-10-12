# Исторический контур Transfermarkt — #1401

История пишет только Native Bronze. Состояние читателя и отключение legacy writer
не предоставляют ей разрешение и не препятствуют отдельному историческому
разрешению. Current, manual и legacy пути сохраняют прежний writer guard.
Исторический физический писатель проверяет persisted campaign/batch/scope,
поколение claim, lease, последовательность попытки, stream, исходную capture
identity и действующую standing policy до HTTP и повторно перед Bronze write.

До оплаченного запроса нужны native gate, standing policy, immutable raw store,
отдельный history control token, непересекающийся пул выходов, включённые history
streams и durable gateway permits. Preflight проверяет существующую содержательную
квалификацию current `current_signal_qualification.json` по правилам #1393.
Отсутствующая или повреждённая квалификация блокирует историю. Для истории не
создаётся новый activation-файл и не принимается scalar `accepted=true`.
По умолчанию история выключена: `TM_HISTORY_STREAMS=0`, history token и
`TRANSFERMARKT_BACKFILL_PROXY_POOL_FILE` пусты. JSON пула подаётся deploy recipe
только в окружение compose; preflight проверяет разделение до остановки сервисов.
Этот документ и unit fixtures не являются разрешением включить production.

Каждая новая batch boundary читает последний promoted registry. Новые targets
добавляются через неизменяемую delta campaign; повторное чтение не создаёт их
второй раз. Исходные campaign rows, scope proofs, batch membership и DQ pins
сохраняются. Если старая запись ждёт unavailable retry до завтра, очередь может
выдать готовую запись другой campaign. Восстанавливаемая партия имеет приоритет;
одновременно допускается одна такая партия. #1402 ordering/window здесь нет.

Batch хранит registry для решения о допуске, содержимое standing policy,
`scope_registry_snapshot_ids`, `scope_writer_pins` и `scope_stream_ids`.
Совместимость ранее оплаченного prefix допускает только точное восстановление
исходного body hash при добавлении трёх career safety ops tables, season-close
table либо их объединения. Известный шаг версии для season-close — current3→4
и history1→2; произвольный скачок версии не принимается. Даты, все caps и
Bronze-права остаются исходными. Grant сохраняет прежние hash и policy_version,
а новые служебные записи требует разрешить действующая policy.
Продолжающийся scope сохраняет исходный registry, child cycle, пути checkpoint,
revision/slot исходного capture. Новый registry не делает старый raw свежим.
Новая партия получает текущую policy. Replay прежней партии проверяет её исходную
policy вместе с действующей: обе должны быть неистёкшими, разрешать операции и
канонические caps. Смена содержимого требует увеличения policy_version.
Legacy batch без сохранённой policy нельзя переиграть с неподтверждённым новым hash.
Fully collected verified local raw можно завершить без HTTP под действующей policy,
сохраняя исходный hash/timestamps. Если прежнее разрешение истекло, а capture ещё
не завершён, batch получает `waiting_policy`; scope/lease/checkpoints/raw и исходные
попытки сохраняются без reset, false complete и оплаченного replay. Другие готовые
entries продолжаются. Автоматическая миграция незавершённого capture в successor
batch при истёкшем разрешении здесь не подтверждена: parked scope остаётся видимым
долгом для дальнейшей безопасной миграции.

При смене сезона current выполняет финальный полный roster capture старого издания
с обычными raw, completeness и Bronze guards; exact continuation сохраняется.
После завершения careers/coaches и подтверждения committed roster публикуется
`transfermarkt_season_close_v1` handoff. Typed empty требует raw proof и отсутствия
физического roster. Переход current → historical в promoted history замечает и
исторический planner; pending handoff запрещает claim. Наличие нового текущего
издания обязательно: исчезнувшая строка не доказывает закрытие сезона.

Транспортные отказы — отдельный outcome. Они сохраняют raw/checkpoint и attempts,
но не расходуют три source attempts и не переводят scope в TERMINAL_ERROR.
Три отказа разных scopes одного history stream за десять минут ставят stream
на паузу на пятнадцать минут. Журнал и structured ERROR alert лежат на существующем
shared result volume в `transfermarkt-backfill/circuits`; planner, finalizer и
писатель используют один журнал. Current stream не затрагивается.

После паузы выдаётся одна recovery probe с frozen batch scope count=1 и
`TM_HISTORY_RECOVERY_PROBE=true`. Контракт #1400 ограничивает её **одной оплаченной
HTTP попыткой вместе с retries**. Успешный неполный scope остаётся CONTINUATION,
с exact checkpoint и исходными receipts. Пауза снимается только по одному новому
проверенному HTTP raw envelope; cached raw, missing/schema evidence не принимаются.
Повторный транспортный отказ или неверный probe снова дают пятнадцать минут паузы.
Source 403/429/challenge сохраняют собственную gateway slowdown policy #1398.

Health выделенного шлюза считает мёртвые exits current-пула отдельно от
history-пула. Отказ history не ухудшает current health. Общий список мёртвых
exits продолжает исключать их из выбора upstream; истечение TTL очищает и
счётчик, и отметку полосы. Health общего шлюза сохраняет прежнее поведение.

Приёмка остаётся отдельной: квалификация #1393 и фактические 100% live current
изданий три дня, ≥99% изменений за24ч; затем разрешённый запуск трёх исторических
партий с checkpoint/resume, DQ после каждой и без ожидания current pool. Реальные
прогоны, throughput, два writer процесса и доставка этой реализации не проверялись
offline тестами. Global career debt/TTL #1394, campaign ordering #1402, current
legacy freeze, Silver и Gold не входят в этот change.
