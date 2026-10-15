# Transfermarkt D4: локальный сбор и проверка артефактов

CLI принимает сохранённые JSONL events и снимки ресурсов. Он проверяет форму,
знаменатели и согласованность артефактов. Команда `demo` дополнительно выполняет
локальные процессные сценарии. Ни одна команда не начинает приёмку и не выдаёт
runtime PASS: `runtime_status=UNKNOWN`, `runtime_eligible=false`,
`actual_acceptance_start=null` сохраняются даже для полностью согласованной фикстуры.

```bash
# Запускать в отдельной рабочей копии, с Python3.12 и подготовленными зависимостями.
PYTHONDONTWRITEBYTECODE=1 /root/.venvs/dpf-test/bin/python \
  dags/scripts/transfermarkt_offline_acceptance.py demo \
  /root/data-platform-football/.local/tasks/acceptance-preparation-20261015-transfermarkt/execution/d4/demo

PYTHONDONTWRITEBYTECODE=1 /root/.venvs/dpf-test/bin/python \
  dags/scripts/transfermarkt_offline_acceptance.py validate \
  /path/to/events.jsonl --manifest-sha256 <externally-pinned-manifest-sha256>
```

`demo` требует новый выходной каталог. Он сохраняет `synthetic-events.jsonl`,
`synthetic-validation.json` и `owned-local-exercises.json`. Фикстура содержит
искусственные пять суток и значения ресурсов. Её даты не обозначают реальные
окна наблюдения. JSONL отдельно помечен `provenance=synthetic_fixture`.
Возврат `validate`: 0 означает согласованный переданный артефакт, 2 — ошибку.
Отсутствующий блок получает отдельный `UNKNOWN`, независимо от других блоков.
Например, writer может быть `VALID`, пока canary остаётся `UNKNOWN`.

Первая строка JSONL — manifest schema1. Остальные строки — события. Полный
исполняемый пример генерирует `offline_harness.fixture()`, точные поля находятся
в `offline_acceptance.KINDS`; неизвестные поля, включая summary `PASS`, отвергаются.
Manifest содержит точную ревизию, неизменные actor/lane/policy/scope/URL/original
capture identities, SHA256 workload, окна и полный список ID событий каждого
типа. `producer` задаёт ID и непрерывную последовательность от1 доN. Каждое событие
содержит `producer_sequence`, `id`, `kind`, `window`. Порядок, пропуски,
повторы и расхождение inventory приводят к отказу. Пустой обязательный
знаменатель также отвергается.

`--manifest-sha256` должен приходить из отдельно закреплённого producer manifest.
Хэш считается от UTF-8 JSON с `sort_keys=True,separators=(',',':'),allow_nan=False`
(функция `digest`). Переданный вместе с подменёнными данными новый хэш не доказывает
независимость producer. Без внешнего pin `producer_status=UNKNOWN`; локальная
согласованность не превращается в доказательство полноты настоящего producer.
Подлинность самого исходного producer и источник его независимого inventory
должны квалифицироваться отдельно.

Для canary нужны три отдельных полных24h окна: baseline1TOTAL, canary1 и
canary2 по2TOTAL. Профили проверяются через существующий `TransfermarktStreams`.
Workload и actor/lane/policy assignment закреплены одним manifest. Все attempts,
retries, errors и provider bytes сверяются с интервальными counters. Timeout
знаменатель включает все attempts, cache completions показаны отдельно.
Проверяются throughput≥1.8x и timeouts<2%. Отчёт сохраняет исходный workload
знаменатель, незавершённые work IDs, lane counts, process RSS, cgroup memory и Trino peaks.

Manifest `sampling_seconds` — техническая настройка частоты снимков; в демо60s.
Она не меняет приёмочные требования владельца. Coverage spans обязаны непрерывно
покрывать всё окно без пробелов или наложений и согласовываться с точными
attempt/completion/query events. Положительные RSS и memory limit обязательны;
unknown/max вместо численного limit отвергается. Trino peak сверяется с
атрибутированными query IDs и peak memory из сохранённых событий. Отсутствующий
query producer не превращается в нулевое потребление.

Ресурсный collector строит один JSONL coverage event из локальных файлов. Он
читает сохранённый `VmRSS` в kB и JSON cgroup со строго тремя численными полями
`memory.current`, `memory.max`, `memory.swap.current`. `memory.current` отражает
общее потребление cgroup и не называется RSS. Counters/Trino берёт из
точных событий переданного JSONL. Он не опрашивает живой Trino или `/proc`.

```bash
PYTHONDONTWRITEBYTECODE=1 /root/.venvs/dpf-test/bin/python \
  dags/scripts/transfermarkt_offline_acceptance.py resource /path/to/events.jsonl \
  --window baseline --id s0 --sequence 1 \
  --start 2026-10-01T00:00:00+00:00 --end 2026-10-01T00:01:00+00:00 \
  --process-status /path/to/retained-proc-status.txt \
  --cgroup-stats /path/to/retained-cgroup-stats.json
```

Writer блок независимо проверяет24h, ровно2writer IDs, общий `st_dev:st_ino`
для writers и maintenance, четыре таблицы из штатного `CAREER_TABLES`,
натуральные ключи, неизменные исходные часы и before/after snapshot hashes.
Каждый ресурсный span содержит наблюдение всех участников и DQ checks всех
четырёх таблиц: count, duplicate count, key hash, original-clock hash.
Отдельный intake требует crash/replay, cache, empty, multiunit и compaction
receipts. Compaction сохраняет original anchors и ограничен ровно четырьмя
таблицами:64files и512MiB на таблицу,2GiB на запуск,600s. Эти данные описывают
проверяемый артефакт, а не разрешение на production compaction.

History блок независимо проверяет24h, каждую portion/task≤2700s, общий
attempt inventory каждой portion≤500 из штатного `MAX_HISTORY_ATTEMPTS`,
полный исходный lease/grant denominator с неизменными actor/grant/clocks,
unclosed leases<1%, enqueue→start evidence и history-attributed current waiting0.
Три consecutive batches обязаны сохранить checkpoint/resume chain, точный
remainder, исходные raw clocks и DQ counts. Transport intake связывает три
разных scope failures за10min и единственный recovery attempt с реальными
строками переданного журнала, проверяет15min pause/alert и свежий raw.
Season intake требует registry pair с последовательными сезонами и полного
старого roster/career/coaches до history claim. Исчезновение из registry не
считается закрытием сезона.

`owned-local-exercises.json` содержит отдельные факты настоящего локального
запуска. Два дочерних процесса одновременно обращаются к штатному `writer_lock`
и обновляют JSON store всех четырёх таблиц без потерь исходных ключей/часов.
Запись receipt сохраняет monotonic critical-section boundaries и точный lock
inode. Дочерний процесс завершает работу с17 после fsync штатного immutable
`write_intents.save_intent`; родитель проверяет checksum, unpack и replay readback.
Отдельный maintenance child использует тот же lock и записывает локальную
уплотнённую копию. Штатный `HistoryTransportCircuit` проходит pause/probe/heal
с искусственными часами и synthetic feedback. Используется pure renderer
`career_refs.apply_physical_refs`; SQL не выполняется. Children ограничены15s;
сценарии работают только со своими временными файлами внутри выходного каталога.

Это не проверка Iceberg commits, настоящих HTTP attempts, gateway lease TTL,
полного24h runtime, H0 activation, genuine1393 authority, source clock
достоверности, настоящей смены сезона или живого compaction. Порядок физического
распределения joint current/history workload при1TOTAL и reserved current1 ещё
нуждается в отдельном утверждённом протоколе; отсутствие такого протокола не
компенсируется synthetic lane labels. Реальные exporter producers/measurements
и будущий bounded live бюджет остаются отдельной работой. Текущие caps,
критерии, holds, anchors и series не изменяются.

Отчёт выдаёт отдельные результаты для canary, writer interval, history interval
и каждого fault/compaction/transport/season scenario. Отсутствующий сценарий
остаётся `UNKNOWN` и не заменяет результат другого критерия. Метрики
`*_sampled_peak_bytes` показывают максимум переданных точечных измерений.
Они не доказывают пик между снимками. `resource_peak_qualification=UNKNOWN`
сохраняется до отдельного intake retained `VmHWM` и cgroup `memory.peak`;
согласованность текущих samples не означает готовность resource peak criterion.

Дополнительный `validate --peak-input retained-peaks.json` принимает JSON list
сохранённых high-water marks. Optional manifest `peak_manifest[window_id]`
закрепляет process identity (boot/PID/start identity), cgroup identity,
`process_started_at`, `cgroup_peak_reset_at`, `observed_at`, SHA256 записи.
Запись дополнительно содержит `window`, `proc_status` с `VmHWM` в kB и численное
`cgroup_memory_peak_bytes` из сохранённого `memory.peak`. Peak artifact получает
`VALID` только при внешнем manifest pin, совпадении всех identities и часов,
process start и cgroup peak reset точно в начале окна и readout точно в конце.
Неизвестные reset boundaries, другой owner или отсутствие pin дают `UNKNOWN`;
нечисленные peaks и неверный record checksum дают `INVALID`. Это локальная
квалификация сохранённого peak artifact; подлинность producer и runtime
приёмка остаются отдельными условиями.

Уточнение контрактов после отдельного ревью:

- Window `attempt_owners` закрепляет полный список attempt ID → work/actor/stream.
  Каждая attempt строка повторяет эти identities. Transport scenario задаёт
  affected `stream_id` и `actor`; все его failures/probe связаны с этим owner.
  Проверка рассматривает все attempts затронутого stream, запрещает paid I/O
  после третьей ошибки до окончания15min pause и требует ровно один фактический
  probe до verified raw heal. Другие streams продолжают независимо.
- Optional `batch_manifest[batch_id]` закрепляет полный список capture units:
  work/raw ID, raw SHA256, capture ID, physical unit ID, original native batch,
  таблицу, rows, kind/empty type и original fetched/captured clocks. Batch event
  `capture_units` обязан точно совпасть с этим списком, включая typed-empty
  units с0rows. `raw_times` и DQ counts выводятся из полного unit denominator.
  Один immutable unit можно повторить в следующих batches без смены часов.
  Одинаковый raw/capture/physical ID не может иметь разные исходные identities
  или часы. Старые cache/raw clocks допустимы; близость к времени batch не нужна.
  Без pinned manifest либо unit proof history claim остаётся `UNKNOWN`.
- Coverage может содержать `process_identity` и `cgroup_identity`; resource CLI
  принимает одноимённые `--process-identity` и `--cgroup-identity`. Retained peak
  квалифицируется только при связи всех samples окна с теми же owners.
  Непривязанные/отсутствующие owners дают `UNKNOWN`. Связанный VmHWM либо
  memory.peak ниже соответствующего point maximum дают `INVALID`.

`capture_id` обозначает исходный **HTTP capture**, а `raw_id` — сохранённую raw
receipt/envelope этого capture; `raw_sha256` закрепляет исходные bytes. Это
не table-specific `career_refs.capture_unit_id`. Один HTTP capture может дать
несколько native/legacy outputs и повторно использоваться из cache в следующих
партиях. Его неизменная source identity состоит из raw ID/hash и original
fetched/captured clocks. Table, physical unit ID, output batch, rows, kind и empty
type относятся к каждому отдельному physical output. Поэтому эти поля могут
различаться между outputs одного capture. Один повторно используемый
`physical_unit_id` сохраняет полный исходный output и его source reference.

Transport criterion самостоятельно проверяет полный pinned attempt inventory,
owners и обязательные attempt поля через ту же проверку, что interval criterion.
Он не фильтрует непроверенные mutable stream labels. Недостоверный shared
attempt proof делает transport `INVALID`, даже если interval уже отказал.
Отсутствующий season scenario или ошибка несвязанного batch DQ по-прежнему
не заменяют результат transport; разрешённые unrelated streams не блокируются.
