# Transfermarkt: безопасная запись карьерных таблиц (#1399)

Этот контракт относится к Bronze Transfermarkt и служебным таблицам ops.
Приёмка на живом runtime, доставка и настройка mounts выполняются отдельно.
Unit-тесты не подтверждают такую приёмку.

## Граница записи

Четыре карьерные таблицы — `transfermarkt_market_value_points`,
`transfermarkt_transfer_events`, `transfermarkt_market_value_history` и
`transfermarkt_transfers` — заменяют данные только для действительно полученных
игроков. Cached native строки сохраняют исходные batch, lineage и значения.
Legacy-проекция может гидратироваться из проверенного исходного capture для
совместимости выбранного scope. Успех требует фактического readback и manifest;
наличие cached игрока не разрешает повторно записать его native карьеру.

TM DML использует локальный committing adapter: он дочитывает результат запроса
и повторяет только явный Iceberg commit conflict с ограниченным числом попыток
и jitter. Общая короткая блокировка покрывает warehouse commit и reconciliation.
HTTP и parsing выполняются вне блокировки. Current, history, discovery registry,
scope/backfill и native ops должны использовать тот же файл блокировки.

## Долговечное доказательство capture

`TM_WRITE_INTENT_DIR` задаёт родительский каталог `career-write-intents`; при
отсутствии используется `TM_PENDING_CHECKPOINT_DIR`, затем
`/opt/airflow/logs/transfermarkt-checkpoints`. Этот каталог и исходный raw store
должны переживать рестарт и быть доступны всем TM задачам reconciliation.
Оригинальный journal содержит parsed frame, raw envelopes, исходные часы,
режим записи, writer revision, batch, scope, поколения сигналов и точный остаток.
Имя journal — SHA256 его канонического тела.

До первой Bronze-записи сохраняется неизменяемый snapshot anchor. Успешная
current или generic career-запись сохраняет в `completed/<intent SHA>.json` оригинальный
journal, реальные physical frames и receipt. `completed/by-unit/` связывает
`(manifest cycle, entity, native batch)` с этим архивом. Записи содержат checksum;
публикация использует atomic replace и fsync файла и каталога. Повторное
подтверждение сохраняет исходный `committed_at` и восстанавливает индекс после
сбоя между архивом и индексом. Оно не создаёт новый источник свежести.

Архив сохраняет actual manifest readback attestation в исходном writer namespace
(dual или native-only). Если следующая порция stable child заменяет manifest row,
проверка требует её точный успешный complete archive того же cycle/entity/scope
и writer revision, обе стороны для dual, оригинальные refs/snapshots и полный
raw. Одного `status=success` недостаточно. Проверка последнего архива не вызывает
новую цепочку архивов. Отсутствующий или повреждённый manifest, attestation,
последний архив или обязательный raw запрещают reconciliation.

Для typed empty после дочитанного DELETE каждой карьерной таблицы, до следующего
DELETE, сохраняется `empty-commits/<table>/<intent SHA>.json`: оригинальный
intent, anchor, реальный snapshot и точные игроки с нулём строк. Recovery читает
именно этот snapshot. Без receipt нельзя приписать более поздний чужой пустой
snapshot старой работе. Сбой между DELETE и fsync receipt остаётся явной ошибкой;
raw и исходный journal сохраняются, успешное завершение не выдумывается.

Capture refs и anchors сохраняют оригинальные snapshots и raw lineage.
Snapshot recovery ищет только ограниченную последовательность после anchor и
проверяет полные business fields, batch, lineage, count и hash. Оно не
перезаписывает более свежую карьеру. Для typed empty требуется исходный полный
пустой endpoint, raw proof и согласованное отсутствие физических строк.

## Сбой после native, до legacy

Если новая полная карьера заменила старую частично записанную работу,
reconciliation может завершить старую работу статусом
`superseded_partial_write`. Этот статус — terminal failure старой работы:
`verified=false`, без signal acknowledgement, без успешного старого dual
manifest и без выдуманного legacy snapshot.

Для каждого игрока старой работы требуется строго более свежий полный успешный
capture. Dual successor требует настоящего успешного dual manifest и обоих
физических count/hash. Genuine native-only successor требует собственного
native manifest и native count/hash; он не объявляется dual success. Оба режима
требуют оригинального полного raw и envelope, lineage и закреплённых snapshots.
Typed empty проверяется отдельно.
Весь исходный bundle проверяется по закреплённым snapshots; текущие строки
проверяются только для завершаемых игроков. Более поздний capture другого
игрока из того же bundle не отменяет доказательство завершаемого игрока.
Native readback использует исходные player/batch refs, включая разные batches
одного cached bundle. Два ограниченных прохода по последним 64 candidates могут
собрать разные актуальные successor capture для отдельных игроков.

Старый native partial snapshot, raw, исходный journal и anchors сохраняются.
Под общей блокировкой публикуется
неизменяемый checksum resolution для точного original intent SHA.
Pending scan исключает только journal с соответствующим terminal resolution.

Отсутствующий complete archive/index, повреждённое доказательство, частичный
новый capture или непокрытый игрок оставляют видимую ошибку и блокировку
reconciliation. Код не выдаёт непроверенную запись за успех. Ограниченный поиск
не заменяет доказательство. Нет удаления старого journal и повторной покупки
старого поколения. Успешное retirement освобождает будущую работу того же scope,
включая игроков, не входивших в старый partial job.

## Свежесть будущей current-работы

Архив может доказать supersession старого job даже через 49 часов. Это разрешает
только завершить старую failure reconciliation. Новый current job получает
cached capture лишь при возрасте raw меньше `min(configured career TTL, 24h)`
и совпадении существующего HTTP cache generation: scope, endpoint, player,
signature, first-detected clock и source-mismatch nonce. Исходные raw
`fetched_at`, signal clocks и lineage сохраняются. Squad TTL 48h не меняется.

Просроченное старое поколение откладывается с точным остатком и нулём HTTP;
независимые игроки продолжают обычную работу. Действительно новое наблюдаемое
поколение проходит обычный путь с прежними caps. При отсутствии исходного
поколения код консервативно не покупает старую работу повторно.

Новая допустимая cached работа создаёт собственный настоящий manifest и
проверенную legacy-проекцию. Native строки остаются исходными. Старый terminal
job остаётся failed. Если такая новая работа уже сохранила intent и упала,
reconciliation может закончить её без нового HTTP после истечения TTL; это
подтверждение исходной работы, а не свежий cache для нового поколения.

## Блокировка и обслуживание: условия доставки

`TM_WRITER_LOCK_PATH` должен указывать на один канонический host-файл,
bind-mounted во все TM и maintenance процессы. Раздельные Airflow logs volumes
не удовлетворяют контракту. `TM_WRITER_LOCK_SHARED_FILE_ID` содержит ожидаемый
`st_dev:st_ino` этого файла в контейнерах. Перед вводом необходимо проверить
совпадение фактической пары во всех процессах. Неверная пара запрещает TM
maintenance; отсутствие доказанного общего файла пропускает TM maintenance.
Это не доказательство того, что действующие mounts уже настроены.

Обычная ограниченная compaction остаётся обязательным условием эксплуатационной
готовности и пока требует доставки и приёмки. Защита snapshots касается только
четырёх TM career tables с refs или anchors; thresholds остальных источников,
Silver и Gold не меняются. Общий maintenance не должен expire защищённые
snapshots или optimize TM без проверенной общей блокировки. Нельзя считать
защиту достаточной только потому, что отдельный TM helper не вызывает expiry:
нужно учитывать общий weekly maintenance.

Удаление retained snapshots, anchors, empty commit receipts, terminal journals, resolutions и complete
archives требует отдельного явного lifecycle-контракта. Здесь не вводится TTL,
debt policy или автоматическая уборка этих доказательств. До такого контракта
они сохраняются; это условие последующей эксплуатации, а не выполненная
runtime-приёмка.
