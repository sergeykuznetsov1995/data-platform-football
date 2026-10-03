# ClubElo #1465: план доставки общего Iceberg writer

Статус 03.10.2026: подготовка PR и плана разрешена; **merge, доставка,
снятие паузы и запуск истории не разрешены**. Этот документ не запускает доставку.
Issue #1465 остаётся открытым до эксплуатационной приёмки.

## Изменение и совместимость

Единственная исполняемая правка — custom snapshot property `operation` →
`dpf.operation` в `replace_identity_partition_arrow_batches`. PyIceberg сам
передаёт `operation` в `Summary`, поэтому прежнее имя вызывает TypeError.
Стандартное свойство `operation=append` остаётся под управлением библиотеки.
Схемы таблиц, partition spec, сигнатура/результат writer, parser versions и
batch identity не меняются; миграция данных не нужна.
Проверки репозитория требуют согласованных generated artifacts: штатным
генератором обновлены только хеш writer в `runtime_contract.lock` и хеш этого
lock в трёх `whoscored-runtime-trust-root-*`. Это производные однострочной
правки, без изменения правил контракта. Они нужны для CI/будущей сборки образа,
а не для копирования поверх lock старого production-образа.

Потребители: `scrapers/clubelo/daily.py:replace_snapshot` (rating_date) и
`dags/scripts/whoscored_frozen_dq.py` (population_sha256). Читателей custom
значения `replace-identity-partition-batch` в репозитории не найдено.
Существующий открытый PR #953 также содержит writer со старым ключом, поэтому
его нельзя считать исправлением; при будущем обновлении той ветки сохранить
эту правку. Отдельного готового PR с переименованием на момент подготовки нет.

Регрессия `tests/unit/scrapers/test_iceberg_partition_transaction.py` использует
реальные PyIceberg 0.11.1 transaction/delete/append/scan, локальные SQLite и
Parquet. SQLAlchemy/greenlet добавлены только в `requirements-ci.txt` для
тестового каталога, зависимости production не меняются. [Каталог SQLite
поддерживается PyIceberg](https://py.iceberg.apache.org/configuration/#sql-catalog).
Проверяются varchar/date ClubElo и строковый ключ WhoScored, две порции данных,
один catalog commit, видимость старых данных до коммита, полная замена после,
соседний раздел, чтение прежнего snapshot и обе группы свойств summary.
Ошибки генератора, записи второго Parquet, отказ коммита, чужой partition и
пустой ввод сохраняют опубликованные metadata/snapshots/строки. Это откат
видимого состояния таблицы; удаление осиротевших файлов не обещается.
Lakekeeper/S3 и живой daily здесь не проверяются.

## Почему обычная доставка ClubElo недостаточна

- `deploy/clubelo/auto_deliver.sh` не доставляет `scrapers/base`; shared gate
  требует совпадения writer с master. После merge и до shared release этот
  gate остановит ClubElo и аналогичный автомат Understat. Не расширять
  allowlist и не добавлять writer в `ALLOWED_LAG` ради обхода.
- `deploy/whoscored/auto_deliver.sh` включает shared, но доставляет его в
  отдельный `/root/whoscored-1017-runtime/src`, а не в общий контур ClubElo.
  Merge может разрешить эту независимую ночную доставку; её надо учесть
  **до разрешения merge**, а не после.
- Готового entrypoint общего автоматического выпуска не найдено. Legacy
  `docs/operations/whoscored-production.md` описывает `blocked-v1`, а ready-v1
  promotion оставляет dormant. Его нельзя выдавать за готовый путь выката.

## Проверенная исходная точка (повторить перед доставкой)

Общий `airflow-scheduler`: project `data-platform`, working_dir
`/root/dpf-whoscored-merge`; `/opt/airflow/{dags,scrapers,scripts}` — mounts этого
дерева. Образ:
`127.0.0.1:5000/ws954/airflow-scheduler@sha256:73c644926e7540ac8f907d16f084c39a44f736b84b58e7027021bf2dc2dd20d3`.
Тот же каталог `scrapers` смонтирован у `airflow-webserver`, `proxy_filter`,
`fbref_proxy_filter`: их startup/import compatibility тоже входит в preflight.
Наличие mount само по себе не означает, что фильтры вызывают writer.
Фактические Compose-конфиги (четыре):

1. `/root/dpf-whoscored-merge/compose.yaml`;
2. `/root/whoscored-954-runtime/deploy-949/prod.override.yaml`;
3. `/root/stack-recovery/recovery.override.yaml`;
4. `/root/stack-recovery/proxy-filter-image-r.override.yaml`.

SHA256 writer до выпуска:
`5ab88ead5d77317f17dde11db8033d79501c6ebd0a48fd5b6eb60efc70ad24d3`;
после данной правки:
`a1d2d2dfa33b496ca891e0752560099ac0b99fef71146c67e4a1d7f09970e9c4`.
Живой `validate_runtime_contract` уже non-enforcing (`files={}`), однако
image-owned startup anchor продолжает проверять contract/lock. Их исходные
хеши соответственно:
`42407580ea2b84b8e3ff41840d5664cb14364d5a9e7b4eae9f9bd19da6946ab2`,
`d8ffb3af1994de1e048d1e9ff8a8cc0d86b86d3091b8733f19a3f2da590ac72e`.
Живые lock/trust roots не изменяются. Репозиторный lock после регенерации имеет
SHA256 `31c466df69cf6503a8ad15a9cbef4f775a4a3f4ed16dc2cbefe4062dab9766df`;
он отличается от живого и не должен ехать с writer в старый образ отдельно.
Старая запись writer в live lock сама по себе не доказывает, что проверка
writer сейчас активна; при этом генератор/проверки сборки требуют актуальных
артефактов в PR.

## Порядок отдельного общего выпуска

1. **До merge** согласовать общий release и его окно с владельцем. Подготовить
   отдельный автоматический entrypoint из интеграционного дерева и провести
   его ревью/репетицию на копии. Это необходимая следующая работа, не часть
   однострочной правки и не уже существующий механизм. Не заменять её ручным
   копированием в production. Входы автомата: утверждённый release SHA,
   manifest ровно одного runtime-файла, ожидаемые before/after SHA256,
   целевое дерево и rollback bundle. Он должен отказывать при любом дрейфе.
   Manifest также фиксирует images/mounts четырёх общих consumers, состояния
   пауз и import-error baseline всего общего DagBag; разрешён только writer delta.
2. На репетиции использовать точный образ и копию актуального общего дерева с
   новым writer. Проверить startup anchor в **новом процессе**, импорты DAG и
   контракты всех источников. Предпочтительный выпуск сохраняет image,
   contract и lock. Если это невозможно, остановить этот путь: нужен отдельно
   согласованный комплект дерево+образ с regen/build/attest/admission, а не
   ручная перепломбировка живого lock.
3. Автомат должен сериализоваться с доставками источников и обслуживанием,
   сохранить прежний файл/права/хеши вне наблюдаемых каталогов, вести журнал
   prepared/inflight/accepted/rolled-back и уметь восстановиться после сбоя.
   Проверить его откат на копии, включая обрыв записи и отказ postflight.
   Способ записи должен быть совместим с фактическими directory seals;
   нельзя создавать временные файлы внутри запертых каталогов вслепую.
   Репетиция должна доказать совместимость атомарной замены файла (смены inode)
   с startup/import boundary. Не подменять её неатомарной записью через `cat`.
4. Получить отдельное разрешение merge и выполнить обязательные CI-гейты.
   Зафиксировать merge SHA и точный payload SHA256; учесть WhoScored-автомат.
   Выпуск общего файла разрешается отдельным решением, merge его не заменяет.
5. Перед окном заново снять mounts/configs/image/hashes, состояния всех
   затронутых DAG и внешних сборщиков. Дождаться отсутствия running/queued
   потребителей writer и пересекающихся доставок; если окно не свободно —
   перенести. Не останавливать чужие процессы. Окно ClubElo
   **04:30–05:25 МСК** (01:30–02:25 UTC) не является общим разрешением;
   WhoScored **05:00–08:00 МСК** (02:00–05:00 UTC) пересекается с ним.
6. Выполнить проверенный shared entrypoint в согласованное окно. Никаких
   live Python/pytest, ручного редактирования production или общего
   `compose up`. Если согласованный способ требует recreate scheduler —
   только из интеграционного дерева, со всеми повторно проверенными
   конфигами, явным сервисом и `--no-deps`; не из feature worktree.
7. Postflight: writer равен payload, contract/lock/image и соседние файлы
   равны manifest, новый процесс проходит startup, scheduler healthy,
   метабаза не содержит новых import errors, DAG перечитаны после выпуска.
   Повторить shared comparison ClubElo/Understat без изменения исключений.
   `auto_deliver.sh --check` пишет служебное состояние/журнал: запускать его
   только как часть разрешённого delivery, не выдавать за read-only probe.
   Принятие общего выпуска фиксировать журналом; ClubElo остаётся paused.
8. При ошибке автомат возвращает сохранённый writer, проверяет before hash
   и те же postflight-гейты, оставляет ClubElo paused и сообщает о rollback.
   Возврат допустим только если текущий хеш всё ещё равен target: чужую правку
   не затирать, при дрейфе остановиться и сохранить доказательства.
   При смене образа/контракта откатывается весь согласованный комплект.
   ClubElo `--rollback` общего writer не восстанавливает. Откат кода не
   откатывает таблицы; destructive data rollback в этот план не входит.

Исполняемая команда общего выпуска появится в отдельно проверенном release
entrypoint. До его подготовки и утверждения доставка **не готова к запуску**;
этот документ задаёт конкретную последовательность и критерии его приёмки.

## Оставшаяся приёмка #1465

После отдельного разрешения владельца и подтверждённой доставки: снять паузу
по регламенту, адресно вернуть ClubElo в EXPECTED_RUNNING с новым backup,
получить успешный daily с записью полного rating_date и validate_data.
Проверить batch/raw, полноту и сохранность соседних дат через read-only SQL.
Затем выполнить историю в согласованное свободное окно и проверить manifest.
Зафиксировать **три успешных прогона нового кода**, паспорт и handoff.
Сейчас успешных новых прогонов 0; история не запускалась; #1465 не закрывать.
