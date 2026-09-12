# FBref — архив аудита GitHub, 24.08–12.09.2026

Срез: **2026-09-12 10:10:15 UTC**, до консолидации доски. [Репозиторий](https://github.com/sergeykuznetsov1995/data-platform-football), [Project 2](https://github.com/users/sergeykuznetsov1995/projects/2).

На исходном срезе открыты **23 непосредственно FBref issues: 15 Todo, 5 In Progress, 2 Blocked, 1 In Review**. Все присутствовали на доске и не были архивированы. Последнее обновление любой из этих issue — 20 августа. Статусы отстают от кода и не доказывают, что все старые дефекты продолжают воспроизводиться.

Это архив состояния и классификации. Новый эпик и закрытие старых карточек относятся к последующему board reset; их точный scope определяется свежей сверкой. Закрытие при консолидации переносит работу, а не подтверждает production acceptance.

## Полнота проверки

Проверены 601/601 карточка Project, 7 страниц с `hasNextPage=false` на последней. Поиск `fbref` вернул 281 issue и 295 PR; лимит 1000 не достигнут. Прямая группа по заголовку или `source:fbref`: 110 issues, 106 PR. Все открытые прямые issues и страницы комментариев перечитаны отдельно. Широкие упоминания других источников не включались автоматически.

Для пяти merged PR периода проверены 30 workflow runs: 18 success, 6 failure, 6 cancelled; логи всех 6 failure runs прочитаны. Финальный head каждого PR имеет 9 SUCCESS checks. У всех пяти пустой `closingIssuesReferences`; GitHub conversation/inline/submitted reviews отсутствуют. Упоминания независимого ревью в описаниях PR не заменяют опубликованный отчёт ревью. Board audit сам не проверял runtime/БД или текущее содержимое FBref.

## Выполненные августовские изменения

| PR / merge UTC | Содержание merged кода | Неподтверждённый остаток |
|---|---|---|
| [#1220](https://github.com/sergeykuznetsov1995/data-platform-football/pull/1220), 25.08 13:00 | Current/history recovery isolation, ограничение historical raw adoption, target-local 404/oversize, непубликующая история по умолчанию. Merge `04b5663362d7`. | Доставка, адресные remediations, свежая current/history acceptance. |
| [#1221](https://github.com/sergeykuznetsov1995/data-platform-football/pull/1221), 25.08 18:13 | Два подтверждённых redirect aliases и ограниченная canary. Merge `5b8342bdc942`. | Фактическое применение и source results. |
| [#1222](https://github.com/sergeykuznetsov1995/data-platform-football/pull/1222), 25.08 19:34 | Current cap 80→16, scope repair после волн, исправление false frontier-closed, сохранение settlement. Merge `f1de514aa374`. | Новый измеренный профиль времени и SLA. Старые 96,5 pages/hour не сегодняшняя скорость. |
| [#1224](https://github.com/sergeykuznetsov1995/data-platform-football/pull/1224), 26.08 11:46 | Source-advertised current season, bounded false-current remediation, отдельный season_stats body limit и response evidence. Merge `e85552682934`. | Доставка, current identity acceptance, exact oversized cohort и отдельная история. |
| [#1226](https://github.com/sergeykuznetsov1995/data-platform-football/pull/1226), 26.08 19:57 | Advertised URL разрешается до label-derived season ID; advertised/resolved identities разделены. Merge `c958ea8a6d11`. | В PR подтверждено read-only evidence 117/117; deploy, SQL mutation и live trigger в рамках PR не выполнялись. |

Старый исследовательский [PR #611](https://github.com/sergeykuznetsov1995/data-platform-football/pull/611), обновлённый 16.06, находится вне окна и не является новым исправлением production-блокера.

## Исправленные CI-регрессии

| Историческая причина | Исправляющий commit | Финальное подтверждение |
|---|---|---|
| #1220: устаревшее shared runtime evidence | `293b598a4c58` | [Unit](https://github.com/sergeykuznetsov1995/data-platform-football/actions/runs/32843840028), [FBref](https://github.com/sergeykuznetsov1995/data-platform-football/actions/runs/32843840122) success |
| #1222: topology test ожидал cap80 вместо16 | `157e3c76512f` | [Unit](https://github.com/sergeykuznetsov1995/data-platform-football/actions/runs/32882831631), [FBref](https://github.com/sergeykuznetsov1995/data-platform-football/actions/runs/32882831657) success |
| #1226: source hash SQL attestation и runtime evidence не обновлены с кодом | `f1c969a84358`, `89c6ac91e018` | [Unit](https://github.com/sergeykuznetsov1995/data-platform-football/actions/runs/33004697687), [FBref](https://github.com/sergeykuznetsov1995/data-platform-football/actions/runs/33004697697) success |

Эти шесть failures не являются открытыми дефектами без нового воспроизведения. Merge и зелёный CI отдельно от доставки, восстановления данных и production GO.

## Все 23 прямые карточки исходного среза

Priority ниже — поле доски; «—» означает незаполненное поле. Остаток — предмет следующего ревью, а не утверждение, что старый кодовый дефект ещё существует.

| Issue | Status / Priority | Updated | Содержание остатка |
|---|---|---|---|
| [#870](https://github.com/sergeykuznetsov1995/data-platform-football/issues/870) | Todo / — | 03.07 | Keeper-match Silver/Gold; проверить уже существующие SQL/metadata. Вне Bronze GO. |
| [#901](https://github.com/sergeykuznetsov1995/data-platform-football/issues/901) | Todo / — | 08.07 | Bronze schedule↔events DQ по `match_id/team_side`; старые 485/2408 ошибочны, последующие 1/11 тоже исторические. |
| [#903](https://github.com/sergeykuznetsov1995/data-platform-football/issues/903) | Todo / — | 08.07 | Gold/xref/aliases и FK/DQ; downstream. |
| [#916](https://github.com/sergeykuznetsov1995/data-platform-football/issues/916) | Todo / — | 08.07 | Silver missing-player-ID contract; свежий остаток не измерен. Bronze сохраняет исходную строку без выдуманного ID. |
| [#923](https://github.com/sergeykuznetsov1995/data-platform-football/issues/923) | Todo / — | 06.08 | Полное discovery, raw/generic/typed coverage и availability. Текущий scope — все мужские турниры. |
| [#945](https://github.com/sergeykuznetsov1995/data-platform-football/issues/945) | In Progress / P1 | 13.08 | История/recovery/completeness/concurrency; сверка уже merged изменений и фактической приёмки. |
| [#949](https://github.com/sergeykuznetsov1995/data-platform-football/issues/949) | In Progress / — | 22.07 | Самостоятельная Bronze acceptance: точный release, current, offline replay, history, итоговый GO. |
| [#1023](https://github.com/sergeykuznetsov1995/data-platform-football/issues/1023) | Todo / — | 27.07 | Стабильные source competition/season identities в Bronze; cross-source canonical joins отдельно. |
| [#1062](https://github.com/sergeykuznetsov1995/data-platform-football/issues/1062) | Todo / P2 | 31.07 | Historical/completed policy не возвращается в daily при повторном discovery. |
| [#1111](https://github.com/sergeykuznetsov1995/data-platform-football/issues/1111) | Todo / — | 04.08 | Watchdog уже есть; остаются durable dead-letter counters и coverage consequence. |
| [#1115](https://github.com/sergeykuznetsov1995/data-platform-football/issues/1115) | Todo / — | 04.08 | Воспроизводимая проверка same-size rewrite/restored-mtime; единичный green run недостаточен. |
| [#1129](https://github.com/sergeykuznetsov1995/data-platform-football/issues/1129) | Blocked / P1 | 06.08 | По последнему комментарию кодовые причины исправлены; сверить доставку и schedule gaps. |
| [#1130](https://github.com/sergeykuznetsov1995/data-platform-football/issues/1130) | In Progress / P1 | 12.08 | Плановый current и свежий Bronze outcome; Silver не критерий текущего GO. |
| [#1131](https://github.com/sergeykuznetsov1995/data-platform-football/issues/1131) | Todo / P1 | 20.08 | Seasonless URL допустимы; проверяется source-advertised/resolved identity и freshness. |
| [#1133](https://github.com/sergeykuznetsov1995/data-platform-football/issues/1133) | Blocked / P1 | 06.08 | Ownership rollover исправлен в #1135 по комментарию; проверить rollout и comp23 remediation. |
| [#1141](https://github.com/sergeykuznetsov1995/data-platform-football/issues/1141) | Todo / — | 06.08 | Новый профиль fetch/parse/control/write после cap/repair изменений. |
| [#1145](https://github.com/sergeykuznetsov1995/data-platform-football/issues/1145) | In Progress / — | 11.08 | Bronze writes, recovery, admission, throughput, discovery и полнота. |
| [#1167](https://github.com/sergeykuznetsov1995/data-platform-football/issues/1167) | Todo / — | 12.08 | Source-advertised editions сборных: comps2–7,657,664,665,678; проверить после #1224/#1226. |
| [#1186](https://github.com/sergeykuznetsov1995/data-platform-football/issues/1186) | In Progress / P1 | 17.08 | Stale raw adoption/false terminal outcomes; #1220 адресует класс, нужны свежие cohorts/evidence. |
| [#1188](https://github.com/sergeykuznetsov1995/data-platform-football/issues/1188) | In Review / P1 | 18.08 | Актуальный transport/lease/clearance остаток и устойчивая серия продуктивных runs. |
| [#1192](https://github.com/sergeykuznetsov1995/data-platform-football/issues/1192) | Todo / — | 19.08 | Match-backed supercups, comp122/2026; сначала подтвердить наличие у источника. |
| [#1206](https://github.com/sergeykuznetsov1995/data-platform-football/issues/1206) | Todo / — | 20.08 | Moved/301/308 atomic defer/backoff без starvation матчей. |
| [#1207](https://github.com/sergeykuznetsov1995/data-platform-football/issues/1207) | Todo / — | 20.08 | Run cap/retry с сохранением aggregate и без бесконечного повторения moved targets. |

## Уточнения, которые нельзя потерять

Из 23 карточек 17 непосредственно Bronze, 3 смешанные (#945/#1023/#1130), 3 преимущественно downstream (#870/#903/#916). Это классификация области, не закрытие требований. #901 остаётся Bronze DQ: исходные таблицы — `bronze.fbref_schedule` и `bronze.fbref_match_events`. #1023 сохраняет Bronze source identity независимо от дальнейшей канонизации.

[Комментарий #1131 от 20.08](https://github.com/sergeykuznetsov1995/data-platform-football/issues/1131#issuecomment-5359230463) отменяет требование даты в current URL. [Комментарий #901 от 08.07](https://github.com/sergeykuznetsov1995/data-platform-football/issues/901#issuecomment-4916234361) исправляет масштаб после правильного `team_side` join. [Комментарий #1111](https://github.com/sergeykuznetsov1995/data-platform-football/issues/1111#issuecomment-5184193081) подтверждает watchdog. У #870/#901/#903/#916 labels Priority не совпадали с пустым полем доски.

Общие зависимости исходного аудита: [#1090](https://github.com/sergeykuznetsov1995/data-platform-football/issues/1090) — source scope; [#1058](https://github.com/sergeykuznetsov1995/data-platform-football/issues/1058)/[#1140](https://github.com/sergeykuznetsov1995/data-platform-football/issues/1140) — coverage denominator; [#1142](https://github.com/sergeykuznetsov1995/data-platform-football/issues/1142) — эксплуатационные требования; [#1155](https://github.com/sergeykuznetsov1995/data-platform-football/issues/1155) — воспроизводимый runtime; [#708](https://github.com/sergeykuznetsov1995/data-platform-football/issues/708) — отдельная историческая кампания. Они не включаются в reset автоматически по одному упоминанию FBref.

В фактическом current DAG `trigger_silver_transform` ожидает child, а release publication lock идёт после него. Bronze boundary требует самостоятельного durable completion/snapshot и безопасного release с отдельным downstream outcome. Простое `publish=false` не доказывает такую реализацию. [Согласованный scope](agreed-scope.md) исключает Silver readiness и proxy budget из Bronze GO; 7-дневный soak ещё не начат.
