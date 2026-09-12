# FBref — архив downstream backlog

Silver/Gold находятся вне [Bronze production scope](agreed-scope.md) и не блокируют его GO. Это сохранённый материал для следующих ревью, не активный план реализации. [Исторические issue states](board-audit.md) не являются итогом последующей консолидации карточек.

| Источник | Отложенное содержание |
|---|---|
| [#870](https://github.com/sergeykuznetsov1995/data-platform-football/issues/870) | Keeper-match Silver/Gold; SQL/metadata частично существуют, нужны свежие coverage и остаток. |
| [#903](https://github.com/sergeykuznetsov1995/data-platform-football/issues/903) | Gold, aliases/xref, canonical IDs и FK/cardinality/DQ. |
| [#916](https://github.com/sergeykuznetsov1995/data-platform-football/issues/916) | Silver missing-player-ID contract; Bronze сохраняет source row без выдуманного ID. |
| Части #945/#1023/#1130 | Silver DQ/витрины и cross-source канонизация. Bronze recovery, source identity и freshness остаются в scope. |

`dags/sql/silver/fbref_team_season_profile.sql` сокращает1888–1889 и1988–1989 до `8889`; оба сезона есть в comp9 registry. Natural keys/joins требуют полной source identity. Фактическая production потеря строк не установлена.

Downstream сохраняет source grain, perspective, lineage и unresolved rows. Ограниченный medallion allowlist не определяет полноту Bronze. Schedule↔events инвариант #901 остаётся Bronze DQ.
