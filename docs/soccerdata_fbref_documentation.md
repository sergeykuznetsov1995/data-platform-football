# SoccerData FBref API Documentation

## Обзор

Библиотека [soccerdata](https://github.com/probberechts/soccerdata) предоставляет Python API для сбора футбольной статистики с FBref.com.

## Инициализация

```python
import soccerdata as sd

# Создание экземпляра для конкретной лиги и сезона
fbref = sd.FBref('ENG-Premier League', '2021')

# Несколько лиг/сезонов
fbref = sd.FBref(['ENG-Premier League', 'ESP-La Liga'], ['2022', '2023'])
```

## Доступные методы

### Справочные данные
| Метод | Описание |
|-------|----------|
| `available_leagues()` | Список доступных лиг |
| `read_leagues()` | Информация о лигах |
| `read_seasons()` | Доступные сезоны для выбранных лиг |

### Командная статистика
| Метод | Описание |
|-------|----------|
| `read_team_season_stats(stat_type)` | Агрегированная статистика команд за сезон |
| `read_team_match_stats(stat_type)` | Статистика команд по матчам |

### Статистика игроков
| Метод | Описание |
|-------|----------|
| `read_player_season_stats(stat_type)` | Агрегированная статистика игроков за сезон |
| `read_player_match_stats(stat_type, match_id)` | Статистика игроков в конкретном матче |

### Матчевые данные
| Метод | Описание |
|-------|----------|
| `read_schedule()` | Расписание матчей и результаты |
| `read_lineup(match_id)` | Составы команд на матч |
| `read_events()` | События матчей (голы, карточки, замены) |
| `read_shot_events()` | Детальные данные об ударах с xG и координатами |

---

## Типы статистики (stat_type)

### Player Season Stats / Team Season Stats

| stat_type | Описание | Ключевые метрики |
|-----------|----------|-----------------|
| `"standard"` | Базовая статистика | Goals, Assists, Minutes, xG, xA, Cards |
| `"shooting"` | Удары | Shots, SoT, SoT%, xG, npxG, Distance |
| `"passing"` | Пасы | Completed, Attempted, %, Distance, Key Passes, xA |
| `"passing_types"` | Типы пасов | Live, Dead, FK, TB, Sw, Crs, TI, CK |
| `"goal_shot_creation"` | Создание моментов | SCA, SCA90, GCA, GCA90, Types |
| `"defense"` | Оборона | Tackles, Blocks, Interceptions, Clearances |
| `"possession"` | Владение | Touches, Take-ons, Carries, Progressive |
| `"playing_time"` | Игровое время | Starts, Subs, Minutes, Points/Match |
| `"misc"` | Прочее | Fouls, Aerials, Recoveries, Own Goals |
| `"keeper"` | Вратари базовая | Saves, GA, CS, PKs |
| `"keeper_adv"` | Вратари продвинутая | PSxG, Launch%, Crosses, Sweeper |

### Player Match Stats

| stat_type | Описание |
|-----------|----------|
| `"summary"` | Основная сводка матча |
| `"keepers"` | Вратарская статистика |
| `"passing"` | Пасы в матче |
| `"passing_types"` | Типы пасов |
| `"defense"` | Оборонительные действия |
| `"possession"` | Владение мячом |
| `"misc"` | Прочая статистика |

### Team Match Stats

| stat_type | Описание |
|-----------|----------|
| `"schedule"` | Расписание |
| `"shooting"` | Удары |
| `"keeper"` | Вратари |
| `"passing"` | Пасы |
| `"passing_types"` | Типы пасов |
| `"goal_shot_creation"` | Создание моментов |
| `"defense"` | Оборона |
| `"possession"` | Владение |
| `"misc"` | Прочее |

---

## Детальные метрики по stat_type

### standard (Базовая статистика)

**Идентификация:**
- `Player` - Имя игрока
- `Nation` - Страна
- `Pos` - Позиция (GK, DF, MF, FW)
- `Squad` - Команда
- `Age` - Возраст
- `Born` - Год рождения

**Игровое время:**
- `MP` - Matches Played
- `Starts` - Матчей в старте
- `Min` - Минуты
- `90s` - Полных 90-минуток

**Результативность:**
- `Gls` - Голы
- `Ast` - Ассисты
- `G+A` - Голы + Ассисты
- `G-PK` - Голы без пенальти
- `PK` - Пенальти забито
- `PKatt` - Пенальти попыток

**Карточки:**
- `CrdY` - Желтые карточки
- `CrdR` - Красные карточки

**Expected (xG):**
- `xG` - Expected Goals
- `npxG` - Non-Penalty xG
- `xAG` - Expected Assists

**Прогрессия:**
- `PrgC` - Progressive Carries
- `PrgP` - Progressive Passes
- `PrgR` - Progressive Passes Received

**Per 90:**
- `Gls/90`, `Ast/90`, `G+A/90`
- `xG/90`, `xAG/90`

### shooting (Удары)

- `Sh` - Удары всего
- `SoT` - Удары в створ
- `SoT%` - % ударов в створ
- `Sh/90` - Удары на 90 мин
- `SoT/90` - Удары в створ на 90 мин
- `G/Sh` - Голы на удар
- `G/SoT` - Голы на удар в створ
- `Dist` - Средняя дистанция удара
- `FK` - Удары со штрафных
- `xG`, `npxG`, `npxG/Sh`
- `G-xG` - Голы минус xG
- `np:G-xG` - Non-penalty: голы минус xG

### passing (Пасы)

**Общее:**
- `Cmp` - Пасы выполнены
- `Att` - Пасы попыток
- `Cmp%` - % точности
- `TotDist` - Общая дистанция пасов
- `PrgDist` - Прогрессивная дистанция

**По дистанции:**
- `Short Cmp/Att/%` - Короткие (5-15 ярдов)
- `Medium Cmp/Att/%` - Средние (15-30 ярдов)
- `Long Cmp/Att/%` - Длинные (30+ ярдов)

**Созидание:**
- `Ast` - Ассисты
- `xAG` - Expected Assists
- `xA` - xA (модель передач)
- `KP` - Key Passes
- `1/3` - Пасы в финальную треть
- `PPA` - Пасы в штрафную
- `CrsPA` - Кроссы в штрафную
- `PrgP` - Прогрессивные пасы

### passing_types (Типы пасов)

- `Live` - Пасы в игре
- `Dead` - Со стандартов
- `FK` - Штрафные
- `TB` - Пасы за спину
- `Sw` - Переводы (Switch)
- `Crs` - Кроссы
- `TI` - Вбрасывания
- `CK` - Угловые
- `In/Out/Str` - Угловые (типы)
- `Off` - Офсайды
- `Blocks` - Заблокированные

### goal_shot_creation (GCA)

**Shot Creating Actions:**
- `SCA` - Shot Creating Actions
- `SCA90` - SCA per 90
- `PassLive` - Живой пас перед ударом
- `PassDead` - Стандарт перед ударом
- `TO` - Take-on перед ударом
- `Sh` - Удар, приведший к еще удару
- `Fld` - Заработанный фол
- `Def` - Оборонительное действие

**Goal Creating Actions:**
- `GCA` - Goal Creating Actions
- `GCA90` - GCA per 90
- (те же типы что и SCA)

### defense (Оборона)

**Отборы:**
- `Tkl` - Tackles
- `TklW` - Tackles Won
- `Def 3rd/Mid 3rd/Att 3rd` - Отборы по зонам

**Dribblers:**
- `Tkl%` - % успешных отборов vs дриблеров
- `Att` - Попыток отобрать
- `Tkl%` - Успешность
- `Lost` - Проиграно дриблерам

**Блоки:**
- `Blocks` - Блоки всего
- `Sh` - Заблокированные удары
- `Pass` - Заблокированные пасы

**Прочее:**
- `Int` - Перехваты
- `Tkl+Int` - Отборы + Перехваты
- `Clr` - Выносы
- `Err` - Ошибки, приведшие к удару

### possession (Владение)

**Касания:**
- `Touches` - Всего касаний
- `Def Pen/Def 3rd/Mid 3rd/Att 3rd/Att Pen` - Касания по зонам
- `Live` - Живые касания

**Take-Ons (Обводки):**
- `Att` - Попыток обводки
- `Succ` - Успешных
- `Succ%` - % успеха
- `Tkld` - Отобрали мяч
- `Tkld%` - % отборов

**Ведение:**
- `Carries` - Ведений мяча
- `TotDist` - Дистанция ведения
- `PrgDist` - Прогрессивная дистанция
- `PrgC` - Прогрессивных ведений
- `1/3` - Ведений в финальную треть
- `CPA` - Ведений в штрафную
- `Mis` - Потеря контроля
- `Dis` - Отобрали при ведении

**Приём:**
- `Rec` - Передач принято
- `PrgR` - Прогрессивных передач принято

### playing_time (Игровое время)

**Основное:**
- `MP` - Матчей сыграно
- `Min` - Минут
- `Mn/MP` - Минут на матч
- `Min%` - % от возможных минут
- `90s` - 90-минуток

**Старты:**
- `Starts` - Старты
- `Mn/Start` - Минут на старт
- `Compl` - Полных матчей

**Замены:**
- `Subs` - Выходы на замену
- `Mn/Sub` - Минут на замену
- `unSub` - Неиспользованные замены

**Командные очки:**
- `PPM` - Points Per Match
- `onG` - Голы команды при игроке
- `onGA` - Пропущено при игроке
- `+/-` - Разница голов
- `+/-90` - Разница на 90 мин
- `On-Off` - Разница с/без игрока
- `onxG`, `onxGA` - xG при игроке
- `xG+/-`, `xG+/-90`

### misc (Прочее)

- `CrdY`, `CrdR` - Карточки
- `2CrdY` - Вторые желтые
- `Fls` - Фолы совершены
- `Fld` - Фолы заработаны
- `Off` - Офсайды
- `PKwon` - Пенальти заработаны
- `PKcon` - Пенальти в свои ворота
- `OG` - Автоголы
- `Recov` - Подборы мяча
- `Won/Lost/%` - Воздушные дуэли

### keeper (Вратари - базовая)

**Игровое время:**
- `MP`, `Starts`, `Min`, `90s`

**Результаты:**
- `GA` - Голы пропущены
- `GA90` - GA на 90 мин
- `SoTA` - Удары в створ против
- `Saves` - Сэйвы
- `Save%` - % сэйвов
- `W`, `D`, `L` - Победы/Ничьи/Поражения
- `CS` - Сухие матчи
- `CS%` - % сухих матчей

**Пенальти:**
- `PKatt` - Пенальти против
- `PKA` - Пропущено с пенальти
- `PKsv` - Отбито пенальти
- `PKm` - Пенальти мимо ворот

### keeper_adv (Вратари - продвинутая)

**Post-Shot xG:**
- `PSxG` - Post-Shot Expected Goals
- `PSxG/SoT` - PSxG на удар в створ
- `PSxG+/-` - PSxG минус пропущено
- `/90` - На 90 минут

**Выносы:**
- `Cmp` - Выносы выполнены
- `Att` - Выносы попыток
- `Cmp%` - % успешных
- `PassAtt` - Пасы вратаря
- `Thr` - Броски рукой
- `Launch%` - % выносов

**Игра ногами:**
- `AvgLen` - Средняя длина паса

**Навесы:**
- `Opp` - Кроссы в штрафную против
- `Stp` - Перехвачено
- `Stp%` - % перехватов
- `#OPA` - Выходы за штрафную
- `#OPA/90`
- `AvgDist` - Средняя дистанция выхода

---

## Примеры использования

```python
import soccerdata as sd

# Инициализация
fbref = sd.FBref('ENG-Premier League', '2023')

# Базовая статистика игроков
standard = fbref.read_player_season_stats(stat_type="standard")

# Статистика ударов
shooting = fbref.read_player_season_stats(stat_type="shooting")

# Статистика пасов
passing = fbref.read_player_season_stats(stat_type="passing")

# Статистика вратарей
keepers = fbref.read_player_season_stats(stat_type="keeper")
keepers_adv = fbref.read_player_season_stats(stat_type="keeper_adv")

# Статистика обороны
defense = fbref.read_player_season_stats(stat_type="defense")

# Создание моментов
gca = fbref.read_player_season_stats(stat_type="goal_shot_creation")

# Владение
possession = fbref.read_player_season_stats(stat_type="possession")

# Статистика игрока в конкретном матче
match_stats = fbref.read_player_match_stats(stat_type="passing", match_id='db261cb0')

# Детальные данные об ударах (с координатами)
shots = fbref.read_shot_events()

# События матчей (голы, карточки, замены)
events = fbref.read_events()
```

---

## Реализованные таблицы (scrapers/fbref_scraper.py)

Текущая реализация собирает **все 11 таблиц**:

| # | Таблица | Источник | Описание |
|---|---------|----------|----------|
| 1 | `fbref_schedule` | `read_schedule()` | Расписание и результаты матчей |
| 2 | `fbref_team_stats` | `read_team_season_stats("standard")` | Базовая статистика команд |
| 3 | `fbref_team_stats_extended` | Merge всех stat_types | Расширенная статистика команд |
| 4 | `fbref_player_stats` | `read_player_season_stats("standard")` | Базовая статистика игроков |
| 5 | `fbref_player_stats_extended` | Merge всех stat_types | Расширенная статистика (~100 колонок) |
| 6 | `fbref_keeper_stats` | `keeper` + `keeper_adv` | Статистика вратарей |
| 7 | `fbref_player_match_stats` | `read_player_match_stats()` | Статистика игроков по матчам |
| 8 | `fbref_shot_events` | `read_shot_events()` | События ударов с xG и координатами |
| 9 | `fbref_match_events` | `read_events()` | Голы, карточки, замены |
| 10 | `fbref_lineups` | `read_lineup()` | Составы команд |
| 11 | `fbref_team_match_stats` | `read_team_match_stats()` | Статистика команд по матчам |

### Поддерживаемые stat_types

**Player/Team Season Stats:** `standard`, `shooting`, `passing`, `passing_types`, `goal_shot_creation`, `defense`, `possession`, `playing_time`, `misc`

**Keeper Stats:** `keeper`, `keeper_adv`

### Особенности реализации

- Использует soccerdata library как основу
- Selenium с Cloudflare bypass для защищённых страниц
- FlareSolverr как альтернатива для bypass
- Rate limiting и proxy rotation для избежания блокировок
- Данные сохраняются в Bronze layer (Iceberg/Parquet)
