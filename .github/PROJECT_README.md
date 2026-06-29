# Task tracker — GitHub Projects v2

Single source of truth для задач по платформе. Issues живут в `sergeykuznetsov1995/data-platform-football`, доска — Project v2 «Data Platform».

## One-time setup

### 1. Авторизовать `gh`

В Claude Code чате (или в обычном терминале):

```bash
gh auth login --web --scopes "project,repo"
```

Scope `project` обязателен — без него Projects v2 API недоступен.

Проверка:

```bash
gh auth status
gh api user -q .login   # должно вернуть sergeykuznetsov1995
```

### 2. Создать labels

```bash
cd /root/data_platform
bash scripts/setup_github_labels.sh
# или без auth — dry-run:
DRY_RUN=1 bash scripts/setup_github_labels.sh
```

Скрипт идемпотентен: повторный запуск обновит цвета/описания, ничего не удалит.

Проверка:

```bash
gh label list -R sergeykuznetsov1995/data-platform-football | wc -l   # ≈ 33
```

### 3. Создать Project + custom fields

```bash
bash scripts/setup_github_project.sh
```

Создаст Project v2 «Data Platform» и поля `Wave` (E0…E7) + `Priority` (P0…P3). Поле `Status` встроенное — расширить через UI (см. ниже).

### 4. Настроить Views в UI

Projects v2 views через CLI не создаются — заводим вручную через web UI проекта. Открыть https://github.com/users/sergeykuznetsov1995/projects/2 и в Settings → Fields → Status дописать опции `Backlog`, `In Review`, `Blocked` (по умолчанию там только Todo / In Progress / Done).

Затем создать 4 view'хи (+ значок “New view”):

| Имя view  | Layout | Group by | Filter             | Sort                |
|-----------|--------|----------|---------------------|---------------------|
| Board     | Board  | Status   | —                   | Priority ↓          |
| By Wave   | Table  | Wave     | `-status:Done`      | Priority ↓          |
| By Area   | Table  | Labels   | `label:area:*`      | Wave ↑, Priority ↓  |
| Backlog   | Table  | —        | `-status:Done`      | Priority ↓, Updated ↓ |

## Conventions

### Каждый issue должен иметь:

- ровно **1× `type:*`** (что это — bug/feat/refactor/docs/infra/dq)
- ровно **1× `area:*`** (где это — bronze/silver/gold/scraper/dag/bi/catalog/infra)
- опционально **1× `wave:*`** (E0…E7), если это медальон-задача
- опционально **1× `source:*`** (fbref/understat/whoscored/…), если касается одного источника
- опционально **1× `priority:p0`** / **`priority:p1`**, если горит (для P2/P3 используйте поле Priority в Project)
- опционально **`status:blocked`** / **`status:needs-triage`** как surface-level маркер

### PR title format

```
<type>(<area>): <imperative summary>

Пример:
fix(silver): close stints for relegated teams in dim_manager
feat(scraper): add FotMob deep stats per-category fetch
docs(catalog): seed OpenMetadata YAML for E4 facts
```

### Commit-to-issue linking

GitHub автоматически линкует упоминания `#<num>` в commit/PR body. Для авто-закрытия issue при merge используйте ключевые слова: `Closes #42`, `Fixes #42`.

### Иерархия

Большие эпики (типа «E1.5 cutover») → Issue с label `type:feat` + `wave:E1`, под него — sub-issues (`Tasks` в issue UI) c узкими scope. Project v2 умеет показывать sub-issue rollup в Table view.

## Maintenance

- **Labels**: редактируйте `.github/labels.yml` → `bash scripts/setup_github_labels.sh`. Удалять старые labels вручную (`gh label delete name -R …`) — скрипт сам не удаляет.
- **Project fields**: добавлять новые поля через UI или повторный запуск `setup_github_project.sh` (он идемпотентен; existing options не трогает).
- **Views**: версионировать не получится — это UI-state в БД GitHub. Если view случайно удалили — пересоздаём по таблице выше.

## Migration backlog

После заведения трекера — мигрировать known followups из memory в issues:

- R2-resolver multi-source spine bug (`feedback_player_resolver_v2_research.md`)
- E1.5 SofaScore xref_player cascade (`source='sofascore'` отсутствует в xref_player)
- E2 Phase 1.5 dim_manager: backfill для всех сезонов 2018–2024 (есть seed, не прогнали в проде)
- E3/E4 DQ-gate ≥3d green parity check before final cutover
- SoFIFA CF block unfreeze (заморожен до фикса soccerdata>=1.9.0)

(Список не исчерпывающий — добавляйте по мере того как достаёте из `~/.claude/projects/-root-data-platform/memory/`.)
