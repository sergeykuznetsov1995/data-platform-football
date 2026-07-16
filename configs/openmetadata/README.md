# OpenMetadata — descriptions as code

This directory holds the metadata-as-code layer for the OpenMetadata catalog at
http://localhost:8585. YAML files in `descriptions/` are the source of truth
for table/column descriptions, tags, and relationships (FK pointers used to
build ER diagrams).

## Workflow

```
0) bootstrap       (this dir, one-time per OM instance) — creates Tier/Domain/PII/UseCase classifications + tags
1) ingest          (schema discovery from Trino)        — creates table entities in OM
2) apply           (this dir)                           — patches descriptions/tags/relationships
3) lineage         (Trino query history → edges)        — wires fct→dim → mart→fct
```

Cadence:
- `om-bootstrap`: one-time per OM instance (and after an approved metadata-database reinitialisation); idempotent, safe to re-run. Never use raw `docker compose down -v` on this platform.
- `om-ingest-trino`: ad-hoc after a schema change in Gold.
- `om-apply-descriptions`: after editing any YAML here (idempotent, safe to re-run).
- `om-lineage-trino`: nightly (Trino query history rolls up the latest day).

> Without `om-bootstrap`, `om-apply-descriptions` returns `PATCH HTTP 404: tag instance for Tier.Gold not found` on every YAML — the tag FQNs referenced in `tags:` must exist as real OM tags first.

## JWT setup

`apply_descriptions.py` authenticates via a bot JWT issued by OpenMetadata:

1. UI → `Settings → Bots → ingestion-bot` → copy the JWT token.
   (Or create a new bot under `Settings → Bots → Add Bot`.)
2. Export to your shell / `.env`:

   ```bash
   export OPENMETADATA_HOST=http://openmetadata-server:8585
   export OPENMETADATA_JWT_TOKEN='<paste token>'
   ```

3. One-time bootstrap of classifications/tags:

   ```bash
   make om-bootstrap   # creates Tier{Bronze,Silver,Gold} / Domain.Football / PII.{None,Low} / UseCase.ML
   ```

4. Apply YAML descriptions:

   ```bash
   make om-apply-descriptions  # runs apply_descriptions.py inside openmetadata-ingestion
   ```

`HTTP 404` on a table = not yet ingested → re-run `om-ingest-trino`.
`HTTP 404: tag instance for X not found` on every YAML = classifications not bootstrapped → run `make om-bootstrap`.
`HTTP 401` = bad / expired JWT → re-issue. Токен также инвалидируется ротацией
RSA-ключей (`make gen-om-jwt-keys`) — после неё перевыпустить и обновить
`OM_JWT_TOKEN` в `.env` + `./scripts/compose.sh --profile heavy up -d --no-deps openmetadata-ingestion`.

## SSO cutover (prod, #866)

С #866 люди входят в OM через Keycloak (`custom-oidc`, confidential client),
а бот-JWT ingestion-контура продолжает работать: провайдер логина и подпись
бот-токенов — независимые механизмы, пока в `OM_AUTH_PUBLIC_KEYS` остаётся
собственный JWKS OM (`http://localhost:8585/api/v1/system/config/jwks`).

> **Главный гоч.** OM 1.13 читает auth-конфиг **из своей БД**
> (`openmetadata_settings`), а env берёт только при первом старте с пустой
> таблицей. На живом сервере `up -d` с новыми `AUTHENTICATION_*` НИЧЕГО не
> переключит — в логе будет `Loaded security configuration from database -
> provider: basic`. Поэтому переключение делает `scripts/om_apply_security_config.py`
> (штатный `PUT /api/v1/system/security/config`, то же, что UI Settings →
> Security; применяется на лету, без рестарта). Env в compose остаётся источником
> истины для чистых стендов и для отката.
>
> **Мало того, при сидинге из env OM берёт НЕ ВСЁ.** Провайдер, issuer, claims,
> `adminPrincipals` — да, а вот **секрет OIDC-клиента и `responseType` — нет**.
> Поэтому «снести настройки в БД + пересоздать сервер» восстанавливает SSO-режим
> БЕЗ секрета, и вход падает на обмене кода:
> `TechnicalException: Bad token response, error=unauthorized_client`.
> Практический вывод: этот путь годится ТОЛЬКО для отката в basic. SSO-конфиг
> всегда применяется скриптом (по API). Если SSO уже включён, а basic-логин
> закрыт (403) — используй `scripts/om_reapply_sso.sh` (берёт Bearer админа-человека
> через временный password-grant на клиенте).
>
> **Перед любым пересидингом конфига сначала закрой meta в Caddy** (`respond 403`
> в блоке `meta` + `up -d --force-recreate caddy`): снос строк настроек заставляет
> OM пересоздать локального админа с паролем `admin/admin`, и на несколько минут
> опубликованный каталог оказывается открыт по дефолтным кредам. Пароль после
> пересидинга обязательно вернуть (см. п.2).
>
> **Вход в UI**: фронтенд OM смотрит на `responseType` (который OM нормализует в
> `id_token`) и запускает implicit-флоу — сырой JWT прилетает в адресную строку,
> и Chrome Safe Browsing помечает страницу как фишинговую. Рабочая ссылка —
> серверный вход `https://meta.<домен>/api/v1/auth/login`: он идёт
> authorization-code flow (`response_type=code`), токена в URL нет.

Порядок на живом проде (детали значений — секция «OpenMetadata SSO» в
`.env.example`):

1. **Клиент в живом Keycloak — только kcadm** (`--import-realm` существующий
   realm не обновляет; шаблон realm — для чистых стендов):

   ```bash
   docker exec -i keycloak /opt/keycloak/bin/kcadm.sh config credentials \
     --server http://localhost:8080 --realm master --user admin \
     --password "$KC_BOOTSTRAP_ADMIN_PASSWORD"
   docker exec -i keycloak /opt/keycloak/bin/kcadm.sh create clients -r football \
     -s clientId=openmetadata -s enabled=true -s publicClient=false \
     -s clientAuthenticatorType=client-secret -s "secret=$OPENMETADATA_OIDC_CLIENT_SECRET" \
     -s standardFlowEnabled=true -s directAccessGrantsEnabled=false \
     -s 'redirectUris=["https://meta.<домен>/callback"]' \
     -s 'webOrigins=["https://meta.<домен>"]' \
     -s 'defaultClientScopes=["profile","email","basic","groups"]'
   ```

2. Сменить пароль дефолтного `admin/admin` — это break-glass-аккаунт, он
   переживёт переключение (пароль хранится в БД и рестарт его не сбрасывает).
   Пароли в API: логин — base64, changePassword — plain:

   ```bash
   TOK=$(curl -s -X POST http://127.0.0.1:8585/api/v1/users/login \
     -H 'Content-Type: application/json' \
     -d "{\"email\":\"admin@open-metadata.org\",\"password\":\"$(printf admin | base64)\"}" \
     | python3 -c 'import sys,json;print(json.load(sys.stdin)["accessToken"])')
   curl -s -X PUT http://127.0.0.1:8585/api/v1/users/changePassword \
     -H "Authorization: Bearer $TOK" -H 'Content-Type: application/json' \
     -d '{"username":"admin","requestType":"SELF","oldPassword":"admin",
          "newPassword":"<новый>","confirmPassword":"<новый>"}'
   ```

3. `make gen-om-jwt-keys` + раскомментировать `OM_RSA_*`/`OM_JWT_*` в `.env`
   (свои ключи подписи бот-JWT вместо публично известных ключей образа) →
   `./scripts/compose.sh --profile heavy up -d --no-deps openmetadata-server`. Смена ключей
   инвалидирует старые бот-токены — перевыпуск в п.6.
4. Раскомментировать блок `OM_AUTH_*` + `OPENMETADATA_OIDC_CLIENT_SECRET`
   в `.env` (для чистых стендов и отката) и **применить конфиг на живом
   сервере** (иначе он останется basic — см. гоч выше):

   ```bash
   set -a; . ./.env; set +a
   OM_ADMIN_PASSWORD='<пароль из п.2>' python3 scripts/om_apply_security_config.py --dry-run
   OM_ADMIN_PASSWORD='<пароль из п.2>' python3 scripts/om_apply_security_config.py
   ```

5. Раскомментировать meta-блок в `configs/caddy/Caddyfile` →
   `./scripts/compose.sh --env-file .env exec caddy caddy reload --config /etc/caddy/Caddyfile`.
6. Первый вход админа из `OM_ADMIN_PRINCIPALS` через браузер; перевыпустить
   токен `ingestion-bot` (Settings → Bots) → `OM_JWT_TOKEN` в `.env` →
   `up -d openmetadata-ingestion` → `make om-ingest-trino && make om-lineage-trino`.

**Break-glass** (Keycloak лежит / OIDC сломался). Через API откатиться нельзя:
в SSO-режиме basic-логин отвечает 403, а Keycloak может быть недоступен.
Откат = снять конфиг из БД и дать compose пересоздать его из env-дефолтов
(они равны basic-режиму):

```bash
# 1) meta наружу больше не отдаём (basic-OM публиковать нельзя)
#    закомментировать блок meta в configs/caddy/Caddyfile, затем:
./scripts/compose.sh --env-file .env exec caddy \
  caddy reload --config /etc/caddy/Caddyfile
# 2) закомментировать блок OM_AUTH_* в .env
#    (OM_RSA_*/OM_JWT_*/OM_JWT_TOKEN НЕ трогать — на них живут бот-токены!)
# 3) снять сохранённый в БД конфиг и пересоздать сервер
./scripts/compose.sh --env-file .env exec -T postgres \
  psql -U openmetadata -d openmetadata -c \
  "DELETE FROM openmetadata_settings WHERE configtype IN
   ('authenticationConfiguration','authorizerConfiguration');"
./scripts/compose.sh --profile heavy up -d --no-deps --force-recreate openmetadata-server
```

Вход — локальный `admin` со СМЕНЁННЫМ паролем (п.2) через SSH-туннель
`127.0.0.1:8585`; ingestion работает всё это время без изменений (бот-JWT
не зависит от провайдера логина — проверено на стенде).

Заведение юзеров: self-signup выключен (`OM_AUTH_ENABLE_SELF_SIGNUP=false`) —
аналитика в OM создаёт админ (UI → Settings → Users → Add User, email =
email юзера в Keycloak), после чего тот входит через SSO. Админы задаются
списком `OM_ADMIN_PRINCIPALS` (KC-username; OM OSS не маппит группы в роли).

## YAML format

See [`descriptions/dim_referee.yaml`](descriptions/dim_referee.yaml) for the
canonical example. Schema:

```yaml
table:
  fullyQualifiedName: trino_iceberg.iceberg.gold.<table>
  description: |
    2-3 sentence summary (what the table is, source, grain).
  tags:
    - Tier.Gold
    - Domain.Football
    - PII.None
columns:
  - name: <pk_or_metric_col>
    description: One-line.
relationships:
  - from: trino_iceberg.iceberg.gold.<self>
    to:   trino_iceberg.iceberg.gold.<other>
    type: FOREIGN_KEY
    description: Optional human-readable join hint.
```

Only PK + key FK + 3-5 main metrics need explicit column descriptions; OM
keeps native column types from the ingested schema regardless.

### Season facts: document squad-sum semantics (#515)

The FBref season facts — [`descriptions/fct_player_season_stats.yaml`](descriptions/fct_player_season_stats.yaml)
and [`descriptions/fct_keeper_season_stats.yaml`](descriptions/fct_keeper_season_stats.yaml) —
aggregate a winter intra-league transfer across both clubs (#515, Variant B):
FBref counter columns are **summed across squads** for the season, `team_id`
comes from the max-minutes club, and ratios with a known formula
(`goals_per_shot`; keeper `save_pct` / `clean_sheet_pct` / `goals_against_per90`
/ `pk_save_pct`) are recomputed from the summed counters. When documenting a
column on these tables, note this on summed/recomputed columns so catalog users
read the totals correctly. FotMob / WhoScored columns are unaffected.

## Adding a new table

1. Drop a new `<table>.yaml` in `descriptions/`.
2. `make om-apply-descriptions` (idempotent — re-runs safely).
3. Refresh the table page in the OM UI.

Lineage edges (`relationships`) are best-effort: the `apply` script logs WARN
on 4xx and continues, since edge creation depends on entity IDs that may not
yet exist when the catalog is freshly bootstrapped — `om-lineage-trino` will
backfill from query history on the next nightly run.

## Removing dropped tables (stale lineage cleanup)

`om-ingest-trino` runs with `markDeletedTables: true`, so when a table disappears
from Trino its OpenMetadata entity is **soft-deleted**. In practice that leaves its
lineage edges in place — in particular the edges added **manually** by
`apply_descriptions.py` (`PUT /api/v1/lineage` from the `relationships:` blocks),
which ingestion's mark-deleted handling does not touch. So edges keep pointing
to/from the dropped table. This is issue #529 — surfaced when the derived gold tier
was dropped in epic #478, where edges like `feat_team_form → dim_team` and
`fct_match → dim_match` were still visible in the catalog.

Lineage edges are removed when the entity is **hard-deleted** with `recursive=true`
(a hard delete removes the entity together with all its relationship rows).
`cleanup_lineage.py` does exactly that for the dropped tables:

```bash
make om-cleanup-lineage          # DRY-RUN: prints the tables it would hard-delete
# review the list, then actually delete (entity + its lineage edges):
./scripts/compose.sh exec openmetadata-ingestion python /opt/configs/cleanup_lineage.py --apply
```

The script targets a curated list of the 19 derived-gold tables dropped in epic
#478 (idempotent — a table already gone is reported `ABSENT` and skipped). When a
future drop needs the same treatment, add its FQN to `DROPPED_TABLES` in the script.

This step is **deliberately manual, not wired into the nightly `om-lineage-trino`** —
hard-deleting a table that is only transiently missing from Trino would permanently
destroy its descriptions, tags, and lineage. Always review the dry-run list first.

> `entity_xref` is **not** cleaned here: it was never part of epic #478. Its own
> drop is the separate followup #211 (already absent from live gold per the Trino
> inventory in #475), so it is out of scope for this #478 cleanup.
