# Миграция на Headscale + публичные дашборды (гибрид)

> **⚠️ ДЕМОНТИРОВАНО 2026-07-07.** VPN-схема прожила 4 дня и снята:
> аналитикам оказалось слишком сложно (клиент, split-DNS, macOS-резолвер).
> Все сервисы теперь публичные за Keycloak SSO, OpenMetadata — только
> SSH-туннель; см. [ANALYST_ONBOARDING.md](ANALYST_ONBOARDING.md).
> Документ оставлен как история решения и справочник по гочам
> (bind-изоляция, MASQUERADE, split-DNS). Команды деинсталляции — в конце.

Что получится:

```
«Простой» юзер ── интернет ──► https://bi.<домен>   (Superset, вход через Keycloak)
Аналитик ──── Headscale VPN ──► jupyter/airflow/trino/meta.<домен>  (403 без VPN)
Keycloak ── https://auth.<домен> (публичный: на него редиректят все браузеры;
            /admin отдаётся только из VPN)
Headscale ── https://hs.<домен> (вход в VPN через тот же Keycloak-аккаунт)
```

Стоимость: домен (~$10/год). Tailscale-облако больше не нужно.
Машинные аккаунты и пайплайны не затрагиваются вообще (инвариант #860).

## 0. Предпосылки

1. Купить домен, далее `<домен>`.
2. DNS: A-записи `bi`, `auth`, `hs`, `jupyter`, `airflow`, `trino`, `meta`
   → публичный IP VM (все на публичный: это нужно Caddy для LE-сертификатов;
   доступ к VPN-именам всё равно режется по remote_ip).
3. В `.env` раскомментировать и заполнить блок «Гибрид» (см. `.env.example`):
   `PLATFORM_DOMAIN`, `PUBLIC_IP`, `HEADSCALE_OIDC_CLIENT_SECRET`,
   `COMPOSE_PROFILES=headscale`. Cutover-переключатели — ПОКА не заполнять.
4. Открыть на файрволе VM: tcp/80, tcp/443, udp/3478.

## 1. Подготовка (без даунтайма, старый Tailscale продолжает работать)

```bash
make render-headscale-config
# клиент headscale в живом realm (import существующий realm не обновляет):
./scripts/compose.sh --env-file .env exec -T keycloak /opt/keycloak/bin/kcadm.sh config credentials \
  --server http://localhost:8080 --realm master --user admin --password "$KC_BOOTSTRAP_ADMIN_PASSWORD"
./scripts/compose.sh --env-file .env exec -T keycloak /opt/keycloak/bin/kcadm.sh create clients -r football \
  -s clientId=headscale -s enabled=true -s publicClient=false \
  -s clientAuthenticatorType=client-secret -s "secret=$HEADSCALE_OIDC_CLIENT_SECRET" \
  -s standardFlowEnabled=true -s 'redirectUris=["https://hs.<домен>/oidc/callback"]' \
  -s 'defaultClientScopes=["profile","email","basic","groups"]'
./scripts/compose.sh --env-file .env --profile headscale up -d --no-deps --force-recreate headscale
./scripts/compose.sh --env-file .env up -d --no-deps --force-recreate caddy
# юзер платформы в headscale появится при первом OIDC-логине; для VM нужен свой:
./scripts/compose.sh --env-file .env exec headscale headscale users create platform
./scripts/compose.sh --env-file .env exec headscale headscale preauthkeys create --user platform --expiration 1h
```

Проверка: `https://hs.<домен>/health` отвечает из интернета с валидным сертом.

## 2. Cutover (окно ~30 мин; людям недоступно, пайплайны живут)

1. VM переходит в свой headscale (ts.net-имя и серт после этого умирают):

   ```bash
   tailscale logout
   tailscale up --login-server https://hs.<домен> --authkey <preauth-key из шага 1>
   tailscale ip -4    # новый 100.x-адрес VM
   ```

2. `.env`: `TS_IP=<новый 100.x>`, `TS_HOSTNAME=386844.tail.<домен>`,
   раскомментировать cutover-переключатели (`OIDC_ISSUER`, `KC_PUBLIC_URL`,
   `JUPYTER_PUBLIC_HOST`, `TRINO_PUBLIC_HOST`, `TRINO_PUBLIC_PORT=443`).
3. Split-DNS: в `configs/headscale/config.yaml.example` раскомментировать
   `extra_records` (jupyter/airflow/trino/meta → новый 100.x VM),
   `make render-headscale-config && ./scripts/compose.sh --env-file .env restart headscale`.
4. Caddy на гибридный роутинг:

   ```bash
   cp configs/caddy/Caddyfile.hybrid.example configs/caddy/Caddyfile
   ```

5. Redirect-URI клиентов в живом Keycloak (kcadm, по одному на клиента):
   - airflow    → `https://airflow.<домен>/oauth-authorized/keycloak`
   - superset   → `https://bi.<домен>/oauth-authorized/keycloak`
   - jupyterhub → `https://jupyter.<домен>/hub/oauth_callback`
   - trino      → `https://trino.<домен>/oauth2/callback`

   ```bash
   # id клиента: kcadm get clients -r football -q clientId=superset --fields id
   ./scripts/compose.sh --env-file .env exec -T keycloak /opt/keycloak/bin/kcadm.sh update clients/<id> -r football \
     -s 'redirectUris=["https://bi.<домен>/oauth-authorized/keycloak"]' \
     -s 'webOrigins=["https://bi.<домен>"]'
   ```

6. Пересоздать сервисы (порядок важен: сначала keycloak и caddy):

   ```bash
   ./scripts/compose.sh --env-file .env up -d --no-deps --force-recreate keycloak caddy
   ./scripts/compose.sh --env-file .env up -d --no-deps --force-recreate trino superset airflow-webserver jupyterhub
   ```

7. Проверки:
   - из интернета: `https://bi.<домен>` — логин-редирект на `auth.<домен>`, вход работает;
     `https://jupyter.<домен>` — 403;
   - из VPN (ноутбук аналитика): jupyter/airflow открываются, спавн ноутбука,
     SELECT из ноутбука; DBeaver: `jdbc:trino://trino.<домен>:443?SSL=true&externalAuthentication=true`;
   - пайплайны: один Airflow-таск зелёный, дашборд Superset живой.

## 3. Онбординг после миграции

- **Viewer**: `scripts/onboard_user.sh <username> <email>` → ссылка `https://bi.<домен>`.
  Всё. Никакого VPN.
- **Аналитик**: то же + группа `analysts`; ставит Tailscale-клиент и выполняет
  `tailscale up --login-server https://hs.<домен>` → браузер → тот же
  Keycloak-логин → нода зарегистрирована сама (OIDC, allowed_groups
  analysts/platform-admins). Приглашений и одобрений в админке больше нет.
- **Оффбординг**: disable в Keycloak + `headscale nodes list` /
  `headscale nodes delete <id>`.

## 4. Rollback (если что-то пошло не так)

1. `.env`: закомментировать cutover-переключатели обратно (Caddyfile:
   `git checkout configs/caddy/Caddyfile`).
2. VM обратно в Tailscale-облако: `tailscale logout && tailscale up`
   (аккаунт decoy10215400@gmail), `tailscale ip -4` → вернуть старые
   `TS_IP`/`TS_HOSTNAME` в `.env`.
3. Вернуть redirect-URI клиентов в Keycloak (kcadm, старые ts.net-значения).
4. `./scripts/compose.sh --env-file .env up -d --no-deps --force-recreate keycloak caddy trino superset airflow-webserver jupyterhub`.

Пайплайны всё это время работают: машинные аккаунты не зависят ни от VPN,
ни от issuer.

## 5. Гочи

- `--import-realm` НЕ обновляет существующий realm — клиент headscale и новые
  redirect-URI вносятся только kcadm/админкой (шаблон realm — для чистых стендов).
- После `tailscale logout` серт `*.ts.net` перестаёт продлеваться — возврата
  к старой схеме без шага rollback-2 нет.
- DERP требует валидного TLS на `hs.<домен>` — сначала шаг 1 (Caddy+серты),
  потом cutover.
- Контейнеры резолвят `auth.<домен>`/`trino.<домен>` в Caddy через
  network-алиасы (compose) — hairpin-NAT не нужен.
- Rate-limit/CrowdSec для публичных bi/auth — следующий шаг после миграции
  (Keycloak brute-force protection уже включён).

## 6. Деинсталляция (выполнена 2026-07-07)

Порядок: сначала веб переключён на публичную схему (Caddyfile один listener,
recreate caddy/keycloak/jupyterhub) и проверен, только потом снос VPN —
до этого момента откат был тривиален.

```bash
# 1. Контейнер и хостовый tailscale (VM была нодой своего же headscale)
docker stop headscale && docker rm headscale
tailscale down && systemctl disable --now tailscaled
apt-get purge -y tailscale && rm -rf /var/lib/tailscale
# 2. systemd-зависимость docker от tailscaled (drop-in)
rm /etc/systemd/system/docker.service.d/<drop-in>.conf && systemctl daemon-reload
# 3. Спустя rollback-окно (~неделя): ключи сервера headscale
docker volume rm headscale_data
```

На клиентах: удалить Tailscale-клиент; macOS — удалить
`/etc/resolver/sk-vpn-2026.uk` (см. ANALYST_ONBOARDING.md). DNS-записи
`hs.`/`meta.` в Cloudflare можно удалить (опционально, ни на что не влияют).
