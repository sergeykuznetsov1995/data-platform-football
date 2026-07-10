# =============================================================================
# Superset Configuration
# =============================================================================
# Mounted at /app/pythonpath/superset_config.py inside the container.
# Loaded automatically by Superset because /app/pythonpath is on PYTHONPATH.
# Reference: https://github.com/apache/superset/blob/master/docker/pythonpath_dev/superset_config.py
# =============================================================================

import os
from typing import Any
from cachelib.redis import RedisCache
from celery.schedules import crontab  # noqa: F401  (kept for users who add scheduled jobs)


# -----------------------------------------------------------------------------
# Core
# -----------------------------------------------------------------------------
SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

# DATABASE_URL is provided by docker-compose: postgresql+psycopg2://superset:...@postgres:5432/superset
SQLALCHEMY_DATABASE_URI = os.environ.get(
    "DATABASE_URL",
    "postgresql+psycopg2://superset:superset@postgres:5432/superset",
)

# Behind a reverse proxy / TLS terminator we will enable Talisman later.
TALISMAN_ENABLED = False

WTF_CSRF_ENABLED = True
# CSRF exemption for SQL Lab and dashboard JSON endpoints handled by Superset defaults.

# Row limits to keep dashboards responsive on a single-node setup.
ROW_LIMIT = 50000
SQL_MAX_ROW = 100000
SUPERSET_WEBSERVER_TIMEOUT = 300


# -----------------------------------------------------------------------------
# Redis (cache + Celery)
# -----------------------------------------------------------------------------
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
REDIS_DB = int(os.environ.get("REDIS_DB", 2))
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD", "")

_REDIS_AUTH = f":{REDIS_PASSWORD}@" if REDIS_PASSWORD else ""
REDIS_URL = f"redis://{_REDIS_AUTH}{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}"


CACHE_CONFIG: dict[str, Any] = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 60 * 60,  # 1 hour
    "CACHE_KEY_PREFIX": "superset_cache_",
    "CACHE_REDIS_HOST": REDIS_HOST,
    "CACHE_REDIS_PORT": REDIS_PORT,
    "CACHE_REDIS_DB": REDIS_DB,
    "CACHE_REDIS_PASSWORD": REDIS_PASSWORD or None,
}

DATA_CACHE_CONFIG: dict[str, Any] = {
    **CACHE_CONFIG,
    "CACHE_DEFAULT_TIMEOUT": 60 * 60 * 24,  # 24 hours for chart data
    "CACHE_KEY_PREFIX": "superset_data_cache_",
}

FILTER_STATE_CACHE_CONFIG: dict[str, Any] = {
    **CACHE_CONFIG,
    "CACHE_DEFAULT_TIMEOUT": 60 * 60 * 24 * 7,
    "CACHE_KEY_PREFIX": "superset_filter_cache_",
}

EXPLORE_FORM_DATA_CACHE_CONFIG: dict[str, Any] = {
    **CACHE_CONFIG,
    "CACHE_DEFAULT_TIMEOUT": 60 * 60 * 24 * 7,
    "CACHE_KEY_PREFIX": "superset_explore_form_cache_",
}


# -----------------------------------------------------------------------------
# Async query results (SQL Lab)
# -----------------------------------------------------------------------------
class CeleryConfig:
    broker_url = REDIS_URL
    result_backend = REDIS_URL
    imports = ("superset.sql_lab", "superset.tasks.scheduler")
    worker_prefetch_multiplier = 1
    task_acks_late = True
    task_annotations = {
        "sql_lab.get_sql_results": {"rate_limit": "100/s"},
    }
    beat_schedule: dict[str, Any] = {
        # No scheduled tasks by default; alerts/reports register their own schedule.
    }


CELERY_CONFIG = CeleryConfig

RESULTS_BACKEND = RedisCache(
    host=REDIS_HOST,
    port=REDIS_PORT,
    key_prefix="superset_results_",
    db=REDIS_DB,
    password=REDIS_PASSWORD or None,
)


# -----------------------------------------------------------------------------
# Feature flags
# -----------------------------------------------------------------------------
FEATURE_FLAGS: dict[str, bool] = {
    "DASHBOARD_NATIVE_FILTERS": True,
    "ALERT_REPORTS": True,
    "DASHBOARD_CROSS_FILTERS": True,
    "ENABLE_TEMPLATE_PROCESSING": True,
}


# -----------------------------------------------------------------------------
# SQL Lab
# -----------------------------------------------------------------------------
SQLLAB_TIMEOUT = 300
SQLLAB_ASYNC_TIME_LIMIT_SEC = 600


# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
ENABLE_TIME_ROTATE = True
LOG_LEVEL = os.environ.get("SUPERSET_LOG_LEVEL", "INFO")

# -----------------------------------------------------------------------------
# Session persistence — make login survive browser close (cookie gets Expires)
# Without this, Flask default = browser-session cookie → tab close = re-login.
# PERMANENT_SESSION_LIFETIME (Superset default = 7d) only applies when permanent.
# -----------------------------------------------------------------------------
def FLASK_APP_MUTATOR(app):
    from flask import session

    @app.before_request
    def _make_session_permanent() -> None:
        session.permanent = True

    # Заявка «Прошу доступ к данным» (viewers -> analysts). Пишет username+дату
    # в файл на volume superset_home; читает/обрабатывает scripts/promote.sh.
    from datetime import datetime, timezone

    from flask import redirect, request
    from flask_login import current_user
    from flask_wtf.csrf import generate_csrf
    from markupsafe import escape

    _REQUESTS_FILE = "/app/superset_home/access_requests.txt"

    @app.route("/access-request", methods=["GET", "POST"])
    def _access_request():
        if not current_user.is_authenticated:
            return redirect("/login/")
        if request.method == "POST":
            line = f"{current_user.username}\t{datetime.now(timezone.utc).isoformat(timespec='seconds')}\n"
            with open(_REQUESTS_FILE, "a", encoding="utf-8") as f:
                f.write(line)
            # уведомить админа в Telegram; заявка уже в файле, поэтому не критично
            _tok = os.environ.get("TELEGRAM_BOT_TOKEN")
            _chat = os.environ.get("TELEGRAM_CHAT_ID")
            if _tok and _chat:
                try:
                    import json as _json
                    import urllib.request as _rq

                    _rq.urlopen(_rq.Request(
                        f"https://api.telegram.org/bot{_tok}/sendMessage",
                        data=_json.dumps({
                            "chat_id": _chat,
                            "text": f"🔑 {current_user.username} просит доступ к данным.\nВыдать: scripts/promote.sh {current_user.username}",
                        }).encode(),
                        headers={"Content-Type": "application/json"},
                    ), timeout=5)
                except Exception:
                    app.logger.exception("access-request: telegram-уведомление не ушло")
            return "<p>Заявка отправлена — администратор выдаст доступ и уведомит вас.</p>"
        return (
            f"<p>Вы вошли как <b>{escape(current_user.username)}</b>.</p>"
            "<form method='post'>"
            f"<input type='hidden' name='csrf_token' value='{generate_csrf()}'>"
            "<button type='submit'>Прошу доступ к данным</button>"
            "</form>"
        )

    # Пункт меню на форму заявки — иначе юзеру её не найти. Gamma есть у всех
    # залогиненных (viewers/analysts), menu_access выдаём ей.
    try:
        app.appbuilder.add_link("Запросить доступ", href="/access-request")
        _sm = app.appbuilder.sm
        # update_perms у Superset выключен — право создаём сами (get-or-create).
        # Вешаем на отдельную роль viewer_menu: её раздаёт только маппинг группы
        # viewers ниже, поэтому у analysts пункт меню скрыт.
        _pv = _sm.add_permission_view_menu("menu_access", "Запросить доступ")
        _role = _sm.add_role("viewer_menu")
        if _pv and _role and _pv not in _role.permissions:
            _sm.add_permission_role(_role, _pv)
    except Exception:  # пункт меню не стоит падения Superset на старте
        app.logger.exception("access-request: не удалось добавить пункт меню")


# -----------------------------------------------------------------------------
# SSO через Keycloak (docs/design/analyst-access.md, фаза 7).
# SUPERSET_OAUTH_ENABLED=true  -> AUTH_OAUTH: analysts = Gamma+sql_lab+analyst_data,
#                                 platform-admins = Admin, авторегистрация
# иначе                        -> обычная форма логина (локальный admin)
# Break-glass: SUPERSET_OAUTH_ENABLED=false в .env + пересоздать superset.
# FAB >= 4.3 нативно знает провайдера keycloak (role_keys = groups из userinfo).
# -----------------------------------------------------------------------------
if os.environ.get("SUPERSET_OAUTH_ENABLED", "").lower() == "true":
    from flask_appbuilder.security.manager import AUTH_OAUTH

    _ISSUER = os.environ["OIDC_ISSUER"]

    AUTH_TYPE = AUTH_OAUTH
    OAUTH_PROVIDERS = [
        {
            "name": "keycloak",
            "icon": "fa-key",
            "token_key": "access_token",
            "remote_app": {
                "client_id": "superset",
                "client_secret": os.environ["SUPERSET_OIDC_CLIENT_SECRET"],
                # discovery обязателен: без jwks_uri authlib падает на id_token
                "server_metadata_url": f"{_ISSUER}/.well-known/openid-configuration",
                "api_base_url": f"{_ISSUER}/protocol/",
                "client_kwargs": {"scope": "openid email profile"},
            },
        }
    ]

    AUTH_USER_REGISTRATION = True
    AUTH_USER_REGISTRATION_ROLE = "Gamma"
    AUTH_ROLES_SYNC_AT_LOGIN = True
    AUTH_ROLES_MAPPING = {
        "analysts": ["Gamma", "sql_lab", "analyst_data"],
        # «Простые» юзеры: дашборды без SQL Lab (и без Jupyter/Airflow —
        # группа viewers не входит в allowed_groups/маппинги этих сервисов);
        # viewer_menu = пункт меню «Запросить доступ» (создаётся в FLASK_APP_MUTATOR)
        "viewers": ["Gamma", "analyst_data", "viewer_menu"],
        "platform-admins": ["Admin"],
    }

    # За Caddy: доверять X-Forwarded-*, иначе redirect_uri будет http://
    ENABLE_PROXY_FIX = True
