# Вход в Airflow (docs/design/analyst-access.md, фаза 6).
#
# AIRFLOW_OAUTH_ENABLED=true  -> SSO через Keycloak (группа analysts = Viewer,
#                                platform-admins = Admin, авторегистрация)
# иначе                       -> обычная форма логина (AUTH_DB, локальный админ)
#
# Break-glass: если Keycloak лежит, поставь AIRFLOW_OAUTH_ENABLED=false в .env
# и пересоздай ТОЛЬКО webserver: docker compose up -d --no-deps airflow-webserver
#
# Читает только webserver; scheduler этот файл игнорирует.
# FAB >= 4.3 нативно знает провайдера "keycloak": берёт preferred_username,
# given/family name, email и role_keys=groups из {api_base_url}openid-connect/userinfo.
import os

from flask_appbuilder.security.manager import AUTH_DB, AUTH_OAUTH

WTF_CSRF_ENABLED = True

if os.environ.get("AIRFLOW_OAUTH_ENABLED", "").lower() == "true":
    ISSUER = os.environ["OIDC_ISSUER"]

    AUTH_TYPE = AUTH_OAUTH
    OAUTH_PROVIDERS = [
        {
            "name": "keycloak",
            "icon": "fa-key",
            "token_key": "access_token",
            "remote_app": {
                "client_id": "airflow",
                "client_secret": os.environ["AIRFLOW_OIDC_CLIENT_SECRET"],
                # discovery: token/authorize/jwks_uri из metadata (без jwks_uri
                # authlib падает на валидации id_token: Missing "jwks_uri")
                "server_metadata_url": f"{ISSUER}/.well-known/openid-configuration",
                # FAB-провайдер keycloak дёргает {api_base_url}openid-connect/userinfo
                "api_base_url": f"{ISSUER}/protocol/",
                "client_kwargs": {"scope": "openid email profile"},
            },
        }
    ]

    # Первый вход = авторегистрация с ролью Viewer; роли пересинхронизируются
    # на каждом логине из groups-claim Keycloak.
    AUTH_USER_REGISTRATION = True
    AUTH_USER_REGISTRATION_ROLE = "Viewer"
    AUTH_ROLES_SYNC_AT_LOGIN = True
    AUTH_ROLES_MAPPING = {
        "analysts": ["Viewer"],
        "platform-admins": ["Admin"],
    }
else:
    AUTH_TYPE = AUTH_DB
