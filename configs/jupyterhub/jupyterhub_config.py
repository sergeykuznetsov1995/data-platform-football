# JupyterHub для аналитиков (docs/design/analyst-access.md, фаза 5).
# Вход через Keycloak (группа analysts), у каждого юзера свой контейнер
# (2G/2cpu) в изолированной сети dp-analyst с персональным томом.
import os
import sys

c = get_config()  # noqa: F821

TS_HOSTNAME = os.environ["TS_HOSTNAME"]
ISSUER = os.environ["OIDC_ISSUER"]

# --- Сеть/база -------------------------------------------------------------
c.JupyterHub.bind_url = "http://0.0.0.0:8000"
c.JupyterHub.hub_ip = "0.0.0.0"
# Имя, по которому контейнеры юзеров находят hub API (docker DNS в dp-analyst)
c.JupyterHub.hub_connect_ip = "jupyterhub"
c.JupyterHub.db_url = "sqlite:////srv/jupyterhub/jupyterhub.sqlite"
c.JupyterHub.cookie_secret_file = "/srv/jupyterhub/jupyterhub_cookie_secret"
# Защита RAM: не больше 6 одновременно работающих ноутбуков (6 x 2G)
c.JupyterHub.active_server_limit = 6

# --- Вход через Keycloak -----------------------------------------------------
from oauthenticator.generic import GenericOAuthenticator  # noqa: E402

c.JupyterHub.authenticator_class = GenericOAuthenticator
c.GenericOAuthenticator.client_id = "jupyterhub"
c.GenericOAuthenticator.client_secret = os.environ["JUPYTERHUB_OIDC_CLIENT_SECRET"]
c.GenericOAuthenticator.oauth_callback_url = f"https://{TS_HOSTNAME}/hub/oauth_callback"
c.GenericOAuthenticator.authorize_url = f"{ISSUER}/protocol/openid-connect/auth"
c.GenericOAuthenticator.token_url = f"{ISSUER}/protocol/openid-connect/token"
c.GenericOAuthenticator.userdata_url = f"{ISSUER}/protocol/openid-connect/userinfo"
c.GenericOAuthenticator.username_claim = "preferred_username"
c.GenericOAuthenticator.scope = ["openid", "profile", "email"]
# Группы из claim groups: пускаем только аналитиков и админов платформы
c.GenericOAuthenticator.manage_groups = True
c.GenericOAuthenticator.claim_groups_key = "groups"
c.GenericOAuthenticator.allowed_groups = {"analysts", "platform-admins"}
c.GenericOAuthenticator.admin_groups = {"platform-admins"}

# --- Контейнеры юзеров -------------------------------------------------------
from dockerspawner import DockerSpawner  # noqa: E402

c.JupyterHub.spawner_class = DockerSpawner
c.DockerSpawner.image = "data-platform/jupyter-singleuser:latest"
c.DockerSpawner.network_name = "dp-analyst"
c.DockerSpawner.remove = True
c.DockerSpawner.mem_limit = "2G"
c.DockerSpawner.cpu_limit = 2
c.DockerSpawner.notebook_dir = "/home/jovyan/work"
c.DockerSpawner.volumes = {"jupyterhub-user-{username}": "/home/jovyan/work"}
# Подключение к Trino из ноутбуков: через Caddy (доверенный серт), общий
# read-only аккаунт analyst_svc. Персональный вариант — OAuth2Authentication()
# (см. docs/ANALYST_ONBOARDING.md).
c.DockerSpawner.environment = {
    "TRINO_HOST": TS_HOSTNAME,
    "TRINO_PORT": "8444",
    "TRINO_USER": "analyst_svc",
    "TRINO_PASSWORD": os.environ["TRINO_ANALYST_SVC_PASSWORD"],
}

# --- Idle-culler: гасим ноутбуки без активности час --------------------------
c.JupyterHub.load_roles = [
    {
        "name": "idle-culler-role",
        "scopes": ["list:users", "read:users:activity", "read:servers", "delete:servers"],
        "services": ["idle-culler"],
    }
]
c.JupyterHub.services = [
    {
        "name": "idle-culler",
        "command": [sys.executable, "-m", "jupyterhub_idle_culler", "--timeout=3600"],
    }
]
