# Superset 4.1.2 — закрывает CVE-2025-27696 / CVE-2025-48912 (было 4.1.1)
# Драйвер Trino — только trino[sqlalchemy]; sqlalchemy-trino НЕ ставить.
FROM apache/superset:4.1.2
USER root
# Версии запиннены (политика репозитория #758). redis НЕ ставим — он уже в базовом образе.
RUN pip install --no-cache-dir \
    "trino[sqlalchemy]==0.338.0" \
    "psycopg2-binary==2.9.12" \
    "Authlib==1.6.9"
USER superset
