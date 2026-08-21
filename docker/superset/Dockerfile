# Image Superset de la plateforme WABA (Level 4.2).
#
# L'image officielle ne connaît pas Trino : le connecteur est un pilote
# SQLAlchemy tiers, et sans lui la source de données refuse d'être enregistrée.
# L'installer ici plutôt qu'au démarrage rend l'image autonome — Superset se
# lance sans accès réseau à PyPI, et sa reproductibilité ne dépend pas de la
# disponibilité d'un dépôt.
FROM apache/superset:4.1.1

USER root

# `trino` fournit le dialecte SQLAlchemy `trino://` ; `psycopg2-binary` est déjà
# présent dans l'image mais épinglé ici pour que la base de métadonnées ne
# dépende pas d'une résolution implicite.
RUN pip install --no-cache-dir \
        "trino[sqlalchemy]==0.330.0" \
        "psycopg2-binary==2.9.9"

COPY docker/superset/superset_config.py /app/pythonpath/superset_config.py

ENV PYTHONPATH=/app/pythonpath \
    SUPERSET_CONFIG_PATH=/app/pythonpath/superset_config.py

# L'image officielle s'exécute sous l'utilisateur `superset`.
USER superset
