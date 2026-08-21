"""Configuration Superset de la plateforme WABA (Level 4.2).

Aucun secret n'est écrit ici : la clé de signature et l'URI de la base de
métadonnées viennent de l'environnement, comme partout ailleurs dans le projet.

Superset est un service à état — il conserve ses tableaux de bord, ses sources
de données et ses utilisateurs dans une base relationnelle. Celle-ci est un
PostgreSQL dédié à la gouvernance, partagé avec Keycloak : la base SQLite par
défaut ne survivrait pas à une exécution concurrente du serveur web et de son
ordonnanceur, et perdrait les tableaux de bord au premier redémarrage.
"""

from __future__ import annotations

import os

# =============================================================================
# Sécurité
# =============================================================================

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

SQLALCHEMY_DATABASE_URI = os.environ["SUPERSET_DATABASE_URI"]

# Superset refuse par défaut d'exécuter du SQL saisi par l'utilisateur sur une
# source qui ne l'autorise pas explicitement. Le laboratoire SQL est utile à la
# démonstration, et les sources sont déclarées en lecture seule.
SQLLAB_CTAS_NO_LIMIT = False

# En développement local, le trafic n'est pas chiffré : Talisman forcerait une
# redirection HTTPS vers un port qui n'écoute pas. Le Level 4 le réactive
# derrière l'Ingress, où le certificat est présenté par le contrôleur.
TALISMAN_ENABLED = False
WTF_CSRF_ENABLED = True
# L'import de tableaux de bord par l'API est une écriture ; sans cette
# exemption, le script d'amorçage se heurterait au jeton CSRF d'un formulaire
# qu'il ne remplit jamais.
WTF_CSRF_EXEMPT_LIST = ["superset.views.core.log", "superset.charts.data.api.data"]

# =============================================================================
# Exécution des requêtes
# =============================================================================

# Les tables Gold tiennent en quelques centaines de lignes ; les tables Silver
# en comptent des centaines de milliers. Le plafond protège le navigateur d'un
# `SELECT *` malencontreux sans gêner les agrégats.
ROW_LIMIT = 50_000
SAMPLES_ROW_LIMIT = 1_000
SQL_MAX_ROW = 100_000

# Trino peut prendre quelques secondes à planifier une requête sur une table
# partitionnée ; le défaut de 30 s coupe des requêtes qui aboutiraient.
SUPERSET_WEBSERVER_TIMEOUT = 120

FEATURE_FLAGS = {
    # Permet de croiser plusieurs sources dans un même tableau de bord, ce dont
    # le tableau « Risque & Conformité » a besoin.
    "DASHBOARD_CROSS_FILTERS": True,
    "DRILL_TO_DETAIL": True,
    # L'import et l'export de tableaux de bord par fichier : c'est ce qui rend
    # les trois tableaux du §4.2 versionnables dans le dépôt plutôt que
    # reconstruits à la main à chaque déploiement.
    "VERSIONED_EXPORT": True,
}

# =============================================================================
# Cache
# =============================================================================
# Un cache mémoire simple suffit à cette échelle. Redis apporterait un partage
# entre plusieurs répliques du serveur web — utile en production, sans objet
# tant que Superset tourne en un exemplaire.

CACHE_CONFIG = {
    "CACHE_TYPE": "SimpleCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
}
DATA_CACHE_CONFIG = CACHE_CONFIG

# =============================================================================
# Présentation
# =============================================================================

APP_NAME = "WABA Group — Analytique Financière"
# Huit pays sur trois fuseaux : l'affichage suit la convention du reste de la
# plateforme, où tout est stocké et calculé en UTC.
DEFAULT_TIMEZONE = "UTC"
