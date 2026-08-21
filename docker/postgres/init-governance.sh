#!/bin/bash
# Crée une base par service de gouvernance, au premier démarrage du serveur.
#
# Superset, Keycloak et OpenMetadata sont trois services à état. Leur donner à
# chacun son PostgreSQL coûterait près d'un gigaoctet sur une machine où la
# mémoire est la ressource contrainte ; leur donner la même base les ferait se
# marcher dessus au premier conflit de nom de table. Un serveur, trois bases :
# l'isolation logique suffit à cette échelle, et le Level 4 sur Kubernetes
# pourra les séparer sans changer une ligne d'application.
#
# Ce script n'est exécuté qu'à l'initialisation du volume : sur une base déjà
# créée, PostgreSQL l'ignore.
set -euo pipefail

for base in ${GOVERNANCE_DATABASES:-superset keycloak}; do
  echo "création de la base ${base}"
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-SQL
    CREATE DATABASE ${base};
    GRANT ALL PRIVILEGES ON DATABASE ${base} TO ${POSTGRES_USER};
SQL
done
