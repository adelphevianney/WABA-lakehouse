#!/usr/bin/env bash
# =============================================================================
# Requêtes analytiques du Level 1 (§1.4 de l'énoncé).
#
# Exécute contre Trino les requêtes de `sql/level1/` : soldes par pays, volumes
# de transactions, comptages par entité, et traçabilité de l'ingestion.
#
# Les fichiers sont montés dans le conteneur Trino et exécutés avec `-f` plutôt
# que passés en ligne de commande : le SQL ne traverse aucun shell, donc aucun
# échappement ne peut l'altérer.
#
# Usage : bash scripts/queries_l1.sh    (ou `make queries-l1`)
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")/.."

# Sous Git Bash, MSYS réécrit les chemins absolus passés aux conteneurs :
# /sql/level1/x.sql devient C:/Program Files/Git/sql/level1/x.sql et le client
# Trino ne trouve rien. La neutralisation est faite ici plutôt que laissée à
# l'appelant, qui n'a pas à connaître ce détail. Sans effet hors Windows.
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL='*'

COMPOSE=(docker compose --env-file .env -f docker/compose.yml)

for file in sql/level1/*.sql; do
  name=$(basename "$file")
  # La première ligne de commentaire de chaque fichier sert de titre.
  title=$(head -1 "$file" | sed 's/^-- *//')
  printf '\n\033[36m=== %s\033[0m\n\n' "$title"
  # `-f` produit du CSV par défaut, là où `--execute` produit un tableau
  # aligné : le format est demandé explicitement pour rester lisible.
  if ! "${COMPOSE[@]}" exec -T trino trino --no-progress --output-format ALIGNED \
       -f "/sql/level1/${name}" 2>/dev/null; then
    printf '\033[31mÉchec de %s\033[0m\n' "$name" >&2
    exit 1
  fi
done

printf '\n\033[32m%d requêtes analytiques exécutées\033[0m\n\n' "$(ls sql/level1/*.sql | wc -l)"
