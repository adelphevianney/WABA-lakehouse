#!/usr/bin/env bash
# =============================================================================
# Test de fumée du socle Level 1.
#
# Prouve la chaîne complète MinIO -> catalogue Iceberg REST -> Trino :
#   1. les trois buckets du sujet existent ;
#   2. Trino expose le catalogue `iceberg` ;
#   3. une table Iceberg partitionnée par country_code peut être créée,
#      alimentée et relue en SQL ;
#   4. les fichiers de données et de métadonnées atterrissent bien dans MinIO.
#
# Usage : bash scripts/smoke_l1.sh    (ou `make smoke-l1`)
#
# Note pour un test depuis Git Bash sous Windows : préfixer par
# `MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL='*'`, sans quoi MSYS réécrit les
# chemins absolus passés aux conteneurs (/data/... -> C:/Program Files/Git/...).
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")/.."

COMPOSE=(docker compose --env-file .env -f docker/compose.yml)

step() { printf '\n\033[36m==> %s\033[0m\n' "$1" >&2; }
ok()   { printf '\033[32m    OK  %s\033[0m\n' "$1" >&2; }
fail() { printf '\033[31m    KO  %s\033[0m\n' "$1" >&2; exit 1; }

# --output-format TSV rend la sortie directement analysable (pas de guillemets
# ni de cadre ASCII) ; --no-progress supprime les échappements de terminal.
# Le client Trino écrit sur stderr un avertissement jline et le type de chaque
# instruction DDL : on les masque, et on ne réaffiche stderr qu'en cas d'échec.
trino_sql() {
  local sql="$1" format="${2:-TSV}"
  if ! "${COMPOSE[@]}" exec -T trino trino --no-progress --output-format "$format" --execute "$sql" 2>/dev/null; then
    "${COMPOSE[@]}" exec -T trino trino --no-progress --execute "$sql" >&2 || true
    fail "échec de la requête : ${sql}"
  fi
}

# Exécute une commande mc dans un conteneur jetable. Les $VARIABLES sont
# volontairement échappées : elles sont résolues par le shell du conteneur, donc
# aucun credential ne transite par la ligne de commande de l'hôte.
mc_run() {
  "${COMPOSE[@]}" run --rm --no-deps minio-init \
    "mc alias set waba http://minio:9000 \"\$MINIO_ROOT_USER\" \"\$MINIO_ROOT_PASSWORD\" > /dev/null && $1" 2>/dev/null
}

# --- 1. Buckets MinIO --------------------------------------------------------
step "1/4 — Buckets MinIO"
for bucket in raw-landing lakehouse archive; do
  "${COMPOSE[@]}" exec -T minio test -d "/data/${bucket}" \
    && ok "bucket ${bucket}" || fail "bucket ${bucket} absent"
done

# --- 2. Catalogue Trino ------------------------------------------------------
step "2/4 — Catalogue Iceberg visible depuis Trino"
trino_sql "SHOW CATALOGS" | grep -qx iceberg \
  && ok "catalogue iceberg exposé" || fail "catalogue iceberg absent"

# --- 3. Aller-retour SQL sur une table Iceberg -------------------------------
step "3/4 — Création, écriture et lecture d'une table Iceberg partitionnée"
trino_sql "DROP TABLE IF EXISTS iceberg.smoke.ping" >/dev/null
trino_sql "CREATE SCHEMA IF NOT EXISTS iceberg.smoke" >/dev/null
trino_sql "CREATE TABLE iceberg.smoke.ping (
             country_code varchar,
             entity_type  varchar,
             amount       double,
             event_date   date
           ) WITH (partitioning = ARRAY['country_code'])" >/dev/null
ok "table créée et partitionnée par country_code"

trino_sql "INSERT INTO iceberg.smoke.ping VALUES
             ('CI','BANK',         125000.0, DATE '2026-01-15'),
             ('SN','INSURANCE',     48000.0, DATE '2026-01-15'),
             ('GH','MOBILE_MONEY',    900.0, DATE '2026-01-16')" >/dev/null
ok "3 lignes insérées"

echo
trino_sql "SELECT country_code, count(*) AS n, sum(amount) AS total_local
           FROM iceberg.smoke.ping GROUP BY 1 ORDER BY 1" ALIGNED

count=$(trino_sql "SELECT count(*) FROM iceberg.smoke.ping" | tr -dc '0-9')
[ "$count" = "3" ] && ok "relecture cohérente (3 lignes)" \
  || fail "attendu 3 lignes, obtenu '${count}'"

# --- 4. Persistance réelle dans MinIO ----------------------------------------
# La vérification passe par l'API S3 (mc) et non par le système de fichiers :
# c'est la vue qu'ont réellement Spark et Trino sur le stockage.
step "4/4 — Objets Iceberg présents dans le bucket lakehouse (API S3)"
objects=$(mc_run "mc ls --recursive waba/lakehouse")

echo "$objects" | grep -q '\.parquet' || fail "aucun objet Parquet dans le bucket lakehouse"
echo "$objects" | grep '\.parquet' | head -3 | awk '{print "    " $NF}'
ok "données Parquet écrites dans s3://lakehouse"

# Iceberg matérialise la clé de partition dans le chemin des objets.
echo "$objects" | grep -q 'country_code=' \
  && ok "partitionnement physique par country_code confirmé" \
  || fail "les objets ne sont pas partitionnés par country_code"

echo "$objects" | grep -q '\.metadata\.json' \
  && ok "métadonnées Iceberg (snapshots) écrites" \
  || fail "aucune métadonnée Iceberg trouvée"

# --- Nettoyage ----------------------------------------------------------------
trino_sql "DROP TABLE iceberg.smoke.ping" >/dev/null
trino_sql "DROP SCHEMA iceberg.smoke"     >/dev/null

printf '\n\033[32m*** Socle Level 1 opérationnel ***\033[0m\n\n'
