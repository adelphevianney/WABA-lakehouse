#!/usr/bin/env bash
# =============================================================================
# Test de fumée du Level 2 — médaillon et orchestration.
#
# Vérifie chaque critère de la grille d'évaluation du niveau :
#   0. le médaillon est construit depuis la couche brute ;
#   1. les trois zones du médaillon existent et contiennent des données ;
#   2. les transformations Silver satisfont les quatre exigences du §2.2 ;
#   3. les 7 tables Gold sont requêtables et filtrables par pays ;
#   4. les KPIs réglementaires tombent dans leurs fourchettes ;
#   5. le rapport réglementaire J+1 est produit ;
#   6. les 4 DAGs Airflow s'analysent sans erreur ;
#   7. aucun identifiant n'est codé en dur ;
#   8. recalculer Silver et Gold ne duplique rien.
#
# Prérequis : le Level 1 a été exécuté (`make smoke-l1`). Le test construit
# ensuite lui-même le médaillon avant de le vérifier.
#
# Usage : bash scripts/smoke_l2.sh    (ou `make smoke-l2`)
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")/.."

# Sous Git Bash, MSYS réécrit les chemins absolus passés aux conteneurs.
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL='*'

COMPOSE=(docker compose --env-file .env -f docker/compose.yml)

step() { printf '\n\033[36m==> %s\033[0m\n' "$1" >&2; }
ok()   { printf '\033[32m    OK  %s\033[0m\n' "$1" >&2; }
fail() { printf '\033[31m    KO  %s\033[0m\n' "$1" >&2; exit 1; }

contains() { [[ "$2" == *"$1"* ]]; }

trino_sql() {
  local sql="$1" format="${2:-TSV}"
  "${COMPOSE[@]}" exec -T trino trino --no-progress --output-format "$format" --execute "$sql" 2>/dev/null
}

trino_file() {
  "${COMPOSE[@]}" exec -T trino trino --no-progress --output-format "${2:-TSV}" -f "$1" 2>/dev/null
}

# --- 0. Construction du médaillon ---------------------------------------------
# Le test se suffit à lui-même à partir d'un Level 1 exécuté : il reconstruit ce
# qu'il s'apprête à vérifier, plutôt que de supposer un état préalable qu'un
# `down -v` aurait effacé.
step "0/9 — Construction du médaillon"
trino_file /sql/internal/create_bronze_views.sql >/dev/null
ok "vues bronze.* créées"

"${COMPOSE[@]}" exec -T spark python3 -m jobs.batch.silver 2>/dev/null | tail -8 | sed 's/^/    /'
"${COMPOSE[@]}" exec -T spark python3 -m jobs.batch.gold 2>/dev/null | tail -9 | sed 's/^/    /'
"${COMPOSE[@]}" exec -T spark python3 -m jobs.batch.regulatory \
  --report-date 2026-03-30 2>/dev/null | tail -2 | sed 's/^/    /'

# --- 1. Les trois zones du médaillon -----------------------------------------
step "1/9 — Les trois zones du médaillon"
for zone in bronze silver gold; do
  schemas=$(trino_sql "SHOW SCHEMAS FROM iceberg")
  contains "$zone" "$schemas" || fail "schéma ${zone} absent"
done
ok "bronze, silver et gold exposés dans Trino"

bronze=$(trino_sql "SELECT count(*) FROM iceberg.bronze.bank_transactions" | tr -dc '0-9')
silver=$(trino_sql "SELECT count(*) FROM iceberg.silver.bank_transactions" | tr -dc '0-9')
gold=$(trino_sql "SELECT count(*) FROM iceberg.gold.daily_transaction_volume" | tr -dc '0-9')
[ "${bronze:-0}" -gt 0 ] && [ "${silver:-0}" -gt 0 ] && [ "${gold:-0}" -gt 0 ] \
  || fail "une zone est vide (bronze=${bronze} silver=${silver} gold=${gold})"
ok "contenus distincts : ${bronze} lignes brutes, ${silver} nettoyées, ${gold} agrégées"

# --- 2. Qualité des transformations Silver ------------------------------------
step "2/9 — Transformations Silver"
qualite=$(trino_file /sql/internal/level2_silver_quality.sql)
read -r doublons enrichissement conversions ibans colonnes <<<"$(echo "$qualite" | tr '\t' ' ')"

[ "$doublons" = "0" ]       && ok "déduplication sur la clé naturelle" || fail "${doublons} doublon(s)"
[ "$enrichissement" = "0" ] && ok "jointure avec les référentiels complète" || fail "${enrichissement} ligne(s) non enrichie(s)"
[ "$conversions" = "0" ]    && ok "conversion en euros exacte" || fail "${conversions} conversion(s) fausse(s)"
[ "$ibans" = "0" ]          && ok "valeurs nulles conformes au métier" || fail "${ibans} IBAN nul hors Ghana"
[ "$colonnes" = "0" ]       && ok "champs personnels masqués, aucune valeur en clair" \
                            || fail "${colonnes} colonne(s) personnelle(s) en clair dans Silver"

# --- 3. Les 7 tables Gold -----------------------------------------------------
step "3/9 — Les 7 tables de KPIs Gold"
tables=$(trino_sql "SHOW TABLES FROM iceberg.gold")
for table in daily_transaction_volume npl_ratio_by_country customer_arpu_monthly \
             loss_ratio_by_product claims_processing_time mobile_money_daily_flow \
             cross_border_transfers; do
  contains "$table" "$tables" || fail "table gold.${table} absente"
done
ok "les 7 tables du §2.3 existent"

# Filtrage par pays, explicitement exigé par la grille.
filtre=$(trino_sql "SELECT count(*) FROM iceberg.gold.daily_transaction_volume
                    WHERE country_code = 'CI'" | tr -dc '0-9')
[ "${filtre:-0}" -gt 0 ] && ok "filtrage par pays opérationnel (${filtre} lignes pour la CI)" \
                         || fail "le filtrage par pays ne renvoie rien"

# --- 4. KPIs réglementaires ---------------------------------------------------
step "4/9 — KPIs réglementaires dans leurs fourchettes"
sante=$(trino_file /sql/internal/level2_health.sql)
read -r npl_pays npl_ok loss_couples loss_ok <<<"$(echo "$sante" | tr '\t' ' ')"

[ "$npl_ok" = "$npl_pays" ] \
  && ok "NPL entre 3 % et 8 % sur ${npl_ok}/${npl_pays} pays" \
  || fail "NPL hors fourchette sur $((npl_pays - npl_ok)) pays"
[ "$loss_ok" = "$loss_couples" ] \
  && ok "loss ratio entre 50 % et 85 % sur ${loss_ok}/${loss_couples} couples" \
  || fail "loss ratio hors fourchette sur $((loss_couples - loss_ok)) couples"

# --- 5. Rapport réglementaire -------------------------------------------------
step "5/9 — Rapport réglementaire J+1"
rapport=$(trino_sql "SELECT count(*) FROM iceberg.gold.regulatory_report" | tr -dc '0-9')
[ "${rapport:-0}" -gt 0 ] && ok "${rapport} ligne(s) de rapport produites" \
                          || fail "aucun rapport réglementaire"

# --- 6. DAGs Airflow ----------------------------------------------------------
step "6/9 — Les 4 DAGs Airflow"
dags=$("${COMPOSE[@]}" exec -T airflow-scheduler airflow dags list --output plain 2>/dev/null)
for dag in dag_ingest_raw dag_bronze_to_silver dag_silver_to_gold dag_regulatory_report; do
  contains "$dag" "$dags" || fail "DAG ${dag} absent"
done
ok "les 4 DAGs du §2.1 sont enregistrés"

erreurs=$("${COMPOSE[@]}" exec -T airflow-scheduler airflow dags list-import-errors 2>/dev/null || true)
contains "No data found" "$erreurs" && ok "aucune erreur d'analyse" \
                                    || fail "erreurs d'analyse de DAG : ${erreurs}"

# --- 7. Aucun identifiant en dur ----------------------------------------------
step "7/9 — Aucun identifiant codé en dur"
motdepasse=$(grep -E '^MINIO_ROOT_PASSWORD=' .env | cut -d= -f2-)
if [ -n "$motdepasse" ] && grep -rqF "$motdepasse" airflow/ jobs/ sql/ docker/ 2>/dev/null; then
  fail "le mot de passe MinIO apparaît dans le code versionné"
fi
ok "le mot de passe MinIO n'apparaît nulle part dans le code"

grep -q 'conn.waba_minio' airflow/dags/waba_common.py \
  && ok "les DAGs lisent leurs accès dans une Connection Airflow" \
  || fail "les DAGs ne référencent aucune Connection"

# --- 8. Idempotence des recalculs ---------------------------------------------
step "8/9 — Idempotence des recalculs Silver et Gold"
avant=$(trino_sql "SELECT count(*) FROM iceberg.silver.bank_transactions" | tr -dc '0-9')
avant_gold=$(trino_sql "SELECT count(*) FROM iceberg.gold.npl_ratio_by_country" | tr -dc '0-9')

"${COMPOSE[@]}" exec -T spark python3 -m jobs.batch.silver --tables bank_transactions >/dev/null 2>&1
"${COMPOSE[@]}" exec -T spark python3 -m jobs.batch.gold --tables npl_ratio_by_country >/dev/null 2>&1

apres=$(trino_sql "SELECT count(*) FROM iceberg.silver.bank_transactions" | tr -dc '0-9')
apres_gold=$(trino_sql "SELECT count(*) FROM iceberg.gold.npl_ratio_by_country" | tr -dc '0-9')

[ "$avant" = "$apres" ] && [ "$avant_gold" = "$apres_gold" ] \
  && ok "recalcul sans duplication (${apres} lignes Silver, ${apres_gold} lignes Gold)" \
  || fail "duplication au recalcul : Silver ${avant}->${apres}, Gold ${avant_gold}->${apres_gold}"

printf '\n\033[32m*** Level 2 opérationnel ***\033[0m\n\n'
