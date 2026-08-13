#!/usr/bin/env bash
# =============================================================================
# Test de fumée du socle Level 1.
#
# Prouve la chaîne complète générateur -> MinIO -> catalogue Iceberg REST -> Trino :
#   1. les trois buckets du sujet existent ;
#   2. Trino expose le catalogue `iceberg` ;
#   3. une table Iceberg partitionnée par country_code peut être créée,
#      alimentée et relue en SQL ;
#   4. les fichiers de données et de métadonnées atterrissent bien dans MinIO ;
#   5. le générateur produit et dépose un jeu de données sur les 8 pays ;
#   6. ces données satisfont les critères du Level 1 (intégrité référentielle,
#      partitionnement par pays, anomalies détectables) ;
#   7. Spark les ingère dans les 8 tables Iceberg raw.* ;
#   8. rejouer l'ingestion sur les mêmes données ne crée aucun doublon.
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
step "1/8 — Buckets MinIO"
for bucket in raw-landing lakehouse archive; do
  "${COMPOSE[@]}" exec -T minio test -d "/data/${bucket}" \
    && ok "bucket ${bucket}" || fail "bucket ${bucket} absent"
done

# --- 2. Catalogue Trino ------------------------------------------------------
step "2/8 — Catalogue Iceberg visible depuis Trino"
trino_sql "SHOW CATALOGS" | grep -qx iceberg \
  && ok "catalogue iceberg exposé" || fail "catalogue iceberg absent"

# --- 3. Aller-retour SQL sur une table Iceberg -------------------------------
step "3/8 — Création, écriture et lecture d'une table Iceberg partitionnée"
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
step "4/8 — Objets Iceberg présents dans le bucket lakehouse (API S3)"
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

# --- Nettoyage de la table de contrôle ---------------------------------------
trino_sql "DROP TABLE iceberg.smoke.ping" >/dev/null
trino_sql "DROP SCHEMA iceberg.smoke"     >/dev/null

# --- 5. Générateur -----------------------------------------------------------
step "5/8 — Génération et dépôt d'un jeu de données multi-pays"
# `--reuse-referentials` est essentiel en exécution répétée : régénérer les
# référentiels rendrait orphelines les clés des fichiers déjà déposés.
"${COMPOSE[@]}" exec -T streamlit python -m generator.seed \
  --preset demo --reuse-referentials --seed 42 2>&1 | tail -1 | sed 's/^/    /'
ok "jeu de données déposé dans raw-landing"

# --- 6. Conformité des données -----------------------------------------------
step "6/8 — Conformité des données déposées"
"${COMPOSE[@]}" exec -T streamlit python -m generator.verify \
  || fail "des contrôles de conformité ont échoué"

# --- 7. Ingestion Spark vers les tables Iceberg -------------------------------
step "7/8 — Ingestion Spark vers les 8 tables raw.*"
"${COMPOSE[@]}" exec -T spark python3 -m jobs.batch.ingest_raw 2>/dev/null \
  | tail -12 | sed 's/^/    /'

tables=$(trino_sql "SHOW TABLES FROM iceberg.raw")
for table in customers accounts branches products \
             bank_transactions insurance_operations mobile_money_payments loan_repayments; do
  echo "$tables" | grep -qx "$table" || fail "table raw.${table} absente"
done
ok "les 8 tables raw.* existent dans Trino"

# Somme des lignes des huit tables, servant de témoin à la vérification
# d'idempotence de l'étape suivante.
RAW_TOTAL_SQL="SELECT (SELECT count(*) FROM iceberg.raw.customers)
                    + (SELECT count(*) FROM iceberg.raw.accounts)
                    + (SELECT count(*) FROM iceberg.raw.branches)
                    + (SELECT count(*) FROM iceberg.raw.products)
                    + (SELECT count(*) FROM iceberg.raw.bank_transactions)
                    + (SELECT count(*) FROM iceberg.raw.insurance_operations)
                    + (SELECT count(*) FROM iceberg.raw.mobile_money_payments)
                    + (SELECT count(*) FROM iceberg.raw.loan_repayments)"

before=$(trino_sql "$RAW_TOTAL_SQL" | tr -dc '0-9')
[ "${before:-0}" -gt 0 ] || fail "les tables raw.* sont vides"
ok "${before} lignes ingérées, interrogeables en SQL"

# --- 8. Idempotence -----------------------------------------------------------
# Critère explicite de l'énoncé. La graine étant identique, le générateur
# reproduit exactement les mêmes identifiants : les fichiers redéposés portent
# un nouveau numéro de séquence mais un contenu ligne à ligne identique.
step "8/8 — Idempotence : réingestion des mêmes données"
"${COMPOSE[@]}" exec -T streamlit python -m generator.seed \
  --preset demo --reuse-referentials --seed 42 2>&1 | tail -1 | sed 's/^/    /'
"${COMPOSE[@]}" exec -T spark python3 -m jobs.batch.ingest_raw 2>/dev/null \
  | tail -12 | sed 's/^/    /'

after=$(trino_sql "$RAW_TOTAL_SQL" | tr -dc '0-9')
[ "$after" = "$before" ] \
  && ok "aucun doublon créé (${before} lignes avant et après)" \
  || fail "idempotence rompue : ${before} lignes avant, ${after} après"

printf '\n\033[32m*** Socle Level 1 opérationnel ***\033[0m\n\n'
